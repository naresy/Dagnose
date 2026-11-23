"""The Doctor worker: a separate process that takes failure jobs off the queue and runs
the pipeline: collect -> redact -> save context -> diagnose -> save diagnosis -> notify.

Only redacted data is ever written to disk; the raw context lives in memory only.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from airflow_doctor.agent import (
    DiagnosisError,
    ProviderConfigError,
    ProviderError,
    ToolBox,
    diagnose,
    make_provider,
)
from airflow_doctor.collector import AirflowAPIError, AirflowClient, collect
from airflow_doctor.config import Settings
from airflow_doctor.models import DiagnosisRun, FailureJob, RedactedContext
from airflow_doctor.notifier import channels_configured, notify
from airflow_doctor.queue import JobQueue, QueuedJob
from airflow_doctor.redactor import RedactionSession, Redactor

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
STALE_AFTER_S = 15 * 60
RETRY_BASE_S = 30.0


def retry_delay_s(attempts: int) -> float:
    """Backoff before the next try when Airflow is unreachable: 30s, 60s, 120s, ..."""
    return float(RETRY_BASE_S * 2 ** max(attempts - 1, 0))


def context_filename(job: FailureJob) -> str:
    parts = [job.dag_id, job.task_id, job.run_id, f"try{job.try_number}"]
    if job.map_index >= 0:
        parts.append(f"map{job.map_index}")
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", "__".join(parts)) + ".json"


def save_context(ctx: RedactedContext, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / context_filename(ctx.job)
    path.write_text(ctx.model_dump_json(indent=2))
    return path


def save_diagnosis(run: DiagnosisRun, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / context_filename(run.job)
    path.write_text(run.model_dump_json(indent=2))
    return path


def process(qjob: QueuedJob, client: AirflowClient, settings: Settings) -> Path:
    # model_validate lets pydantic parse failed_at (accepts "Z" on every Python version).
    job = FailureJob.model_validate(
        {
            "dag_id": qjob.dag_id,
            "run_id": qjob.run_id,
            "task_id": qjob.task_id,
            "try_number": qjob.try_number,
            "map_index": qjob.map_index,
            "failed_at": qjob.failed_at,
        }
    )
    raw = collect(job, client, settings)
    session = Redactor.from_settings(settings).session()
    redacted = session.redact(raw)
    del raw  # only the redacted context goes anywhere from here
    if redacted.redactions:
        log.info("job %s: redacted %s", qjob.id, redacted.summary())
    path = save_context(redacted, settings.contexts_dir)
    run: DiagnosisRun | None = None
    reason: str | None = "diagnosis is turned off"
    if settings.diagnose:
        run, reason = run_diagnosis(qjob.id, redacted, client, session, settings)
    if channels_configured(settings):
        report = notify(redacted, run, settings, session=session, reason=reason, name=path.name)
        if report.sent:
            log.info("job %s: notified %s", qjob.id, ", ".join(report.sent))
    return path


def run_diagnosis(
    job_id: int,
    ctx: RedactedContext,
    client: AirflowClient,
    session: RedactionSession,
    settings: Settings,
) -> tuple[DiagnosisRun | None, str | None]:
    """Diagnose and save. Returns (run, None) or (None, why there is no diagnosis).
    A config problem (no credentials) or a bad answer is logged and skipped, so the
    saved context is not lost; an outage is re-raised so the job is retried."""
    try:
        provider = make_provider(settings)
        run = diagnose(
            ctx, provider, ToolBox(ctx, client, session), max_turns=settings.llm_max_turns
        )
    except ProviderConfigError as exc:
        log.warning("job %s: diagnosis skipped: %s", job_id, exc)
        return None, str(exc)
    except DiagnosisError as exc:
        log.warning("job %s: no diagnosis: %s", job_id, exc)
        return None, str(exc)
    except ProviderError:
        raise
    path = save_diagnosis(run, settings.diagnoses_dir)
    d = run.diagnosis
    log.info(
        "job %s: %s (%.2f, evidence %d/%d) -> %s",
        job_id, d.category, d.confidence, run.evidence_verified, run.evidence_total, path,
    )  # fmt: skip
    return run, None


def run(settings: Settings, *, once: bool = False, poll_interval_s: float = 2.0) -> int:
    """Process jobs until stopped (with once=True: until no job is due; jobs waiting
    for a retry stay queued for the next run).
    Returns the number of jobs processed."""
    queue = JobQueue(settings.queue_path)
    queue.requeue_stale(STALE_AFTER_S)
    processed = 0
    with AirflowClient(settings) as client:
        while True:
            queue.ingest_spool(settings.spool_dir)
            qjob = queue.claim()
            if qjob is None:
                if once:
                    return processed
                time.sleep(poll_interval_s)
                continue
            try:
                path = process(qjob, client, settings)
            except (AirflowAPIError, ProviderError) as exc:  # outage: worth retrying
                retry = qjob.attempts < MAX_ATTEMPTS
                delay = retry_delay_s(qjob.attempts)
                queue.mark_error(qjob.id, str(exc), retry=retry, retry_after_s=delay)
                log.warning("job %s failed (retry=%s, in %ss): %s", qjob.id, retry, delay, exc)
            except Exception as exc:
                queue.mark_error(qjob.id, f"{type(exc).__name__}: {exc}", retry=False)
                log.exception("job %s failed", qjob.id)
            else:
                queue.mark_done(qjob.id)
                log.info("job %s: %s.%s -> %s", qjob.id, qjob.dag_id, qjob.task_id, path)
            processed += 1
