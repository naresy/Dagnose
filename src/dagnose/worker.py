"""The Dagnose worker: a separate process that takes failure jobs off the queue and runs
the pipeline: collect -> redact -> save context -> diagnose -> save diagnosis -> notify.

Only redacted data is ever written to disk; the raw context lives in memory only.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from dagnose.agent import (
    DiagnosisError,
    ProviderConfigError,
    ProviderError,
    ToolBox,
    diagnose,
    make_provider,
)
from dagnose.collector import AirflowAPIError, AirflowClient, collect
from dagnose.config import Settings
from dagnose.incidents import Incident, IncidentStore, fingerprint
from dagnose.models import DiagnosisRun, FailureJob, RedactedContext, Usage
from dagnose.notifier import channels_configured, notify
from dagnose.queue import JobQueue, QueuedJob
from dagnose.redactor import RedactionSession, Redactor
from dagnose.watcher import poll_failures

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

    store = IncidentStore(settings.queue_path)
    window_s = settings.dedupe_window_hours * 3600
    fp = fingerprint(redacted)
    incident = (
        store.record(redacted, window_s=window_s)
        if window_s > 0
        else Incident(fp, 1, time.time(), None, None, 0, is_new=True)
    )

    run: DiagnosisRun | None = None
    reason: str | None = "diagnosis is turned off"
    if settings.diagnose:
        if incident.diagnosis_path:
            run = reuse_diagnosis(Path(incident.diagnosis_path), redacted, fp)
            save_diagnosis(run, settings.diagnoses_dir)
            log.info("job %s: same failure seen %d×, diagnosis reused", qjob.id, incident.count)
        elif store.take_model_call(settings.max_diagnoses_per_hour):
            run, reason = run_diagnosis(qjob.id, redacted, client, session, settings, fp)
            if run is not None and window_s > 0:
                store.save_diagnosis(fp, settings.diagnoses_dir / context_filename(run.job))
        else:
            reason = (
                f"the limit of {settings.max_diagnoses_per_hour} diagnoses per hour was "
                "reached (DAGNOSE_MAX_DIAGNOSES_PER_HOUR)"
            )
            log.warning("job %s: %s", qjob.id, reason)

    if channels_configured(settings):
        due = (
            incident.is_new
            or incident.last_notified_at is None
            or time.time() - incident.last_notified_at >= settings.renotify_minutes * 60
        )
        if not due:
            store.mark_suppressed(fp)
            log.info(
                "job %s: same failure %d×, card skipped (renotify later)", qjob.id, incident.count
            )
            return path
        report = notify(
            redacted, run, settings, session=session, reason=reason, name=path.name,
            occurrence=incident.count, first_seen=incident.first_seen,
        )  # fmt: skip
        if report.sent:
            if window_s > 0:
                store.mark_notified(fp)
            log.info("job %s: notified %s", qjob.id, ", ".join(report.sent))
    return path


def reuse_diagnosis(path: Path, ctx: RedactedContext, fp: str) -> DiagnosisRun:
    """The earlier diagnosis of the same failure, for this job, at no model cost."""
    earlier = DiagnosisRun.model_validate_json(path.read_text())
    return earlier.model_copy(
        update={
            "job": ctx.job,
            "fingerprint": fp,
            "reused_from": earlier.reused_from or earlier.job.run_id,
            "usage": Usage(),
            "turns": 0,
            "tool_calls": [],
            "duration_s": 0.0,
            "created_at": datetime.now(timezone.utc),
        }
    )


def run_diagnosis(
    job_id: int,
    ctx: RedactedContext,
    client: AirflowClient,
    session: RedactionSession,
    settings: Settings,
    fp: str | None = None,
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
    run.fingerprint = fp
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
    use_spool = settings.watch in ("plugin", "both")
    use_api = settings.watch in ("api", "both")
    next_poll = 0.0  # poll right away
    with AirflowClient(settings) as client:
        while True:
            if use_spool:
                queue.ingest_spool(settings.spool_dir)
            if use_api and time.monotonic() >= next_poll:
                try:
                    poll_failures(client, queue, settings)
                except AirflowAPIError as exc:
                    log.warning("poll: Airflow API not reachable, will retry: %s", exc)
                next_poll = time.monotonic() + settings.poll_interval_s
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
