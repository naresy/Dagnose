"""The Doctor worker: a separate process that takes failure jobs off the queue and runs
the pipeline. Step 1 runs collect only and saves each FailureContext as JSON."""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

from airflow_doctor.collector import AirflowAPIError, AirflowClient, collect
from airflow_doctor.config import Settings
from airflow_doctor.models import FailureContext, FailureJob
from airflow_doctor.queue import JobQueue, QueuedJob

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


def save_context(ctx: FailureContext, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / context_filename(ctx.job)
    path.write_text(ctx.model_dump_json(indent=2))
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
    ctx = collect(job, client, settings)
    return save_context(ctx, settings.contexts_dir)


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
            except AirflowAPIError as exc:  # Airflow unreachable / auth: worth retrying
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
