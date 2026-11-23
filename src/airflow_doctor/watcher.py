"""Find failed tasks by polling the Airflow REST API (no plugin needed).

This is what makes Airflow Doctor work where the listener plugin cannot: Kubernetes
and Celery workers on other machines (no shared spool folder), managed Airflow
(MWAA, Cloud Composer, Astronomer), and tasks killed from outside (out of memory,
lost worker), which Airflow marks failed later without the task's listener running.

Each poll asks for tasks that reached state "failed" since the last poll, minus an
overlap for clock differences between this machine and Airflow. Re-seeing a failure
is harmless: the queue keeps one job per DAG run / task / try / map index.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow_doctor.collector import AirflowClient
from airflow_doctor.config import Settings
from airflow_doctor.queue import JobQueue

log = logging.getLogger(__name__)

WATERMARK_KEY = "poll_watermark"


def poll_failures(
    client: AirflowClient,
    queue: JobQueue,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> int:
    """Queue failed tasks found since the last poll. Returns how many were new.
    Raises AirflowAPIError when Airflow cannot be reached; the watermark then stays,
    so nothing is skipped."""
    now = now or datetime.now(timezone.utc)
    last = queue.get_state(WATERMARK_KEY)
    if last:
        since = datetime.fromisoformat(last) - timedelta(seconds=settings.poll_overlap_s)
    else:
        since = now - timedelta(minutes=settings.poll_lookback_minutes)

    added = 0
    for ti in client.failed_task_instances(since, settings.watch_dag_ids or None):
        map_index = ti.get("map_index")
        added += queue.enqueue(
            dag_id=str(ti["dag_id"]),
            run_id=str(ti["dag_run_id"]),
            task_id=str(ti["task_id"]),
            try_number=ti.get("try_number"),
            map_index=-1 if map_index is None else int(map_index),
            failed_at=ti.get("end_date"),
        )
    queue.set_state(WATERMARK_KEY, now.isoformat())
    if added:
        log.info("poll: %d new failed task(s) since %s", added, since.isoformat())
    return added
