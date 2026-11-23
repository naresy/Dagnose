from __future__ import annotations

from typing import Any

import httpx
import pytest

from airflow_doctor import worker
from airflow_doctor.collector import airflow_api
from airflow_doctor.queue import JobQueue


def test_retry_delay_grows() -> None:
    assert [worker.retry_delay_s(n) for n in (1, 2, 3)] == [30.0, 60.0, 120.0]


def test_airflow_down_keeps_job_for_later(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An outage must not burn all retries at once and mark the job as failed."""
    settings = make_settings()
    JobQueue(settings.queue_path).enqueue("d", "r", "t", 1)
    down = httpx.MockTransport(lambda request: httpx.Response(503))
    real_client = airflow_api.AirflowClient
    monkeypatch.setattr(worker, "AirflowClient", lambda s: real_client(s, transport=down))

    assert worker.run(settings, once=True) == 1  # one attempt, then waits
    queue = JobQueue(settings.queue_path)
    assert queue.counts() == {"pending": 1}
    assert queue.claim() is None  # backoff not over yet
