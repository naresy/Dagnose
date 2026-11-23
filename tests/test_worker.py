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


def test_worker_saves_only_redacted_context(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The raw context (with secrets) must never be written to disk."""
    from tests.conftest import FIXTURES, ReplayTransport, load_recording

    rec = load_recording(FIXTURES / "api" / "v2" / "broken_leaky_credentials.json")
    settings = make_settings()
    job = rec["job"]
    JobQueue(settings.queue_path).enqueue(
        job["dag_id"], job["run_id"], job["task_id"], job["try_number"]
    )
    real_client = airflow_api.AirflowClient
    monkeypatch.setattr(
        worker,
        "AirflowClient",
        lambda s: real_client(s, transport=ReplayTransport(rec["exchanges"])),
    )

    assert worker.run(settings, once=True) == 1
    [saved] = list(settings.contexts_dir.glob("*.json"))
    text = saved.read_text()
    for secret in (
        "Sup3r-S3cret-Pw",
        "data-oncall@example.com",
        "ZmFrZS1zaWduYXR1cmUtZm9yLXRlc3Rz",
    ):
        assert secret not in text
    assert '"redactions"' in text and "[REDACTED:password#1]" in text
