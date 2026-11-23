from __future__ import annotations

import json
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


def _leaky_job(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch, **settings_overrides: Any
) -> Any:
    from tests.conftest import FIXTURES, ReplayTransport, load_recording

    rec = load_recording(FIXTURES / "api" / "v2" / "broken_leaky_credentials.json")
    settings = make_settings(**settings_overrides)
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
    return settings


def test_worker_saves_diagnosis(make_settings: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.test_agent import ScriptedProvider, answer

    settings = _leaky_job(make_settings, monkeypatch, diagnose=True)
    fake = ScriptedProvider([answer(category="permissions", evidence=[])])
    monkeypatch.setattr(worker, "make_provider", lambda s: fake)

    assert worker.run(settings, once=True) == 1
    [saved] = list(settings.diagnoses_dir.glob("*.json"))
    run = json.loads(saved.read_text())
    assert run["diagnosis"]["category"] == "permissions"
    assert run["job"]["dag_id"] == "broken_leaky_credentials"
    # The model only ever saw the redacted context.
    assert "Sup3r-S3cret-Pw" not in fake.started["prompt"]
    assert "[REDACTED:password#1]" in fake.started["prompt"]
    assert JobQueue(settings.queue_path).counts() == {"done": 1}


def test_worker_without_credentials_still_saves_context(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from airflow_doctor.agent import ProviderConfigError

    settings = _leaky_job(make_settings, monkeypatch, diagnose=True)

    def no_key(s: Any) -> Any:
        raise ProviderConfigError("no credentials")

    monkeypatch.setattr(worker, "make_provider", no_key)
    assert worker.run(settings, once=True) == 1
    assert len(list(settings.contexts_dir.glob("*.json"))) == 1
    assert not settings.diagnoses_dir.exists()
    assert JobQueue(settings.queue_path).counts() == {"done": 1}


def test_worker_retries_when_the_model_is_down(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from airflow_doctor.agent import ProviderError
    from airflow_doctor.agent.provider import Turn

    settings = _leaky_job(make_settings, monkeypatch, diagnose=True)

    class Down:
        name, model = "down", "m"

        def start(self, **kwargs: Any) -> Any:
            return self

        def send(self, *args: Any, **kwargs: Any) -> Turn:
            raise ProviderError("overloaded")

    monkeypatch.setattr(worker, "make_provider", lambda s: Down())
    assert worker.run(settings, once=True) == 1
    assert JobQueue(settings.queue_path).counts() == {"pending": 1}  # retried later, not lost


def test_worker_notifies_with_the_reason(
    make_settings: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _leaky_job(
        make_settings, monkeypatch, slack_webhook_url="https://hooks.example.test/x"
    )
    sent: list[dict[str, Any]] = []

    def fake_notify(ctx: Any, run: Any, s: Any, **kwargs: Any) -> Any:
        from airflow_doctor.notifier import NotifyReport

        sent.append({"ctx": ctx, "run": run, **kwargs})
        return NotifyReport(sent=["slack"])

    monkeypatch.setattr(worker, "notify", fake_notify)
    assert worker.run(settings, once=True) == 1
    [call] = sent
    assert call["run"] is None and call["reason"] == "diagnosis is turned off"
    assert call["session"] is not None  # same redaction session as the context
    assert "Sup3r-S3cret-Pw" not in call["ctx"].model_dump_json()
