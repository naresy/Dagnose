"""Same failure again: fingerprints, reused diagnoses, quiet repeats, hourly model limit."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dagnose import worker
from dagnose.incidents import IncidentStore, fingerprint, signature
from dagnose.models import (
    ExceptionInfo,
    FailureContext,
    FailureJob,
    LogLine,
    RedactedContext,
    StackFrame,
)
from dagnose.notifier import NotifyReport
from dagnose.queue import JobQueue
from dagnose.redactor import Redactor
from tests.conftest import FIXTURES
from tests.test_agent import ScriptedProvider, answer

KEYERROR = FIXTURES / "contexts" / "v2" / "demo_orders_tax.json"


def ctx_of(path: Path, run_id: str | None = None) -> RedactedContext:
    raw = FailureContext.model_validate_json(path.read_text())
    if run_id:
        raw.job = raw.job.model_copy(update={"run_id": run_id})
    return Redactor().redact(raw)


def failing(
    message: str, *, task: str = "t", line: int = 26, exc_type: str = "AirflowTaskTimeout"
) -> RedactedContext:
    frames = [StackFrame(filename="/opt/airflow/dags/demo/export.py", lineno=line, name="export")]
    return RedactedContext(
        job=FailureJob(dag_id="d", run_id="manual__x", task_id=task),
        exceptions=[ExceptionInfo(type=exc_type, message=message, frames=frames)],
        collected_at="2026-11-24T00:00:00Z",  # type: ignore[arg-type]
    )


# ---- fingerprints ---------------------------------------------------------------------


def test_same_failure_different_details_same_fingerprint() -> None:
    a = failing(
        "Timeout, PID: 66 after 10.0s at 2026-11-23T10:00:01Z (ti 01a203e5-5607-7a6f-ab49-0ba768892e74)"
    )
    b = failing(
        "Timeout, PID: 9041 after 10.2s at 2026-11-24T03:07:44Z (ti 9f9e03e5-1111-7a6f-ab49-0ba768892eff)"
    )
    assert fingerprint(a) == fingerprint(b)
    assert (
        signature(a)
        == "d | t | export.py:26 | AirflowTaskTimeout: Timeout, PID: <n> after <n>s at <time> (ti <uuid>)"
    )


@pytest.mark.parametrize(
    "other",
    [
        failing("Timeout, PID: 66", task="other_task"),  # another task
        failing("Timeout, PID: 66", line=30),  # another line
        failing("Timeout, PID: 66", exc_type="KeyError"),  # another exception
    ],
)
def test_different_failure_different_fingerprint(other: RedactedContext) -> None:
    assert fingerprint(failing("Timeout, PID: 66")) != fingerprint(other)


def test_different_key_is_a_different_failure() -> None:
    assert fingerprint(failing("'country'", exc_type="KeyError")) != fingerprint(
        failing("'region'", exc_type="KeyError")
    )


def test_same_failure_on_airflow_2_and_3() -> None:
    """The timeout DAG fails the same way on both versions (PIDs differ)."""
    v1 = ctx_of(FIXTURES / "contexts" / "v1" / "demo_partner_export.json")
    v2 = ctx_of(FIXTURES / "contexts" / "v2" / "demo_partner_export.json")
    assert v1.exception and v2.exception and v1.exception.message != v2.exception.message
    assert fingerprint(v1) == fingerprint(v2)


def test_no_exception_uses_the_last_error_line() -> None:
    c = RedactedContext(
        job=FailureJob(dag_id="d", run_id="r", task_id="t"),
        log_tail=[LogLine(n=1, level="error", message="Process exited with -9 (pid 812)")],
        collected_at="2026-11-24T00:00:00Z",  # type: ignore[arg-type]
    )
    assert signature(c) == "d | t | - | Process exited with -<n> (pid <n>)"


# ---- store ----------------------------------------------------------------------------


def test_store_counts_and_window(tmp_path: Path) -> None:
    store = IncidentStore(tmp_path / "q.db")
    c = failing("boom")
    first = store.record(c, window_s=3600, now=1000)
    again = store.record(c, window_s=3600, now=2000)
    assert (first.is_new, first.count) == (True, 1)
    assert (again.is_new, again.count, again.first_seen) == (False, 2, 1000)
    later = store.record(c, window_s=3600, now=2000 + 3601)  # quiet for longer than the window
    assert (later.is_new, later.count) == (True, 1)


def test_store_reuses_only_existing_diagnosis(tmp_path: Path) -> None:
    store = IncidentStore(tmp_path / "q.db")
    c = failing("boom")
    fp = store.record(c, window_s=3600, now=1000).fingerprint
    store.save_diagnosis(fp, tmp_path / "gone.json")
    assert store.record(c, window_s=3600, now=1001).diagnosis_path is None  # file missing
    (tmp_path / "d.json").write_text("{}")
    store.save_diagnosis(fp, tmp_path / "d.json")
    assert store.record(c, window_s=3600, now=1002).diagnosis_path == str(tmp_path / "d.json")


def test_hourly_model_limit(tmp_path: Path) -> None:
    store = IncidentStore(tmp_path / "q.db")
    assert [store.take_model_call(2, now=t) for t in (0, 10, 20)] == [True, True, False]
    assert store.take_model_call(2, now=3605)  # the first call is more than an hour old
    assert all(store.take_model_call(0, now=4000 + i) for i in range(50))  # 0 = no limit


# ---- the worker -----------------------------------------------------------------------


@pytest.fixture
def worker_env(make_settings: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A worker whose collect returns the KeyError failure for every job, a scripted
    model, and a captured notifier."""
    settings = make_settings(diagnose=True, slack_webhook_url="https://hooks.example.test/x")
    provider = ScriptedProvider([answer() for _ in range(10)])
    cards: list[dict[str, Any]] = []

    def fake_collect(job: FailureJob, client: Any, s: Any) -> FailureContext:
        raw = FailureContext.model_validate_json(KEYERROR.read_text())
        raw.job = job
        return raw

    def fake_notify(ctx: Any, run: Any, s: Any, **kwargs: Any) -> NotifyReport:
        cards.append({"run": run, **kwargs})
        return NotifyReport(sent=["slack"])

    monkeypatch.setattr(worker, "collect", fake_collect)
    monkeypatch.setattr(worker, "make_provider", lambda s: provider)
    monkeypatch.setattr(worker, "notify", fake_notify)

    def run_jobs(n: int, **overrides: Any) -> None:
        s = settings.model_copy(update=overrides)
        queue = JobQueue(s.queue_path)
        for i in range(n):
            queue.enqueue("demo_orders_tax", f"scheduled__{i:03d}", "compute_tax", 1)
        worker.run(s, once=True)

    return settings, provider, cards, run_jobs


def test_repeats_reuse_the_diagnosis_and_stay_quiet(worker_env: Any) -> None:
    """A DAG failing the same way 5 times: one model call, one card."""
    settings, provider, cards, run_jobs = worker_env
    run_jobs(5)
    assert len(provider.sent) == 1  # the model was called once
    saved = [json.loads(p.read_text()) for p in sorted(settings.diagnoses_dir.glob("*.json"))]
    assert len(saved) == 5
    assert sum(d["reused_from"] is not None for d in saved) == 4
    assert all(d["fingerprint"] == saved[0]["fingerprint"] for d in saved)
    assert len(cards) == 1 and cards[0]["occurrence"] == 1


def test_still_failing_card_after_the_renotify_interval(worker_env: Any) -> None:
    settings, provider, cards, run_jobs = worker_env
    run_jobs(3, renotify_minutes=0)
    assert [c["occurrence"] for c in cards] == [1, 2, 3]
    assert cards[2]["run"].reused_from is not None
    from dagnose.notifier import build_card

    card = build_card(
        ctx_of(KEYERROR),
        cards[2]["run"],
        ui_url=None,
        occurrence=3,
        first_seen=cards[2]["first_seen"],
    )
    assert card.title == "demo_orders_tax.compute_tax still failing (3×)"
    assert card.note and "Same failure 3×" in card.note and "no new model call" in card.note


def test_hourly_limit_stops_model_calls(worker_env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Different failures beyond the limit are still collected and announced, not diagnosed."""
    settings, provider, cards, run_jobs = worker_env
    import dagnose.worker as w

    counter = iter(range(100))
    monkeypatch.setattr(
        w, "fingerprint", lambda ctx: f"fp{next(counter)}"
    )  # every failure distinct
    run_jobs(3, max_diagnoses_per_hour=2, dedupe_window_hours=0)
    assert len(provider.sent) == 2
    assert len(cards) == 3
    assert cards[2]["run"] is None and "limit of 2 diagnoses per hour" in cards[2]["reason"]


def test_dedupe_off(worker_env: Any) -> None:
    settings, provider, cards, run_jobs = worker_env
    run_jobs(3, dedupe_window_hours=0)
    assert len(provider.sent) == 3 and len(cards) == 3
