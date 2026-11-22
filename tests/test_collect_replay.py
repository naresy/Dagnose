"""Collector against recorded real Airflow API traffic (no live Airflow needed)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from airflow_doctor.collector import AirflowClient, collect
from airflow_doctor.models import FailureJob
from tests.conftest import FIXTURES, ReplayTransport, load_recording, recordings

# What a correct collection must find for each broken DAG: exception type, and the
# DAG-code frame (file, line) the failure points to.
EXPECTED = {
    "broken_python_keyerror": ("KeyError", "python_keyerror.py", 24),
    "broken_missing_table": ("OperationalError", "missing_table.py", 23),
    "broken_task_timeout": ("AirflowTaskTimeout", "task_timeout.py", 22),
    "broken_bad_connection": ("AirflowNotFoundException", "bad_connection.py", 20),
    "broken_missing_dependency": ("ModuleNotFoundError", "missing_dependency.py", 19),
}

V2 = recordings("v2")


def _collect(path: Path, settings: Any) -> Any:
    rec = load_recording(path)
    job = FailureJob(**rec["job"])
    with AirflowClient(settings, transport=ReplayTransport(rec["exchanges"])) as client:
        return collect(job, client, settings)


def test_all_broken_dags_are_recorded() -> None:
    assert {p.stem for p in V2} == set(EXPECTED)


@pytest.mark.parametrize("path", V2, ids=lambda p: p.stem)
def test_collect_v2(path: Path, make_settings: Any) -> None:
    ctx = _collect(path, make_settings())
    exc_type, user_file, user_line = EXPECTED[path.stem]

    assert ctx.collection_errors == []
    assert ctx.api_version == "v2"
    assert ctx.airflow_version and ctx.airflow_version.startswith("3.")
    assert ctx.task_state == "failed"
    assert ctx.exception is not None and ctx.exception.type == exc_type
    user_frames = [f for f in ctx.exception.frames if f.in_user_code]
    assert user_frames, "the failing line in DAG code must be found"
    assert user_frames[-1].filename.endswith(user_file)
    assert user_frames[-1].lineno == user_line
    assert ctx.dag_file and ctx.dag_file.endswith(user_file)
    assert ctx.dag_source and "@dag(" in ctx.dag_source
    assert ctx.log_tail and any(line.level == "error" for line in ctx.log_tail)
    assert [line.n for line in ctx.log_tail] == list(range(1, len(ctx.log_tail) + 1))
    assert any(r.is_current and r.state == "failed" for r in ctx.run_history)


@pytest.mark.parametrize("path", V2, ids=lambda p: p.stem)
def test_collect_matches_saved_context(path: Path, make_settings: Any) -> None:
    """The saved contexts feed later stages (redactor, agent); keep them in sync."""
    ctx = _collect(path, make_settings())
    saved = json.loads((FIXTURES / "contexts" / "v2" / path.name).read_text())
    got = json.loads(ctx.model_dump_json())
    for volatile in ("collected_at", "recent_commits"):  # git is not part of the replay
        saved.pop(volatile)
        got.pop(volatile)
    assert got == saved


def test_keyerror_context_shows_the_bad_payload(make_settings: Any) -> None:
    ctx = _collect(FIXTURES / "api" / "v2" / "broken_python_keyerror.json", make_settings())
    assert ctx.task_params["op_args"] == [{"amount": 19.99, "currency": "USD", "order_id": 42}]


def test_timeout_log_keeps_task_stdout(make_settings: Any) -> None:
    ctx = _collect(FIXTURES / "api" / "v2" / "broken_task_timeout.json", make_settings())
    messages = [line.message for line in ctx.log_tail]
    assert any("Fetching page 1" in m for m in messages)
    assert "Process timed out" in messages


def test_one_failing_endpoint_does_not_lose_the_rest(make_settings: Any) -> None:
    rec = load_recording(FIXTURES / "api" / "v2" / "broken_python_keyerror.json")
    exchanges = [
        {**ex, "status": 500, "body": "boom"} if "/dagSources/" in ex["path"] else ex
        for ex in rec["exchanges"]
    ]
    settings = make_settings()
    with AirflowClient(settings, transport=ReplayTransport(exchanges)) as client:
        ctx = collect(FailureJob(**rec["job"]), client, settings)
    assert ctx.dag_source is None
    assert len(ctx.collection_errors) == 1 and "dag source" in ctx.collection_errors[0]
    assert ctx.exception is not None and ctx.exception.type == "KeyError"


def test_try_number_defaults_to_latest(make_settings: Any) -> None:
    rec = load_recording(FIXTURES / "api" / "v2" / "broken_python_keyerror.json")
    job = FailureJob(**{**rec["job"], "try_number": None})
    settings = make_settings()
    with AirflowClient(settings, transport=ReplayTransport(rec["exchanges"])) as client:
        ctx = collect(job, client, settings)
    assert ctx.job.try_number == 1
    assert ctx.exception is not None
