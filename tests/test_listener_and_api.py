"""Listener (with a stubbed `airflow` package), API client details, git history."""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import httpx
import pytest

from dagnose.collector import AirflowAPIError, AirflowClient
from dagnose.collector.git import recent_commits
from dagnose.models import FailureJob

# ---- listener ---------------------------------------------------------------------


@pytest.fixture
def listener(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    airflow = types.ModuleType("airflow")
    listeners = types.ModuleType("airflow.listeners")
    listeners.hookimpl = lambda fn: fn  # type: ignore[attr-defined]
    plugins_manager = types.ModuleType("airflow.plugins_manager")
    plugins_manager.AirflowPlugin = type("AirflowPlugin", (), {})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.listeners", listeners)
    monkeypatch.setitem(sys.modules, "airflow.plugins_manager", plugins_manager)
    monkeypatch.setenv("DAGNOSE_HOME", str(tmp_path))
    for name in ("dagnose.plugin", "dagnose.plugin.listener"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    return importlib.import_module("dagnose.plugin.listener")


def test_listener_spools_failed_task(listener: Any, tmp_path: Path) -> None:
    ti = types.SimpleNamespace(dag_id="d", run_id="r", task_id="t", try_number=2, map_index=None)
    listener.on_task_instance_failed(previous_state="running", task_instance=ti)
    files = list((tmp_path / "spool").glob("*.json"))
    assert len(files) == 1
    job = json.loads(files[0].read_text())
    assert job["dag_id"] == "d" and job["try_number"] == 2 and job["map_index"] == -1
    assert job["failed_at"]


def test_listener_never_raises(listener: Any, caplog: pytest.LogCaptureFixture) -> None:
    listener.on_task_instance_failed(previous_state=None, task_instance=object())
    assert "could not enqueue" in caplog.text


# ---- API client ---------------------------------------------------------------------


def _client(make_settings: Any, handler: Any, **overrides: Any) -> AirflowClient:
    return AirflowClient(make_settings(**overrides), transport=httpx.MockTransport(handler))


def test_detects_v1_and_uses_basic_auth(make_settings: Any) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/api/v2/version":
            return httpx.Response(404)
        if request.url.path == "/api/v1/version":
            return httpx.Response(200, json={"version": "2.10.5"})
        return httpx.Response(200, json={"fileloc": "/opt/airflow/dags/a.py"})

    with _client(make_settings, handler) as client:
        assert client.api == "v1"
        assert client.airflow_version() == "2.10.5"
        client.dag("my_dag")
    assert seen[-1].url.path == "/api/v1/dags/my_dag"
    assert seen[-1].headers["authorization"].startswith("Basic ")


def test_run_id_is_path_encoded(make_settings: Any) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.raw_path.decode())
        if request.url.path == "/auth/token":
            return httpx.Response(201, json={"access_token": "tok"})
        return httpx.Response(200, json={})

    job = FailureJob(dag_id="d", run_id="manual__2026-11-22T15:34:51+00:00", task_id="t")
    with _client(make_settings, handler, airflow_api="v2") as client:
        client.task_instance(job)
    assert (
        paths[-1]
        == "/api/v2/dags/d/dagRuns/manual__2026-11-22T15%3A34%3A51%2B00%3A00/taskInstances/t"
    )


def test_expired_token_logs_in_again(make_settings: Any) -> None:
    logins = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal logins
        if request.url.path == "/auth/token":
            logins += 1
            return httpx.Response(201, json={"access_token": f"tok{logins}"})
        if request.headers.get("authorization") == "Bearer tok1":
            return httpx.Response(401)
        return httpx.Response(200, json={"version": "3.3.2"})

    with _client(make_settings, handler, airflow_api="v2") as client:
        assert client.airflow_version() == "3.3.2"
    assert logins == 2


def test_unreachable_airflow_raises(make_settings: Any) -> None:
    with (
        _client(make_settings, lambda r: httpx.Response(404)) as client,
        pytest.raises(AirflowAPIError, match="No Airflow REST API"),
    ):
        _ = client.api


# ---- git ----------------------------------------------------------------------------


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_recent_commits_for_dag_file(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "dev@example.com")
    _git(tmp_path, "config", "user.name", "Dev")
    dag = tmp_path / "dags" / "orders.py"
    dag.parent.mkdir()
    dag.write_text("x = 1\n")
    (tmp_path / "other.py").write_text("y = 1\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "add orders dag")
    dag.write_text("x = 2\n" + "# padding\n" * 50)
    _git(tmp_path, "commit", "-qam", "change tax lookup")

    commits = recent_commits(tmp_path, "dags/orders.py", limit=5, diff_max_chars=250)
    assert [c.subject for c in commits] == ["change tax lookup", "add orders dag"]
    assert "+x = 2" in commits[0].diff and commits[0].diff_truncated
    assert "other.py" not in commits[1].diff


def test_recent_commits_missing_file(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    with pytest.raises(FileNotFoundError):
        recent_commits(tmp_path, "dags/nope.py", limit=5, diff_max_chars=100)
