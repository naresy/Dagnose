"""`dagnose check`, against a fake Airflow, a fake model and fake Slack / SMTP."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from dagnose.agent import ProviderConfigError
from dagnose.check import Result, run_checks


class FakeAirflow:
    def __init__(
        self,
        *,
        login_ok: bool = True,
        plugins: list[str] | None = None,
        api: str = "v2",
        up: bool = True,
        failed: list[dict[str, Any]] | None = None,
        log: list[dict[str, Any]] | None = None,
    ) -> None:
        self.login_ok, self.plugins, self.api, self.up = (
            login_ok,
            plugins or ["dagnose"],
            api,
            up,
        )
        self.failed, self.log = failed or [], log or []
        self.slack: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.url.host == "hooks.slack.com":
            self.slack.append(json.loads(request.content))
            return httpx.Response(200, text="ok")
        if not self.up:
            return httpx.Response(404)
        if path.endswith("/version"):
            ok = path.startswith(f"/api/{self.api}/")
            return httpx.Response(
                200 if ok else 404, json={"version": "3.3.2" if self.api == "v2" else "2.11.1"}
            )
        if path == "/auth/token":
            return httpx.Response(201 if self.login_ok else 401, json={"access_token": "t"})
        if not self.login_ok:
            return httpx.Response(403, json={"title": "Forbidden"})
        if path.endswith("/plugins"):
            return httpx.Response(200, json={"plugins": [{"name": n} for n in self.plugins]})
        if path.endswith("/taskInstances/list"):
            return httpx.Response(200, json={"task_instances": self.failed})
        if "/logs/" in path:
            return httpx.Response(200, json={"content": self.log})
        return httpx.Response(200, json={"dags": []})


class FakeProvider:
    name, model = "anthropic", "claude-opus-5-5"

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def verify(self) -> int:
        if self.error:
            raise self.error
        return 5


def results(
    make_settings: Any, airflow: FakeAirflow, provider: FakeProvider | None = None, **kwargs: Any
) -> dict[str, Result]:
    settings = make_settings(**kwargs)
    out = run_checks(
        settings,
        transport=httpx.MockTransport(airflow),
        provider_factory=lambda s: provider or FakeProvider(),
    )
    return {r.name: r for r in out}


def test_all_good(make_settings: Any) -> None:
    r = results(make_settings, FakeAirflow(), diagnose=True, watch="both")
    assert {k: v.status for k, v in r.items()} == {
        "Airflow API": "ok",
        "Airflow login": "ok",
        "Finding failures": "ok",
        "Task logs": "skip",  # no recent failure to read
        "Storage": "ok",
        "Git history": "skip",
        "LLM": "ok",
        "Slack": "skip",
        "Email": "skip",
        "Redaction": "ok",
    }
    assert "Airflow 3.3.2" in r["Airflow API"].detail
    assert "plugin installed" in r["Finding failures"].detail
    assert "no tokens spent" in r["LLM"].detail


def test_airflow_unreachable(make_settings: Any) -> None:
    r = results(make_settings, FakeAirflow(up=False))
    assert r["Airflow API"].status == "fail" and "DAGNOSE_AIRFLOW_URL" in (
        r["Airflow API"].fix or ""
    )
    assert r["Airflow login"].status == r["Finding failures"].status == "skip"


@pytest.mark.parametrize("api", ["v1", "v2"])
def test_bad_login(api: str, make_settings: Any) -> None:
    r = results(make_settings, FakeAirflow(login_ok=False, api=api))
    login = r["Airflow login"]
    assert login.status == "fail" and "Viewer role" in (login.fix or "")
    assert ("basic_auth" in (login.fix or "")) is (api == "v1")


def test_plugin_only_without_plugin_fails(make_settings: Any) -> None:
    r = results(make_settings, FakeAirflow(plugins=["other"]), watch="plugin")
    assert r["Finding failures"].status == "fail"
    assert "DAGNOSE_WATCH=api" in (r["Finding failures"].fix or "")
    # The same Airflow is fine when polling is on.
    assert (
        results(make_settings, FakeAirflow(plugins=["other"]), watch="both")[
            "Finding failures"
        ].status
        == "ok"
    )


def test_llm_problems(make_settings: Any) -> None:
    bad = FakeProvider(ProviderConfigError("no Anthropic credentials"))
    r = results(make_settings, FakeAirflow(), bad, diagnose=True)
    assert r["LLM"].status == "fail" and "DAGNOSE_DIAGNOSE=false" in (r["LLM"].fix or "")
    assert results(make_settings, FakeAirflow(), bad, diagnose=False)["LLM"].status == "skip"


def test_git(make_settings: Any, tmp_path: Path) -> None:
    assert (
        results(make_settings, FakeAirflow(), git_repo_path=tmp_path)["Git history"].status
        == "fail"
    )
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "dags").mkdir()
    r = results(make_settings, FakeAirflow(), git_repo_path=tmp_path, git_dags_subdir="dags")
    assert r["Git history"].status == "ok"


def test_slack_test_message_only_when_asked(make_settings: Any) -> None:
    airflow = FakeAirflow()
    hook = "https://hooks.slack.com/services/T0/B0/x"
    settings = make_settings(slack_webhook_url=hook)
    quiet = {
        r.name: r
        for r in run_checks(
            settings,
            transport=httpx.MockTransport(airflow),
            provider_factory=lambda s: FakeProvider(),
        )
    }
    assert quiet["Slack"].status == "ok" and airflow.slack == []
    loud = {
        r.name: r
        for r in run_checks(
            settings,
            send_test=True,
            transport=httpx.MockTransport(airflow),
            provider_factory=lambda s: FakeProvider(),
        )
    }
    assert loud["Slack"].detail == "test message sent" and len(airflow.slack) == 1


def test_email_partly_configured(make_settings: Any) -> None:
    r = results(make_settings, FakeAirflow(), smtp_host="smtp.example.test")
    assert r["Email"].status == "fail" and "EMAIL_TO" in (r["Email"].fix or "")


def test_cli_exit_codes(make_settings: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from typer.testing import CliRunner

    from dagnose import check as check_mod
    from dagnose.cli import app

    ok = [Result("Airflow API", "ok", "fine"), Result("Slack", "warn", "odd URL", "fix it")]
    monkeypatch.setattr(check_mod, "run_checks", lambda s, **k: iter(ok))
    out = CliRunner().invoke(app, ["check"])
    assert (
        out.exit_code == 0 and "All good (1 warning(s))" in out.stdout and "→ fix it" in out.stdout
    )

    bad = [Result("LLM", "fail", "no key", "set the key")]
    monkeypatch.setattr(check_mod, "run_checks", lambda s, **k: iter(bad))
    out = CliRunner().invoke(app, ["check"])
    assert out.exit_code == 1 and "1 problem(s) to fix" in out.stdout


FAILED_TI = {
    "dag_id": "sales",
    "dag_run_id": "r1",
    "task_id": "load",
    "try_number": 1,
    "map_index": -1,
    "end_date": "2026-11-23T10:00:00Z",
}


def test_task_log_readable(make_settings: Any) -> None:
    log = [{"event": "Task failed with exception", "level": "error", "logger": "task"}]
    r = results(make_settings, FakeAirflow(failed=[FAILED_TI], log=log))
    assert r["Task logs"].status == "ok" and "sales.load" in r["Task logs"].detail


def test_task_log_not_served_by_airflow(make_settings: Any) -> None:
    """Found while testing the install guide: Airflow components without a shared
    [api] secret_key cannot serve task logs, so diagnoses would miss the error."""
    log = [
        {"event": "::group::Log message source details"},
        {"event": "!!!! Please make sure that all your Airflow components (e.g. schedulers, api-servers) "
                  "have the same 'secret_key' configured in '[api]' section"},
        {"event": "::endgroup::"},
    ]  # fmt: skip
    r = results(make_settings, FakeAirflow(failed=[FAILED_TI], log=log))
    assert r["Task logs"].status == "warn"
    assert "AIRFLOW__API__SECRET_KEY" in (r["Task logs"].fix or "")
