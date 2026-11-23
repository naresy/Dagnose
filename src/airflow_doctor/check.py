"""`airflow-doctor check`: is this installation set up correctly?

Each check says what it found and, when something is wrong, what to change. Nothing
here costs money or posts anywhere unless asked: the LLM key is checked with the free
token-counting endpoint, and Slack / email only get a test message with --send-test.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import httpx

from airflow_doctor.collector import AirflowAPIError, AirflowClient
from airflow_doctor.config import Settings

Status = Literal["ok", "warn", "fail", "skip"]


@dataclass
class Result:
    name: str
    status: Status
    detail: str
    fix: str | None = None


def run_checks(
    settings: Settings,
    *,
    send_test: bool = False,
    transport: httpx.BaseTransport | None = None,
    provider_factory: Callable[[Settings], Any] | None = None,
    smtp_factory: Callable[..., Any] | None = None,
) -> Iterator[Result]:
    yield from _airflow(settings, transport)
    yield _storage(settings)
    yield _git(settings)
    yield _llm(settings, provider_factory)
    yield _slack(settings, send_test, transport)
    yield _email(settings, send_test, smtp_factory)
    yield _redaction()


# ---- Airflow ------------------------------------------------------------------------


def _airflow(settings: Settings, transport: httpx.BaseTransport | None) -> Iterator[Result]:
    url = settings.airflow_url
    with AirflowClient(settings, transport=transport) as client:
        try:
            api = client.api
            version = client.airflow_version()
        except AirflowAPIError as exc:
            yield Result("Airflow API", "fail", f"{url}: {exc}",
                         "Set AIRFLOW_DOCTOR_AIRFLOW_URL to the Airflow webserver / API server URL.")  # fmt: skip
            yield Result("Airflow login", "skip", "needs the Airflow API")
            yield Result("Finding failures", "skip", "needs the Airflow API")
            return
        yield Result("Airflow API", "ok", f"{url} · Airflow {version} · REST API {api}")

        try:
            client._get("/dags", params={"limit": 1})
            since = datetime.now(timezone.utc) - timedelta(minutes=5)
            client.failed_task_instances(since, max_pages=1)
        except AirflowAPIError as exc:
            who = settings.airflow_username or "(no user set)"
            fix = (
                "Set AIRFLOW_DOCTOR_AIRFLOW_USERNAME / _PASSWORD (or _TOKEN) to an Airflow "
                "user that can read DAGs, DAG runs, task instances and logs. The Viewer role "
                "is enough."
            )
            if api == "v1" and "403" in str(exc):
                fix += " On Airflow 2 also enable basic auth: AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth"
            yield Result("Airflow login", "fail", f"user {who}: {exc}", fix)
            yield Result("Finding failures", "skip", "needs a working Airflow login")
            return
        yield Result(
            "Airflow login",
            "ok",
            f"user {settings.airflow_username or '(token)'} can read DAGs and task instances",
        )

        yield _watch(settings, client)


def _watch(settings: Settings, client: AirflowClient) -> Result:
    plugin_installed: bool | None
    try:
        names = {p.get("name") for p in client._get("/plugins").get("plugins", [])}
        plugin_installed = "airflow_doctor" in names
    except AirflowAPIError:
        plugin_installed = None  # this user may not be allowed to list plugins
    polling = f"API polling every {settings.poll_interval_s:.0f}s"
    plugin = {
        True: "plugin installed",
        False: "plugin not installed",
        None: "plugin status unknown",
    }[plugin_installed]

    if settings.watch == "api":
        return Result("Finding failures", "ok", f"watch=api · {polling}")
    if settings.watch == "both":
        return Result("Finding failures", "ok", f"watch=both · {polling} · {plugin}")
    # watch == "plugin": the plugin is the only source.
    if plugin_installed is False:
        return Result("Finding failures", "fail", "watch=plugin but the airflow_doctor plugin is not installed in Airflow",
                      "pip install airflow-doctor into the Airflow image, or set AIRFLOW_DOCTOR_WATCH=api.")  # fmt: skip
    if not settings.spool_dir.exists():
        return Result("Finding failures", "warn", f"watch=plugin · {plugin} · no spool folder yet at {settings.spool_dir}",
                      "The Airflow containers and the worker must share this folder (AIRFLOW_DOCTOR_HOME). "
                      "Or set AIRFLOW_DOCTOR_WATCH=api, which needs no shared folder.")  # fmt: skip
    return Result("Finding failures", "ok", f"watch=plugin · {plugin} · spool {settings.spool_dir}")


# ---- local ----------------------------------------------------------------------------


def _storage(settings: Settings) -> Result:
    home = settings.home
    try:
        home.mkdir(parents=True, exist_ok=True)
        probe = home / ".write-check"
        probe.write_text("ok")
        probe.unlink()
    except OSError as exc:
        return Result(
            "Storage",
            "fail",
            f"{home}: {exc}",
            "Set AIRFLOW_DOCTOR_HOME to a folder the worker can write.",
        )
    return Result("Storage", "ok", f"{home} is writable")


def _git(settings: Settings) -> Result:
    repo = settings.git_repo_path
    if repo is None:
        return Result(
            "Git history", "skip", "not configured (optional: AIRFLOW_DOCTOR_GIT_REPO_PATH)"
        )
    result = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--is-inside-work-tree"],
        capture_output=True, text=True, check=False,
    )  # fmt: skip
    if result.returncode != 0:
        return Result("Git history", "fail", f"{repo} is not a git repository",
                      "Point AIRFLOW_DOCTOR_GIT_REPO_PATH at a clone of the repo that holds your DAGs.")  # fmt: skip
    subdir = repo / settings.git_dags_subdir if settings.git_dags_subdir else repo
    if not subdir.is_dir():
        return Result("Git history", "fail", f"{subdir} does not exist",
                      "Set AIRFLOW_DOCTOR_GIT_DAGS_SUBDIR to the DAGs folder inside the repo.")  # fmt: skip
    return Result("Git history", "ok", f"{subdir}")


def _llm(settings: Settings, provider_factory: Callable[[Settings], Any] | None) -> Result:
    from airflow_doctor.agent import ProviderConfigError, ProviderError, make_provider

    if not settings.diagnose:
        return Result(
            "LLM",
            "skip",
            "diagnosis is off (AIRFLOW_DOCTOR_DIAGNOSE=false): cards say 'Not diagnosed'",
        )
    try:
        provider = (provider_factory or make_provider)(settings)
        provider.verify()
    except ProviderConfigError as exc:
        return Result("LLM", "fail", str(exc),
                      "Set AIRFLOW_DOCTOR_LLM_API_KEY (e.g. in .env), or AIRFLOW_DOCTOR_DIAGNOSE=false "
                      "to collect and notify without a diagnosis.")  # fmt: skip
    except ProviderError as exc:
        return Result(
            "LLM", "warn", str(exc), "The model API is unreachable right now; try again later."
        )
    return Result(
        "LLM", "ok", f"{provider.name} · {provider.model} · key accepted (no tokens spent)"
    )


def _slack(settings: Settings, send_test: bool, transport: httpx.BaseTransport | None) -> Result:
    from airflow_doctor.notifier.slack import NotifyError, send_slack

    if not settings.slack_webhook_url:
        return Result(
            "Slack", "skip", "not configured (optional: AIRFLOW_DOCTOR_SLACK_WEBHOOK_URL)"
        )
    url = settings.slack_webhook_url.get_secret_value()
    if not url.startswith("https://hooks.slack.com/") and not url.startswith("http://127.0.0.1"):
        return Result("Slack", "warn", "the webhook URL does not look like https://hooks.slack.com/services/...",
                      "Copy the Incoming Webhook URL from your Slack app settings.")  # fmt: skip
    if not send_test:
        return Result("Slack", "ok", "configured · run with --send-test to post a test message")
    try:
        send_slack(url, {"text": ":white_check_mark: Airflow Doctor test message: Slack is set up."},
                   transport=transport, sleep=lambda s: None)  # fmt: skip
    except NotifyError as exc:
        return Result(
            "Slack",
            "fail",
            str(exc),
            "Check the webhook URL and that the Slack app is still installed.",
        )
    return Result("Slack", "ok", "test message sent")


def _email(settings: Settings, send_test: bool, smtp_factory: Callable[..., Any] | None) -> Result:
    from email.message import EmailMessage

    from airflow_doctor.notifier.mail import send_email
    from airflow_doctor.notifier.slack import NotifyError

    if not settings.email_enabled:
        partial = settings.smtp_host or settings.email_from or settings.email_to
        if partial:
            return Result("Email", "fail", "partly configured",
                          "Set all of AIRFLOW_DOCTOR_SMTP_HOST, AIRFLOW_DOCTOR_EMAIL_FROM and "
                          "AIRFLOW_DOCTOR_EMAIL_TO (JSON list).")  # fmt: skip
        return Result(
            "Email",
            "skip",
            "not configured (optional: AIRFLOW_DOCTOR_SMTP_HOST, _EMAIL_FROM, _EMAIL_TO)",
        )
    if not send_test:
        return Result(
            "Email",
            "ok",
            f"{settings.smtp_host}:{settings.smtp_port} · run with --send-test to send a test email",
        )
    msg = EmailMessage()
    msg["Subject"] = "[Airflow Doctor] test message"
    msg["From"] = settings.email_from
    msg["To"] = ", ".join(settings.email_to)
    msg.set_content("Airflow Doctor can send email. This is a test message.\n")
    try:
        send_email(msg, settings, smtp_factory=smtp_factory)
    except NotifyError as exc:
        return Result("Email", "fail", str(exc), "Check AIRFLOW_DOCTOR_SMTP_* settings.")
    return Result("Email", "ok", f"test email sent to {', '.join(settings.email_to)}")


def _redaction() -> Result:
    from airflow_doctor.models import FailureContext, FailureJob, LogLine
    from airflow_doctor.redactor import Redactor

    secret = "Ch3ck-" + os.urandom(6).hex()
    ctx = FailureContext(
        job=FailureJob(dag_id="check", run_id="check", task_id="check"),
        log_tail=[LogLine(n=1, message=f"postgresql://u:{secret}@db/x password={secret}")],
        collected_at=datetime.now(timezone.utc),
    )
    redacted = Redactor().redact(ctx)
    if secret in redacted.model_dump_json():
        return Result(
            "Redaction",
            "fail",
            "a planted test secret was not removed",
            "Please report this as a bug.",
        )
    return Result("Redaction", "ok", "self-test passed")
