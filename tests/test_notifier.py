"""Notifier: cards, Slack and email rendering, delivery, retries and the outbox.
No real Slack or SMTP server is contacted."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import httpx
import pytest

from dagnose.models import (
    Diagnosis,
    DiagnosisRun,
    Evidence,
    FailureContext,
    RedactedContext,
)
from dagnose.notifier import build_card, notify, resend_outbox
from dagnose.notifier.card import airflow_link
from dagnose.notifier.mail import build_email, email_html, email_text
from dagnose.notifier.slack import NotifyError, send_slack, slack_payload
from dagnose.redactor import Redactor
from tests.conftest import FIXTURES

LEAKY = FIXTURES / "contexts" / "v2" / "demo_crm_session_sync.json"
KEYERROR_V1 = FIXTURES / "contexts" / "v1" / "demo_orders_tax.json"
KEYERROR_V2 = FIXTURES / "contexts" / "v2" / "demo_orders_tax.json"
SECRETS = [
    "Sup3r-S3cret-Pw",
    "data-oncall@example.com",
    "ZmFrZS1zaWduYXR1cmUtZm9yLXRlc3Rz",
    "ck_7Hq2LmX9pR4vT8wZ3nB6yK1d",
]
WEBHOOK = "https://hooks.example.test/services/T/B/x"


def load(path: Path) -> RedactedContext:
    return Redactor().redact(FailureContext.model_validate_json(path.read_text()))


def diagnosis_run(ctx: RedactedContext, **overrides: Any) -> DiagnosisRun:
    d = {
        "summary": "compute_tax reads a 'country' key the payload lacks.",
        "category": "code_bug",
        "root_cause": "extract_orders stopped returning 'country'.",
        "evidence": [
            Evidence(
                source="dag_source", line=26, quote='TAX_RATES[order["country"]]', verified=True
            ),
            Evidence(source="log", line=3, quote="disk full", verified=False),
        ],
        "confidence": 0.6,
        "suggested_fix": "Return 'country' from extract_orders.",
        "patch": "--- a\n+++ b\n",
    }
    d.update(overrides.pop("diagnosis", {}))
    fields: dict[str, Any] = {
        "job": ctx.job,
        "diagnosis": Diagnosis(**d),
        "provider": "anthropic",
        "model": "claude-opus-5-5",
        "turns": 1,
        "duration_s": 3.2,
        "evidence_verified": 1,
        "evidence_total": 2,
        "confidence_capped": True,
        "model_confidence": 0.95,
        "created_at": datetime(2026, 11, 23, tzinfo=timezone.utc),
    }
    fields.update(overrides)
    return DiagnosisRun(**fields)


# ---- card ---------------------------------------------------------------------------


def test_card_with_diagnosis() -> None:
    ctx = load(KEYERROR_V2)
    card = build_card(ctx, diagnosis_run(ctx), ui_url="http://airflow.test")
    assert card.title == "demo_orders_tax.compute_tax failed"
    assert card.headline == "Code bug · confidence 0.60"
    assert card.diagnosed and card.has_patch
    assert [(e.verified, e.where) for e in card.evidence] == [
        (True, "dag_source:26"),
        (False, "log:3"),
    ]
    assert card.note and "lowered from 0.95" in card.note and "1 of 2" in card.note
    assert ("Try", "1") in card.facts


def test_card_without_diagnosis_shows_the_error_and_why() -> None:
    ctx = load(KEYERROR_V2)
    card = build_card(ctx, None, ui_url=None, reason="no Anthropic credentials")
    assert card.headline == "Not diagnosed" and not card.diagnosed
    assert card.summary == "KeyError: 'country'"
    assert card.note == "No diagnosis: no Anthropic credentials"
    assert [(e.where, e.quote) for e in card.evidence] == [("orders_tax.py:26", "in compute_tax")]
    assert card.airflow_url is None


def test_card_redacts_again_before_sending() -> None:
    """Even if a secret slipped into a diagnosis, it must not reach Slack or email."""
    ctx = load(LEAKY)
    leaky = {
        "summary": "Password Sup3r-S3cret-Pw rejected for postgresql://u:Sup3r-S3cret-Pw@db/x",
        "suggested_fix": "mail data-oncall@example.com",
    }
    card = build_card(ctx, diagnosis_run(ctx, diagnosis=leaky), ui_url="http://airflow.test")
    rendered = json.dumps(slack_payload(card)) + email_text(card) + email_html(card)
    for secret in SECRETS:
        assert secret not in rendered


def test_long_text_is_cut() -> None:
    ctx = load(KEYERROR_V2)
    card = build_card(ctx, diagnosis_run(ctx, diagnosis={"root_cause": "x" * 5000}), ui_url=None)
    assert (
        card.root_cause is not None
        and len(card.root_cause) <= 1200
        and card.root_cause.endswith("…")
    )


@pytest.mark.parametrize(
    ("fixture", "expected"),
    [
        (
            KEYERROR_V2,
            "http://ui.test/dags/demo_orders_tax/runs/{run}/tasks/compute_tax?try_number=1",
        ),
        (
            KEYERROR_V1,
            "http://ui.test/dags/demo_orders_tax/grid?dag_run_id={raw}&task_id=compute_tax&tab=logs",
        ),
    ],
)
def test_airflow_links(fixture: Path, expected: str) -> None:
    from urllib.parse import quote, quote_plus

    ctx = load(fixture)
    run_id = ctx.job.run_id
    assert airflow_link(ctx, "http://ui.test/") == expected.format(
        run=quote(run_id, safe=""), raw=quote_plus(run_id)
    )


# ---- slack --------------------------------------------------------------------------


def test_slack_payload() -> None:
    ctx = load(KEYERROR_V2)
    payload = slack_payload(build_card(ctx, diagnosis_run(ctx), ui_url="http://ui.test"))
    types = [b["type"] for b in payload["blocks"]]
    assert types == [
        "header",
        "section",
        "section",
        "section",
        "section",
        "context",
        "context",
        "actions",
    ]
    text = json.dumps(payload)
    assert ":white_check_mark: `dag_source:26`" in text and ":grey_question: `log:3`" in text
    assert "A patch is in the diagnosis file" in text
    assert payload["blocks"][-1]["elements"][0]["url"].startswith("http://ui.test/dags/")
    assert payload["text"].startswith("demo_orders_tax.compute_tax failed: ")


def test_slack_escapes_markup() -> None:
    ctx = load(KEYERROR_V2)
    weird = {"summary": "<!channel> & `rm -rf` <http://evil|click>"}
    payload = slack_payload(build_card(ctx, diagnosis_run(ctx, diagnosis=weird), ui_url=None))
    section = payload["blocks"][1]["text"]["text"]
    assert "&lt;!channel&gt; &amp; 'rm -rf' &lt;http://evil|click&gt;" in section


def test_slack_section_limit() -> None:
    ctx = load(KEYERROR_V2)
    card = build_card(ctx, diagnosis_run(ctx), ui_url=None)
    card.root_cause = "y" * 5000  # bypass the card limit to test the Slack one
    blocks = slack_payload(card)["blocks"]
    assert all(len(b["text"]["text"]) <= 3000 for b in blocks if b["type"] == "section")


class Hook:
    def __init__(self, *statuses: int, retry_after: str | None = None) -> None:
        self.statuses = list(statuses)
        self.retry_after = retry_after
        self.bodies: list[dict[str, Any]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(json.loads(request.content))
        status = self.statuses.pop(0)
        headers = {"retry-after": self.retry_after} if status == 429 and self.retry_after else {}
        return httpx.Response(status, text="ok" if status == 200 else "error", headers=headers)


def test_slack_retries_rate_limits_and_server_errors() -> None:
    hook, waits = Hook(429, 503, 200, retry_after="7"), []
    send_slack(WEBHOOK, {"text": "x"}, transport=httpx.MockTransport(hook), sleep=waits.append)
    assert len(hook.bodies) == 3 and waits == [7.0, 4.0]


def test_slack_gives_up_after_three_attempts() -> None:
    hook = Hook(500, 500, 500)
    with pytest.raises(NotifyError, match="after 3 attempts"):
        send_slack(
            WEBHOOK, {"text": "x"}, transport=httpx.MockTransport(hook), sleep=lambda s: None
        )


def test_slack_does_not_retry_bad_requests() -> None:
    hook = Hook(400)
    with pytest.raises(NotifyError, match="rejected"):
        send_slack(
            WEBHOOK, {"text": "x"}, transport=httpx.MockTransport(hook), sleep=lambda s: None
        )
    assert len(hook.bodies) == 1


# ---- email --------------------------------------------------------------------------


class FakeSMTP:
    sent: list[EmailMessage] = []
    calls: list[str] = []
    fail = False

    def __init__(self, host: str, port: int) -> None:
        self.calls.append(f"connect {host}:{port}")

    def __enter__(self) -> FakeSMTP:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def starttls(self, context: Any) -> None:
        self.calls.append("starttls")

    def login(self, user: str, password: str) -> None:
        self.calls.append(f"login {user}")

    def send_message(self, msg: EmailMessage) -> None:
        if FakeSMTP.fail:
            raise ConnectionRefusedError("down")
        self.sent.append(msg)


@pytest.fixture
def smtp() -> type[FakeSMTP]:
    FakeSMTP.sent, FakeSMTP.calls, FakeSMTP.fail = [], [], False
    return FakeSMTP


def mail_settings(make_settings: Any, **extra: Any) -> Any:
    return make_settings(
        smtp_host="smtp.test",
        smtp_username="dagnose",
        smtp_password="pw",
        email_from="dagnose@example.test",
        email_to=["oncall@example.test", "data@example.test"],
        **extra,
    )


def test_email(make_settings: Any, smtp: type[FakeSMTP]) -> None:
    ctx = load(KEYERROR_V2)
    report = notify(ctx, diagnosis_run(ctx), mail_settings(make_settings), smtp_factory=smtp)
    assert report.sent == ["email"]
    assert smtp.calls == ["connect smtp.test:587", "starttls", "login dagnose"]
    [msg] = smtp.sent
    assert (
        msg["Subject"] == "[Dagnose] demo_orders_tax.compute_tax failed: Code bug · confidence 0.60"
    )
    assert msg["To"] == "oncall@example.test, data@example.test"
    assert msg.get_body(("plain",)).get_content().startswith("demo_orders_tax.compute_tax failed")
    assert "<h3>Root cause</h3>" in msg.get_body(("html",)).get_content()


def test_email_html_is_escaped() -> None:
    ctx = load(KEYERROR_V2)
    card = build_card(
        ctx, diagnosis_run(ctx, diagnosis={"summary": "<script>x</script>"}), ui_url=None
    )
    msg = build_email(card, "a@example.test", ["b@example.test"])
    assert "<script>" not in msg.get_body(("html",)).get_content()


# ---- notify + outbox ----------------------------------------------------------------


def test_notify_nothing_configured(make_settings: Any) -> None:
    ctx = load(KEYERROR_V2)
    assert notify(ctx, None, make_settings()).sent == []


def test_failed_delivery_goes_to_outbox_and_resends(
    make_settings: Any, smtp: type[FakeSMTP]
) -> None:
    ctx = load(LEAKY)
    settings = mail_settings(make_settings, slack_webhook_url=WEBHOOK)
    smtp.fail = True
    report = notify(
        ctx, None, settings, name="job.json", reason="off",
        transport=httpx.MockTransport(Hook(500, 500, 500)), smtp_factory=smtp, sleep=lambda s: None,
    )  # fmt: skip
    assert report.sent == [] and set(report.failed) == {"slack", "email"}
    assert sorted(p.name for p in settings.outbox_dir.iterdir()) == [
        "job.email.json",
        "job.slack.json",
    ]
    for path in settings.outbox_dir.iterdir():
        for secret in SECRETS:
            assert secret not in path.read_text()

    smtp.fail = False
    hook = Hook(200)
    again = resend_outbox(
        settings, transport=httpx.MockTransport(hook), smtp_factory=smtp, sleep=lambda s: None
    )
    assert sorted(again.sent) == ["job.email.json", "job.slack.json"]
    assert list(settings.outbox_dir.iterdir()) == []
    assert hook.bodies[0]["blocks"][0]["text"]["text"] == "demo_crm_session_sync.sync_crm failed"
    assert smtp.sent[0]["Subject"].startswith("[Dagnose] demo_crm_session_sync.sync_crm")


def test_card_without_exception_shows_the_last_error_line() -> None:
    """A task killed by a signal (out of memory) has no exception; Airflow's last
    critical line explains it."""
    from dagnose.models import LogLine

    ctx = load(KEYERROR_V2)
    ctx.exceptions = []
    ctx.log_tail = [
        LogLine(
            n=4, level="info", logger="task.stdout", message="loading 40M rows into memory ..."
        ),
        LogLine(
            n=5,
            level="critical",
            logger="task",
            message="Process terminated by signal. Likely out of memory error (OOM).",
        ),
    ]
    card = build_card(ctx, None, ui_url=None, reason="off")
    assert card.summary == (
        "The task failed without an exception. Last error: "
        "Process terminated by signal. Likely out of memory error (OOM)."
    )
