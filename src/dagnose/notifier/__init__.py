"""Slack and email diagnosis cards.

A failed delivery never re-runs the pipeline (that would spend model tokens again):
the rendered message is saved to the outbox and `dagnose outbox --resend`
sends it later.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import httpx

from dagnose.config import Settings
from dagnose.models import DiagnosisRun, RedactedContext
from dagnose.notifier.card import Card, build_card
from dagnose.notifier.mail import build_email, email_html, email_text, send_email
from dagnose.notifier.slack import NotifyError, send_slack, slack_payload
from dagnose.redactor import RedactionSession

__all__ = ["Card", "NotifyError", "NotifyReport", "build_card", "notify", "resend_outbox"]

log = logging.getLogger(__name__)


@dataclass
class NotifyReport:
    sent: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)  # channel -> error
    outboxed: list[Path] = field(default_factory=list)


def channels_configured(settings: Settings) -> bool:
    return bool(settings.slack_webhook_url) or settings.email_enabled


def notify(
    ctx: RedactedContext,
    run: DiagnosisRun | None,
    settings: Settings,
    *,
    session: RedactionSession | None = None,
    reason: str | None = None,
    name: str = "notification",
    transport: httpx.BaseTransport | None = None,
    smtp_factory: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> NotifyReport:
    """Send the card to every configured channel. Never raises for delivery problems."""
    report = NotifyReport()
    card = build_card(
        ctx,
        run,
        ui_url=settings.airflow_ui_url or settings.airflow_url,
        reason=reason,
        session=session,
    )
    if settings.slack_webhook_url:
        payload = slack_payload(card)
        try:
            send_slack(
                settings.slack_webhook_url.get_secret_value(),
                payload,
                transport=transport,
                sleep=sleep,
            )
            report.sent.append("slack")
        except NotifyError as exc:
            report.failed["slack"] = str(exc)
            report.outboxed.append(
                _to_outbox(settings, name, "slack", json.dumps(payload), str(exc))
            )
    if settings.email_enabled:
        assert settings.email_from is not None
        msg = build_email(card, settings.email_from, settings.email_to)
        try:
            send_email(msg, settings, smtp_factory=smtp_factory)
            report.sent.append("email")
        except NotifyError as exc:
            report.failed["email"] = str(exc)
            body = json.dumps(
                {
                    "subject": str(msg["Subject"]),
                    "text": email_text(card),
                    "html": email_html(card),
                }
            )
            report.outboxed.append(_to_outbox(settings, name, "email", body, str(exc)))
    for channel, error in report.failed.items():
        log.warning("%s: %s notification failed, saved to outbox: %s", name, channel, error)
    return report


def _to_outbox(settings: Settings, name: str, channel: str, body: str, error: str) -> Path:
    settings.outbox_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(name).stem
    path = settings.outbox_dir / f"{stem}.{channel}.json"
    path.write_text(
        json.dumps({"channel": channel, "body": body, "error": error, "saved_at": time.time()})
    )
    return path


def resend_outbox(
    settings: Settings,
    *,
    transport: httpx.BaseTransport | None = None,
    smtp_factory: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> NotifyReport:
    """Try every saved notification again; delete the ones that go through."""
    report = NotifyReport()
    if not settings.outbox_dir.is_dir():
        return report
    for path in sorted(settings.outbox_dir.glob("*.json")):
        item = json.loads(path.read_text())
        try:
            if item["channel"] == "slack":
                if not settings.slack_webhook_url:
                    raise NotifyError("Slack is not configured")
                send_slack(
                    settings.slack_webhook_url.get_secret_value(),
                    json.loads(item["body"]),
                    transport=transport,
                    sleep=sleep,
                )
            elif item["channel"] == "email":
                if not settings.email_enabled:
                    raise NotifyError("email is not configured")
                assert settings.email_from is not None
                parts = json.loads(item["body"])
                msg = EmailMessage()
                msg["Subject"] = parts["subject"]
                msg["From"] = settings.email_from
                msg["To"] = ", ".join(settings.email_to)
                msg.set_content(parts["text"])
                msg.add_alternative(parts["html"], subtype="html")
                send_email(msg, settings, smtp_factory=smtp_factory)
            else:
                raise NotifyError(f"unknown channel {item['channel']!r}")
        except NotifyError as exc:
            report.failed[path.name] = str(exc)
            continue
        path.unlink()
        report.sent.append(path.name)
    return report
