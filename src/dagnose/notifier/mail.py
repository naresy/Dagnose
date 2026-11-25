"""Email: render a Card as plain text + HTML and send it over SMTP."""

from __future__ import annotations

import html
import smtplib
import ssl
from collections.abc import Callable
from email.message import EmailMessage
from typing import Any

from dagnose.notifier.card import Card
from dagnose.notifier.slack import NotifyError, evidence_title


def email_subject(card: Card) -> str:
    return f"[Dagnose] {card.title}: {card.headline}"[:200]


def email_text(card: Card) -> str:
    lines = [card.title, card.headline, "", card.summary]
    if card.fix:
        lines += ["", "What to do:", card.fix]
        if card.has_patch:
            lines.append("(A ready-made patch is in the diagnosis file.)")
    if card.root_cause:
        lines += ["", "Why it failed:", card.root_cause]
    if card.evidence:
        lines += ["", evidence_title(card) + ":"]
        for ev in card.evidence:
            mark = "" if ev.verified else " (not found in the logs)"
            lines += [f"  {ev.where}{mark}:", f"    {ev.quote}"]
    if card.note:
        lines += ["", card.note]
    lines += ["", " | ".join(f"{k}: {v}" for k, v in card.facts)]
    if card.airflow_url:
        lines += ["", f"Open in Airflow: {card.airflow_url}"]
    if card.footer:
        lines += ["", card.footer]
    return "\n".join(lines) + "\n"


MUTED = "color:#5f6368;font-size:13px"
BODY = "font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#202124"
BUTTON = "background:#2f6fed;color:#fff;padding:8px 14px;border-radius:4px;text-decoration:none"


def email_html(card: Card) -> str:
    e = html.escape

    def block(title: str, body: str) -> str:
        return f"<h3 style='margin:16px 0 4px'>{e(title)}</h3>{body}"

    parts = [
        f"<h2 style='margin:0 0 4px'>{e(card.title)}</h2>",
        f"<p style='margin:0'><strong>{e(card.headline)}</strong><br>{e(card.summary)}</p>",
    ]
    if card.fix:
        patch = (
            f"<p style='{MUTED}'>A ready-made patch is in the diagnosis file.</p>"
            if card.has_patch
            else ""
        )
        parts.append(block("What to do", f"<p style='margin:0'>{e(card.fix)}</p>{patch}"))
    if card.root_cause:
        parts.append(block("Why it failed", f"<p style='margin:0'>{e(card.root_cause)}</p>"))
    if card.evidence:
        rows = "".join(
            f"<tr><td style='{MUTED};padding:4px 12px 4px 0;vertical-align:top'>{e(ev.where)}"
            f"{'' if ev.verified else '<br><em>not found in the logs</em>'}</td>"
            f"<td style='padding:4px 0'><code>{e(ev.quote)}</code></td></tr>"
            for ev in card.evidence
        )
        parts.append(block(evidence_title(card), f"<table>{rows}</table>"))
    if card.note:
        parts.append(f"<p style='{MUTED}'>{e(card.note)}</p>")
    facts = "".join(
        f"<td style='padding:0 16px 0 0'><span style='{MUTED}'>{e(k)}</span><br>{e(v)}</td>"
        for k, v in card.facts
    )
    parts.append("<hr style='border:0;border-top:1px solid #ddd;margin:16px 0'>")
    parts.append(f"<table><tr>{facts}</tr></table>")
    if card.airflow_url:
        url = e(card.airflow_url, quote=True)
        parts.append(
            f"<p style='margin:16px 0'><a href='{url}' style='{BUTTON}'>Open in Airflow</a></p>"
        )
    if card.footer:
        parts.append(f"<p style='{MUTED}'>{e(card.footer)}</p>")
    return f"<html><body style='{BODY}'>" + "".join(parts) + "</body></html>"


def build_email(card: Card, sender: str, recipients: list[str]) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = email_subject(card)
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(email_text(card))
    msg.add_alternative(email_html(card), subtype="html")
    return msg


def send_email(
    msg: EmailMessage,
    settings: Any,
    *,
    smtp_factory: Callable[..., Any] | None = None,
) -> None:
    host, port, security = settings.smtp_host, settings.smtp_port, settings.smtp_security
    password = settings.smtp_password.get_secret_value() if settings.smtp_password else ""
    context = ssl.create_default_context()
    try:
        if smtp_factory is not None:
            server = smtp_factory(host, port)
        elif security == "ssl":
            server = smtplib.SMTP_SSL(host, port, timeout=15, context=context)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
        with server:
            if security == "starttls":
                server.starttls(context=context)
            if settings.smtp_username:
                server.login(settings.smtp_username, password)
            server.send_message(msg)
    except (smtplib.SMTPException, OSError) as exc:
        raise NotifyError(f"email not sent: {type(exc).__name__}: {exc}") from exc
