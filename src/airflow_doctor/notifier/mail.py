"""Email: render a Card as plain text + HTML and send it over SMTP."""

from __future__ import annotations

import html
import smtplib
import ssl
from collections.abc import Callable
from email.message import EmailMessage
from typing import Any

from airflow_doctor.notifier.card import Card
from airflow_doctor.notifier.slack import NotifyError


def email_subject(card: Card) -> str:
    return f"[Airflow Doctor] {card.title}: {card.headline}"[:200]


def email_text(card: Card) -> str:
    lines = [card.title, card.headline, "", card.summary]
    if card.root_cause:
        lines += ["", "Root cause:", card.root_cause]
    if card.evidence:
        lines += ["", "Evidence:" if card.diagnosed else "Failed at:"]
        lines += [f"  [{'ok' if e.verified else '??'}] {e.where}: {e.quote}" for e in card.evidence]
    if card.fix:
        lines += ["", "Suggested fix:", card.fix]
        if card.has_patch:
            lines.append("(A patch is in the diagnosis file.)")
    if card.note:
        lines += ["", card.note]
    lines += ["", " | ".join(f"{k}: {v}" for k, v in card.facts)]
    if card.airflow_url:
        lines += ["", f"Open in Airflow: {card.airflow_url}"]
    return "\n".join(lines) + "\n"


def email_html(card: Card) -> str:
    e = html.escape
    parts = [
        f"<h2 style='margin:0'>{e(card.title)}</h2>",
        f"<p><strong>{e(card.headline)}</strong><br>{e(card.summary)}</p>",
    ]
    if card.root_cause:
        parts.append(f"<h3>Root cause</h3><p>{e(card.root_cause)}</p>")
    if card.evidence:
        rows = "".join(
            f"<li>{'&#10003;' if ev.verified else '?'} "
            f"<code>{e(ev.where)}</code> {e(ev.quote)}</li>"
            for ev in card.evidence
        )
        label = "Evidence" if card.diagnosed else "Failed at"
        parts.append(f"<h3>{label}</h3><ul>{rows}</ul>")
    if card.fix:
        patch = "<p><em>A patch is in the diagnosis file.</em></p>" if card.has_patch else ""
        parts.append(f"<h3>Suggested fix</h3><p>{e(card.fix)}</p>{patch}")
    if card.note:
        parts.append(f"<p style='color:#666'>{e(card.note)}</p>")
    parts.append(
        "<p style='color:#666'>"
        + " &middot; ".join(f"{e(k)}: <code>{e(v)}</code>" for k, v in card.facts)
        + "</p>"
    )
    if card.airflow_url:
        parts.append(f"<p><a href='{e(card.airflow_url, quote=True)}'>Open in Airflow</a></p>")
    return "<html><body style='font-family:sans-serif'>" + "".join(parts) + "</body></html>"


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
