"""Slack: render a Card as Block Kit and post it to an incoming webhook.

Buttons here are links only (open in Airflow). Feedback buttons (correct / wrong /
retry) need a Slack app with an interactivity endpoint; they come with incident
memory, where the feedback is stored.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

from dagnose.notifier.card import Card

SECTION_LIMIT = 3000  # Slack's limit for a section's text
HEADER_LIMIT = 150
MAX_ATTEMPTS = 3
MAX_RETRY_AFTER_S = 30.0


class NotifyError(RuntimeError):
    pass


def _esc(text: str) -> str:
    """Escape for Slack mrkdwn, and keep backticks from breaking inline code."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("`", "'")


def _section(text: str) -> dict[str, Any]:
    if len(text) > SECTION_LIMIT:
        text = text[: SECTION_LIMIT - 3] + "..."
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict[str, Any]:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def evidence_title(card: Card) -> str:
    if not card.diagnosed:
        return "Failed at"
    if card.evidence_found:
        found, total = card.evidence_found
        return f"Evidence ({found} of {total} quotes found in the logs)"
    return "Evidence"


def slack_payload(card: Card) -> dict[str, Any]:
    """Layout: what happened, what to do, why, the evidence, then the details. No emoji:
    the category and confidence are words, and an unchecked quote says so."""
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": card.title[:HEADER_LIMIT]}},
        _section(f"*{_esc(card.headline)}*\n{_esc(card.summary)}"),
    ]
    if card.fix:
        extra = "\n_A ready-made patch is in the diagnosis file._" if card.has_patch else ""
        blocks.append(_section(f"*What to do*\n{_esc(card.fix)}{extra}"))
    if card.root_cause:
        blocks.append(_section(f"*Why it failed*\n{_esc(card.root_cause)}"))
    if card.evidence:
        rows = [
            f"{_esc(e.where)}: `{_esc(e.quote)}`"
            + ("" if e.verified else "  _(not found in the logs)_")
            for e in card.evidence
        ]
        blocks.append(_section(f"*{evidence_title(card)}*\n" + "\n".join(rows)))
    if card.note:
        blocks.append(_context(_esc(card.note)))
    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{_esc(k)}*\n{_esc(v)}"} for k, v in card.facts[:10]
            ],
        }
    )
    if card.airflow_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Open in Airflow"},
                        "url": card.airflow_url,
                        "style": "primary",
                    }
                ],
            }
        )
    if card.footer:
        blocks.append(_context(_esc(card.footer)))
    return {"text": f"{card.title}: {card.summary}"[:300], "blocks": blocks}


def send_webhook(
    service: str,
    webhook_url: str,
    payload: dict[str, Any],
    *,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """POST a JSON message to a chat webhook (Slack, Teams workflow, Google Chat).
    Retries rate limits (honouring Retry-After), 5xx and connection errors; a 4xx means
    the payload or webhook is wrong, so no retry. 200 and 202 (Teams) mean delivered."""
    last = ""
    with httpx.Client(timeout=10, transport=transport) as http:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = http.post(webhook_url, json=payload)
            except httpx.HTTPError as exc:
                last = f"connection error: {type(exc).__name__}"
                wait = float(2**attempt)
            else:
                if resp.status_code in (200, 201, 202, 204):
                    return
                last = f"HTTP {resp.status_code}: {resp.text[:200]}"
                if resp.status_code == 429:
                    wait = min(
                        float(resp.headers.get("retry-after", 2**attempt)), MAX_RETRY_AFTER_S
                    )
                elif resp.status_code >= 500:
                    wait = float(2**attempt)
                else:
                    raise NotifyError(f"{service} rejected the message: {last}")
            if attempt < MAX_ATTEMPTS:
                sleep(wait)
    raise NotifyError(f"{service} unreachable after {MAX_ATTEMPTS} attempts: {last}")


def send_slack(
    webhook_url: str,
    payload: dict[str, Any],
    *,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    send_webhook("Slack", webhook_url, payload, transport=transport, sleep=sleep)
