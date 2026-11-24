"""Microsoft Teams (Adaptive Card through a Teams workflow) and Google Chat (cardsV2).

Both are rendered from the same Card as Slack and email, so they carry the same
redacted text. Teams: create the webhook with the Workflows app ("Post to a channel when
a webhook request is received"); the old Office 365 connectors are retired. Google Chat:
Apps & integrations -> Webhooks in the space.
"""

from __future__ import annotations

import html
from typing import Any

from dagnose.notifier.card import Card

# ---- Microsoft Teams ------------------------------------------------------------------


def teams_payload(card: Card) -> dict[str, Any]:
    def text(value: str, **extra: Any) -> dict[str, Any]:
        return {"type": "TextBlock", "text": value, "wrap": True, **extra}

    body: list[dict[str, Any]] = [
        text(card.title, size="Large", weight="Bolder"),
        text(card.headline, weight="Bolder", color="Attention" if card.diagnosed else "Warning"),
        text(card.summary),
    ]
    if card.root_cause:
        body += [text("Root cause", weight="Bolder", spacing="Medium"), text(card.root_cause)]
    if card.evidence:
        body.append(
            text("Evidence" if card.diagnosed else "Failed at", weight="Bolder", spacing="Medium")
        )
        body += [
            text(
                f"{'✅' if e.verified else '❔'} `{e.where}` {e.quote}",
                fontType="Monospace",
                size="Small",
            )
            for e in card.evidence
        ]
    if card.fix:
        body += [text("Suggested fix", weight="Bolder", spacing="Medium"), text(card.fix)]
        if card.has_patch:
            body.append(text("A patch is in the diagnosis file.", isSubtle=True, size="Small"))
    if card.note:
        body.append(text(card.note, isSubtle=True, size="Small"))
    body.append({"type": "FactSet", "facts": [{"title": k, "value": v} for k, v in card.facts]})
    content: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "msteams": {"width": "Full"},
        "body": body,
    }
    if card.airflow_url:
        content["actions"] = [
            {"type": "Action.OpenUrl", "title": "Open in Airflow", "url": card.airflow_url}
        ]
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": content,
            }
        ],
    }


# ---- Google Chat ----------------------------------------------------------------------


def google_chat_payload(card: Card) -> dict[str, Any]:
    e = html.escape  # Google Chat text widgets interpret a few HTML tags

    def para(value: str) -> dict[str, Any]:
        return {"textParagraph": {"text": value}}

    sections: list[dict[str, Any]] = [
        {"widgets": [para(f"<b>{e(card.headline)}</b><br>{e(card.summary)}")]}
    ]
    if card.root_cause:
        sections.append({"header": "Root cause", "widgets": [para(e(card.root_cause))]})
    if card.evidence:
        rows = "<br>".join(
            f"{'✅' if ev.verified else '❔'} "
            f'<font color="#5b8cff">{e(ev.where)}</font> {e(ev.quote)}'
            for ev in card.evidence
        )
        sections.append(
            {"header": "Evidence" if card.diagnosed else "Failed at", "widgets": [para(rows)]}
        )
    if card.fix:
        patch = "<br><i>A patch is in the diagnosis file.</i>" if card.has_patch else ""
        sections.append({"header": "Suggested fix", "widgets": [para(e(card.fix) + patch)]})
    footer: list[dict[str, Any]] = []
    if card.note:
        footer.append(para(f"<i>{e(card.note)}</i>"))
    footer.append(para(" · ".join(f"{e(k)}: {e(v)}" for k, v in card.facts)))
    if card.airflow_url:
        footer.append(
            {
                "buttonList": {
                    "buttons": [
                        {
                            "text": "Open in Airflow",
                            "onClick": {"openLink": {"url": card.airflow_url}},
                        }
                    ]
                }
            }
        )
    sections.append({"widgets": footer})
    return {
        "text": f"{card.title}: {card.summary}"[:300],
        "cardsV2": [
            {
                "cardId": "dagnose",
                "card": {
                    "header": {"title": card.title, "subtitle": card.headline},
                    "sections": sections,
                },
            }
        ],
    }
