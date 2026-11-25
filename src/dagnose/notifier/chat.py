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
from dagnose.notifier.slack import evidence_title

# ---- Microsoft Teams ------------------------------------------------------------------


def teams_payload(card: Card) -> dict[str, Any]:
    def text(value: str, **extra: Any) -> dict[str, Any]:
        return {"type": "TextBlock", "text": value, "wrap": True, **extra}

    body: list[dict[str, Any]] = [
        text(card.title, size="Large", weight="Bolder"),
        text(card.headline, weight="Bolder", color="Attention" if card.diagnosed else "Warning"),
        text(card.summary),
    ]
    if card.fix:
        body += [text("What to do", weight="Bolder", spacing="Medium"), text(card.fix)]
        if card.has_patch:
            body.append(
                text("A ready-made patch is in the diagnosis file.", isSubtle=True, size="Small")
            )
    if card.root_cause:
        body += [text("Why it failed", weight="Bolder", spacing="Medium"), text(card.root_cause)]
    if card.evidence:
        body.append(text(evidence_title(card), weight="Bolder", spacing="Medium"))
        for ev in card.evidence:
            body.append(text(ev.where + ("" if ev.verified else " (not found in the logs)"),
                             isSubtle=True, size="Small", spacing="Small"))  # fmt: skip
            body.append(text(ev.quote, fontType="Monospace", size="Small", spacing="None"))
    if card.note:
        body.append(text(card.note, isSubtle=True, size="Small", spacing="Medium"))
    body.append(
        {
            "type": "FactSet",
            "spacing": "Medium",
            "facts": [{"title": k, "value": v} for k, v in card.facts],
        }  # fmt: skip
    )
    if card.footer:
        body.append(text(card.footer, isSubtle=True, size="Small"))
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
    if card.fix:
        patch = "<br><i>A ready-made patch is in the diagnosis file.</i>" if card.has_patch else ""
        sections.append({"header": "What to do", "widgets": [para(e(card.fix) + patch)]})
    if card.root_cause:
        sections.append({"header": "Why it failed", "widgets": [para(e(card.root_cause))]})
    if card.evidence:
        rows = "<br>".join(
            f"<b>{e(ev.where)}</b>{'' if ev.verified else ' <i>(not found in the logs)</i>'}"
            f'<br><font color="#5f6368">{e(ev.quote)}</font>'
            for ev in card.evidence
        )
        sections.append({"header": evidence_title(card), "widgets": [para(rows)]})
    footer: list[dict[str, Any]] = []
    if card.note:
        footer.append(para(f"<i>{e(card.note)}</i>"))
    footer += [{"decoratedText": {"topLabel": e(k), "text": e(v)}} for k, v in card.facts]
    if card.footer:
        footer.append(para(f'<font color="#5f6368">{e(card.footer)}</font>'))
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
