"""Microsoft Teams and Google Chat cards: shape, escaping, no secrets, delivery, outbox."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from dagnose.models import FailureContext, RedactedContext
from dagnose.notifier import build_card, notify, resend_outbox
from dagnose.notifier.chat import google_chat_payload, teams_payload
from dagnose.redactor import Redactor
from tests.conftest import FIXTURES
from tests.test_notifier import SECRETS, diagnosis_run

LEAKY = FIXTURES / "contexts" / "v2" / "demo_crm_session_sync.json"
KEYERROR = FIXTURES / "contexts" / "v2" / "demo_orders_tax.json"
TEAMS = "https://prod-00.westeurope.logic.azure.example/workflows/abc/triggers/manual/paths/invoke"
GCHAT = "https://chat.googleapis.example/v1/spaces/AAA/messages?key=k&token=t"


def load(path: Path) -> RedactedContext:
    return Redactor().redact(FailureContext.model_validate_json(path.read_text()))


def test_teams_adaptive_card() -> None:
    ctx = load(KEYERROR)
    payload = teams_payload(build_card(ctx, diagnosis_run(ctx), ui_url="http://ui.test"))
    [attachment] = payload["attachments"]
    assert payload["type"] == "message"
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
    content = attachment["content"]
    assert content["type"] == "AdaptiveCard" and content["version"] == "1.4"
    texts = [b.get("text", "") for b in content["body"]]
    assert texts[0] == "demo_orders_tax.compute_tax failed"
    assert "DAG code, line 26" in texts and "What to do" in texts and "Why it failed" in texts
    assert texts.index("What to do") < texts.index("Why it failed")  # the action comes first
    assert content["actions"][0]["url"].startswith("http://ui.test/dags/demo_orders_tax/")
    assert any(b["type"] == "FactSet" for b in content["body"])


def test_google_chat_card_escapes_html() -> None:
    ctx = load(KEYERROR)
    weird = {"summary": "<script>alert(1)</script> & <a href=x>click</a>"}
    payload = google_chat_payload(
        build_card(ctx, diagnosis_run(ctx, diagnosis=weird), ui_url="http://ui.test")
    )
    card = payload["cardsV2"][0]["card"]
    assert card["header"]["title"] == "demo_orders_tax.compute_tax failed"
    first = card["sections"][0]["widgets"][0]["textParagraph"]["text"]
    assert "<script>" not in first and "&lt;script&gt;" in first and "&amp;" in first
    button = card["sections"][-1]["widgets"][-1]["buttonList"]["buttons"][0]
    assert button["onClick"]["openLink"]["url"].startswith("http://ui.test/")


def test_no_secrets_in_teams_or_google_chat() -> None:
    ctx = load(LEAKY)
    leaky = {
        "summary": "password Sup3r-S3cret-Pw for postgresql://u:Sup3r-S3cret-Pw@db/x",
        "suggested_fix": "mail data-oncall@example.com",
    }
    card = build_card(ctx, diagnosis_run(ctx, diagnosis=leaky), ui_url="http://ui.test")
    rendered = json.dumps(teams_payload(card)) + json.dumps(google_chat_payload(card))
    for secret in SECRETS:
        assert secret not in rendered


def test_delivered_to_teams_and_google_chat(make_settings: Any) -> None:
    ctx = load(KEYERROR)
    hits: list[tuple[str, dict[str, Any]]] = []

    def endpoints(request: httpx.Request) -> httpx.Response:
        hits.append((request.url.host, json.loads(request.content)))
        return httpx.Response(202 if "logic" in request.url.host else 200)  # Teams answers 202

    settings = make_settings(teams_webhook_url=TEAMS, google_chat_webhook_url=GCHAT)
    report = notify(
        ctx,
        diagnosis_run(ctx),
        settings,
        transport=httpx.MockTransport(endpoints),
        sleep=lambda s: None,
    )
    assert sorted(report.sent) == ["google_chat", "teams"] and report.failed == {}
    hosts = {h for h, _ in hits}
    assert any("logic" in h for h in hosts) and any("chat" in h for h in hosts)


def test_failed_teams_message_is_resent_from_outbox(make_settings: Any) -> None:
    ctx = load(KEYERROR)
    settings = make_settings(teams_webhook_url=TEAMS)
    down = httpx.MockTransport(lambda r: httpx.Response(503))
    report = notify(
        ctx, None, settings, name="job.json", reason="off", transport=down, sleep=lambda s: None
    )
    assert report.failed.keys() == {"teams"}
    assert [p.name for p in settings.outbox_dir.iterdir()] == ["job.teams.json"]
    delivered: list[dict[str, Any]] = []
    up = httpx.MockTransport(
        lambda r: delivered.append(json.loads(r.content)) or httpx.Response(202)
    )
    again = resend_outbox(settings, transport=up, sleep=lambda s: None)
    assert again.sent == ["job.teams.json"] and list(settings.outbox_dir.iterdir()) == []
    assert (
        delivered[0]["attachments"][0]["content"]["body"][0]["text"]
        == "demo_orders_tax.compute_tax failed"
    )
