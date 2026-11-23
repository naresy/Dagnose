"""Anthropic adapter against a mocked HTTP transport: request shape and response mapping.
No network, no API key, no cost."""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx2
import pytest

from dagnose.agent.anthropic_provider import FALLBACK_BETA, AnthropicProvider
from dagnose.agent.prompt import DIAGNOSIS_SCHEMA
from dagnose.agent.provider import ProviderConfigError, ProviderError, ToolResult, ToolSpec

TOOL = ToolSpec(
    name="list_run_tasks",
    description="List tasks.",
    input_schema={
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    },
)


def message(content: list[dict[str, Any]], stop: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": content,
        "stop_reason": stop,
        "stop_sequence": None,
        "usage": {
            "input_tokens": 1200,
            "output_tokens": 80,
            "cache_read_input_tokens": 1000,
            "cache_creation_input_tokens": 0,
        },
        **extra,
    }


class Recorder:
    def __init__(self, *responses: tuple[int, dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        status, body = self.responses.pop(0)
        return httpx2.Response(status, json=body)

    def body(self, i: int) -> dict[str, Any]:
        data: dict[str, Any] = json.loads(self.requests[i].content)
        return data


def provider(rec: Recorder, **kwargs: Any) -> AnthropicProvider:
    client = anthropic.Anthropic(
        api_key="test-key",
        max_retries=0,
        http_client=anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(rec)),
    )
    return AnthropicProvider("claude-opus-5-5", client=client, **kwargs)


def test_request_shape_and_tool_round_trip() -> None:
    thinking = {"type": "thinking", "thinking": "", "signature": "sig-abc"}
    tool_use = {"type": "tool_use", "id": "toolu_1", "name": "list_run_tasks", "input": {}}
    final = {"type": "text", "text": '{"summary": "ok"}'}
    rec = Recorder(
        (200, message([thinking, tool_use], "tool_use")), (200, message([final], "end_turn"))
    )
    session = provider(rec).start(
        system="SYS", prompt="CONTEXT", tools=[TOOL], output_schema=DIAGNOSIS_SCHEMA
    )

    turn = session.send()
    assert turn.stop == "tool_use"
    assert [(c.id, c.name, c.input) for c in turn.tool_calls] == [("toolu_1", "list_run_tasks", {})]
    assert turn.usage.input_tokens == 1200 and turn.usage.cache_read_input_tokens == 1000
    assert turn.served_model == "claude-opus-5-5"

    first = rec.body(0)
    assert first["model"] == "claude-opus-5-5"
    assert first["system"] == "SYS"
    assert first["messages"] == [{"role": "user", "content": "CONTEXT"}]
    assert first["output_config"] == {
        "effort": "high",
        "format": {"type": "json_schema", "schema": DIAGNOSIS_SCHEMA},
    }
    assert first["tools"][0]["strict"] is True
    assert first["cache_control"] == {"type": "ephemeral"}
    assert first["fallbacks"] == "default"
    assert FALLBACK_BETA in rec.requests[0].headers["anthropic-beta"]
    for removed in ("temperature", "top_p", "top_k", "thinking", "tool_choice"):
        assert removed not in first  # Opus 5.5: thinking is always on; forced tool_choice 400s

    turn = session.send([ToolResult("toolu_1", "[]")])
    assert turn.stop == "done" and turn.text == '{"summary": "ok"}'
    second = rec.body(1)["messages"]
    # Append-only history; the thinking block goes back unchanged.
    assert second[0] == {"role": "user", "content": "CONTEXT"}
    assert second[1]["role"] == "assistant"
    assert second[1]["content"][0]["type"] == "thinking"
    assert second[1]["content"][0]["signature"] == "sig-abc"
    assert second[2] == {
        "role": "user",
        "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "[]", "is_error": False}
        ],
    }


def test_without_fallbacks_uses_the_stable_endpoint() -> None:
    rec = Recorder((200, message([{"type": "text", "text": "{}"}], "end_turn")))
    provider(rec, fallbacks=False, effort="medium").start(
        system="S", prompt="P", tools=[], output_schema=DIAGNOSIS_SCHEMA
    ).send()
    body = rec.body(0)
    assert "fallbacks" not in body and "tools" not in body
    assert "cache_control" not in body  # one turn, nothing to reuse: no cache-write premium
    assert FALLBACK_BETA not in rec.requests[0].headers.get("anthropic-beta", "")
    assert body["output_config"]["effort"] == "medium"


def test_refusal_maps_category() -> None:
    rec = Recorder(
        (
            200,
            message(
                [],
                "refusal",
                stop_details={"type": "refusal", "category": "cyber", "explanation": None},
            ),
        )
    )
    turn = (
        provider(rec).start(system="S", prompt="P", tools=[], output_schema=DIAGNOSIS_SCHEMA).send()
    )
    assert turn.stop == "refusal" and turn.refusal_category == "cyber"


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, ProviderConfigError),
        (404, ProviderConfigError),
        (429, ProviderError),
        (529, ProviderError),
    ],
)
def test_errors(status: int, error: type[Exception]) -> None:
    rec = Recorder((status, {"type": "error", "error": {"type": "x", "message": "nope"}}))
    session = provider(rec).start(system="S", prompt="P", tools=[], output_schema=DIAGNOSIS_SCHEMA)
    with pytest.raises(error) as info:
        session.send()
    if status in (429, 529):
        assert not isinstance(info.value, ProviderConfigError)  # outages are retried


def test_no_credentials_is_a_config_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """No key anywhere: a clear config error, and no request is sent."""
    for var in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))  # no `ant auth login` profile on disk
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    rec = Recorder()
    client = anthropic.Anthropic(
        max_retries=0, http_client=anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(rec))
    )
    session = AnthropicProvider(client=client).start(
        system="S", prompt="P", tools=[], output_schema=DIAGNOSIS_SCHEMA
    )
    with pytest.raises(ProviderConfigError, match="no Anthropic credentials"):
        session.send()
    assert rec.requests == []
