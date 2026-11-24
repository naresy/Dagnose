"""Claude on Bedrock, Vertex AI and Foundry: the real SDK clients over a mocked network.

Checks what each platform actually receives (URL, model, auth, body), that server-side
fallbacks are off where the platform does not support them, and the setup errors.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx2
import pytest

from dagnose.agent import make_provider
from dagnose.agent.anthropic_provider import AnthropicProvider
from dagnose.agent.clients import build_client
from dagnose.agent.prompt import DIAGNOSIS_SCHEMA
from dagnose.agent.provider import ProviderConfigError

ANSWER = {
    "id": "msg_1",
    "type": "message",
    "role": "assistant",
    "model": "claude-opus-5-5",
    "content": [{"type": "text", "text": "{}"}],
    "stop_reason": "end_turn",
    "stop_sequence": None,
    "usage": {"input_tokens": 10, "output_tokens": 2},
}


class Capture:
    def __init__(self) -> None:
        self.requests: list[httpx2.Request] = []

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        return httpx2.Response(200, json=ANSWER)

    def http(self) -> Any:
        return anthropic.DefaultHttpxClient(transport=httpx2.MockTransport(self))


def send_once(client: Any, model: str, fallbacks: bool) -> None:
    provider = AnthropicProvider(model, client=client, fallbacks=fallbacks, name="x")
    provider.start(system="S", prompt="P", tools=[], output_schema=DIAGNOSIS_SCHEMA).send()


def test_bedrock_request(monkeypatch: pytest.MonkeyPatch) -> None:
    cap = Capture()
    client = anthropic.AnthropicBedrockMantle(
        aws_region="eu-west-1",
        aws_access_key="AKID",
        aws_secret_key="secret",
        http_client=cap.http(),
        max_retries=0,
    )
    send_once(client, "anthropic.claude-opus-5-5", fallbacks=False)
    [req] = cap.requests
    body = json.loads(req.content)
    assert "eu-west-1" in req.url.host and "bedrock" in req.url.host
    assert body["model"] == "anthropic.claude-opus-5-5"
    assert "fallbacks" not in body
    assert req.headers["authorization"].startswith("AWS4-HMAC-SHA256")  # SigV4-signed
    assert body["output_config"]["format"]["type"] == "json_schema"


def test_vertex_request() -> None:
    cap = Capture()
    client = anthropic.AnthropicVertex(
        project_id="acme-data",
        region="global",
        access_token="ya29.test",
        http_client=cap.http(),
        max_retries=0,
    )
    send_once(client, "claude-opus-5-5", fallbacks=False)
    [req] = cap.requests
    assert "/projects/acme-data/locations/global/" in req.url.path
    assert "claude-opus-5-5" in req.url.path  # Vertex puts the model in the URL
    assert req.headers["authorization"] == "Bearer ya29.test"
    assert "fallbacks" not in json.loads(req.content)


def test_foundry_request() -> None:
    cap = Capture()
    client = anthropic.AnthropicFoundry(
        resource="acme-ai", api_key="foundry-key", http_client=cap.http(), max_retries=0
    )
    send_once(client, "claude-opus-5-5", fallbacks=False)
    [req] = cap.requests
    assert req.url.host.startswith("acme-ai.")
    assert json.loads(req.content)["model"] == "claude-opus-5-5"
    assert "fallbacks" not in json.loads(req.content)


# ---- selection ------------------------------------------------------------------------


def test_build_client_per_platform(make_settings: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKID")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    bedrock = build_client(make_settings(llm_provider="bedrock", aws_region="eu-west-1"))
    assert (bedrock.provider, bedrock.model, bedrock.fallbacks) == (
        "bedrock",
        "anthropic.claude-opus-5-5",
        False,
    )
    assert isinstance(bedrock.client, anthropic.AnthropicBedrockMantle)

    foundry = build_client(
        make_settings(llm_provider="foundry", llm_foundry_resource="acme-ai", llm_api_key="k")
    )
    assert isinstance(foundry.client, anthropic.AnthropicFoundry) and not foundry.fallbacks

    first_party = build_client(make_settings(llm_api_key="k"))
    assert first_party.provider == "anthropic" and first_party.fallbacks  # only here

    via_model = build_client(
        make_settings(llm_model="bedrock/claude-opus-5-5", aws_region="us-east-1")
    )
    assert via_model.provider == "bedrock" and via_model.model == "anthropic.claude-opus-5-5"


def test_make_provider_reports_the_platform(make_settings: Any) -> None:
    p = make_provider(
        make_settings(llm_provider="foundry", llm_foundry_resource="acme-ai", llm_api_key="k")
    )
    assert (p.name, p.model) == ("foundry", "claude-opus-5-5")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"llm_provider": "vertex"}, "DAGNOSE_LLM_PROJECT"),
        ({"llm_provider": "foundry"}, "DAGNOSE_LLM_FOUNDRY_RESOURCE"),
        ({"llm_model": "openai/gpt-x"}, "not supported"),
    ],
)
def test_platform_setup_errors(overrides: dict[str, Any], message: str, make_settings: Any) -> None:
    with pytest.raises(ProviderConfigError, match=message):
        build_client(make_settings(**overrides))
