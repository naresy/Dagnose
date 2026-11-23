"""Claude via the official Anthropic SDK.

- Final answer as structured output (output_config.format, JSON schema) so it always
  parses; tools are strict so their inputs always match their schemas.
- The conversation is append-only and the assistant content is sent back exactly as
  returned, which keeps thinking blocks valid across turns.
- Server-side refusal fallback is on by default (fallbacks="default").
- Prompt caching when tools are on (multi-turn); a one-turn call is not cached.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import anthropic

from dagnose.agent.provider import (
    ProviderConfigError,
    ProviderError,
    StopReason,
    ToolCall,
    ToolResult,
    ToolSpec,
    Turn,
)
from dagnose.models import Usage

FALLBACK_BETA = "server-side-fallback-2026-07-01"

_STOP: dict[str, StopReason] = {
    "end_turn": "done",
    "tool_use": "tool_use",
    "max_tokens": "max_tokens",
    "refusal": "refusal",
    "pause_turn": "continue",
    "stop_sequence": "done",
}


class AnthropicProvider:
    name = "anthropic"

    def __init__(
        self,
        model: str = "claude-opus-5-5",
        *,
        api_key: str | None = None,
        effort: str = "high",
        fallbacks: bool = True,
        max_tokens: int = 16000,
        client: anthropic.Anthropic | None = None,
    ) -> None:
        self.model = model
        self.effort = effort
        self.fallbacks = fallbacks
        self.max_tokens = max_tokens
        # No api_key: the SDK resolves ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or an
        # `ant auth login` profile itself.
        self.client = client or anthropic.Anthropic(api_key=api_key)

    def start(
        self,
        *,
        system: str,
        prompt: str,
        tools: Sequence[ToolSpec],
        output_schema: dict[str, Any],
    ) -> AnthropicSession:
        return AnthropicSession(self, system, prompt, tools, output_schema)

    def verify(self) -> int:
        """Check the key and model without generating anything (token counting is free).
        Returns the counted tokens; raises ProviderConfigError / ProviderError."""
        try:
            resp = self.client.messages.count_tokens(
                model=self.model, messages=[{"role": "user", "content": "ping"}]
            )
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as exc:
            raise ProviderConfigError(f"Anthropic rejected the credentials: {exc}") from exc
        except (anthropic.NotFoundError, anthropic.BadRequestError) as exc:
            raise ProviderConfigError(f"Anthropic rejected the model: {exc}") from exc
        except (anthropic.RateLimitError, anthropic.APIConnectionError) as exc:
            raise ProviderError(f"Anthropic unavailable: {exc}") from exc
        except anthropic.APIStatusError as exc:
            raise ProviderError(f"Anthropic error {exc.status_code}: {exc}") from exc
        except TypeError as exc:
            if "authentication method" not in str(exc):
                raise
            raise ProviderConfigError(
                "no Anthropic credentials: set DAGNOSE_LLM_API_KEY or "
                "ANTHROPIC_API_KEY, or run `ant auth login`"
            ) from exc
        return int(resp.input_tokens)


class AnthropicSession:
    def __init__(
        self,
        provider: AnthropicProvider,
        system: str,
        prompt: str,
        tools: Sequence[ToolSpec],
        output_schema: dict[str, Any],
    ) -> None:
        self.p = provider
        self.system = system
        self.tools = [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
                "strict": True,
            }
            for t in tools
        ]
        self.output_schema = output_schema
        self.messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
        self._started = False

    def send(self, tool_results: Sequence[ToolResult] = (), note: str | None = None) -> Turn:
        if self._started:
            content: list[dict[str, Any]] = [
                {
                    "type": "tool_result",
                    "tool_use_id": r.call_id,
                    "content": r.content,
                    "is_error": r.is_error,
                }
                for r in tool_results
            ]
            if note:
                content.append({"type": "text", "text": note})
            if content:
                self.messages.append({"role": "user", "content": content})
        self._started = True

        kwargs: dict[str, Any] = {
            "model": self.p.model,
            "max_tokens": self.p.max_tokens,
            "system": self.system,
            "messages": self.messages,
            "output_config": {
                "effort": self.p.effort,
                "format": {"type": "json_schema", "schema": self.output_schema},
            },
        }
        if self.tools:
            kwargs["tools"] = self.tools
            # Cache only when the conversation can have several turns. A one-turn
            # diagnosis would pay the cache-write premium and never read it back.
            kwargs["cache_control"] = {"type": "ephemeral"}
        try:
            if self.p.fallbacks:
                response: Any = self.p.client.beta.messages.create(
                    **kwargs, betas=[FALLBACK_BETA], fallbacks="default"
                )
            else:
                response = self.p.client.messages.create(**kwargs)
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as exc:
            raise ProviderConfigError(f"Anthropic rejected the credentials: {exc}") from exc
        except (anthropic.NotFoundError, anthropic.BadRequestError) as exc:
            raise ProviderConfigError(f"Anthropic rejected the request: {exc}") from exc
        except (anthropic.RateLimitError, anthropic.APIConnectionError) as exc:
            raise ProviderError(f"Anthropic unavailable: {exc}") from exc
        except anthropic.APIStatusError as exc:
            raise ProviderError(f"Anthropic error {exc.status_code}: {exc}") from exc
        except anthropic.AnthropicError as exc:
            raise ProviderConfigError(str(exc)) from exc
        except TypeError as exc:
            # The SDK raises a plain TypeError, before any network call, when no API
            # key, auth token or profile can be found.
            if "authentication method" not in str(exc):
                raise
            raise ProviderConfigError(
                "no Anthropic credentials: set DAGNOSE_LLM_API_KEY or "
                "ANTHROPIC_API_KEY, or run `ant auth login`"
            ) from exc

        # Append exactly what came back (thinking blocks included): append-only history.
        self.messages.append({"role": "assistant", "content": response.content})

        text = "".join(b.text for b in response.content if getattr(b, "type", "") == "text")
        calls = [
            ToolCall(id=b.id, name=b.name, input=dict(b.input))
            for b in response.content
            if getattr(b, "type", "") == "tool_use"
        ]
        u = response.usage
        details = getattr(response, "stop_details", None)
        return Turn(
            stop=_STOP.get(str(response.stop_reason), "done"),
            text=text,
            tool_calls=calls,
            usage=Usage(
                input_tokens=u.input_tokens or 0,
                output_tokens=u.output_tokens or 0,
                cache_read_input_tokens=getattr(u, "cache_read_input_tokens", 0) or 0,
                cache_creation_input_tokens=getattr(u, "cache_creation_input_tokens", 0) or 0,
            ),
            served_model=getattr(response, "model", None),
            refusal_category=getattr(details, "category", None) if details else None,
        )
