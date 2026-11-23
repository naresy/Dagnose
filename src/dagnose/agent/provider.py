"""Provider-neutral interface between the diagnosis loop and an LLM.

The loop only sees these types. Each provider (Anthropic today; OpenAI or Ollama can
be added as their own adapters) turns them into its own API calls and keeps its own
conversation state, so provider details such as thinking blocks never leak here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from dagnose.models import Usage

StopReason = Literal["done", "tool_use", "max_tokens", "refusal", "continue"]


class ProviderError(RuntimeError):
    """The model call failed in a way that is worth retrying later (rate limit, outage)."""


class ProviderConfigError(ProviderError):
    """Missing or invalid credentials / model. Retrying will not help."""


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict[str, Any]  # JSON Schema, strict: every property required


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass(frozen=True)
class ToolResult:
    call_id: str
    content: str
    is_error: bool = False


@dataclass
class Turn:
    stop: StopReason
    text: str = ""  # final answer text (JSON when stop == "done")
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    served_model: str | None = None
    refusal_category: str | None = None


class ChatSession(Protocol):
    def send(self, tool_results: Sequence[ToolResult] = (), note: str | None = None) -> Turn:
        """Send tool results (and an optional note) and get the model's next turn.
        The first call sends the initial prompt."""
        ...


class LLMProvider(Protocol):
    name: str
    model: str

    def start(
        self,
        *,
        system: str,
        prompt: str,
        tools: Sequence[ToolSpec],
        output_schema: dict[str, Any],
    ) -> ChatSession: ...

    def verify(self) -> int:
        """Check credentials and model without spending tokens where possible."""
        ...
