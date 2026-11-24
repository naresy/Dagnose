"""Diagnosis agent: LLM loop with read-only tools, structured JSON output."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dagnose.agent.diagnose import DiagnosisError, diagnose
from dagnose.agent.provider import LLMProvider, ProviderConfigError, ProviderError
from dagnose.agent.tools import ToolBox

if TYPE_CHECKING:
    from dagnose.config import Settings

__all__ = [
    "DiagnosisError",
    "LLMProvider",
    "ProviderConfigError",
    "ProviderError",
    "ToolBox",
    "diagnose",
    "make_provider",
]


def make_provider(settings: Settings) -> LLMProvider:
    """Claude on the configured platform: the Claude API, Bedrock, Vertex AI or Foundry.
    The model may also carry the platform, e.g. "bedrock/claude-opus-5-5"."""
    from dagnose.agent.anthropic_provider import AnthropicProvider
    from dagnose.agent.clients import build_client

    choice = build_client(settings)
    return AnthropicProvider(
        choice.model,
        client=choice.client,
        effort=settings.llm_effort,
        fallbacks=choice.fallbacks,
        name=choice.provider,
    )
