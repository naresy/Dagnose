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
    """Build the configured provider. Model may be "claude-opus-5-5" or
    "anthropic/claude-opus-5-5"; only Anthropic is implemented so far."""
    provider, _, model = settings.llm_model.rpartition("/")
    provider = provider or settings.llm_provider
    if provider != "anthropic":
        raise ProviderConfigError(
            f"LLM provider {provider!r} is not supported yet (supported: anthropic)"
        )
    from dagnose.agent.anthropic_provider import AnthropicProvider

    key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else None
    return AnthropicProvider(
        model,
        api_key=key,
        effort=settings.llm_effort,
        fallbacks=settings.llm_fallbacks,
    )
