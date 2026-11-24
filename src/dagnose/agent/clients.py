"""Build the Claude client for the configured platform.

The same AnthropicProvider (and so the same diagnosis loop) runs on every platform; only
the SDK client differs. On Bedrock, Vertex AI and Foundry the prompt stays inside the
customer's own cloud account. Server-side refusal fallback exists only on the Claude
API, so it is switched off elsewhere (a refusal then becomes "not diagnosed").
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dagnose.agent.provider import ProviderConfigError

PROVIDERS = ("anthropic", "bedrock", "vertex", "foundry")


@dataclass
class ClientChoice:
    provider: str
    model: str  # as the platform names it, e.g. "anthropic.claude-opus-5-5" on Bedrock
    client: Any
    fallbacks: bool


def build_client(settings: Any) -> ClientChoice:
    prefix, _, model = settings.llm_model.rpartition("/")
    provider = prefix or settings.llm_provider
    if provider not in PROVIDERS:
        raise ProviderConfigError(
            f"LLM provider {provider!r} is not supported (supported: {', '.join(PROVIDERS)})"
        )
    key = settings.llm_api_key.get_secret_value() if settings.llm_api_key else None
    client: Any
    try:
        import anthropic

        if provider == "anthropic":
            # No key: the SDK finds ANTHROPIC_API_KEY, ANTHROPIC_AUTH_TOKEN or a profile.
            return ClientChoice(
                provider, model, anthropic.Anthropic(api_key=key), settings.llm_fallbacks
            )
        if provider == "bedrock":
            region = settings.llm_region or settings.aws_region
            client = anthropic.AnthropicBedrockMantle(aws_region=region)
            bedrock_model = model if model.startswith("anthropic.") else f"anthropic.{model}"
            return ClientChoice(provider, bedrock_model, client, False)
        if provider == "vertex":
            if not settings.llm_project:
                raise ProviderConfigError("DAGNOSE_LLM_PROVIDER=vertex needs DAGNOSE_LLM_PROJECT")
            client = anthropic.AnthropicVertex(
                project_id=settings.llm_project, region=settings.llm_region or "global"
            )
            return ClientChoice(provider, model, client, False)
        # foundry
        if not settings.llm_foundry_resource:
            raise ProviderConfigError(
                "DAGNOSE_LLM_PROVIDER=foundry needs DAGNOSE_LLM_FOUNDRY_RESOURCE"
            )
        if key:
            client = anthropic.AnthropicFoundry(resource=settings.llm_foundry_resource, api_key=key)
        else:
            try:
                from azure.identity import DefaultAzureCredential, get_bearer_token_provider
            except ImportError as exc:
                raise ProviderConfigError(
                    "Foundry without an API key uses Entra ID: pip install 'dagnose[azure]'"
                ) from exc
            token_provider = get_bearer_token_provider(
                DefaultAzureCredential(), settings.llm_azure_scope
            )
            client = anthropic.AnthropicFoundry(
                resource=settings.llm_foundry_resource, azure_ad_token_provider=token_provider
            )
        return ClientChoice(provider, model, client, False)
    except ProviderConfigError:
        raise
    except Exception as exc:  # missing cloud SDK, unreadable cloud credentials, ...
        raise ProviderConfigError(
            f"could not set up {provider}: {type(exc).__name__}: {exc}"
        ) from exc
