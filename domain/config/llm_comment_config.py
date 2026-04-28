from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LLMCommentConfig:
    enabled: bool = True
    comment_generation_strategy: str = "rules"
    api_key_env: str = "AZURE_OPENAI_KEY"
    require_api_key: bool = True
    model: str = "gpt-4o-mini"
    base_url: str = "https://sefazce-cegid-sophiaq.openai.azure.com/"
    timeout_seconds: int = 30
    temperature: float = 0.2
    max_output_tokens: int = 120
    api_type: str = "azure"
    api_version: str = "2024-12-01-preview"
