from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LLMCommentConfig:
    enabled: bool = False
    api_key_env: str = "OPENAI_API_KEY"
    model: str = "gpt-5.4-nano"
    base_url: str = "https://api.openai.com/v1"
    timeout_seconds: int = 60
    temperature: float = 0.2
    max_output_tokens: int = 180
