from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LLMCommentConfig:
    enabled: bool = True
    comment_generation_strategy: str = "rules"
    api_key_env: str = ""
    require_api_key: bool = False
    model: str = "qwen2.5:7b"
    base_url: str = "http://localhost:11434/v1"
    timeout_seconds: int = 20
    temperature: float = 0.2
    max_output_tokens: int = 120
