"""Inference configuration loading for OpenAI-backed operations."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from yaml import safe_load


class RateLimitSettings(BaseModel):
    """Rate-limit configuration for future request orchestration."""

    max_requests_per_minute: int = Field(gt=0)
    max_tokens_per_minute: int = Field(gt=0)


class BatchSettings(BaseModel):
    """Batch execution defaults reserved for later issues."""

    max_batch_size: int = Field(gt=0)
    poll_interval_seconds: int = Field(gt=0)
    max_poll_duration_seconds: int = Field(gt=0)


class InferenceConfig(BaseModel):
    """Validated OpenAI inference configuration and resolved secret access."""

    model_config = ConfigDict(extra="forbid")

    api_key_env: str = Field(min_length=1)
    default_model: str = Field(min_length=1)
    extraction_model: str = Field(min_length=1)
    compilation_model: str = Field(min_length=1)
    temperature: float = Field(ge=0.0, le=2.0)
    max_tokens: int = Field(gt=0)
    rate_limit: RateLimitSettings
    batch: BatchSettings
    api_key: SecretStr = Field(exclude=True, repr=False)

    @model_validator(mode="after")
    def validate_api_key(self) -> "InferenceConfig":
        """Resolve the configured API key environment variable."""
        if self.api_key.get_secret_value().strip():
            return self

        key_name = self.api_key_env.strip()
        if not key_name:
            raise ValueError("api_key_env must not be blank")

        secret = os.environ.get(key_name, "").strip()
        if not secret:
            raise ValueError(f"required API key environment variable '{key_name}' is not set")

        self.api_key = SecretStr(secret)
        return self

    @classmethod
    def load(
        cls,
        config_path: Path | None = None,
        *,
        environ: dict[str, str] | None = None,
    ) -> "InferenceConfig":
        """Load inference configuration from YAML plus env var overrides."""
        resolved_env = environ or os.environ
        path = config_path or Path("config/inference.yaml")
        if not path.exists():
            raise FileNotFoundError(f"inference config not found at '{path}'")

        payload = safe_load(path.read_text(encoding="utf-8")) or {}
        openai_config = payload.get("openai", {}) or {}
        merged = _apply_env_overrides(openai_config, resolved_env)
        merged["api_key"] = resolved_env.get(str(merged.get("api_key_env", "")), "")
        return cls.model_validate(merged)


def _apply_env_overrides(config: dict[str, Any], environ: dict[str, str]) -> dict[str, Any]:
    """Overlay environment values onto the YAML config."""
    merged: dict[str, Any] = {
        **config,
        "rate_limit": dict(config.get("rate_limit", {}) or {}),
        "batch": dict(config.get("batch", {}) or {}),
    }

    scalar_overrides: dict[str, tuple[str, type[Any]]] = {
        "api_key_env": ("KNOWLEDGE_FORGE_OPENAI_API_KEY_ENV", str),
        "default_model": ("KNOWLEDGE_FORGE_OPENAI_DEFAULT_MODEL", str),
        "extraction_model": ("KNOWLEDGE_FORGE_OPENAI_EXTRACTION_MODEL", str),
        "compilation_model": ("KNOWLEDGE_FORGE_OPENAI_COMPILATION_MODEL", str),
        "temperature": ("KNOWLEDGE_FORGE_OPENAI_TEMPERATURE", float),
        "max_tokens": ("KNOWLEDGE_FORGE_OPENAI_MAX_TOKENS", int),
    }
    nested_overrides: dict[tuple[str, str], tuple[str, type[Any]]] = {
        ("rate_limit", "max_requests_per_minute"): (
            "KNOWLEDGE_FORGE_OPENAI_RATE_LIMIT_MAX_REQUESTS_PER_MINUTE",
            int,
        ),
        ("rate_limit", "max_tokens_per_minute"): (
            "KNOWLEDGE_FORGE_OPENAI_RATE_LIMIT_MAX_TOKENS_PER_MINUTE",
            int,
        ),
        ("batch", "max_batch_size"): ("KNOWLEDGE_FORGE_OPENAI_BATCH_MAX_BATCH_SIZE", int),
        ("batch", "poll_interval_seconds"): ("KNOWLEDGE_FORGE_OPENAI_BATCH_POLL_INTERVAL_SECONDS", int),
        ("batch", "max_poll_duration_seconds"): (
            "KNOWLEDGE_FORGE_OPENAI_BATCH_MAX_POLL_DURATION_SECONDS",
            int,
        ),
    }

    for field_name, (env_name, caster) in scalar_overrides.items():
        if env_name in environ:
            merged[field_name] = caster(environ[env_name])

    for (group_name, field_name), (env_name, caster) in nested_overrides.items():
        if env_name in environ:
            merged[group_name][field_name] = caster(environ[env_name])

    return merged
