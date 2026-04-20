"""Inference configuration loading for OpenAI-backed operations."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from yaml import safe_load

from knowledge_forge.inference.cost import ModelPricing


class RateLimitSettings(BaseModel):
    """Rate-limit configuration for future request orchestration."""

    max_requests_per_minute: int = Field(gt=0)
    max_tokens_per_minute: int = Field(gt=0)


class BatchSettings(BaseModel):
    """Batch execution defaults reserved for later issues."""

    max_batch_size: int = Field(gt=0)
    poll_interval_seconds: int = Field(gt=0)
    max_poll_duration_seconds: int = Field(gt=0)


class ExtractionStrategy(str, Enum):
    """Supported extraction scheduler execution strategies."""

    BATCH = "batch"
    DIRECT_SERIAL = "direct_serial"
    DIRECT_LIMITED = "direct_limited"


class ExtractionModelRouteRule(BaseModel):
    """Explicit, reviewable routing rule for one extraction model choice."""

    model_config = ConfigDict(extra="forbid")

    model: str = Field(min_length=1)
    record_types: list[str] = Field(default_factory=list)
    section_types: list[str] = Field(default_factory=list)
    min_estimated_prompt_tokens: int | None = Field(default=None, ge=0)
    max_estimated_prompt_tokens: int | None = Field(default=None, ge=0)
    retry_class: str | None = None


class ExtractionModelRoutingSettings(BaseModel):
    """Model routing rules applied by the extraction run scheduler."""

    model_config = ConfigDict(extra="forbid")

    rules: list[ExtractionModelRouteRule] = Field(default_factory=list)


class ExtractionSettings(BaseModel):
    """Scheduler settings for durable extraction runs."""

    model_config = ConfigDict(extra="forbid")

    strategy: ExtractionStrategy = ExtractionStrategy.DIRECT_SERIAL
    direct_concurrency: int = Field(default=1, gt=0)
    batch_chunk_size: int = Field(default=25, gt=0)
    token_estimate_chars_per_token: float = Field(default=4.0, gt=0.0)
    dispatch_cooldown_seconds: float = Field(default=15.0, ge=0.0)
    model_routing: ExtractionModelRoutingSettings = Field(default_factory=ExtractionModelRoutingSettings)


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
    extraction: ExtractionSettings = Field(default_factory=ExtractionSettings)
    pricing: dict[str, ModelPricing] = Field(default_factory=dict)
    api_key: SecretStr = Field(exclude=True, repr=False)

    @model_validator(mode="after")
    def validate_api_key(self) -> "InferenceConfig":
        """Validate that the configured API key was resolved before model validation."""
        key_name = self.api_key_env.strip()
        if not key_name:
            raise ValueError("api_key_env must not be blank")

        if not self.api_key.get_secret_value().strip():
            raise ValueError(f"required API key environment variable '{key_name}' is not set")

        return self

    @classmethod
    def load(
        cls,
        config_path: Path | None = None,
        *,
        environ: dict[str, str] | None = None,
    ) -> "InferenceConfig":
        """Load inference configuration from YAML plus env var overrides."""
        resolved_env = os.environ if environ is None else environ
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
        "extraction": dict(config.get("extraction", {}) or {}),
        "pricing": dict(config.get("pricing", {}) or {}),
    }
    extraction_model_routing = (
        merged["extraction"].get("model_routing", {})
        if isinstance(merged["extraction"].get("model_routing", {}), dict)
        else {}
    )
    merged["extraction"]["model_routing"] = {
        **extraction_model_routing,
        "rules": list(extraction_model_routing.get("rules", []) or []),
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
        ("extraction", "strategy"): ("KNOWLEDGE_FORGE_OPENAI_EXTRACTION_STRATEGY", str),
        ("extraction", "direct_concurrency"): ("KNOWLEDGE_FORGE_OPENAI_EXTRACTION_DIRECT_CONCURRENCY", int),
        ("extraction", "batch_chunk_size"): ("KNOWLEDGE_FORGE_OPENAI_EXTRACTION_BATCH_CHUNK_SIZE", int),
        ("extraction", "token_estimate_chars_per_token"): (
            "KNOWLEDGE_FORGE_OPENAI_EXTRACTION_TOKEN_ESTIMATE_CHARS_PER_TOKEN",
            float,
        ),
        ("extraction", "dispatch_cooldown_seconds"): (
            "KNOWLEDGE_FORGE_OPENAI_EXTRACTION_DISPATCH_COOLDOWN_SECONDS",
            float,
        ),
    }

    for field_name, (env_name, caster) in scalar_overrides.items():
        if env_name in environ:
            merged[field_name] = caster(environ[env_name])

    for (group_name, field_name), (env_name, caster) in nested_overrides.items():
        if env_name in environ:
            merged[group_name][field_name] = caster(environ[env_name])

    return merged
