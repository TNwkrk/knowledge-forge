"""Tests for the OpenAI inference configuration and client wrapper."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from knowledge_forge.inference import InferenceClient, InferenceConfig


def test_inference_config_loads_yaml_with_env_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "inference.yaml"
    config_path.write_text(
        """
openai:
  api_key_env: OPENAI_API_KEY
  default_model: gpt-4o-mini
  extraction_model: gpt-4o-mini
  compilation_model: gpt-4o-mini
  temperature: 0.0
  max_tokens: 2048
  rate_limit:
    max_requests_per_minute: 60
    max_tokens_per_minute: 50000
  batch:
    max_batch_size: 200
    poll_interval_seconds: 15
    max_poll_duration_seconds: 120
""".strip(),
        encoding="utf-8",
    )

    config = InferenceConfig.load(
        config_path,
        environ={
            "OPENAI_API_KEY": "test-secret",
            "KNOWLEDGE_FORGE_OPENAI_DEFAULT_MODEL": "gpt-4.1-mini",
            "KNOWLEDGE_FORGE_OPENAI_MAX_TOKENS": "1024",
            "KNOWLEDGE_FORGE_OPENAI_RATE_LIMIT_MAX_REQUESTS_PER_MINUTE": "120",
        },
    )

    assert config.default_model == "gpt-4.1-mini"
    assert config.extraction_model == "gpt-4o-mini"
    assert config.max_tokens == 1024
    assert config.rate_limit.max_requests_per_minute == 120
    assert config.api_key.get_secret_value() == "test-secret"


def test_inference_config_requires_api_key_from_provided_env_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "inference.yaml"
    config_path.write_text(
        """
openai:
  api_key_env: OPENAI_API_KEY
  default_model: gpt-4o
  extraction_model: gpt-4o
  compilation_model: gpt-4o
  temperature: 0.0
  max_tokens: 4096
  rate_limit:
    max_requests_per_minute: 500
    max_tokens_per_minute: 150000
  batch:
    max_batch_size: 50000
    poll_interval_seconds: 60
    max_poll_duration_seconds: 86400
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("OPENAI_API_KEY", "should-not-be-used")

    with pytest.raises(ValueError, match="required API key environment variable 'OPENAI_API_KEY' is not set"):
        InferenceConfig.load(config_path, environ={})


def test_inference_client_initializes_sdk_with_secret_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    def fake_openai(*, api_key: str) -> object:
        captured["api_key"] = api_key
        return SimpleNamespace(responses=SimpleNamespace(create=lambda **_: None))

    monkeypatch.setattr("knowledge_forge.inference.client.OpenAI", fake_openai)
    config = InferenceConfig.model_validate(
        {
            "api_key_env": "OPENAI_API_KEY",
            "default_model": "gpt-4o",
            "extraction_model": "gpt-4o",
            "compilation_model": "gpt-4o",
            "temperature": 0.0,
            "max_tokens": 4096,
            "rate_limit": {
                "max_requests_per_minute": 500,
                "max_tokens_per_minute": 150000,
            },
            "batch": {
                "max_batch_size": 50000,
                "poll_interval_seconds": 60,
                "max_poll_duration_seconds": 86400,
            },
            "api_key": "sdk-secret",
        }
    )

    client = InferenceClient(config)

    assert captured["api_key"] == "sdk-secret"
    assert "sdk-secret" not in repr(config)
    assert "api_key" not in config.model_dump()
    assert client.config is config


def test_inference_client_completes_direct_request() -> None:
    response = SimpleNamespace(
        id="resp_123",
        model="gpt-4o",
        output_text="A direct answer",
        usage=SimpleNamespace(input_tokens=12, output_tokens=4),
    )
    sdk_client = SimpleNamespace(
        responses=SimpleNamespace(create=lambda **_: response),
    )
    config = _build_config()

    result = InferenceClient(config, sdk_client=sdk_client).complete(
        prompt="What is Knowledge Forge?",
        system="Answer clearly.",
    )

    assert result.response_text == "A direct answer"
    assert result.parsed_json is None
    assert result.model_used == "gpt-4o"
    assert result.input_tokens == 12
    assert result.output_tokens == 4
    assert result.request_id == "resp_123"


def test_inference_client_validates_schema_bound_response() -> None:
    schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "steps": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "required": ["title", "steps"],
        "additionalProperties": False,
    }
    response = SimpleNamespace(
        id="resp_456",
        output_text=json.dumps({"title": "Startup", "steps": ["Power on", "Wait for ready"]}),
        usage=SimpleNamespace(input_tokens=20, output_tokens=10),
    )
    requests: list[dict[str, object]] = []

    def fake_create(**kwargs: object) -> object:
        requests.append(kwargs)
        return response

    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=fake_create))
    config = _build_config()

    result = InferenceClient(config, sdk_client=sdk_client).complete(
        prompt="Extract the startup procedure.",
        system="Return structured JSON.",
        schema=schema,
    )

    assert result.parsed_json == {"title": "Startup", "steps": ["Power on", "Wait for ready"]}
    assert requests[0]["text"] == {
        "format": {
            "type": "json_schema",
            "name": "knowledge_forge_schema",
            "schema": schema,
            "strict": True,
        }
    }


def test_inference_client_rejects_invalid_schema_response() -> None:
    schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}},
        "required": ["title"],
        "additionalProperties": False,
    }
    response = SimpleNamespace(
        output_text=json.dumps({"steps": ["Missing title"]}),
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_: response))

    with pytest.raises(ValueError, match="response did not satisfy schema"):
        InferenceClient(_build_config(), sdk_client=sdk_client).complete(
            prompt="Extract a title.",
            system="Return JSON.",
            schema=schema,
        )


def test_inference_client_rejects_non_json_schema_response() -> None:
    schema = {"type": "object"}
    response = SimpleNamespace(
        output_text="not json at all",
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_: response))

    with pytest.raises(ValueError, match="response was not valid JSON"):
        InferenceClient(_build_config(), sdk_client=sdk_client).complete(
            prompt="Return JSON.",
            system="Return JSON.",
            schema=schema,
        )


def _build_config() -> InferenceConfig:
    return InferenceConfig.model_validate(
        {
            "api_key_env": "OPENAI_API_KEY",
            "default_model": "gpt-4o",
            "extraction_model": "gpt-4o",
            "compilation_model": "gpt-4o",
            "temperature": 0.0,
            "max_tokens": 4096,
            "rate_limit": {
                "max_requests_per_minute": 500,
                "max_tokens_per_minute": 150000,
            },
            "batch": {
                "max_batch_size": 50000,
                "poll_interval_seconds": 60,
                "max_poll_duration_seconds": 86400,
            },
            "api_key": "test-secret",
        }
    )
