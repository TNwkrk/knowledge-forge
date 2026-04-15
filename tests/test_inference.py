"""Tests for the OpenAI inference configuration and client wrapper."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.inference import (
    BatchBuilder,
    BatchJob,
    InferenceClient,
    InferenceConfig,
    InferenceLogEntry,
    InferenceLogger,
    aggregate_costs,
    estimate_cost,
    submit_batch,
)
from knowledge_forge.inference.logger import iter_log_entries


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
  pricing:
    gpt-4o-mini:
      input_per_million_tokens: 0.15
      output_per_million_tokens: 0.60
    gpt-4.1-mini:
      input_per_million_tokens: 0.40
      output_per_million_tokens: 1.60
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


def test_inference_config_defaults_pricing_when_omitted(tmp_path: Path) -> None:
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

    config = InferenceConfig.load(config_path, environ={"OPENAI_API_KEY": "test-secret"})

    assert config.pricing == {}


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
            "pricing": {
                "gpt-4o": {
                    "input_per_million_tokens": 2.5,
                    "output_per_million_tokens": 10.0,
                }
            },
            "api_key": "sdk-secret",
        }
    )

    client = InferenceClient(config)

    assert captured["api_key"] == "sdk-secret"
    assert "sdk-secret" not in repr(config)
    assert "api_key" not in config.model_dump()
    assert client.config is config


def test_inference_client_completes_direct_request(tmp_path: Path) -> None:
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
    logger = InferenceLogger(tmp_path / "logs")

    result = InferenceClient(config, sdk_client=sdk_client, logger=logger).complete(
        prompt="What is Knowledge Forge?",
        system="Answer clearly.",
        prompt_template="tests/direct",
        source_doc_id="doc-001",
        source_section_id="sec-001",
        pipeline_run_id="run-001",
    )

    assert result.response_text == "A direct answer"
    assert result.parsed_json is None
    assert result.model_used == "gpt-4o"
    assert result.input_tokens == 12
    assert result.output_tokens == 4
    assert result.request_id == "resp_123"
    log_entries = list((tmp_path / "logs").rglob("*.json"))
    assert len(log_entries) == 1
    payload = json.loads(log_entries[0].read_text(encoding="utf-8"))
    assert payload["status"] == "success"
    assert payload["prompt_template"] == "tests/direct"
    assert payload["source_doc_id"] == "doc-001"
    assert payload["source_section_id"] == "sec-001"
    assert payload["pipeline_run_id"] == "run-001"
    assert payload["estimated_cost_usd"] == estimate_cost("gpt-4o", 12, 4, config.pricing)


def test_inference_client_allows_missing_pricing_on_success(tmp_path: Path) -> None:
    response = SimpleNamespace(
        id="resp_no_pricing",
        model="gpt-4o-unknown",
        output_text="A direct answer",
        usage=SimpleNamespace(input_tokens=12, output_tokens=4),
    )
    sdk_client = SimpleNamespace(
        responses=SimpleNamespace(create=lambda **_: response),
    )
    config = _build_config_without_pricing()
    logger = InferenceLogger(tmp_path / "logs")

    result = InferenceClient(config, sdk_client=sdk_client, logger=logger).complete(
        prompt="What is Knowledge Forge?",
        system="Answer clearly.",
    )

    assert result.model_used == "gpt-4o-unknown"
    payload = json.loads(next((tmp_path / "logs").rglob("*.json")).read_text(encoding="utf-8"))
    assert payload["estimated_cost_usd"] == 0.0


def test_inference_client_ignores_logger_oserror(tmp_path: Path) -> None:
    response = SimpleNamespace(
        id="resp_log_fail",
        model="gpt-4o",
        output_text="A direct answer",
        usage=SimpleNamespace(input_tokens=2, output_tokens=1),
    )
    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_: response))
    logger = InferenceLogger(tmp_path / "logs")

    def fail_log(*_: object, **__: object) -> None:
        raise OSError("disk full")

    logger.log = fail_log  # type: ignore[method-assign]
    result = InferenceClient(_build_config(), sdk_client=sdk_client, logger=logger).complete(
        prompt="What is Knowledge Forge?",
        system="Answer clearly.",
    )

    assert result.response_text == "A direct answer"
    assert not list((tmp_path / "logs").rglob("*.json"))


def test_inference_client_validates_schema_bound_response(tmp_path: Path) -> None:
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
    logger = InferenceLogger(tmp_path / "logs")

    result = InferenceClient(config, sdk_client=sdk_client, logger=logger).complete(
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
    payload = json.loads(next((tmp_path / "logs").rglob("*.json")).read_text(encoding="utf-8"))
    assert payload["schema_valid"] is True


def test_inference_client_rejects_invalid_schema_response(tmp_path: Path) -> None:
    schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}},
        "required": ["title"],
        "additionalProperties": False,
    }
    response = SimpleNamespace(
        id="resp_invalid",
        output_text=json.dumps({"steps": ["Missing title"]}),
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_: response))
    logger = InferenceLogger(tmp_path / "logs")

    with pytest.raises(ValueError, match="response did not satisfy schema"):
        InferenceClient(_build_config(), sdk_client=sdk_client, logger=logger).complete(
            prompt="Extract a title.",
            system="Return JSON.",
            schema=schema,
        )
    payload = json.loads(next((tmp_path / "logs").rglob("*.json")).read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["schema_valid"] is False


def test_inference_client_rejects_non_json_schema_response(tmp_path: Path) -> None:
    schema = {"type": "object"}
    response = SimpleNamespace(
        id="resp_not_json",
        output_text="not json at all",
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=lambda **_: response))
    logger = InferenceLogger(tmp_path / "logs")

    with pytest.raises(ValueError, match="response was not valid JSON"):
        InferenceClient(_build_config(), sdk_client=sdk_client, logger=logger).complete(
            prompt="Return JSON.",
            system="Return JSON.",
            schema=schema,
        )
    payload = json.loads(next((tmp_path / "logs").rglob("*.json")).read_text(encoding="utf-8"))
    assert payload["request_id"] == "resp_not_json"
    assert payload["estimated_cost_usd"] == estimate_cost("gpt-4o", 1, 1, _build_config().pricing)


def test_batch_builder_writes_openai_batch_jsonl_with_multiple_requests(tmp_path: Path) -> None:
    builder = BatchBuilder(_build_config())
    builder.add_request(
        custom_id="req-001",
        prompt="Summarize startup steps.",
        system="Return concise JSON.",
        model="gpt-4o",
        schema={
            "type": "object",
            "properties": {"title": {"type": "string"}},
            "required": ["title"],
            "additionalProperties": False,
        },
    )
    builder.add_request(
        custom_id="req-002",
        prompt="Summarize shutdown steps.",
        system="Return concise text.",
        model="gpt-4o",
    )

    output_path = builder.build_jsonl(tmp_path / "batch" / "requests.jsonl")

    lines = output_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    second = json.loads(lines[1])

    assert first["custom_id"] == "req-001"
    assert first["method"] == "POST"
    assert first["url"] == "/v1/responses"
    assert first["body"]["model"] == "gpt-4o"
    assert first["body"]["input"] == [
        {"role": "system", "content": "Return concise JSON."},
        {"role": "user", "content": "Summarize startup steps."},
    ]
    assert first["body"]["text"] == {
        "format": {
            "type": "json_schema",
            "name": "knowledge_forge_schema",
            "schema": {
                "type": "object",
                "properties": {"title": {"type": "string"}},
                "required": ["title"],
                "additionalProperties": False,
            },
            "strict": True,
        }
    }
    assert second["custom_id"] == "req-002"
    assert "text" not in second["body"]
    assert builder.request_count == 2


def test_batch_builder_enforces_max_batch_size() -> None:
    config = _build_config()
    config.batch.max_batch_size = 1
    builder = BatchBuilder(config)
    builder.add_request(
        custom_id="req-001",
        prompt="First prompt",
        system="First system",
        model="gpt-4o",
    )

    with pytest.raises(ValueError, match="max_batch_size"):
        builder.add_request(
            custom_id="req-002",
            prompt="Second prompt",
            system="Second system",
            model="gpt-4o",
        )


def test_batch_builder_rejects_duplicate_custom_ids() -> None:
    builder = BatchBuilder(_build_config())
    builder.add_request(
        custom_id="req-001",
        prompt="Prompt one",
        system="System one",
        model="gpt-4o",
    )

    with pytest.raises(ValueError, match="duplicate custom_id 'req-001' is not allowed in a batch"):
        builder.add_request(
            custom_id="req-001",
            prompt="Prompt two",
            system="System two",
            model="gpt-4o",
        )


def test_batch_builder_rejects_mixed_models_in_same_batch() -> None:
    builder = BatchBuilder(_build_config())
    builder.add_request(
        custom_id="req-001",
        prompt="Prompt one",
        system="System one",
        model="gpt-4o",
    )

    with pytest.raises(ValueError, match="same model"):
        builder.add_request(
            custom_id="req-002",
            prompt="Prompt two",
            system="System two",
            model="gpt-4.1",
        )


def test_submit_batch_uploads_jsonl_and_returns_typed_job(tmp_path: Path) -> None:
    builder = BatchBuilder(_build_config())
    builder.add_request(
        custom_id="req-001",
        prompt="Summarize startup steps.",
        system="Return concise text.",
        model="gpt-4o",
    )
    jsonl_path = builder.build_jsonl(tmp_path / "requests.jsonl")
    captured: dict[str, object] = {}

    def fake_files_create(*, file: object, purpose: str) -> object:
        captured["purpose"] = purpose
        captured["file_name"] = getattr(file, "name", None)
        captured["uploaded_bytes"] = getattr(file, "read")()
        return SimpleNamespace(id="file-batch-123")

    def fake_batches_create(*, input_file_id: str, endpoint: str, completion_window: str) -> object:
        captured["input_file_id"] = input_file_id
        captured["endpoint"] = endpoint
        captured["completion_window"] = completion_window
        return SimpleNamespace(
            id="batch-123",
            status="validating",
            created_at=1_765_698_600,
            input_file_id=input_file_id,
            request_counts=SimpleNamespace(total=1),
        )

    sdk_client = SimpleNamespace(
        files=SimpleNamespace(create=fake_files_create),
        batches=SimpleNamespace(create=fake_batches_create),
    )

    job = submit_batch(jsonl_path, _build_config(), sdk_client=sdk_client)

    assert isinstance(job, BatchJob)
    assert job.batch_id == "batch-123"
    assert job.status == "validating"
    assert job.input_file_id == "file-batch-123"
    assert job.request_count == 1
    assert job.created_at == datetime.fromtimestamp(1_765_698_600, tz=UTC)
    assert captured["purpose"] == "batch"
    assert captured["file_name"] == str(jsonl_path)
    assert captured["uploaded_bytes"] == jsonl_path.read_bytes()
    assert captured["endpoint"] == "/v1/responses"
    assert captured["completion_window"] == "24h"


def test_aggregate_costs_rolls_up_logs(tmp_path: Path) -> None:
    logger = InferenceLogger(tmp_path / "logs")
    logger.log(
        InferenceLogEntry(
            request_id="req-1",
            pipeline_run_id="run-1",
            mode="direct",
            model="gpt-4o",
            prompt_template="tests/direct",
            source_doc_id="doc-001",
            source_section_id="sec-001",
            input_tokens=100,
            output_tokens=25,
            estimated_cost_usd=0.0005,
            latency_ms=100,
            status="success",
            schema_valid=True,
            timestamp=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
        )
    )
    logger.log(
        InferenceLogEntry(
            request_id="req-2",
            pipeline_run_id="run-2",
            mode="direct",
            model="gpt-4o-mini",
            prompt_template="tests/direct",
            source_doc_id="doc-002",
            source_section_id="sec-002",
            input_tokens=200,
            output_tokens=50,
            estimated_cost_usd=0.00006,
            latency_ms=90,
            status="success",
            schema_valid=None,
            timestamp=datetime(2026, 4, 16, 9, 30, tzinfo=UTC),
        )
    )

    report = aggregate_costs(tmp_path / "logs")

    assert report.total.request_count == 2
    assert report.total.input_tokens == 300
    assert report.total.output_tokens == 75
    assert report.by_model["gpt-4o"].request_count == 1
    assert report.by_date["2026-04-15"].estimated_cost_usd == 0.0005
    assert report.by_pipeline_run["run-2"].output_tokens == 50


def test_aggregate_costs_groups_dates_in_utc(tmp_path: Path) -> None:
    logger = InferenceLogger(tmp_path / "logs")
    logger.log(
        InferenceLogEntry(
            request_id="req-tz",
            pipeline_run_id="run-tz",
            mode="direct",
            model="gpt-4o",
            prompt_template="tests/direct",
            source_doc_id="doc-001",
            source_section_id="sec-001",
            input_tokens=10,
            output_tokens=5,
            estimated_cost_usd=0.0001,
            latency_ms=50,
            status="success",
            schema_valid=True,
            timestamp=datetime.fromisoformat("2026-04-16T00:30:00+02:00"),
        )
    )

    report = aggregate_costs(tmp_path / "logs")

    assert "2026-04-15" in report.by_date


def test_iter_log_entries_skips_malformed_logs(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    valid_dir = log_dir / "2026-04-15"
    valid_dir.mkdir(parents=True, exist_ok=True)
    (valid_dir / "broken.json").write_text("{broken", encoding="utf-8")
    (valid_dir / "mismatch.json").write_text(json.dumps({"model": "gpt-4o"}), encoding="utf-8")
    valid_payload = InferenceLogEntry(
        request_id="req-valid",
        mode="direct",
        model="gpt-4o",
        input_tokens=1,
        output_tokens=1,
        estimated_cost_usd=0.0,
        latency_ms=1,
        status="success",
        timestamp=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
    )
    (valid_dir / "valid.json").write_text(valid_payload.model_dump_json(), encoding="utf-8")

    entries = list(iter_log_entries(log_dir))

    assert len(entries) == 1
    assert entries[0].request_id == "req-valid"


def test_inference_costs_cli_reports_aggregates(tmp_path: Path) -> None:
    logger = InferenceLogger(tmp_path / "logs")
    logger.log(
        InferenceLogEntry(
            request_id="req-cli",
            pipeline_run_id="run-cli",
            mode="direct",
            model="gpt-4o",
            prompt_template="tests/direct",
            source_doc_id="doc-001",
            source_section_id="sec-001",
            input_tokens=100,
            output_tokens=40,
            estimated_cost_usd=0.00065,
            latency_ms=50,
            status="success",
            schema_valid=True,
            timestamp=datetime(2026, 4, 15, 15, 0, tzinfo=UTC),
        )
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["inference", "costs", "--log-dir", str(tmp_path / "logs")])

    assert result.exit_code == 0
    assert "Requests: 1" in result.output
    assert "BY MODEL" in result.output
    assert "gpt-4o\t1\t100\t40\t0.000650" in result.output


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
            "pricing": {
                "gpt-4o": {
                    "input_per_million_tokens": 2.5,
                    "output_per_million_tokens": 10.0,
                },
                "gpt-4o-mini": {
                    "input_per_million_tokens": 0.15,
                    "output_per_million_tokens": 0.60,
                },
                "gpt-4.1": {
                    "input_per_million_tokens": 2.0,
                    "output_per_million_tokens": 8.0,
                },
                "gpt-4.1-mini": {
                    "input_per_million_tokens": 0.4,
                    "output_per_million_tokens": 1.6,
                },
            },
            "api_key": "test-secret",
        }
    )


def _build_config_without_pricing() -> InferenceConfig:
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
