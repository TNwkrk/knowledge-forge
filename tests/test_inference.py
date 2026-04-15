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
    BatchResults,
    BatchStatus,
    InferenceClient,
    InferenceConfig,
    InferenceLogEntry,
    InferenceLogger,
    RetryPolicy,
    aggregate_costs,
    estimate_cost,
    ingest_results,
    poll_batch,
    retry_transient,
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


def test_retry_transient_retries_rate_limit_then_succeeds() -> None:
    attempts = {"count": 0}
    delays: list[float] = []

    class FakeRateLimitError(RuntimeError):
        status_code = 429

    def operation() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise FakeRateLimitError("rate limit exceeded")
        return "ok"

    result = retry_transient(
        operation,
        policy=RetryPolicy(max_retries=3, initial_delay_seconds=0.5, jitter_seconds=0.0),
        sleep_fn=delays.append,
    )

    assert result == "ok"
    assert attempts["count"] == 3
    assert delays == [0.5, 1.0]


def test_inference_client_retries_transient_direct_errors(tmp_path: Path) -> None:
    attempts = {"count": 0}

    class FakeTransientError(RuntimeError):
        status_code = 503

    response = SimpleNamespace(
        id="resp_retry",
        model="gpt-4o",
        output_text="Recovered answer",
        usage=SimpleNamespace(input_tokens=6, output_tokens=2),
    )

    def fake_create(**_: object) -> object:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise FakeTransientError("temporarily unavailable")
        return response

    sdk_client = SimpleNamespace(responses=SimpleNamespace(create=fake_create))
    logger = InferenceLogger(tmp_path / "logs")

    result = InferenceClient(_build_config(), sdk_client=sdk_client, logger=logger).complete(
        prompt="Retry this request.",
        system="Answer clearly.",
        retry_policy=RetryPolicy(max_retries=2, initial_delay_seconds=0.0),
    )

    assert result.response_text == "Recovered answer"
    assert attempts["count"] == 2


def test_poll_batch_detects_completion_after_in_progress() -> None:
    responses = iter(
        [
            SimpleNamespace(
                id="batch-123",
                status="in_progress",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=2),
                output_file_id=None,
                error_file_id=None,
                completed_at=None,
                failed_at=None,
            ),
            SimpleNamespace(
                id="batch-123",
                status="completed",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=2),
                output_file_id="file-out-123",
                error_file_id="file-err-123",
                completed_at=1_765_698_720,
                failed_at=None,
            ),
        ]
    )
    sdk_client = SimpleNamespace(batches=SimpleNamespace(retrieve=lambda _batch_id: next(responses)))
    sleeps: list[int] = []

    status = poll_batch(
        "batch-123",
        _build_config(),
        sdk_client=sdk_client,
        poll_interval_seconds=5,
        sleep_fn=sleeps.append,
        monotonic_fn=iter([0.0, 1.0]).__next__,
    )

    assert isinstance(status, BatchStatus)
    assert status.status == "completed"
    assert status.output_file_id == "file-out-123"
    assert sleeps == [5]


def test_poll_batch_returns_failed_terminal_status() -> None:
    sdk_client = SimpleNamespace(
        batches=SimpleNamespace(
            retrieve=lambda _batch_id: SimpleNamespace(
                id="batch-999",
                status="failed",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=1),
                output_file_id=None,
                error_file_id="file-err-999",
                completed_at=None,
                failed_at=1_765_698_660,
            )
        )
    )

    status = poll_batch("batch-999", _build_config(), sdk_client=sdk_client)

    assert status.status == "failed"
    assert status.error_file_id == "file-err-999"


def test_poll_batch_respects_zero_timeout_parameters_without_sleeping() -> None:
    retrieve_calls: list[str] = []

    def _retrieve(batch_id: str) -> SimpleNamespace:
        retrieve_calls.append(batch_id)
        return SimpleNamespace(
            id="batch-zero",
            status="in_progress",
            created_at=1_765_698_600,
            request_counts=SimpleNamespace(total=1),
            output_file_id=None,
            error_file_id=None,
            completed_at=None,
            failed_at=None,
        )

    sdk_client = SimpleNamespace(batches=SimpleNamespace(retrieve=_retrieve))
    sleep_calls: list[int] = []

    with pytest.raises(TimeoutError):
        poll_batch(
            "batch-zero",
            _build_config(),
            sdk_client=sdk_client,
            poll_interval_seconds=0,
            max_poll_duration_seconds=0,
            sleep_fn=sleep_calls.append,
            monotonic_fn=iter([0.0, 0.0]).__next__,
        )

    assert sleep_calls == []
    assert retrieve_calls == ["batch-zero"]


def test_ingest_results_parses_successes_classifies_failures_and_logs(tmp_path: Path) -> None:
    output_lines = "\n".join(
        [
            json.dumps(
                {
                    "custom_id": "req-success",
                    "response": {
                        "status_code": 200,
                        "body": {
                            "id": "resp-success",
                            "model": "gpt-4o",
                            "output_text": json.dumps({"title": "Startup"}),
                            "usage": {"input_tokens": 20, "output_tokens": 8},
                        },
                    },
                }
            ),
            json.dumps(
                {
                    "custom_id": "req-invalid",
                    "response": {
                        "status_code": 200,
                        "body": {
                            "id": "resp-invalid",
                            "model": "gpt-4o",
                            "output_text": json.dumps({"steps": ["Missing title"]}),
                            "usage": {"input_tokens": 10, "output_tokens": 4},
                        },
                    },
                }
            ),
        ]
    )
    error_lines = "\n".join(
        [
            json.dumps(
                {
                    "custom_id": "req-rate-limit",
                    "error": {"message": "Rate limit exceeded", "status_code": 429},
                }
            ),
            json.dumps(
                {
                    "custom_id": "req-policy",
                    "error": {"message": "Blocked by content policy", "status_code": 400},
                }
            ),
        ]
    )
    sdk_client = SimpleNamespace(
        batches=SimpleNamespace(
            retrieve=lambda _batch_id: SimpleNamespace(
                id="batch-456",
                status="completed",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=4),
                output_file_id="file-out-456",
                error_file_id="file-err-456",
                completed_at=1_765_698_900,
                failed_at=None,
            )
        ),
        files=SimpleNamespace(
            retrieve_content=lambda file_id: output_lines if file_id == "file-out-456" else error_lines,
        ),
    )

    results = ingest_results(
        "batch-456",
        _build_config(),
        schemas_by_custom_id={
            "req-success": {
                "type": "object",
                "properties": {"title": {"type": "string"}},
                "required": ["title"],
                "additionalProperties": False,
            },
            "req-invalid": {
                "type": "object",
                "properties": {"title": {"type": "string"}},
                "required": ["title"],
                "additionalProperties": False,
            },
        },
        sdk_client=sdk_client,
        data_dir=tmp_path,
    )

    assert isinstance(results, BatchResults)
    assert results.stats.total == 4
    assert results.stats.succeeded == 1
    assert results.stats.failed == 3
    assert results.successful[0].custom_id == "req-success"
    assert {failure.custom_id: failure.error_type for failure in results.failed} == {
        "req-invalid": "schema_invalid",
        "req-rate-limit": "rate_limit",
        "req-policy": "content_policy",
    }
    assert results.retry_custom_ids == ["req-rate-limit"]
    log_entries = list(iter_log_entries(tmp_path / "inference_logs"))
    assert len(log_entries) == 4
    assert sum(1 for entry in log_entries if entry.status == "success") == 1
    assert sum(1 for entry in log_entries if entry.status == "error") == 3


def test_ingest_results_classifies_http_status_failures_from_output(tmp_path: Path) -> None:
    output_lines = json.dumps(
        {
            "custom_id": "req-server-error",
            "response": {
                "status_code": 503,
                "body": {
                    "id": "resp-server-error",
                    "error": {"message": "Upstream service unavailable"},
                },
            },
        }
    )
    sdk_client = SimpleNamespace(
        batches=SimpleNamespace(
            retrieve=lambda _batch_id: SimpleNamespace(
                id="batch-503",
                status="completed",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=1),
                output_file_id="file-out-503",
                error_file_id=None,
                completed_at=1_765_698_900,
                failed_at=None,
            )
        ),
        files=SimpleNamespace(retrieve_content=lambda _file_id: output_lines),
    )

    results = ingest_results("batch-503", _build_config(), sdk_client=sdk_client, data_dir=tmp_path)

    assert results.stats.total == 1
    assert results.stats.succeeded == 0
    assert results.stats.failed == 1
    failure = results.failed[0]
    assert failure.custom_id == "req-server-error"
    assert failure.error_type == "server_error"
    assert failure.retriable is True
    assert failure.status_code == 503
    assert results.retry_custom_ids == ["req-server-error"]


def test_ingest_results_decodes_bytes_batch_file_content(tmp_path: Path) -> None:
    output_lines = json.dumps(
        {
            "custom_id": "req-bytes",
            "response": {
                "status_code": 200,
                "body": {
                    "id": "resp-bytes",
                    "model": "gpt-4o",
                    "output_text": json.dumps({"title": "From bytes"}),
                    "usage": {"input_tokens": 3, "output_tokens": 2},
                },
            },
        }
    ).encode("utf-8")
    sdk_client = SimpleNamespace(
        batches=SimpleNamespace(
            retrieve=lambda _batch_id: SimpleNamespace(
                id="batch-bytes",
                status="completed",
                created_at=1_765_698_600,
                request_counts=SimpleNamespace(total=1),
                output_file_id="file-out-bytes",
                error_file_id=None,
                completed_at=1_765_698_900,
                failed_at=None,
            )
        ),
        files=SimpleNamespace(retrieve_content=lambda _file_id: output_lines),
    )

    results = ingest_results("batch-bytes", _build_config(), sdk_client=sdk_client, data_dir=tmp_path)

    assert results.stats.total == 1
    assert results.stats.succeeded == 1
    assert results.stats.failed == 0
    assert results.successful[0].custom_id == "req-bytes"
    assert results.successful[0].parsed_json == {"title": "From bytes"}


def test_inference_batch_status_cli_reports_terminal_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "inference.yaml"
    _write_test_config(config_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-secret")

    def fake_poll_batch(batch_id: str, config: InferenceConfig) -> BatchStatus:
        assert batch_id == "batch-cli"
        assert config.default_model == "gpt-4o"
        return BatchStatus(
            batch_id=batch_id,
            status="completed",
            created_at=datetime(2026, 4, 15, 12, 0, tzinfo=UTC),
            request_count=3,
            output_file_id="file-out-cli",
            error_file_id=None,
            completed_at=datetime(2026, 4, 15, 12, 5, tzinfo=UTC),
            failed_at=None,
        )

    monkeypatch.setattr("knowledge_forge.cli.poll_batch", fake_poll_batch)

    runner = CliRunner()
    result = runner.invoke(cli, ["inference", "batch-status", "batch-cli", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Status: completed" in result.output
    assert "Output file: file-out-cli" in result.output


def test_inference_batch_ingest_cli_reports_retry_queue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "inference.yaml"
    _write_test_config(config_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-secret")

    def fake_ingest_results(batch_id: str, config: InferenceConfig, data_dir: Path | None = None) -> BatchResults:
        assert batch_id == "batch-cli"
        assert config.default_model == "gpt-4o"
        assert data_dir == tmp_path / "data"
        return BatchResults(
            batch_id=batch_id,
            successful=[],
            failed=[],
            stats={"total": 2, "succeeded": 1, "failed": 1},
            retry_custom_ids=["req-001"],
        )

    monkeypatch.setattr("knowledge_forge.cli.ingest_results", fake_ingest_results)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "inference",
            "batch-ingest",
            "batch-cli",
            "--config",
            str(config_path),
            "--data-dir",
            str(tmp_path / "data"),
        ],
    )

    assert result.exit_code == 0
    assert "Total: 2" in result.output
    assert "Retry custom_ids: req-001" in result.output


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


def _write_test_config(path: Path) -> None:
    path.write_text(
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
