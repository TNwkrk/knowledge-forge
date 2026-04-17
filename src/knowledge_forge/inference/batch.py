"""Batch JSONL construction, polling, and ingestion helpers for OpenAI inference."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema import validate
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.inference.config import InferenceConfig
from knowledge_forge.inference.cost import estimate_cost
from knowledge_forge.inference.logger import InferenceLogEntry, InferenceLogger, utc_now
from knowledge_forge.inference.openai_schema import prepare_openai_json_schema
from knowledge_forge.inference.retry import RetryPolicy, retry_transient
from knowledge_forge.intake.importer import get_data_dir

BATCH_ENDPOINT = "/v1/responses"
BATCH_METHOD = "POST"
BATCH_COMPLETION_WINDOW = "24h"
TERMINAL_BATCH_STATUSES = frozenset({"completed", "failed", "cancelled", "expired"})
MISSING = object()


class BatchRequestBody(BaseModel):
    """Normalized request body for a single Responses API batch item."""

    model_config = ConfigDict(extra="forbid")

    model: str = Field(min_length=1)
    input: list[dict[str, Any]] = Field(min_length=2)
    temperature: float = Field(ge=0.0, le=2.0)
    max_output_tokens: int = Field(gt=0)
    text: dict[str, Any] | None = None


class BatchRequest(BaseModel):
    """One JSONL line for the OpenAI Batch API."""

    model_config = ConfigDict(extra="forbid")

    custom_id: str = Field(min_length=1)
    method: str = Field(default=BATCH_METHOD)
    url: str = Field(default=BATCH_ENDPOINT)
    body: BatchRequestBody


class BatchJob(BaseModel):
    """Typed batch job metadata returned after submission."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    created_at: datetime
    input_file_id: str = Field(min_length=1)
    request_count: int = Field(ge=0)


class BatchStatus(BaseModel):
    """Normalized status view returned while polling a batch job."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    created_at: datetime
    request_count: int = Field(ge=0)
    output_file_id: str | None = None
    error_file_id: str | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None


class BatchSuccess(BaseModel):
    """Parsed successful item returned from batch ingestion."""

    model_config = ConfigDict(extra="forbid")

    custom_id: str = Field(min_length=1)
    request_id: str | None = None
    model: str = Field(min_length=1)
    response_text: str
    parsed_json: Any | None = None
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    schema_valid: bool | None = None


class BatchFailure(BaseModel):
    """Normalized failure reported for one batch item."""

    model_config = ConfigDict(extra="forbid")

    custom_id: str = Field(min_length=1)
    error_type: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retriable: bool = False
    status_code: int | None = None
    request_id: str | None = None


class BatchStats(BaseModel):
    """Roll-up counts for an ingested batch output."""

    model_config = ConfigDict(extra="forbid")

    total: int = Field(ge=0)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)


class BatchResults(BaseModel):
    """Successful and failed items produced after downloading a batch output."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1)
    successful: list[BatchSuccess] = Field(default_factory=list)
    failed: list[BatchFailure] = Field(default_factory=list)
    stats: BatchStats
    retry_custom_ids: list[str] = Field(default_factory=list)


class BatchBuilder:
    """Collect Responses API requests and write them as a JSONL batch file."""

    def __init__(self, config: InferenceConfig) -> None:
        self.config = config
        self._requests: list[BatchRequest] = []
        self._custom_ids: set[str] = set()
        self._model_name: str | None = None

    def add_request(
        self,
        custom_id: str,
        prompt: str,
        system: str,
        model: str,
        schema: dict[str, Any] | None = None,
    ) -> None:
        """Add a single request to the batch and validate batch-level constraints."""
        if custom_id in self._custom_ids:
            raise ValueError(f"duplicate custom_id '{custom_id}' is not allowed in a batch")
        if len(self._requests) >= self.config.batch.max_batch_size:
            raise ValueError(f"batch exceeds configured max_batch_size of {self.config.batch.max_batch_size}")
        if self._model_name is not None and model != self._model_name:
            raise ValueError("all requests in a batch must target the same model")

        body: dict[str, Any] = {
            "model": model,
            "input": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_tokens,
        }
        if schema is not None:
            request_schema = prepare_openai_json_schema(schema)
            body["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "knowledge_forge_schema",
                    "schema": request_schema,
                    "strict": True,
                }
            }

        request = BatchRequest(
            custom_id=custom_id,
            method=BATCH_METHOD,
            url=BATCH_ENDPOINT,
            body=BatchRequestBody.model_validate(body),
        )
        self._requests.append(request)
        self._custom_ids.add(custom_id)
        self._model_name = model

    def build_jsonl(self, output_path: Path) -> Path:
        """Write the accumulated requests to a JSONL file."""
        self._validate_ready_to_build()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines = [request.model_dump_json(exclude_none=True) for request in self._requests]
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return output_path

    @property
    def request_count(self) -> int:
        """Return the number of queued batch requests."""
        return len(self._requests)

    def _validate_ready_to_build(self) -> None:
        if not self._requests:
            raise ValueError("cannot build a batch JSONL file with no requests")
        if len(self._requests) > self.config.batch.max_batch_size:
            raise ValueError(f"batch exceeds configured max_batch_size of {self.config.batch.max_batch_size}")


def submit_batch(
    jsonl_path: Path,
    config: InferenceConfig,
    *,
    sdk_client: Any | None = None,
) -> BatchJob:
    """Upload a JSONL batch file and create a batch job via the OpenAI SDK."""
    if not jsonl_path.exists():
        raise FileNotFoundError(f"batch input file not found at '{jsonl_path}'")

    request_count = _count_requests(jsonl_path)
    if request_count == 0:
        raise ValueError("batch input file must contain at least one request")
    if request_count > config.batch.max_batch_size:
        raise ValueError(f"batch exceeds configured max_batch_size of {config.batch.max_batch_size}")

    client = sdk_client or OpenAI(api_key=config.api_key.get_secret_value())
    with jsonl_path.open("rb") as batch_file:
        uploaded = client.files.create(file=batch_file, purpose="batch")

    batch = client.batches.create(
        input_file_id=_read_attr(uploaded, "id"),
        endpoint=BATCH_ENDPOINT,
        completion_window=BATCH_COMPLETION_WINDOW,
    )
    created_at = datetime.fromtimestamp(int(_read_attr(batch, "created_at")), tz=UTC)
    request_counts = _read_attr(batch, "request_counts", default=None)
    total_requests = _read_attr(request_counts, "total", default=request_count)

    return BatchJob(
        batch_id=_read_attr(batch, "id"),
        status=_read_attr(batch, "status"),
        created_at=created_at,
        input_file_id=_read_attr(batch, "input_file_id", default=_read_attr(uploaded, "id")),
        request_count=int(total_requests),
    )


def poll_batch(
    batch_id: str,
    config: InferenceConfig,
    *,
    sdk_client: Any | None = None,
    retry_policy: RetryPolicy | None = None,
    poll_interval_seconds: int | None = None,
    max_poll_duration_seconds: int | None = None,
    sleep_fn: Any = time.sleep,
    monotonic_fn: Any = time.monotonic,
) -> BatchStatus:
    """Poll a batch until it reaches a terminal state or the timeout expires."""
    client = sdk_client or OpenAI(api_key=config.api_key.get_secret_value())
    interval = poll_interval_seconds if poll_interval_seconds is not None else config.batch.poll_interval_seconds
    max_duration = (
        max_poll_duration_seconds if max_poll_duration_seconds is not None else config.batch.max_poll_duration_seconds
    )
    started = monotonic_fn()

    while True:
        batch = retry_transient(
            lambda: client.batches.retrieve(batch_id),
            policy=retry_policy,
        )
        status = _normalize_batch_status(batch)
        if status.status in TERMINAL_BATCH_STATUSES:
            return status
        if monotonic_fn() - started >= max_duration:
            raise TimeoutError(f"batch '{batch_id}' did not complete within {max_duration} seconds")
        sleep_fn(interval)


def ingest_results(
    batch_id: str,
    config: InferenceConfig,
    *,
    schemas_by_custom_id: dict[str, dict[str, Any]] | None = None,
    sdk_client: Any | None = None,
    logger: InferenceLogger | None = None,
    data_dir: Path | None = None,
    retry_policy: RetryPolicy | None = None,
) -> BatchResults:
    """Download, parse, validate, and classify results for a completed batch."""
    client = sdk_client or OpenAI(api_key=config.api_key.get_secret_value())
    batch = retry_transient(
        lambda: client.batches.retrieve(batch_id),
        policy=retry_policy,
    )
    status = _normalize_batch_status(batch)
    if status.output_file_id is None and status.error_file_id is None:
        raise ValueError(f"batch '{batch_id}' does not have any result files to ingest")

    successes: list[BatchSuccess] = []
    failures: list[BatchFailure] = []
    seen_failure_ids: set[str] = set()
    resolved_logger = logger or InferenceLogger(get_data_dir(data_dir) / "inference_logs")

    if status.output_file_id is not None:
        output_text = retry_transient(
            lambda: _download_file_content(client, status.output_file_id),
            policy=retry_policy,
        )
        for line in _iter_jsonl(output_text):
            custom_id = str(line.get("custom_id") or "")
            response = line.get("response")
            if response is None:
                failure = _parse_error_payload(line)
                failures.append(failure)
                seen_failure_ids.add(failure.custom_id)
                _log_batch_failure(resolved_logger, batch_id, failure)
                continue

            response_body = response.get("body", response)
            try:
                success = _parse_success_payload(
                    custom_id=custom_id,
                    response_payload=response,
                    response_body=response_body,
                    schema=schemas_by_custom_id.get(custom_id) if schemas_by_custom_id else None,
                )
            except ValueError as exc:
                response_status_code = response.get("status_code")
                failure = _classify_failure(
                    custom_id=custom_id,
                    message=str(exc),
                    status_code=response_status_code if isinstance(response_status_code, int) else None,
                    request_id=_extract_request_id(response_body),
                )
                failures.append(failure)
                seen_failure_ids.add(failure.custom_id)
                _log_batch_failure(resolved_logger, batch_id, failure)
                continue

            successes.append(success)
            _log_batch_success(resolved_logger, batch_id, success, config)

    if status.error_file_id is not None:
        error_text = retry_transient(
            lambda: _download_file_content(client, status.error_file_id),
            policy=retry_policy,
        )
        for line in _iter_jsonl(error_text):
            failure = _parse_error_payload(line)
            if failure.custom_id in seen_failure_ids:
                continue
            failures.append(failure)
            seen_failure_ids.add(failure.custom_id)
            _log_batch_failure(resolved_logger, batch_id, failure)

    retry_custom_ids = [failure.custom_id for failure in failures if failure.retriable]
    return BatchResults(
        batch_id=batch_id,
        successful=successes,
        failed=failures,
        stats=BatchStats(total=len(successes) + len(failures), succeeded=len(successes), failed=len(failures)),
        retry_custom_ids=retry_custom_ids,
    )


def _count_requests(jsonl_path: Path) -> int:
    return sum(1 for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip())


def _normalize_batch_status(batch: Any) -> BatchStatus:
    request_counts = _read_attr(batch, "request_counts", default=None)
    total_requests = _read_attr(request_counts, "total", default=0)
    return BatchStatus(
        batch_id=_read_attr(batch, "id"),
        status=_read_attr(batch, "status"),
        created_at=datetime.fromtimestamp(int(_read_attr(batch, "created_at")), tz=UTC),
        request_count=int(total_requests),
        output_file_id=_read_attr(batch, "output_file_id", default=None),
        error_file_id=_read_attr(batch, "error_file_id", default=None),
        completed_at=_read_timestamp(batch, "completed_at"),
        failed_at=_read_timestamp(batch, "failed_at"),
    )


def _coerce_file_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, bytes):
        return content.decode("utf-8")

    text = getattr(content, "text", None)
    if isinstance(text, str):
        return text
    if callable(text):
        text_value = text()
        if isinstance(text_value, str):
            return text_value
        if isinstance(text_value, bytes):
            return text_value.decode("utf-8")

    read = getattr(content, "read", None)
    if callable(read):
        read_value = read()
        if isinstance(read_value, str):
            return read_value
        if isinstance(read_value, bytes):
            return read_value.decode("utf-8")

    return str(content)


def _download_file_content(client: Any, file_id: str) -> str:
    if hasattr(client.files, "retrieve_content"):
        return _coerce_file_content_text(client.files.retrieve_content(file_id))
    content = client.files.content(file_id)
    return _coerce_file_content_text(content)


def _iter_jsonl(payload: str) -> Iterator[dict[str, Any]]:
    for raw_line in StringIO(payload):
        line = raw_line.strip()
        if not line:
            continue
        parsed = json.loads(line)
        if not isinstance(parsed, dict):
            raise ValueError("batch output file contained a non-object JSONL line")
        yield parsed


def _parse_success_payload(
    *,
    custom_id: str,
    response_payload: dict[str, Any],
    response_body: dict[str, Any],
    schema: dict[str, Any] | None,
) -> BatchSuccess:
    status_code = response_payload.get("status_code")
    if isinstance(status_code, int) and status_code >= 400:
        raise ValueError(_error_message(response_body))

    response_text = _extract_response_text(response_body)
    parsed_json = None
    schema_valid: bool | None = None if schema is None else False
    if schema is not None:
        try:
            parsed_json = json.loads(response_text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"schema invalid: response was not valid JSON: {exc.msg}") from exc
        try:
            validate(instance=parsed_json, schema=schema)
        except JsonSchemaValidationError as exc:
            raise ValueError(f"schema invalid: {exc.message}") from exc
        schema_valid = True
    else:
        try:
            parsed_json = json.loads(response_text)
        except json.JSONDecodeError:
            parsed_json = None

    input_tokens, output_tokens = _extract_token_counts(response_body)
    return BatchSuccess(
        custom_id=custom_id,
        request_id=_extract_request_id(response_body),
        model=_extract_model(response_body),
        response_text=response_text,
        parsed_json=parsed_json,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        schema_valid=schema_valid,
    )


def _parse_error_payload(payload: dict[str, Any]) -> BatchFailure:
    custom_id = str(payload.get("custom_id") or "")
    error = payload.get("error")
    if isinstance(error, dict):
        message = _error_message(error)
        status_code = error.get("status_code")
        request_id = error.get("request_id")
    else:
        response = payload.get("response")
        response_body = response.get("body", response) if isinstance(response, dict) else {}
        message = _error_message(response_body or payload)
        status_code = response.get("status_code") if isinstance(response, dict) else None
        request_id = _extract_request_id(response_body) if isinstance(response_body, dict) else None
    return _classify_failure(
        custom_id=custom_id,
        message=message,
        status_code=status_code if isinstance(status_code, int) else None,
        request_id=request_id if isinstance(request_id, str) else None,
    )


def _classify_failure(
    *,
    custom_id: str,
    message: str,
    status_code: int | None = None,
    request_id: str | None = None,
) -> BatchFailure:
    lowered = message.lower()
    if status_code == 429 or "rate limit" in lowered:
        error_type = "rate_limit"
        retriable = True
    elif "content policy" in lowered or "safety system" in lowered:
        error_type = "content_policy"
        retriable = False
    elif "schema invalid" in lowered or "did not satisfy schema" in lowered:
        error_type = "schema_invalid"
        retriable = False
    elif status_code == 408 or "timeout" in lowered:
        error_type = "timeout"
        retriable = True
    elif status_code in {500, 503}:
        error_type = "server_error"
        retriable = True
    else:
        error_type = "unknown"
        retriable = False

    return BatchFailure(
        custom_id=custom_id or "unknown-custom-id",
        error_type=error_type,
        message=message,
        retriable=retriable,
        status_code=status_code,
        request_id=request_id,
    )


def _log_batch_success(
    logger: InferenceLogger,
    batch_id: str,
    success: BatchSuccess,
    config: InferenceConfig,
) -> None:
    estimated_cost = _safe_estimate_cost(
        model=success.model,
        input_tokens=success.input_tokens,
        output_tokens=success.output_tokens,
        pricing=config.pricing,
    )
    logger.log(
        InferenceLogEntry(
            request_id=success.request_id,
            batch_id=batch_id,
            mode="batch",
            model=success.model,
            source_section_id=success.custom_id,
            input_tokens=success.input_tokens,
            output_tokens=success.output_tokens,
            estimated_cost_usd=estimated_cost,
            latency_ms=0,
            status="success",
            schema_valid=success.schema_valid,
            timestamp=utc_now(),
        )
    )


def _log_batch_failure(logger: InferenceLogger, batch_id: str, failure: BatchFailure) -> None:
    logger.log(
        InferenceLogEntry(
            request_id=failure.request_id,
            batch_id=batch_id,
            mode="batch",
            model="unknown",
            source_section_id=failure.custom_id,
            input_tokens=0,
            output_tokens=0,
            estimated_cost_usd=0.0,
            latency_ms=0,
            status="error",
            schema_valid=False if failure.error_type == "schema_invalid" else None,
            error=failure.message,
            timestamp=utc_now(),
        )
    )


def _read_attr(value: Any, name: str, *, default: Any = MISSING) -> Any:
    if value is None:
        if default is not MISSING:
            return default
        raise ValueError(f"expected batch response to include '{name}'")

    if isinstance(value, dict):
        result = value.get(name, default)
    else:
        result = getattr(value, name, default)

    if result is MISSING:
        raise ValueError(f"expected batch response to include '{name}'")
    return result


def _read_timestamp(value: Any, name: str) -> datetime | None:
    raw_timestamp = _read_attr(value, name, default=None)
    if raw_timestamp is None:
        return None
    return datetime.fromtimestamp(int(raw_timestamp), tz=UTC)


def _extract_response_text(response: dict[str, Any]) -> str:
    output_text = response.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = response.get("output", [])
    segments: list[str] = []
    for item in output or []:
        if not isinstance(item, dict):
            continue
        for block in item.get("content", []) or []:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                segments.append(block["text"])
    if segments:
        return "".join(segments)
    raise ValueError("batch response did not contain any text output")


def _extract_token_counts(response: dict[str, Any]) -> tuple[int, int]:
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        return 0, 0
    return int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0), int(
        usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0
    )


def _extract_model(response: dict[str, Any]) -> str:
    return str(response.get("model") or "unknown")


def _extract_request_id(response: dict[str, Any]) -> str | None:
    request_id = response.get("id")
    return str(request_id) if request_id is not None else None


def _error_message(payload: dict[str, Any]) -> str:
    if "message" in payload and isinstance(payload["message"], str):
        return payload["message"]
    error = payload.get("error")
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"]
    return "unknown batch error"


def _safe_estimate_cost(
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    pricing: dict[str, Any],
) -> float:
    try:
        return estimate_cost(model, input_tokens, output_tokens, pricing)
    except ValueError:
        return 0.0
