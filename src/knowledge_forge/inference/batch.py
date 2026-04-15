"""Batch JSONL construction and submission helpers for OpenAI inference."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.inference.config import InferenceConfig

BATCH_ENDPOINT = "/v1/responses"
BATCH_METHOD = "POST"
BATCH_COMPLETION_WINDOW = "24h"
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
            body["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "knowledge_forge_schema",
                    "schema": schema,
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


def _count_requests(jsonl_path: Path) -> int:
    return sum(1 for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line.strip())


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
