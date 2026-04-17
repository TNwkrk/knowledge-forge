"""OpenAI inference client wrapper for direct requests."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from time import perf_counter
from typing import Any

from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.inference.config import InferenceConfig
from knowledge_forge.inference.cost import estimate_cost
from knowledge_forge.inference.logger import InferenceLogEntry, InferenceLogger, utc_now
from knowledge_forge.inference.openai_schema import prepare_openai_json_schema
from knowledge_forge.inference.retry import RetryPolicy, retry_transient
from knowledge_forge.inference.schema_validator import validate_response
from knowledge_forge.intake.importer import get_data_dir

MAX_JSON_ERROR_SNIPPET_LENGTH = 200
LOGGER = logging.getLogger(__name__)


class InferenceResult(BaseModel):
    """Typed response details returned from a direct inference call."""

    model_config = ConfigDict(extra="forbid")

    response_text: str
    parsed_json: Any | None = None
    model_used: str
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    latency_ms: int = Field(ge=0)
    request_id: str | None = None


class InferenceClient:
    """Thin wrapper around the OpenAI SDK for direct requests."""

    def __init__(
        self,
        config: InferenceConfig,
        *,
        sdk_client: Any | None = None,
        logger: InferenceLogger | None = None,
        data_dir: Path | None = None,
    ) -> None:
        self.config = config
        self._client = sdk_client or OpenAI(api_key=config.api_key.get_secret_value())
        self._logger = logger or InferenceLogger(get_data_dir(data_dir) / "inference_logs")

    def complete(
        self,
        prompt: str,
        system: str,
        model: str | None = None,
        schema: dict[str, Any] | None = None,
        *,
        prompt_template: str | None = None,
        source_doc_id: str | None = None,
        source_section_id: str | None = None,
        batch_id: str | None = None,
        pipeline_run_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> InferenceResult:
        """Send a direct request and normalize the OpenAI response."""
        request_model = model or self.config.default_model
        request_args = {
            "model": request_model,
            "input": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_tokens,
        }
        if schema is not None:
            request_schema = prepare_openai_json_schema(schema)
            request_args["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "knowledge_forge_schema",
                    "schema": request_schema,
                    "strict": True,
                }
            }

        started = perf_counter()
        timestamp = utc_now()
        response: Any | None = None
        latency_ms = 0
        input_tokens = 0
        output_tokens = 0
        model_used = request_model
        request_id: str | None = None
        response_text = ""
        schema_valid: bool | None = None if schema is None else False

        try:
            response = retry_transient(
                lambda: self._client.responses.create(**request_args),
                policy=retry_policy,
            )
            latency_ms = int((perf_counter() - started) * 1000)
            response_text = _extract_response_text(response)
            input_tokens, output_tokens = _extract_token_counts(response)
            model_used = _extract_model_used(response, request_model)
            request_id = _extract_request_id(response)

            parsed_json = None
            if schema is not None:
                try:
                    parsed_json = json.loads(response_text)
                except json.JSONDecodeError as exc:
                    response_length = len(response_text)
                    snippet = response_text[:MAX_JSON_ERROR_SNIPPET_LENGTH]
                    if response_length > MAX_JSON_ERROR_SNIPPET_LENGTH:
                        snippet += "..."
                    raise ValueError(f"response was not valid JSON: {exc.msg}. Output snippet: {snippet!r}") from exc
                validation = validate_response(parsed_json, schema)
                if not validation.valid:
                    snippet = response_text[:MAX_JSON_ERROR_SNIPPET_LENGTH]
                    if len(response_text) > MAX_JSON_ERROR_SNIPPET_LENGTH:
                        snippet += "..."
                    joined_errors = "; ".join(validation.errors)
                    raise ValueError(f"response did not satisfy schema: {joined_errors}. Output snippet: {snippet!r}")
                schema_valid = True

            estimated_cost_usd = _safe_estimate_cost(
                model=model_used,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                pricing=self.config.pricing,
            )
            result = InferenceResult(
                response_text=response_text,
                parsed_json=parsed_json,
                model_used=model_used,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
                request_id=request_id,
            )
            self._safe_log(
                InferenceLogEntry(
                    request_id=request_id,
                    batch_id=batch_id,
                    pipeline_run_id=pipeline_run_id,
                    mode="direct",
                    model=model_used,
                    prompt_template=prompt_template,
                    source_doc_id=source_doc_id,
                    source_section_id=source_section_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    estimated_cost_usd=estimated_cost_usd,
                    latency_ms=latency_ms,
                    status="success",
                    schema_valid=schema_valid,
                    timestamp=timestamp,
                )
            )
            return result
        except Exception as exc:
            if latency_ms == 0:
                latency_ms = int((perf_counter() - started) * 1000)
            if response is not None:
                input_tokens, output_tokens = _extract_token_counts(response)
                model_used = _extract_model_used(response, request_model)
                request_id = _extract_request_id(response)
            estimated_cost_usd = _safe_estimate_cost(
                model=model_used,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                pricing=self.config.pricing,
            )
            self._safe_log(
                InferenceLogEntry(
                    request_id=request_id,
                    batch_id=batch_id,
                    pipeline_run_id=pipeline_run_id,
                    mode="direct",
                    model=model_used,
                    prompt_template=prompt_template,
                    source_doc_id=source_doc_id,
                    source_section_id=source_section_id,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    estimated_cost_usd=estimated_cost_usd,
                    latency_ms=latency_ms,
                    status="error",
                    schema_valid=schema_valid,
                    error=str(exc),
                    timestamp=timestamp,
                )
            )
            raise

    def _safe_log(self, entry: InferenceLogEntry) -> None:
        try:
            self._logger.log(entry)
        except OSError as exc:
            LOGGER.warning("failed to persist inference log entry: %s", exc)
            return


def _extract_response_text(response: Any) -> str:
    """Extract textual output from SDK or mocked response objects."""
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    if isinstance(response, dict):
        output_text = response.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text
        output = response.get("output", [])
    else:
        output = getattr(response, "output", [])

    segments: list[str] = []
    for item in output or []:
        content = getattr(item, "content", None)
        if content is None and isinstance(item, dict):
            content = item.get("content", [])
        for block in content or []:
            text_value = getattr(block, "text", None)
            if text_value is None and isinstance(block, dict):
                text_value = block.get("text")
            if isinstance(text_value, str):
                segments.append(text_value)

    if not segments:
        raise ValueError("OpenAI response did not contain any text output")
    return "".join(segments)


def _extract_token_counts(response: Any) -> tuple[int, int]:
    """Normalize token usage fields across SDK response shapes."""
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    if usage is None:
        return 0, 0

    input_tokens = getattr(usage, "input_tokens", None)
    output_tokens = getattr(usage, "output_tokens", None)
    if isinstance(usage, dict):
        input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
        output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))

    return int(input_tokens or 0), int(output_tokens or 0)


def _extract_model_used(response: Any, fallback: str) -> str:
    """Return the model reported by the SDK, or the requested fallback."""
    if isinstance(response, dict):
        return str(response.get("model", fallback))
    return str(getattr(response, "model", fallback))


def _extract_request_id(response: Any) -> str | None:
    if isinstance(response, dict):
        request_id = response.get("id")
    else:
        request_id = getattr(response, "id", None)
    return str(request_id) if request_id is not None else None


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
