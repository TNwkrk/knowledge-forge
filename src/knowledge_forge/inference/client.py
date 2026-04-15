"""OpenAI inference client wrapper for direct requests."""

from __future__ import annotations

import json
from time import perf_counter
from typing import Any

from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema import validate
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.inference.config import InferenceConfig

MAX_JSON_ERROR_SNIPPET_LENGTH = 200


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

    def __init__(self, config: InferenceConfig, *, sdk_client: Any | None = None) -> None:
        self.config = config
        self._client = sdk_client or OpenAI(api_key=config.api_key.get_secret_value())

    def complete(
        self,
        prompt: str,
        system: str,
        model: str | None = None,
        schema: dict[str, Any] | None = None,
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
            request_args["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "knowledge_forge_schema",
                    "schema": schema,
                    "strict": True,
                }
            }

        started = perf_counter()
        response = self._client.responses.create(**request_args)
        latency_ms = int((perf_counter() - started) * 1000)

        response_text = _extract_response_text(response)
        parsed_json = None
        if schema is not None:
            try:
                parsed_json = json.loads(response_text)
            except json.JSONDecodeError as exc:
                snippet = response_text[:MAX_JSON_ERROR_SNIPPET_LENGTH]
                if len(response_text) > MAX_JSON_ERROR_SNIPPET_LENGTH:
                    snippet += "..."
                raise ValueError(f"response was not valid JSON: {exc.msg}. Output snippet: {snippet!r}") from exc
            try:
                validate(instance=parsed_json, schema=schema)
            except JsonSchemaValidationError as exc:
                raise ValueError(f"response did not satisfy schema: {exc.message}") from exc

        input_tokens, output_tokens = _extract_token_counts(response)
        return InferenceResult(
            response_text=response_text,
            parsed_json=parsed_json,
            model_used=_extract_model_used(response, request_model),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            request_id=getattr(response, "id", None),
        )


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
