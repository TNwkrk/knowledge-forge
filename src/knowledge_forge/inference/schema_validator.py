"""Helpers for validating schema-bound inference responses."""

from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator
from pydantic import BaseModel, ConfigDict, Field


class ValidationResult(BaseModel):
    """Validation outcome for a JSON payload against a schema."""

    model_config = ConfigDict(extra="forbid")

    valid: bool
    errors: list[str] = Field(default_factory=list)
    repaired: bool = False


def validate_response(response_json: object, schema: dict[str, Any]) -> ValidationResult:
    """Validate a decoded JSON response against a JSON Schema."""
    validator = Draft202012Validator(schema)
    failures = sorted(validator.iter_errors(response_json), key=_error_sort_key)
    if not failures:
        return ValidationResult(valid=True)

    return ValidationResult(valid=False, errors=[_format_error(error) for error in failures])


def _format_error(error: object) -> str:
    path = ""
    json_path = getattr(error, "json_path", "$")
    if isinstance(json_path, str):
        path = json_path
    message = getattr(error, "message", "schema validation failed")
    return f"{path}: {message}"


def _error_sort_key(error: object) -> tuple[str, str]:
    json_path = getattr(error, "json_path", "$")
    message = getattr(error, "message", "")
    return str(json_path), str(message)
