"""Repair loop for invalid extraction responses."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.inference import InferenceClient
from knowledge_forge.inference.schema_validator import validate_response


class RepairResult(BaseModel):
    """Result of attempting to repair an invalid extraction response."""

    model_config = ConfigDict(extra="forbid")

    valid: bool
    repaired_json: Any | None = None
    strategy: str = Field(min_length=1)
    attempts: int = Field(ge=0)
    errors: list[str] = Field(default_factory=list)
    flagged_for_review: bool = False
    relaxed_schema: bool = False
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)


def repair_extraction(
    invalid_response: object,
    schema: dict[str, Any],
    original_prompt: str,
    *,
    client: InferenceClient | None = None,
    system: str = "",
    model: str | None = None,
    prompt_template: str | None = None,
    source_doc_id: str | None = None,
    source_section_id: str | None = None,
    max_attempts: int = 2,
) -> RepairResult:
    """Attempt a bounded repair loop before flagging manual review."""
    errors = [str(invalid_response)]
    if client is None or max_attempts <= 0:
        return RepairResult(
            valid=False,
            strategy="manual_review",
            attempts=0,
            errors=errors,
            flagged_for_review=True,
        )

    attempts = 0
    if attempts < max_attempts:
        attempts += 1
        try:
            result = client.complete(
                prompt=_build_repair_prompt(original_prompt=original_prompt, invalid_response=invalid_response),
                system=system,
                model=model,
                schema=schema,
                prompt_template=_repair_prompt_template(prompt_template, "reprompt"),
                source_doc_id=source_doc_id,
                source_section_id=source_section_id,
            )
            return RepairResult(
                valid=True,
                repaired_json=result.parsed_json,
                strategy="reprompt",
                attempts=attempts,
                errors=errors,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
            )
        except Exception as exc:  # pragma: no cover - behavior asserted via final outcome
            errors.append(str(exc))

    if attempts < max_attempts:
        attempts += 1
        relaxed_schema = relax_schema(schema)
        try:
            result = client.complete(
                prompt=_build_relaxed_repair_prompt(original_prompt=original_prompt, invalid_response=invalid_response),
                system=system,
                model=model,
                schema=relaxed_schema,
                prompt_template=_repair_prompt_template(prompt_template, "relaxed"),
                source_doc_id=source_doc_id,
                source_section_id=source_section_id,
            )
            validation = validate_response(result.parsed_json, schema)
            if validation.valid:
                return RepairResult(
                    valid=True,
                    repaired_json=result.parsed_json,
                    strategy="relaxed_schema",
                    attempts=attempts,
                    errors=errors,
                    relaxed_schema=True,
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                )
            errors.extend(validation.errors)
        except Exception as exc:  # pragma: no cover - behavior asserted via final outcome
            errors.append(str(exc))

    return RepairResult(
        valid=False,
        strategy="manual_review",
        attempts=attempts,
        errors=errors,
        flagged_for_review=True,
    )


def relax_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Relax non-essential strictness so the model can self-correct shape issues."""
    relaxed: dict[str, Any] = {}
    for key, value in schema.items():
        if key in {"required", "additionalProperties", "minItems", "maxItems", "minLength", "maxLength", "enum"}:
            continue
        if key == "properties" and isinstance(value, dict):
            relaxed[key] = {
                name: relax_schema(prop) if isinstance(prop, dict) else prop for name, prop in value.items()
            }
            continue
        if key == "items" and isinstance(value, dict):
            relaxed[key] = relax_schema(value)
            continue
        if isinstance(value, dict):
            relaxed[key] = relax_schema(value)
            continue
        if isinstance(value, list):
            relaxed[key] = [relax_schema(item) if isinstance(item, dict) else item for item in value]
            continue
        relaxed[key] = value
    return relaxed


def _build_repair_prompt(*, original_prompt: str, invalid_response: object) -> str:
    return (
        "The previous extraction response was invalid. Correct it and return only JSON that matches the schema.\n\n"
        f"Original extraction prompt:\n{original_prompt}\n\n"
        f"Invalid response or validation failure:\n{invalid_response}"
    )


def _build_relaxed_repair_prompt(*, original_prompt: str, invalid_response: object) -> str:
    return (
        "The previous extraction response still failed strict validation. Rebuild the response from scratch, "
        "preserve only information supported by the source section, and return only JSON.\n\n"
        f"Original extraction prompt:\n{original_prompt}\n\n"
        f"Prior invalid response or validation failure:\n{invalid_response}"
    )


def _repair_prompt_template(prompt_template: str | None, suffix: str) -> str | None:
    if prompt_template is None:
        return None
    return f"{prompt_template}/{suffix}"
