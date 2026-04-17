"""Helpers for adapting JSON Schema to the OpenAI Responses API subset."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def prepare_openai_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Return an OpenAI-compatible JSON Schema without mutating the input."""
    return _normalize_schema(deepcopy(schema))


def _normalize_schema(schema: object) -> object:
    if isinstance(schema, dict):
        normalized = {key: _normalize_schema(value) for key, value in schema.items()}
        properties = normalized.get("properties")
        if isinstance(properties, dict):
            required = normalized.get("required")
            required_names = set(required) if isinstance(required, list) else set()
            normalized_properties: dict[str, Any] = {}
            all_names = list(properties.keys())
            for name, property_schema in properties.items():
                if name not in required_names:
                    normalized_properties[name] = _make_nullable(property_schema)
                else:
                    normalized_properties[name] = property_schema
            normalized["properties"] = normalized_properties
            normalized["required"] = all_names
        return normalized
    if isinstance(schema, list):
        return [_normalize_schema(item) for item in schema]
    return schema


def _make_nullable(schema: object) -> object:
    if not isinstance(schema, dict):
        return schema

    schema_type = schema.get("type")
    if isinstance(schema_type, str):
        if schema_type == "null":
            return schema
        updated = dict(schema)
        updated["type"] = [schema_type, "null"]
        return updated
    if isinstance(schema_type, list):
        if "null" in schema_type:
            return schema
        updated = dict(schema)
        updated["type"] = [*schema_type, "null"]
        return updated

    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        if any(isinstance(option, dict) and option.get("type") == "null" for option in any_of):
            return schema
        updated = dict(schema)
        updated["anyOf"] = [*any_of, {"type": "null"}]
        return updated

    one_of = schema.get("oneOf")
    if isinstance(one_of, list):
        if any(isinstance(option, dict) and option.get("type") == "null" for option in one_of):
            return schema
        updated = dict(schema)
        updated["oneOf"] = [*one_of, {"type": "null"}]
        return updated

    updated = dict(schema)
    updated["anyOf"] = [schema, {"type": "null"}]
    updated.pop("default", None)
    return updated
