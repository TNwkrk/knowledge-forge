"""Tests for extraction repair helpers."""

from __future__ import annotations

from types import SimpleNamespace

from knowledge_forge.extract.repair import relax_schema, repair_extraction


class _RepairClient:
    def __init__(self, responses: list[object]) -> None:
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def complete(self, **kwargs: object) -> object:
        self.calls.append(kwargs)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_repair_extraction_reprompts_before_relaxing_schema() -> None:
    client = _RepairClient(
        [
            SimpleNamespace(parsed_json={"records": [{"title": "Startup"}]}, input_tokens=10, output_tokens=20),
        ]
    )

    result = repair_extraction(
        "missing title",
        {
            "type": "object",
            "properties": {
                "records": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"title": {"type": "string"}},
                        "required": ["title"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["records"],
            "additionalProperties": False,
        },
        "original prompt",
        client=client,
        system="Return JSON.",
        prompt_template="extraction/procedure",
    )

    assert result.valid is True
    assert result.strategy == "reprompt"
    assert result.attempts == 1
    assert client.calls[0]["prompt_template"] == "extraction/procedure/reprompt"


def test_repair_extraction_flags_manual_review_after_failed_attempts() -> None:
    client = _RepairClient([ValueError("still invalid"), ValueError("still invalid again")])

    result = repair_extraction(
        "bad output",
        {"type": "object"},
        "original prompt",
        client=client,
        system="Return JSON.",
        prompt_template="extraction/procedure",
        max_attempts=2,
    )

    assert result.valid is False
    assert result.flagged_for_review is True
    assert result.strategy == "manual_review"
    assert result.attempts == 2


def test_relax_schema_drops_strict_constraints() -> None:
    relaxed = relax_schema(
        {
            "type": "object",
            "properties": {
                "title": {"type": "string", "minLength": 1},
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string", "enum": ["a", "b"]},
                },
            },
            "required": ["title"],
            "additionalProperties": False,
        }
    )

    assert "required" not in relaxed
    assert "additionalProperties" not in relaxed
    assert "minLength" not in relaxed["properties"]["title"]
    assert "minItems" not in relaxed["properties"]["items"]
    assert "enum" not in relaxed["properties"]["items"]["items"]
