"""Tests for extraction schema models and JSON Schema export."""

from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from knowledge_forge.extract import (
    JSON_SCHEMA_REGISTRY,
    SCHEMA_REGISTRY,
    AlarmDefinition,
    Applicability,
    ContradictionCandidate,
    PartReference,
    Procedure,
    ProcedureStep,
    RevisionNote,
    SpecValue,
    SupersessionAssessment,
    SupersessionCandidate,
    SupersessionRecordMetadata,
    TroubleshootingEntry,
    Warning,
    get_json_schema,
    get_schema_model,
)


def _base_payload() -> dict[str, object]:
    return {
        "source_doc_id": "honeywell-dc1000-service-manual-rev3",
        "source_page_range": {"start_page": 18, "end_page": 20},
        "source_heading": "Startup Procedure",
        "parser_version": "docling-1.2.0",
        "extraction_version": "f1",
        "confidence": 0.94,
        "bucket_context": [
            {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"},
        ],
    }


def _applicability_payload() -> dict[str, object]:
    payload = _base_payload()
    payload.update(
        {
            "manufacturer": "Honeywell",
            "family": "DC1000",
            "models": ["DC1000", "DC1100"],
            "serial_range": "SN1000-SN2999",
            "revision": "Rev 3",
        }
    )
    return payload


VALID_PAYLOADS: dict[str, tuple[type[object], dict[str, object]]] = {
    "procedure_step": (
        ProcedureStep,
        {
            **_base_payload(),
            "step_number": 1,
            "instruction": "Verify the discharge valve is open.",
            "note": "Use local lockout procedure before inspection.",
            "caution": "Do not energize the motor dry.",
            "figure_ref": "Fig. 3-2",
        },
    ),
    "applicability": (Applicability, _applicability_payload()),
    "warning": (
        Warning,
        {
            **_base_payload(),
            "severity": "danger",
            "text": "Disconnect mains power before opening the cabinet.",
            "context": "Electrical service",
            "applicability": _applicability_payload(),
        },
    ),
    "procedure": (
        Procedure,
        {
            **_base_payload(),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_payload(),
                    "source_page_range": {"start_page": 18, "end_page": 18},
                    "step_number": 1,
                    "instruction": "Verify the discharge valve is open.",
                    "note": "Use local lockout procedure before inspection.",
                    "caution": "Do not energize the motor dry.",
                    "figure_ref": "Fig. 3-2",
                }
            ],
            "applicability": _applicability_payload(),
            "warnings": [
                {
                    **_base_payload(),
                    "severity": "warning",
                    "text": "Wear insulated gloves.",
                    "context": "Startup checks",
                    "applicability": None,
                }
            ],
            "tools_required": ["multimeter", "torque wrench"],
        },
    ),
    "spec_value": (
        SpecValue,
        {
            **_base_payload(),
            "parameter": "Supply voltage",
            "value": "24",
            "unit": "VDC",
            "conditions": "Nominal input",
            "applicability": None,
        },
    ),
    "alarm_definition": (
        AlarmDefinition,
        {
            **_base_payload(),
            "code": "AL-14",
            "description": "Low process pressure",
            "cause": "Measured pressure stayed below threshold for 10 seconds.",
            "remedy": "Inspect suction line and transducer calibration.",
            "severity": "warning",
        },
    ),
    "troubleshooting_entry": (
        TroubleshootingEntry,
        {
            **_base_payload(),
            "symptom": "Display remains blank after startup.",
            "possible_causes": ["No control power", "Blown input fuse"],
            "remedies": ["Check incoming supply", "Replace fuse F1"],
        },
    ),
    "part_reference": (
        PartReference,
        {
            **_base_payload(),
            "part_number": "320044-001",
            "description": "Input fuse, 2A slow-blow",
            "quantity": 1,
            "applicability": None,
        },
    ),
    "revision_note": (
        RevisionNote,
        {
            **_base_payload(),
            "revision_id": "Rev 3",
            "date": "2025-01-12",
            "changes": ["Updated wiring diagram", "Added AL-14 troubleshooting guidance"],
            "supersedes": "Rev 2",
        },
    ),
    "supersession_candidate": (
        SupersessionCandidate,
        {
            **_base_payload(),
            "superseding_record_id": "dc1000--revision-note--003",
            "superseded_record_id": "dc1000--revision-note--002",
            "rationale": "Rev 3 explicitly replaces the previous wiring instructions.",
            "precedence_basis": "newer revision note",
        },
    ),
    "contradiction_candidate": (
        ContradictionCandidate,
        {
            **_base_payload(),
            "record_ids": ["dc1000--spec--001", "dc1000--spec--014"],
            "conflicting_claim": "Nominal supply voltage differs between two authoritative sources.",
            "rationale": "One manual states 24 VDC and a later bulletin states 48 VDC.",
            "review_status": "unreviewed",
            "compared_records": [
                {
                    "record_id": "dc1000--spec--001",
                    "source_doc_id": "honeywell-dc1000-service-manual-rev3",
                    "document_type": "Service Manual",
                    "document_class": "authoritative-technical",
                    "revision": "Rev 3",
                    "publication_date": "2025-01-12",
                    "precedence_level": 2,
                    "precedence_label": "revised manual",
                },
                {
                    "record_id": "dc1000--spec--014",
                    "source_doc_id": "honeywell-dc1000-service-bulletin-revb",
                    "document_type": "Service Bulletin",
                    "document_class": "authoritative-technical",
                    "revision": "Rev B",
                    "publication_date": "2025-02-05",
                    "precedence_level": 1,
                    "precedence_label": "service bulletin or addendum",
                },
            ],
            "supersession": {
                "superseding_record_id": "dc1000--spec--014",
                "superseded_record_id": "dc1000--spec--001",
                "confidence": "high",
                "reason": "Service bulletin has stronger authority than the revised manual.",
                "precedence_rule_applied": "service bulletin or addendum (level 1) outranks revised manual (level 2)",
                "document_types_compared": ["Service Bulletin", "Service Manual"],
            },
        },
    ),
}


INVALID_MUTATIONS: dict[str, tuple[str, object]] = {
    "procedure_step": ("step_number", 0),
    "applicability": ("models", []),
    "warning": ("severity", "fatal"),
    "procedure": ("steps", []),
    "spec_value": ("parameter", " "),
    "alarm_definition": ("code", " "),
    "troubleshooting_entry": ("possible_causes", []),
    "part_reference": ("quantity", 0),
    "revision_note": ("changes", []),
    "supersession_candidate": ("precedence_basis", " "),
    "contradiction_candidate": ("record_ids", ["only-one"]),
}

VALID_CASES = [(record_type, model, payload) for record_type, (model, payload) in VALID_PAYLOADS.items()]


@pytest.mark.parametrize(("record_type", "model", "payload"), VALID_CASES)
def test_extraction_schema_models_accept_valid_examples(
    record_type: str,
    model: type[object],
    payload: dict[str, object],
) -> None:
    instance = model.model_validate(payload)  # type: ignore[attr-defined]

    assert instance.source_doc_id == payload["source_doc_id"]
    assert instance.source_page_range.start_page == payload["source_page_range"]["start_page"]
    assert get_schema_model(record_type) is model


def test_supersession_assessment_models_accept_valid_examples() -> None:
    metadata = SupersessionRecordMetadata.model_validate(
        {
            "record_id": "rec-001",
            "source_doc_id": "doc-001",
            "document_type": "Service Manual",
            "document_class": "authoritative-technical",
            "revision": "Rev 3",
            "publication_date": "2025-01-12",
            "precedence_level": 2,
            "precedence_label": "revised manual",
        }
    )
    assessment = SupersessionAssessment.model_validate(
        {
            "superseding_record_id": "rec-002",
            "superseded_record_id": "rec-001",
            "confidence": "medium",
            "reason": "Rev 4 is newer than Rev 3.",
            "precedence_rule_applied": "same document type; newer revision `Rev 4` supersedes `Rev 3`",
            "document_types_compared": ["Service Manual", "Service Manual"],
        }
    )

    assert metadata.precedence_level == 2
    assert assessment.confidence == "medium"


@pytest.mark.parametrize(("record_type", "model", "payload"), VALID_CASES)
def test_extraction_schema_models_reject_invalid_examples(
    record_type: str,
    model: type[object],
    payload: dict[str, object],
) -> None:
    field_name, invalid_value = INVALID_MUTATIONS[record_type]
    invalid_payload = deepcopy(payload)
    invalid_payload[field_name] = invalid_value

    with pytest.raises(ValidationError):
        model.model_validate(invalid_payload)  # type: ignore[attr-defined]


def test_procedure_normalizes_nullable_tools_required_to_empty_list() -> None:
    payload = deepcopy(VALID_PAYLOADS["procedure"][1])
    payload["tools_required"] = None

    procedure = Procedure.model_validate(payload)

    assert procedure.tools_required == []


def test_schema_registry_contains_all_expected_record_types() -> None:
    assert set(SCHEMA_REGISTRY) == {
        "procedure",
        "procedure_step",
        "warning",
        "spec_value",
        "alarm_definition",
        "troubleshooting_entry",
        "part_reference",
        "applicability",
        "revision_note",
        "supersession_candidate",
        "contradiction_candidate",
    }


@pytest.mark.parametrize("record_type", sorted(VALID_PAYLOADS))
def test_json_schema_registry_exports_prompt_ready_schemas(record_type: str) -> None:
    schema = get_json_schema(record_type)

    assert schema == JSON_SCHEMA_REGISTRY[record_type]
    assert schema["type"] == "object"
    assert "source_doc_id" in schema["properties"]
    assert "bucket_context" in schema["properties"]
    assert "$defs" not in schema


def _contains_local_ref(value: object) -> bool:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            return True
        return any(_contains_local_ref(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_local_ref(item) for item in value)
    return False


@pytest.mark.parametrize("record_type", sorted(VALID_PAYLOADS))
def test_json_schema_registry_inlines_local_defs_refs(record_type: str) -> None:
    schema = get_json_schema(record_type)

    assert _contains_local_ref(schema) is False


def test_unknown_schema_lookup_raises_key_error() -> None:
    with pytest.raises(KeyError, match="unknown extraction record type"):
        get_schema_model("not-a-real-type")
