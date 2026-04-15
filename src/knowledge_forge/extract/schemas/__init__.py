"""Extraction record schemas and registry."""

from __future__ import annotations

from typing import TypeAlias

from knowledge_forge.extract.schemas.alarm_definition import AlarmDefinition
from knowledge_forge.extract.schemas.applicability import Applicability
from knowledge_forge.extract.schemas.base import (
    BucketContext,
    ExtractionSchemaModel,
    ProvenancedRecord,
    SourcePageRange,
)
from knowledge_forge.extract.schemas.contradiction_candidate import ContradictionCandidate
from knowledge_forge.extract.schemas.part_reference import PartReference
from knowledge_forge.extract.schemas.procedure import Procedure
from knowledge_forge.extract.schemas.procedure_step import ProcedureStep
from knowledge_forge.extract.schemas.revision_note import RevisionNote
from knowledge_forge.extract.schemas.spec_value import SpecValue
from knowledge_forge.extract.schemas.supersession_candidate import SupersessionCandidate
from knowledge_forge.extract.schemas.troubleshooting_entry import TroubleshootingEntry
from knowledge_forge.extract.schemas.warning import Warning

RecordSchema: TypeAlias = type[ExtractionSchemaModel]

SCHEMA_REGISTRY: dict[str, RecordSchema] = {
    "procedure": Procedure,
    "procedure_step": ProcedureStep,
    "warning": Warning,
    "spec_value": SpecValue,
    "alarm_definition": AlarmDefinition,
    "troubleshooting_entry": TroubleshootingEntry,
    "part_reference": PartReference,
    "applicability": Applicability,
    "revision_note": RevisionNote,
    "supersession_candidate": SupersessionCandidate,
    "contradiction_candidate": ContradictionCandidate,
}

JSON_SCHEMA_REGISTRY: dict[str, dict[str, object]] = {
    name: model.json_schema() for name, model in SCHEMA_REGISTRY.items()
}


def get_schema_model(record_type: str) -> RecordSchema:
    """Look up an extraction schema model by type name."""
    try:
        return SCHEMA_REGISTRY[record_type]
    except KeyError as exc:
        raise KeyError(f"unknown extraction record type '{record_type}'") from exc


def get_json_schema(record_type: str) -> dict[str, object]:
    """Return the JSON Schema for one extraction record type."""
    return JSON_SCHEMA_REGISTRY[record_type]


__all__ = [
    "AlarmDefinition",
    "Applicability",
    "BucketContext",
    "ContradictionCandidate",
    "ExtractionSchemaModel",
    "get_json_schema",
    "get_schema_model",
    "JSON_SCHEMA_REGISTRY",
    "PartReference",
    "Procedure",
    "ProcedureStep",
    "ProvenancedRecord",
    "RecordSchema",
    "RevisionNote",
    "SCHEMA_REGISTRY",
    "SourcePageRange",
    "SpecValue",
    "SupersessionCandidate",
    "TroubleshootingEntry",
    "Warning",
]
