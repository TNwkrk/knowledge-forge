"""Provenance attachment, validation, and audit helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pydantic import ValidationError

from knowledge_forge.extract.schemas import BucketContext, ExtractionSchemaModel, ProvenancedRecord
from knowledge_forge.intake.importer import get_data_dir, load_manifest
from knowledge_forge.parse.quality import ParseMetadata
from knowledge_forge.parse.sectioning import Section


@dataclass(frozen=True)
class ExtractionMetadata:
    """Execution metadata needed to stamp records with deterministic provenance."""

    model: str
    prompt_template: str
    prompt_version: str
    confidence: float
    bucket_context: list[BucketContext]

    @property
    def extraction_version(self) -> str:
        """Stable extraction version string for persisted records."""
        return f"{self.prompt_template}@{self.prompt_version}:{self.model}"


@dataclass(frozen=True)
class ProvenanceAuditRow:
    """Per-record provenance audit result."""

    record_type: str
    record_id: str
    valid: bool
    errors: list[str]


@dataclass(frozen=True)
class ProvenanceAuditReport:
    """Summary of provenance completeness for one document."""

    doc_id: str
    total_records: int
    valid_records: int
    invalid_records: int
    rows: list[ProvenanceAuditRow]


def attach_provenance(
    record: ExtractionSchemaModel,
    section: Section,
    parse_meta: ParseMetadata,
    extraction_meta: ExtractionMetadata,
) -> ExtractionSchemaModel:
    """Overwrite AI-supplied provenance with deterministic local metadata."""
    page_start, page_end = section.page_range
    provenanced = _attach_nested_provenance(
        record,
        source_doc_id=section.doc_id,
        source_page_range={
            "start_page": page_start or 1,
            "end_page": page_end or page_start or 1,
        },
        source_heading=section.title,
        parser_version=parse_meta.parser_version,
        extraction_version=extraction_meta.extraction_version,
        confidence=extraction_meta.confidence,
        bucket_context=[context.model_copy() for context in extraction_meta.bucket_context],
    )
    validate_record_provenance(provenanced)
    return provenanced


def validate_record_provenance(record: ExtractionSchemaModel) -> None:
    """Raise a clear error when a record lacks complete provenance."""
    if not isinstance(record, ProvenancedRecord):
        raise ValueError(f"record type '{type(record).__name__}' does not support provenance validation")
    try:
        type(record).model_validate(record.model_dump(mode="python"))
    except ValidationError as exc:
        raise ValueError(f"record is missing complete provenance: {exc}") from exc


def load_parse_metadata(doc_id: str, *, data_dir: Path | None = None) -> ParseMetadata:
    """Load persisted parse metadata for one document."""
    resolved_data_dir = get_data_dir(data_dir)
    meta_path = resolved_data_dir / "parsed" / doc_id / "meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"parse metadata not found for doc_id '{doc_id}'")
    return ParseMetadata.model_validate_json(meta_path.read_text(encoding="utf-8"))


def load_bucket_context(doc_id: str, *, data_dir: Path | None = None) -> list[BucketContext]:
    """Load deterministic bucket context from the persisted manifest."""
    resolved_data_dir = get_data_dir(data_dir)
    manifest = load_manifest(resolved_data_dir, doc_id)
    if not manifest.bucket_assignments:
        raise ValueError(f"manifest '{doc_id}' has no bucket assignments for provenance")
    return [
        BucketContext(
            bucket_id=assignment.bucket_id,
            dimension=assignment.dimension,
            value=assignment.value,
        )
        for assignment in manifest.bucket_assignments
    ]


def audit_document_provenance(doc_id: str, *, data_dir: Path | None = None) -> ProvenanceAuditReport:
    """Audit all persisted extracted records for provenance completeness."""
    resolved_data_dir = get_data_dir(data_dir)
    extracted_dir = resolved_data_dir / "extracted" / doc_id
    if not extracted_dir.exists():
        raise FileNotFoundError(f"extracted records not found for doc_id '{doc_id}'")

    rows: list[ProvenanceAuditRow] = []
    for record_path in sorted(extracted_dir.glob("*/*.json")):
        if record_path.parent.name == "reviews":
            continue
        try:
            record = _load_record_for_audit(record_path)
            validate_record_provenance(record)
        except (KeyError, ValidationError, ValueError) as exc:
            rows.append(
                ProvenanceAuditRow(
                    record_type=record_path.parent.name,
                    record_id=record_path.stem,
                    valid=False,
                    errors=[str(exc)],
                )
            )
            continue
        rows.append(
            ProvenanceAuditRow(
                record_type=record_path.parent.name,
                record_id=record_path.stem,
                valid=True,
                errors=[],
            )
        )

    valid_records = sum(1 for row in rows if row.valid)
    return ProvenanceAuditReport(
        doc_id=doc_id,
        total_records=len(rows),
        valid_records=valid_records,
        invalid_records=len(rows) - valid_records,
        rows=rows,
    )


def _attach_nested_provenance(record: ExtractionSchemaModel, **provenance: object) -> ExtractionSchemaModel:
    updates: dict[str, object] = dict(provenance)
    for field_name in type(record).model_fields:
        value = getattr(record, field_name)
        if isinstance(value, ProvenancedRecord):
            updates[field_name] = _attach_nested_provenance(value, **provenance)
        elif isinstance(value, list) and value and all(isinstance(item, ProvenancedRecord) for item in value):
            updates[field_name] = [_attach_nested_provenance(item, **provenance) for item in value]
    return type(record).model_validate({**record.model_dump(mode="python"), **updates})


def _load_record_for_audit(record_path: Path) -> ExtractionSchemaModel:
    from knowledge_forge.extract.schemas import get_schema_model

    model = get_schema_model(record_path.parent.name)
    return model.model_validate_json(record_path.read_text(encoding="utf-8"))
