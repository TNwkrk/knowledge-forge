"""Section-to-record extraction engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

from yaml import safe_load

from knowledge_forge.extract.provenance import (
    ExtractionMetadata,
    attach_provenance,
    load_bucket_context,
    load_parse_metadata,
    validate_record_provenance,
)
from knowledge_forge.extract.repair import repair_extraction
from knowledge_forge.extract.schemas import ExtractionSchemaModel, get_json_schema, get_schema_model
from knowledge_forge.inference import InferenceClient
from knowledge_forge.inference.config import InferenceConfig
from knowledge_forge.inference.schema_validator import ValidationResult
from knowledge_forge.intake.importer import get_data_dir, load_manifest
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse.sectioning import Section, SectionType

ExtractedRecord: TypeAlias = ExtractionSchemaModel

SECTION_RECORD_TYPE_MAP: dict[SectionType, list[str]] = {
    "safety": ["warning"],
    "installation": ["procedure", "warning"],
    "configuration": ["procedure", "warning"],
    "startup": ["procedure", "warning"],
    "shutdown": ["procedure", "warning"],
    "maintenance": ["procedure", "warning"],
    "troubleshooting": ["troubleshooting_entry", "alarm_definition"],
    "specifications": ["spec_value"],
    "parts": ["part_reference"],
    "revision_notes": ["revision_note"],
    "other": ["applicability"],
}


@dataclass(frozen=True)
class PromptTemplate:
    """Loaded prompt template metadata for one extraction record type."""

    system: str
    user: str
    schema_ref: str
    version: str = "v1"
    model: str | None = None


@dataclass(frozen=True)
class ExtractionAttempt:
    """Normalized extraction payload after validation or repair."""

    parsed_json: object
    validation: ValidationResult
    output_tokens: int
    repaired: bool
    repair_attempts: int
    repair_errors: list[str]


@dataclass(frozen=True)
class ExtractionReviewFlag:
    """Review metadata persisted for low-confidence or failed extractions."""

    doc_id: str
    section_id: str
    record_type: str
    reasons: list[str]
    min_confidence: float
    record_ids: list[str]
    record_confidences: list[float]
    repair_attempts: int
    errors: list[str]


def extract_document(
    doc_id: str,
    *,
    section_id: str | None = None,
    config: InferenceConfig | None = None,
    client: InferenceClient | None = None,
    data_dir: Path | None = None,
    min_confidence: float = 0.0,
    max_repair_attempts: int = 2,
) -> list[ExtractedRecord]:
    """Extract records for one document or one specific section."""
    resolved_data_dir = get_data_dir(data_dir)
    all_sections = load_sections(doc_id, data_dir=resolved_data_dir)
    sections = all_sections
    if section_id is not None:
        sections = [section for section in sections if section.section_id == section_id]
        if not sections:
            raise FileNotFoundError(f"section '{section_id}' not found for doc_id '{doc_id}'")

    active_client = client
    if active_client is None:
        active_config = config or InferenceConfig.load()
        active_client = InferenceClient(active_config, data_dir=resolved_data_dir)

    extracted: list[ExtractedRecord] = []
    for section in sections:
        extracted.extend(
            extract_section(
                section,
                client=active_client,
                data_dir=resolved_data_dir,
                min_confidence=min_confidence,
                max_repair_attempts=max_repair_attempts,
            )
        )

    if section_id is None and sections and len(sections) == len(all_sections):
        _maybe_mark_manifest_extracted(doc_id, data_dir=resolved_data_dir)
    return extracted


def extract_section(
    section: Section,
    record_types: list[str] | None = None,
    *,
    client: InferenceClient,
    data_dir: Path | None = None,
    min_confidence: float = 0.0,
    max_repair_attempts: int = 2,
) -> list[ExtractedRecord]:
    """Extract typed records from one canonical section."""
    if record_types is None:
        resolved_record_types = record_types_for_section_type(section.section_type)
    else:
        resolved_record_types = record_types
    resolved_data_dir = get_data_dir(data_dir)

    extracted: list[ExtractedRecord] = []
    section_quality = load_section_quality(section, data_dir=resolved_data_dir)
    parse_meta = load_parse_metadata(section.doc_id, data_dir=resolved_data_dir)
    bucket_context = load_bucket_context(section.doc_id, data_dir=resolved_data_dir)
    for record_type in resolved_record_types:
        template = load_prompt_template(record_type)
        if template.schema_ref != record_type:
            raise ValueError(
                f"prompt template schema_ref mismatch for record_type '{record_type}': got '{template.schema_ref}'"
            )
        schema = _record_list_schema(record_type)
        prompt = render_prompt(template, section=section, record_type=record_type)
        model = template.model or client.config.extraction_model
        attempt, failed_errors, failed_attempts = _extract_with_repair(
            section=section,
            record_type=record_type,
            prompt=prompt,
            template=template,
            model=model,
            schema=schema,
            client=client,
            max_repair_attempts=max_repair_attempts,
        )
        if attempt is None:
            save_review_flag(
                ExtractionReviewFlag(
                    doc_id=section.doc_id,
                    section_id=section.section_id,
                    record_type=record_type,
                    reasons=["repair_failed"],
                    min_confidence=min_confidence,
                    record_ids=[],
                    record_confidences=[],
                    repair_attempts=failed_attempts,
                    errors=failed_errors,
                ),
                data_dir=resolved_data_dir,
            )
            continue

        records = _parse_records(attempt.parsed_json, record_type=record_type)
        scored_records = apply_confidence_scores(
            records,
            validation=attempt.validation,
            repaired=attempt.repaired,
            section_quality=section_quality,
            output_tokens=attempt.output_tokens,
            max_output_tokens=client.config.max_tokens,
        )
        records_with_provenance = [
            attach_provenance(
                record,
                section,
                parse_meta,
                ExtractionMetadata(
                    model=model,
                    prompt_template=f"extraction/{record_type}",
                    prompt_version=template.version,
                    confidence=record.confidence,
                    bucket_context=bucket_context,
                ),
            )
            for record in scored_records
        ]
        review_flag = build_review_flag(
            section=section,
            record_type=record_type,
            records=records_with_provenance,
            min_confidence=min_confidence,
            repair_attempts=attempt.repair_attempts,
            errors=attempt.repair_errors,
        )
        if review_flag is not None:
            save_review_flag(review_flag, data_dir=resolved_data_dir)
        save_records(
            section=section,
            record_type=record_type,
            records=records_with_provenance,
            data_dir=resolved_data_dir,
        )
        extracted.extend(records_with_provenance)

    return extracted


def load_sections(doc_id: str, *, data_dir: Path | None = None) -> list[Section]:
    """Load persisted canonical sections for one document."""
    resolved_data_dir = get_data_dir(data_dir)
    sections_dir = resolved_data_dir / "sections" / doc_id
    if not sections_dir.exists():
        raise FileNotFoundError(f"sections not found for doc_id '{doc_id}'")

    return [
        Section.model_validate_json(path.read_text(encoding="utf-8")) for path in sorted(sections_dir.glob("*.json"))
    ]


def record_types_for_section_type(section_type: SectionType) -> list[str]:
    """Return the configured extraction record types for one section kind."""
    return list(SECTION_RECORD_TYPE_MAP[section_type])


def load_prompt_template(record_type: str, *, base_dir: Path | None = None) -> PromptTemplate:
    """Load one extraction prompt template from disk."""
    template_dir = base_dir or (Path(__file__).resolve().parents[1] / "inference" / "prompts" / "extraction")
    template_path = template_dir / f"{record_type}.yaml"
    if not template_path.exists():
        raise FileNotFoundError(f"prompt template not found for record type '{record_type}'")

    payload = safe_load(template_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"prompt template is not a YAML mapping: {template_path}")
    required_keys = {"system", "user", "schema_ref"}
    missing = required_keys - payload.keys()
    if missing:
        raise ValueError(f"prompt template missing required keys {sorted(missing)}: {template_path}")
    return PromptTemplate(
        system=str(payload["system"]),
        user=str(payload["user"]),
        schema_ref=str(payload["schema_ref"]),
        version=str(payload.get("version", "v1")),
        model=str(payload["model"]) if payload.get("model") else None,
    )


def render_prompt(template: PromptTemplate, *, section: Section, record_type: str) -> str:
    """Render the user prompt for one section extraction request."""
    start_page, end_page = section.page_range
    return template.user.format(
        doc_id=section.doc_id,
        section_id=section.section_id,
        section_type=section.section_type,
        record_type=record_type,
        section_title=section.title,
        section_heading_path=" > ".join(section.heading_path) if section.heading_path else section.title,
        page_start=start_page if start_page is not None else "unknown",
        page_end=end_page if end_page is not None else "unknown",
        section_content=section.content,
    )


def save_records(
    *,
    section: Section,
    record_type: str,
    records: list[ExtractedRecord],
    data_dir: Path,
) -> list[Path]:
    """Persist extracted records under the canonical extracted/ directory."""
    target_dir = data_dir / "extracted" / section.doc_id / record_type
    target_dir.mkdir(parents=True, exist_ok=True)

    written_paths: list[Path] = []
    for index, record in enumerate(records, start=1):
        validate_record_provenance(record)
        record_id = build_record_id(section.section_id, record_type, index)
        output_path = target_dir / f"{record_id}.json"
        output_path.write_text(record.model_dump_json(indent=2), encoding="utf-8")
        written_paths.append(output_path)

    return written_paths


def build_record_id(section_id: str, record_type: str, sequence: int) -> str:
    """Build a deterministic record identifier from section and sequence."""
    return f"{section_id}--{record_type}--{sequence:03d}"


def load_section_quality(section: Section, *, data_dir: Path) -> float:
    """Load normalized parse quality for a section's document when available."""
    quality_path = data_dir / "parsed" / section.doc_id / "quality.json"
    default_score = 1.0 if section.content.strip() else 0.5
    if not quality_path.exists():
        return default_score

    try:
        payload = json.loads(quality_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_score

    overall_score = payload.get("overall_score")
    if not isinstance(overall_score, (int, float)):
        return default_score
    return round(max(0.0, min(float(overall_score) / 100.0, 1.0)), 3)


def apply_confidence_scores(
    records: list[ExtractedRecord],
    *,
    validation: ValidationResult,
    repaired: bool,
    section_quality: float,
    output_tokens: int,
    max_output_tokens: int,
) -> list[ExtractedRecord]:
    """Attach deterministic confidence scores to extracted records."""
    schema_score = 1.0 if validation.valid else 0.0
    repair_score = 0.7 if repaired else 1.0
    token_headroom = 1.0
    if max_output_tokens > 0:
        token_headroom = max(0.0, 1.0 - min(output_tokens / max_output_tokens, 1.0))
    confidence = round((schema_score + repair_score + section_quality + token_headroom) / 4.0, 3)

    scored: list[ExtractedRecord] = []
    for record in records:
        scored.append(record.model_copy(update={"confidence": confidence}))
    return scored


def build_review_flag(
    *,
    section: Section,
    record_type: str,
    records: list[ExtractedRecord],
    min_confidence: float,
    repair_attempts: int,
    errors: list[str],
) -> ExtractionReviewFlag | None:
    """Build a review artifact when extraction needs operator attention."""
    if min_confidence <= 0:
        return None

    flagged_ids: list[str] = []
    flagged_confidences: list[float] = []
    for index, record in enumerate(records, start=1):
        if record.confidence < min_confidence:
            flagged_ids.append(build_record_id(section.section_id, record_type, index))
            flagged_confidences.append(record.confidence)
    if not flagged_ids:
        return None

    return ExtractionReviewFlag(
        doc_id=section.doc_id,
        section_id=section.section_id,
        record_type=record_type,
        reasons=["below_min_confidence"],
        min_confidence=min_confidence,
        record_ids=flagged_ids,
        record_confidences=flagged_confidences,
        repair_attempts=repair_attempts,
        errors=errors,
    )


def save_review_flag(flag: ExtractionReviewFlag, *, data_dir: Path) -> Path:
    """Persist extraction review metadata next to extracted records."""
    review_dir = data_dir / "extracted" / flag.doc_id / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    output_path = review_dir / f"{flag.section_id}--{flag.record_type}.json"
    output_path.write_text(json.dumps(flag.__dict__, indent=2), encoding="utf-8")
    return output_path


def _record_list_schema(record_type: str) -> dict[str, object]:
    record_schema = get_json_schema(record_type)
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "records": {
                "type": "array",
                "items": record_schema,
            }
        },
        "required": ["records"],
    }


def _extract_with_repair(
    *,
    section: Section,
    record_type: str,
    prompt: str,
    template: PromptTemplate,
    model: str,
    schema: dict[str, object],
    client: InferenceClient,
    max_repair_attempts: int,
) -> tuple[ExtractionAttempt | None, list[str], int]:
    """Return ``(attempt, errors, repair_attempts)``; attempt is None on irrecoverable failure."""
    prompt_template = f"extraction/{record_type}"
    try:
        result = client.complete(
            prompt=prompt,
            system=template.system,
            model=model,
            schema=schema,
            prompt_template=prompt_template,
            source_doc_id=section.doc_id,
            source_section_id=section.section_id,
        )
        return (
            ExtractionAttempt(
                parsed_json=result.parsed_json,
                validation=ValidationResult(valid=True),
                output_tokens=result.output_tokens,
                repaired=False,
                repair_attempts=0,
                repair_errors=[],
            ),
            [],
            0,
        )
    except ValueError as exc:
        repair = repair_extraction(
            str(exc),
            schema,
            prompt,
            client=client,
            system=template.system,
            model=model,
            prompt_template=prompt_template,
            source_doc_id=section.doc_id,
            source_section_id=section.section_id,
            max_attempts=max_repair_attempts,
        )
        if not repair.valid or repair.repaired_json is None:
            return (None, repair.errors, repair.attempts)
        return (
            ExtractionAttempt(
                parsed_json=repair.repaired_json,
                validation=ValidationResult(valid=True, repaired=True),
                output_tokens=repair.output_tokens,
                repaired=True,
                repair_attempts=repair.attempts,
                repair_errors=repair.errors,
            ),
            repair.errors,
            repair.attempts,
        )


def _parse_records(parsed_json: object, *, record_type: str) -> list[ExtractedRecord]:
    if not isinstance(parsed_json, dict):
        raise ValueError("extraction response must be a JSON object")

    payload = parsed_json.get("records")
    if not isinstance(payload, list):
        raise ValueError("extraction response must contain a 'records' list")

    model = get_schema_model(record_type)
    return [model.model_validate(item) for item in payload]


def _maybe_mark_manifest_extracted(doc_id: str, *, data_dir: Path) -> None:
    manifest_path = data_dir / "manifests" / f"{doc_id}.yaml"
    if not manifest_path.exists():
        return

    manifest = load_manifest(data_dir, doc_id)
    if manifest.document.status == DocumentStatus.EXTRACTED:
        return

    updated = manifest.transition_status(DocumentStatus.EXTRACTED, reason="structured extraction complete")
    manifest_path.write_text(updated.to_yaml(), encoding="utf-8")
