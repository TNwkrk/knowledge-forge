"""Section-to-record extraction engine."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

from yaml import safe_load

from knowledge_forge.extract.schemas import ExtractionSchemaModel, get_json_schema, get_schema_model
from knowledge_forge.inference import InferenceClient
from knowledge_forge.inference.config import InferenceConfig
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
    model: str | None = None


def extract_document(
    doc_id: str,
    *,
    section_id: str | None = None,
    config: InferenceConfig | None = None,
    client: InferenceClient | None = None,
    data_dir: Path | None = None,
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
        extracted.extend(extract_section(section, client=active_client, data_dir=resolved_data_dir))

    if section_id is None and sections and len(sections) == len(all_sections):
        _maybe_mark_manifest_extracted(doc_id, data_dir=resolved_data_dir)
    return extracted


def extract_section(
    section: Section,
    record_types: list[str] | None = None,
    *,
    client: InferenceClient,
    data_dir: Path | None = None,
) -> list[ExtractedRecord]:
    """Extract typed records from one canonical section."""
    if record_types is None:
        resolved_record_types = record_types_for_section_type(section.section_type)
    else:
        resolved_record_types = record_types
    resolved_data_dir = get_data_dir(data_dir)

    extracted: list[ExtractedRecord] = []
    for record_type in resolved_record_types:
        template = load_prompt_template(record_type)
        if template.schema_ref != record_type:
            raise ValueError(
                "prompt template schema_ref mismatch for "
                f"record_type '{record_type}': got '{template.schema_ref}'"
            )
        schema = _record_list_schema(record_type)
        prompt = render_prompt(template, section=section, record_type=record_type)
        model = template.model or client.config.extraction_model
        result = client.complete(
            prompt=prompt,
            system=template.system,
            model=model,
            schema=schema,
            prompt_template=f"extraction/{record_type}",
            source_doc_id=section.doc_id,
            source_section_id=section.section_id,
        )
        records = _parse_records(result.parsed_json, record_type=record_type)
        save_records(section=section, record_type=record_type, records=records, data_dir=resolved_data_dir)
        extracted.extend(records)

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

    payload = safe_load(template_path.read_text(encoding="utf-8")) or {}
    return PromptTemplate(
        system=str(payload["system"]),
        user=str(payload["user"]),
        schema_ref=str(payload["schema_ref"]),
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
        record_id = build_record_id(section.section_id, record_type, index)
        output_path = target_dir / f"{record_id}.json"
        output_path.write_text(record.model_dump_json(indent=2), encoding="utf-8")
        written_paths.append(output_path)

    return written_paths


def build_record_id(section_id: str, record_type: str, sequence: int) -> str:
    """Build a deterministic record identifier from section and sequence."""
    return f"{section_id}--{record_type}--{sequence:03d}"


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
