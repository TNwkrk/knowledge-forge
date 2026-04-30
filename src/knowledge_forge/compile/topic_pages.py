"""Topic-page compilation for bucket-scoped extracted records."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from yaml import safe_load

from knowledge_forge.compile.contradiction_notes import (
    ContradictionNoteEntry,
    build_note_entries,
    render_inline_contradiction_notes,
)
from knowledge_forge.compile.source_pages import (
    GENERATED_BY,
    PUBLISH_RUN_PLACEHOLDER,
    CompiledPage,
    CompileMetadata,
)
from knowledge_forge.extract.engine import load_sections
from knowledge_forge.extract.reviewability import assess_section_reviewability
from knowledge_forge.extract.schemas import (
    AlarmDefinition,
    Applicability,
    ExtractionSchemaModel,
    Procedure,
    ProcedureStep,
    SpecValue,
    TroubleshootingEntry,
    Warning,
    get_schema_model,
)
from knowledge_forge.inference import InferenceClient
from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import ManifestEntry, slugify
from knowledge_forge.parse.sectioning import Section, SectionType

COMPILATION_VERSION = "topic-pages-v1"
TOPIC_DIGEST_TYPE_MAP: dict[str, str] = {
    "startup_procedure": "workflow-guidance",
    "shutdown_procedure": "workflow-guidance",
    "maintenance_procedure": "workflow-guidance",
    "alarm_reference": "fault-code",
    "specifications": "controller",
    "troubleshooting": "symptom",
}
TOPIC_TITLES: dict[str, str] = {
    "startup_procedure": "Startup Procedure",
    "shutdown_procedure": "Shutdown Procedure",
    "maintenance_procedure": "Maintenance Procedure",
    "alarm_reference": "Alarm Reference",
    "specifications": "Specifications",
    "troubleshooting": "Troubleshooting",
}
TOPIC_SECTION_TYPE_MAP: dict[str, set[SectionType]] = {
    "startup_procedure": {"startup"},
    "shutdown_procedure": {"shutdown"},
    "maintenance_procedure": {"maintenance"},
    "alarm_reference": {"troubleshooting", "other"},
    "specifications": {"specifications", "other"},
    "troubleshooting": {"troubleshooting", "other"},
}
TOPIC_RECORD_TYPE_MAP: dict[str, set[str]] = {
    "startup_procedure": {"procedure"},
    "shutdown_procedure": {"procedure"},
    "maintenance_procedure": {"procedure"},
    "alarm_reference": {"alarm_definition"},
    "specifications": {"spec_value"},
    "troubleshooting": {"troubleshooting_entry"},
}
OTHER_SECTION_MIN_CONFIDENCE = 0.8
TOPIC_SCOPE_DOMINANCE_THRESHOLD = 0.6
UI_ADMIN_EXCLUSION_TERMS = {
    "application",
    "backup",
    "browser",
    "button",
    "certificate",
    "display",
    "editor",
    "export",
    "graphic",
    "import",
    "macro",
    "menu",
    "runtime",
    "screen",
    "security",
    "title bar",
    "user",
    "visibility",
    "window",
    "xml",
}
FIELD_EQUIPMENT_TERMS = {
    "breaker",
    "cabinet",
    "contact",
    "controller",
    "current",
    "discharge",
    "energize",
    "equipment",
    "input",
    "motor",
    "output",
    "panel",
    "power",
    "pump",
    "relay",
    "signal switch",
    "supply",
    "terminal",
    "unit",
    "valve",
    "voltage",
    "wire",
}
TOPIC_DOMAIN_TERMS: dict[str, set[str]] = {
    "startup_procedure": {"energize", "power up", "power on", "start", "startup"},
    "shutdown_procedure": {"de-energize", "power down", "power off", "shut down", "shutdown", "stop"},
    "maintenance_procedure": {"clean", "inspect", "lubricate", "maint", "maintenance", "pm", "replace", "service"},
}
LOW_SIGNAL_SPEC_TERMS = {
    "application",
    "backup",
    "certificate",
    "compliance",
    "csv",
    "encoding",
    "export",
    "file",
    "graphic",
    "import",
    "menu",
    "regulatory",
    "rohs",
    "runtime",
    "security",
    "software",
    "unicode",
    "utf-8",
    "visibility",
    "weee",
    "xml",
}
LOW_SIGNAL_SPEC_PARAMETERS = {"a", "b", "c", "d", "e", "f", "g", "h", "l", "w", "x", "y", "z"}


@dataclass(frozen=True)
class CompilationPromptTemplate:
    """Prompt template metadata for topic-page compilation."""

    system: str
    user: str
    version: str = "v1"
    model: str | None = None


@dataclass(frozen=True)
class TopicRecord:
    """A compiled-topic input record with section and manifest context."""

    doc_id: str
    manifest: ManifestEntry
    section: Section
    record_type: str
    record_path: Path
    record: ExtractionSchemaModel

    @property
    def citation(self) -> str:
        page_range = record_page_label(self.record)
        return f"[Source: {self.doc_id}, {page_range}]"


def compile_topic_page(
    bucket_id: str,
    topic: str,
    records: list[TopicRecord] | None = None,
    *,
    client: InferenceClient,
    data_dir: Path | None = None,
    contradiction_entries: list[ContradictionNoteEntry] | None = None,
) -> CompiledPage:
    """Compile one bucket/topic Markdown page from extracted records."""
    if topic not in TOPIC_TITLES:
        raise ValueError(f"unsupported topic '{topic}'")

    resolved_data_dir = get_data_dir(data_dir)
    topic_records = (
        records if records is not None else _load_topic_records(bucket_id, topic, data_dir=resolved_data_dir)
    )
    topic_records = _filter_topic_records(bucket_id, topic, topic_records)
    if not topic_records:
        raise FileNotFoundError(f"no extracted records found for bucket '{bucket_id}' and topic '{topic}'")

    prompt_template = load_compilation_prompt_template("topic_page")
    prompt = render_topic_prompt(
        prompt_template,
        bucket_id=bucket_id,
        topic=topic,
        records=topic_records,
    )
    result = client.complete(
        prompt=prompt,
        system=prompt_template.system,
        model=prompt_template.model or client.config.compilation_model,
        prompt_template="compilation/topic_page",
        source_section_id=topic,
    )

    generated_at = utc_timestamp()
    output_path = resolved_data_dir / "compiled" / "topic-pages" / slugify(bucket_id) / f"{topic}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    source_documents = _build_source_documents(topic_records)
    publish_metadata = _build_publish_metadata(bucket_id, topic, topic_records)
    frontmatter = {
        "title": _build_title(topic_records, topic),
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": source_documents,
        "generated_at": generated_at,
        "extraction_version": ", ".join(sorted({entry.record.extraction_version for entry in topic_records})),
        "compilation_version": f"{COMPILATION_VERSION}@{prompt_template.version}",
        "bucket_id": bucket_id,
        "topic": topic,
        **publish_metadata,
    }
    content = _render_content(
        bucket_id=bucket_id,
        topic=topic,
        llm_markdown=result.response_text,
        records=topic_records,
        data_dir=resolved_data_dir,
        contradiction_entries=contradiction_entries,
    )
    page = CompiledPage(
        output_path=output_path,
        doc_id=bucket_id,
        frontmatter=frontmatter,
        content=content,
        compile_metadata=CompileMetadata(
            generated_at=generated_at,
            extraction_versions=sorted({entry.record.extraction_version for entry in topic_records}),
            parser_versions=sorted({entry.record.parser_version for entry in topic_records}),
            record_counts=dict(sorted(Counter(entry.record_type for entry in topic_records).items())),
            review_flag_count=0,
        ),
    )
    output_path.write_text(page.render(), encoding="utf-8")
    return page


def compile_bucket_topic_pages(
    bucket_id: str,
    *,
    client: InferenceClient,
    data_dir: Path | None = None,
) -> list[CompiledPage]:
    """Compile every supported topic page for one bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    topic_records = _load_bucket_records(bucket_id, data_dir=resolved_data_dir)
    bucket_contradiction_entries = build_note_entries(bucket_id, data_dir=resolved_data_dir)
    pages: list[CompiledPage] = []
    for topic in TOPIC_TITLES:
        records_for_topic = _filter_topic_records(
            bucket_id,
            topic,
            [entry for entry in topic_records if classify_topic(entry) == topic],
        )
        if not records_for_topic:
            continue
        pages.append(
            compile_topic_page(
                bucket_id,
                topic,
                records_for_topic,
                client=client,
                data_dir=resolved_data_dir,
                contradiction_entries=bucket_contradiction_entries,
            )
        )
    return pages


def compile_all_topic_pages(
    *,
    client: InferenceClient,
    data_dir: Path | None = None,
) -> list[CompiledPage]:
    """Compile topic pages for every bucket that has extracted records."""
    resolved_data_dir = get_data_dir(data_dir)
    pages: list[CompiledPage] = []
    for bucket_id in _discover_bucket_ids(resolved_data_dir):
        pages.extend(compile_bucket_topic_pages(bucket_id, client=client, data_dir=resolved_data_dir))
    return pages


def load_compilation_prompt_template(
    name: str,
    *,
    base_dir: Path | None = None,
) -> CompilationPromptTemplate:
    """Load one compilation prompt template from disk."""
    template_dir = base_dir or (Path(__file__).resolve().parents[1] / "inference" / "prompts" / "compilation")
    template_path = template_dir / f"{name}.yaml"
    if not template_path.exists():
        raise FileNotFoundError(f"compilation prompt template not found: {template_path}")

    payload = safe_load(template_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"prompt template is not a YAML mapping: {template_path}")
    missing = {"system", "user"} - payload.keys()
    if missing:
        raise ValueError(f"prompt template missing required keys {sorted(missing)}: {template_path}")
    return CompilationPromptTemplate(
        system=str(payload["system"]),
        user=str(payload["user"]),
        version=str(payload.get("version", "v1")),
        model=str(payload["model"]) if payload.get("model") else None,
    )


def render_topic_prompt(
    template: CompilationPromptTemplate,
    *,
    bucket_id: str,
    topic: str,
    records: list[TopicRecord],
) -> str:
    """Render the user prompt for one topic-page compilation."""
    prompt_records = [
        {
            "doc_id": entry.doc_id,
            "section_id": entry.section.section_id,
            "section_title": entry.section.title,
            "record_type": entry.record_type,
            "citation": entry.citation,
            "claim": _prompt_claim(entry),
            "applicability": applicability_text(_extract_applicability(entry.record)),
        }
        for entry in records
    ]
    return template.user.format(
        bucket_id=bucket_id,
        topic=topic,
        topic_title=TOPIC_TITLES[topic],
        source_records=json.dumps(prompt_records, indent=2),
    )


def classify_topic(entry: TopicRecord) -> str | None:
    """Map one extracted record into a compiled topic-page type."""
    for topic, record_types in TOPIC_RECORD_TYPE_MAP.items():
        if entry.record_type not in record_types:
            continue
        if entry.section.section_type not in TOPIC_SECTION_TYPE_MAP[topic]:
            continue
        if entry.section.section_type == "other":
            assessment = assess_section_reviewability(entry.section)
            if not assessment.reviewable or entry.record.confidence < OTHER_SECTION_MIN_CONFIDENCE:
                continue
        if not _record_is_admissible_for_topic(entry, topic):
            continue
        return topic
    return None


def utc_timestamp() -> str:
    """Return a canonical UTC timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def record_page_label(record: ExtractionSchemaModel) -> str:
    """Return a human-readable page label for record provenance."""
    start_page = record.source_page_range.start_page
    end_page = record.source_page_range.end_page
    if start_page == end_page:
        return f"p.{start_page}"
    return f"pp.{start_page}-{end_page}"


def applicability_text(applicability: Applicability | None) -> str | None:
    """Render applicability metadata into one compact line."""
    if applicability is None:
        return None

    details = [
        f"{applicability.manufacturer} {applicability.family}",
        f"models: {', '.join(applicability.models)}",
    ]
    if applicability.revision:
        details.append(f"revision: {applicability.revision}")
    if applicability.serial_range:
        details.append(f"serial range: {applicability.serial_range}")
    return "; ".join(details)


def _render_content(
    *,
    bucket_id: str,
    topic: str,
    llm_markdown: str,
    records: list[TopicRecord],
    data_dir: Path,
    contradiction_entries: list[ContradictionNoteEntry] | None = None,
) -> str:
    summary_lines = _normalize_llm_markdown(llm_markdown)
    cited_claims = _render_claims(records)
    applicability_notes = _render_applicability_notes(records)
    contradiction_notes = render_inline_contradiction_notes(
        bucket_id,
        record_ids={entry.record_path.stem for entry in records},
        data_dir=data_dir,
        entries=contradiction_entries,
    )

    lines = [f"# {TOPIC_TITLES[topic]}", ""]
    if summary_lines:
        lines.extend(["## Draft Synthesis", "", *summary_lines, ""])
    lines.extend(["## Source-backed Claims", "", *cited_claims, ""])
    if applicability_notes:
        lines.extend(["## Applicability Differences", "", *applicability_notes, ""])
    if contradiction_notes:
        lines.extend(["## Potential Contradictions", "", *contradiction_notes, ""])
    return "\n".join(lines).rstrip()


def _normalize_llm_markdown(payload: str) -> list[str]:
    lines = [line.rstrip() for line in payload.strip().splitlines() if line.strip()]
    if not lines:
        return []

    normalized: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[Source:") and normalized:
            normalized[-1] = f"{normalized[-1]} {stripped}"
            continue
        if "[Source:" in line:
            normalized.append(line)
            continue
        next_line = lines[index + 1].strip() if index + 1 < len(lines) else None
        if next_line and next_line.startswith("[Source:"):
            normalized.append(line)
    return normalized


def _render_claims(records: list[TopicRecord]) -> list[str]:
    lines: list[str] = []
    for entry in records:
        lines.extend(_render_record_claims(entry))
    return lines


def _render_record_claims(entry: TopicRecord) -> list[str]:
    record = entry.record
    citation = entry.citation
    if isinstance(record, Procedure):
        lines = [f"- {record.title} {citation}"]
        for step in record.steps:
            lines.append(f"  1. {step.instruction} {_nested_citation(entry.doc_id, step)}")
        for warning in record.warnings:
            lines.append(f"  - Warning: {warning.text} {_nested_citation(entry.doc_id, warning)}")
        if record.tools_required:
            lines.append(f"  - Tools required: {', '.join(record.tools_required)} {citation}")
        if (applicability := applicability_text(record.applicability)) is not None:
            lines.append(f"  - Applicability: {applicability} {citation}")
        return lines
    if isinstance(record, SpecValue):
        unit = f" {record.unit}" if record.unit else ""
        conditions = f" ({record.conditions})" if record.conditions else ""
        lines = [f"- {record.parameter}: {record.value}{unit}{conditions} {citation}"]
        if (applicability := applicability_text(record.applicability)) is not None:
            lines.append(f"  - Applicability: {applicability} {citation}")
        return lines
    if isinstance(record, AlarmDefinition):
        return [
            (
                f"- {record.code}: {record.description}. Cause: {record.cause}. "
                f"Remedy: {record.remedy}. Severity: {record.severity}. {citation}"
            )
        ]
    if isinstance(record, TroubleshootingEntry):
        return [
            (
                f"- {record.symptom}. Possible causes: {', '.join(record.possible_causes)}. "
                f"Remedies: {', '.join(record.remedies)}. {citation}"
            )
        ]
    return [f"- {_prompt_claim(entry)} {citation}"]


def _render_applicability_notes(records: list[TopicRecord]) -> list[str]:
    seen: dict[str, list[str]] = {}
    for entry in records:
        applicability = applicability_text(_extract_applicability(entry.record))
        if applicability is None:
            continue
        seen.setdefault(applicability, []).append(entry.citation)
    if len(seen) <= 1:
        return []
    return [
        f"- {applicability} ({', '.join(sorted(set(citations)))})" for applicability, citations in sorted(seen.items())
    ]


def _build_title(records: list[TopicRecord], topic: str) -> str:
    families = sorted({_scope_family(entry) for entry in records if _scope_family(entry)})
    manufacturers = sorted({entry.manifest.document.manufacturer for entry in records})
    if len(manufacturers) == 1:
        manufacturer = manufacturers[0]
    else:
        manufacturer = sorted(records, key=lambda entry: entry.doc_id)[0].manifest.document.manufacturer
    if len(families) == 1:
        return f"{manufacturer} {families[0]} {TOPIC_TITLES[topic]}"
    return f"{manufacturer} {TOPIC_TITLES[topic]}"


def _build_source_documents(records: list[TopicRecord]) -> list[dict[str, object]]:
    seen: dict[tuple[str, str], dict[str, object]] = {}
    for entry in records:
        document = entry.manifest.document
        locator = f'section "{entry.section.title}" ({record_page_label(entry.record)})'
        seen.setdefault(
            (entry.doc_id, locator),
            {
                "doc_id": entry.doc_id,
                "title": f"{document.manufacturer} {document.family} {document.document_type} ({document.revision})",
                "attachment_id": None,
                "locator": locator,
                "revision": document.revision,
                "manufacturer": document.manufacturer,
                "family": document.family,
            },
        )
    return [seen[key] for key in sorted(seen)]


def _build_publish_metadata(bucket_id: str, topic: str, records: list[TopicRecord]) -> dict[str, object]:
    digest_type = TOPIC_DIGEST_TYPE_MAP[topic]
    slug = _build_digest_slug(bucket_id, topic, digest_type=digest_type)
    metadata: dict[str, object] = {
        "digest_type": digest_type,
        "slug": slug,
        "status": "draft",
        "knowledge_record_ids": [],
        "cross_links": [],
        "tags": _build_tags(records, digest_type=digest_type),
    }
    if digest_type == "controller":
        metadata["controller_models"] = _controller_models(records)
        metadata["system_types"] = []
    elif digest_type == "fault-code":
        metadata["fault_code"] = _fault_code_value(records, fallback=slug)
        metadata["controller_models"] = _controller_models(records)
    elif digest_type == "symptom":
        metadata["symptom_key"] = _symptom_key(records, fallback=slug)
        metadata["system_types"] = []
    elif digest_type == "workflow-guidance":
        metadata["workflow_key"] = slug
    return metadata


def _build_digest_slug(bucket_id: str, topic: str, *, digest_type: str) -> str:
    bucket_slug = slugify(bucket_id)
    if digest_type == "controller":
        return f"{bucket_slug}-controller-digest"
    if digest_type == "fault-code":
        return f"{bucket_slug}-alarm-reference"
    if digest_type == "symptom":
        return f"{bucket_slug}-troubleshooting"
    return f"{bucket_slug}-{slugify(topic)}"


def _controller_models(records: list[TopicRecord]) -> list[str]:
    models: set[str] = set()
    for entry in records:
        models.update(entry.manifest.document.model_applicability)
    return sorted(models)


def _fault_code_value(records: list[TopicRecord], *, fallback: str) -> str:
    codes = sorted(
        {
            record.code.strip()
            for entry in records
            for record in [entry.record]
            if isinstance(record, AlarmDefinition) and record.code.strip()
        }
    )
    if not codes:
        return fallback
    if len(codes) == 1:
        return codes[0]
    return ", ".join(codes)


def _symptom_key(records: list[TopicRecord], *, fallback: str) -> str:
    symptoms = sorted(
        {
            slugify(record.symptom)
            for entry in records
            for record in [entry.record]
            if isinstance(record, TroubleshootingEntry) and record.symptom.strip()
        }
    )
    if not symptoms:
        return fallback
    if len(symptoms) == 1:
        return symptoms[0]
    return fallback


def _build_tags(records: list[TopicRecord], *, digest_type: str) -> list[str]:
    first_document = sorted(records, key=lambda entry: entry.doc_id)[0].manifest.document
    tags = {slugify(first_document.manufacturer), digest_type}
    family_tags = {slugify(_scope_family(entry)) for entry in records if _scope_family(entry)}
    if len(family_tags) == 1:
        tags.update(family_tags)
    if digest_type == "controller":
        tags.add("controller-family")
    return sorted(tags)


def _discover_bucket_ids(data_dir: Path) -> list[str]:
    bucket_ids: set[str] = set()
    for manifest in list_manifests(data_dir):
        if not (data_dir / "extracted" / manifest.doc_id).exists():
            continue
        bucket_ids.update(assignment.bucket_id for assignment in manifest.bucket_assignments)
    return sorted(bucket_ids)


def _load_topic_records(bucket_id: str, topic: str, *, data_dir: Path) -> list[TopicRecord]:
    return [entry for entry in _load_bucket_records(bucket_id, data_dir=data_dir) if classify_topic(entry) == topic]


def _load_bucket_records(bucket_id: str, *, data_dir: Path) -> list[TopicRecord]:
    entries: list[TopicRecord] = []
    for manifest in list_manifests(data_dir):
        if bucket_id not in {assignment.bucket_id for assignment in manifest.bucket_assignments}:
            continue
        extracted_dir = data_dir / "extracted" / manifest.doc_id
        if not extracted_dir.exists():
            continue
        sections = {section.section_id: section for section in load_sections(manifest.doc_id, data_dir=data_dir)}
        for record_type, record_path, record in _load_extracted_records(manifest.doc_id, data_dir=data_dir):
            if bucket_id not in {context.bucket_id for context in record.bucket_context}:
                continue
            section = sections.get(_section_id_from_record_path(record_path))
            if section is None:
                continue
            entries.append(
                TopicRecord(
                    doc_id=manifest.doc_id,
                    manifest=manifest,
                    section=section,
                    record_type=record_type,
                    record_path=record_path,
                    record=record,
                )
            )
    return entries


def _load_extracted_records(
    doc_id: str,
    *,
    data_dir: Path,
) -> list[tuple[str, Path, ExtractionSchemaModel]]:
    extracted_dir = data_dir / "extracted" / doc_id
    records: list[tuple[str, Path, ExtractionSchemaModel]] = []
    for record_dir in sorted(path for path in extracted_dir.iterdir() if path.is_dir() and path.name != "reviews"):
        model = get_schema_model(record_dir.name)
        for record_path in sorted(record_dir.glob("*.json")):
            record = model.model_validate_json(record_path.read_text(encoding="utf-8"))
            records.append((record_dir.name, record_path, record))
    return records


def _section_id_from_record_path(record_path: Path) -> str:
    return record_path.stem.rsplit("--", 2)[0]


def _extract_applicability(record: ExtractionSchemaModel) -> Applicability | None:
    if isinstance(record, (Procedure, SpecValue)):
        return record.applicability
    return None


def _prompt_claim(entry: TopicRecord) -> str:
    record = entry.record
    if isinstance(record, Procedure):
        return f"{record.title}: {'; '.join(step.instruction for step in record.steps)}"
    if isinstance(record, SpecValue):
        unit = f" {record.unit}" if record.unit else ""
        conditions = f" ({record.conditions})" if record.conditions else ""
        return f"{record.parameter}: {record.value}{unit}{conditions}"
    if isinstance(record, AlarmDefinition):
        return f"{record.code}: {record.description}; cause {record.cause}; remedy {record.remedy}"
    if isinstance(record, TroubleshootingEntry):
        return f"{record.symptom}; causes {', '.join(record.possible_causes)}; remedies {', '.join(record.remedies)}"
    return f"{entry.record_type} from {entry.section.title}"


def _nested_citation(doc_id: str, record: ProcedureStep | Warning) -> str:
    return f"[Source: {doc_id}, {record_page_label(record)}]"


def _filter_topic_records(bucket_id: str, topic: str, records: list[TopicRecord]) -> list[TopicRecord]:
    del bucket_id
    if not records:
        return []
    filtered = [entry for entry in records if _record_is_admissible_for_topic(entry, topic)]
    if not filtered:
        return []
    return _cohere_topic_scope(topic, filtered)


def _cohere_topic_scope(topic: str, records: list[TopicRecord]) -> list[TopicRecord]:
    if topic not in {"specifications", "startup_procedure", "shutdown_procedure", "maintenance_procedure"}:
        return records

    scope_counts = Counter(_scope_key(entry) for entry in records)
    if len(scope_counts) <= 1:
        return records

    dominant_scope, dominant_count = scope_counts.most_common(1)[0]
    minimum_count = max(2, math.ceil(len(records) * TOPIC_SCOPE_DOMINANCE_THRESHOLD))
    if dominant_count < minimum_count:
        return []
    return [entry for entry in records if _scope_key(entry) == dominant_scope]


def _record_is_admissible_for_topic(entry: TopicRecord, topic: str) -> bool:
    if topic == "specifications" and isinstance(entry.record, SpecValue):
        return _spec_is_high_signal(entry)
    if topic in TOPIC_DOMAIN_TERMS and isinstance(entry.record, Procedure):
        return _procedure_matches_topic(entry, topic)
    if topic == "troubleshooting" and isinstance(entry.record, TroubleshootingEntry):
        return _troubleshooting_is_field_relevant(entry)
    if topic == "alarm_reference" and isinstance(entry.record, AlarmDefinition):
        return _alarm_definition_is_field_relevant(entry)
    return True


def _spec_is_high_signal(entry: TopicRecord) -> bool:
    record = entry.record
    if not isinstance(record, SpecValue):
        return False
    context_text = _normalize_topic_text(
        " ".join(
            part
            for part in (
                entry.section.title,
                record.parameter,
                record.value,
                record.unit or "",
                record.conditions or "",
                entry.manifest.document.family,
            )
            if part
        )
    )
    parameter = _normalize_topic_text(record.parameter)
    if parameter in LOW_SIGNAL_SPEC_PARAMETERS:
        return False
    if any(term in context_text for term in LOW_SIGNAL_SPEC_TERMS):
        return False
    return True


def _procedure_matches_topic(entry: TopicRecord, topic: str) -> bool:
    record = entry.record
    if not isinstance(record, Procedure):
        return False
    text = _normalize_topic_text(
        " ".join(
            [
                entry.section.title,
                record.title,
                *[step.instruction for step in record.steps],
                entry.manifest.document.family,
            ]
        )
    )
    section_type = entry.section.section_type
    allowed_section_type = {
        "startup_procedure": "startup",
        "shutdown_procedure": "shutdown",
        "maintenance_procedure": "maintenance",
    }[topic]
    has_topic_terms = any(term in text for term in TOPIC_DOMAIN_TERMS[topic])
    has_ui_admin_terms = any(term in text for term in UI_ADMIN_EXCLUSION_TERMS)
    has_field_terms = any(term in text for term in FIELD_EQUIPMENT_TERMS)

    if section_type == allowed_section_type:
        if has_ui_admin_terms and not has_field_terms:
            return False
        return True
    if not has_topic_terms:
        return False
    if has_ui_admin_terms and not has_field_terms:
        return False
    return True


def _troubleshooting_is_field_relevant(entry: TopicRecord) -> bool:
    record = entry.record
    if not isinstance(record, TroubleshootingEntry):
        return False
    text = _normalize_topic_text(
        " ".join([record.symptom, *record.possible_causes, *record.remedies, entry.section.title])
    )
    return not any(term in text for term in UI_ADMIN_EXCLUSION_TERMS)


def _alarm_definition_is_field_relevant(entry: TopicRecord) -> bool:
    record = entry.record
    if not isinstance(record, AlarmDefinition):
        return False
    text = _normalize_topic_text(
        " ".join([record.code, record.description, record.cause, record.remedy, entry.section.title])
    )
    return any(term in text for term in {"alarm", "fault", "error", "indicator", "status"}) or not any(
        term in text for term in UI_ADMIN_EXCLUSION_TERMS
    )


def _scope_key(entry: TopicRecord) -> tuple[str, str]:
    applicability = _extract_applicability(entry.record)
    manufacturer = applicability.manufacturer if applicability is not None else entry.manifest.document.manufacturer
    family = applicability.family if applicability is not None else entry.manifest.document.family
    return (_normalize_topic_text(manufacturer), _normalize_topic_text(family))


def _scope_family(entry: TopicRecord) -> str:
    applicability = _extract_applicability(entry.record)
    if applicability is not None:
        return applicability.family
    return entry.manifest.document.family


def _normalize_topic_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()
