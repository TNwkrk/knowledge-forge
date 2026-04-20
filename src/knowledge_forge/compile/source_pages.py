"""Source-page compilation for extracted manuals."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from yaml import safe_dump

from knowledge_forge.extract.engine import load_sections
from knowledge_forge.extract.reviewability import assess_section_reviewability
from knowledge_forge.extract.schemas import ExtractionSchemaModel, get_schema_model
from knowledge_forge.intake.importer import get_data_dir, list_manifests, load_manifest
from knowledge_forge.intake.manifest import STATUS_ORDER, DocumentStatus, ManifestEntry, slugify
from knowledge_forge.parse.sectioning import Section

COMPILATION_VERSION = "source-pages-v1"
GENERATED_BY = "knowledge-forge"
PUBLISH_RUN_PLACEHOLDER = "unpublished"


class CompileMetadata(BaseModel):
    """Metadata about one source-page compilation run."""

    model_config = ConfigDict(extra="forbid")

    generated_at: str
    extraction_versions: list[str]
    parser_versions: list[str]
    record_counts: dict[str, int]
    review_flag_count: int = Field(ge=0)


class CompiledPage(BaseModel):
    """A compiled Markdown page plus its metadata."""

    model_config = ConfigDict(extra="forbid")

    output_path: Path
    doc_id: str
    frontmatter: dict[str, Any]
    content: str
    compile_metadata: CompileMetadata

    def render(self) -> str:
        """Render the page as Markdown with YAML frontmatter."""
        frontmatter = safe_dump(self.frontmatter, sort_keys=False).strip()
        return f"---\n{frontmatter}\n---\n\n{self.content.rstrip()}\n"


class ReviewFlag(BaseModel):
    """Persisted extraction review metadata."""

    model_config = ConfigDict(extra="forbid")

    doc_id: str
    section_id: str
    record_type: str
    reasons: list[str]
    min_confidence: float
    record_ids: list[str]
    record_confidences: list[float]
    repair_attempts: int
    errors: list[str]


def compile_source_page(doc_id: str, *, data_dir: Path | None = None) -> CompiledPage:
    """Compile one reviewable Markdown page for a source manual."""
    resolved_data_dir = get_data_dir(data_dir)
    manifest = load_manifest(resolved_data_dir, doc_id)
    sections = load_sections(doc_id, data_dir=resolved_data_dir)
    extracted_records = _load_extracted_records(doc_id, data_dir=resolved_data_dir)
    review_flags = _load_review_flags(doc_id, data_dir=resolved_data_dir)

    if not extracted_records and not review_flags:
        raise FileNotFoundError(f"no extracted records or review flags found for doc_id '{doc_id}'")

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    section_counts = _count_records_by_section(sections, extracted_records)
    record_counts = dict(sorted(Counter(record_type for record_type, _, _ in extracted_records).items()))
    extraction_versions = sorted({record.extraction_version for _, _, record in extracted_records})
    parser_versions = sorted({record.parser_version for _, _, record in extracted_records})

    frontmatter = _build_frontmatter(
        manifest=manifest,
        generated_at=generated_at,
        extraction_versions=extraction_versions,
    )
    content = _render_content(
        manifest=manifest,
        sections=sections,
        extracted_records=extracted_records,
        review_flags=review_flags,
        section_counts=section_counts,
        record_counts=record_counts,
        data_dir=resolved_data_dir,
    )
    output_path = resolved_data_dir / "compiled" / "source-pages" / f"{doc_id}.md"
    page = CompiledPage(
        output_path=output_path,
        doc_id=doc_id,
        frontmatter=frontmatter,
        content=content,
        compile_metadata=CompileMetadata(
            generated_at=generated_at,
            extraction_versions=extraction_versions,
            parser_versions=parser_versions,
            record_counts=record_counts,
            review_flag_count=len(review_flags),
        ),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(page.render(), encoding="utf-8")
    _mark_manifest_compiled(manifest, data_dir=resolved_data_dir)
    return page


def compile_all_source_pages(*, data_dir: Path | None = None) -> list[CompiledPage]:
    """Compile source pages for every document with extracted records."""
    resolved_data_dir = get_data_dir(data_dir)
    pages: list[CompiledPage] = []
    for manifest in list_manifests(resolved_data_dir):
        if (resolved_data_dir / "extracted" / manifest.doc_id).exists():
            pages.append(compile_source_page(manifest.doc_id, data_dir=resolved_data_dir))
    return pages


def _build_frontmatter(
    *,
    manifest: ManifestEntry,
    generated_at: str,
    extraction_versions: list[str],
) -> dict[str, Any]:
    document = manifest.document
    title = f"Source Manual: {document.manufacturer} {document.family} {document.document_type} ({document.revision})"
    return {
        "title": title,
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": [
            {
                "doc_id": manifest.doc_id,
                "revision": document.revision,
                "manufacturer": document.manufacturer,
                "family": document.family,
            }
        ],
        "generated_at": generated_at,
        "extraction_version": ", ".join(extraction_versions) if extraction_versions else "unknown",
        "compilation_version": COMPILATION_VERSION,
        "doc_id": manifest.doc_id,
        "document_type": document.document_type,
        "models": document.model_applicability,
        "status": DocumentStatus.COMPILED.value,
    }


def _render_content(
    *,
    manifest: ManifestEntry,
    sections: list[Section],
    extracted_records: list[tuple[str, Path, ExtractionSchemaModel]],
    review_flags: list[ReviewFlag],
    section_counts: dict[str, int],
    record_counts: dict[str, int],
    data_dir: Path,
) -> str:
    document = manifest.document
    extraction_versions = sorted({record.extraction_version for _, _, record in extracted_records})
    parser_versions = sorted({record.parser_version for _, _, record in extracted_records})
    bucket_ids = sorted({context.bucket_id for _, _, record in extracted_records for context in record.bucket_context})
    reviewability_by_section = {section.section_id: assess_section_reviewability(section) for section in sections}
    visible_sections = [section for section in sections if reviewability_by_section[section.section_id].reviewable]
    suppressed_sections = [
        section for section in sections if not reviewability_by_section[section.section_id].reviewable
    ]
    lines = [
        f"# Source Manual: {document.manufacturer} {document.family} {document.document_type}",
        "",
        "## Document Metadata",
        "",
        f"- Doc ID: `{manifest.doc_id}`",
        f"- Source file: `{document.source_path}`",
        f"- Manufacturer: {document.manufacturer}",
        f"- Family: {document.family}",
        f"- Models: {', '.join(document.model_applicability)}",
        f"- Document type: {document.document_type}",
        f"- Revision: {document.revision}",
        f"- Publication date: {document.publication_date.isoformat() if document.publication_date else 'unknown'}",
        f"- Language: {document.language}",
        f"- Pipeline status: `{DocumentStatus.COMPILED.value}`",
        "",
        "## Section Index",
        "",
    ]

    for section in visible_sections:
        anchor = _anchor(section.title)
        start_page, end_page = section.page_range
        page_label = _format_page_range(start_page, end_page)
        heading_path = " > ".join(section.heading_path) if section.heading_path else section.title
        lines.append(
            f"- [{section.title}](#{anchor}) "
            f"({section.section_type}, {page_label}, {section_counts.get(section.section_id, 0)} records)"
        )
        lines.append(f"  Path: `{heading_path}`")

    lines.extend(
        [
            "",
            "## Extraction Summary",
            "",
        ]
    )
    for record_type, count in record_counts.items():
        lines.append(f"- `{record_type}`: {count}")

    lines.extend(
        [
            "",
            "## Quality Notes",
            "",
        ]
    )
    low_confidence_threshold = min((flag.min_confidence for flag in review_flags), default=0.75)
    quality_lines = _render_quality_notes(
        review_flags=review_flags,
        extracted_records=extracted_records,
        low_confidence_threshold=low_confidence_threshold,
        suppressed_sections=suppressed_sections,
    )
    lines.extend(quality_lines)

    lines.extend(
        [
            "",
            "## Provenance Chain",
            "",
            f"- Parser versions: {', '.join(parser_versions) if parser_versions else 'unknown'}",
            f"- Extraction versions: {', '.join(extraction_versions) if extraction_versions else 'unknown'}",
            f"- Bucket scope: {', '.join(bucket_ids) if bucket_ids else 'none'}",
        ]
    )

    records_by_section = defaultdict(list)
    for record_type, record_path, record in extracted_records:
        section_id = _section_id_from_record_path(record_path)
        records_by_section[section_id].append((record_type, record_path, record))

    lines.extend(
        [
            "",
            "## Extracted Sections",
            "",
        ]
    )
    for section in visible_sections:
        lines.extend(
            [
                f"### {section.title}",
                "",
                f"- Section type: `{section.section_type}`",
                f"- Page range: {_format_page_range(*section.page_range)}",
                f"- Heading path: `{' > '.join(section.heading_path) if section.heading_path else section.title}`",
            ]
        )
        section_records = sorted(records_by_section.get(section.section_id, []), key=lambda item: str(item[1]))
        if not section_records:
            lines.extend(["- Extracted records: none", ""])
            continue
        lines.append("- Extracted records:")
        for record_type, record_path, record in section_records:
            relative_path = Path("..") / Path("..") / record_path.relative_to(data_dir)
            page_label = _format_page_range(record.source_page_range.start_page, record.source_page_range.end_page)
            lines.append(
                "  - "
                f"`{record_type}` `{record_path.stem}` "
                f"(confidence {record.confidence:.3f}, pages {page_label}) "
                f"[artifact]({relative_path.as_posix()})"
            )
        lines.append("")

    return "\n".join(lines).rstrip()


def _render_quality_notes(
    *,
    review_flags: list[ReviewFlag],
    extracted_records: list[tuple[str, Path, ExtractionSchemaModel]],
    low_confidence_threshold: float = 0.75,
    suppressed_sections: list[Section] | None = None,
) -> list[str]:
    low_confidence_rows = [
        (record_type, record_path.stem, record.confidence)
        for record_type, record_path, record in extracted_records
        if record.confidence < low_confidence_threshold
    ]
    lines: list[str] = []
    if not low_confidence_rows and not review_flags and not suppressed_sections:
        return ["- No low-confidence or flagged extractions."]

    if low_confidence_rows:
        lines.append(f"- Low-confidence records (threshold: {low_confidence_threshold:.2f}):")
        for record_type, record_id, confidence in low_confidence_rows:
            lines.append(f"  - `{record_type}` `{record_id}` at confidence {confidence:.3f}")

    if review_flags:
        lines.append("- Review flags:")
        for flag in review_flags:
            reasons = ", ".join(flag.reasons) if flag.reasons else "unspecified"
            lines.append(
                f"  - Section `{flag.section_id}` `{flag.record_type}` "
                f"(reasons: {reasons}; repair_attempts: {flag.repair_attempts})"
            )
            if flag.errors:
                lines.append(f"    Errors: {'; '.join(flag.errors)}")
    if suppressed_sections:
        examples = ", ".join(f"`{section.title}`" for section in suppressed_sections[:5])
        lines.append(
            f"- Suppressed non-reviewable sections from reviewer-facing output: {len(suppressed_sections)}"
            + (f" (examples: {examples})" if examples else "")
        )
    return lines


def _load_extracted_records(
    doc_id: str,
    *,
    data_dir: Path,
) -> list[tuple[str, Path, ExtractionSchemaModel]]:
    extracted_dir = data_dir / "extracted" / doc_id
    if not extracted_dir.exists():
        return []

    records: list[tuple[str, Path, ExtractionSchemaModel]] = []
    for record_dir in sorted(path for path in extracted_dir.iterdir() if path.is_dir() and path.name != "reviews"):
        model = get_schema_model(record_dir.name)
        for record_path in sorted(record_dir.glob("*.json")):
            record = model.model_validate_json(record_path.read_text(encoding="utf-8"))
            records.append((record_dir.name, record_path, record))
    return records


def _load_review_flags(doc_id: str, *, data_dir: Path) -> list[ReviewFlag]:
    review_dir = data_dir / "extracted" / doc_id / "reviews"
    if not review_dir.exists():
        return []
    return [
        ReviewFlag.model_validate(json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(review_dir.glob("*.json"))
    ]


def _count_records_by_section(
    sections: list[Section],
    extracted_records: list[tuple[str, Path, ExtractionSchemaModel]],
) -> dict[str, int]:
    counts = {section.section_id: 0 for section in sections}
    for _, record_path, _ in extracted_records:
        section_id = _section_id_from_record_path(record_path)
        counts[section_id] = counts.get(section_id, 0) + 1
    return counts


def _section_id_from_record_path(record_path: Path) -> str:
    return record_path.stem.rsplit("--", 2)[0]


def _format_page_range(start_page: int | None, end_page: int | None) -> str:
    if start_page is None and end_page is None:
        return "pages unknown"
    if start_page == end_page:
        return f"p.{start_page}"
    if start_page is None:
        return f"through p.{end_page}"
    if end_page is None:
        return f"from p.{start_page}"
    return f"pp.{start_page}-{end_page}"


def _anchor(title: str) -> str:
    return slugify(title)


def _mark_manifest_compiled(manifest: ManifestEntry, *, data_dir: Path) -> None:
    if STATUS_ORDER.index(manifest.document.status) >= STATUS_ORDER.index(DocumentStatus.COMPILED):
        return
    manifest_path = data_dir / "manifests" / f"{manifest.doc_id}.yaml"
    updated = manifest.transition_status(DocumentStatus.COMPILED, reason="source page compiled")
    manifest_path.write_text(updated.to_yaml(), encoding="utf-8")
