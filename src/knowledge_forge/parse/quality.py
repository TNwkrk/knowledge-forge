"""Canonical parse artifacts and quality scoring helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from yaml import safe_load

from knowledge_forge.intake.importer import get_data_dir

HEADING_LABELS: frozenset[str] = frozenset(
    {
        "document_title",
        "page_header",
        "section_header",
        "section_title",
        "subtitle",
        "title",
    }
)


class ParseTextArtifact(BaseModel):
    """Canonical representation of parsed text content."""

    item_ref: str | None = None
    label: str
    text: str
    page_numbers: list[int] = Field(default_factory=list)


class ParseTableArtifact(BaseModel):
    """Canonical representation of a parsed table."""

    item_ref: str | None = None
    label: str | None = None
    page_numbers: list[int] = Field(default_factory=list)
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    data: list[list[Any]] = Field(default_factory=list)


class ParsePageArtifact(BaseModel):
    """Canonical representation of page metadata from a parse."""

    page_number: int = Field(ge=1)
    width: float | None = None
    height: float | None = None
    source_ref: str | None = None


class StructuredParseArtifact(BaseModel):
    """Canonical parser-neutral structure artifact."""

    doc_id: str
    parser: str
    parser_version: str
    page_count: int = Field(ge=0)
    texts: list[ParseTextArtifact] = Field(default_factory=list)
    tables: list[ParseTableArtifact] = Field(default_factory=list)
    pages: list[ParsePageArtifact] = Field(default_factory=list)


class HeadingNode(BaseModel):
    """A single node in the heading tree artifact."""

    title: str
    label: str
    level: int = Field(ge=1)
    page_number: int | None = Field(default=None, ge=1)
    item_ref: str | None = None
    children: list["HeadingNode"] = Field(default_factory=list)


class HeadingTreeArtifact(BaseModel):
    """Persisted heading tree output."""

    doc_id: str
    headings: list[HeadingNode] = Field(default_factory=list)


class TablesArtifact(BaseModel):
    """Persisted tables output."""

    doc_id: str
    tables: list[ParseTableArtifact] = Field(default_factory=list)


class PageMapItem(BaseModel):
    """A page-mapped parse item."""

    item_ref: str | None = None
    item_type: Literal["text", "table"]
    label: str | None = None
    page_numbers: list[int] = Field(default_factory=list)
    text: str | None = None
    table_rows: int | None = Field(default=None, ge=0)


class PageMapArtifact(BaseModel):
    """Persisted content-to-page map."""

    doc_id: str
    items: list[PageMapItem] = Field(default_factory=list)


class ParseMetadata(BaseModel):
    """Persisted parser execution metadata."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    doc_id: str
    parser: str
    parser_version: str
    processed_at: str
    processing_time_seconds: float = Field(ge=0)
    page_count: int = Field(ge=0)
    status: str
    input_path: str
    input_checksum: str
    document_hash: str | None = None
    timings: Any = None
    confidence: Any = None
    errors: list[Any] = Field(default_factory=list)
    fallback_parser: str | None = None
    fallback_attempted: bool = False
    fallback_reason: str | None = None
    candidate_runs: list["ParseCandidateRun"] = Field(default_factory=list)


class ParseCandidateRun(BaseModel):
    """A single parser candidate considered for a document."""

    parser: str
    parser_version: str
    artifact_dir: str
    overall_score: float | None = Field(default=None, ge=0, le=100)
    passes_threshold: bool | None = None
    selected: bool = False
    reason: str | None = None


class ParseQualityMetrics(BaseModel):
    """Per-metric parse quality breakdown."""

    heading_coverage: float = Field(ge=0, le=100)
    table_extraction_rate: float = Field(ge=0, le=100)
    text_completeness: float = Field(ge=0, le=100)
    structure_depth: float = Field(ge=0, le=100)
    page_coverage: float = Field(ge=0, le=100)


class ParseQualityThresholds(BaseModel):
    """Configurable quality gates for parse artifacts."""

    minimum_quality_score: float = Field(default=70.0, ge=0, le=100)


class ParseQualityReport(BaseModel):
    """Final parse quality report saved alongside parse artifacts."""

    doc_id: str
    parser: str
    parser_version: str
    page_count: int = Field(ge=0)
    generated_at: str
    artifact_schema_version: str = "1.0"
    metrics: ParseQualityMetrics
    overall_score: float = Field(ge=0, le=100)
    thresholds: ParseQualityThresholds
    passes_threshold: bool


class ParseArtifactBundle(BaseModel):
    """Loaded parse artifacts used for validation and scoring."""

    doc_id: str
    content: str
    structure: StructuredParseArtifact
    headings: HeadingTreeArtifact
    tables: TablesArtifact
    page_map: PageMapArtifact
    meta: ParseMetadata


def score_bundle(
    bundle: ParseArtifactBundle,
    *,
    config_path: Path | None = None,
) -> ParseQualityReport:
    """Score an already-loaded artifact bundle and return the quality report (no disk I/O)."""
    thresholds = load_parse_quality_thresholds(config_path=config_path)
    metrics = ParseQualityMetrics(
        heading_coverage=_score_heading_coverage(bundle.structure, bundle.headings),
        table_extraction_rate=_score_table_extraction_rate(bundle.tables, bundle.page_map),
        text_completeness=_score_text_completeness(bundle.structure, bundle.content),
        structure_depth=_score_structure_depth(bundle.headings),
        page_coverage=_score_page_coverage(bundle.page_map, bundle.meta.page_count),
    )
    overall_score = round(
        (
            metrics.heading_coverage * 0.25
            + metrics.table_extraction_rate * 0.20
            + metrics.text_completeness * 0.25
            + metrics.structure_depth * 0.15
            + metrics.page_coverage * 0.15
        ),
        2,
    )
    return ParseQualityReport(
        doc_id=bundle.doc_id,
        parser=bundle.meta.parser,
        parser_version=bundle.meta.parser_version,
        page_count=bundle.meta.page_count,
        generated_at=datetime.now(timezone.utc).isoformat(),
        metrics=metrics,
        overall_score=overall_score,
        thresholds=thresholds,
        passes_threshold=overall_score >= thresholds.minimum_quality_score,
    )


def score_parse(
    doc_id: str,
    *,
    data_dir: Path | None = None,
    config_path: Path | None = None,
    write_report: bool = True,
) -> ParseQualityReport:
    """Load parse artifacts from disk, score them, optionally persist `quality.json`, and return the report."""
    bundle = load_parse_artifacts(doc_id, data_dir=data_dir)
    report = score_bundle(bundle, config_path=config_path)
    if write_report:
        output_path = get_data_dir(data_dir) / "parsed" / doc_id / "quality.json"
        output_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return report


def load_parse_artifacts(doc_id: str, *, data_dir: Path | None = None) -> ParseArtifactBundle:
    """Load and validate the canonical parse artifacts for a document."""
    resolved_data_dir = get_data_dir(data_dir)
    output_dir = resolved_data_dir / "parsed" / doc_id
    if not output_dir.exists():
        raise FileNotFoundError(f"parse artifacts not found for doc_id '{doc_id}'")

    content_path = output_dir / "content.md"
    structure_path = output_dir / "structure.json"
    headings_path = output_dir / "headings.json"
    tables_path = output_dir / "tables.json"
    page_map_path = output_dir / "page_map.json"
    meta_path = output_dir / "meta.json"
    for path in (content_path, structure_path, headings_path, tables_path, page_map_path, meta_path):
        if not path.exists():
            raise FileNotFoundError(f"required parse artifact missing: {path}")

    return ParseArtifactBundle(
        doc_id=doc_id,
        content=content_path.read_text(encoding="utf-8"),
        structure=StructuredParseArtifact.model_validate_json(structure_path.read_text(encoding="utf-8")),
        headings=HeadingTreeArtifact.model_validate_json(headings_path.read_text(encoding="utf-8")),
        tables=TablesArtifact.model_validate_json(tables_path.read_text(encoding="utf-8")),
        page_map=PageMapArtifact.model_validate_json(page_map_path.read_text(encoding="utf-8")),
        meta=ParseMetadata.model_validate_json(meta_path.read_text(encoding="utf-8")),
    )


def load_parse_quality_thresholds(*, config_path: Path | None = None) -> ParseQualityThresholds:
    """Load parse quality thresholds from `config/pipeline.yaml` when present."""
    path = config_path or Path("config/pipeline.yaml")
    if not path.exists():
        return ParseQualityThresholds()

    payload = safe_load(path.read_text(encoding="utf-8")) or {}
    parse_config: dict[str, Any] = payload.get("stages", {}).get("parse", {}) or {}
    if "minimum_quality_score" not in parse_config:
        return ParseQualityThresholds()
    return ParseQualityThresholds(minimum_quality_score=parse_config["minimum_quality_score"])


def _score_heading_coverage(structure: StructuredParseArtifact, headings: HeadingTreeArtifact) -> float:
    """Score how many heading-like items were preserved in the heading tree."""
    heading_candidates = sum(1 for item in structure.texts if item.label.casefold() in HEADING_LABELS)
    actual_headings = len(_flatten_headings(headings.headings))
    if heading_candidates == 0:
        return 100.0 if actual_headings == 0 else 0.0
    return round(min(actual_headings / heading_candidates, 1.0) * 100, 2)


def _score_table_extraction_rate(tables: TablesArtifact, page_map: PageMapArtifact) -> float:
    """Score whether extracted tables are non-empty and page-mapped."""
    if not tables.tables:
        return 100.0

    mapped_refs = {item.item_ref for item in page_map.items if item.item_type == "table" and item.item_ref is not None}
    valid_tables = sum(
        1
        for table in tables.tables
        if table.row_count > 0
        and table.column_count > 0
        and table.page_numbers
        and table.item_ref is not None
        and table.item_ref in mapped_refs
    )
    return round((valid_tables / len(tables.tables)) * 100, 2)


def _score_text_completeness(structure: StructuredParseArtifact, content: str) -> float:
    """Score markdown completeness relative to the canonical text blocks."""
    structure_characters = sum(len(item.text) for item in structure.texts if item.text.strip())
    if structure_characters <= 0:
        return 0.0

    markdown_characters = sum(1 for char in content if not char.isspace())
    return round(min(markdown_characters / structure_characters, 1.0) * 100, 2)


def _score_structure_depth(headings: HeadingTreeArtifact) -> float:
    """Score heading hierarchy richness and nesting depth."""
    flat = _flatten_headings(headings.headings)
    if not flat:
        return 0.0

    max_depth = max(_heading_depth(node) for node in headings.headings)
    level_span = max(node.level for node in flat) - min(node.level for node in flat) + 1
    depth_component = min(max_depth / 3, 1.0)
    level_component = min(level_span / 3, 1.0)
    return round(((depth_component * 0.6) + (level_component * 0.4)) * 100, 2)


def _score_page_coverage(page_map: PageMapArtifact, page_count: int) -> float:
    """Score how many document pages are represented in the page map."""
    if page_count <= 0:
        return 0.0
    covered_pages = {page for item in page_map.items for page in item.page_numbers}
    return round(min(len(covered_pages) / page_count, 1.0) * 100, 2)


def _flatten_headings(nodes: list[HeadingNode]) -> list[HeadingNode]:
    """Flatten a heading tree into a single ordered list."""
    flattened: list[HeadingNode] = []
    for node in nodes:
        flattened.append(node)
        flattened.extend(_flatten_headings(node.children))
    return flattened


def _heading_depth(node: HeadingNode) -> int:
    """Return the maximum nesting depth beneath a heading node."""
    if not node.children:
        return 1
    return 1 + max(_heading_depth(child) for child in node.children)
