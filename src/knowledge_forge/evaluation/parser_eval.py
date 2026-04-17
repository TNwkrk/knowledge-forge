"""Parser evaluation helpers for benchmark fixture sets."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from pydantic import BaseModel, Field

from knowledge_forge.intake.manifest import slugify
from knowledge_forge.parse.quality import (
    HeadingNode,
    HeadingTreeArtifact,
    PageMapArtifact,
    ParseArtifactBundle,
    ParseMetadata,
    StructuredParseArtifact,
    TablesArtifact,
)
from knowledge_forge.parse.sectioning import build_sections_from_artifacts


class ExpectedTable(BaseModel):
    """Ground-truth table expectations for one fixture."""

    rows: int = Field(ge=0)
    columns: int = Field(ge=0)
    required_cells: list[str] = Field(default_factory=list)


class ParserEvalFixtureGroundTruth(BaseModel):
    """Expected parser output characteristics for one benchmark fixture."""

    fixture_id: str
    title: str
    document_type: str
    document_class: str = "authoritative-technical"
    expected_headings: list[str] = Field(default_factory=list)
    expected_tables: list[ExpectedTable] = Field(default_factory=list)
    required_text: list[str] = Field(default_factory=list)
    expected_section_count: int = Field(ge=0)
    expected_section_types: list[str] = Field(default_factory=list)


class ParserEvalMetrics(BaseModel):
    """Stable metric surface for parser evaluation reports."""

    heading_accuracy: float = Field(ge=0, le=100)
    table_extraction_accuracy: float = Field(ge=0, le=100)
    text_completeness: float = Field(ge=0, le=100)
    structure_fidelity: float = Field(ge=0, le=100)


class ParserFixtureScore(BaseModel):
    """Per-fixture parser evaluation result."""

    fixture_id: str
    title: str
    source_pdf: str
    parser: str
    parser_version: str
    metrics: ParserEvalMetrics
    overall_score: float = Field(ge=0, le=100)
    expected_section_types: list[str] = Field(default_factory=list)
    actual_section_types: list[str] = Field(default_factory=list)


class ParserEvalReport(BaseModel):
    """Aggregate parser evaluation report for one committed fixture set."""

    fixture_set: str
    parser: str
    parser_versions: list[str] = Field(default_factory=list)
    generated_at: str
    report_schema_version: str = "1.0"
    metrics: ParserEvalMetrics
    overall_score: float = Field(ge=0, le=100)
    fixture_reports: list[ParserFixtureScore] = Field(default_factory=list)


def evaluate_parser(fixture_set: str, parser: str) -> ParserEvalReport:
    """Evaluate committed parser artifacts for one fixture set and parser lane."""
    fixture_root = _fixture_set_root(fixture_set)
    repo_root = Path(__file__).resolve().parents[3]
    fixture_reports: list[ParserFixtureScore] = []
    parser_versions: list[str] = []

    for fixture_dir in sorted(path for path in fixture_root.iterdir() if path.is_dir()):
        ground_truth = ParserEvalFixtureGroundTruth.model_validate_json(
            (fixture_dir / "ground_truth.json").read_text(encoding="utf-8")
        )
        if ground_truth.fixture_id != fixture_dir.name:
            raise ValueError(
                f"fixture_id mismatch in '{fixture_dir}': "
                f"ground_truth.fixture_id={ground_truth.fixture_id!r} "
                f"but directory name is {fixture_dir.name!r}"
            )
        bundle = _load_fixture_bundle(fixture_dir, parser)
        metrics = ParserEvalMetrics(
            heading_accuracy=_score_heading_accuracy(ground_truth.expected_headings, bundle.headings),
            table_extraction_accuracy=_score_table_accuracy(ground_truth.expected_tables, bundle.tables),
            text_completeness=_score_text_completeness(ground_truth.required_text, bundle.content),
            structure_fidelity=_score_structure_fidelity(ground_truth, bundle),
        )
        overall_score = round(
            mean(
                [
                    metrics.heading_accuracy,
                    metrics.table_extraction_accuracy,
                    metrics.text_completeness,
                    metrics.structure_fidelity,
                ]
            ),
            2,
        )
        actual_section_types = _actual_section_types(bundle, ground_truth)
        fixture_reports.append(
            ParserFixtureScore(
                fixture_id=ground_truth.fixture_id,
                title=ground_truth.title,
                source_pdf=str((fixture_dir / "source.pdf").relative_to(repo_root)),
                parser=bundle.meta.parser,
                parser_version=bundle.meta.parser_version,
                metrics=metrics,
                overall_score=overall_score,
                expected_section_types=ground_truth.expected_section_types,
                actual_section_types=actual_section_types,
            )
        )
        parser_versions.append(bundle.meta.parser_version)

    if not fixture_reports:
        raise FileNotFoundError(f"no fixtures found in parser eval set '{fixture_set}'")

    aggregate_metrics = ParserEvalMetrics(
        heading_accuracy=round(mean(report.metrics.heading_accuracy for report in fixture_reports), 2),
        table_extraction_accuracy=round(
            mean(report.metrics.table_extraction_accuracy for report in fixture_reports), 2
        ),
        text_completeness=round(mean(report.metrics.text_completeness for report in fixture_reports), 2),
        structure_fidelity=round(mean(report.metrics.structure_fidelity for report in fixture_reports), 2),
    )
    return ParserEvalReport(
        fixture_set=fixture_set,
        parser=parser,
        parser_versions=sorted(set(parser_versions)),
        generated_at=datetime.now(timezone.utc).isoformat(),
        metrics=aggregate_metrics,
        overall_score=round(mean(report.overall_score for report in fixture_reports), 2),
        fixture_reports=fixture_reports,
    )


def _fixture_set_root(fixture_set: str) -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    root = repo_root / "tests" / "fixtures" / "parser_eval" / slugify(fixture_set)
    if not root.exists():
        raise FileNotFoundError(f"parser eval fixture set not found: {root}")
    return root


def _load_fixture_bundle(fixture_dir: Path, parser: str) -> ParseArtifactBundle:
    parser_dir = fixture_dir / "parsers" / slugify(parser)
    if not parser_dir.exists():
        raise FileNotFoundError(f"parser artifacts not found for '{parser}' in fixture '{fixture_dir.name}'")

    return ParseArtifactBundle(
        doc_id=fixture_dir.name,
        content=(parser_dir / "content.md").read_text(encoding="utf-8"),
        structure=StructuredParseArtifact.model_validate_json(
            (parser_dir / "structure.json").read_text(encoding="utf-8")
        ),
        headings=HeadingTreeArtifact.model_validate_json((parser_dir / "headings.json").read_text(encoding="utf-8")),
        tables=TablesArtifact.model_validate_json((parser_dir / "tables.json").read_text(encoding="utf-8")),
        page_map=PageMapArtifact.model_validate_json((parser_dir / "page_map.json").read_text(encoding="utf-8")),
        meta=ParseMetadata.model_validate_json((parser_dir / "meta.json").read_text(encoding="utf-8")),
    )


def _score_heading_accuracy(expected_headings: list[str], headings: HeadingTreeArtifact) -> float:
    if not expected_headings:
        return 100.0

    actual = [_normalize_text(node.title) for node in _flatten_headings(headings.headings)]
    if not actual:
        return 0.0

    expected = [_normalize_text(value) for value in expected_headings]
    matched = sum(1 for title in expected if title in actual)
    precision = matched / len(actual)
    recall = matched / len(expected)
    if precision + recall == 0:
        return 0.0
    return round((2 * precision * recall / (precision + recall)) * 100, 2)


def _score_table_accuracy(expected_tables: list[ExpectedTable], tables: TablesArtifact) -> float:
    if not expected_tables:
        return 100.0 if not tables.tables else 0.0

    remaining = list(tables.tables)
    matched = 0
    for expected in expected_tables:
        for index, table in enumerate(remaining):
            if table.row_count != expected.rows or table.column_count != expected.columns:
                continue
            flattened = {_normalize_text(str(cell)) for row in table.data for cell in row}
            if not all(_normalize_text(value) in flattened for value in expected.required_cells):
                continue
            matched += 1
            remaining.pop(index)
            break
    return round((matched / len(expected_tables)) * 100, 2)


def _score_text_completeness(required_text: list[str], content: str) -> float:
    if not required_text:
        return 100.0

    haystack = _normalize_text(content)
    matched = sum(1 for snippet in required_text if _normalize_text(snippet) in haystack)
    return round((matched / len(required_text)) * 100, 2)


def _score_structure_fidelity(ground_truth: ParserEvalFixtureGroundTruth, bundle: ParseArtifactBundle) -> float:
    sections = build_sections_from_artifacts(
        doc_id=bundle.doc_id,
        structure=bundle.structure,
        heading_tree=bundle.headings,
        document_type=ground_truth.document_type,
        document_class=ground_truth.document_class,
    )
    actual_count = len(sections)
    if ground_truth.expected_section_count == 0:
        count_score = 100.0 if actual_count == 0 else 0.0
    else:
        count_score = max(0.0, 100.0 - (abs(actual_count - ground_truth.expected_section_count) * 25.0))

    expected_types = {_normalize_text(value) for value in ground_truth.expected_section_types}
    actual_types = {_normalize_text(section.section_type) for section in sections}
    if not expected_types:
        type_score = 100.0
    else:
        type_score = round((len(expected_types & actual_types) / len(expected_types)) * 100, 2)

    return round((count_score * 0.4) + (type_score * 0.6), 2)


def _actual_section_types(bundle: ParseArtifactBundle, ground_truth: ParserEvalFixtureGroundTruth) -> list[str]:
    sections = build_sections_from_artifacts(
        doc_id=bundle.doc_id,
        structure=bundle.structure,
        heading_tree=bundle.headings,
        document_type=ground_truth.document_type,
        document_class=ground_truth.document_class,
    )
    seen: list[str] = []
    for section in sections:
        if section.section_type not in seen:
            seen.append(section.section_type)
    return seen


def _flatten_headings(nodes: list[HeadingNode]) -> list[HeadingNode]:
    flattened: list[HeadingNode] = []
    for node in nodes:
        flattened.append(node)
        flattened.extend(_flatten_headings(node.children))
    return flattened


def _normalize_text(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def write_parser_report(report: ParserEvalReport, *, output_dir: Path) -> Path:
    """Persist a parser evaluation report under the local data directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{slugify(report.fixture_set)}--{slugify(report.parser)}.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return report_path
