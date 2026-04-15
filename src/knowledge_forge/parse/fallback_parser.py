"""Optional fallback parser lane for low-quality primary parses."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from ocrmypdf.pdfinfo import PdfInfo
from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.intake.importer import get_data_dir
from knowledge_forge.intake.manifest import compute_sha256
from knowledge_forge.parse.quality import (
    HeadingNode,
    HeadingTreeArtifact,
    PageMapArtifact,
    PageMapItem,
    ParseArtifactBundle,
    ParseMetadata,
    ParsePageArtifact,
    ParseQualityReport,
    ParseTableArtifact,
    ParseTextArtifact,
    StructuredParseArtifact,
    TablesArtifact,
    score_bundle,
)


class FallbackParseResult(BaseModel):
    """Paths to the parse artifacts produced for a fallback parser run."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    doc_id: str
    parser: str
    content_path: Path
    structure_path: Path
    headings_path: Path
    tables_path: Path
    page_map_path: Path
    meta_path: Path
    quality_path: Path
    parser_version: str
    page_count: int
    processing_time: float
    quality_report: ParseQualityReport


class FallbackTextItem(BaseModel):
    """Canonical text item returned by a fallback parser backend."""

    item_ref: str | None = None
    label: str = "text"
    text: str
    page_numbers: list[int] = Field(default_factory=list)


class FallbackTableItem(BaseModel):
    """Canonical table item returned by a fallback parser backend."""

    item_ref: str | None = None
    label: str | None = "table"
    page_numbers: list[int] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)


class FallbackPayload(BaseModel):
    """Normalized fallback parser output before artifact persistence."""

    parser: str = "marker"
    parser_version: str = "unknown"
    markdown: str
    texts: list[FallbackTextItem] = Field(default_factory=list)
    tables: list[FallbackTableItem] = Field(default_factory=list)
    pages: list[ParsePageArtifact] = Field(default_factory=list)
    status: str = "success"
    timings: dict[str, Any] = Field(default_factory=dict)
    confidence: dict[str, Any] | None = None
    errors: list[Any] = Field(default_factory=list)


def available_fallback_parser() -> str | None:
    """Return the available fallback parser implementation name, if any."""
    return "marker" if shutil.which("marker_single") else None


def parse_document(
    doc_id: str,
    *,
    data_dir: Path | None = None,
    output_dir: Path | None = None,
) -> FallbackParseResult:
    """Run the configured fallback parser and persist canonical parse artifacts."""
    resolved_data_dir = get_data_dir(data_dir)
    normalized_path = resolved_data_dir / "normalized" / f"{doc_id}.pdf"
    if not normalized_path.exists():
        raise FileNotFoundError(f"normalized PDF not found for doc_id '{doc_id}'")

    target_dir = output_dir or resolved_data_dir / "parsed" / doc_id / "runs" / "marker"
    target_dir.mkdir(parents=True, exist_ok=True)

    page_count = _derive_page_count(doc_id=doc_id, data_dir=resolved_data_dir, normalized_path=normalized_path)
    start = time.perf_counter()
    payload = _extract_with_marker(normalized_path, page_count=page_count)
    if not payload.pages and page_count > 0:
        payload = payload.model_copy(update={"pages": _build_fallback_pages(page_count)})
    processing_time = time.perf_counter() - start

    structure = StructuredParseArtifact(
        doc_id=doc_id,
        parser=payload.parser,
        parser_version=payload.parser_version,
        page_count=page_count,
        texts=[
            ParseTextArtifact(
                item_ref=item.item_ref,
                label=item.label,
                text=item.text,
                page_numbers=item.page_numbers,
            )
            for item in payload.texts
        ],
        tables=[
            ParseTableArtifact(
                item_ref=item.item_ref,
                label=item.label,
                page_numbers=item.page_numbers,
                row_count=len(item.data),
                column_count=max((len(row) for row in item.data), default=0),
                data=item.data,
            )
            for item in payload.tables
        ],
        pages=payload.pages,
    )
    headings = HeadingTreeArtifact(doc_id=doc_id, headings=_build_heading_tree(payload.markdown, structure.texts))
    tables = TablesArtifact(doc_id=doc_id, tables=structure.tables)
    page_map = PageMapArtifact(doc_id=doc_id, items=_build_page_map(structure))

    content_path = target_dir / "content.md"
    structure_path = target_dir / "structure.json"
    headings_path = target_dir / "headings.json"
    tables_path = target_dir / "tables.json"
    page_map_path = target_dir / "page_map.json"
    meta_path = target_dir / "meta.json"
    quality_path = target_dir / "quality.json"

    content_path.write_text(payload.markdown, encoding="utf-8")
    _write_json(structure_path, structure.model_dump(mode="json"))
    _write_json(headings_path, headings.model_dump(mode="json"))
    _write_json(tables_path, tables.model_dump(mode="json"))
    _write_json(page_map_path, page_map.model_dump(mode="json"))

    metadata = ParseMetadata(
        doc_id=doc_id,
        parser=payload.parser,
        parser_version=payload.parser_version,
        processed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        processing_time_seconds=round(processing_time, 4),
        page_count=page_count,
        status=payload.status,
        input_path=str(normalized_path),
        input_checksum=compute_sha256(normalized_path),
        document_hash=None,
        timings=payload.timings,
        confidence=payload.confidence,
        errors=payload.errors,
    )
    _write_json(meta_path, metadata.model_dump(mode="json"))

    bundle = ParseArtifactBundle(
        doc_id=doc_id,
        content=payload.markdown,
        structure=structure,
        headings=headings,
        tables=tables,
        page_map=page_map,
        meta=metadata,
    )
    quality_report = score_bundle(bundle)
    quality_path.write_text(quality_report.model_dump_json(indent=2), encoding="utf-8")
    return FallbackParseResult(
        doc_id=doc_id,
        parser=payload.parser,
        content_path=content_path,
        structure_path=structure_path,
        headings_path=headings_path,
        tables_path=tables_path,
        page_map_path=page_map_path,
        meta_path=meta_path,
        quality_path=quality_path,
        parser_version=payload.parser_version,
        page_count=page_count,
        processing_time=round(processing_time, 4),
        quality_report=quality_report,
    )


def _extract_with_marker(pdf_path: Path, *, page_count: int) -> FallbackPayload:
    """Run the Marker CLI and normalize its Markdown output."""
    marker_command = shutil.which("marker_single")
    if marker_command is None:
        raise RuntimeError("Marker CLI is not installed; install marker-pdf to enable fallback parsing")

    with tempfile.TemporaryDirectory(prefix="knowledge-forge-marker-") as temp_dir:
        output_dir = Path(temp_dir)
        command = [
            marker_command,
            str(pdf_path),
            "--output_dir",
            str(output_dir),
        ]
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(
                "Marker fallback parsing failed: "
                + (completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}")
            )

        markdown_paths = sorted(output_dir.rglob("*.md"))
        if not markdown_paths:
            raise RuntimeError("Marker fallback parsing did not produce a Markdown artifact")
        markdown = markdown_paths[0].read_text(encoding="utf-8")

    return FallbackPayload(
        parser="marker",
        parser_version="cli",
        markdown=markdown,
        texts=_markdown_to_text_items(markdown),
        pages=_build_fallback_pages(page_count),
    )


def _markdown_to_text_items(markdown: str) -> list[FallbackTextItem]:
    """Split Markdown into a conservative list of heading and body blocks."""
    items: list[FallbackTextItem] = []
    item_index = 0
    paragraph_lines: list[str] = []

    def flush_paragraph() -> None:
        nonlocal item_index
        text = "\n".join(paragraph_lines).strip()
        if not text:
            return
        items.append(FallbackTextItem(item_ref=f"#/texts/{item_index}", label="text", text=text))
        item_index += 1
        paragraph_lines.clear()

    for line in markdown.splitlines():
        stripped = line.strip()
        if not stripped:
            flush_paragraph()
            continue

        match = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if match:
            flush_paragraph()
            level = len(match.group(1))
            label = "title" if level == 1 else "section_header"
            items.append(FallbackTextItem(item_ref=f"#/texts/{item_index}", label=label, text=match.group(2).strip()))
            item_index += 1
            continue

        paragraph_lines.append(stripped)

    flush_paragraph()
    return items


def _build_heading_tree(markdown: str, text_items: list[ParseTextArtifact]) -> list[HeadingNode]:
    """Build a heading tree from markdown heading markers when available."""
    heading_items = [item for item in text_items if item.label in {"title", "section_header"}]
    headings: list[HeadingNode] = []
    stack: list[HeadingNode] = []

    for item in heading_items:
        level = 1 if item.label == "title" else 2
        node = HeadingNode(
            title=item.text.strip(),
            label=item.label,
            level=level,
            page_number=item.page_numbers[0] if item.page_numbers else None,
            item_ref=item.item_ref,
            children=[],
        )
        while stack and stack[-1].level >= level:
            stack.pop()
        if stack:
            stack[-1].children.append(node)
        else:
            headings.append(node)
        stack.append(node)

    if headings:
        return headings

    # If the markdown had no explicit headings, synthesize a single document title.
    title = next((line.strip() for line in markdown.splitlines() if line.strip()), "Fallback parse")
    return [
        HeadingNode(
            title=title[:120],
            label="title",
            level=1,
            page_number=None,
            item_ref=None,
            children=[],
        )
    ]


def _infer_missing_page_numbers(items: list[Any]) -> list[list[int]]:
    """Infer page numbers when fallback output lacks exact provenance."""
    resolved: list[list[int] | None] = [list(item.page_numbers) if item.page_numbers else None for item in items]

    last_seen: list[int] | None = None
    for index, page_numbers in enumerate(resolved):
        if page_numbers:
            last_seen = page_numbers
        elif last_seen:
            resolved[index] = list(last_seen)

    next_seen: list[int] | None = None
    for index in range(len(resolved) - 1, -1, -1):
        page_numbers = resolved[index]
        if page_numbers:
            next_seen = page_numbers
        elif next_seen:
            resolved[index] = list(next_seen)

    return [list(page_numbers) if page_numbers else [1] for page_numbers in resolved]


def _build_page_map(structure: StructuredParseArtifact) -> list[PageMapItem]:
    """Create a content-to-page map from fallback text and table provenance."""
    items: list[PageMapItem] = []
    text_page_numbers = _infer_missing_page_numbers(structure.texts)
    for item, page_numbers in zip(structure.texts, text_page_numbers, strict=True):
        items.append(
            PageMapItem(
                item_ref=item.item_ref,
                item_type="text",
                label=item.label,
                page_numbers=page_numbers,
                text=item.text,
            )
        )
    table_page_numbers = _infer_missing_page_numbers(structure.tables)
    for item, page_numbers in zip(structure.tables, table_page_numbers, strict=True):
        items.append(
            PageMapItem(
                item_ref=item.item_ref,
                item_type="table",
                label=item.label,
                page_numbers=page_numbers,
                table_rows=item.row_count,
            )
        )
    return items


def _derive_page_count(*, doc_id: str, data_dir: Path, normalized_path: Path) -> int:
    """Determine page count from normalization metadata, with PDF introspection fallback."""
    meta_path = data_dir / "normalized" / f"{doc_id}.meta.json"
    if meta_path.exists():
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            page_count = payload.get("page_count")
            if isinstance(page_count, int) and page_count > 0:
                return page_count
        except (OSError, json.JSONDecodeError):
            pass

    try:
        return len(PdfInfo(normalized_path).pages)
    except Exception:
        return 0


def _build_fallback_pages(page_count: int) -> list[ParsePageArtifact]:
    """Build conservative page artifacts from an inferred page count."""
    return [ParsePageArtifact(page_number=index) for index in range(1, page_count + 1)]


def _write_json(path: Path, payload: Any) -> None:
    """Persist a JSON payload using the repo's standard formatting."""
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
