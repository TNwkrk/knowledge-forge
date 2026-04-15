"""Canonical sectioning for parsed documents."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import slugify
from knowledge_forge.parse.quality import HEADING_LABELS, HeadingNode, HeadingTreeArtifact, StructuredParseArtifact

SectionType = Literal[
    "safety",
    "installation",
    "configuration",
    "startup",
    "shutdown",
    "maintenance",
    "troubleshooting",
    "specifications",
    "parts",
    "revision_notes",
    "other",
]


class Section(BaseModel):
    """A canonical extraction unit derived from parsed document content."""

    doc_id: str
    section_id: str
    section_type: SectionType
    title: str
    content: str
    page_range: tuple[int | None, int | None] = Field(default=(None, None))
    heading_path: list[str] = Field(default_factory=list)
    parent_section_id: str | None = None


@dataclass(frozen=True)
class _HeadingEntry:
    item_ref: str | None
    title: str
    level: int
    page_number: int | None
    path: tuple[str, ...]
    parent_item_ref: str | None


@dataclass(frozen=True)
class _HeadingEvent:
    index: int
    item_ref: str | None
    title: str
    level: int
    page_number: int | None
    path: tuple[str, ...]
    parent_item_ref: str | None
    label: str


@dataclass(frozen=True)
class _DraftSection:
    title: str
    heading_path: tuple[str, ...]
    parent_item_ref: str | None
    section_type: SectionType
    content: str
    page_range: tuple[int | None, int | None]
    item_ref: str | None = None


_SECTION_PATTERNS: tuple[tuple[SectionType, tuple[str, ...]], ...] = (
    ("revision_notes", ("revision", "change log", "changelog", "release note", "amendment", "history")),
    ("troubleshooting", ("troubleshooting", "diagnostic", "fault", "alarm", "error code", "problem")),
    ("specifications", ("specification", "specifications", "technical data", "ratings", "dimensions", "electrical")),
    ("maintenance", ("maintenance", "inspection", "cleaning", "lubrication", "replacement")),
    ("installation", ("installation", "mounting", "wiring", "setup", "preparation")),
    ("configuration", ("configuration", "config", "parameter", "programming", "settings", "commissioning")),
    ("startup", ("startup", "start up", "start-up", "initial start", "power on", "operation")),
    ("shutdown", ("shutdown", "shut down", "stop procedure", "power off", "decommission")),
    ("safety", ("safety", "warning", "caution", "danger", "hazard", "precaution")),
    ("parts", ("parts", "spare part", "replacement part", "bill of materials", "bom")),
)


def section_document(doc_id: str, *, data_dir: Path | None = None) -> list[Section]:
    """Split one parsed document into typed canonical sections and persist them."""
    resolved_data_dir = get_data_dir(data_dir)
    parsed_dir = resolved_data_dir / "parsed" / doc_id
    structure_path = parsed_dir / "structure.json"
    headings_path = parsed_dir / "headings.json"
    if not structure_path.exists():
        raise FileNotFoundError(f"parsed structure not found for doc_id '{doc_id}'")
    if not headings_path.exists():
        raise FileNotFoundError(f"heading tree not found for doc_id '{doc_id}'")

    structure = StructuredParseArtifact.model_validate_json(structure_path.read_text(encoding="utf-8"))
    heading_tree = HeadingTreeArtifact.model_validate_json(headings_path.read_text(encoding="utf-8"))

    sections = _build_sections(doc_id=doc_id, structure=structure, heading_tree=heading_tree)
    _persist_sections(sections, resolved_data_dir)
    return sections


def section_all_documents(*, data_dir: Path | None = None) -> list[list[Section]]:
    """Run sectioning for every parsed document in the current data directory."""
    resolved_data_dir = get_data_dir(data_dir)
    results: list[list[Section]] = []
    for manifest in list_manifests(resolved_data_dir):
        if (resolved_data_dir / "parsed" / manifest.doc_id / "structure.json").exists():
            results.append(section_document(manifest.doc_id, data_dir=resolved_data_dir))
    return results


def _build_sections(
    *,
    doc_id: str,
    structure: StructuredParseArtifact,
    heading_tree: HeadingTreeArtifact,
) -> list[Section]:
    heading_lookup = {
        entry.item_ref: entry for entry in _flatten_headings(heading_tree.headings) if entry.item_ref is not None
    }
    heading_events = _build_heading_events(structure, heading_lookup)
    draft_sections = _draft_sections(doc_id=doc_id, structure=structure, heading_events=heading_events)

    item_ref_to_section_id: dict[str, str] = {}
    sections: list[Section] = []
    for draft in draft_sections:
        section_id = _build_section_id(doc_id=doc_id, title=draft.title, content=draft.content)
        if draft.item_ref:
            item_ref_to_section_id[draft.item_ref] = section_id
        sections.append(
            Section(
                doc_id=doc_id,
                section_id=section_id,
                section_type=draft.section_type,
                title=draft.title,
                content=draft.content,
                page_range=draft.page_range,
                heading_path=list(draft.heading_path),
                parent_section_id=None,
            )
        )

    for section, draft in zip(sections, draft_sections, strict=True):
        if draft.parent_item_ref is not None:
            section.parent_section_id = item_ref_to_section_id.get(draft.parent_item_ref)

    return sections


def _draft_sections(
    *,
    doc_id: str,
    structure: StructuredParseArtifact,
    heading_events: list[_HeadingEvent],
) -> list[_DraftSection]:
    section_events = [event for event in heading_events if event.label not in {"title", "document_title"}]
    drafts: list[_DraftSection] = []

    if not section_events:
        title = _document_title(structure) or doc_id
        body_items = structure.texts
        tables = list(structure.tables)
        content = _render_section_content(title=None, body_items=body_items, tables=tables)
        drafts.append(
            _DraftSection(
                title=title,
                heading_path=(title,),
                parent_item_ref=None,
                section_type=_classify_section(title, content),
                content=content,
                page_range=_page_range_for_items(body_items, tables),
            )
        )
        return drafts

    first_section_index = section_events[0].index
    if first_section_index > 0:
        preamble_items = structure.texts[:first_section_index]
        preamble_title = _document_title(structure) or "Overview"
        content = _render_section_content(title=None, body_items=preamble_items, tables=[])
        if content.strip():
            drafts.append(
                _DraftSection(
                    title=preamble_title,
                    heading_path=(preamble_title,),
                    parent_item_ref=None,
                    section_type="other",
                    content=content,
                    page_range=_page_range_for_items(preamble_items, []),
                )
            )

    assigned_table_indexes: set[int] = set()
    for index, event in enumerate(section_events):
        next_index = section_events[index + 1].index if index + 1 < len(section_events) else len(structure.texts)
        body_items = structure.texts[event.index + 1 : next_index]
        current_start = event.page_number or _first_page_number(body_items) or 1
        next_start = section_events[index + 1].page_number if index + 1 < len(section_events) else None
        tables, table_indexes = _collect_tables_for_section(
            structure,
            current_start=current_start,
            next_start=next_start,
            assigned_table_indexes=assigned_table_indexes,
        )
        assigned_table_indexes.update(table_indexes)
        content = _render_section_content(title=event.title, body_items=body_items, tables=tables)
        drafts.append(
            _DraftSection(
                title=event.title,
                heading_path=event.path,
                parent_item_ref=event.parent_item_ref,
                section_type=_classify_section(event.title, content),
                content=content,
                page_range=_page_range_for_items(body_items, tables, fallback_page=event.page_number),
                item_ref=event.item_ref,
            )
        )

    return drafts


def _flatten_headings(
    nodes: list[HeadingNode],
    *,
    path: tuple[str, ...] = (),
    parent_item_ref: str | None = None,
) -> list[_HeadingEntry]:
    flattened: list[_HeadingEntry] = []
    for node in nodes:
        current_path = (*path, node.title)
        flattened.append(
            _HeadingEntry(
                item_ref=node.item_ref,
                title=node.title,
                level=node.level,
                page_number=node.page_number,
                path=current_path,
                parent_item_ref=parent_item_ref,
            )
        )
        flattened.extend(
            _flatten_headings(
                node.children,
                path=current_path,
                parent_item_ref=node.item_ref,
            )
        )
    return flattened


def _build_heading_events(
    structure: StructuredParseArtifact,
    heading_lookup: dict[str, _HeadingEntry],
) -> list[_HeadingEvent]:
    events: list[_HeadingEvent] = []
    for index, item in enumerate(structure.texts):
        heading_entry = heading_lookup.get(item.item_ref)
        if heading_entry is None and item.label not in HEADING_LABELS:
            continue
        if heading_entry is None:
            title = item.text.strip()
            level = _infer_heading_level(item.label)
            path = (title,)
            parent_item_ref = None
            page_number = _first_page_number([item])
        else:
            title = heading_entry.title
            level = heading_entry.level
            path = heading_entry.path
            parent_item_ref = heading_entry.parent_item_ref
            page_number = heading_entry.page_number or _first_page_number([item])
        events.append(
            _HeadingEvent(
                index=index,
                item_ref=item.item_ref,
                title=title,
                level=level,
                page_number=page_number,
                path=path,
                parent_item_ref=parent_item_ref,
                label=item.label,
            )
        )
    return events


def _infer_heading_level(label: str) -> int:
    if label in {"title", "document_title"}:
        return 1
    if label in {"section_header", "subtitle"}:
        return 2
    return 3


def _document_title(structure: StructuredParseArtifact) -> str | None:
    for item in structure.texts:
        if item.label in {"title", "document_title"} and item.text.strip():
            return item.text.strip()
    return None


def _render_section_content(
    *,
    title: str | None,
    body_items: list[object],
    tables: list[object],
) -> str:
    chunks: list[str] = []
    if title:
        chunks.append(f"## {title}")

    for item in body_items:
        text = getattr(item, "text", "").strip()
        label = getattr(item, "label", "")
        if not text:
            continue
        if label in HEADING_LABELS:
            continue
        chunks.append(text)

    for table in tables:
        table_markdown = _render_table_markdown(getattr(table, "data", []))
        if table_markdown:
            chunks.append(table_markdown)

    return "\n\n".join(chunk for chunk in chunks if chunk.strip()).strip()


def _render_table_markdown(rows: list[list[object]]) -> str:
    if not rows:
        return ""
    normalized_rows = [[str(cell).strip() for cell in row] for row in rows]
    width = max((len(row) for row in normalized_rows), default=0)
    if width == 0:
        return ""
    padded_rows = [row + [""] * (width - len(row)) for row in normalized_rows]
    header = padded_rows[0]
    divider = ["---"] * width
    body = padded_rows[1:] or [[""] * width]
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(divider) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in body)
    return "\n".join(lines)


def _collect_tables_for_section(
    structure: StructuredParseArtifact,
    *,
    current_start: int,
    next_start: int | None,
    assigned_table_indexes: set[int],
) -> tuple[list[object], set[int]]:
    selected: list[object] = []
    selected_indexes: set[int] = set()
    for index, table in enumerate(structure.tables):
        if index in assigned_table_indexes:
            continue
        pages = [page for page in table.page_numbers if page is not None]
        if not pages:
            continue
        first_page = min(pages)
        if first_page < current_start:
            continue
        if next_start is not None and first_page >= next_start:
            continue
        selected.append(table)
        selected_indexes.add(index)
    return selected, selected_indexes


def _page_range_for_items(
    body_items: list[object],
    tables: list[object],
    *,
    fallback_page: int | None = None,
) -> tuple[int | None, int | None]:
    pages: list[int] = []
    for item in body_items:
        pages.extend(page for page in getattr(item, "page_numbers", []) if page is not None)
    for table in tables:
        pages.extend(page for page in getattr(table, "page_numbers", []) if page is not None)
    if not pages and fallback_page is not None:
        pages = [fallback_page]
    if not pages:
        return (None, None)
    return (min(pages), max(pages))


def _first_page_number(items: list[object]) -> int | None:
    for item in items:
        for page in getattr(item, "page_numbers", []):
            if page is not None:
                return page
    return None


def _classify_section(title: str, content: str) -> SectionType:
    del content
    haystack = title.casefold()
    haystack = re.sub(r"[^a-z0-9]+", " ", haystack)
    for section_type, patterns in _SECTION_PATTERNS:
        if any(pattern in haystack for pattern in patterns):
            return section_type
    return "other"


def _build_section_id(*, doc_id: str, title: str, content: str) -> str:
    digest = hashlib.sha256(f"{doc_id}\n{title.strip()}\n{content.strip()}".encode("utf-8")).hexdigest()[:12]
    return f"{doc_id}--{slugify(title) or 'section'}--{digest}"


def _persist_sections(sections: list[Section], data_dir: Path) -> None:
    if not sections:
        return
    target_dir = data_dir / "sections" / sections[0].doc_id
    target_dir.mkdir(parents=True, exist_ok=True)
    for existing in target_dir.glob("*.json"):
        existing.unlink()
    for section in sections:
        (target_dir / f"{section.section_id}.json").write_text(
            json.dumps(section.model_dump(mode="json"), indent=2),
            encoding="utf-8",
        )
