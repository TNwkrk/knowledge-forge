"""Canonical sectioning for parsed documents."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from knowledge_forge.intake.importer import get_data_dir, list_manifests, load_manifest
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
    "workflow",
    "sop",
    "checklist",
    "inspection",
    "commissioning",
    "wiring",
    "drawing",
    "diagram",
    "addendum",
    "bulletin",
    "seasonal-procedure",
    "other",
]


class SectionStep(BaseModel):
    """One ordered step preserved from a structured operational section."""

    step_number: int = Field(ge=1)
    text: str
    page_number: int | None = Field(default=None, ge=1)
    item_ref: str | None = None


class FigureRegion(BaseModel):
    """A page-scoped figure region with any parsed callout text."""

    label: str
    page_range: tuple[int | None, int | None] = Field(default=(None, None))
    callouts: list[str] = Field(default_factory=list)


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
    ordered_steps: list[SectionStep] = Field(default_factory=list)
    figure_regions: list[FigureRegion] = Field(default_factory=list)


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
    ordered_steps: list[SectionStep]
    figure_regions: list[FigureRegion]
    item_ref: str | None = None


@dataclass(frozen=True)
class _SectionContext:
    document_type: str = ""
    document_class: str = ""

    @property
    def normalized_document_type(self) -> str:
        return slugify(self.document_type)

    @property
    def normalized_document_class(self) -> str:
        return self.document_class.strip().casefold()


_MANUAL_SECTION_PATTERNS: tuple[tuple[SectionType, tuple[str, ...]], ...] = (
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

_OPERATIONAL_SECTION_PATTERNS: tuple[tuple[SectionType, tuple[str, ...]], ...] = (
    (
        "seasonal-procedure",
        ("winterization", "seasonal procedure", "season startup", "spring startup", "summer startup"),
    ),
    ("sop", ("standard operating procedure", "sop")),
    ("checklist", ("checklist", "check list", "verification list", "task list")),
    ("inspection", ("inspection", "inspection sheet", "inspection template", "inspection form")),
    ("commissioning", ("commissioning", "commissioning checklist", "commissioning sheet", "acceptance test")),
    ("workflow", ("workflow", "procedure", "process", "sequence of operation", "method")),
)

_DRAWING_SECTION_PATTERNS: tuple[tuple[SectionType, tuple[str, ...]], ...] = (
    ("wiring", ("wiring diagram", "terminal wiring", "wiring", "connection diagram", "terminal layout")),
    ("drawing", ("engineering drawing", "mechanical drawing", "drawing", "layout")),
    ("diagram", ("diagram", "schematic", "figure", "illustration", "p&id", "pid")),
)

_BULLETIN_SECTION_PATTERNS: tuple[tuple[SectionType, tuple[str, ...]], ...] = (
    ("addendum", ("addendum", "supplement", "supplemental", "appendix update")),
    ("bulletin", ("service bulletin", "bulletin", "field notice", "notice", "advisory")),
    ("revision_notes", ("revision", "release note", "change log", "history", "supersession")),
)

_DOCUMENT_TYPE_DEFAULTS: dict[str, SectionType] = {
    "startup-procedure": "workflow",
    "shutdown-procedure": "workflow",
    "winterization-procedure": "seasonal-procedure",
    "pm-procedure": "workflow",
    "sop": "sop",
    "checklist": "checklist",
    "service-bulletin": "bulletin",
    "bulletin": "bulletin",
    "addendum": "addendum",
    "engineering-drawing": "drawing",
    "wiring-diagram": "wiring",
    "pid": "diagram",
    "inspection-template": "inspection",
    "commissioning-sheet": "commissioning",
    "best-practice": "workflow",
    "safety-procedure": "workflow",
}

_STEP_NUMBER_PATTERN = re.compile(r"^(?:step\s+)?(?P<number>\d+)[\).\:\-]?\s+(?P<body>.+)$", re.IGNORECASE)
_CHECKBOX_PATTERN = re.compile(r"^(?:[-*•]\s+)?\[(?: |x|X)\]\s+(?P<body>.+)$")
_BULLET_PATTERN = re.compile(r"^(?:[-*•])\s+(?P<body>.+)$")
_FIGURE_LABEL_PATTERN = re.compile(
    r"^(?P<label>(?:figure|fig\.?|diagram|drawing|wiring diagram|schematic|p&id)[A-Za-z0-9.\- ]*)$",
    re.IGNORECASE,
)
_CALLOUT_PATTERN = re.compile(
    r"^(?:callout\s+)?(?:[A-Z]{1,3}|\d{1,3}|[A-Z]\d{1,2})[\)\.\:\-]\s+.+$",
    re.IGNORECASE,
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
    try:
        manifest = load_manifest(resolved_data_dir, doc_id)
        context = _SectionContext(
            document_type=manifest.document.document_type,
            document_class=manifest.document.document_class,
        )
    except FileNotFoundError:
        context = _SectionContext()

    sections = _build_sections(
        doc_id=doc_id,
        structure=structure,
        heading_tree=heading_tree,
        context=context,
    )
    _persist_sections(sections, resolved_data_dir)
    return sections


def section_all_documents(*, data_dir: Path | None = None) -> list[list[Section]]:
    """Run sectioning for every parsed document in the current data directory."""
    resolved_data_dir = get_data_dir(data_dir)
    results: list[list[Section]] = []
    for manifest in list_manifests(resolved_data_dir):
        parsed_dir = resolved_data_dir / "parsed" / manifest.doc_id
        structure_path = parsed_dir / "structure.json"
        headings_path = parsed_dir / "headings.json"
        if structure_path.exists() and headings_path.exists():
            results.append(section_document(manifest.doc_id, data_dir=resolved_data_dir))
    return results


def build_sections_from_artifacts(
    *,
    doc_id: str,
    structure: StructuredParseArtifact,
    heading_tree: HeadingTreeArtifact,
    document_type: str = "",
    document_class: str = "",
) -> list[Section]:
    """Build typed sections from pre-loaded parse artifacts without filesystem I/O.

    Intended for evaluation harnesses and testing that operate on committed
    parse artifact snapshots rather than live pipeline outputs.
    """
    context = _SectionContext(
        document_type=document_type,
        document_class=document_class,
    )
    return _build_sections(
        doc_id=doc_id,
        structure=structure,
        heading_tree=heading_tree,
        context=context,
    )


def _build_sections(
    *,
    doc_id: str,
    structure: StructuredParseArtifact,
    heading_tree: HeadingTreeArtifact,
    context: _SectionContext,
) -> list[Section]:
    heading_lookup = {
        entry.item_ref: entry for entry in _flatten_headings(heading_tree.headings) if entry.item_ref is not None
    }
    heading_events = _build_heading_events(structure, heading_lookup)
    draft_sections = _draft_sections(doc_id=doc_id, structure=structure, heading_events=heading_events, context=context)

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
                ordered_steps=draft.ordered_steps,
                figure_regions=draft.figure_regions,
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
    context: _SectionContext,
) -> list[_DraftSection]:
    non_section_heading_labels = {
        "title",
        "document_title",
        "page_header",
        "page_footer",
        "running_header",
        "running_footer",
    }
    section_events = [event for event in heading_events if event.label not in non_section_heading_labels]
    drafts: list[_DraftSection] = []

    if not section_events:
        title = _document_title(structure) or doc_id
        body_items = structure.texts
        tables = list(structure.tables)
        content = _render_section_content(title=None, body_items=body_items, tables=tables)
        section_type = _classify_section(title, content, heading_path=(title,), context=context)
        drafts.append(
            _DraftSection(
                title=title,
                heading_path=(title,),
                parent_item_ref=None,
                section_type=section_type,
                content=content,
                page_range=_page_range_for_items(body_items, tables),
                ordered_steps=_extract_ordered_steps(body_items, section_type=section_type, context=context),
                figure_regions=_extract_figure_regions(
                    title=title,
                    body_items=body_items,
                    section_type=section_type,
                ),
            )
        )
        return drafts

    assigned_table_indexes: set[int] = set()
    first_section_index = section_events[0].index
    if first_section_index > 0:
        preamble_items = structure.texts[:first_section_index]
        preamble_title = _document_title(structure) or "Overview"
        first_section_page = section_events[0].page_number or _first_page_number(structure.texts[first_section_index:])
        preamble_tables, table_indexes = _collect_tables_for_section(
            structure,
            current_start=1,
            next_start=first_section_page,
            assigned_table_indexes=assigned_table_indexes,
        )
        assigned_table_indexes.update(table_indexes)
        content = _render_section_content(title=None, body_items=preamble_items, tables=preamble_tables)
        if content.strip():
            section_type = _classify_section(
                preamble_title,
                content,
                heading_path=(preamble_title,),
                context=context,
            )
            drafts.append(
                _DraftSection(
                    title=preamble_title,
                    heading_path=(preamble_title,),
                    parent_item_ref=None,
                    section_type=section_type,
                    content=content,
                    page_range=_page_range_for_items(preamble_items, preamble_tables),
                    ordered_steps=_extract_ordered_steps(preamble_items, section_type=section_type, context=context),
                    figure_regions=_extract_figure_regions(
                        title=preamble_title,
                        body_items=preamble_items,
                        section_type=section_type,
                    ),
                )
            )

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
        section_type = _classify_section(event.title, content, heading_path=event.path, context=context)
        drafts.append(
            _DraftSection(
                title=event.title,
                heading_path=event.path,
                parent_item_ref=event.parent_item_ref,
                section_type=section_type,
                content=content,
                page_range=_page_range_for_items(body_items, tables, fallback_page=event.page_number),
                ordered_steps=_extract_ordered_steps(body_items, section_type=section_type, context=context),
                figure_regions=_extract_figure_regions(
                    title=event.title,
                    body_items=body_items,
                    section_type=section_type,
                ),
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


def _classify_section(
    title: str,
    content: str,
    *,
    heading_path: tuple[str, ...],
    context: _SectionContext,
) -> SectionType:
    title_haystack = _normalize_classifier_text(title)
    parent_haystack = _normalize_classifier_text(" ".join(heading_path[1:-1]))
    content_haystack = _normalize_classifier_text(content[:500])
    patterns = _patterns_for_context(context)

    for section_type, candidates in patterns:
        if any(candidate in title_haystack for candidate in candidates):
            return section_type
    if parent_haystack:
        for section_type, candidates in patterns:
            if any(candidate in parent_haystack for candidate in candidates):
                return section_type
    document_default = _DOCUMENT_TYPE_DEFAULTS.get(context.normalized_document_type)
    if document_default is not None and title_haystack not in {"overview", "notes", "instructions"}:
        return document_default
    if _title_is_content_ambiguous(title_haystack):
        for section_type, candidates in patterns:
            if any(candidate in content_haystack for candidate in candidates):
                return section_type
    return "other"


def _patterns_for_context(context: _SectionContext) -> tuple[tuple[SectionType, tuple[str, ...]], ...]:
    document_type = context.normalized_document_type
    if (
        document_type
        in {
            "sop",
            "checklist",
            "startup-procedure",
            "shutdown-procedure",
            "winterization-procedure",
            "pm-procedure",
            "inspection-template",
            "commissioning-sheet",
            "best-practice",
            "safety-procedure",
        }
        or context.normalized_document_class == "operational"
    ):
        return (
            _OPERATIONAL_SECTION_PATTERNS
            + _DRAWING_SECTION_PATTERNS
            + _BULLETIN_SECTION_PATTERNS
            + _MANUAL_SECTION_PATTERNS
        )
    if document_type in {"engineering-drawing", "wiring-diagram", "pid"}:
        return (
            _DRAWING_SECTION_PATTERNS
            + _OPERATIONAL_SECTION_PATTERNS
            + _BULLETIN_SECTION_PATTERNS
            + _MANUAL_SECTION_PATTERNS
        )
    if document_type in {"service-bulletin", "bulletin", "addendum", "supplemental-guide", "supersession-notice"}:
        return (
            _BULLETIN_SECTION_PATTERNS
            + _DRAWING_SECTION_PATTERNS
            + _OPERATIONAL_SECTION_PATTERNS
            + _MANUAL_SECTION_PATTERNS
        )
    return (
        _MANUAL_SECTION_PATTERNS
        + _OPERATIONAL_SECTION_PATTERNS
        + _BULLETIN_SECTION_PATTERNS
        + _DRAWING_SECTION_PATTERNS
    )


def _normalize_classifier_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _title_is_content_ambiguous(title_haystack: str) -> bool:
    return title_haystack in {
        "",
        "overview",
        "procedure",
        "instructions",
        "guidance",
        "notes",
        "details",
        "general",
    }


def _extract_ordered_steps(
    body_items: list[object],
    *,
    section_type: SectionType,
    context: _SectionContext,
) -> list[SectionStep]:
    step_sections = {
        "workflow",
        "sop",
        "checklist",
        "inspection",
        "commissioning",
        "seasonal-procedure",
    }
    steps: list[SectionStep] = []
    if section_type not in step_sections and context.normalized_document_class != "operational":
        return steps

    fallback_items: list[tuple[object, str]] = []
    next_step_number = 1
    for item in body_items:
        text = getattr(item, "text", "").strip()
        label = getattr(item, "label", "")
        if not text or label in HEADING_LABELS:
            continue
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parsed = _parse_step_line(line, prefer_checklist=section_type == "checklist")
            if parsed is not None:
                step_number, body = parsed
                steps.append(
                    SectionStep(
                        step_number=step_number or next_step_number,
                        text=body,
                        page_number=_first_page_number([item]),
                        item_ref=getattr(item, "item_ref", None),
                    )
                )
                next_step_number = steps[-1].step_number + 1
                continue
            fallback_items.append((item, line))

    if steps:
        return steps

    if section_type not in step_sections:
        return []

    for step_number, (item, line) in enumerate(fallback_items, start=1):
        steps.append(
            SectionStep(
                step_number=step_number,
                text=line,
                page_number=_first_page_number([item]),
                item_ref=getattr(item, "item_ref", None),
            )
        )
    return steps


def _parse_step_line(line: str, *, prefer_checklist: bool) -> tuple[int | None, str] | None:
    numbered_match = _STEP_NUMBER_PATTERN.match(line)
    if numbered_match is not None:
        return int(numbered_match.group("number")), numbered_match.group("body").strip()

    checkbox_match = _CHECKBOX_PATTERN.match(line)
    if checkbox_match is not None:
        return None, checkbox_match.group("body").strip()

    if prefer_checklist:
        bullet_match = _BULLET_PATTERN.match(line)
        if bullet_match is not None:
            return None, bullet_match.group("body").strip()
    return None


def _extract_figure_regions(
    *,
    title: str,
    body_items: list[object],
    section_type: SectionType,
) -> list[FigureRegion]:
    drawing_section_types = {"wiring", "drawing", "diagram"}
    if section_type not in drawing_section_types:
        return []

    regions: list[dict[str, object]] = []
    current_region: dict[str, object] | None = None

    for item in body_items:
        text = getattr(item, "text", "").strip()
        label = getattr(item, "label", "")
        if not text or label in HEADING_LABELS:
            continue
        page_number = _first_page_number([item])
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            figure_match = _FIGURE_LABEL_PATTERN.match(line)
            if figure_match is not None:
                current_region = {
                    "label": figure_match.group("label").strip(),
                    "page_range": (page_number, page_number),
                    "callouts": [],
                }
                regions.append(current_region)
                continue
            if current_region is None:
                current_region = {
                    "label": title,
                    "page_range": (page_number, page_number),
                    "callouts": [],
                }
                regions.append(current_region)
            if _CALLOUT_PATTERN.match(line):
                current_region["callouts"].append(line)

    return [FigureRegion.model_validate(region) for region in regions]


def _build_section_id(*, doc_id: str, title: str, content: str) -> str:
    digest = hashlib.sha256(f"{doc_id}\n{title.strip()}\n{content.strip()}".encode("utf-8")).hexdigest()[:12]
    title_slug = slugify(title) or "section"
    return f"{doc_id}--{title_slug[:80]}--{digest}"


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
