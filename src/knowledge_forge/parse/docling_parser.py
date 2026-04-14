"""Docling-backed parsing pipeline for normalized PDFs."""

from __future__ import annotations

import json
import time
from enum import Enum
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from knowledge_forge.intake.importer import get_data_dir, list_manifests, load_manifest
from knowledge_forge.intake.manifest import DocumentStatus, ManifestEntry, compute_sha256


class ParseResult(BaseModel):
    """Paths to the parse artifacts produced for a document."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    doc_id: str
    content_path: Path
    structure_path: Path
    headings_path: Path
    tables_path: Path
    page_map_path: Path
    meta_path: Path
    parser_version: str
    page_count: int
    processing_time: float


_HEADING_LEVELS: dict[str, int] = {
    "title": 1,
    "document_title": 1,
    "section_header": 2,
    "subtitle": 2,
    "page_header": 3,
    "section_title": 3,
}


def parse_document(doc_id: str, *, data_dir: Path | None = None) -> ParseResult:
    """Run Docling against a normalized PDF and persist the parse artifacts."""
    resolved_data_dir = get_data_dir(data_dir)
    manifest = load_manifest(resolved_data_dir, doc_id)
    normalized_path = _resolve_normalized_path(resolved_data_dir, doc_id)
    output_dir = resolved_data_dir / "parsed" / doc_id
    output_dir.mkdir(parents=True, exist_ok=True)

    converter = _get_document_converter()
    start = time.perf_counter()
    conversion = converter.convert(normalized_path)
    processing_time = time.perf_counter() - start
    status_value = _enum_value(getattr(conversion, "status", None))
    if status_value not in {"success", "partial_success"}:
        raise RuntimeError(f"Docling conversion failed for '{doc_id}' with status '{status_value}'")

    document = conversion.document
    structure = document.export_to_dict()
    content = document.export_to_markdown()
    headings = _build_heading_tree(structure)
    tables = structure.get("tables", [])
    page_map = _build_page_map(structure)
    parser_version = _get_docling_version()
    page_count = _derive_page_count(structure, conversion)

    content_path = output_dir / "content.md"
    structure_path = output_dir / "structure.json"
    headings_path = output_dir / "headings.json"
    tables_path = output_dir / "tables.json"
    page_map_path = output_dir / "page_map.json"
    meta_path = output_dir / "meta.json"

    content_path.write_text(content, encoding="utf-8")
    _write_json(structure_path, structure)
    _write_json(headings_path, {"doc_id": doc_id, "headings": headings})
    _write_json(tables_path, {"doc_id": doc_id, "tables": tables})
    _write_json(page_map_path, {"doc_id": doc_id, "items": page_map})
    _write_json(
        meta_path,
        {
            "doc_id": doc_id,
            "parser": "docling",
            "parser_version": parser_version,
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "processing_time_seconds": round(processing_time, 4),
            "page_count": page_count,
            "status": status_value,
            "input_path": str(normalized_path),
            "input_checksum": compute_sha256(normalized_path),
            "document_hash": _get_nested_value(conversion, "input", "document_hash"),
            "timings": _to_serializable(getattr(conversion, "timings", None)),
            "confidence": _to_serializable(getattr(conversion, "confidence", None)),
            "errors": _to_serializable(getattr(conversion, "errors", [])),
        },
    )

    _persist_manifest_status(manifest, resolved_data_dir)
    return ParseResult(
        doc_id=doc_id,
        content_path=content_path,
        structure_path=structure_path,
        headings_path=headings_path,
        tables_path=tables_path,
        page_map_path=page_map_path,
        meta_path=meta_path,
        parser_version=parser_version,
        page_count=page_count,
        processing_time=round(processing_time, 4),
    )


def parse_all_documents(*, data_dir: Path | None = None) -> list[ParseResult]:
    """Parse every manifest that has a normalized PDF available."""
    resolved_data_dir = get_data_dir(data_dir)
    results: list[ParseResult] = []
    for manifest in list_manifests(resolved_data_dir):
        normalized_path = resolved_data_dir / "normalized" / f"{manifest.doc_id}.pdf"
        if not normalized_path.exists():
            continue
        results.append(parse_document(manifest.doc_id, data_dir=resolved_data_dir))
    return results


def _get_document_converter() -> Any:
    """Import and instantiate the Docling document converter lazily."""
    from docling.document_converter import DocumentConverter

    return DocumentConverter()


def _resolve_normalized_path(data_dir: Path, doc_id: str) -> Path:
    """Return the normalized PDF path for a document."""
    path = data_dir / "normalized" / f"{doc_id}.pdf"
    if not path.exists():
        raise FileNotFoundError(f"normalized PDF not found for doc_id '{doc_id}'")
    return path


def _derive_page_count(structure: dict[str, Any], conversion: Any) -> int:
    """Derive the parsed page count from Docling output."""
    pages = structure.get("pages")
    if isinstance(pages, dict):
        return len(pages)
    if isinstance(pages, list):
        return len(pages)
    input_page_count = _get_nested_value(conversion, "input", "page_count")
    return int(input_page_count) if input_page_count is not None else 0


def _build_heading_tree(structure: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert flat Docling text nodes into a nested heading tree."""
    headings: list[dict[str, Any]] = []
    stack: list[dict[str, Any]] = []
    for text_item in structure.get("texts", []):
        label = str(text_item.get("label", "")).casefold()
        level = _HEADING_LEVELS.get(label)
        title = str(text_item.get("text", "")).strip()
        if level is None or not title:
            continue

        node = {
            "title": title,
            "label": label,
            "level": level,
            "page_number": _first_page_number(text_item),
            "item_ref": text_item.get("self_ref"),
            "children": [],
        }
        while stack and stack[-1]["level"] >= level:
            stack.pop()
        if stack:
            stack[-1]["children"].append(node)
        else:
            headings.append(node)
        stack.append(node)
    return headings


def _build_page_map(structure: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a content-to-page map from text and table provenance."""
    items: list[dict[str, Any]] = []
    for collection_name, item_type in (("texts", "text"), ("tables", "table")):
        for item in structure.get(collection_name, []):
            page_numbers = _page_numbers(item)
            if not page_numbers:
                continue
            entry: dict[str, Any] = {
                "item_ref": item.get("self_ref"),
                "item_type": item_type,
                "label": item.get("label"),
                "page_numbers": page_numbers,
            }
            if item_type == "text":
                entry["text"] = item.get("text")
            else:
                entry["table_rows"] = len(item.get("data", [])) if isinstance(item.get("data"), list) else None
            items.append(entry)
    return items


def _first_page_number(item: dict[str, Any]) -> int | None:
    """Extract the first page number from a Docling item provenance list."""
    page_numbers = _page_numbers(item)
    return page_numbers[0] if page_numbers else None


def _page_numbers(item: dict[str, Any]) -> list[int]:
    """Collect unique ordered page numbers from a Docling item provenance list."""
    numbers: list[int] = []
    for provenance in item.get("prov", []) or []:
        page_number = provenance.get("page_no")
        if isinstance(page_number, int) and page_number not in numbers:
            numbers.append(page_number)
    return numbers


def _persist_manifest_status(manifest: ManifestEntry, data_dir: Path) -> None:
    """Transition a manifest to parsed status and persist it."""
    parsed = manifest.transition_status(
        DocumentStatus.PARSED,
        reason="parsed via Docling",
    )
    if parsed == manifest and manifest.document.status == DocumentStatus.PARSED:
        return

    manifest_path = data_dir / "manifests" / f"{manifest.doc_id}.yaml"
    manifest_path.write_text(parsed.to_yaml(), encoding="utf-8")


def _get_docling_version() -> str:
    """Return the installed Docling version when available."""
    try:
        return version("docling")
    except PackageNotFoundError:
        return "unknown"


def _enum_value(value: Any) -> str:
    """Normalize enums and other status values into strings."""
    if isinstance(value, Enum):
        return str(value.value)
    return str(value or "unknown")


def _get_nested_value(value: Any, *parts: str) -> Any:
    """Traverse object attributes or dict keys without raising."""
    current = value
    for part in parts:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
            continue
        current = getattr(current, part, None)
    return current


def _write_json(path: Path, payload: Any) -> None:
    """Persist a JSON payload using the repo's standard formatting."""
    path.write_text(json.dumps(_to_serializable(payload), indent=2, sort_keys=True), encoding="utf-8")


def _to_serializable(value: Any) -> Any:
    """Recursively convert Docling and Pydantic objects into JSON-safe values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _to_serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_serializable(item) for item in value]
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _to_serializable(model_dump(mode="json"))
    if hasattr(value, "__dict__"):
        return _to_serializable(vars(value))
    return str(value)


try:  # pragma: no cover - optional import boundary
    from prefect import flow

    @flow(name="parse-document")
    def parse_flow(doc_id: str, data_dir: str | Path | None = None) -> ParseResult:
        """Prefect flow wrapper for Docling parsing."""
        return parse_document(doc_id, data_dir=data_dir if data_dir else None)

except ImportError:  # pragma: no cover
    pass
