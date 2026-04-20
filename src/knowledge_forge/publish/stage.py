"""Publish staging for FlowCommander handoff."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field
from yaml import safe_dump, safe_load

from knowledge_forge.compile.source_pages import CompiledPage, CompileMetadata
from knowledge_forge.intake.importer import get_data_dir
from knowledge_forge.intake.manifest import slugify
from knowledge_forge.publish.manifest import PublishManifest, generate_publish_manifest

ALLOWED_PUBLISH_DIRECTORIES = frozenset(
    {
        "controllers",
        "fault-codes",
        "symptoms",
        "workflow-guidance",
        "contradictions",
        "supersessions",
        "source-index",
        "_manifests",
        "_publish-log",
        "_sources",
    }
)
TOPIC_DIRECTORY_MAP = {
    "startup_procedure": "workflow-guidance",
    "shutdown_procedure": "workflow-guidance",
    "maintenance_procedure": "workflow-guidance",
    "alarm_reference": "fault-codes",
    "troubleshooting": "symptoms",
    "specifications": "controllers",
}
TOPIC_DIGEST_TYPE_MAP = {
    "startup_procedure": "workflow-guidance",
    "shutdown_procedure": "workflow-guidance",
    "maintenance_procedure": "workflow-guidance",
    "alarm_reference": "fault-code",
    "troubleshooting": "symptom",
    "specifications": "controller",
}


class StagedPublish(BaseModel):
    """Filesystem locations and manifest details for a staged publish run."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    publish_run_id: str
    stage_dir: Path
    publish_root: Path
    manifest_path: Path
    log_path: Path
    source_snapshot_paths: list[Path]
    files_written: list[str]
    files_updated: list[str] = Field(default_factory=list)
    files_removed: list[str] = Field(default_factory=list)


def stage_publish(
    publish_run_id: str,
    compiled_pages: list[CompiledPage],
    *,
    data_dir: Path | None = None,
) -> StagedPublish:
    """Stage compiled pages into the publish contract directory layout."""
    if not publish_run_id.strip():
        raise ValueError("publish_run_id must not be blank")
    if not compiled_pages:
        raise ValueError("compiled_pages must not be empty")

    resolved_data_dir = get_data_dir(data_dir)
    stage_dir = resolved_data_dir / "publish" / publish_run_id
    if stage_dir.exists() and any(stage_dir.iterdir()):
        raise FileExistsError(
            f"publish stage directory already exists and is non-empty: {stage_dir}. "
            "Use a different publish_run_id or remove the existing staged run."
        )
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    publish_root.mkdir(parents=True, exist_ok=True)

    files_written: list[str] = []
    source_snapshot_paths: list[Path] = []
    buckets: set[str] = set()
    source_documents: set[str] = set()
    extraction_versions: set[str] = set()
    compilation_versions: set[str] = set()

    for page in compiled_pages:
        staged_page = _build_staged_page(page, publish_run_id=publish_run_id)
        if staged_page is None:
            continue

        destination = publish_root / staged_page.relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(_render_markdown(staged_page.frontmatter, staged_page.content), encoding="utf-8")
        files_written.append(staged_page.relative_path.as_posix())

        bucket_id = staged_page.frontmatter.get("bucket_id")
        if isinstance(bucket_id, str) and bucket_id:
            buckets.add(bucket_id)
        extraction_version = staged_page.frontmatter.get("extraction_version")
        if isinstance(extraction_version, str) and extraction_version:
            extraction_versions.update(_split_versions(extraction_version))
        compilation_version = staged_page.frontmatter.get("compilation_version")
        if isinstance(compilation_version, str) and compilation_version:
            compilation_versions.update(_split_versions(compilation_version))

        for source_document in _source_documents(staged_page.frontmatter):
            doc_id = source_document["doc_id"]
            source_documents.add(doc_id)
            snapshot_path = publish_root / "_sources" / f"{doc_id}.json"
            if snapshot_path in source_snapshot_paths:
                continue
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(json.dumps(source_document, indent=2, sort_keys=True), encoding="utf-8")
            source_snapshot_paths.append(snapshot_path)

    if not files_written:
        raise ValueError("compiled_pages did not contain any publishable pages for the FlowCommander contract")

    manifest = PublishManifest.model_validate(
        generate_publish_manifest(
            publish_run_id,
            files_written,
            source_documents=source_documents,
            buckets=buckets,
            extraction_version=_join_versions(extraction_versions),
            compilation_version=_join_versions(compilation_versions),
        )
    )
    manifest_path = publish_root / "_manifests" / f"{publish_run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

    log_path = publish_root / "_publish-log" / f"{publish_run_id}.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

    return StagedPublish(
        publish_run_id=publish_run_id,
        stage_dir=stage_dir,
        publish_root=publish_root,
        manifest_path=manifest_path,
        log_path=log_path,
        source_snapshot_paths=source_snapshot_paths,
        files_written=sorted(files_written),
    )


def load_compiled_pages(
    compiled_root: str | Path = "compiled",
    *,
    data_dir: Path | None = None,
) -> list[CompiledPage]:
    """Load compiled Markdown pages from disk for publish staging."""
    resolved_data_dir = get_data_dir(data_dir)
    resolved_root = Path(compiled_root)
    if not resolved_root.is_absolute():
        resolved_root = (resolved_data_dir / resolved_root).resolve()
    if not resolved_root.exists():
        raise FileNotFoundError(f"compiled root not found: {resolved_root}")

    pages: list[CompiledPage] = []
    for markdown_path in sorted(resolved_root.rglob("*.md")):
        frontmatter, content = _split_frontmatter(markdown_path)
        pages.append(
            CompiledPage(
                output_path=markdown_path,
                doc_id=str(frontmatter.get("doc_id") or frontmatter.get("bucket_id") or markdown_path.stem),
                frontmatter=frontmatter,
                content=content,
                compile_metadata=CompileMetadata(
                    generated_at=str(frontmatter.get("generated_at", "unknown")),
                    extraction_versions=_split_versions(frontmatter.get("extraction_version")),
                    parser_versions=[],
                    record_counts={},
                    review_flag_count=0,
                ),
            )
        )
    return pages


class _StagedPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relative_path: Path
    frontmatter: dict[str, object]
    content: str


def _build_staged_page(page: CompiledPage, *, publish_run_id: str) -> _StagedPage | None:
    page_group = _compiled_page_group(page.output_path)
    if page_group == "overview-pages":
        return None
    if page_group == "source-pages":
        return _stage_source_index_page(page, publish_run_id=publish_run_id)
    if page_group == "topic-pages":
        return _stage_topic_digest_page(page, publish_run_id=publish_run_id)
    if page_group == "contradiction-notes":
        return _stage_contradiction_page(page, publish_run_id=publish_run_id)
    raise ValueError(f"unsupported compiled page group '{page_group}' for publish staging")


def _stage_source_index_page(page: CompiledPage, *, publish_run_id: str) -> _StagedPage:
    frontmatter = dict(page.frontmatter)
    frontmatter["publish_run"] = publish_run_id
    doc_id = str(frontmatter.get("doc_id") or page.doc_id)
    return _StagedPage(
        relative_path=Path("source-index") / f"{doc_id}.md",
        frontmatter=frontmatter,
        content=page.content,
    )


def _stage_topic_digest_page(page: CompiledPage, *, publish_run_id: str) -> _StagedPage:
    frontmatter = _digest_frontmatter(page.frontmatter, publish_run_id=publish_run_id)
    directory = _digest_directory(frontmatter)
    slug = str(frontmatter["slug"])
    return _StagedPage(
        relative_path=Path(directory) / f"{slug}.md",
        frontmatter=frontmatter,
        content=_render_digest_content(page.content, frontmatter),
    )


def _stage_contradiction_page(page: CompiledPage, *, publish_run_id: str) -> _StagedPage:
    bucket_id = str(page.frontmatter.get("bucket_id") or page.doc_id)
    slug = str(page.frontmatter.get("slug") or slugify(bucket_id))
    frontmatter: dict[str, object] = {
        "title": str(page.frontmatter.get("title") or f"Contradictions for {bucket_id}"),
        "digest_type": "contradiction",
        "slug": slug,
        "status": "draft",
        "source_documents": _normalized_digest_source_documents(page.frontmatter),
        "knowledge_record_ids": [],
        "cross_links": [],
        "generated_by": str(page.frontmatter.get("generated_by", "knowledge-forge")),
        "publish_run": publish_run_id,
        "generated_at": str(page.frontmatter.get("generated_at", "unknown")),
        "extraction_version": str(page.frontmatter.get("extraction_version", "unknown")),
        "compilation_version": str(page.frontmatter.get("compilation_version", "unknown")),
        "bucket_id": bucket_id,
        "contradiction_key": slug,
        "conflicting_pages": [],
        "resolution_status": "needs-review",
        "tags": ["contradiction", slugify(bucket_id)],
    }
    return _StagedPage(
        relative_path=Path("contradictions") / f"{slug}.md",
        frontmatter=frontmatter,
        content=_render_digest_content(page.content, frontmatter),
    )


def _digest_frontmatter(frontmatter: dict[str, object], *, publish_run_id: str) -> dict[str, object]:
    topic = frontmatter.get("topic")
    bucket_id = frontmatter.get("bucket_id")
    digest_type = str(frontmatter.get("digest_type") or _digest_type_for_topic(topic))
    slug = str(frontmatter.get("slug") or _digest_slug_for_topic(bucket_id, topic, digest_type=digest_type))
    normalized: dict[str, object] = {
        "title": str(frontmatter.get("title") or slug.replace("-", " ").title()),
        "digest_type": digest_type,
        "slug": slug,
        "status": str(frontmatter.get("status") or "draft"),
        "source_documents": _normalized_digest_source_documents(frontmatter),
        "knowledge_record_ids": _list_value(frontmatter.get("knowledge_record_ids")),
        "cross_links": _list_value(frontmatter.get("cross_links")),
        "generated_by": str(frontmatter.get("generated_by", "knowledge-forge")),
        "publish_run": publish_run_id,
        "generated_at": str(frontmatter.get("generated_at", "unknown")),
        "extraction_version": str(frontmatter.get("extraction_version", "unknown")),
        "compilation_version": str(frontmatter.get("compilation_version", "unknown")),
        "bucket_id": str(bucket_id) if isinstance(bucket_id, str) else "",
        "tags": _list_value(frontmatter.get("tags")),
    }
    if digest_type == "controller":
        normalized["controller_models"] = _list_value(frontmatter.get("controller_models"))
        normalized["system_types"] = _list_value(frontmatter.get("system_types"))
    elif digest_type == "fault-code":
        normalized["fault_code"] = str(frontmatter.get("fault_code") or slug)
        normalized["controller_models"] = _list_value(frontmatter.get("controller_models"))
    elif digest_type == "symptom":
        normalized["symptom_key"] = str(frontmatter.get("symptom_key") or slug)
        normalized["system_types"] = _list_value(frontmatter.get("system_types"))
    elif digest_type == "workflow-guidance":
        normalized["workflow_key"] = str(frontmatter.get("workflow_key") or slug)
    elif digest_type == "supersession":
        normalized["superseded_slug"] = str(frontmatter.get("superseded_slug") or "")
        normalized["replacement_slug"] = str(frontmatter.get("replacement_slug") or "")
        normalized["reason"] = str(frontmatter.get("reason") or "")
    return normalized


def _render_digest_content(content: str, frontmatter: dict[str, object]) -> str:
    sections = _extract_sections(content)
    summary_lines = sections.get("Draft Synthesis") or sections.get("Summary") or _fallback_summary(frontmatter)
    guidance_lines = (
        sections.get("Field Guidance")
        or _combine_sections(
            sections.get("Source-backed Claims"),
            sections.get("Applicability Differences"),
            sections.get("Potential Contradictions"),
        )
        or ["- Review the source citations for synthesized guidance."]
    )
    citation_lines = _render_source_citations(frontmatter)
    related_lines = _render_related_pages(frontmatter)
    lines = [
        "## Summary",
        "",
        *summary_lines,
        "",
        "## Field Guidance",
        "",
        *guidance_lines,
        "",
        "## Source Citations",
        "",
        *citation_lines,
        "",
        "## Related Pages",
        "",
        *related_lines,
    ]
    return "\n".join(lines).rstrip()


def _extract_sections(content: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current_name: str | None = None
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if line.startswith("## "):
            current_name = line[3:].strip()
            sections[current_name] = []
            continue
        if line.startswith("# "):
            continue
        if current_name is not None:
            sections[current_name].append(line)
    return {name: _strip_blank_edges(lines) for name, lines in sections.items() if _strip_blank_edges(lines)}


def _strip_blank_edges(lines: list[str]) -> list[str]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def _combine_sections(*sections: list[str] | None) -> list[str]:
    lines: list[str] = []
    for section in sections:
        if not section:
            continue
        if lines:
            lines.append("")
        lines.extend(section)
    return lines


def _fallback_summary(frontmatter: dict[str, object]) -> list[str]:
    digest_type = str(frontmatter.get("digest_type", "digest"))
    title = str(frontmatter.get("title", "Compiled digest"))
    return [f"{title} is a draft {digest_type} staged from Knowledge Forge compiled output for downstream review."]


def _render_source_citations(frontmatter: dict[str, object]) -> list[str]:
    lines: list[str] = []
    for document in _normalized_digest_source_documents(frontmatter):
        title = str(document.get("title") or document.get("doc_id") or "Unknown source")
        attachment_id = document.get("attachment_id")
        locator = str(document.get("locator") or "locator unavailable")
        claim_summary = "Compiled into this digest during publish staging."
        attachment_label = (
            f"attachment: {attachment_id}" if attachment_id not in (None, "") else "attachment: unavailable"
        )
        lines.append(f"- `{title}` | `{attachment_label}` | `{locator}`")
        lines.append(f"  {claim_summary}")
    if lines:
        return lines
    return ["- No source citations were carried into the staged digest."]


def _render_related_pages(frontmatter: dict[str, object]) -> list[str]:
    cross_links = _list_value(frontmatter.get("cross_links"))
    if not cross_links:
        return ["- None yet."]
    return [f"- {link}" for link in cross_links]


def _normalized_digest_source_documents(frontmatter: dict[str, object]) -> list[dict[str, object]]:
    documents: list[dict[str, object]] = []
    for entry in _source_documents(frontmatter):
        normalized = dict(entry)
        if "title" not in normalized or not normalized["title"]:
            normalized["title"] = normalized.get("doc_id") or "Unknown source"
        normalized.setdefault("attachment_id", None)
        normalized.setdefault("locator", "locator unavailable")
        documents.append(normalized)
    return documents


def _digest_directory(frontmatter: dict[str, object]) -> str:
    digest_type = str(frontmatter["digest_type"])
    directory_map = {
        "controller": "controllers",
        "fault-code": "fault-codes",
        "symptom": "symptoms",
        "workflow-guidance": "workflow-guidance",
        "contradiction": "contradictions",
        "supersession": "supersessions",
    }
    try:
        directory = directory_map[digest_type]
    except KeyError as exc:
        raise ValueError(f"unsupported digest_type for publish staging: {digest_type}") from exc
    if directory not in ALLOWED_PUBLISH_DIRECTORIES:
        raise ValueError(f"unsupported publish directory for digest_type {digest_type}: {directory}")
    return directory


def _compiled_page_group(output_path: Path) -> str:
    output_parts = output_path.parts
    try:
        compiled_index = output_parts.index("compiled")
    except ValueError as exc:
        raise ValueError(f"compiled page path does not include a compiled/ segment: {output_path}") from exc
    compiled_relative = Path(*output_parts[compiled_index + 1 :])
    if not compiled_relative.parts:
        raise ValueError(f"compiled page path is missing a page type: {output_path}")
    return compiled_relative.parts[0]


def _digest_type_for_topic(topic: object) -> str:
    if not isinstance(topic, str) or not topic:
        raise ValueError("compiled topic page missing frontmatter topic")
    try:
        return TOPIC_DIGEST_TYPE_MAP[topic]
    except KeyError as exc:
        raise ValueError(f"unsupported topic page type '{topic}' for publish staging") from exc


def _digest_slug_for_topic(bucket_id: object, topic: object, *, digest_type: str) -> str:
    if not isinstance(bucket_id, str) or not bucket_id:
        raise ValueError("compiled topic page missing frontmatter bucket_id")
    if not isinstance(topic, str) or not topic:
        raise ValueError("compiled topic page missing frontmatter topic")
    bucket_slug = slugify(bucket_id)
    if digest_type == "controller":
        return f"{bucket_slug}-controller-digest"
    if digest_type == "fault-code":
        return f"{bucket_slug}-alarm-reference"
    if digest_type == "symptom":
        return f"{bucket_slug}-troubleshooting"
    return f"{bucket_slug}-{slugify(topic)}"


def _render_markdown(frontmatter: dict[str, object], content: str) -> str:
    yaml_frontmatter = safe_dump(frontmatter, sort_keys=False).strip()
    return f"---\n{yaml_frontmatter}\n---\n\n{content.rstrip()}\n"


def _split_frontmatter(path: Path) -> tuple[dict[str, object], str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise ValueError(f"compiled page missing YAML frontmatter: {path}")
    _, remainder = text.split("---\n", 1)
    frontmatter_block, content = remainder.split("\n---\n", 1)
    payload = safe_load(frontmatter_block) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"compiled page frontmatter is not a mapping: {path}")
    return _normalize_frontmatter(payload), content.lstrip("\n")


def _source_documents(frontmatter: dict[str, object]) -> list[dict[str, object]]:
    payload = frontmatter.get("source_documents")
    if not isinstance(payload, list):
        return []
    documents: list[dict[str, object]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        doc_id = entry.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            continue
        documents.append({str(key): _normalize_frontmatter(value) for key, value in entry.items()})
    return documents


def _list_value(value: object) -> list[object]:
    if not isinstance(value, list):
        return []
    return [_normalize_frontmatter(item) for item in value]


def _join_versions(values: Iterable[str]) -> str:
    ordered = sorted({value for value in values if value})
    return ", ".join(ordered) if ordered else "unknown"


def _split_versions(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _normalize_frontmatter(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _normalize_frontmatter(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_frontmatter(item) for item in value]
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value
