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

TOPIC_DIRECTORY_MAP = {
    "startup_procedure": "procedures",
    "shutdown_procedure": "procedures",
    "maintenance_procedure": "procedures",
    "alarm_reference": "troubleshooting",
    "troubleshooting": "troubleshooting",
    "specifications": "specs",
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
        relative_path = _publish_relative_path(page)
        destination = publish_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)

        frontmatter = dict(page.frontmatter)
        frontmatter["publish_run"] = publish_run_id
        destination.write_text(_render_markdown(frontmatter, page.content), encoding="utf-8")
        files_written.append(destination.relative_to(publish_root).as_posix())

        bucket_id = frontmatter.get("bucket_id")
        if isinstance(bucket_id, str) and bucket_id:
            buckets.add(bucket_id)
        extraction_version = frontmatter.get("extraction_version")
        if isinstance(extraction_version, str) and extraction_version:
            extraction_versions.add(extraction_version)
        compilation_version = frontmatter.get("compilation_version")
        if isinstance(compilation_version, str) and compilation_version:
            compilation_versions.add(compilation_version)

        for source_document in _source_documents(frontmatter):
            doc_id = source_document["doc_id"]
            source_documents.add(doc_id)
            snapshot_path = publish_root / "_sources" / f"{doc_id}.json"
            if snapshot_path not in source_snapshot_paths:
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                snapshot_path.write_text(json.dumps(source_document, indent=2, sort_keys=True), encoding="utf-8")
                source_snapshot_paths.append(snapshot_path)

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


def _publish_relative_path(page: CompiledPage) -> Path:
    output_parts = page.output_path.parts
    try:
        compiled_index = output_parts.index("compiled")
    except ValueError as exc:
        raise ValueError(f"compiled page path does not include a compiled/ segment: {page.output_path}") from exc

    compiled_relative = Path(*output_parts[compiled_index + 1 :])
    if not compiled_relative.parts:
        raise ValueError(f"compiled page path is missing a page type: {page.output_path}")

    page_group = compiled_relative.parts[0]
    if page_group == "source-pages":
        return Path("source-index") / f"{page.doc_id}.md"
    if page_group == "overview-pages":
        overview_relative = Path(*compiled_relative.parts[1:])
        if overview_relative.parts[:1] == ("manufacturers",):
            overview_relative = Path(*overview_relative.parts[1:])
        return Path("manufacturers") / overview_relative
    if page_group == "topic-pages":
        if len(compiled_relative.parts) < 2:
            raise ValueError(f"compiled topic page path is missing a bucket slug: {page.output_path}")
        bucket_slug = compiled_relative.parts[1]
        topic = page.frontmatter.get("topic")
        if not isinstance(topic, str) or not topic:
            raise ValueError(f"compiled topic page missing frontmatter topic: {page.output_path}")
        directory = TOPIC_DIRECTORY_MAP.get(topic)
        if directory is None:
            raise ValueError(f"unsupported topic page type '{topic}' for publish staging")
        return Path(directory) / f"{bucket_slug}-{slugify(topic)}.md"
    if page_group == "contradiction-notes":
        if len(compiled_relative.parts) < 2:
            raise ValueError(f"compiled contradiction-notes page path is missing a filename: {page.output_path}")
        return Path("analysis") / "contradiction-notes" / compiled_relative.parts[1]
    raise ValueError(f"unsupported compiled page group '{page_group}' for publish staging")


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


def _source_documents(frontmatter: dict[str, object]) -> list[dict[str, str]]:
    payload = frontmatter.get("source_documents")
    if not isinstance(payload, list):
        return []
    documents: list[dict[str, str]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        doc_id = entry.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            continue
        documents.append({key: str(value) for key, value in entry.items() if value is not None})
    return documents


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
