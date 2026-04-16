"""Tests for publish staging and contract validation."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner
from yaml import safe_dump

from knowledge_forge.cli import cli
from knowledge_forge.compile.source_pages import CompiledPage, CompileMetadata
from knowledge_forge.publish import stage_publish, validate_publish_output


def _compiled_page(
    output_path: Path,
    *,
    doc_id: str,
    frontmatter: dict[str, object],
    content: str = "# Compiled\n",
) -> CompiledPage:
    return CompiledPage(
        output_path=output_path,
        doc_id=doc_id,
        frontmatter=frontmatter,
        content=content,
        compile_metadata=CompileMetadata(
            generated_at="2026-04-16T17:30:00Z",
            extraction_versions=["extract-v1"],
            parser_versions=["parser-v1"],
            record_counts={"records": 1},
            review_flag_count=0,
        ),
    )


def _frontmatter(
    *,
    publish_run: str = "unpublished",
    title: str = "Compiled Page",
    source_documents: list[dict[str, str]] | None = None,
    bucket_id: str | None = None,
    topic: str | None = None,
    doc_id: str | None = None,
    page_type: str | None = None,
    canonical_identity: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "title": title,
        "generated_by": "knowledge-forge",
        "publish_run": publish_run,
        "source_documents": source_documents
        or [
            {
                "doc_id": "honeywell-dc1000-service-manual-rev-3",
                "revision": "Rev 3",
                "manufacturer": "Honeywell",
                "family": "DC1000",
            }
        ],
        "generated_at": "2026-04-16T17:30:00Z",
        "extraction_version": "extract-v1",
        "compilation_version": "compile-v1",
    }
    if bucket_id is not None:
        payload["bucket_id"] = bucket_id
    if topic is not None:
        payload["topic"] = topic
    if doc_id is not None:
        payload["doc_id"] = doc_id
    if page_type is not None:
        payload["page_type"] = page_type
    if canonical_identity is not None:
        payload["canonical_identity"] = canonical_identity
    return payload


def _write_markdown(path: Path, frontmatter: dict[str, object], content: str = "# Title\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\n{safe_dump(frontmatter, sort_keys=False).strip()}\n---\n\n{content}", encoding="utf-8")


def _write_manifest(
    stage_dir: Path,
    run_id: str,
    *,
    files_written: list[str],
    files_removed: list[str] | None = None,
) -> None:
    manifest_path = stage_dir / "repo-wiki" / "knowledge" / "_manifests" / f"{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "publish_run_id": run_id,
                "generated_at": "2026-04-16T17:30:00Z",
                "knowledge_forge_version": "0.1.0",
                "source_documents": ["honeywell-dc1000-service-manual-rev-3"],
                "buckets": ["honeywell/dc1000/family"],
                "files_written": files_written,
                "files_updated": [],
                "files_removed": files_removed or [],
                "extraction_version": "extract-v1",
                "compilation_version": "compile-v1",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_stage_publish_maps_compiled_pages_and_generates_publish_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source_page = _compiled_page(
        data_dir / "compiled" / "source-pages" / "honeywell-dc1000-service-manual-rev-3.md",
        doc_id="honeywell-dc1000-service-manual-rev-3",
        frontmatter=_frontmatter(doc_id="honeywell-dc1000-service-manual-rev-3"),
    )
    topic_page = _compiled_page(
        data_dir / "compiled" / "topic-pages" / "honeywell-dc1000-family" / "startup_procedure.md",
        doc_id="honeywell/dc1000/family",
        frontmatter=_frontmatter(
            title="Honeywell DC1000 Startup Procedure",
            bucket_id="honeywell/dc1000/family",
            topic="startup_procedure",
        ),
    )
    overview_page = _compiled_page(
        data_dir / "compiled" / "overview-pages" / "manufacturers" / "honeywell" / "dc1000" / "_index.md",
        doc_id="honeywell/dc1000/family",
        frontmatter=_frontmatter(
            title="Honeywell DC1000 Family Overview",
            page_type="family_overview",
        ),
    )

    staged = stage_publish("kf-20260416-001", [source_page, topic_page, overview_page], data_dir=data_dir)

    assert staged.stage_dir == data_dir / "publish" / "kf-20260416-001"
    assert staged.publish_root == staged.stage_dir / "repo-wiki" / "knowledge"
    assert sorted(staged.files_written) == [
        "manufacturers/honeywell/dc1000/_index.md",
        "procedures/honeywell-dc1000-family-startup-procedure.md",
        "source-index/honeywell-dc1000-service-manual-rev-3.md",
    ]
    assert (staged.publish_root / "source-index" / "honeywell-dc1000-service-manual-rev-3.md").exists()
    assert (staged.publish_root / "procedures" / "honeywell-dc1000-family-startup-procedure.md").exists()
    manifest = json.loads(staged.manifest_path.read_text(encoding="utf-8"))
    assert manifest["publish_run_id"] == "kf-20260416-001"
    assert manifest["files_written"] == staged.files_written
    assert manifest["source_documents"] == ["honeywell-dc1000-service-manual-rev-3"]
    assert validate_publish_output(staged.stage_dir).valid is True


def test_validate_publish_output_accepts_supported_target_directories(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-20260416-002"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    source_doc = {
        "doc_id": "honeywell-dc1000-service-manual-rev-3",
        "revision": "Rev 3",
        "manufacturer": "Honeywell",
        "family": "DC1000",
    }

    files_written = [
        "manufacturers/honeywell/_index.md",
        "manufacturers/honeywell/dc1000/_index.md",
        "procedures/honeywell-dc1000-family-startup-procedure.md",
        "specs/honeywell-dc1000-family-specifications.md",
        "troubleshooting/honeywell-dc1000-family-troubleshooting.md",
        "workflow-guidance/honeywell-dc1000-family-sop.md",
        "parts/honeywell-dc1000-family-parts.md",
        "safety/honeywell-dc1000-family-safety.md",
        "source-index/honeywell-dc1000-service-manual-rev-3.md",
    ]

    _write_markdown(
        publish_root / "manufacturers" / "honeywell" / "_index.md",
        _frontmatter(
            title="Honeywell Manufacturer Index",
            source_documents=[source_doc],
            page_type="manufacturer_index",
        ),
    )
    _write_markdown(
        publish_root / "manufacturers" / "honeywell" / "dc1000" / "_index.md",
        _frontmatter(
            title="Honeywell DC1000 Family Overview",
            source_documents=[source_doc],
            page_type="family_overview",
        ),
    )
    _write_markdown(
        publish_root / "procedures" / "honeywell-dc1000-family-startup-procedure.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family", topic="startup_procedure"),
    )
    _write_markdown(
        publish_root / "specs" / "honeywell-dc1000-family-specifications.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family", topic="specifications"),
    )
    _write_markdown(
        publish_root / "troubleshooting" / "honeywell-dc1000-family-troubleshooting.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family", topic="troubleshooting"),
    )
    _write_markdown(
        publish_root / "workflow-guidance" / "honeywell-dc1000-family-sop.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family"),
    )
    _write_markdown(
        publish_root / "parts" / "honeywell-dc1000-family-parts.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family"),
    )
    _write_markdown(
        publish_root / "safety" / "honeywell-dc1000-family-safety.md",
        _frontmatter(source_documents=[source_doc], bucket_id="honeywell/dc1000/family"),
    )
    _write_markdown(
        publish_root / "source-index" / "honeywell-dc1000-service-manual-rev-3.md",
        _frontmatter(source_documents=[source_doc], doc_id="honeywell-dc1000-service-manual-rev-3"),
    )
    _write_manifest(stage_dir, "kf-20260416-002", files_written=files_written)

    report = validate_publish_output(stage_dir)

    assert report.valid is True
    assert report.errors == []


def test_validate_publish_output_rejects_invalid_structure_and_duplicate_identities(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-20260416-003"
    publish_root = stage_dir / "repo-wiki" / "knowledge"

    stage_dir.mkdir(parents=True, exist_ok=True)
    (stage_dir / "notes.txt").write_text("outside subtree", encoding="utf-8")
    _write_markdown(
        publish_root / "source-index" / "wrong-name.md",
        _frontmatter(doc_id="expected-doc-id"),
    )
    _write_markdown(
        publish_root / "procedures" / "bad-prefix.md",
        _frontmatter(bucket_id="honeywell/dc1000/family", topic="startup_procedure"),
    )
    _write_markdown(
        publish_root / "unknown-dir" / "page.md",
        _frontmatter(),
    )
    _write_markdown(
        publish_root / "specs" / "honeywell-dc1000-family-first.md",
        _frontmatter(bucket_id="honeywell/dc1000/family", canonical_identity="duplicate:spec-page"),
    )
    _write_markdown(
        publish_root / "specs" / "honeywell-dc1000-family-second.md",
        _frontmatter(bucket_id="honeywell/dc1000/family", canonical_identity="duplicate:spec-page"),
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-003",
        files_written=[
            "source-index/wrong-name.md",
            "procedures/bad-prefix.md",
            "specs/honeywell-dc1000-family-first.md",
            "specs/honeywell-dc1000-family-second.md",
        ],
        files_removed=["source-index/not-previously-published.md"],
    )

    report = validate_publish_output(stage_dir)

    assert report.valid is False
    assert any("outside allowed subtree" in error for error in report.errors)
    assert any("unrecognized target directory" in error for error in report.errors)
    assert any("filename must match frontmatter doc_id" in error for error in report.errors)
    assert any("filename must start with bucket slug" in error for error in report.errors)
    assert any("duplicate canonical identity" in error for error in report.errors)
    assert any("orphan removal not previously published" in error for error in report.errors)


def test_publish_validate_cli_reports_success_and_failure(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    valid_stage_dir = data_dir / "publish" / "kf-20260416-004"
    invalid_stage_dir = data_dir / "publish" / "kf-20260416-005"

    _write_markdown(
        valid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _frontmatter(
            doc_id="doc-1",
            source_documents=[{"doc_id": "doc-1", "manufacturer": "Honeywell", "family": "DC1000"}],
        ),
    )
    _write_manifest(valid_stage_dir, "kf-20260416-004", files_written=["source-index/doc-1.md"])

    _write_markdown(
        invalid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "wrong.md",
        _frontmatter(
            doc_id="doc-2",
            source_documents=[{"doc_id": "doc-2", "manufacturer": "Honeywell", "family": "DC1000"}],
        ),
    )
    _write_manifest(invalid_stage_dir, "kf-20260416-005", files_written=["source-index/wrong.md"])

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    success = runner.invoke(cli, ["publish", "validate", "kf-20260416-004"], env=env)
    assert success.exit_code == 0
    assert "Valid: yes" in success.output

    failure = runner.invoke(cli, ["publish", "validate", "kf-20260416-005"], env=env)
    assert failure.exit_code != 0
    assert "Valid: no" in failure.output
    assert "publish validation failed" in failure.output
