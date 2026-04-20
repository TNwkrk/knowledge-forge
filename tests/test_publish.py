"""Tests for publish staging, contract validation, and PR creation."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner
from yaml import safe_dump

from knowledge_forge.cli import cli
from knowledge_forge.compile.source_pages import CompiledPage, CompileMetadata
from knowledge_forge.publish import (
    create_publish_pr,
    generate_publish_manifest,
    list_publish_runs,
    stage_publish,
    validate_publish_output,
)
from knowledge_forge.publish.pr import PRResult


def _source_doc(
    *,
    doc_id: str = "honeywell-dc1000-service-manual-rev-3",
    title: str = "Honeywell DC1000 Service Manual (Rev 3)",
    locator: str = 'section "Startup Procedure" (p.18)',
) -> dict[str, object]:
    return {
        "doc_id": doc_id,
        "title": title,
        "attachment_id": None,
        "locator": locator,
        "revision": "Rev 3",
        "manufacturer": "Honeywell",
        "family": "DC1000",
    }


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
        "source_documents": source_documents or [_source_doc()],
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


def _digest_frontmatter(
    *,
    digest_type: str,
    slug: str,
    title: str = "Compiled Digest",
    source_documents: list[dict[str, object]] | None = None,
    cross_links: list[str] | None = None,
    bucket_id: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "title": title,
        "digest_type": digest_type,
        "slug": slug,
        "status": "draft",
        "source_documents": source_documents or [_source_doc()],
        "knowledge_record_ids": [],
        "cross_links": cross_links or [],
        "generated_by": "knowledge-forge",
        "publish_run": "kf-20260416-001",
        "generated_at": "2026-04-16T17:30:00Z",
        "extraction_version": "extract-v1",
        "compilation_version": "compile-v1",
        "tags": [digest_type],
    }
    if bucket_id is not None:
        payload["bucket_id"] = bucket_id
    if digest_type == "controller":
        payload["controller_models"] = ["DC1000"]
        payload["system_types"] = []
    elif digest_type == "fault-code":
        payload["fault_code"] = "RUN"
        payload["controller_models"] = ["DC1000"]
    elif digest_type == "symptom":
        payload["symptom_key"] = "startup-failure"
        payload["system_types"] = []
    elif digest_type == "workflow-guidance":
        payload["workflow_key"] = slug
    elif digest_type == "contradiction":
        payload["contradiction_key"] = slug
        payload["conflicting_pages"] = []
        payload["resolution_status"] = "needs-review"
    elif digest_type == "supersession":
        payload["superseded_slug"] = "old-page"
        payload["replacement_slug"] = "new-page"
        payload["reason"] = "updated guidance"
    return payload


def _source_index_frontmatter(
    *,
    doc_id: str = "doc-1",
    source_documents: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    default_source = {
        "doc_id": doc_id,
        "revision": "Rev 3",
        "manufacturer": "Honeywell",
        "family": "DC1000",
    }
    return {
        "title": "Source Manual",
        "generated_by": "knowledge-forge",
        "publish_run": "kf-20260416-001",
        "source_documents": source_documents or [default_source],
        "generated_at": "2026-04-16T17:30:00Z",
        "extraction_version": "extract-v1",
        "compilation_version": "source-pages-v1",
        "doc_id": doc_id,
    }


def _source_index_docs(doc_id: str) -> list[dict[str, str]]:
    return [
        {
            "doc_id": doc_id,
            "revision": "Rev 3",
            "manufacturer": "Honeywell",
            "family": "DC1000",
        }
    ]


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


def _init_git_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=path, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "config", "user.name", "Knowledge Forge"], cwd=path, check=True, capture_output=True, text=True
    )
    subprocess.run(
        ["git", "config", "user.email", "knowledge-forge@example.com"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["git", "add", "-A"], cwd=path, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "Initial"], cwd=path, check=True, capture_output=True, text=True
    )
    subprocess.run(["git", "remote", "add", "origin", str(path)], cwd=path, check=True, capture_output=True, text=True)
    subprocess.run(["git", "fetch", "origin"], cwd=path, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
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
        )
        | {
            "digest_type": "workflow-guidance",
            "slug": "honeywell-dc1000-family-startup-procedure",
            "status": "draft",
            "knowledge_record_ids": [],
            "cross_links": ["../controllers/honeywell-dc1000-family-controller-digest.md"],
            "workflow_key": "honeywell-dc1000-family-startup-procedure",
            "tags": ["workflow-guidance"],
        },
        content=(
            "# Startup Procedure\n\n"
            "## Draft Synthesis\n\n"
            "- Follow the staged startup sequence.\n\n"
            "## Source-backed Claims\n\n"
            "- Verify valve position before energizing. [Source: honeywell-dc1000-service-manual-rev-3, p.18]\n"
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
        "source-index/honeywell-dc1000-service-manual-rev-3.md",
        "workflow-guidance/honeywell-dc1000-family-startup-procedure.md",
    ]
    assert not (staged.publish_root / "manufacturers" / "honeywell" / "dc1000" / "_index.md").exists()
    assert (staged.publish_root / "source-index" / "honeywell-dc1000-service-manual-rev-3.md").exists()
    digest_path = staged.publish_root / "workflow-guidance" / "honeywell-dc1000-family-startup-procedure.md"
    assert digest_path.exists()
    digest_text = digest_path.read_text(encoding="utf-8")
    assert "digest_type: workflow-guidance" in digest_text
    assert "workflow_key: honeywell-dc1000-family-startup-procedure" in digest_text
    assert "## Summary" in digest_text
    assert "## Field Guidance" in digest_text
    assert "## Source Citations" in digest_text
    assert "## Related Pages" in digest_text
    manifest = json.loads(staged.manifest_path.read_text(encoding="utf-8"))
    assert manifest["publish_run_id"] == "kf-20260416-001"
    assert manifest["files_written"] == staged.files_written
    assert manifest["source_documents"] == ["honeywell-dc1000-service-manual-rev-3"]
    assert validate_publish_output(staged.stage_dir).valid is True


def test_stage_publish_maps_specs_to_controllers_digest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    specs_page = _compiled_page(
        data_dir / "compiled" / "topic-pages" / "honeywell-dc1000-family" / "specifications.md",
        doc_id="honeywell/dc1000/family",
        frontmatter=_frontmatter(
            title="Honeywell DC1000 Specifications",
            bucket_id="honeywell/dc1000/family",
            topic="specifications",
        )
        | {
            "digest_type": "controller",
            "slug": "honeywell-dc1000-family-controller-digest",
            "status": "draft",
            "knowledge_record_ids": [],
            "cross_links": [],
            "controller_models": ["DC1000", "DC1200"],
            "system_types": [],
            "tags": ["controller"],
        },
        content=(
            "# Specifications\n\n"
            "## Draft Synthesis\n\n"
            "- Use this controller digest for family-level operating context.\n\n"
            "## Source-backed Claims\n\n"
            "- Operating pressure: 15 PSI.\n"
        ),
    )

    staged = stage_publish("kf-20260416-controllers", [specs_page], data_dir=data_dir)

    assert staged.files_written == [
        "controllers/honeywell-dc1000-family-controller-digest.md",
    ]
    assert validate_publish_output(staged.stage_dir).valid is True


def test_publish_stage_cli_loads_compiled_pages_from_disk(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    compiled_root = data_dir / "compiled" / "source-pages"
    compiled_root.mkdir(parents=True, exist_ok=True)
    compiled_page = compiled_root / "honeywell-dc1000-service-manual-rev-3.md"
    compiled_page.write_text(
        """---
title: Source Manual
generated_by: knowledge-forge
publish_run: unpublished
source_documents:
  - doc_id: honeywell-dc1000-service-manual-rev-3
    revision: Rev 3
    manufacturer: Honeywell
    family: DC1000
generated_at: 2026-04-16T17:30:00Z
extraction_version: extract-v1
compilation_version: source-pages-v1
doc_id: honeywell-dc1000-service-manual-rev-3
---

# Source Manual
""",
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["publish", "stage", "kf-20260416-123"],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Files written: 1" in result.output
    assert (data_dir / "publish" / "kf-20260416-123" / "repo-wiki" / "knowledge" / "source-index").exists()


def test_stage_publish_maps_contradiction_notes_to_analysis_subtree(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    contradiction_page = _compiled_page(
        data_dir / "compiled" / "contradiction-notes" / "honeywell-dc1000-family.md",
        doc_id="honeywell/dc1000/family",
        frontmatter=_frontmatter(
            title="Contradiction Notes: honeywell/dc1000/family",
            bucket_id="honeywell/dc1000/family",
        ),
    )

    staged = stage_publish("kf-20260416-002", [contradiction_page], data_dir=data_dir)

    assert staged.files_written == ["contradictions/honeywell-dc1000-family.md"]
    contradiction_path = staged.publish_root / "contradictions" / "honeywell-dc1000-family.md"
    assert contradiction_path.exists()
    contradiction_text = contradiction_path.read_text(encoding="utf-8")
    assert "digest_type: contradiction" in contradiction_text
    assert "resolution_status: needs-review" in contradiction_text
    assert validate_publish_output(staged.stage_dir).valid is True


def test_generate_publish_manifest_returns_expected_contract_fields() -> None:
    manifest = generate_publish_manifest(
        "kf-20260416-001",
        ["source-index/doc-1.md", "workflow-guidance/doc-1-startup.md"],
        source_documents=["doc-1"],
        buckets=["honeywell/dc1000/family"],
        files_updated=["controllers/doc-1-controller.md"],
        files_removed=["source-index/doc-old.md"],
        extraction_version="extract-v1",
        compilation_version="compile-v1",
    )

    assert manifest["publish_run_id"] == "kf-20260416-001"
    assert manifest["knowledge_forge_version"]
    assert manifest["source_documents"] == ["doc-1"]
    assert manifest["buckets"] == ["honeywell/dc1000/family"]
    assert manifest["files_written"] == ["source-index/doc-1.md", "workflow-guidance/doc-1-startup.md"]
    assert manifest["files_updated"] == ["controllers/doc-1-controller.md"]
    assert manifest["files_removed"] == ["source-index/doc-old.md"]
    assert manifest["generated_at"].endswith("Z")


def test_validate_publish_output_accepts_supported_target_directories(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-20260416-002"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    source_doc = _source_doc()

    files_written = [
        "controllers/honeywell-dc1000-family-controller-digest.md",
        "fault-codes/honeywell-dc1000-family-alarm-reference.md",
        "symptoms/honeywell-dc1000-family-troubleshooting.md",
        "workflow-guidance/honeywell-dc1000-family-startup-procedure.md",
        "contradictions/honeywell-dc1000-family.md",
        "source-index/honeywell-dc1000-service-manual-rev-3.md",
    ]

    _write_markdown(
        publish_root / "controllers" / "honeywell-dc1000-family-controller-digest.md",
        _digest_frontmatter(
            digest_type="controller",
            slug="honeywell-dc1000-family-controller-digest",
            title="Honeywell DC1000 Controller Digest",
            source_documents=[source_doc],
            cross_links=["../workflow-guidance/honeywell-dc1000-family-startup-procedure.md"],
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nController summary.\n\n## Field Guidance\n\nController guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- ../workflow-guidance/honeywell-dc1000-family-startup-procedure.md\n",
    )
    _write_markdown(
        publish_root / "fault-codes" / "honeywell-dc1000-family-alarm-reference.md",
        _digest_frontmatter(
            digest_type="fault-code",
            slug="honeywell-dc1000-family-alarm-reference",
            title="Honeywell DC1000 Alarm Reference",
            source_documents=[source_doc],
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nAlarm summary.\n\n## Field Guidance\n\nAlarm guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- None yet.\n",
    )
    _write_markdown(
        publish_root / "symptoms" / "honeywell-dc1000-family-troubleshooting.md",
        _digest_frontmatter(
            digest_type="symptom",
            slug="honeywell-dc1000-family-troubleshooting",
            title="Honeywell DC1000 Troubleshooting",
            source_documents=[source_doc],
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nSymptom summary.\n\n## Field Guidance\n\nSymptom guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- None yet.\n",
    )
    _write_markdown(
        publish_root / "workflow-guidance" / "honeywell-dc1000-family-startup-procedure.md",
        _digest_frontmatter(
            digest_type="workflow-guidance",
            slug="honeywell-dc1000-family-startup-procedure",
            title="Honeywell DC1000 Startup Procedure",
            source_documents=[source_doc],
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nWorkflow summary.\n\n## Field Guidance\n\nWorkflow guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- None yet.\n",
    )
    _write_markdown(
        publish_root / "contradictions" / "honeywell-dc1000-family.md",
        _digest_frontmatter(
            digest_type="contradiction",
            slug="honeywell-dc1000-family",
            title="Contradictions for Honeywell DC1000",
            source_documents=[source_doc],
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nContradiction summary.\n\n## Field Guidance\n\nContradiction guidance.\n\n"
        "## Source Citations\n\n- Citation\n\n## Related Pages\n\n- None yet.\n",
    )
    _write_markdown(
        publish_root / "source-index" / "honeywell-dc1000-service-manual-rev-3.md",
        _source_index_frontmatter(
            doc_id="honeywell-dc1000-service-manual-rev-3",
            source_documents=[
                {
                    "doc_id": "honeywell-dc1000-service-manual-rev-3",
                    "revision": "Rev 3",
                    "manufacturer": "Honeywell",
                    "family": "DC1000",
                }
            ],
        ),
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
        _source_index_frontmatter(doc_id="expected-doc-id"),
    )
    _write_markdown(
        publish_root / "workflow-guidance" / "bad-prefix.md",
        _digest_frontmatter(
            digest_type="workflow-guidance",
            slug="expected-slug",
            title="Bad Prefix",
        ),
        "## Field Guidance\n\nMissing ordered sections.\n",
    )
    _write_markdown(
        publish_root / "unknown-dir" / "page.md",
        _digest_frontmatter(digest_type="workflow-guidance", slug="unknown-dir-page"),
    )
    _write_markdown(
        publish_root / "controllers" / "first-duplicate.md",
        _digest_frontmatter(digest_type="controller", slug="duplicate-page"),
    )
    _write_markdown(
        publish_root / "controllers" / "second-duplicate.md",
        _digest_frontmatter(digest_type="controller", slug="duplicate-page"),
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-003",
        files_written=[
            "source-index/wrong-name.md",
            "workflow-guidance/bad-prefix.md",
            "controllers/first-duplicate.md",
            "controllers/second-duplicate.md",
        ],
        files_removed=["source-index/not-previously-published.md"],
    )

    report = validate_publish_output(stage_dir)

    assert report.valid is False
    assert any("outside allowed subtree" in error for error in report.errors)
    assert any("unrecognized target directory" in error for error in report.errors)
    assert any("filename must match frontmatter doc_id" in error for error in report.errors)
    assert any("filename must match frontmatter slug" in error for error in report.errors)
    assert any("duplicate canonical identity" in error for error in report.errors)
    assert any("orphan removal not previously published" in error for error in report.errors)


def test_validate_publish_output_rejects_missing_or_invalid_list_fields(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-20260416-006"
    publish_root = stage_dir / "repo-wiki" / "knowledge"

    invalid_frontmatter = _digest_frontmatter(
        digest_type="controller",
        slug="honeywell-dc1000-family-controller-digest",
        bucket_id="honeywell/dc1000/family",
    )
    invalid_frontmatter.pop("knowledge_record_ids")
    invalid_frontmatter["tags"] = ["controller", ""]
    invalid_frontmatter["cross_links"] = ["../workflow-guidance/honeywell-dc1000-family-startup-procedure.md", 123]

    _write_markdown(
        publish_root / "controllers" / "honeywell-dc1000-family-controller-digest.md",
        invalid_frontmatter,
        "## Summary\n\nController summary.\n\n## Field Guidance\n\nController guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- ../workflow-guidance/honeywell-dc1000-family-startup-procedure.md\n",
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-006",
        files_written=["controllers/honeywell-dc1000-family-controller-digest.md"],
    )

    report = validate_publish_output(stage_dir)

    assert report.valid is False
    assert any("missing required frontmatter fields ['knowledge_record_ids']" in error for error in report.errors)
    assert any("tags[1] must be a non-empty string" in error for error in report.errors)
    assert any("cross_links[1] must be a non-empty string" in error for error in report.errors)


def test_validate_publish_output_rejects_bucket_slug_mismatch(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-20260416-007"
    publish_root = stage_dir / "repo-wiki" / "knowledge"

    _write_markdown(
        publish_root / "controllers" / "wrong-controller-slug.md",
        _digest_frontmatter(
            digest_type="controller",
            slug="wrong-controller-slug",
            bucket_id="honeywell/dc1000/family",
        ),
        "## Summary\n\nController summary.\n\n## Field Guidance\n\nController guidance.\n\n## Source Citations\n\n"
        "- Citation\n\n## Related Pages\n\n- None yet.\n",
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-007",
        files_written=["controllers/wrong-controller-slug.md"],
    )

    report = validate_publish_output(stage_dir)

    assert report.valid is False
    assert any("slug must match bucket-derived pattern" in error for error in report.errors)


def test_publish_validate_cli_reports_success_and_failure(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    valid_stage_dir = data_dir / "publish" / "kf-20260416-004"
    invalid_stage_dir = data_dir / "publish" / "kf-20260416-005"

    _write_markdown(
        valid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(valid_stage_dir, "kf-20260416-004", files_written=["source-index/doc-1.md"])

    _write_markdown(
        invalid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "wrong.md",
        _source_index_frontmatter(
            doc_id="doc-2",
            source_documents=_source_index_docs("doc-2"),
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


def test_list_publish_runs_reports_ready_and_missing_manifest_runs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    valid_stage_dir = data_dir / "publish" / "kf-20260416-010"
    missing_stage_dir = data_dir / "publish" / "kf-20260416-011"

    _write_markdown(
        valid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(valid_stage_dir, "kf-20260416-010", files_written=["source-index/doc-1.md"])
    missing_stage_dir.mkdir(parents=True, exist_ok=True)

    runs = list_publish_runs(data_dir)

    assert [run.publish_run_id for run in runs] == ["kf-20260416-010", "kf-20260416-011"]
    assert runs[0].status == "ready"
    assert runs[1].status == "missing-manifest"


def test_list_publish_runs_validate_flag_returns_valid_or_invalid(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    valid_stage_dir = data_dir / "publish" / "kf-20260416-010"
    missing_stage_dir = data_dir / "publish" / "kf-20260416-011"

    _write_markdown(
        valid_stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(valid_stage_dir, "kf-20260416-010", files_written=["source-index/doc-1.md"])
    missing_stage_dir.mkdir(parents=True, exist_ok=True)

    runs = list_publish_runs(data_dir, validate=True)

    assert [run.publish_run_id for run in runs] == ["kf-20260416-010", "kf-20260416-011"]
    assert runs[0].status == "valid"
    assert runs[1].status == "missing-manifest"


def test_list_publish_runs_id_mismatch_when_manifest_run_id_differs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stage_dir = data_dir / "publish" / "kf-20260416-010"

    _write_markdown(
        stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    # Write a manifest file named after the directory but with a mismatched publish_run_id inside
    manifest_path = stage_dir / "repo-wiki" / "knowledge" / "_manifests" / "kf-20260416-010.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "publish_run_id": "kf-20260416-WRONG",
                "generated_at": "2026-04-16T17:30:00Z",
                "knowledge_forge_version": "0.1.0",
                "source_documents": ["doc-1"],
                "buckets": [],
                "files_written": ["source-index/doc-1.md"],
                "files_updated": [],
                "files_removed": [],
                "extraction_version": "extract-v1",
                "compilation_version": "compile-v1",
            }
        ),
        encoding="utf-8",
    )

    runs = list_publish_runs(data_dir)

    assert len(runs) == 1
    assert runs[0].publish_run_id == "kf-20260416-010"
    assert runs[0].status == "id-mismatch"


def test_publish_log_and_inspect_cli_report_staged_history(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stage_dir = data_dir / "publish" / "kf-20260416-012"
    _write_markdown(
        stage_dir / "repo-wiki" / "knowledge" / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-012",
        files_written=["source-index/doc-1.md"],
    )

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    # Default (no --validate): status is "ready"
    log_result = runner.invoke(cli, ["publish", "log"], env=env)
    assert log_result.exit_code == 0
    assert "RUN ID\tSTATUS\tGENERATED AT\tSTAGE DIR" in log_result.output
    assert "kf-20260416-012\tready" in log_result.output

    # With --validate: status is "valid"
    log_validate_result = runner.invoke(cli, ["publish", "log", "--validate"], env=env)
    assert log_validate_result.exit_code == 0
    assert "kf-20260416-012\tvalid" in log_validate_result.output

    inspect_result = runner.invoke(cli, ["publish", "inspect", "kf-20260416-012"], env=env)
    assert inspect_result.exit_code == 0
    assert "Publish run: kf-20260416-012" in inspect_result.output
    assert "Source documents: honeywell-dc1000-service-manual-rev-3" in inspect_result.output
    assert "write\tsource-index/doc-1.md" in inspect_result.output


def test_create_publish_pr_dry_run_syncs_only_knowledge_subtree(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stage_dir = data_dir / "publish" / "kf-20260416-006"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    repo_path = tmp_path / "FlowCommander"
    _init_git_repo(repo_path)

    (repo_path / "repo-wiki" / "notes.md").parent.mkdir(parents=True, exist_ok=True)
    (repo_path / "repo-wiki" / "notes.md").write_text("keep me", encoding="utf-8")
    (repo_path / "repo-wiki" / "knowledge" / "source-index").mkdir(parents=True, exist_ok=True)
    (repo_path / "repo-wiki" / "knowledge" / "source-index" / "old-doc.md").write_text("old", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=repo_path, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "commit", "-m", "Add initial wiki files"], cwd=repo_path, check=True, capture_output=True, text=True
    )

    _write_markdown(
        publish_root / "source-index" / "new-doc.md",
        _source_index_frontmatter(
            doc_id="new-doc",
            source_documents=_source_index_docs("new-doc"),
        ),
    )
    _write_manifest(
        stage_dir,
        "kf-20260416-006",
        files_written=["source-index/new-doc.md"],
        files_removed=["source-index/old-doc.md"],
    )
    prior_stage_dir = data_dir / "publish" / "kf-20260416-000"
    _write_manifest(prior_stage_dir, "kf-20260416-000", files_written=["source-index/old-doc.md"])

    result = create_publish_pr(
        "kf-20260416-006",
        "TNwkrk/FlowCommander",
        data_dir=data_dir,
        dry_run=True,
        target_repo_path=repo_path,
    )

    assert result == PRResult(
        pr_url=None,
        pr_number=None,
        branch="knowledge-forge/kf-20260416-006",
        files_added=["source-index/new-doc.md"],
        files_updated=[],
        files_removed=["source-index/old-doc.md"],
        dry_run=True,
        target_repo_path=repo_path,
    )
    assert (repo_path / "repo-wiki" / "notes.md").read_text(encoding="utf-8") == "keep me"
    assert (repo_path / "repo-wiki" / "knowledge" / "source-index" / "new-doc.md").exists()
    assert not (repo_path / "repo-wiki" / "knowledge" / "source-index" / "old-doc.md").exists()


def test_create_publish_pr_creates_commit_and_pr_with_labels(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stage_dir = data_dir / "publish" / "kf-20260416-007"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    repo_path = tmp_path / "FlowCommander"
    _init_git_repo(repo_path)

    _write_markdown(
        publish_root / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(stage_dir, "kf-20260416-007", files_written=["source-index/doc-1.md"])

    calls: list[tuple[str, object]] = []

    class StubGitHubApi:
        def create_pull_request(
            self,
            repo: str,
            *,
            title: str,
            body: str,
            head: str,
            base: str,
            draft: bool,
        ) -> tuple[int, str]:
            calls.append(("create_pull_request", repo, title, body, head, base, draft))
            return 42, "https://github.com/TNwkrk/FlowCommander/pull/42"

        def add_labels(self, repo: str, issue_number: int, labels: list[str]) -> None:
            calls.append(("add_labels", repo, issue_number, labels))

    result = create_publish_pr(
        "kf-20260416-007",
        "TNwkrk/FlowCommander",
        data_dir=data_dir,
        target_repo_path=repo_path,
        github_api=StubGitHubApi(),
    )

    assert result.pr_number == 42
    assert result.pr_url == "https://github.com/TNwkrk/FlowCommander/pull/42"
    assert result.branch == "knowledge-forge/kf-20260416-007"
    assert result.files_added == ["source-index/doc-1.md"]
    assert ("add_labels", "TNwkrk/FlowCommander", 42, ["knowledge-forge", "auto-generated"]) in calls
    create_call = next(call for call in calls if call[0] == "create_pull_request")
    assert create_call[1] == "TNwkrk/FlowCommander"
    assert create_call[2] == "[Knowledge Forge] Publish honeywell dc1000 family"
    assert "## Source documents" in create_call[3]
    assert create_call[4] == "knowledge-forge/kf-20260416-007"
    assert create_call[5] == "main"
    assert create_call[6] is True


def test_publish_pr_cli_reports_dry_run(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stage_dir = data_dir / "publish" / "kf-20260416-008"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    repo_path = tmp_path / "FlowCommander"
    _init_git_repo(repo_path)

    _write_markdown(
        publish_root / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(stage_dir, "kf-20260416-008", files_written=["source-index/doc-1.md"])

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    result = runner.invoke(
        cli,
        [
            "publish",
            "pr",
            "kf-20260416-008",
            "--dry-run",
            "--target-repo-path",
            str(repo_path),
        ],
        env=env,
    )

    assert result.exit_code == 0
    assert "Dry run: yes" in result.output
    assert "Branch: knowledge-forge/kf-20260416-008" in result.output


def test_stage_publish_raises_on_duplicate_run_id(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source_page = _compiled_page(
        data_dir / "compiled" / "source-pages" / "doc-1.md",
        doc_id="doc-1",
        frontmatter=_frontmatter(doc_id="doc-1"),
    )
    stage_publish("kf-dup-001", [source_page], data_dir=data_dir)
    with pytest.raises(FileExistsError, match="already exists and is non-empty"):
        stage_publish("kf-dup-001", [source_page], data_dir=data_dir)


def test_validate_rejects_manifest_paths_with_dotdot_segments(tmp_path: Path) -> None:
    stage_dir = tmp_path / "data" / "publish" / "kf-dotdot-001"
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    _write_markdown(
        publish_root / "source-index" / "doc-1.md",
        _source_index_frontmatter(
            doc_id="doc-1",
            source_documents=_source_index_docs("doc-1"),
        ),
    )
    _write_manifest(
        stage_dir,
        "kf-dotdot-001",
        files_written=["../outside-knowledge/evil.md"],
    )

    report = validate_publish_output(stage_dir)

    assert report.valid is False
    assert any("must not contain .. segments" in error for error in report.errors)


def test_load_frontmatter_accepts_crlf_newlines(tmp_path: Path) -> None:
    from knowledge_forge.publish.validate import _load_frontmatter

    md_file = tmp_path / "page.md"
    crlf_content = "---\r\ntitle: CRLF Page\r\ngenerated_by: knowledge-forge\r\n---\r\n\r\n# Content\r\n"
    md_file.write_bytes(crlf_content.encode("utf-8"))
    frontmatter, errors = _load_frontmatter(md_file)
    assert errors == []
    assert frontmatter is not None
    assert frontmatter["title"] == "CRLF Page"
