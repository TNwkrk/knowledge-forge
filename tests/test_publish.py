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
from knowledge_forge.publish import create_publish_pr, stage_publish, validate_publish_output
from knowledge_forge.publish.pr import PRResult


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
        _frontmatter(
            doc_id="new-doc",
            source_documents=[{"doc_id": "new-doc", "manufacturer": "Honeywell", "family": "DC1000"}],
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
        _frontmatter(
            doc_id="doc-1",
            source_documents=[{"doc_id": "doc-1", "manufacturer": "Honeywell", "family": "DC1000"}],
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
        _frontmatter(
            doc_id="doc-1",
            source_documents=[{"doc_id": "doc-1", "manufacturer": "Honeywell", "family": "DC1000"}],
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
        _frontmatter(doc_id="doc-1", source_documents=[{"doc_id": "doc-1"}]),
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
