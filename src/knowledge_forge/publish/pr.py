"""Git and GitHub helpers for FlowCommander publish pull requests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Protocol
from urllib import request

from pydantic import BaseModel, ConfigDict

from knowledge_forge.publish.stage import PublishManifest
from knowledge_forge.publish.validate import ValidationReport, validate_publish_output

DEFAULT_TARGET_REPO = "TNwkrk/FlowCommander"
DEFAULT_LOCAL_TARGET_PATH = Path("/Users/taylor/development/FlowCommander")
DEFAULT_LABELS = ("knowledge-forge", "auto-generated")


class PRResult(BaseModel):
    """Summary of a publish PR workflow run."""

    model_config = ConfigDict(extra="forbid")

    pr_url: str | None
    pr_number: int | None
    branch: str
    files_added: list[str]
    files_updated: list[str]
    files_removed: list[str]
    dry_run: bool = False
    target_repo_path: Path


class _GitRunner(Protocol):
    def __call__(self, repo_path: Path, *args: str) -> str: ...


class GitHubApi:
    """Minimal GitHub REST client for pull-request creation."""

    def __init__(self, token: str) -> None:
        self._token = token

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
        payload = self._request_json(
            "POST",
            f"https://api.github.com/repos/{repo}/pulls",
            {
                "title": title,
                "body": body,
                "head": head,
                "base": base,
                "draft": draft,
            },
        )
        return int(payload["number"]), str(payload["html_url"])

    def add_labels(self, repo: str, issue_number: int, labels: list[str]) -> None:
        self._request_json(
            "POST",
            f"https://api.github.com/repos/{repo}/issues/{issue_number}/labels",
            {"labels": labels},
        )

    def _request_json(self, method: str, url: str, payload: dict[str, object]) -> dict[str, object]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url,
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "knowledge-forge",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with request.urlopen(req) as response:
            return json.loads(response.read().decode("utf-8"))


def create_publish_pr(
    publish_run_id: str,
    target_repo: str,
    *,
    data_dir: Path | None = None,
    dry_run: bool = False,
    target_repo_path: Path | None = None,
    github_token: str | None = None,
    git_runner: _GitRunner | None = None,
    github_api: GitHubApi | None = None,
) -> PRResult:
    """Create a FlowCommander publish PR from one staged publish run."""
    if not publish_run_id.strip():
        raise ValueError("publish_run_id must not be blank")
    if not target_repo.strip():
        raise ValueError("target_repo must not be blank")

    stage_dir = _resolve_data_dir(data_dir) / "publish" / publish_run_id
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    report = validate_publish_output(stage_dir)
    if not report.valid:
        raise ValueError(f"publish validation failed for {publish_run_id}: {'; '.join(report.errors)}")

    manifest = _load_publish_manifest(publish_root, publish_run_id)
    run_git = git_runner or _run_git
    repo_path = _prepare_target_repo(
        publish_run_id,
        target_repo,
        stage_dir=stage_dir,
        target_repo_path=target_repo_path,
        git_runner=run_git,
    )
    base_branch = _default_branch(repo_path, run_git)
    branch = f"knowledge-forge/{publish_run_id}"
    run_git(repo_path, "checkout", "-B", branch, f"origin/{base_branch}")

    files_added, files_updated, files_removed = _sync_publish_tree(
        publish_root,
        repo_path / "repo-wiki" / "knowledge",
        manifest,
    )
    run_git(repo_path, "add", "-A", "--", "repo-wiki/knowledge")

    if dry_run:
        return PRResult(
            pr_url=None,
            pr_number=None,
            branch=branch,
            files_added=files_added,
            files_updated=files_updated,
            files_removed=files_removed,
            dry_run=True,
            target_repo_path=repo_path,
        )

    if not files_added and not files_updated and not files_removed:
        return PRResult(
            pr_url=None,
            pr_number=None,
            branch=branch,
            files_added=[],
            files_updated=[],
            files_removed=[],
            dry_run=False,
            target_repo_path=repo_path,
        )

    run_git(
        repo_path,
        "commit",
        "-m",
        f"Publish Knowledge Forge run {publish_run_id}",
    )
    run_git(repo_path, "push", "--set-upstream", "origin", branch)

    token = github_token or os.getenv("GITHUB_TOKEN")
    if not token and github_api is None:
        raise ValueError("GITHUB_TOKEN must be set to create a publish PR")

    api = github_api or GitHubApi(token=token or "")
    pr_number, pr_url = api.create_pull_request(
        target_repo,
        title=f"[Knowledge Forge] Publish {_bucket_description(manifest)}",
        body=_build_pr_body(manifest, report),
        head=branch,
        base=base_branch,
        draft=True,
    )
    api.add_labels(target_repo, pr_number, list(DEFAULT_LABELS))

    return PRResult(
        pr_url=pr_url,
        pr_number=pr_number,
        branch=branch,
        files_added=files_added,
        files_updated=files_updated,
        files_removed=files_removed,
        dry_run=False,
        target_repo_path=repo_path,
    )


def _resolve_data_dir(data_dir: Path | None) -> Path:
    if data_dir is not None:
        return data_dir
    override = os.getenv("KNOWLEDGE_FORGE_DATA_DIR")
    return Path(override).expanduser().resolve() if override else Path("data").resolve()


def _load_publish_manifest(publish_root: Path, publish_run_id: str) -> PublishManifest:
    manifest_path = publish_root / "_manifests" / f"{publish_run_id}.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"publish manifest not found for {publish_run_id}: {manifest_path}")
    return PublishManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))


def _prepare_target_repo(
    publish_run_id: str,
    target_repo: str,
    *,
    stage_dir: Path,
    target_repo_path: Path | None,
    git_runner: _GitRunner,
) -> Path:
    repo_path = _resolve_target_repo_path(
        publish_run_id,
        target_repo,
        stage_dir=stage_dir,
        explicit_path=target_repo_path,
    )
    if repo_path.exists():
        git_runner(repo_path, "fetch", "origin")
        return repo_path

    repo_path.parent.mkdir(parents=True, exist_ok=True)
    git_runner(repo_path.parent, "clone", f"git@github.com:{target_repo}.git", str(repo_path))
    return repo_path


def _resolve_target_repo_path(
    publish_run_id: str,
    target_repo: str,
    *,
    stage_dir: Path,
    explicit_path: Path | None,
) -> Path:
    if explicit_path is not None:
        return explicit_path.resolve()

    env_path = os.getenv("KNOWLEDGE_FORGE_PUBLISH_REPO_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    if target_repo == DEFAULT_TARGET_REPO and DEFAULT_LOCAL_TARGET_PATH.exists():
        return DEFAULT_LOCAL_TARGET_PATH.resolve()

    slug = target_repo.replace("/", "-")
    return (stage_dir / "_work" / slug).resolve()


def _default_branch(repo_path: Path, git_runner: _GitRunner) -> str:
    head_ref = git_runner(repo_path, "symbolic-ref", "--short", "refs/remotes/origin/HEAD").strip()
    if not head_ref:
        return "main"
    return head_ref.split("/", 1)[1]


def _sync_publish_tree(
    publish_root: Path,
    target_root: Path,
    manifest: PublishManifest,
) -> tuple[list[str], list[str], list[str]]:
    files_added: list[str] = []
    files_updated: list[str] = []
    files_removed: list[str] = []

    for relative_path in sorted(set(manifest.files_written + manifest.files_updated)):
        source = publish_root / relative_path
        destination = target_root / relative_path
        if not source.exists():
            raise FileNotFoundError(f"staged publish file is missing: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            files_added.append(relative_path)
            shutil.copy2(source, destination)
            continue
        if source.read_bytes() != destination.read_bytes():
            files_updated.append(relative_path)
            shutil.copy2(source, destination)

    for relative_path in manifest.files_removed:
        destination = target_root / relative_path
        if not destination.exists():
            continue
        destination.unlink()
        _prune_empty_parents(destination.parent, target_root)
        files_removed.append(relative_path)

    return files_added, files_updated, files_removed


def _prune_empty_parents(path: Path, stop_at: Path) -> None:
    current = path
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _bucket_description(manifest: PublishManifest) -> str:
    if manifest.buckets:
        return ", ".join(bucket.replace("/", " ") for bucket in manifest.buckets)
    if len(manifest.source_documents) == 1:
        return manifest.source_documents[0]
    return f"{len(manifest.source_documents)} source documents"


def _build_pr_body(manifest: PublishManifest, report: ValidationReport) -> str:
    files_added = len(manifest.files_written)
    files_updated = len(manifest.files_updated)
    files_removed = len(manifest.files_removed)
    source_lines = "\n".join(f"- `{doc_id}`" for doc_id in manifest.source_documents)
    warning_lines = "\n".join(f"- {warning}" for warning in report.warnings) or "- None"
    bucket_lines = "\n".join(f"- `{bucket}`" for bucket in manifest.buckets) or "- None"
    return (
        "## Summary\n"
        "This PR was generated by Knowledge Forge and proposes staged wiki output for FlowCommander review.\n\n"
        "## Buckets\n"
        f"{bucket_lines}\n\n"
        "## Source documents\n"
        f"{source_lines}\n\n"
        "## File counts\n"
        f"- Added: {files_added}\n"
        f"- Updated: {files_updated}\n"
        f"- Removed: {files_removed}\n\n"
        "## Manifest\n"
        f"- `repo-wiki/knowledge/_manifests/{manifest.publish_run_id}.json`\n\n"
        "## Warnings\n"
        f"{warning_lines}\n"
    )


def _run_git(repo_path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_path,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
