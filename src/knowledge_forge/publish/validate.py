"""Publish contract validation for FlowCommander handoff."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict, Field
from yaml import YAMLError, safe_load

from knowledge_forge.intake.manifest import slugify
from knowledge_forge.publish.manifest import PublishManifest

ALLOWED_TARGET_DIRECTORIES = frozenset(
    {
        "analysis",
        "manufacturers",
        "procedures",
        "specs",
        "troubleshooting",
        "workflow-guidance",
        "parts",
        "safety",
        "source-index",
        "_manifests",
        "_sources",
        "_publish-log",
    }
)
REQUIRED_FRONTMATTER_FIELDS = frozenset(
    {
        "title",
        "generated_by",
        "publish_run",
        "source_documents",
        "generated_at",
    }
)


class ValidationReport(BaseModel):
    """Validation result for one staged publish directory."""

    model_config = ConfigDict(extra="forbid")

    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


def validate_publish_output(publish_dir: str | Path) -> ValidationReport:
    """Validate one staged publish directory against the documented contract."""
    stage_dir = Path(publish_dir)
    publish_root = stage_dir / "repo-wiki" / "knowledge"
    errors: list[str] = []
    warnings: list[str] = []

    if not stage_dir.exists():
        return ValidationReport(valid=False, errors=[f"publish directory does not exist: {stage_dir}"])
    if not publish_root.exists():
        return ValidationReport(valid=False, errors=[f"missing publish subtree: {publish_root}"])

    all_files = sorted(path for path in stage_dir.rglob("*") if path.is_file())
    markdown_files: list[Path] = []
    canonical_identities: dict[str, str] = {}

    for path in all_files:
        try:
            relative_to_publish = path.relative_to(publish_root)
        except ValueError:
            errors.append(f"file exists outside allowed subtree: {path.relative_to(stage_dir).as_posix()}")
            continue

        if not relative_to_publish.parts:
            continue
        root_dir = relative_to_publish.parts[0]
        if root_dir not in ALLOWED_TARGET_DIRECTORIES:
            errors.append(f"file exists in unrecognized target directory: {relative_to_publish.as_posix()}")
            continue
        if path.suffix == ".md":
            markdown_files.append(path)

    for markdown_path in markdown_files:
        relative_to_publish = markdown_path.relative_to(publish_root)
        frontmatter, parse_errors = _load_frontmatter(markdown_path)
        errors.extend(f"{relative_to_publish.as_posix()}: {message}" for message in parse_errors)
        if parse_errors:
            continue
        assert frontmatter is not None

        source_documents = _source_documents(frontmatter)
        errors.extend(_validate_common_frontmatter(relative_to_publish, frontmatter))
        errors.extend(_validate_slug_conventions(relative_to_publish, frontmatter))

        if not source_documents:
            errors.append(f"{relative_to_publish.as_posix()}: source_documents must contain at least one entry")
            continue

        identity = _canonical_identity(relative_to_publish, frontmatter)
        prior_path = canonical_identities.get(identity)
        if prior_path is not None:
            errors.append(
                f"duplicate canonical identity '{identity}' claimed by "
                f"{prior_path} and {relative_to_publish.as_posix()}"
            )
        else:
            canonical_identities[identity] = relative_to_publish.as_posix()

    manifest_errors, manifest_warnings = _validate_publish_manifests(publish_root, stage_dir.parent)
    errors.extend(manifest_errors)
    warnings.extend(manifest_warnings)
    return ValidationReport(valid=not errors, errors=errors, warnings=warnings)


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


def _load_frontmatter(path: Path) -> tuple[dict[str, object] | None, list[str]]:
    payload = path.read_text(encoding="utf-8")
    lines = payload.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != "---":
        return None, ["missing YAML frontmatter"]

    closing_index = next(
        (index for index, line in enumerate(lines[1:], start=1) if line.rstrip("\r\n") == "---"),
        None,
    )
    if closing_index is None:
        return None, ["unterminated YAML frontmatter block"]

    raw_frontmatter = "".join(lines[1:closing_index])
    try:
        parsed = safe_load(raw_frontmatter)
    except YAMLError as exc:
        return None, [f"invalid YAML frontmatter ({exc})"]
    if not isinstance(parsed, dict):
        return None, ["frontmatter is not a mapping"]
    return parsed, []


def _validate_common_frontmatter(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_FRONTMATTER_FIELDS if field not in frontmatter)
    if missing:
        errors.append(f"{relative_path.as_posix()}: missing required frontmatter fields {missing}")
    generated_by = frontmatter.get("generated_by")
    if generated_by != "knowledge-forge":
        errors.append(f"{relative_path.as_posix()}: generated_by must be 'knowledge-forge'")
    source_documents = frontmatter.get("source_documents")
    if not isinstance(source_documents, list):
        errors.append(f"{relative_path.as_posix()}: source_documents must be a list")
    generated_at = frontmatter.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at.endswith("Z"):
        errors.append(f"{relative_path.as_posix()}: generated_at must be an ISO-8601 UTC timestamp")
    publish_run = frontmatter.get("publish_run")
    if not isinstance(publish_run, str) or not publish_run.strip():
        errors.append(f"{relative_path.as_posix()}: publish_run must be a non-empty string")
    return errors


def _validate_slug_conventions(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    root_dir = relative_path.parts[0]
    if root_dir == "source-index":
        doc_id = frontmatter.get("doc_id")
        if not isinstance(doc_id, str) or relative_path.stem != doc_id:
            errors.append(f"{relative_path.as_posix()}: source-index filename must match frontmatter doc_id")
        return errors

    if root_dir == "manufacturers":
        source_documents = _source_documents(frontmatter)
        if not source_documents:
            return errors
        manufacturer_slug = slugify(source_documents[0].get("manufacturer", ""))
        if len(relative_path.parts) >= 2 and relative_path.parts[1] != manufacturer_slug:
            errors.append(
                f"{relative_path.as_posix()}: manufacturer path slug must match source_documents manufacturer"
            )
        if len(relative_path.parts) >= 3 and relative_path.parts[2] != "_index.md":
            family_value = frontmatter.get("family")
            if not isinstance(family_value, str) or not family_value.strip():
                family_value = source_documents[0].get("family", "")
            family_slug = slugify(family_value)
            if relative_path.parts[2] != family_slug:
                errors.append(f"{relative_path.as_posix()}: family path slug must match source_documents family")
        return errors

    bucket_id = frontmatter.get("bucket_id")
    if isinstance(bucket_id, str) and bucket_id:
        expected_prefix = slugify(bucket_id)
        if not relative_path.stem.startswith(expected_prefix):
            errors.append(f"{relative_path.as_posix()}: filename must start with bucket slug '{expected_prefix}'")
    return errors


def _canonical_identity(relative_path: Path, frontmatter: dict[str, object]) -> str:
    explicit_identity = frontmatter.get("canonical_identity")
    if isinstance(explicit_identity, str) and explicit_identity:
        return explicit_identity

    root_dir = relative_path.parts[0]
    if root_dir == "source-index":
        return f"source:{frontmatter.get('doc_id', relative_path.stem)}"
    bucket_id = frontmatter.get("bucket_id")
    topic = frontmatter.get("topic")
    if isinstance(bucket_id, str) and isinstance(topic, str):
        return f"{root_dir}:{bucket_id}:{topic}"
    return f"{root_dir}:{relative_path.with_suffix('').as_posix()}"


def _validate_publish_manifests(publish_root: Path, publish_history_root: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    manifest_dir = publish_root / "_manifests"
    manifest_paths = sorted(manifest_dir.glob("*.json"))
    if not manifest_paths:
        warnings.append("no publish manifest found under _manifests/")
        return errors, warnings

    prior_written_files = _prior_written_files(publish_history_root, exclude_publish_root=publish_root)
    for manifest_path in manifest_paths:
        try:
            manifest = PublishManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:  # pragma: no cover - defensive parsing wrapper
            errors.append(f"{manifest_path.relative_to(publish_root).as_posix()}: invalid publish manifest ({exc})")
            continue

        for relative_file in manifest.files_written + manifest.files_updated + manifest.files_removed:
            if relative_file.startswith("/"):
                errors.append(
                    f"{manifest_path.relative_to(publish_root).as_posix()}: manifest file path must be relative"
                )
                continue
            if ".." in PurePosixPath(relative_file).parts:
                errors.append(
                    f"{manifest_path.relative_to(publish_root).as_posix()}: manifest path must not contain .. "
                    f"segments ({relative_file})"
                )
                continue
            target = publish_root / relative_file
            try:
                target.relative_to(publish_root)
            except ValueError:
                errors.append(
                    f"{manifest_path.relative_to(publish_root).as_posix()}: manifest path escapes publish subtree "
                    f"({relative_file})"
                )

        for removed_file in manifest.files_removed:
            if removed_file not in prior_written_files:
                errors.append(
                    f"{manifest_path.relative_to(publish_root).as_posix()}: orphan removal not previously published "
                    f"by Knowledge Forge ({removed_file})"
                )
    return errors, warnings


def _prior_written_files(publish_history_root: Path, *, exclude_publish_root: Path) -> set[str]:
    prior_written_files: set[str] = set()
    for manifest_path in publish_history_root.glob("*/repo-wiki/knowledge/_manifests/*.json"):
        if manifest_path.parent.parent == exclude_publish_root:
            continue
        try:
            manifest = PublishManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        prior_written_files.update(manifest.files_written)
        prior_written_files.update(manifest.files_updated)
    return prior_written_files
