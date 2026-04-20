"""Publish contract validation for FlowCommander handoff."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict, Field
from yaml import YAMLError, safe_load

from knowledge_forge.intake.manifest import slugify
from knowledge_forge.publish.manifest import PublishManifest

ALLOWED_TARGET_DIRECTORIES = frozenset(
    {
        "controllers",
        "fault-codes",
        "symptoms",
        "workflow-guidance",
        "contradictions",
        "supersessions",
        "source-index",
        "_manifests",
        "_sources",
        "_publish-log",
    }
)
DIGEST_DIRECTORY_TO_TYPE = {
    "controllers": "controller",
    "fault-codes": "fault-code",
    "symptoms": "symptom",
    "workflow-guidance": "workflow-guidance",
    "contradictions": "contradiction",
    "supersessions": "supersession",
}
REQUIRED_DIGEST_FRONTMATTER_FIELDS = frozenset(
    {
        "title",
        "digest_type",
        "slug",
        "status",
        "source_documents",
        "knowledge_record_ids",
        "tags",
        "cross_links",
        "generated_by",
        "publish_run",
        "generated_at",
        "extraction_version",
        "compilation_version",
    }
)
REQUIRED_SOURCE_INDEX_FRONTMATTER_FIELDS = frozenset(
    {
        "title",
        "doc_id",
        "generated_by",
        "publish_run",
        "generated_at",
        "source_documents",
    }
)
PAGE_TYPE_REQUIRED_FIELDS = {
    "controller": frozenset({"controller_models", "system_types"}),
    "fault-code": frozenset({"fault_code", "controller_models"}),
    "symptom": frozenset({"symptom_key", "system_types"}),
    "workflow-guidance": frozenset({"workflow_key"}),
    "contradiction": frozenset({"contradiction_key", "conflicting_pages", "resolution_status"}),
    "supersession": frozenset({"superseded_slug", "replacement_slug", "reason"}),
}
REQUIRED_DIGEST_SECTIONS = (
    "## Summary",
    "## Field Guidance",
    "## Source Citations",
    "## Related Pages",
)
ALLOWED_STATUS_VALUES = frozenset({"draft", "approved", "superseded"})


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

        errors.extend(_validate_markdown_page(relative_to_publish, markdown_path, frontmatter))

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


def _validate_markdown_page(relative_path: Path, markdown_path: Path, frontmatter: dict[str, object]) -> list[str]:
    root_dir = relative_path.parts[0]
    if root_dir == "source-index":
        return _validate_source_index_page(relative_path, frontmatter)
    return _validate_digest_page(relative_path, markdown_path, frontmatter)


def _validate_digest_page(relative_path: Path, markdown_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_DIGEST_FRONTMATTER_FIELDS if field not in frontmatter)
    if missing:
        errors.append(f"{relative_path.as_posix()}: missing required frontmatter fields {missing}")

    expected_digest_type = DIGEST_DIRECTORY_TO_TYPE.get(relative_path.parts[0])
    digest_type = frontmatter.get("digest_type")
    if digest_type != expected_digest_type:
        errors.append(
            f"{relative_path.as_posix()}: digest_type must be '{expected_digest_type}' "
            f"for directory '{relative_path.parts[0]}'"
        )

    errors.extend(_validate_common_generated_fields(relative_path, frontmatter))
    errors.extend(_validate_slug_and_filename(relative_path, frontmatter))
    errors.extend(_validate_status(relative_path, frontmatter))
    errors.extend(_validate_digest_source_documents(relative_path, frontmatter))
    errors.extend(_validate_string_list_field(relative_path, frontmatter, "knowledge_record_ids"))
    errors.extend(_validate_string_list_field(relative_path, frontmatter, "tags"))
    errors.extend(_validate_string_list_field(relative_path, frontmatter, "cross_links"))

    required_page_type_fields = PAGE_TYPE_REQUIRED_FIELDS.get(str(digest_type), frozenset())
    missing_page_type_fields = sorted(field for field in required_page_type_fields if field not in frontmatter)
    if missing_page_type_fields:
        errors.append(f"{relative_path.as_posix()}: missing page-type frontmatter fields {missing_page_type_fields}")

    errors.extend(_validate_required_sections(relative_path, markdown_path))
    return errors


def _validate_source_index_page(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_SOURCE_INDEX_FRONTMATTER_FIELDS if field not in frontmatter)
    if missing:
        errors.append(f"{relative_path.as_posix()}: missing required frontmatter fields {missing}")

    errors.extend(_validate_common_generated_fields(relative_path, frontmatter))

    doc_id = frontmatter.get("doc_id")
    if not isinstance(doc_id, str) or relative_path.stem != doc_id:
        errors.append(f"{relative_path.as_posix()}: source-index filename must match frontmatter doc_id")

    source_documents = _source_documents(frontmatter)
    if not source_documents:
        errors.append(f"{relative_path.as_posix()}: source_documents must contain at least one entry")
        return errors

    matching_entry = next((entry for entry in source_documents if entry.get("doc_id") == doc_id), None)
    if matching_entry is None:
        errors.append(f"{relative_path.as_posix()}: source_documents must include an entry matching frontmatter doc_id")
        return errors
    for field in ("revision", "manufacturer", "family"):
        value = matching_entry.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{relative_path.as_posix()}: source-index source_documents entry missing '{field}'")
    return errors


def _validate_common_generated_fields(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    generated_by = frontmatter.get("generated_by")
    if generated_by != "knowledge-forge":
        errors.append(f"{relative_path.as_posix()}: generated_by must be 'knowledge-forge'")
    generated_at = frontmatter.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at.endswith("Z"):
        errors.append(f"{relative_path.as_posix()}: generated_at must be an ISO-8601 UTC timestamp")
    publish_run = frontmatter.get("publish_run")
    if not isinstance(publish_run, str) or not publish_run.strip():
        errors.append(f"{relative_path.as_posix()}: publish_run must be a non-empty string")
    return errors


def _validate_slug_and_filename(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    slug = frontmatter.get("slug")
    if not isinstance(slug, str) or not slug.strip():
        errors.append(f"{relative_path.as_posix()}: slug must be a non-empty string")
        return errors
    if slug != slugify(slug):
        errors.append(f"{relative_path.as_posix()}: slug must be lowercase hyphen-separated")
    if relative_path.stem != slug:
        errors.append(f"{relative_path.as_posix()}: filename must match frontmatter slug")
    bucket_id = frontmatter.get("bucket_id")
    if isinstance(bucket_id, str) and bucket_id.strip():
        expected_slug = _expected_bucket_slug(bucket_id=bucket_id, digest_type=frontmatter.get("digest_type"))
        if expected_slug is not None and slug != expected_slug:
            errors.append(
                f"{relative_path.as_posix()}: slug must match bucket-derived pattern '{expected_slug}' "
                f"for digest_type '{frontmatter.get('digest_type')}'"
            )
        elif frontmatter.get("digest_type") == "workflow-guidance" and not slug.startswith(f"{slugify(bucket_id)}-"):
            errors.append(
                f"{relative_path.as_posix()}: workflow-guidance slug must start with "
                f"'{slugify(bucket_id)}-' when bucket_id is present"
            )
    return errors


def _validate_status(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    status = frontmatter.get("status")
    if status not in ALLOWED_STATUS_VALUES:
        return [f"{relative_path.as_posix()}: status must be one of {sorted(ALLOWED_STATUS_VALUES)}"]
    return []


def _validate_digest_source_documents(relative_path: Path, frontmatter: dict[str, object]) -> list[str]:
    errors: list[str] = []
    source_documents = _source_documents(frontmatter)
    if not source_documents:
        errors.append(f"{relative_path.as_posix()}: source_documents must contain at least one entry")
        return errors

    for index, entry in enumerate(source_documents):
        title = entry.get("title")
        locator = entry.get("locator")
        if not isinstance(title, str) or not title.strip():
            errors.append(f"{relative_path.as_posix()}: source_documents[{index}] missing title")
        if not isinstance(locator, str) or not locator.strip():
            errors.append(f"{relative_path.as_posix()}: source_documents[{index}] missing locator")
        if "attachment_id" not in entry:
            errors.append(f"{relative_path.as_posix()}: source_documents[{index}] missing attachment_id key")
    return errors


def _validate_string_list_field(relative_path: Path, frontmatter: dict[str, object], field_name: str) -> list[str]:
    payload = frontmatter.get(field_name)
    if not isinstance(payload, list):
        return [f"{relative_path.as_posix()}: {field_name} must be a list"]
    errors: list[str] = []
    for index, value in enumerate(payload):
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{relative_path.as_posix()}: {field_name}[{index}] must be a non-empty string")
    return errors


def _expected_bucket_slug(*, bucket_id: str, digest_type: object) -> str | None:
    bucket_slug = slugify(bucket_id)
    if digest_type == "controller":
        return f"{bucket_slug}-controller-digest"
    if digest_type == "fault-code":
        return f"{bucket_slug}-alarm-reference"
    if digest_type == "symptom":
        return f"{bucket_slug}-troubleshooting"
    if digest_type == "contradiction":
        return bucket_slug
    return None


def _validate_required_sections(relative_path: Path, markdown_path: Path) -> list[str]:
    payload = markdown_path.read_text(encoding="utf-8")
    body = _markdown_body(payload)
    positions: list[int] = []
    errors: list[str] = []
    for heading in REQUIRED_DIGEST_SECTIONS:
        marker = f"\n{heading}\n"
        position = body.find(marker)
        if position == -1 and body.startswith(f"{heading}\n"):
            position = 0
        if position == -1:
            errors.append(f"{relative_path.as_posix()}: missing required section '{heading}'")
            continue
        positions.append(position)
    if len(positions) == len(REQUIRED_DIGEST_SECTIONS) and positions != sorted(positions):
        errors.append(
            f"{relative_path.as_posix()}: required sections must appear in order {list(REQUIRED_DIGEST_SECTIONS)}"
        )
    return errors


def _markdown_body(payload: str) -> str:
    lines = payload.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != "---":
        return payload
    closing_index = next(
        (index for index, line in enumerate(lines[1:], start=1) if line.rstrip("\r\n") == "---"),
        None,
    )
    if closing_index is None:
        return payload
    return "".join(lines[closing_index + 1 :]).lstrip("\r\n")


def _source_documents(frontmatter: dict[str, object]) -> list[dict[str, object]]:
    payload = frontmatter.get("source_documents")
    if not isinstance(payload, list):
        return []
    documents: list[dict[str, object]] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        documents.append({str(key): value for key, value in entry.items()})
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


def _canonical_identity(relative_path: Path, frontmatter: dict[str, object]) -> str:
    root_dir = relative_path.parts[0]
    if root_dir == "source-index":
        return f"source:{frontmatter.get('doc_id', relative_path.stem)}"
    digest_type = frontmatter.get("digest_type", DIGEST_DIRECTORY_TO_TYPE.get(root_dir, root_dir))
    slug = frontmatter.get("slug", relative_path.stem)
    return f"digest:{digest_type}:{slug}"


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
                continue
            if PurePosixPath(relative_file).parts[0] not in ALLOWED_TARGET_DIRECTORIES:
                errors.append(
                    f"{manifest_path.relative_to(publish_root).as_posix()}: manifest path targets unrecognized "
                    f"directory ({relative_file})"
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
