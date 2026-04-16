"""Publish manifest helpers and history inspection."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, ConfigDict, ValidationError

from knowledge_forge import __version__
from knowledge_forge.intake.importer import get_data_dir


class PublishManifest(BaseModel):
    """Persisted metadata for one staged publish run."""

    model_config = ConfigDict(extra="forbid")

    publish_run_id: str
    generated_at: str
    knowledge_forge_version: str
    source_documents: list[str]
    buckets: list[str]
    files_written: list[str]
    files_updated: list[str]
    files_removed: list[str]
    extraction_version: str
    compilation_version: str


class PublishRunSummary(BaseModel):
    """Summary row for one staged publish run."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    publish_run_id: str
    generated_at: str | None = None
    status: str
    stage_dir: Path
    manifest_path: Path | None = None


def generate_publish_manifest(
    publish_run_id: str,
    staged_files: Iterable[str],
    *,
    source_documents: Iterable[str],
    buckets: Iterable[str],
    files_updated: Iterable[str] = (),
    files_removed: Iterable[str] = (),
    extraction_version: str = "unknown",
    compilation_version: str = "unknown",
) -> dict[str, object]:
    """Build a publish manifest payload for one staged run."""
    manifest = PublishManifest(
        publish_run_id=publish_run_id,
        generated_at=_utc_timestamp(),
        knowledge_forge_version=__version__,
        source_documents=sorted({value for value in source_documents if value}),
        buckets=sorted({value for value in buckets if value}),
        files_written=sorted({value for value in staged_files if value}),
        files_updated=sorted({value for value in files_updated if value}),
        files_removed=sorted({value for value in files_removed if value}),
        extraction_version=extraction_version or "unknown",
        compilation_version=compilation_version or "unknown",
    )
    return manifest.model_dump(mode="json")


def load_publish_manifest(stage_dir: Path, publish_run_id: str) -> PublishManifest:
    """Load one publish manifest from a staged publish directory."""
    manifest_path = stage_dir / "repo-wiki" / "knowledge" / "_manifests" / f"{publish_run_id}.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"publish manifest not found for {publish_run_id}: {manifest_path}")
    return PublishManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))


def list_publish_runs(data_dir: Path | None = None, *, validate: bool = False) -> list[PublishRunSummary]:
    """List staged publish runs and their validation status.

    When *validate* is False (the default) only the manifest is read; status is
    ``"ready"`` when the manifest parses and the embedded ``publish_run_id``
    matches the directory name.  Pass ``validate=True`` to run the full
    :func:`~knowledge_forge.publish.validate.validate_publish_output` check and
    receive ``"valid"``/``"invalid"`` statuses instead.
    """
    resolved_data_dir = get_data_dir(data_dir)
    publish_root = resolved_data_dir / "publish"
    if not publish_root.exists():
        return []

    runs: list[PublishRunSummary] = []
    for stage_dir in sorted(path for path in publish_root.iterdir() if path.is_dir()):
        manifest_path = stage_dir / "repo-wiki" / "knowledge" / "_manifests" / f"{stage_dir.name}.json"
        if not manifest_path.exists():
            runs.append(
                PublishRunSummary(
                    publish_run_id=stage_dir.name,
                    status="missing-manifest",
                    stage_dir=stage_dir,
                )
            )
            continue

        try:
            manifest = PublishManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, ValidationError) as exc:
            runs.append(
                PublishRunSummary(
                    publish_run_id=stage_dir.name,
                    status=f"invalid-manifest ({type(exc).__name__})",
                    stage_dir=stage_dir,
                    manifest_path=manifest_path,
                )
            )
            continue

        if manifest.publish_run_id != stage_dir.name:
            runs.append(
                PublishRunSummary(
                    publish_run_id=stage_dir.name,
                    generated_at=manifest.generated_at,
                    status="id-mismatch",
                    stage_dir=stage_dir,
                    manifest_path=manifest_path,
                )
            )
            continue

        if validate:
            from knowledge_forge.publish.validate import validate_publish_output

            report = validate_publish_output(stage_dir)
            status = "valid" if report.valid else "invalid"
        else:
            status = "ready"

        runs.append(
            PublishRunSummary(
                publish_run_id=stage_dir.name,
                generated_at=manifest.generated_at,
                status=status,
                stage_dir=stage_dir,
                manifest_path=manifest_path,
            )
        )

    runs.sort(key=lambda run: (run.generated_at or "", run.publish_run_id), reverse=True)
    return runs


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
