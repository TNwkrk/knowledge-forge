"""Durable extraction run planning, persistence, and resume helpers."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.extract.engine import (
    ExtractionFingerprint,
    ExtractionWorkItemResult,
    build_extraction_fingerprint,
    execute_work_item,
    load_prompt_template,
    load_section_quality,
    load_sections,
    record_types_for_section_type,
    utc_now,
)
from knowledge_forge.extract.provenance import load_bucket_context, load_parse_metadata
from knowledge_forge.inference import InferenceClient
from knowledge_forge.inference.config import InferenceConfig
from knowledge_forge.intake.importer import get_data_dir, load_manifest
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse.sectioning import Section

RUNS_DIRNAME = "extraction_runs"


class ExtractionRunItemStatus(str, Enum):
    """Allowed durable queue states for one extraction work item."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExtractionRunStatus(str, Enum):
    """Coarse status for the overall extraction run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_FAILURES = "completed_with_failures"


class ExtractionRunDocumentScope(BaseModel):
    """Requested extraction scope for one document inside a durable run."""

    model_config = ConfigDict(extra="forbid")

    doc_id: str = Field(min_length=1)
    section_ids: list[str] = Field(default_factory=list)
    full_document: bool = True


class ExtractionRunItem(BaseModel):
    """One durable extraction queue item."""

    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1)
    doc_id: str = Field(min_length=1)
    section_id: str = Field(min_length=1)
    section_type: str = Field(min_length=1)
    record_type: str = Field(min_length=1)
    fingerprint: ExtractionFingerprint
    status: ExtractionRunItemStatus = ExtractionRunItemStatus.PENDING
    attempt_count: int = Field(default=0, ge=0)
    record_ids: list[str] = Field(default_factory=list)
    last_error: str | None = None
    last_attempted_at: datetime | None = None
    completed_at: datetime | None = None


class ExtractionRun(BaseModel):
    """Persisted durable extraction-run artifact."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    status: ExtractionRunStatus = ExtractionRunStatus.PENDING
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    documents: list[ExtractionRunDocumentScope] = Field(min_length=1)
    item_count: int = Field(ge=0)
    min_confidence: float = Field(ge=0.0, le=1.0)
    max_repair_attempts: int = Field(ge=0)
    items: list[ExtractionRunItem] = Field(default_factory=list)


class ExtractionRunExecution(BaseModel):
    """Summary returned after running or resuming a durable extraction run."""

    model_config = ConfigDict(extra="forbid")

    run: ExtractionRun
    run_path: Path
    executed_item_ids: list[str] = Field(default_factory=list)
    records_emitted: int = Field(default=0, ge=0)


def start_extraction_run(
    doc_ids: list[str],
    *,
    config: InferenceConfig,
    data_dir: Path | None = None,
    section_ids: list[str] | None = None,
    min_confidence: float = 0.0,
    max_repair_attempts: int = 2,
) -> ExtractionRunExecution:
    """Create and execute a fresh durable extraction run."""
    resolved_data_dir = get_data_dir(data_dir)
    run = create_extraction_run(
        doc_ids,
        config=config,
        data_dir=resolved_data_dir,
        section_ids=section_ids,
        min_confidence=min_confidence,
        max_repair_attempts=max_repair_attempts,
    )
    return execute_extraction_run(run.run_id, config=config, data_dir=resolved_data_dir)


def create_extraction_run(
    doc_ids: list[str],
    *,
    config: InferenceConfig,
    data_dir: Path | None = None,
    section_ids: list[str] | None = None,
    min_confidence: float = 0.0,
    max_repair_attempts: int = 2,
) -> ExtractionRun:
    """Persist a new durable extraction-run artifact without executing it."""
    if not doc_ids:
        raise ValueError("at least one doc_id is required to start an extraction run")
    if section_ids and len(doc_ids) != 1:
        raise ValueError("section-targeted extraction runs currently support exactly one doc_id")

    resolved_data_dir = get_data_dir(data_dir)
    now = utc_now()
    run_id = _next_run_id(resolved_data_dir, today=now.date())
    documents = _build_document_scopes(doc_ids, section_ids=section_ids)
    client = InferenceClient(config, data_dir=resolved_data_dir)
    items = _build_run_items(documents, client=client, data_dir=resolved_data_dir)
    run = ExtractionRun(
        run_id=run_id,
        created_at=now,
        updated_at=now,
        documents=documents,
        item_count=len(items),
        min_confidence=min_confidence,
        max_repair_attempts=max_repair_attempts,
        items=items,
    )
    save_extraction_run(run, data_dir=resolved_data_dir)
    _sync_completed_document_statuses(run, data_dir=resolved_data_dir)
    return run


def execute_extraction_run(
    run_id: str,
    *,
    config: InferenceConfig,
    data_dir: Path | None = None,
    retry_failed_only: bool = False,
) -> ExtractionRunExecution:
    """Run pending items for a durable extraction run."""
    resolved_data_dir = get_data_dir(data_dir)
    run = load_extraction_run(run_id, data_dir=resolved_data_dir)
    client = InferenceClient(config, data_dir=resolved_data_dir)
    run = _prepare_run_for_execution(
        run,
        client=client,
        data_dir=resolved_data_dir,
        retry_failed_only=retry_failed_only,
    )

    now = utc_now()
    if run.started_at is None:
        run = run.model_copy(update={"started_at": now, "updated_at": now})
    run = run.model_copy(update={"status": ExtractionRunStatus.RUNNING, "updated_at": utc_now()})
    run_path = save_extraction_run(run, data_dir=resolved_data_dir)

    executed_item_ids: list[str] = []
    records_emitted = 0
    for index, item in enumerate(run.items):
        if item.status != ExtractionRunItemStatus.PENDING:
            continue

        section = _load_section(item.doc_id, item.section_id, data_dir=resolved_data_dir)
        item_attempted_at = utc_now()
        run.items[index] = item.model_copy(
            update={
                "status": ExtractionRunItemStatus.IN_PROGRESS,
                "attempt_count": item.attempt_count + 1,
                "last_attempted_at": item_attempted_at,
                "last_error": None,
            }
        )
        run = run.model_copy(update={"updated_at": item_attempted_at})
        run_path = save_extraction_run(run, data_dir=resolved_data_dir)

        parse_meta = load_parse_metadata(item.doc_id, data_dir=resolved_data_dir)
        bucket_context = load_bucket_context(item.doc_id, data_dir=resolved_data_dir)
        section_quality = load_section_quality(section, data_dir=resolved_data_dir)
        result = execute_work_item(
            section=section,
            record_type=item.record_type,
            client=client,
            data_dir=resolved_data_dir,
            max_repair_attempts=run.max_repair_attempts,
            section_quality=section_quality,
            min_confidence=run.min_confidence,
            parse_meta=parse_meta,
            bucket_context=bucket_context,
            pipeline_run_id=run.run_id,
        )
        executed_item_ids.append(item.item_id)
        records_emitted += len(result.records)
        run.items[index] = _update_item_from_result(run.items[index], result)
        run = run.model_copy(update={"updated_at": utc_now()})
        run_path = save_extraction_run(run, data_dir=resolved_data_dir)
        _sync_completed_document_statuses(run, data_dir=resolved_data_dir)

    final_status = _derive_run_status(run)
    final_now = utc_now()
    completed_at = final_now if final_status in _TERMINAL_RUN_STATUSES else None
    run = run.model_copy(
        update={
            "status": final_status,
            "updated_at": final_now,
            "completed_at": completed_at,
        }
    )
    run_path = save_extraction_run(run, data_dir=resolved_data_dir)
    _sync_completed_document_statuses(run, data_dir=resolved_data_dir)
    return ExtractionRunExecution(
        run=run,
        run_path=run_path,
        executed_item_ids=executed_item_ids,
        records_emitted=records_emitted,
    )


def resume_extraction_run(
    run_id: str,
    *,
    config: InferenceConfig,
    data_dir: Path | None = None,
) -> ExtractionRunExecution:
    """Resume interrupted or partial extraction work from a persisted run."""
    return execute_extraction_run(run_id, config=config, data_dir=data_dir, retry_failed_only=False)


def retry_failed_extraction_run(
    run_id: str,
    *,
    config: InferenceConfig,
    data_dir: Path | None = None,
) -> ExtractionRunExecution:
    """Retry only failed items from a persisted run."""
    return execute_extraction_run(run_id, config=config, data_dir=data_dir, retry_failed_only=True)


def load_extraction_run(run_id: str, *, data_dir: Path | None = None) -> ExtractionRun:
    """Load one persisted extraction-run artifact by id."""
    resolved_data_dir = get_data_dir(data_dir)
    run_path = _run_path(resolved_data_dir, run_id)
    if not run_path.exists():
        raise FileNotFoundError(f"extraction run not found: {run_id}")
    return ExtractionRun.model_validate_json(run_path.read_text(encoding="utf-8"))


def save_extraction_run(run: ExtractionRun, *, data_dir: Path | None = None) -> Path:
    """Persist a durable extraction-run artifact."""
    resolved_data_dir = get_data_dir(data_dir)
    run_dir = resolved_data_dir / RUNS_DIRNAME
    run_dir.mkdir(parents=True, exist_ok=True)
    run_path = _run_path(resolved_data_dir, run.run_id)
    run_path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
    return run_path


def summarize_run_status(run: ExtractionRun) -> dict[str, int]:
    """Return per-status counts for one run."""
    counts = Counter(item.status.value for item in run.items)
    return {status.value: counts.get(status.value, 0) for status in ExtractionRunItemStatus}


def _prepare_run_for_execution(
    run: ExtractionRun,
    *,
    client: InferenceClient,
    data_dir: Path,
    retry_failed_only: bool,
) -> ExtractionRun:
    updated_items: list[ExtractionRunItem] = []
    for item in run.items:
        current_fingerprint = _rebuild_item_fingerprint(item, client=client, data_dir=data_dir)
        item_update = item
        if current_fingerprint != item.fingerprint:
            item_update = item.model_copy(
                update={
                    "fingerprint": current_fingerprint,
                    "status": ExtractionRunItemStatus.PENDING,
                    "record_ids": [],
                    "last_error": None,
                    "completed_at": None,
                }
            )
        elif retry_failed_only:
            if item.status == ExtractionRunItemStatus.FAILED:
                item_update = item.model_copy(
                    update={
                        "status": ExtractionRunItemStatus.PENDING,
                        "last_error": None,
                        "completed_at": None,
                    }
                )
        elif item.status == ExtractionRunItemStatus.IN_PROGRESS:
            item_update = item.model_copy(update={"status": ExtractionRunItemStatus.PENDING, "last_error": None})
        updated_items.append(item_update)

    return run.model_copy(update={"items": updated_items, "updated_at": utc_now()})


def _build_document_scopes(doc_ids: list[str], *, section_ids: list[str] | None) -> list[ExtractionRunDocumentScope]:
    if section_ids:
        return [ExtractionRunDocumentScope(doc_id=doc_ids[0], section_ids=sorted(section_ids), full_document=False)]
    return [ExtractionRunDocumentScope(doc_id=doc_id, full_document=True) for doc_id in doc_ids]


def _build_run_items(
    documents: list[ExtractionRunDocumentScope],
    *,
    client: InferenceClient,
    data_dir: Path,
) -> list[ExtractionRunItem]:
    items: list[ExtractionRunItem] = []
    for document in documents:
        sections = load_sections(document.doc_id, data_dir=data_dir)
        if document.section_ids:
            requested = set(document.section_ids)
            sections = [section for section in sections if section.section_id in requested]
            if len(sections) != len(requested):
                missing = sorted(requested - {section.section_id for section in sections})
                raise FileNotFoundError(f"sections not found for doc_id '{document.doc_id}': {', '.join(missing)}")
        for section in sections:
            for record_type in record_types_for_section_type(section.section_type):
                template = load_prompt_template(record_type)
                model = template.model or client.config.extraction_model
                fingerprint = build_extraction_fingerprint(
                    section=section,
                    record_type=record_type,
                    model=model,
                    prompt_template=template,
                )
                items.append(
                    ExtractionRunItem(
                        item_id=f"{section.section_id}::{record_type}",
                        doc_id=document.doc_id,
                        section_id=section.section_id,
                        section_type=section.section_type,
                        record_type=record_type,
                        fingerprint=fingerprint,
                    )
                )
    return items


def _rebuild_item_fingerprint(
    item: ExtractionRunItem, *, client: InferenceClient, data_dir: Path
) -> ExtractionFingerprint:
    section = _load_section(item.doc_id, item.section_id, data_dir=data_dir)
    template = load_prompt_template(item.record_type)
    model = template.model or client.config.extraction_model
    return build_extraction_fingerprint(
        section=section,
        record_type=item.record_type,
        model=model,
        prompt_template=template,
    )


def _load_section(doc_id: str, section_id: str, *, data_dir: Path) -> Section:
    sections = {section.section_id: section for section in load_sections(doc_id, data_dir=data_dir)}
    try:
        return sections[section_id]
    except KeyError as exc:
        raise FileNotFoundError(f"section '{section_id}' not found for doc_id '{doc_id}'") from exc


def _update_item_from_result(item: ExtractionRunItem, result: ExtractionWorkItemResult) -> ExtractionRunItem:
    status = ExtractionRunItemStatus.SUCCEEDED if result.status == "succeeded" else ExtractionRunItemStatus.FAILED
    error = None if not result.errors else "; ".join(result.errors)
    return item.model_copy(
        update={
            "status": status,
            "fingerprint": result.fingerprint,
            "record_ids": result.record_ids,
            "last_error": error,
            "completed_at": utc_now(),
        }
    )


def _sync_completed_document_statuses(run: ExtractionRun, *, data_dir: Path) -> None:
    for document in run.documents:
        if not document.full_document:
            continue
        doc_items = [item for item in run.items if item.doc_id == document.doc_id]
        if doc_items and all(
            item.status in {ExtractionRunItemStatus.SUCCEEDED, ExtractionRunItemStatus.SKIPPED} for item in doc_items
        ):
            manifest_path = data_dir / "manifests" / f"{document.doc_id}.yaml"
            manifest = load_manifest(data_dir, document.doc_id)
            if manifest.document.status != DocumentStatus.EXTRACTED:
                updated = manifest.transition_status(
                    DocumentStatus.EXTRACTED, reason=f"extraction run {run.run_id} complete"
                )
                manifest_path.write_text(updated.to_yaml(), encoding="utf-8")


def _derive_run_status(run: ExtractionRun) -> ExtractionRunStatus:
    item_statuses = {item.status for item in run.items}
    if item_statuses <= {ExtractionRunItemStatus.SUCCEEDED, ExtractionRunItemStatus.SKIPPED}:
        return ExtractionRunStatus.COMPLETED
    if ExtractionRunItemStatus.FAILED in item_statuses:
        return ExtractionRunStatus.COMPLETED_WITH_FAILURES
    if ExtractionRunItemStatus.PENDING in item_statuses or ExtractionRunItemStatus.IN_PROGRESS in item_statuses:
        return ExtractionRunStatus.RUNNING
    return ExtractionRunStatus.PENDING


_TERMINAL_RUN_STATUSES = {
    ExtractionRunStatus.COMPLETED,
    ExtractionRunStatus.COMPLETED_WITH_FAILURES,
}


def _next_run_id(data_dir: Path, *, today: date) -> str:
    run_dir = data_dir / RUNS_DIRNAME
    if not run_dir.exists():
        return f"er-{today.strftime('%Y%m%d')}-001"

    prefix = f"er-{today.strftime('%Y%m%d')}-"
    sequences = []
    for path in run_dir.glob(f"{prefix}*.json"):
        try:
            sequences.append(int(path.stem.rsplit("-", 1)[1]))
        except (IndexError, ValueError):
            continue
    next_sequence = max(sequences, default=0) + 1
    return f"{prefix}{next_sequence:03d}"


def _run_path(data_dir: Path, run_id: str) -> Path:
    return data_dir / RUNS_DIRNAME / f"{run_id}.json"
