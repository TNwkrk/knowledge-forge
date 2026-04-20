"""Durable extraction run planning, persistence, and throughput scheduling."""

from __future__ import annotations

import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from knowledge_forge.extract.engine import (
    ExtractionFingerprint,
    ExtractionWorkItemResult,
    build_extraction_fingerprint,
    execute_work_item,
    load_prompt_template,
    load_section_quality,
    load_sections,
    persist_work_item_result,
    prepare_extraction_work_item,
    record_types_for_section_type,
    utc_now,
)
from knowledge_forge.extract.provenance import load_bucket_context, load_parse_metadata
from knowledge_forge.inference import (
    BatchBuilder,
    BatchFailure,
    BatchSuccess,
    InferenceClient,
    InferenceConfig,
    ValidationResult,
    ingest_results,
    poll_batch,
    submit_batch,
)
from knowledge_forge.inference.config import ExtractionStrategy
from knowledge_forge.inference.retry import RetryPolicy, suggested_delay_seconds
from knowledge_forge.intake.importer import get_data_dir, load_manifest
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse.sectioning import Section

RUNS_DIRNAME = "extraction_runs"
_ROLLING_WINDOW_SECONDS = 60.0


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


class ExtractionRunSchedulerSettings(BaseModel):
    """Persisted scheduler choices for one durable extraction run."""

    model_config = ConfigDict(extra="forbid")

    strategy: ExtractionStrategy
    max_requests_per_minute: int = Field(gt=0)
    max_tokens_per_minute: int = Field(gt=0)
    direct_concurrency: int = Field(gt=0)
    batch_chunk_size: int = Field(gt=0)
    dispatch_cooldown_seconds: float = Field(ge=0.0)
    token_estimate_chars_per_token: float = Field(gt=0.0)


class ExtractionThrottleEvent(BaseModel):
    """Persisted throttle/cooldown event recorded on the durable run."""

    model_config = ConfigDict(extra="forbid")

    occurred_at: datetime
    reason: str = Field(min_length=1)
    delay_seconds: float = Field(ge=0.0)
    item_id: str | None = None


class ExtractionRunMetrics(BaseModel):
    """Run-level observability counters for the extraction scheduler."""

    model_config = ConfigDict(extra="forbid")

    pending: int = Field(default=0, ge=0)
    running: int = Field(default=0, ge=0)
    succeeded: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    skipped: int = Field(default=0, ge=0)
    estimated_tokens_queued: int = Field(default=0, ge=0)
    estimated_tokens_dispatched: int = Field(default=0, ge=0)
    throttle_seconds: float = Field(default=0.0, ge=0.0)
    rate_limit_429_count: int = Field(default=0, ge=0)
    direct_dispatch_count: int = Field(default=0, ge=0)
    batch_dispatch_count: int = Field(default=0, ge=0)
    fallback_dispatch_count: int = Field(default=0, ge=0)
    throttle_events: list[ExtractionThrottleEvent] = Field(default_factory=list)


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
    selected_model: str | None = None
    dispatch_mode: str | None = None
    estimated_total_tokens: int = Field(default=0, ge=0)


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
    scheduler: ExtractionRunSchedulerSettings
    metrics: ExtractionRunMetrics = Field(default_factory=ExtractionRunMetrics)
    items: list[ExtractionRunItem] = Field(default_factory=list)


class ExtractionRunExecution(BaseModel):
    """Summary returned after running or resuming a durable extraction run."""

    model_config = ConfigDict(extra="forbid")

    run: ExtractionRun
    run_path: Path
    executed_item_ids: list[str] = Field(default_factory=list)
    records_emitted: int = Field(default=0, ge=0)


class _PreparedScheduledItem(BaseModel):
    """Prepared scheduler state for one work item dispatch."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    item_index: int
    estimated_total_tokens: int = Field(ge=0)
    selected_model: str = Field(min_length=1)
    fallback_used: bool = False
    prepared: Any


class _RollingBudget:
    """Rolling 60-second request/token window enforced before dispatch."""

    def __init__(self) -> None:
        self._entries: deque[tuple[float, int, int]] = deque()

    def reserve(self, *, now: float, requests: int, tokens: int) -> None:
        self._prune(now)
        self._entries.append((now, requests, tokens))

    def required_wait(
        self,
        *,
        now: float,
        requests: int,
        tokens: int,
        max_requests_per_minute: int,
        max_tokens_per_minute: int,
    ) -> float:
        self._prune(now)
        used_requests = sum(entry[1] for entry in self._entries)
        used_tokens = sum(entry[2] for entry in self._entries)
        if used_requests + requests <= max_requests_per_minute and used_tokens + tokens <= max_tokens_per_minute:
            return 0.0

        waits: list[float] = []
        future_requests = used_requests
        future_tokens = used_tokens
        for timestamp, req_count, token_count in self._entries:
            future_requests -= req_count
            future_tokens -= token_count
            release_after = max((timestamp + _ROLLING_WINDOW_SECONDS) - now, 0.0)
            if (
                future_requests + requests <= max_requests_per_minute
                and future_tokens + tokens <= max_tokens_per_minute
            ):
                waits.append(release_after)
                break
        return max(waits, default=_ROLLING_WINDOW_SECONDS)

    def _prune(self, now: float) -> None:
        while self._entries and now - self._entries[0][0] >= _ROLLING_WINDOW_SECONDS:
            self._entries.popleft()


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
        scheduler=_scheduler_settings_from_config(config),
        items=items,
    )
    run = _refresh_run_metrics(run)
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
    run = _refresh_run_metrics(run)
    run_path = save_extraction_run(run, data_dir=resolved_data_dir)

    executed_item_ids: list[str] = []
    records_emitted = 0
    budget = _RollingBudget()
    cooldown_until = 0.0

    if run.scheduler.strategy == ExtractionStrategy.BATCH:
        run, executed_item_ids, records_emitted = _execute_batch_strategy(
            run,
            client=client,
            data_dir=resolved_data_dir,
            budget=budget,
            cooldown_until=cooldown_until,
        )
    elif run.scheduler.strategy == ExtractionStrategy.DIRECT_LIMITED and run.scheduler.direct_concurrency > 1:
        run, executed_item_ids, records_emitted = _execute_direct_limited_strategy(
            run,
            client=client,
            data_dir=resolved_data_dir,
            budget=budget,
            cooldown_until=cooldown_until,
        )
    else:
        run, executed_item_ids, records_emitted = _execute_direct_serial_strategy(
            run,
            client=client,
            data_dir=resolved_data_dir,
            budget=budget,
            cooldown_until=cooldown_until,
        )

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
    run = _refresh_run_metrics(run)
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
                    "selected_model": current_fingerprint.model,
                }
            )
        elif retry_failed_only and item.status == ExtractionRunItemStatus.FAILED:
            item_update = item.model_copy(
                update={
                    "status": ExtractionRunItemStatus.PENDING,
                    "last_error": None,
                    "completed_at": None,
                }
            )
        elif not retry_failed_only and item.status == ExtractionRunItemStatus.IN_PROGRESS:
            item_update = item.model_copy(update={"status": ExtractionRunItemStatus.PENDING, "last_error": None})
        updated_items.append(item_update)

    scheduler = (
        run.scheduler
        if isinstance(run.scheduler, ExtractionRunSchedulerSettings)
        else _scheduler_settings_from_config(client.config)
    )
    return _refresh_run_metrics(
        run.model_copy(update={"items": updated_items, "updated_at": utc_now(), "scheduler": scheduler})
    )


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
                        selected_model=model,
                    )
                )
    return items


def _execute_direct_serial_strategy(
    run: ExtractionRun,
    *,
    client: InferenceClient,
    data_dir: Path,
    budget: _RollingBudget,
    cooldown_until: float,
) -> tuple[ExtractionRun, list[str], int]:
    executed_item_ids: list[str] = []
    records_emitted = 0

    while True:
        pending_indexes = [
            index for index, item in enumerate(run.items) if item.status == ExtractionRunItemStatus.PENDING
        ]
        if not pending_indexes:
            break

        index = pending_indexes[0]
        item = run.items[index]
        prepared_item = _prepare_scheduled_item(run, index=index, client=client, data_dir=data_dir)
        run, cooldown_until = _wait_for_dispatch_window(
            run,
            requests=1,
            tokens=prepared_item.estimated_total_tokens,
            budget=budget,
            cooldown_until=cooldown_until,
            data_dir=data_dir,
        )
        run = _mark_item_dispatched(
            run,
            prepared_item=prepared_item,
            dispatch_mode="direct",
            dispatched_tokens=prepared_item.estimated_total_tokens,
        )
        save_extraction_run(run, data_dir=data_dir)
        budget.reserve(now=time.monotonic(), requests=1, tokens=prepared_item.estimated_total_tokens)
        section = prepared_item.prepared.section
        parse_meta = load_parse_metadata(item.doc_id, data_dir=data_dir)
        bucket_context = load_bucket_context(item.doc_id, data_dir=data_dir)
        section_quality = load_section_quality(section, data_dir=data_dir)

        try:
            result = execute_work_item(
                section=section,
                record_type=item.record_type,
                client=client,
                data_dir=data_dir,
                max_repair_attempts=run.max_repair_attempts,
                section_quality=section_quality,
                min_confidence=run.min_confidence,
                parse_meta=parse_meta,
                bucket_context=bucket_context,
                pipeline_run_id=run.run_id,
                model_override=prepared_item.selected_model,
                retry_policy=RetryPolicy(max_retries=0),
            )
        except Exception as exc:
            if _is_rate_limit_error(exc):
                run, cooldown_until = _apply_shared_cooldown(
                    run,
                    delay_seconds=max(run.scheduler.dispatch_cooldown_seconds, suggested_delay_seconds(exc)),
                    reason=str(exc),
                    item_id=item.item_id,
                )
                run.items[index] = run.items[index].model_copy(
                    update={
                        "status": ExtractionRunItemStatus.PENDING,
                        "last_error": str(exc),
                        "dispatch_mode": "direct",
                        "selected_model": prepared_item.selected_model,
                        "estimated_total_tokens": prepared_item.estimated_total_tokens,
                    }
                )
                run = _refresh_run_metrics(run.model_copy(update={"updated_at": utc_now()}))
                save_extraction_run(run, data_dir=data_dir)
                continue
            raise

        executed_item_ids.append(item.item_id)
        records_emitted += len(result.records)
        run.items[index] = _update_item_from_result(run.items[index], result, dispatch_mode="direct")
        run = _refresh_run_metrics(run.model_copy(update={"updated_at": utc_now()}))
        save_extraction_run(run, data_dir=data_dir)
        _sync_completed_document_statuses(run, data_dir=data_dir)

    return run, executed_item_ids, records_emitted


def _execute_direct_limited_strategy(
    run: ExtractionRun,
    *,
    client: InferenceClient,
    data_dir: Path,
    budget: _RollingBudget,
    cooldown_until: float,
) -> tuple[ExtractionRun, list[str], int]:
    executed_item_ids: list[str] = []
    records_emitted = 0
    pending_indexes = deque(
        index for index, item in enumerate(run.items) if item.status == ExtractionRunItemStatus.PENDING
    )
    inflight: dict[Future[ExtractionWorkItemResult], tuple[int, _PreparedScheduledItem]] = {}

    with ThreadPoolExecutor(max_workers=run.scheduler.direct_concurrency) as executor:
        while pending_indexes or inflight:
            while pending_indexes and len(inflight) < run.scheduler.direct_concurrency:
                index = pending_indexes.popleft()
                item = run.items[index]
                prepared_item = _prepare_scheduled_item(run, index=index, client=client, data_dir=data_dir)
                run, cooldown_until = _wait_for_dispatch_window(
                    run,
                    requests=1,
                    tokens=prepared_item.estimated_total_tokens,
                    budget=budget,
                    cooldown_until=cooldown_until,
                    data_dir=data_dir,
                )
                run = _mark_item_dispatched(
                    run,
                    prepared_item=prepared_item,
                    dispatch_mode="direct",
                    dispatched_tokens=prepared_item.estimated_total_tokens,
                )
                save_extraction_run(run, data_dir=data_dir)
                budget.reserve(now=time.monotonic(), requests=1, tokens=prepared_item.estimated_total_tokens)
                section = prepared_item.prepared.section
                parse_meta = load_parse_metadata(item.doc_id, data_dir=data_dir)
                bucket_context = load_bucket_context(item.doc_id, data_dir=data_dir)
                section_quality = load_section_quality(section, data_dir=data_dir)
                future = executor.submit(
                    execute_work_item,
                    section=section,
                    record_type=item.record_type,
                    client=client,
                    data_dir=data_dir,
                    max_repair_attempts=run.max_repair_attempts,
                    section_quality=section_quality,
                    min_confidence=run.min_confidence,
                    parse_meta=parse_meta,
                    bucket_context=bucket_context,
                    pipeline_run_id=run.run_id,
                    model_override=prepared_item.selected_model,
                    retry_policy=RetryPolicy(max_retries=0),
                )
                inflight[future] = (index, prepared_item)

            if not inflight:
                continue

            done, _ = wait(tuple(inflight.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                index, prepared_item = inflight.pop(future)
                item = run.items[index]
                try:
                    result = future.result()
                except Exception as exc:
                    if _is_rate_limit_error(exc):
                        run, cooldown_until = _apply_shared_cooldown(
                            run,
                            delay_seconds=max(run.scheduler.dispatch_cooldown_seconds, suggested_delay_seconds(exc)),
                            reason=str(exc),
                            item_id=item.item_id,
                        )
                        run.items[index] = run.items[index].model_copy(
                            update={
                                "status": ExtractionRunItemStatus.PENDING,
                                "last_error": str(exc),
                                "dispatch_mode": "direct",
                                "selected_model": prepared_item.selected_model,
                                "estimated_total_tokens": prepared_item.estimated_total_tokens,
                            }
                        )
                        pending_indexes.append(index)
                        run = _refresh_run_metrics(run.model_copy(update={"updated_at": utc_now()}))
                        save_extraction_run(run, data_dir=data_dir)
                        _sync_completed_document_statuses(run, data_dir=data_dir)
                        continue
                    else:
                        raise
                else:
                    executed_item_ids.append(item.item_id)
                    records_emitted += len(result.records)
                    run.items[index] = _update_item_from_result(run.items[index], result, dispatch_mode="direct")

                run = _refresh_run_metrics(run.model_copy(update={"updated_at": utc_now()}))
                save_extraction_run(run, data_dir=data_dir)
                _sync_completed_document_statuses(run, data_dir=data_dir)

    return run, executed_item_ids, records_emitted


def _execute_batch_strategy(
    run: ExtractionRun,
    *,
    client: InferenceClient,
    data_dir: Path,
    budget: _RollingBudget,
    cooldown_until: float,
) -> tuple[ExtractionRun, list[str], int]:
    executed_item_ids: list[str] = []
    records_emitted = 0
    batch_sequence = 1

    while True:
        pending_indexes = [
            index for index, item in enumerate(run.items) if item.status == ExtractionRunItemStatus.PENDING
        ]
        if not pending_indexes:
            break

        prepared_items = [
            _prepare_scheduled_item(run, index=index, client=client, data_dir=data_dir) for index in pending_indexes
        ]
        batch_model = prepared_items[0].selected_model
        chunk: list[_PreparedScheduledItem] = []
        for prepared_item in prepared_items:
            if prepared_item.selected_model != batch_model:
                break
            if len(chunk) >= run.scheduler.batch_chunk_size:
                break
            chunk.append(prepared_item)

        batch_requests = len(chunk)
        batch_tokens = sum(item.estimated_total_tokens for item in chunk)
        run, cooldown_until = _wait_for_dispatch_window(
            run,
            requests=batch_requests,
            tokens=batch_tokens,
            budget=budget,
            cooldown_until=cooldown_until,
            data_dir=data_dir,
        )

        builder = BatchBuilder(client.config)
        schemas_by_custom_id: dict[str, dict[str, Any]] = {}
        custom_id_to_prepared: dict[str, _PreparedScheduledItem] = {}
        for prepared_item in chunk:
            prepared = prepared_item.prepared
            builder.add_request(
                custom_id=run.items[prepared_item.item_index].item_id,
                prompt=prepared.prompt,
                system=prepared.template.system,
                model=prepared.model,
                schema=prepared.schema,
            )
            schemas_by_custom_id[run.items[prepared_item.item_index].item_id] = prepared.schema
            custom_id_to_prepared[run.items[prepared_item.item_index].item_id] = prepared_item
            run = _mark_item_dispatched(
                run,
                prepared_item=prepared_item,
                dispatch_mode="batch",
                dispatched_tokens=prepared_item.estimated_total_tokens,
                dispatched_batch_item=True,
            )

        save_extraction_run(run, data_dir=data_dir)
        budget.reserve(now=time.monotonic(), requests=batch_requests, tokens=batch_tokens)

        batch_dir = data_dir / RUNS_DIRNAME / run.run_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = batch_dir / f"batch-{batch_sequence:03d}.jsonl"
        batch_sequence += 1
        builder.build_jsonl(jsonl_path)
        batch_job = submit_batch(jsonl_path, client.config, sdk_client=client._client)
        poll_batch(batch_job.batch_id, client.config, sdk_client=client._client)
        results = ingest_results(
            batch_job.batch_id,
            client.config,
            schemas_by_custom_id=schemas_by_custom_id,
            sdk_client=client._client,
            data_dir=data_dir,
        )

        successes = {result.custom_id: result for result in results.successful}
        failures = {result.custom_id: result for result in results.failed}
        for custom_id, prepared_item in custom_id_to_prepared.items():
            item = run.items[prepared_item.item_index]
            if custom_id in successes:
                success = successes[custom_id]
                result = _persist_batch_success(
                    run,
                    prepared_item=prepared_item,
                    success=success,
                    client=client,
                    data_dir=data_dir,
                )
                run.items[prepared_item.item_index] = _update_item_from_result(item, result, dispatch_mode="batch")
                executed_item_ids.append(item.item_id)
                records_emitted += len(result.records)
            elif custom_id in failures:
                failure = failures[custom_id]
                if _is_batch_rate_limit_failure(failure):
                    run, cooldown_until = _apply_shared_cooldown(
                        run,
                        delay_seconds=max(
                            run.scheduler.dispatch_cooldown_seconds,
                            suggested_delay_seconds(Exception(failure.message)),
                        ),
                        reason=failure.message,
                        item_id=item.item_id,
                    )
                    run.items[prepared_item.item_index] = item.model_copy(
                        update={
                            "status": ExtractionRunItemStatus.PENDING,
                            "last_error": failure.message,
                            "dispatch_mode": "batch",
                            "selected_model": prepared_item.selected_model,
                            "estimated_total_tokens": prepared_item.estimated_total_tokens,
                        }
                    )
                else:
                    run.items[prepared_item.item_index] = item.model_copy(
                        update={
                            "status": ExtractionRunItemStatus.FAILED,
                            "last_error": failure.message,
                            "dispatch_mode": "batch",
                            "selected_model": prepared_item.selected_model,
                            "estimated_total_tokens": prepared_item.estimated_total_tokens,
                            "completed_at": utc_now(),
                        }
                    )
            else:
                run.items[prepared_item.item_index] = item.model_copy(
                    update={
                        "status": ExtractionRunItemStatus.FAILED,
                        "last_error": f"batch result missing for {custom_id}",
                        "dispatch_mode": "batch",
                        "selected_model": prepared_item.selected_model,
                        "estimated_total_tokens": prepared_item.estimated_total_tokens,
                        "completed_at": utc_now(),
                    }
                )

        run = _refresh_run_metrics(run.model_copy(update={"updated_at": utc_now()}))
        save_extraction_run(run, data_dir=data_dir)
        _sync_completed_document_statuses(run, data_dir=data_dir)

    return run, executed_item_ids, records_emitted


def _persist_batch_success(
    run: ExtractionRun,
    *,
    prepared_item: _PreparedScheduledItem,
    success: BatchSuccess,
    client: InferenceClient,
    data_dir: Path,
) -> ExtractionWorkItemResult:
    prepared = prepared_item.prepared
    parse_meta = load_parse_metadata(prepared.section.doc_id, data_dir=data_dir)
    bucket_context = load_bucket_context(prepared.section.doc_id, data_dir=data_dir)
    section_quality = load_section_quality(prepared.section, data_dir=data_dir)
    parsed_json = success.parsed_json
    if parsed_json is None:
        raise ValueError(f"batch success for {prepared.section.section_id} did not include parsed JSON")
    return persist_work_item_result(
        prepared=prepared,
        parsed_json=parsed_json,
        validation=ValidationResult(valid=success.schema_valid is not False),
        output_tokens=success.output_tokens,
        repaired=False,
        repair_attempts=0,
        repair_errors=[],
        client=client,
        data_dir=data_dir,
        section_quality=section_quality,
        min_confidence=run.min_confidence,
        parse_meta=parse_meta,
        bucket_context=bucket_context,
    )


def _prepare_scheduled_item(
    run: ExtractionRun,
    *,
    index: int,
    client: InferenceClient,
    data_dir: Path,
) -> _PreparedScheduledItem:
    item = run.items[index]
    section = _load_section(item.doc_id, item.section_id, data_dir=data_dir)
    retry_class = "rate_limit" if item.last_error and "rate limit" in item.last_error.lower() else None
    selected_model = _select_model_for_item(run, item=item, section=section, client=client, retry_class=retry_class)
    prepared = prepare_extraction_work_item(
        section=section,
        record_type=item.record_type,
        client=client,
        model_override=selected_model,
    )
    estimated_total_tokens = _estimate_total_tokens(prepared.prompt, client.config, run.scheduler)
    default_model = item.fingerprint.model
    return _PreparedScheduledItem(
        item_index=index,
        estimated_total_tokens=estimated_total_tokens,
        selected_model=selected_model,
        fallback_used=selected_model != default_model,
        prepared=prepared,
    )


def _mark_item_dispatched(
    run: ExtractionRun,
    *,
    prepared_item: _PreparedScheduledItem,
    dispatch_mode: str,
    dispatched_tokens: int,
    dispatched_batch_item: bool = False,
) -> ExtractionRun:
    item = run.items[prepared_item.item_index]
    item_attempted_at = utc_now()
    updated_item = item.model_copy(
        update={
            "status": ExtractionRunItemStatus.IN_PROGRESS,
            "attempt_count": item.attempt_count + 1,
            "last_attempted_at": item_attempted_at,
            "last_error": None,
            "selected_model": prepared_item.selected_model,
            "dispatch_mode": dispatch_mode,
            "estimated_total_tokens": prepared_item.estimated_total_tokens,
        }
    )
    items = list(run.items)
    items[prepared_item.item_index] = updated_item
    metrics = run.metrics.model_copy(
        update={
            "estimated_tokens_dispatched": run.metrics.estimated_tokens_dispatched + dispatched_tokens,
            "direct_dispatch_count": run.metrics.direct_dispatch_count + (0 if dispatched_batch_item else 1),
            "batch_dispatch_count": run.metrics.batch_dispatch_count + (1 if dispatched_batch_item else 0),
            "fallback_dispatch_count": run.metrics.fallback_dispatch_count + (1 if prepared_item.fallback_used else 0),
        }
    )
    return _refresh_run_metrics(
        run.model_copy(update={"items": items, "metrics": metrics, "updated_at": item_attempted_at})
    )


def _wait_for_dispatch_window(
    run: ExtractionRun,
    *,
    requests: int,
    tokens: int,
    budget: _RollingBudget,
    cooldown_until: float,
    data_dir: Path,
) -> tuple[ExtractionRun, float]:
    now = time.monotonic()
    budget_wait = budget.required_wait(
        now=now,
        requests=requests,
        tokens=tokens,
        max_requests_per_minute=run.scheduler.max_requests_per_minute,
        max_tokens_per_minute=run.scheduler.max_tokens_per_minute,
    )
    cooldown_wait = max(cooldown_until - now, 0.0)
    wait_seconds = max(budget_wait, cooldown_wait)
    if wait_seconds <= 0:
        return run, cooldown_until

    run = _record_throttle_time(run, wait_seconds, reason="pre_dispatch_budget_wait")
    save_extraction_run(run, data_dir=data_dir)
    time.sleep(wait_seconds)
    return run, max(cooldown_until, now + wait_seconds)


def _apply_shared_cooldown(
    run: ExtractionRun,
    *,
    delay_seconds: float,
    reason: str,
    item_id: str | None,
) -> tuple[ExtractionRun, float]:
    effective_delay = max(delay_seconds, 0.0)
    event = ExtractionThrottleEvent(
        occurred_at=utc_now(),
        reason=reason,
        delay_seconds=effective_delay,
        item_id=item_id,
    )
    metrics = run.metrics.model_copy(
        update={
            "rate_limit_429_count": run.metrics.rate_limit_429_count + 1,
            "throttle_events": [*run.metrics.throttle_events, event],
        }
    )
    updated_run = _refresh_run_metrics(run.model_copy(update={"metrics": metrics, "updated_at": utc_now()}))
    return updated_run, time.monotonic() + effective_delay


def _record_throttle_time(run: ExtractionRun, delay_seconds: float, *, reason: str) -> ExtractionRun:
    event = ExtractionThrottleEvent(
        occurred_at=utc_now(),
        reason=reason,
        delay_seconds=delay_seconds,
        item_id=None,
    )
    metrics = run.metrics.model_copy(
        update={
            "throttle_seconds": run.metrics.throttle_seconds + delay_seconds,
            "throttle_events": [*run.metrics.throttle_events, event],
        }
    )
    return _refresh_run_metrics(run.model_copy(update={"metrics": metrics, "updated_at": utc_now()}))


def _estimate_total_tokens(prompt: str, config: InferenceConfig, scheduler: ExtractionRunSchedulerSettings) -> int:
    estimated_prompt_tokens = max(int(len(prompt) / scheduler.token_estimate_chars_per_token), 1)
    return estimated_prompt_tokens + config.max_tokens


def _select_model_for_item(
    run: ExtractionRun,
    *,
    item: ExtractionRunItem,
    section: Section,
    client: InferenceClient,
    retry_class: str | None,
) -> str:
    template = load_prompt_template(item.record_type)
    default_model = template.model or client.config.extraction_model
    estimated_prompt_tokens = max(int(len(section.content) / run.scheduler.token_estimate_chars_per_token), 1)
    extraction = getattr(client.config, "extraction", None)
    routing = getattr(extraction, "model_routing", None)
    rules = getattr(routing, "rules", [])
    for rule in rules:
        if rule.retry_class is not None and rule.retry_class != retry_class:
            continue
        if rule.record_types and item.record_type not in rule.record_types:
            continue
        if rule.section_types and section.section_type not in rule.section_types:
            continue
        if rule.min_estimated_prompt_tokens is not None and estimated_prompt_tokens < rule.min_estimated_prompt_tokens:
            continue
        if rule.max_estimated_prompt_tokens is not None and estimated_prompt_tokens > rule.max_estimated_prompt_tokens:
            continue
        return rule.model
    return default_model


def _rebuild_item_fingerprint(
    item: ExtractionRunItem, *, client: InferenceClient, data_dir: Path
) -> ExtractionFingerprint:
    section = _load_section(item.doc_id, item.section_id, data_dir=data_dir)
    template = load_prompt_template(item.record_type)
    model = item.selected_model or template.model or client.config.extraction_model
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


def _update_item_from_result(
    item: ExtractionRunItem,
    result: ExtractionWorkItemResult,
    *,
    dispatch_mode: str,
) -> ExtractionRunItem:
    status = ExtractionRunItemStatus.SUCCEEDED if result.status == "succeeded" else ExtractionRunItemStatus.FAILED
    error = None if not result.errors else "; ".join(result.errors)
    return item.model_copy(
        update={
            "status": status,
            "fingerprint": result.fingerprint,
            "record_ids": result.record_ids,
            "last_error": error,
            "completed_at": utc_now(),
            "selected_model": result.fingerprint.model,
            "dispatch_mode": dispatch_mode,
        }
    )


def _refresh_run_metrics(run: ExtractionRun) -> ExtractionRun:
    counts = summarize_run_status(run)
    queued_tokens = sum(
        item.estimated_total_tokens
        for item in run.items
        if item.status in {ExtractionRunItemStatus.PENDING, ExtractionRunItemStatus.IN_PROGRESS}
    )
    metrics = run.metrics.model_copy(
        update={
            "pending": counts["pending"],
            "running": counts["in_progress"],
            "succeeded": counts["succeeded"],
            "failed": counts["failed"],
            "skipped": counts["skipped"],
            "estimated_tokens_queued": queued_tokens,
        }
    )
    return run.model_copy(update={"metrics": metrics, "item_count": len(run.items)})


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


def _scheduler_settings_from_config(config: InferenceConfig) -> ExtractionRunSchedulerSettings:
    extraction = getattr(config, "extraction", None)
    rate_limit = getattr(config, "rate_limit", None)
    return ExtractionRunSchedulerSettings(
        strategy=getattr(extraction, "strategy", ExtractionStrategy.DIRECT_SERIAL),
        max_requests_per_minute=getattr(rate_limit, "max_requests_per_minute", 500),
        max_tokens_per_minute=getattr(rate_limit, "max_tokens_per_minute", 150000),
        direct_concurrency=getattr(extraction, "direct_concurrency", 1),
        batch_chunk_size=getattr(extraction, "batch_chunk_size", 25),
        dispatch_cooldown_seconds=getattr(extraction, "dispatch_cooldown_seconds", 15.0),
        token_estimate_chars_per_token=getattr(extraction, "token_estimate_chars_per_token", 4.0),
    )


def _is_rate_limit_error(error: Exception) -> bool:
    status_code = getattr(error, "status_code", None)
    if status_code is None:
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
    if status_code == 429:
        return True
    message = str(error).lower()
    return "rate limit" in message or "429" in message


def _is_batch_rate_limit_failure(failure: BatchFailure) -> bool:
    return failure.status_code == 429 or "rate limit" in failure.message.lower()


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
