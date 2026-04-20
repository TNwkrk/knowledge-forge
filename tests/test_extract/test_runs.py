"""Tests for durable extraction runs and resume semantics."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from knowledge_forge.extract.engine import ExtractionWorkItemResult
from knowledge_forge.extract.runs import (
    ExtractionRunItemStatus,
    ExtractionRunStatus,
    create_extraction_run,
    execute_extraction_run,
    load_extraction_run,
    resume_extraction_run,
    retry_failed_extraction_run,
)
from knowledge_forge.inference.config import ExtractionStrategy
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus
from knowledge_forge.parse.sectioning import Section


def _register_parsed_fixture(pdf_path: Path, data_dir: Path) -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000"],
        document_type="Service Manual",
        revision="Rev 3",
        publication_date=None,
        language="en",
        priority=1,
    )
    result = register_document(request, data_dir=data_dir)
    manifest_path = data_dir / "manifests" / f"{result.manifest.doc_id}.yaml"
    manifest = result.manifest.transition_status(DocumentStatus.PARSED, reason="test parsed fixture")
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")
    return result.manifest.doc_id


def _write_pdf(path: Path) -> Path:
    path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n")
    return path


def _write_section(path: Path, section: Section) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(section.model_dump_json(indent=2), encoding="utf-8")


def _write_parse_meta(data_dir: Path, doc_id: str, *, parser_version: str = "docling-9.9.9") -> None:
    meta_path = data_dir / "parsed" / doc_id / "meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "parser": "docling",
                "parser_version": parser_version,
                "processed_at": "2026-04-16T00:00:00Z",
                "processing_time_seconds": 1.2,
                "page_count": 99,
                "status": "success",
                "input_path": str(data_dir / "normalized" / f"{doc_id}.pdf"),
                "input_checksum": "a" * 64,
                "document_hash": None,
                "timings": {},
                "confidence": None,
                "errors": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_bucket_assignments(data_dir: Path, doc_id: str) -> None:
    manifest_path = data_dir / "manifests" / f"{doc_id}.yaml"
    manifest = load_manifest(data_dir, doc_id)
    updated_manifest = manifest.model_copy(
        update={
            "bucket_assignments": [
                BucketAssignment(
                    doc_id=doc_id,
                    bucket_id="honeywell/dc1000/family",
                    dimension="family",
                    value="DC1000",
                )
            ]
        }
    )
    manifest_path.write_text(updated_manifest.to_yaml(), encoding="utf-8")


class _FakeConfig:
    def __init__(
        self,
        *,
        strategy: ExtractionStrategy = ExtractionStrategy.DIRECT_SERIAL,
        max_tokens: int = 4096,
        max_requests_per_minute: int = 500,
        max_tokens_per_minute: int = 150000,
        direct_concurrency: int = 1,
        batch_chunk_size: int = 25,
        routing_rules: list[object] | None = None,
    ) -> None:
        self.extraction_model = "gpt-4o-mini"
        self.max_tokens = max_tokens
        self.temperature = 0.0
        self.rate_limit = SimpleNamespace(
            max_requests_per_minute=max_requests_per_minute,
            max_tokens_per_minute=max_tokens_per_minute,
        )
        self.batch = SimpleNamespace(max_batch_size=50000, poll_interval_seconds=60, max_poll_duration_seconds=86400)
        self.extraction = SimpleNamespace(
            strategy=strategy,
            direct_concurrency=direct_concurrency,
            batch_chunk_size=batch_chunk_size,
            dispatch_cooldown_seconds=15.0,
            token_estimate_chars_per_token=4.0,
            model_routing=SimpleNamespace(rules=list(routing_rules or [])),
        )


class _FingerprintOnlyClient:
    def __init__(self, config: object, *, data_dir: Path | None = None) -> None:
        self.config = config
        self._client = object()


def _succeeded_result(item, *, record_ids: list[str] | None = None) -> ExtractionWorkItemResult:
    return ExtractionWorkItemResult(
        doc_id=item.doc_id,
        section_id=item.section_id,
        record_type=item.record_type,
        status="succeeded",
        fingerprint=item.fingerprint,
        records=[],
        record_ids=record_ids or [f"{item.section_id}--{item.record_type}--001"],
        errors=[],
        review_flag=None,
        repair_attempts=0,
        output_paths=[],
    )


def _failed_result(item, *, error: str = "repair failed") -> ExtractionWorkItemResult:
    return ExtractionWorkItemResult(
        doc_id=item.doc_id,
        section_id=item.section_id,
        record_type=item.record_type,
        status="failed",
        fingerprint=item.fingerprint,
        records=[],
        record_ids=[],
        errors=[error],
        review_flag=None,
        repair_attempts=1,
        output_paths=[],
    )


def _make_run_fixture(tmp_path: Path, *, include_second_section: bool = False) -> tuple[Path, str]:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    first_section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="1. Verify the discharge valve is open.\n2. Apply control power.",
        page_range=(18, 20),
        heading_path=["DC1000 Service Manual", "Startup Procedure"],
    )
    _write_section(data_dir / "sections" / doc_id / f"{first_section.section_id}.json", first_section)
    if include_second_section:
        second_section = Section(
            doc_id=doc_id,
            section_id=f"{doc_id}--maintenance--002",
            section_type="maintenance",
            title="Maintenance Procedure",
            content="1. Lock out power.\n2. Check the pump seals.",
            page_range=(21, 22),
            heading_path=["DC1000 Service Manual", "Maintenance Procedure"],
        )
        _write_section(data_dir / "sections" / doc_id / f"{second_section.section_id}.json", second_section)
    return data_dir, doc_id


def _load_item(run_id: str, record_type: str, *, data_dir: Path):
    return next(
        item for item in load_extraction_run(run_id, data_dir=data_dir).items if item.record_type == record_type
    )


def test_partial_run_persists_checkpoint_and_resume_skips_completed_items(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    config = _FakeConfig()
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)
    call_order: list[str] = []

    def interrupting_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        call_order.append(item.record_type)
        if item.record_type == "procedure":
            return _succeeded_result(item)
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", interrupting_execute_work_item)

    with pytest.raises(RuntimeError, match="simulated interruption"):
        execute_extraction_run(run.run_id, config=config, data_dir=data_dir)

    checkpointed = load_extraction_run(run.run_id, data_dir=data_dir)
    statuses = {item.record_type: item.status for item in checkpointed.items}
    assert statuses == {
        "procedure": ExtractionRunItemStatus.SUCCEEDED,
        "warning": ExtractionRunItemStatus.IN_PROGRESS,
    }
    assert (data_dir / "extraction_runs" / f"{run.run_id}.json").exists()
    assert load_manifest(data_dir, doc_id).document.status == DocumentStatus.PARSED

    resumed_calls: list[str] = []

    def resuming_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        resumed_calls.append(item.record_type)
        return _succeeded_result(item)

    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", resuming_execute_work_item)
    execution = resume_extraction_run(run.run_id, config=config, data_dir=data_dir)
    resumed = load_extraction_run(run.run_id, data_dir=data_dir)

    assert resumed_calls == ["warning"]
    assert all(item.status == ExtractionRunItemStatus.SUCCEEDED for item in resumed.items)
    assert execution.run.status.value == "completed"
    assert load_manifest(data_dir, doc_id).document.status == DocumentStatus.EXTRACTED


def test_retry_failed_items_preserves_prior_successful_work(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    config = _FakeConfig()
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)

    def first_pass_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        if item.record_type == "procedure":
            return _succeeded_result(item, record_ids=["procedure-success"])
        return _failed_result(item)

    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", first_pass_execute_work_item)
    first_execution = execute_extraction_run(run.run_id, config=config, data_dir=data_dir)
    failed_run = load_extraction_run(run.run_id, data_dir=data_dir)
    assert first_execution.run.status.value == "completed_with_failures"
    failed_items = {item.record_type: item for item in failed_run.items}
    assert failed_items["procedure"].status == ExtractionRunItemStatus.SUCCEEDED
    assert failed_items["procedure"].record_ids == ["procedure-success"]
    assert failed_items["warning"].status == ExtractionRunItemStatus.FAILED
    assert load_manifest(data_dir, doc_id).document.status == DocumentStatus.PARSED

    retry_calls: list[str] = []

    def retry_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        retry_calls.append(item.record_type)
        return _succeeded_result(item, record_ids=["warning-success"])

    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", retry_execute_work_item)
    retry_execution = retry_failed_extraction_run(run.run_id, config=config, data_dir=data_dir)
    retried = load_extraction_run(run.run_id, data_dir=data_dir)
    retried_items = {item.record_type: item for item in retried.items}

    assert retry_calls == ["warning"]
    assert retry_execution.run.status.value == "completed"
    assert retried_items["procedure"].record_ids == ["procedure-success"]
    assert retried_items["warning"].record_ids == ["warning-success"]
    assert retried_items["procedure"].attempt_count == 1
    assert retried_items["warning"].attempt_count == 2
    assert load_manifest(data_dir, doc_id).document.status == DocumentStatus.EXTRACTED


def test_scheduler_enforces_rolling_token_budget_before_dispatch(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    config = _FakeConfig(max_tokens_per_minute=100)
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    monkeypatch.setattr("knowledge_forge.extract.runs._estimate_total_tokens", lambda *_args, **_kwargs: 60)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)
    sleeps: list[float] = []
    clock = {"now": 0.0}

    monkeypatch.setattr("knowledge_forge.extract.runs.time.monotonic", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr("knowledge_forge.extract.runs.time.sleep", fake_sleep)
    monkeypatch.setattr(
        "knowledge_forge.extract.runs.execute_work_item",
        lambda **kwargs: _succeeded_result(_load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)),
    )

    execution = execute_extraction_run(run.run_id, config=config, data_dir=data_dir)

    assert execution.run.status == ExtractionRunStatus.COMPLETED
    assert sleeps == [60.0]
    assert execution.run.metrics.throttle_seconds == 60.0
    assert execution.run.metrics.estimated_tokens_dispatched == 120


def test_shared_cooldown_on_429_retries_item_globally(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    config = _FakeConfig()
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    monkeypatch.setattr("knowledge_forge.extract.runs._estimate_total_tokens", lambda *_args, **_kwargs: 10)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)
    sleeps: list[float] = []
    clock = {"now": 0.0}
    snapshots: list[dict[str, str]] = []
    original_save = __import__("knowledge_forge.extract.runs", fromlist=["save_extraction_run"]).save_extraction_run

    class FakeRateLimitError(RuntimeError):
        status_code = 429

    monkeypatch.setattr("knowledge_forge.extract.runs.time.monotonic", lambda: clock["now"])

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    def wrapped_save(run_obj, *, data_dir=None):
        snapshots.append({item.record_type: item.status.value for item in run_obj.items})
        return original_save(run_obj, data_dir=data_dir)

    attempts = {"procedure": 0, "warning": 0}

    def flaky_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        attempts[item.record_type] += 1
        if item.record_type == "procedure" and attempts[item.record_type] == 1:
            raise FakeRateLimitError("Rate limit exceeded. Please try again in 6s.")
        return _succeeded_result(item)

    monkeypatch.setattr("knowledge_forge.extract.runs.time.sleep", fake_sleep)
    monkeypatch.setattr("knowledge_forge.extract.runs.save_extraction_run", wrapped_save)
    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", flaky_execute_work_item)

    execution = execute_extraction_run(run.run_id, config=config, data_dir=data_dir)
    persisted = load_extraction_run(run.run_id, data_dir=data_dir)

    assert execution.run.status == ExtractionRunStatus.COMPLETED
    assert persisted.metrics.rate_limit_429_count == 1
    assert persisted.metrics.throttle_seconds == 15.0
    assert sleeps == [15.0]
    assert attempts["procedure"] == 2
    assert any(snapshot == {"procedure": "pending", "warning": "pending"} for snapshot in snapshots)


def test_batch_strategy_is_explicit_and_counts_batch_dispatches(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    config = _FakeConfig(strategy=ExtractionStrategy.BATCH)
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)
    submitted_custom_ids: list[str] = []

    monkeypatch.setattr(
        "knowledge_forge.extract.runs.submit_batch",
        lambda jsonl_path, _config, sdk_client=None: (
            submitted_custom_ids.extend(
                json.loads(line)["custom_id"] for line in jsonl_path.read_text(encoding="utf-8").splitlines() if line
            )
            or SimpleNamespace(batch_id="batch-001")
        ),
    )
    monkeypatch.setattr("knowledge_forge.extract.runs.poll_batch", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "knowledge_forge.extract.runs.ingest_results",
        lambda *args, **kwargs: SimpleNamespace(
            successful=[
                SimpleNamespace(custom_id=custom_id, parsed_json={}, output_tokens=0, schema_valid=True)
                for custom_id in submitted_custom_ids
            ],
            failed=[],
        ),
    )
    monkeypatch.setattr(
        "knowledge_forge.extract.runs._persist_batch_success",
        lambda _run, *, prepared_item, success, client, data_dir: _succeeded_result(
            run.items[prepared_item.item_index]
        ),
    )

    execution = execute_extraction_run(run.run_id, config=config, data_dir=data_dir)

    assert execution.run.scheduler.strategy == ExtractionStrategy.BATCH
    assert execution.run.metrics.batch_dispatch_count == 2
    assert execution.run.metrics.direct_dispatch_count == 0
    assert sorted(submitted_custom_ids) == sorted(item.item_id for item in execution.run.items)


def test_model_routing_rule_records_selected_model_and_fallback_usage(monkeypatch, tmp_path: Path) -> None:
    data_dir, doc_id = _make_run_fixture(tmp_path)
    warning_fallback = SimpleNamespace(
        model="gpt-4.1-mini",
        record_types=["warning"],
        section_types=[],
        min_estimated_prompt_tokens=None,
        max_estimated_prompt_tokens=None,
        retry_class=None,
    )
    config = _FakeConfig(routing_rules=[warning_fallback])
    monkeypatch.setattr("knowledge_forge.extract.runs.InferenceClient", _FingerprintOnlyClient)
    run = create_extraction_run([doc_id], config=config, data_dir=data_dir)

    def routed_execute_work_item(**kwargs):
        item = _load_item(run.run_id, kwargs["record_type"], data_dir=data_dir)
        chosen_model = kwargs.get("model_override") or item.fingerprint.model
        return replace(_succeeded_result(item), fingerprint=replace(item.fingerprint, model=chosen_model))

    monkeypatch.setattr("knowledge_forge.extract.runs.execute_work_item", routed_execute_work_item)

    execution = execute_extraction_run(run.run_id, config=config, data_dir=data_dir)
    routed_items = {item.record_type: item for item in execution.run.items}

    assert routed_items["procedure"].selected_model == "gpt-4o-mini"
    assert routed_items["warning"].selected_model == "gpt-4.1-mini"
    assert routed_items["warning"].fingerprint.model == "gpt-4.1-mini"
    assert execution.run.metrics.fallback_dispatch_count == 1
