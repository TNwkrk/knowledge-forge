"""Tests for durable extraction runs and resume semantics."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from knowledge_forge.extract.engine import ExtractionWorkItemResult
from knowledge_forge.extract.runs import (
    ExtractionRunItemStatus,
    create_extraction_run,
    execute_extraction_run,
    load_extraction_run,
    resume_extraction_run,
    retry_failed_extraction_run,
)
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
    extraction_model: str = "gpt-4o-mini"
    max_tokens: int = 4096


class _FingerprintOnlyClient:
    def __init__(self, config: object, *, data_dir: Path | None = None) -> None:
        self.config = config


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


def _make_run_fixture(tmp_path: Path) -> tuple[Path, str]:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="1. Verify the discharge valve is open.\n2. Apply control power.",
        page_range=(18, 20),
        heading_path=["DC1000 Service Manual", "Startup Procedure"],
    )
    _write_section(data_dir / "sections" / doc_id / f"{section.section_id}.json", section)
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
