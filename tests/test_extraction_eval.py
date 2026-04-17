"""Tests for the extraction evaluation harness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.evaluation import evaluate_extraction


def test_evaluate_extraction_scores_committed_fixture_set() -> None:
    report = evaluate_extraction("baseline")

    assert report.fixture_set == "baseline"
    assert report.extraction_versions == ["extraction/baseline@v1:test-model"]
    fixture_ids = {fr.fixture_id for fr in report.fixture_reports}
    assert {"manual-structured", "sop-checklist"} == fixture_ids
    assert report.metrics.record_count_accuracy == 100.0
    assert report.metrics.field_accuracy == {
        "procedure": 100.0,
        "spec_value": 100.0,
        "warning": 100.0,
    }
    assert report.metrics.provenance_completeness == 100.0
    assert report.metrics.schema_compliance_rate == 100.0
    assert report.metrics.confidence_distribution.total_records == 5
    assert report.overall_score == 100.0


def test_eval_extraction_cli_writes_report(tmp_path: Path) -> None:
    runner = CliRunner()
    data_dir = tmp_path / "data"

    result = runner.invoke(
        cli,
        ["eval", "extraction", "baseline"],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Fixture set: baseline" in result.output
    assert "Overall score: 100.00" in result.output

    report_path = data_dir / "evaluation" / "extraction" / "baseline.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["fixture_set"] == "baseline"
    assert payload["extraction_versions"] == ["extraction/baseline@v1:test-model"]


def test_evaluate_extraction_missing_fixture_set_raises() -> None:
    with pytest.raises(FileNotFoundError, match="extraction eval fixture set not found"):
        evaluate_extraction("does-not-exist")


def test_evaluate_extraction_unknown_record_type_scores_as_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A JSON file under an unknown record type directory must not crash the harness."""
    from knowledge_forge.evaluation import extraction_eval

    fixture_dir = tmp_path / "unknown-type-fixture"
    extracted_dir = fixture_dir / "extracted" / "not_a_real_type"
    extracted_dir.mkdir(parents=True)
    (extracted_dir / "record-001.json").write_text(json.dumps({"field": "value"}), encoding="utf-8")
    (fixture_dir / "ground_truth.json").write_text(
        json.dumps(
            {
                "fixture_id": "unknown-type-fixture",
                "title": "Unknown type test",
                "source_pdf": "tests/fixtures/parser_eval/baseline/manual-structured/source.pdf",
                "expected_records": {},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(extraction_eval, "_fixture_set_root", lambda _: tmp_path)

    report = evaluate_extraction("unknown-type-test")
    assert len(report.fixture_reports) == 1
    fixture_report = report.fixture_reports[0]
    assert fixture_report.actual_record_counts == {"not_a_real_type": 1}
    assert fixture_report.metrics.schema_compliance_rate == 0.0
