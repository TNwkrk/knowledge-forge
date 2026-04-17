"""Tests for the extraction evaluation harness."""

from __future__ import annotations

import json
from pathlib import Path

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
