"""Tests for the intake CLI commands."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.intake.manifest import ManifestEntry


def _create_pdf(path: Path, content: bytes = b"%PDF-1.4\n% knowledge forge\n") -> Path:
    path.write_bytes(content)
    return path


def test_intake_register_creates_manifest_and_raw_copy(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf")
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--model",
            "DC1100",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--publication-date",
            "2024-01-15",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Registered honeywell-dc1000-service-manual-rev-3" in result.output

    manifest_path = data_dir / "manifests" / "honeywell-dc1000-service-manual-rev-3.yaml"
    checksum_index_path = data_dir / "manifests" / "checksum-index.yaml"
    raw_path = data_dir / "raw" / "honeywell-dc1000-service-manual-rev-3.pdf"

    assert manifest_path.exists()
    assert checksum_index_path.exists()
    assert raw_path.exists()
    assert raw_path.read_bytes() == source.read_bytes()

    manifest = ManifestEntry.from_yaml(manifest_path.read_text(encoding="utf-8"))
    assert manifest.document.source_path == source.resolve()
    assert manifest.document.model_applicability == ["DC1000", "DC1100"]
    assert manifest.document.document_class == "authoritative-technical"
    assert manifest.document.status.value == "registered"


def test_intake_register_pack_registers_core_entries_and_skips_conditionals(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source_root = tmp_path / "assets"
    source_root.mkdir(parents=True)
    (source_root / "first.pdf").write_bytes(b"%PDF-1.4\n% one\n")
    (source_root / "second.pdf").write_bytes(b"%PDF-1.4\n% two\n")
    manifest_path = tmp_path / "pack.yaml"
    manifest_path.write_text(
        """
name: Rockwell Pump Station Control Stack
manufacturer: Rockwell
bucket: Pump Station Control Stack
scope: HMI, PLC, network, wiring, and control power
goal: First real manufacturer bucket.
source_dir: ./assets
documents:
  - filename: first.pdf
    family: PanelView Plus 7
    model_applicability: [PanelView Plus 7]
    document_type: datasheet
    revision: rev-a
  - filename: second.pdf
    family: CompactLogix
    model_applicability: [CompactLogix]
    document_type: operation-manual
    revision: rev-b
    include: conditional
        """.strip(),
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["intake", "register-pack", str(manifest_path)],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Registered: 1" in result.output
    assert "Skipped conditionals: second.pdf" in result.output

    manifests = sorted(
        path.name for path in (data_dir / "manifests").glob("*.yaml") if path.name != "checksum-index.yaml"
    )
    assert manifests == ["rockwell-panelview-plus-7-datasheet-rev-a.yaml"]
    manifest = ManifestEntry.from_yaml((data_dir / "manifests" / manifests[0]).read_text(encoding="utf-8"))
    assert manifest.document.curated_bucket == "Pump Station Control Stack"


def test_intake_register_pack_allows_missing_files_when_requested(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source_root = tmp_path / "assets"
    source_root.mkdir(parents=True)
    manifest_path = tmp_path / "pack.yaml"
    manifest_path.write_text(
        """
name: Rockwell Pump Station Control Stack
manufacturer: Rockwell
bucket: Pump Station Control Stack
scope: HMI, PLC, network, wiring, and control power
goal: First real manufacturer bucket.
source_dir: ./assets
documents:
  - filename: missing.pdf
    family: CompactLogix
    model_applicability: [CompactLogix]
    document_type: operation-manual
    revision: rev-a
        """.strip(),
        encoding="utf-8",
    )
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["intake", "register-pack", str(manifest_path), "--allow-missing"],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Missing files:" in result.output
    assert "missing.pdf" in result.output


def test_intake_list_and_inspect_show_registered_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    register = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-type",
            "Quick Start Guide",
            "--revision",
            "Rev B",
            "--language",
            "en",
            "--priority",
            "3",
        ],
        env=env,
    )
    assert register.exit_code == 0

    listing = runner.invoke(cli, ["intake", "list"], env=env)
    assert listing.exit_code == 0
    assert "DOC ID\tSTATUS\tMANUFACTURER\tFAMILY\tTYPE\tREVISION" in listing.output
    assert "honeywell-dc1000-quick-start-guide-rev-b\tregistered\tHoneywell\tDC1000" in listing.output

    inspect = runner.invoke(
        cli,
        ["intake", "inspect", "honeywell-dc1000-quick-start-guide-rev-b"],
        env=env,
    )
    assert inspect.exit_code == 0
    assert "manufacturer: Honeywell" in inspect.output
    assert "document_class: authoritative-technical" in inspect.output
    assert "document_type: Quick Start Guide" in inspect.output
    assert "model_applicability:" in inspect.output


def test_intake_register_detects_duplicate_checksum(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source_a = _create_pdf(tmp_path / "manual-a.pdf", content=b"%PDF-1.4\n% same checksum\n")
    source_b = _create_pdf(tmp_path / "manual-b.pdf", content=b"%PDF-1.4\n% same checksum\n")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    first = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source_a),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )
    assert first.exit_code == 0

    second = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source_b),
            "--manufacturer",
            "Other Manufacturer",
            "--family",
            "Different Family",
            "--model",
            "Different Model",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 99",
            "--language",
            "en",
            "--priority",
            "1",
        ],
        env=env,
    )

    assert second.exit_code == 0
    assert "Document already registered" in second.output
    manifests = sorted((data_dir / "manifests").glob("*.yaml"))
    assert len([path for path in manifests if path.name != "checksum-index.yaml"]) == 1


def test_intake_register_force_creates_new_document_version(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf", content=b"%PDF-1.4\n% same checksum\n")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    initial = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )
    assert initial.exit_code == 0

    forced = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--force",
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )

    assert forced.exit_code == 0
    assert "Registered honeywell-dc1000-service-manual-rev-3" in forced.output

    manifest = ManifestEntry.from_yaml(
        (data_dir / "manifests" / "honeywell-dc1000-service-manual-rev-3.yaml").read_text(encoding="utf-8")
    )
    assert manifest.document_version.version_number == 2
    assert [version.version_number for version in manifest.document_versions] == [1, 2]
    assert manifest.document.status.value == "registered"
    assert manifest.status_history[-1].reason == "forced re-registration"


def test_intake_register_prompts_for_missing_fields(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf")
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["intake", "register", str(source)],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
        input="Honeywell\nDC1000\nDC1000, DC1100\nService Manual\nRev 3\nen\n2\n",
    )

    assert result.exit_code == 0
    assert "Registered honeywell-dc1000-service-manual-rev-3" in result.output


def test_intake_bucket_updates_single_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    register = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--model",
            "DC1100",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--publication-date",
            "2024-01-15",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )
    assert register.exit_code == 0

    bucket = runner.invoke(cli, ["intake", "bucket", "honeywell-dc1000-service-manual-rev-3"], env=env)

    assert bucket.exit_code == 0
    assert "Bucketed honeywell-dc1000-service-manual-rev-3" in bucket.output
    assert "Assignments: 8" in bucket.output

    manifest = ManifestEntry.from_yaml(
        (data_dir / "manifests" / "honeywell-dc1000-service-manual-rev-3.yaml").read_text(encoding="utf-8")
    )
    assert manifest.document.status.value == "bucketed"
    assert len(manifest.bucket_assignments) == 8
    assert manifest.status_history[-1].to_status == manifest.document.status
    assert manifest.status_history[-1].reason == "bucket assignments generated"


def test_intake_bucket_all_only_processes_unassigned_manifests(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    for revision in ("Rev 3", "Rev 4"):
        source = _create_pdf(tmp_path / f"{revision}.pdf", content=f"%PDF-1.4\n% {revision}\n".encode())
        register = runner.invoke(
            cli,
            [
                "intake",
                "register",
                str(source),
                "--manufacturer",
                "Honeywell",
                "--family",
                "DC1000",
                "--model",
                "DC1000",
                "--document-type",
                "Service Manual",
                "--revision",
                revision,
                "--language",
                "en",
                "--priority",
                "2",
            ],
            env=env,
        )
        assert register.exit_code == 0

    first_bucket = runner.invoke(cli, ["intake", "bucket", "honeywell-dc1000-service-manual-rev-3"], env=env)
    assert first_bucket.exit_code == 0

    bucket_all = runner.invoke(cli, ["intake", "bucket", "--all"], env=env)

    assert bucket_all.exit_code == 0
    assert "Bucketed 1 manifest(s)." in bucket_all.output
    assert "honeywell-dc1000-service-manual-rev-4\t7 assignments" in bucket_all.output


def test_intake_register_accepts_non_manual_document_class_and_type(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "sop.pdf")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    register = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-class",
            "operational",
            "--document-type",
            "sop",
            "--revision",
            "Rev A",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )
    assert register.exit_code == 0

    bucket = runner.invoke(cli, ["intake", "bucket", "honeywell-dc1000-sop-rev-a"], env=env)
    assert bucket.exit_code == 0
    assert "Assignments: 7" in bucket.output

    manifest = ManifestEntry.from_yaml((data_dir / "manifests" / "honeywell-dc1000-sop-rev-a.yaml").read_text("utf-8"))
    assert manifest.document.document_class == "operational"
    assert manifest.document.document_type == "sop"
    document_class_bucket = [item for item in manifest.bucket_assignments if item.dimension == "document_class"]
    assert len(document_class_bucket) == 1
    assert document_class_bucket[0].value == "operational"


def test_intake_status_reports_current_status_and_history(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _create_pdf(tmp_path / "manual.pdf")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    register = runner.invoke(
        cli,
        [
            "intake",
            "register",
            str(source),
            "--manufacturer",
            "Honeywell",
            "--family",
            "DC1000",
            "--model",
            "DC1000",
            "--document-type",
            "Service Manual",
            "--revision",
            "Rev 3",
            "--language",
            "en",
            "--priority",
            "2",
        ],
        env=env,
    )
    assert register.exit_code == 0

    bucket = runner.invoke(cli, ["intake", "bucket", "honeywell-dc1000-service-manual-rev-3"], env=env)
    assert bucket.exit_code == 0

    status = runner.invoke(cli, ["intake", "status", "honeywell-dc1000-service-manual-rev-3"], env=env)

    assert status.exit_code == 0
    assert "Current status: bucketed" in status.output
    assert "Current version: honeywell-dc1000-service-manual-rev-3--v001" in status.output
    assert "none -> registered" in status.output
    assert "registered -> bucketed (bucket assignments generated)" in status.output
