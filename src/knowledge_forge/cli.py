"""CLI entry points for Knowledge Forge."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import click

from knowledge_forge.bucketing.assigner import bucket_manifest, bucket_unassigned_manifests
from knowledge_forge.compile import (
    compile_all_overviews,
    compile_all_source_pages,
    compile_all_topic_pages,
    compile_bucket_topic_pages,
    compile_family_overview,
    compile_manufacturer_index,
    compile_source_page,
)
from knowledge_forge.extract import audit_document_provenance, extract_document
from knowledge_forge.inference import InferenceClient, InferenceConfig, aggregate_costs, ingest_results, poll_batch
from knowledge_forge.intake.importer import (
    RegistrationRequest,
    get_data_dir,
    list_manifests,
    load_manifest,
    register_document,
)
from knowledge_forge.intake.manifest import CANONICAL_DOCUMENT_TYPE_VALUES, DOCUMENT_CLASS_VALUES
from knowledge_forge.normalize import inspect_normalization, normalize_document
from knowledge_forge.parse import parse_document, score_parse, section_document
from knowledge_forge.publish import validate_publish_output


@click.group(help="Knowledge Forge command line interface.")
def cli() -> None:
    """Top-level CLI group for Knowledge Forge commands."""


@cli.group(help="Register and inspect source documents.")
def intake() -> None:
    """Intake command group."""


@cli.group(help="Inspect and operate the inference layer.")
def inference() -> None:
    """Inference command group."""


@cli.group(help="Compile reviewable knowledge artifacts from extracted records.")
def compile() -> None:
    """Compilation command group."""


@cli.group(help="Stage and validate publish-ready FlowCommander handoff output.")
def publish() -> None:
    """Publish command group."""


@intake.command("register")
@click.argument("pdf_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--force", is_flag=True, help="Re-register a duplicate checksum as a new document version.")
@click.option("--manufacturer", type=str, help="Document manufacturer.")
@click.option("--family", type=str, help="Product family or series.")
@click.option(
    "--model",
    "models",
    multiple=True,
    help="Model applicability. Repeat for multiple models.",
)
@click.option(
    "--document-class",
    type=click.Choice(DOCUMENT_CLASS_VALUES, case_sensitive=False),
    help="Document class: authoritative-technical, operational, or contextual.",
)
@click.option(
    "--document-type",
    type=str,
    help=("Document type. Canonical examples: " + ", ".join(CANONICAL_DOCUMENT_TYPE_VALUES[:8]) + ", ..."),
)
@click.option("--revision", type=str, help="Document revision identifier.")
@click.option(
    "--publication-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Publication date in YYYY-MM-DD format.",
)
@click.option("--language", type=str, help="Two-letter ISO 639-1 language code.")
@click.option("--priority", type=click.IntRange(min=1), help="Processing priority where 1 is highest.")
def intake_register(
    pdf_path: Path,
    force: bool,
    manufacturer: str | None,
    family: str | None,
    models: tuple[str, ...],
    document_class: str | None,
    document_type: str | None,
    revision: str | None,
    publication_date: object | None,
    language: str | None,
    priority: int | None,
) -> None:
    """Register a source document into the local manifest store."""
    if not pdf_path.suffix.casefold() == ".pdf":
        raise click.ClickException("source file must be a PDF")

    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer=manufacturer or click.prompt("Manufacturer"),
        family=family or click.prompt("Family"),
        model_applicability=list(models) if models else _prompt_models(),
        document_class=document_class or "authoritative-technical",
        document_type=document_type or click.prompt("Document type"),
        revision=revision or click.prompt("Revision"),
        publication_date=_coerce_publication_date(publication_date),
        language=language or click.prompt("Language", default="en", show_default=True),
        priority=priority if priority is not None else click.prompt("Priority", default=3, type=int, show_default=True),
        force=force,
    )

    try:
        result = register_document(request)
    except (FileExistsError, FileNotFoundError, IsADirectoryError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if result.created:
        click.echo(f"Registered {result.manifest.doc_id}")
        click.echo(f"Manifest: {result.manifest_path}")
        click.echo(f"Raw copy: {result.raw_path}")
        return

    click.echo(
        f"Document already registered with checksum {result.manifest.document.checksum}: {result.manifest.doc_id}"
    )
    click.echo(f"Manifest: {result.manifest_path}")


@intake.command("list")
def intake_list() -> None:
    """List all registered manifest entries."""
    manifests = list_manifests(get_data_dir())
    if not manifests:
        click.echo("No manifests found.")
        return

    click.echo("DOC ID\tSTATUS\tMANUFACTURER\tFAMILY\tTYPE\tREVISION")
    for manifest in manifests:
        document = manifest.document
        click.echo(
            "\t".join(
                [
                    manifest.doc_id,
                    document.status.value,
                    document.manufacturer,
                    document.family,
                    document.document_type,
                    document.revision,
                ]
            )
        )


@intake.command("inspect")
@click.argument("doc_id", type=str)
def intake_inspect(doc_id: str) -> None:
    """Print the full persisted manifest for a registered document."""
    try:
        manifest = load_manifest(get_data_dir(), doc_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(manifest.to_yaml().strip())


@intake.command("status")
@click.argument("doc_id", type=str)
def intake_status(doc_id: str) -> None:
    """Show the current lifecycle status and transition history for a document."""
    try:
        manifest = load_manifest(get_data_dir(), doc_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Document: {manifest.doc_id}")
    click.echo(f"Current status: {manifest.document.status.value}")
    click.echo(f"Current version: {manifest.document_version.version_id}")
    click.echo("History:")
    for transition in manifest.status_history:
        source = transition.from_status.value if transition.from_status is not None else "none"
        reason = f" ({transition.reason})" if transition.reason else ""
        click.echo(f"- {transition.changed_at.isoformat()} {source} -> {transition.to_status.value}{reason}")


@intake.command("bucket")
@click.argument("doc_id", required=False, type=str)
@click.option("--all", "bucket_all", is_flag=True, help="Bucket every manifest without assignments.")
def intake_bucket(doc_id: str | None, bucket_all: bool) -> None:
    """Assign deterministic buckets to one or more manifests."""
    if bucket_all and doc_id is not None:
        raise click.ClickException("pass either a doc_id or --all, not both")
    if not bucket_all and doc_id is None:
        raise click.ClickException("pass a doc_id or use --all")

    data_dir = get_data_dir()
    if bucket_all:
        results = bucket_unassigned_manifests(data_dir)
        if not results:
            click.echo("No unassigned manifests found.")
            return

        click.echo(f"Bucketed {len(results)} manifest(s).")
        for result in results:
            click.echo(f"{result.manifest.doc_id}\t{len(result.manifest.bucket_assignments)} assignments")
        return

    try:
        result = bucket_manifest(data_dir, doc_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Bucketed {result.manifest.doc_id}")
    click.echo(f"Assignments: {len(result.manifest.bucket_assignments)}")
    click.echo(f"Manifest: {result.manifest_path}")


@cli.command("normalize", context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs=-1, type=str)
@click.option("--all", "normalize_all", is_flag=True, help="Normalize every registered manifest.")
def normalize(args: tuple[str, ...], normalize_all: bool) -> None:
    """Run OCR normalization for one or more documents, or inspect prior results."""
    if args[:1] == ("inspect",):
        if normalize_all:
            raise click.ClickException("normalize inspect does not support --all")
        if len(args) != 2:
            raise click.ClickException("pass a doc_id to normalize inspect")
        _normalize_inspect(args[1])
        return

    doc_id = args[0] if args else None
    if normalize_all and len(args) > 0:
        raise click.ClickException("pass either a doc_id or --all, not both")
    if not normalize_all and doc_id is None:
        raise click.ClickException("pass a doc_id or use --all")

    data_dir = get_data_dir()
    if normalize_all:
        manifests = list_manifests(data_dir)
        if not manifests:
            click.echo("No manifests found.")
            return

        for manifest in manifests:
            result = normalize_document(manifest.doc_id, data_dir=data_dir)
            click.echo(f"Normalized {manifest.doc_id} -> {result.output_path}")
        return

    try:
        result = normalize_document(doc_id, data_dir=data_dir)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Normalized {doc_id}")
    click.echo(f"Output: {result.output_path}")


@cli.command("parse")
@click.argument("args", nargs=-1, type=str)
@click.option("--all", "parse_all", is_flag=True, help="Parse every normalized document.")
@click.option(
    "--parser",
    "parser_name",
    type=click.Choice(["auto", "docling", "fallback"], case_sensitive=False),
    default="auto",
    show_default=True,
    help="Parser mode to run.",
)
@click.option("--quality", "show_quality", is_flag=True, help="Show parse quality for a parsed document.")
def parse(args: tuple[str, ...], parse_all: bool, parser_name: str, show_quality: bool) -> None:
    """Parse one or more normalized documents with parser selection support."""
    if show_quality:
        if parse_all:
            raise click.ClickException("parse --quality does not support --all")
        if len(args) != 1:
            raise click.ClickException("pass a doc_id to parse --quality")
        _parse_quality(args[0])
        return

    doc_id = args[0] if args else None
    if parse_all and len(args) > 0:
        raise click.ClickException("pass either a doc_id or --all, not both")
    if not parse_all and doc_id is None:
        raise click.ClickException("pass a doc_id or use --all")

    data_dir = get_data_dir()
    if parse_all:
        manifests = list_manifests(data_dir)
        normalized_doc_ids = [
            manifest.doc_id for manifest in manifests if (data_dir / "normalized" / f"{manifest.doc_id}.pdf").exists()
        ]
        if not normalized_doc_ids:
            click.echo("No normalized manifests found.")
            return

        for manifest_doc_id in normalized_doc_ids:
            result = parse_document(manifest_doc_id, data_dir=data_dir, parser=parser_name)
            click.echo(
                f"Parsed {manifest_doc_id} with {result.parser} -> {result.content_path} "
                f"(quality {result.quality_report.overall_score:.2f})"
            )
        return

    try:
        result = parse_document(doc_id, data_dir=data_dir, parser=parser_name)
    except (FileNotFoundError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Parsed {doc_id}")
    click.echo(f"Parser: {result.parser}")
    click.echo(f"Content: {result.content_path}")
    click.echo(f"Quality score: {result.quality_report.overall_score:.2f}")


@cli.command("section")
@click.argument("doc_id", required=False, type=str)
@click.option("--all", "section_all", is_flag=True, help="Section every parsed document.")
def section(doc_id: str | None, section_all: bool) -> None:
    """Split parsed documents into typed canonical sections."""
    if section_all and doc_id is not None:
        raise click.ClickException("pass either a doc_id or --all, not both")
    if not section_all and doc_id is None:
        raise click.ClickException("pass a doc_id or use --all")

    data_dir = get_data_dir()
    if section_all:
        manifests = list_manifests(data_dir)
        parsed_doc_ids = [
            manifest.doc_id
            for manifest in manifests
            if (data_dir / "parsed" / manifest.doc_id / "structure.json").exists()
            and (data_dir / "parsed" / manifest.doc_id / "headings.json").exists()
        ]
        if not parsed_doc_ids:
            click.echo("No parsed manifests found.")
            return

        for manifest_doc_id in parsed_doc_ids:
            sections = section_document(manifest_doc_id, data_dir=data_dir)
            click.echo(f"Sectioned {manifest_doc_id} -> {len(sections)} sections")
        return

    try:
        sections = section_document(doc_id, data_dir=data_dir)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Sectioned {doc_id}")
    click.echo(f"Sections: {len(sections)}")
    if sections:
        click.echo(f"Output dir: {data_dir / 'sections' / doc_id}")


@cli.command("extract")
@click.argument("args", nargs=-1, type=str)
@click.option("--section", "section_id", type=str, help="Extract only one section from the document.")
@click.option(
    "--min-confidence",
    type=click.FloatRange(min=0.0, max=1.0),
    default=0.0,
    show_default=True,
    help="Flag records below this confidence threshold for review.",
)
@click.option(
    "--max-repair-attempts",
    type=click.IntRange(min=0),
    default=2,
    show_default=True,
    help="Maximum repair attempts for invalid extraction responses.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=Path("config/inference.yaml"),
    show_default=True,
    help="Inference configuration file.",
)
def extract(
    args: tuple[str, ...], section_id: str | None, min_confidence: float, max_repair_attempts: int, config_path: Path
) -> None:
    """Extract structured records from canonical sections."""
    if args[:1] == ("provenance",):
        if len(args) != 2:
            raise click.ClickException("pass a doc_id to extract provenance")
        if section_id is not None:
            raise click.ClickException("extract provenance does not support --section")
        _extract_provenance(args[1])
        return

    if len(args) != 1:
        raise click.ClickException("pass a doc_id or use 'extract provenance <doc_id>'")
    doc_id = args[0]

    try:
        config = InferenceConfig.load(config_path)
        records = extract_document(
            doc_id,
            section_id=section_id,
            config=config,
            data_dir=get_data_dir(),
            min_confidence=min_confidence,
            max_repair_attempts=max_repair_attempts,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Extracted {len(records)} record(s) for {doc_id}")
    if section_id is not None:
        click.echo(f"Section: {section_id}")
    if min_confidence > 0:
        click.echo(f"Review threshold: {min_confidence:.2f}")
    click.echo(f"Output dir: {get_data_dir() / 'extracted' / doc_id}")


def _extract_provenance(doc_id: str) -> None:
    try:
        report = audit_document_provenance(doc_id, data_dir=get_data_dir())
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Document: {report.doc_id}")
    click.echo(f"Total records: {report.total_records}")
    click.echo(f"Valid provenance: {report.valid_records}")
    click.echo(f"Invalid provenance: {report.invalid_records}")
    if report.invalid_records:
        click.echo("INVALID RECORDS")
        for row in report.rows:
            if row.valid:
                continue
            click.echo(f"{row.record_type}\t{row.record_id}\t{'; '.join(row.errors)}")


@compile.command("source-pages")
@click.argument("doc_id", required=False, type=str)
@click.option("--all", "compile_all", is_flag=True, help="Compile source pages for every extracted document.")
def compile_source_pages(doc_id: str | None, compile_all: bool) -> None:
    """Compile reviewable Markdown source pages from extracted records."""
    if compile_all and doc_id is not None:
        raise click.ClickException("pass either a doc_id or --all, not both")
    if not compile_all and doc_id is None:
        raise click.ClickException("pass a doc_id or use --all")

    data_dir = get_data_dir()
    if compile_all:
        try:
            pages = compile_all_source_pages(data_dir=data_dir)
        except (FileNotFoundError, ValueError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
        if not pages:
            click.echo("No extracted manifests found.")
            return
        click.echo(f"Compiled {len(pages)} source page(s).")
        for page in pages:
            click.echo(f"{page.doc_id}\t{page.output_path}")
        return

    try:
        page = compile_source_page(doc_id, data_dir=data_dir)
    except (FileNotFoundError, ValueError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Compiled source page for {doc_id}")
    click.echo(f"Output: {page.output_path}")


@compile.command("topic-pages")
@click.argument("bucket_id", required=False, type=str)
@click.option("--all", "compile_all", is_flag=True, help="Compile topic pages for every extracted bucket.")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=Path("config/inference.yaml"),
    show_default=True,
    help="Path to the inference config file.",
)
def compile_topic_pages(bucket_id: str | None, compile_all: bool, config_path: Path | None) -> None:
    """Compile cross-source topic pages from bucket-scoped extracted records."""
    if compile_all and bucket_id is not None:
        raise click.ClickException("pass either a bucket_id or --all, not both")
    if not compile_all and bucket_id is None:
        raise click.ClickException("pass a bucket_id or use --all")

    data_dir = get_data_dir()
    try:
        config = InferenceConfig.load(config_path)
        client = InferenceClient(config, data_dir=data_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if compile_all:
        try:
            pages = compile_all_topic_pages(client=client, data_dir=data_dir)
        except (FileNotFoundError, ValueError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
        if not pages:
            click.echo("No extracted buckets/records found to compile.")
            return
        click.echo(f"Compiled {len(pages)} topic page(s).")
        for page in pages:
            click.echo(f"{page.frontmatter['bucket_id']}\t{page.frontmatter['topic']}\t{page.output_path}")
        return

    try:
        pages = compile_bucket_topic_pages(bucket_id, client=client, data_dir=data_dir)
    except (FileNotFoundError, ValueError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc
    if not pages:
        click.echo(f"No topic pages found for bucket {bucket_id}.")
        return

    click.echo(f"Compiled {len(pages)} topic page(s) for {bucket_id}")
    for page in pages:
        click.echo(f"{page.frontmatter['topic']}\t{page.output_path}")


@compile.command("overviews")
@click.argument("target", required=False, type=str)
@click.option("--all", "compile_all", is_flag=True, help="Compile every family overview and manufacturer index.")
@click.option(
    "--manufacturer",
    "manufacturer_only",
    is_flag=True,
    help="Treat TARGET as a manufacturer instead of a family bucket.",
)
def compile_overviews(target: str | None, compile_all: bool, manufacturer_only: bool) -> None:
    """Compile family overview pages and manufacturer indexes."""
    if compile_all and target is not None:
        raise click.ClickException("pass either a target or --all, not both")
    if not compile_all and target is None:
        raise click.ClickException("pass a family bucket/manufacturer or use --all")

    data_dir = get_data_dir()
    if compile_all:
        try:
            pages = compile_all_overviews(data_dir=data_dir)
        except (FileNotFoundError, ValueError, KeyError) as exc:
            raise click.ClickException(str(exc)) from exc
        if not pages:
            click.echo("No extracted family buckets found to compile.")
            return
        click.echo(f"Compiled {len(pages)} overview page(s).")
        for page in pages:
            click.echo(f"{page.frontmatter.get('page_type', 'overview')}\t{page.output_path}")
        return

    try:
        page = (
            compile_manufacturer_index(target, data_dir=data_dir)
            if manufacturer_only
            else compile_family_overview(target, data_dir=data_dir)
        )
    except (FileNotFoundError, ValueError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc

    descriptor = "manufacturer index" if manufacturer_only else "family overview"
    click.echo(f"Compiled {descriptor} for {target}")
    click.echo(f"Output: {page.output_path}")


@publish.command("validate")
@click.argument("publish_run_id", type=str)
def publish_validate(publish_run_id: str) -> None:
    """Validate one staged publish run against the publish contract."""
    stage_dir = get_data_dir() / "publish" / publish_run_id
    report = validate_publish_output(stage_dir)
    click.echo(f"Publish run: {publish_run_id}")
    click.echo(f"Stage dir: {stage_dir}")
    click.echo(f"Valid: {'yes' if report.valid else 'no'}")
    if report.warnings:
        click.echo("WARNINGS")
        for warning in report.warnings:
            click.echo(f"- {warning}")
    if report.errors:
        click.echo("ERRORS")
        for error in report.errors:
            click.echo(f"- {error}")
        raise click.ClickException(f"publish validation failed for {publish_run_id}")


@inference.command("costs")
@click.option(
    "--log-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Override the inference log directory.",
)
def inference_costs(log_dir: Path | None) -> None:
    """Summarize logged inference token usage and estimated costs."""
    resolved_log_dir = log_dir or (get_data_dir() / "inference_logs")
    report = aggregate_costs(resolved_log_dir)

    click.echo(f"Log directory: {resolved_log_dir}")
    click.echo(f"Requests: {report.total.request_count}")
    click.echo(f"Input tokens: {report.total.input_tokens}")
    click.echo(f"Output tokens: {report.total.output_tokens}")
    click.echo(f"Estimated cost (USD): ${report.total.estimated_cost_usd:.6f}")

    if report.by_model:
        click.echo("BY MODEL")
        click.echo("MODEL\tREQUESTS\tINPUT_TOKENS\tOUTPUT_TOKENS\tEST_COST_USD")
        for model, totals in report.by_model.items():
            click.echo(
                f"{model}\t{totals.request_count}\t{totals.input_tokens}\t"
                f"{totals.output_tokens}\t{totals.estimated_cost_usd:.6f}"
            )

    if report.by_date:
        click.echo("BY DATE")
        click.echo("DATE\tREQUESTS\tINPUT_TOKENS\tOUTPUT_TOKENS\tEST_COST_USD")
        for day, totals in report.by_date.items():
            click.echo(
                f"{day}\t{totals.request_count}\t{totals.input_tokens}\t"
                f"{totals.output_tokens}\t{totals.estimated_cost_usd:.6f}"
            )

    if report.by_pipeline_run:
        click.echo("BY PIPELINE RUN")
        click.echo("PIPELINE_RUN\tREQUESTS\tINPUT_TOKENS\tOUTPUT_TOKENS\tEST_COST_USD")
        for pipeline_run_id, totals in report.by_pipeline_run.items():
            click.echo(
                f"{pipeline_run_id}\t{totals.request_count}\t{totals.input_tokens}\t"
                f"{totals.output_tokens}\t{totals.estimated_cost_usd:.6f}"
            )


@inference.command("batch-status")
@click.argument("batch_id", type=str)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=Path("config/inference.yaml"),
    show_default=True,
    help="Inference configuration file.",
)
def inference_batch_status(batch_id: str, config_path: Path) -> None:
    """Poll a batch job until it reaches a terminal state."""
    try:
        config = InferenceConfig.load(config_path)
        status = poll_batch(batch_id, config)
    except (FileNotFoundError, TimeoutError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Batch: {status.batch_id}")
    click.echo(f"Status: {status.status}")
    click.echo(f"Created: {status.created_at.isoformat()}")
    click.echo(f"Requests: {status.request_count}")
    if status.output_file_id is not None:
        click.echo(f"Output file: {status.output_file_id}")
    if status.error_file_id is not None:
        click.echo(f"Error file: {status.error_file_id}")
    if status.completed_at is not None:
        click.echo(f"Completed: {status.completed_at.isoformat()}")
    if status.failed_at is not None:
        click.echo(f"Failed: {status.failed_at.isoformat()}")


@inference.command("batch-ingest")
@click.argument("batch_id", type=str)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=Path("config/inference.yaml"),
    show_default=True,
    help="Inference configuration file.",
)
@click.option(
    "--data-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
    help="Override the data directory used for inference logs.",
)
def inference_batch_ingest(batch_id: str, config_path: Path, data_dir: Path | None) -> None:
    """Download and summarize a completed batch output."""
    try:
        config = InferenceConfig.load(config_path)
        results = ingest_results(batch_id, config, data_dir=data_dir)
    except (FileNotFoundError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Batch: {results.batch_id}")
    click.echo(f"Total: {results.stats.total}")
    click.echo(f"Succeeded: {results.stats.succeeded}")
    click.echo(f"Failed: {results.stats.failed}")
    if results.retry_custom_ids:
        click.echo(f"Retry custom_ids: {', '.join(results.retry_custom_ids)}")
    else:
        click.echo("Retry custom_ids: none")


def _normalize_inspect(doc_id: str) -> None:
    """Inspect persisted per-page OCR metadata for a document."""
    try:
        result = inspect_normalization(doc_id, data_dir=get_data_dir())
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Document: {doc_id}")
    click.echo(f"Output: {result.output_path}")
    click.echo("PAGE\tOCR\tTEXT_BEFORE\tVECTOR\tDENSITY_BEFORE\tDENSITY_AFTER\tCONFIDENCE\tBYPASS_REASON")
    for page in result.page_metadata:
        click.echo(
            "\t".join(
                [
                    str(page.page_number),
                    "yes" if page.ocr_applied else "no",
                    "yes" if page.has_text_before else "no",
                    "yes" if page.has_vector else "no",
                    f"{page.text_density_before:.4f}",
                    f"{page.text_density_after:.4f}",
                    f"{page.confidence:.3f}",
                    page.bypass_reason or "-",
                ]
            )
        )


def _parse_quality(doc_id: str) -> None:
    """Display parse quality metrics for a document without rewriting the persisted report."""
    try:
        report = score_parse(doc_id, data_dir=get_data_dir(), write_report=False)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Document: {doc_id}")
    click.echo(f"Overall score: {report.overall_score:.2f}")
    click.echo(f"Passes threshold: {'yes' if report.passes_threshold else 'no'}")
    click.echo("METRIC\tSCORE")
    click.echo(f"heading_coverage\t{report.metrics.heading_coverage:.2f}")
    click.echo(f"table_extraction_rate\t{report.metrics.table_extraction_rate:.2f}")
    click.echo(f"text_completeness\t{report.metrics.text_completeness:.2f}")
    click.echo(f"structure_depth\t{report.metrics.structure_depth:.2f}")
    click.echo(f"page_coverage\t{report.metrics.page_coverage:.2f}")


def _prompt_models() -> list[str]:
    """Prompt for a comma-separated model applicability list."""
    raw_value = click.prompt("Model applicability (comma-separated)")
    models = [item.strip() for item in raw_value.split(",") if item.strip()]
    if not models:
        raise click.ClickException("at least one model applicability value is required")
    return models


def _coerce_publication_date(value: object) -> date | None:
    """Convert Click values into the manifest's date type."""
    if value is None:
        return None

    return value.date()  # type: ignore[union-attr]


if __name__ == "__main__":
    cli()
