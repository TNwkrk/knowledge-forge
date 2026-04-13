"""CLI entry points for Knowledge Forge."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import click

from knowledge_forge.bucketing.assigner import bucket_manifest, bucket_unassigned_manifests
from knowledge_forge.intake.importer import (
    RegistrationRequest,
    get_data_dir,
    list_manifests,
    load_manifest,
    register_document,
)


@click.group(help="Knowledge Forge command line interface.")
def cli() -> None:
    """Top-level CLI group for Knowledge Forge commands."""


@cli.group(help="Register and inspect source manuals.")
def intake() -> None:
    """Intake command group."""


@intake.command("register")
@click.argument("pdf_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--manufacturer", type=str, help="Document manufacturer.")
@click.option("--family", type=str, help="Product family or series.")
@click.option(
    "--model",
    "models",
    multiple=True,
    help="Model applicability. Repeat for multiple models.",
)
@click.option("--document-type", type=str, help="Document type, such as Service Manual.")
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
    manufacturer: str | None,
    family: str | None,
    models: tuple[str, ...],
    document_type: str | None,
    revision: str | None,
    publication_date: object | None,
    language: str | None,
    priority: int | None,
) -> None:
    """Register a source manual into the local manifest store."""
    if not pdf_path.suffix.casefold() == ".pdf":
        raise click.ClickException("source file must be a PDF")

    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer=manufacturer or click.prompt("Manufacturer"),
        family=family or click.prompt("Family"),
        model_applicability=list(models) if models else _prompt_models(),
        document_type=document_type or click.prompt("Document type"),
        revision=revision or click.prompt("Revision"),
        publication_date=_coerce_publication_date(publication_date),
        language=language or click.prompt("Language", default="en", show_default=True),
        priority=priority if priority is not None else click.prompt("Priority", default=3, type=int, show_default=True),
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
