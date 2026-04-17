"""Family overview and manufacturer index page compilation."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from knowledge_forge.compile.source_pages import (
    GENERATED_BY,
    PUBLISH_RUN_PLACEHOLDER,
    CompiledPage,
    CompileMetadata,
)
from knowledge_forge.compile.topic_pages import TOPIC_TITLES, _load_bucket_records, classify_topic, utc_timestamp
from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import ManifestEntry, slugify

COMPILATION_VERSION = "overview-pages-v1"
FAMILY_DIMENSIONS = frozenset({"product_family", "family", "curated_bucket"})


@dataclass(frozen=True)
class FamilyOverviewInput:
    """Context needed to compile one family overview page."""

    bucket_id: str
    manufacturer: str
    bucket_label: str
    manifests: list[ManifestEntry]
    topics: list[str]


def compile_family_overview(bucket_id: str, *, data_dir: Path | None = None) -> CompiledPage:
    """Compile a reviewable family overview page for one family bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    family_input = _load_family_overview_input(bucket_id, data_dir=resolved_data_dir)
    generated_at = utc_timestamp()
    manufacturer_slug = slugify(family_input.manufacturer)
    family_slug = slugify(family_input.bucket_label)
    output_path = (
        resolved_data_dir
        / "compiled"
        / "overview-pages"
        / "manufacturers"
        / manufacturer_slug
        / family_slug
        / "_index.md"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    frontmatter = {
        "title": f"{family_input.manufacturer} {family_input.bucket_label} Family Overview",
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": _build_source_documents(family_input.manifests),
        "generated_at": generated_at,
        "document_statuses": _join_sorted(_manifest_statuses(family_input.manifests)),
        "compilation_version": COMPILATION_VERSION,
        "bucket_id": family_input.bucket_id,
        "manufacturer": family_input.manufacturer,
        "family": family_input.bucket_label,
        "page_type": "family_overview",
    }
    record_counts = {
        "documents": len(family_input.manifests),
        "topics": len(family_input.topics),
        "document_types": len({manifest.document.document_type for manifest in family_input.manifests}),
    }
    page = CompiledPage(
        output_path=output_path,
        doc_id=family_input.bucket_id,
        frontmatter=frontmatter,
        content=_render_family_overview(family_input),
        compile_metadata=CompileMetadata(
            generated_at=generated_at,
            extraction_versions=[],
            parser_versions=[],
            record_counts=record_counts,
            review_flag_count=0,
        ),
    )
    output_path.write_text(page.render(), encoding="utf-8")
    return page


def compile_manufacturer_index(manufacturer: str, *, data_dir: Path | None = None) -> CompiledPage:
    """Compile a manufacturer index page spanning every family overview."""
    resolved_data_dir = get_data_dir(data_dir)
    family_inputs = _load_manufacturer_family_inputs(manufacturer, data_dir=resolved_data_dir)
    if not family_inputs:
        raise FileNotFoundError(f"no extracted family buckets found for manufacturer '{manufacturer}'")

    manufacturer_name = family_inputs[0].manufacturer
    generated_at = utc_timestamp()
    output_path = (
        resolved_data_dir / "compiled" / "overview-pages" / "manufacturers" / slugify(manufacturer_name) / "_index.md"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    manifests = [manifest for family_input in family_inputs for manifest in family_input.manifests]
    frontmatter = {
        "title": f"{manufacturer_name} Manufacturer Index",
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": _build_source_documents(manifests),
        "generated_at": generated_at,
        "document_statuses": _join_sorted(_manifest_statuses(manifests)),
        "compilation_version": COMPILATION_VERSION,
        "manufacturer": manufacturer_name,
        "page_type": "manufacturer_index",
    }
    page = CompiledPage(
        output_path=output_path,
        doc_id=slugify(manufacturer_name),
        frontmatter=frontmatter,
        content=_render_manufacturer_index(manufacturer_name, family_inputs),
        compile_metadata=CompileMetadata(
            generated_at=generated_at,
            extraction_versions=[],
            parser_versions=[],
            record_counts={
                "families": len(family_inputs),
                "documents": len(manifests),
                "topics": sum(len(family_input.topics) for family_input in family_inputs),
            },
            review_flag_count=0,
        ),
    )
    output_path.write_text(page.render(), encoding="utf-8")
    return page


def compile_all_overviews(*, data_dir: Path | None = None) -> list[CompiledPage]:
    """Compile every family overview plus one manufacturer index per manufacturer."""
    resolved_data_dir = get_data_dir(data_dir)
    family_inputs = _discover_family_overview_inputs(resolved_data_dir)
    pages: list[CompiledPage] = []
    for family_input in family_inputs:
        pages.append(compile_family_overview(family_input.bucket_id, data_dir=resolved_data_dir))

    manufacturers = sorted({family_input.manufacturer for family_input in family_inputs})
    for manufacturer in manufacturers:
        pages.append(compile_manufacturer_index(manufacturer, data_dir=resolved_data_dir))
    return pages


def _render_family_overview(family_input: FamilyOverviewInput) -> str:
    document_types = Counter(manifest.document.document_type for manifest in family_input.manifests)
    document_classes = Counter(manifest.document.document_class for manifest in family_input.manifests)
    statuses = Counter(manifest.document.status.value for manifest in family_input.manifests)
    models = sorted({model for manifest in family_input.manifests for model in manifest.document.model_applicability})

    lines = [
        f"# {family_input.manufacturer} {family_input.bucket_label}",
        "",
        "## Family Summary",
        "",
        f"- Bucket: `{family_input.bucket_id}`",
        (
            f"- Family description: Review index for {family_input.manufacturer} "
            f"{family_input.bucket_label} knowledge artifacts."
        ),
        f"- Document count: {len(family_input.manifests)}",
        f"- Models covered: {', '.join(models) if models else 'unknown'}",
        f"- Topic pages: {len(family_input.topics)}",
        "",
        "## Available Topics",
        "",
    ]

    if family_input.topics:
        for topic in family_input.topics:
            lines.append(
                f"- [{TOPIC_TITLES[topic]}](../../../topic-pages/{slugify(family_input.bucket_id)}/{topic}.md)"
            )
    else:
        lines.append("- No compiled topic pages available yet.")

    lines.extend(
        [
            "",
            "## Document Inventory",
            "",
            *[f"- `{document_type}`: {count}" for document_type, count in sorted(document_types.items())],
            "",
            "## Quality Summary",
            "",
            *[f"- `{status}`: {count}" for status, count in sorted(statuses.items())],
            "",
            "## Document Class Coverage",
            "",
            *[f"- `{document_class}`: {count}" for document_class, count in sorted(document_classes.items())],
            "",
            "## Source Documents",
            "",
        ]
    )

    for manifest in sorted(family_input.manifests, key=lambda entry: entry.doc_id):
        document = manifest.document
        lines.append(
            f"- `{manifest.doc_id}` — {document.document_type} "
            f"({document.document_class}, revision {document.revision})"
        )

    return "\n".join(lines).rstrip()


def _render_manufacturer_index(manufacturer: str, family_inputs: list[FamilyOverviewInput]) -> str:
    lines = [
        f"# {manufacturer}",
        "",
        "## Product Families",
        "",
    ]
    for family_input in sorted(family_inputs, key=lambda entry: entry.bucket_label.casefold()):
        family_slug = slugify(family_input.bucket_label)
        topic_summary = ", ".join(TOPIC_TITLES[topic] for topic in family_input.topics)
        lines.append(
            f"- [{family_input.bucket_label}]({family_slug}/_index.md) — "
            f"{len(family_input.manifests)} documents; "
            f"topics: {topic_summary or 'none'}"
        )
    return "\n".join(lines).rstrip()


def _discover_family_overview_inputs(data_dir: Path) -> list[FamilyOverviewInput]:
    family_bucket_ids: set[str] = set()
    for manifest in list_manifests(data_dir):
        if not (data_dir / "extracted" / manifest.doc_id).exists():
            continue
        for assignment in manifest.bucket_assignments:
            if assignment.dimension in FAMILY_DIMENSIONS:
                family_bucket_ids.add(assignment.bucket_id)
    return [_load_family_overview_input(bucket_id, data_dir=data_dir) for bucket_id in sorted(family_bucket_ids)]


def _load_manufacturer_family_inputs(manufacturer: str, *, data_dir: Path) -> list[FamilyOverviewInput]:
    manufacturer_slug = slugify(manufacturer)
    return [
        family_input
        for family_input in _discover_family_overview_inputs(data_dir)
        if slugify(family_input.manufacturer) == manufacturer_slug
    ]


def _load_family_overview_input(bucket_id: str, *, data_dir: Path) -> FamilyOverviewInput:
    manifests = [
        manifest
        for manifest in list_manifests(data_dir)
        if (data_dir / "extracted" / manifest.doc_id).exists()
        and bucket_id in {assignment.bucket_id for assignment in manifest.bucket_assignments}
    ]
    if not manifests:
        raise FileNotFoundError(f"no extracted manifests found for family bucket '{bucket_id}'")

    manifests = sorted(manifests, key=lambda manifest: manifest.doc_id)
    first_document = manifests[0].document
    matching_assignment = next(
        assignment
        for assignment in manifests[0].bucket_assignments
        if assignment.bucket_id == bucket_id and assignment.dimension in FAMILY_DIMENSIONS
    )
    topics = sorted(
        {topic for topic in _discover_topics_for_bucket(bucket_id, data_dir=data_dir) if topic in TOPIC_TITLES}
    )
    return FamilyOverviewInput(
        bucket_id=bucket_id,
        manufacturer=first_document.manufacturer,
        bucket_label=matching_assignment.value,
        manifests=manifests,
        topics=topics,
    )


def _discover_topics_for_bucket(bucket_id: str, *, data_dir: Path) -> list[str]:
    topic_dir = data_dir / "compiled" / "topic-pages" / slugify(bucket_id)
    if topic_dir.exists():
        return sorted(path.stem for path in topic_dir.glob("*.md"))
    topic_records = _load_bucket_records(bucket_id, data_dir=data_dir)
    return sorted({topic for record in topic_records if (topic := classify_topic(record)) is not None})


def _build_source_documents(manifests: list[ManifestEntry]) -> list[dict[str, str]]:
    seen: dict[str, dict[str, str]] = {}
    for manifest in manifests:
        document = manifest.document
        seen.setdefault(
            manifest.doc_id,
            {
                "doc_id": manifest.doc_id,
                "revision": document.revision,
                "manufacturer": document.manufacturer,
                "family": document.family,
            },
        )
    return [seen[doc_id] for doc_id in sorted(seen)]


def _manifest_statuses(manifests: list[ManifestEntry]) -> list[str]:
    return sorted({manifest.document.status.value for manifest in manifests})


def _join_sorted(values: list[str]) -> str:
    return ", ".join(values) if values else "unknown"
