"""Bucket taxonomy definitions for pre-processing classification."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BucketDimension:
    """Canonical bucket taxonomy dimension."""

    key: str
    label: str
    description: str


BUCKET_DIMENSIONS: tuple[BucketDimension, ...] = (
    BucketDimension(
        key="manufacturer",
        label="Manufacturer",
        description="Groups every manual from the same vendor.",
    ),
    BucketDimension(
        key="product_family",
        label="Product family",
        description="Groups manuals for a product family or series.",
    ),
    BucketDimension(
        key="model_applicability",
        label="Model applicability",
        description="Groups manuals that apply to one or more specific models.",
    ),
    BucketDimension(
        key="document_type",
        label="Document type",
        description="Separates service manuals, quick starts, bulletins, and supplements.",
    ),
    BucketDimension(
        key="document_class",
        label="Document class",
        description="Separates authoritative technical, operational, and contextual source families.",
    ),
    BucketDimension(
        key="revision_authority",
        label="Revision authority",
        description="Captures the current revision label used for later precedence analysis.",
    ),
    BucketDimension(
        key="publication_date",
        label="Publication date",
        description="Buckets manuals by publication date for temporal ordering within a family.",
    ),
)

BUCKET_DIMENSION_KEYS: tuple[str, ...] = tuple(dimension.key for dimension in BUCKET_DIMENSIONS)
