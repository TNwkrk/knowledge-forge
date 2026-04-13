# Knowledge Forge — Bucket Taxonomy

Knowledge Forge assigns each registered manifest to deterministic pre-processing
buckets before OCR, parsing, or inference work begins.

Bucket IDs use the canonical path shape:

`{manufacturer}/{family}/{dimension}`

Each path component is slugified from manifest fields. The assignment's `value`
stores the concrete member inside that bucket, which allows multi-model manuals
to belong to multiple values under the same bucket dimension.

## Dimensions

| Dimension | Source field | Example bucket ID | Example value | Notes |
|---|---|---|---|---|
| `manufacturer` | `document.manufacturer` | `honeywell/dc1000/manufacturer` | `Honeywell` | Vendor-wide grouping |
| `product_family` | `document.family` | `honeywell/dc1000/product-family` | `DC1000` | Product line or series grouping |
| `model_applicability` | `document.model_applicability[]` | `honeywell/dc1000/model-applicability` | `DC1100` | One assignment per applicable model |
| `document_type` | `document.document_type` | `honeywell/dc1000/document-type` | `Service Manual` | Separates manuals, bulletins, supplements, and guides |
| `revision_authority` | `document.revision` | `honeywell/dc1000/revision-authority` | `Rev 3` | Current best-available revision label until supersession rules exist |
| `publication_date` | `document.publication_date` | `honeywell/dc1000/publication-date` | `2024-01-15` | Temporal ordering key inside a family |

## Fallback rules

- Missing `publication_date` becomes the bucket value `undated`.
- Empty or repeated model entries collapse to a single ordered set.
- If a future manifest reaches bucketing with a missing string field, the
  assigner falls back to `unknown-*` values instead of failing the run.

## Example

A Honeywell DC1000 service manual for models `DC1000` and `DC1100`, revision
`Rev 3`, published on `2024-01-15`, produces these assignments:

| Bucket ID | Dimension | Value |
|---|---|---|
| `honeywell/dc1000/manufacturer` | `manufacturer` | `Honeywell` |
| `honeywell/dc1000/product-family` | `product_family` | `DC1000` |
| `honeywell/dc1000/model-applicability` | `model_applicability` | `DC1000` |
| `honeywell/dc1000/model-applicability` | `model_applicability` | `DC1100` |
| `honeywell/dc1000/document-type` | `document_type` | `Service Manual` |
| `honeywell/dc1000/revision-authority` | `revision_authority` | `Rev 3` |
| `honeywell/dc1000/publication-date` | `publication_date` | `2024-01-15` |

After bucketing, the manifest status moves from `registered` to `bucketed`.
