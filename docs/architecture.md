# Knowledge Forge — Architecture

## System overview

Knowledge Forge is a local-first pipeline that converts *promoted candidate source packs* — technical manuals, SOPs, checklists, inspection and service forms, drawings, and other field-service source material — into structured, reviewable knowledge artifacts. It is a separate system from FlowCommander. FlowCommander owns operational intake and editorial promotion; Knowledge Forge is the refinement engine. See [`FlowCommander/docs/operational-intake-model.md`](https://github.com/TNwkrk/FlowCommander/blob/main/docs/operational-intake-model.md) for the upstream contract.

```
(curated pack | FC-promoted pack) → intake → pre-bucket → OCR/parse → LLM structured extraction → LLM wiki compilation → guardrails → review → publish PR
```

Every stage produces durable artifacts on disk. No stage depends on hosted services for its core processing loop.

## Guiding principles

1. **Filesystem-first.** All pipeline artifacts live on disk in well-known paths. A database may index them later, but the files are the source of truth.
2. **Provenance everywhere.** Every extracted record traces back to a source document, page range, heading, parser version, extraction version, and confidence score.
3. **Rerun safety.** Any stage can be re-executed without corrupting previously approved outputs. Idempotent writes keyed on content hashes.
4. **Pre-bucket before heavy processing.** Source documents are classified and grouped before any LLM inference runs. This bounds cost and scopes contradiction analysis.
5. **LLM inference is a first-class subsystem.** The OpenAI integration is not bolted on—it has its own client layer, logging, cost tracking, batch support, and retry logic.
6. **PR-based publish.** Nothing writes directly into FlowCommander. All output goes through a staged publish step that opens a pull request for human review.
7. **Embeddings are not the knowledge layer.** Structured extraction and compiled wiki pages are the primary outputs. Embeddings may be generated downstream but are not the source of truth.

## Pipeline stages

### Stage 0 — Intake and manifest

Every source document entering the system (whether from a curated source pack or an FC-promoted candidate pack) gets a manifest record:

| Field | Purpose |
|---|---|
| `source_path` | Original file location |
| `checksum` | SHA-256 of the source file |
| `manufacturer` | E.g. Honeywell, Grundfos |
| `family` | Product family or series |
| `model_applicability` | Specific models covered |
| `document_type` | Service manual, installation manual, bulletin, datasheet, parts list, SOP, checklist, drawing, field form, training material, etc. |
| `document_class` | `authoritative-technical`, `operational`, or `contextual` |
| `revision` | Document revision identifier |
| `publication_date` | When the document was published |
| `language` | ISO 639-1 |
| `priority` | Processing priority (1 = highest) |
| `status` | `registered`, `bucketed`, `normalized`, `parsed`, `extracted`, `compiled`, `published` |

Manifests are stored as YAML or JSON files in `data/manifests/`.

### Stage 1 — Pre-bucketing

Before any heavy processing, each manifest entry is assigned to one or more buckets:

- **Manufacturer** — groups all docs from one vendor
- **Product family** — groups docs for a product line
- **Model / applicability** — groups docs for specific models
- **Document type** — service manual vs. bulletin vs. supplement
- **Document class** — authoritative technical vs. operational vs. contextual
- **Revision authority** — which doc supersedes which
- **Publication date** — temporal ordering within a bucket

Buckets are the unit of contradiction analysis. Only records within the same bucket are compared.

### Stage 2 — OCR and normalization

Scanned or inconsistent PDFs are normalized before parsing:

- OCRmyPDF adds a text layer to scanned pages
- Per-page text-density analysis decides which pages actually need OCR
- Metadata (page count, per-page OCR confidence, skew correction, bypass reasons) is recorded
- Normalized PDFs are saved to `data/normalized/`
- Already-digital PDFs may skip OCR but still get a normalization record

### Stage 3 — Layout-aware parsing

The primary parser (Docling) produces structured output:

| Artifact | Format | Path |
|---|---|---|
| Markdown body | `.md` | `data/parsed/{doc_id}/content.md` |
| Structured JSON | `.json` | `data/parsed/{doc_id}/structure.json` |
| Heading tree | `.json` | `data/parsed/{doc_id}/headings.json` |
| Table outputs | `.json` | `data/parsed/{doc_id}/tables.json` |
| Page map | `.json` | `data/parsed/{doc_id}/page_map.json` |
| Parse metadata | `.json` | `data/parsed/{doc_id}/meta.json` |
| Parse quality report | `.json` | `data/parsed/{doc_id}/quality.json` |

`structure.json` is the canonical parser-neutral artifact envelope: `doc_id`, `parser`, `parser_version`, `page_count`, normalized `texts`, normalized `tables`, and normalized `pages`.

If the primary parser scores below the configured quality threshold, a fallback parser (MinerU or Marker) runs and its outputs are compared.
The selected parser's artifacts remain at the top-level document paths, while parser-specific runs stay under `data/parsed/{doc_id}/runs/{parser}/` for inspection.

### Stage 4 — Canonical sectioning

Parsed content is split into typed sections:

- safety, installation, configuration, startup, shutdown
- maintenance (preventive, corrective, seasonal), troubleshooting, specifications, parts
- revision notes, addenda, bulletins
- workflow / SOP / checklist, inspection / commissioning
- wiring / drawings / diagrams

Each section becomes a unit of extraction. Section boundaries and types are recorded with their source page ranges.

### Stage 5 — OpenAI structured extraction

Each section is sent to the OpenAI API with a schema-bound prompt. The model returns structured records:

- `procedure`, `procedure_step`
- `warning`
- `spec_value`
- `alarm_definition`
- `troubleshooting_entry`
- `part_reference`
- `applicability`
- `revision_note`
- `supersession_candidate`
- `contradiction_candidate`

Every record carries provenance: source document, page range, heading, parser version, extraction model, extraction version, confidence.

Invalid outputs go through a repair path (re-prompt or schema relaxation) before being flagged for manual review.

### Stage 6 — OpenAI wiki compilation

Extracted records are compiled into Markdown wiki pages:

1. **Source pages** — one per manual, with metadata, section index, extraction quality notes
2. **Compiled topic pages** — merged procedures, specs, troubleshooting across a bucket
3. **Family overview pages** — high-level pages per product family or model group
4. **Contradiction note pages** — competing claims with sources and precedence

All pages carry frontmatter with generation metadata.

### Stage 7 — Review and publish

Compiled output is staged in `data/publish/` and then pushed as a PR into the FlowCommander `repo-wiki/knowledge/` subtree. See [publish-contract.md](publish-contract.md) for the full specification.

## Data model

Core entities (filesystem-first, database-indexable later):

| Entity | Description |
|---|---|
| `document` | A registered source manual |
| `document_version` | A specific revision of a document |
| `bucket_assignment` | Links a document to its buckets |
| `parse_run` | One execution of a parser against a document |
| `section` | A typed chunk of parsed content |
| `extracted_record` | A structured record from LLM extraction |
| `contradiction_candidate` | Two records that may conflict |
| `compiled_page` | A generated wiki page |
| `publish_run` | One execution of the publish workflow |

## Orchestration

Prefect manages pipeline orchestration:

- Each stage is a Prefect flow or task
- Flows can be run individually or chained
- Batch processing is coordinated through Prefect
- State is tracked via manifest status fields and artifact existence on disk

## Technology stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Orchestration | Prefect |
| OCR | OCRmyPDF |
| Primary parser | Docling |
| Fallback parser | MinerU or Marker |
| LLM inference | OpenAI API (direct + batch) |
| Wiki output | Markdown with YAML frontmatter |
| Publish | Git + GitHub API (PR-based) |
| Config | YAML / env files |
| Testing | pytest |
| Linting | ruff |

## Non-goals for v1

- No user-facing web app
- No hosted Supabase in the digestion loop
- No raw embeddings as source of truth
- No automatic contradiction resolution
- No direct writes to FlowCommander main branch
