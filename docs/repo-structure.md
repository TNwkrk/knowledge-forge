# Knowledge Forge — Repository Structure and Artifact Flow

## Current committed baseline

The repository is currently in a bootstrap state. The checked-in baseline now
includes the Python package foundation, local developer tooling, and CI wiring
from Phase 1 Issues A-1 through A-3 plus the planning and operating docs that
were already present.

Committed today:

```
knowledge-forge/
├── AGENTS.md
├── README.md
├── .pre-commit-config.yaml
├── .github/
│   └── workflows/
│       └── ci.yaml
├── .codex/
│   ├── README.md
│   └── config.toml
├── pyproject.toml
├── .env.example
├── .gitignore
├── src/
│   └── knowledge_forge/
│       ├── __init__.py
│       ├── cli.py
│       ├── bucketing/
│       ├── compile/
│       ├── evaluation/
│       ├── extract/
│       ├── inference/
│       ├── intake/
│       │   ├── __init__.py
│       │   ├── importer.py
│       │   └── manifest.py
│       ├── normalize/
│       ├── parse/
│       └── publish/
├── tests/
│   ├── conftest.py
│   ├── fixtures/
│   ├── golden/
│   ├── test_intake_cli.py
│   └── test_package.py
├── data/
│   ├── .gitkeep
│   └── README.md
└── docs/
    ├── agent-workflow.md
    ├── architecture.md
    ├── codex-issue-runbook.md
    ├── evals.md
    ├── inference-layer.md
    ├── publish-contract.md
    ├── repo-structure.md
    └── roadmap.md
```

The package root, manifest schema, intake CLI, local lint/format hooks, and CI
quality gate are implemented today. Most deeper module paths below are still
the intended future structure, not already-implemented pipeline code.

## Repository layout

```
knowledge-forge/
├── AGENTS.md
├── README.md
├── .pre-commit-config.yaml
├── .codex/
│   ├── README.md
│   └── config.toml
├── pyproject.toml
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yaml
├── docs/
│   ├── agent-workflow.md
│   ├── architecture.md
│   ├── codex-issue-runbook.md
│   ├── evals.md
│   ├── roadmap.md
│   ├── publish-contract.md
│   ├── inference-layer.md
│   ├── repo-structure.md
│   ├── bucket-taxonomy.md          # defined in Epic B
│   └── extraction-schemas.md       # defined in Epic F
├── config/
│   ├── inference.yaml              # OpenAI model and rate config
│   ├── pipeline.yaml               # pipeline stage defaults
│   └── source-packs/               # checked-in source-pack manifests for repeatable onboarding
├── src/
│   └── knowledge_forge/
│       ├── __init__.py
│       ├── cli.py                   # CLI entry points
│       ├── intake/                  # manifest and import
│       │   ├── __init__.py
│       │   ├── manifest.py
│       │   └── importer.py
│       ├── bucketing/               # pre-bucketing logic
│       │   ├── __init__.py
│       │   ├── taxonomy.py
│       │   └── assigner.py
│       ├── normalize/               # OCR and normalization
│       │   ├── __init__.py
│       │   └── ocr.py
│       ├── parse/                   # layout-aware parsing
│       │   ├── __init__.py
│       │   ├── docling_parser.py
│       │   ├── fallback_parser.py
│       │   ├── quality.py
│       │   └── sectioning.py
│       ├── inference/               # OpenAI inference layer
│       │   ├── __init__.py
│       │   ├── client.py
│       │   ├── config.py
│       │   ├── logger.py
│       │   ├── batch.py
│       │   ├── retry.py
│       │   ├── cost.py
│       │   ├── schema_validator.py
│       │   └── prompts/
│       │       ├── extraction/
│       │       ├── compilation/
│       │       └── analysis/
│       ├── extract/                 # structured extraction engine
│       │   ├── __init__.py
│       │   ├── schemas/
│       │   ├── engine.py
│       │   ├── repair.py
│       │   └── provenance.py
│       ├── compile/                 # wiki compilation
│       │   ├── __init__.py
│       │   ├── source_pages.py
│       │   ├── topic_pages.py
│       │   ├── overview_pages.py
│       │   └── contradiction_notes.py
│       ├── publish/                 # PR-based publish
│       │   ├── __init__.py
│       │   ├── stage.py
│       │   ├── validate.py
│       │   ├── pr.py
│       │   └── manifest.py
│       └── evaluation/              # quality tooling
│           ├── __init__.py
│           ├── parser_eval.py
│           └── extraction_eval.py
├── tests/
│   ├── conftest.py
│   ├── test_intake/
│   ├── test_bucketing/
│   ├── test_normalize/
│   ├── test_parse/
│   ├── test_inference/
│   ├── test_extract/
│   ├── test_compile/
│   ├── test_publish/
│   ├── fixtures/
│   └── golden/
├── scripts/
│   └── README.md
└── data/                            # gitignored except conventions
    ├── .gitkeep
    ├── README.md                    # documents the data directory layout
    ├── manifests/                   # intake manifests and checksum index
    │   └── checksum-index.yaml
    ├── raw/                         # original source PDFs
    ├── normalized/                  # OCR-normalized PDFs and per-doc metadata JSON
    ├── parsed/                      # parser outputs per document
    │   └── {doc_id}/
    │       ├── content.md
    │       ├── structure.json
    │       ├── headings.json
    │       ├── tables.json
    │       ├── page_map.json
    │       ├── meta.json
    │       ├── quality.json
    │       └── runs/
    │           └── {parser}/
    │               ├── content.md
    │               ├── structure.json
    │               ├── headings.json
    │               ├── tables.json
    │               ├── page_map.json
    │               ├── meta.json
    │               └── quality.json
    ├── sections/                    # canonical sections
    │   └── {doc_id}/
    │       └── {section_id}.json
    ├── extraction_runs/             # durable extraction-run checkpoints
    │   └── {run_id}.json
    ├── extracted/                   # extraction records
    │   └── {doc_id}/
    │       ├── {record_type}/
    │       │   └── {record_id}.json
    │       └── reviews/
    │           └── {section_id}--{record_type}.json
    ├── compiled/                    # wiki compilation output
    │   ├── source-pages/
    │   ├── topic-pages/
    │   ├── overview-pages/
    │   └── contradiction-notes/
    │       ├── {bucket_slug}.md
    │       ├── {bucket_slug}-review.md
    │       └── {bucket_slug}-review-status.json
    ├── publish/                     # staged publish output
    │   └── {publish_run_id}/
    ├── inference_logs/              # inference request logs
    │   └── {date}/
    └── evaluation/                  # eval harness outputs
```

## Data directory conventions

The `data/` directory is gitignored (except for `.gitkeep` files and
`README.md`). It contains local pipeline artifacts and future publish staging
artifacts.

### Current practical use

Before the full pipeline exists, `data/` is still the right conceptual place
for:
- local manifests and source inputs
- parser and extraction outputs
- compiled working pages
- publish-ready staged artifacts destined for FlowCommander review

Agents should stage FlowCommander-facing output here first rather than writing
directly into the downstream repository during normal work.

Publish staging rewrites the internal compile intermediates into the downstream
FlowCommander digest taxonomy under `repo-wiki/knowledge/`. In particular,
older internal groupings such as manufacturer overviews and topic pages are not
the publish contract themselves; `src/knowledge_forge/publish/` is responsible
for mapping them into `controllers/`, `fault-codes/`, `symptoms/`,
`workflow-guidance/`, `contradictions/`, `supersessions/`, and `source-index/`
before PR handoff.

## Evaluation skeleton

The repository now includes a lightweight place for future fixture-driven
quality checks without pretending a full benchmark harness already exists.

- `docs/evals.md` explains how fixture and golden-file checks should be added
  over time
- `tests/fixtures/` is for committed source inputs and normalized intermediate
  samples used by parser or extraction tests
- `tests/golden/` is for expected structured outputs or Markdown snapshots that
  future tests can compare against

This skeleton is intentionally minimal. Add concrete fixtures only when an issue
needs them and the expected output shape is stable enough to review.

### Naming conventions

- **doc_id**: Derived from manifest fields. Format: `{manufacturer}-{family}-{doc_type}-{revision}` slugified. Example: `honeywell-dc1000-service-manual-rev3`. The `doc_type` segment reflects the full document type vocabulary (service-manual, installation-manual, bulletin, datasheet, parts-list, sop, checklist, drawing, field-form, training-material, etc.)
- **curated_bucket**: Optional manifest hint for a manufacturer-scoped cross-family bucket such as `Pump Station Control Stack`. This does not replace the real `family` value; it adds an extra bucket dimension when a reviewed source pack intentionally spans several product families.
- **section_id**: `{doc_id}--{slug(title)}--{digest}`. Example: `honeywell-dc1000-service-manual-rev3--maintenance-procedures--a1b2c3d4`. The title-derived slug may be truncated during generation.
- **record_id**: `{section_id}--{record_type}--{sequence}`. Example: `honeywell-dc1000-service-manual-rev3--maintenance-procedures--a1b2c3d4--procedure--001`
- **run_id**: `er-{YYYYMMDD}-{sequence}`. Example: `er-20260417-001`
- **publish_run_id**: `kf-{YYYYMMDD}-{sequence}`. Example: `kf-20240115-001`

### Artifact lifecycle

```
raw/{filename}.pdf
  → normalized/{doc_id}.pdf
    → parsed/{doc_id}/content.md + structure.json + ... + quality.json
      → sections/{doc_id}/{section_id}.json
        → extraction_runs/{run_id}.json
          → extracted/{doc_id}/{record_type}/{record_id}.json
            → compiled/{page_type}/{slug}.md
              → publish/{publish_run_id}/{path}.md
                → PR into FlowCommander
```

Each `extraction_runs/{run_id}.json` artifact stores the durable queue for one
extraction attempt across one or more documents. Items are keyed at the
`section_id + record_type` unit of work and persist the current checkpoint state
plus the input fingerprint required for safe reuse: `doc_id`, `section_id`,
section content hash, prompt/version, schema/version, and model. Full-document
manifests advance to `extracted` only after every required item for that
document is complete in the run. Replacing one work item's outputs now swaps in
the new generation and removes stale record files plus stale review flags from
the superseded generation.

### Canonical parse artifact contract

`data/parsed/{doc_id}/structure.json` is the parser-neutral contract for later stages.
It carries:
- `doc_id`, `parser`, `parser_version`, `page_count`
- normalized `texts` entries with `item_ref`, `label`, `text`, and `page_numbers`
- normalized `tables` entries with `item_ref`, `label`, `page_numbers`, `row_count`, `column_count`, and `data`
- normalized `pages` entries with `page_number`, dimensions, and source reference

`quality.json` stores the parser quality report with:
- per-metric scores for heading coverage, table extraction, text completeness, structure depth, and page coverage
- overall score and the configured acceptance threshold

When fallback parsing is attempted, the selected parser's artifacts stay at the
top level of `data/parsed/{doc_id}/`, and each candidate parser run is also
preserved under `data/parsed/{doc_id}/runs/{parser}/` for comparison.

### Idempotency

Each stage checks for existing artifacts before processing:
- If the output exists and the input hash matches, skip processing
- If the input has changed, reprocess and overwrite
- Approved outputs (post-publish) are never overwritten by reprocessing

### What gets committed to git

| Path | Git status |
|---|---|
| `AGENTS.md` | Committed |
| `.codex/` | Committed |
| `docs/` | Committed |
| `src/` | Committed |
| `tests/` | Committed |
| `config/` | Committed (no secrets) |
| `scripts/` | Committed |
| `data/` | Gitignored (except .gitkeep and README.md) |
| `.env` | Gitignored |
| `.env.example` | Committed |

## Configuration

### Environment variables

```
OPENAI_API_KEY=sk-...
KNOWLEDGE_FORGE_DATA_DIR=./data
FLOWCOMMANDER_REPO=TNwkrk/FlowCommander
FLOWCOMMANDER_REPO_PATH=/Users/taylor/development/FlowCommander
GITHUB_TOKEN=ghp_...
```

### Config files

- `config/inference.yaml` — OpenAI model settings, rate limits, batch config
- `config/pipeline.yaml` — stage-level defaults (OCR settings, parser selection, quality thresholds)

Repo-local agent defaults live in `.codex/config.toml`.

Config files are committed. Secrets come from environment variables only.
