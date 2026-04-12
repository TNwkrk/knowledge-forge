# Knowledge Forge — Repository Structure and Artifact Flow

## Repository layout

```
knowledge-forge/
├── README.md
├── pyproject.toml
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yaml
├── docs/
│   ├── architecture.md
│   ├── roadmap.md
│   ├── publish-contract.md
│   ├── inference-layer.md
│   ├── repo-structure.md
│   ├── bucket-taxonomy.md          # defined in Epic B
│   └── extraction-schemas.md       # defined in Epic F
├── config/
│   ├── inference.yaml              # OpenAI model and rate config
│   └── pipeline.yaml               # pipeline stage defaults
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
│   └── fixtures/
├── scripts/
│   └── README.md
└── data/                            # gitignored except conventions
    ├── .gitkeep
    ├── README.md                    # documents the data directory layout
    ├── manifests/                   # intake manifests
    ├── raw/                         # original source PDFs
    ├── normalized/                  # OCR-normalized PDFs
    ├── parsed/                      # parser outputs per document
    │   └── {doc_id}/
    │       ├── content.md
    │       ├── structure.json
    │       ├── headings.json
    │       ├── tables.json
    │       ├── page_map.json
    │       └── meta.json
    ├── sections/                    # canonical sections
    │   └── {doc_id}/
    │       └── {section_id}.json
    ├── extracted/                   # extraction records
    │   └── {doc_id}/
    │       └── {record_type}/
    │           └── {record_id}.json
    ├── compiled/                    # wiki compilation output
    │   ├── source-pages/
    │   ├── topic-pages/
    │   ├── overview-pages/
    │   └── contradiction-notes/
    ├── publish/                     # staged publish output
    │   └── {publish_run_id}/
    ├── inference_logs/              # inference request logs
    │   └── {date}/
    └── evaluation/                  # eval harness outputs
```

## Data directory conventions

The `data/` directory is gitignored (except for `.gitkeep` files and `README.md`). It contains all pipeline artifacts.

### Naming conventions

- **doc_id**: Derived from manifest fields. Format: `{manufacturer}-{family}-{doc_type}-{revision}` slugified. Example: `honeywell-dc1000-service-manual-rev3`
- **section_id**: `{doc_id}--{section_type}--{sequence}`. Example: `honeywell-dc1000-service-manual-rev3--maintenance--003`
- **record_id**: `{section_id}--{record_type}--{sequence}`. Example: `honeywell-dc1000-service-manual-rev3--maintenance--003--procedure--001`
- **publish_run_id**: `kf-{YYYYMMDD}-{sequence}`. Example: `kf-20240115-001`

### Artifact lifecycle

```
raw/{filename}.pdf
  → normalized/{doc_id}.pdf
    → parsed/{doc_id}/content.md + structure.json + ...
      → sections/{doc_id}/{section_id}.json
        → extracted/{doc_id}/{record_type}/{record_id}.json
          → compiled/{page_type}/{slug}.md
            → publish/{publish_run_id}/{path}.md
              → PR into FlowCommander
```

### Idempotency

Each stage checks for existing artifacts before processing:
- If the output exists and the input hash matches, skip processing
- If the input has changed, reprocess and overwrite
- Approved outputs (post-publish) are never overwritten by reprocessing

### What gets committed to git

| Path | Git status |
|---|---|
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
GITHUB_TOKEN=ghp_...
```

### Config files

- `config/inference.yaml` — OpenAI model settings, rate limits, batch config
- `config/pipeline.yaml` — stage-level defaults (OCR settings, parser selection, quality thresholds)

Config files are committed. Secrets come from environment variables only.
