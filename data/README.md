# Knowledge Forge Data Directory

`data/` is the local working area for pipeline artifacts. Its contents stay
gitignored by default except for this README and the `.gitkeep` files that
preserve the agreed directory layout.

This layout follows [docs/repo-structure.md](../docs/repo-structure.md), which
is the source of truth for artifact paths and naming conventions.

## Directory layout

- `manifests/` stores intake manifests and related registration metadata.
- `raw/` stores original source PDFs or other source files.
- `normalized/` stores OCR-normalized or otherwise cleaned source files plus
  `*.meta.json` metadata for each normalization run.
- `parsed/` stores parser outputs for each document under `parsed/{doc_id}/`.
- `sections/` stores canonical section JSON files under `sections/{doc_id}/`.
- `extraction_runs/` stores durable extraction-run checkpoints under
  `extraction_runs/{run_id}.json`.
- `extracted/` stores extraction records under
  `extracted/{doc_id}/{record_type}/`.
- `compiled/` stores generated wiki artifacts. The repo structure doc reserves
  subpaths such as `source-pages/`, `topic-pages/`, `overview-pages/`, and
  `contradiction-notes/`.
- `publish/` stores staged publish-ready output under `publish/{publish_run_id}/`
  before any PR-based handoff to FlowCommander.
- `inference_logs/` stores logged inference requests and responses, organized by
  date and run.
- `evaluation/` stores evaluation harness outputs and review artifacts.

## Naming conventions

- `doc_id`: `{manufacturer}-{family}-{doc_type}-{revision}` slugified
- `section_id`: `{doc_id}--{section_type}--{sequence}`
- `record_id`: `{section_id}--{record_type}--{sequence}`
- `run_id`: `er-{YYYYMMDD}-{sequence}`
- `publish_run_id`: `kf-{YYYYMMDD}-{sequence}`

Examples from the repo structure doc:

- `doc_id`: `honeywell-dc1000-service-manual-rev3`
- `section_id`: `honeywell-dc1000-service-manual-rev3--maintenance--003`
- `record_id`:
  `honeywell-dc1000-service-manual-rev3--maintenance--003--procedure--001`
- `publish_run_id`: `kf-20240115-001`

## Working rules

- Keep generated artifacts in `data/` until there is an explicit publish flow.
- Do not write directly into the local FlowCommander clone as a side effect of
  normal Knowledge Forge runs.
- Preserve provenance and reviewability so reruns and PR-based publication stay
  auditable.
- Treat `extraction_runs/` as the source of truth for extraction progress during
  long or resumable runs. Successful item reuse depends on the persisted
  fingerprint stored there, and output replacement for one `section_id +
  record_type` now removes stale record files and superseded review flags.
