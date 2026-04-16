# Knowledge Forge

Knowledge Forge is a local-first or self-hosted manual digestion system for turning technical manuals into trustworthy, reviewable knowledge artifacts.

Its job is to:
- register and classify manuals
- pre-bucket them before heavy processing
- normalize and parse technical PDFs
- use the OpenAI API to extract canonical knowledge records
- use the OpenAI API to compile human-readable wiki pages
- detect contradiction and supersession candidates
- publish approved wiki output into the FlowCommander `repo-wiki` by pull request

Knowledge Forge is a separate system from FlowCommander. Hosted Supabase may store approved outputs later for retrieval, but it is **not** responsible for the digestion pipeline.

## Codex-ready baseline

This repository includes a lightweight local operating baseline so future Codex
work can proceed without guessing repo conventions:
- `AGENTS.md` defines repo-specific agent workflow and integration rules
- `docs/codex-issue-runbook.md` defines the standard one-issue-at-a-time Codex
  execution loop
- `.codex/` contains repo-local Codex defaults
- `.env.example` documents shared local environment variables
- `data/README.md` defines the local staging area for generated artifacts
- `docs/agent-workflow.md` explains the Knowledge Forge to FlowCommander working
  model
- `docs/evals.md` defines the lightweight fixture and golden-file eval skeleton
- `config/inference.yaml` now includes model pricing used for request cost
  estimation and inference log summaries

The setup is intentionally light. Durable behavior lives in repo docs rather
than hidden local automation.

## Canonical entry points

Start here when orienting in the repo:
- `README.md` explains what Knowledge Forge is and how it differs from
  FlowCommander
- `AGENTS.md` defines operating rules, safe-edit guidance, and delivery
  expectations
- `docs/codex-issue-runbook.md` defines the standard issue execution loop for
  Codex work
- `docs/roadmap.md` defines the phased implementation plan and issue sequence
- `docs/publish-contract.md` defines the FlowCommander publish boundary
- `docs/repo-structure.md` defines the intended repository layout and artifact
  flow
- `docs/agent-workflow.md` defines the two-repo workflow and phase boundaries
- `docs/evals.md` defines where future fixture-driven validation should live

## Python project bootstrap

Issues `#1`, `#2`, and `#3` establish the canonical Python package, local
developer tooling, and CI baseline for this repo:

- `pyproject.toml` defines packaging metadata, runtime dependencies, and the
  development extras used today
- `.pre-commit-config.yaml` defines the shared local lint and format hooks
- `.github/workflows/ci.yaml` runs a fast preflight on all pull requests,
  including drafts, and runs full lint/test on pushes to `main` and pull
  requests that are ready for review
- `src/knowledge_forge/` is the package root for future implementation work
- `tests/` contains the initial smoke-test scaffold for imports and the CLI

### Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Local secrets with Infisical

Keep Python configuration environment-variable based. `OPENAI_API_KEY` should be
provided by Infisical at command runtime, not by committed files in this repo.
`.env.example` remains documentation only.

```bash
infisical login
infisical init
infisical run -- python -m pytest
infisical run -- python -c "import os; print(bool(os.environ['OPENAI_API_KEY']))"
infisical run -- python -m knowledge_forge.cli --help
```

If the project is already linked on your machine, `infisical init` may not be
needed again. The local Infisical link file is ignored by git.

### Current validation commands

```bash
ruff check .
ruff format --check .
python -m pytest
python -c "import knowledge_forge"
python -m knowledge_forge.cli --help
```

To run these with secrets available:

```bash
infisical run -- ruff check .
infisical run -- ruff format --check .
infisical run -- python -m pytest
infisical run -- python -c "import knowledge_forge"
infisical run -- python -m knowledge_forge.cli --help
```

## Core model

This system is **not**:

`PDF -> raw chunks -> embeddings -> done`

This system **is**:

`manual intake -> pre-bucket -> OCR/parse -> LLM structured extraction -> LLM wiki compilation -> review -> publish`

The parser recovers structure.
The OpenAI API converts that structure into canonical records and reviewable wiki output.

## Why this repo exists

FlowCommander needs a scalable way to digest a large backlog of manuals into knowledge that humans can inspect and that downstream AI systems can trust.

The existing incremental ingestion path is suitable for future uploads, but it is not the right shape for bulk backlog seeding. Knowledge Forge exists to own the heavier content-processing factory:
- intake and manifesting
- pre-bucketing
- OCR and parsing
- OpenAI-powered extraction
- OpenAI-powered wiki compilation
- contradiction and supersession analysis
- publish/export tooling

## Repository strategy

Knowledge Forge should remain separate from FlowCommander.

### Knowledge Forge owns
- manifest and intake
- pre-bucketing rules
- normalization and OCR
- parsing
- OpenAI inference layer
- extraction schemas
- wiki compiler
- publish tooling

### FlowCommander owns
- product app
- Ask AI consumption
- approved imported wiki content
- downstream retrieval and storage contracts

## Publish boundary

Knowledge Forge should not directly mutate FlowCommander during normal processing.

Required flow:
1. Knowledge Forge processes manuals
2. It generates compiled wiki artifacts
3. It stages publish-ready output
4. It opens a PR against FlowCommander
5. That PR writes into a dedicated `repo-wiki/knowledge/` subtree
6. Humans review and merge

That keeps the process auditable, reviewable, and reversible.

## Working with FlowCommander locally

When available, use the local FlowCommander clone at
`/Users/taylor/development/FlowCommander` as the downstream reference point.

Use it to:
- inspect current `repo-wiki/knowledge/` structure
- validate that generated artifacts fit downstream expectations
- prepare publish-ready changes before opening a PR

Do not treat local repo access as permission to casually edit FlowCommander from
intermediate output. The approval boundary remains the FlowCommander PR review
process.

## Goals

### Primary goals
- build a reliable backlog-digestion pipeline for manuals
- pre-bucket manuals before LLM processing
- use the OpenAI API for schema-bound extraction and wiki compilation
- produce a human-readable compiled wiki
- publish approved wiki artifacts into FlowCommander by PR
- preserve provenance and rerun safety
- leave room for future Ask AI integration

### Non-goals for v1
- no user-facing web app first
- no hosted Supabase digestion
- no raw chunk embeddings as source of truth
- no fully automatic contradiction resolution
- no silent direct writes into FlowCommander main

## Recommended technical direction

### Proposed v1 stack
- Python
- Prefect for orchestration
- OCRmyPDF for scan normalization
- Docling as primary parser
- MinerU or Marker as fallback parser
- OpenAI API for structured extraction and wiki compilation
- OpenAI Batch API for corpus-scale processing
- Markdown wiki compiler
- Git and PR publish flow into FlowCommander

## OpenAI inference layer

OpenAI inference is a first-class subsystem.

It should be used for:
- structured extraction of procedures, warnings, specs, troubleshooting entries, applicability rules, and revision notes
- normalization of parsed sections into canonical forms
- wiki compilation into readable Markdown pages
- contradiction and supersession candidate analysis inside scoped buckets

### Two inference modes
1. Direct request mode
   - prompt development
   - debugging
   - single-document testing

2. Batch mode
   - backlog runs
   - section-by-section extraction
   - corpus-scale wiki compilation
   - future embeddings

The system should support request logging, retry and repair, token and cost accounting, batch job building, batch result ingestion, and provenance attachment.

## High-level pipeline

### Stage 0: intake and manifest
Every manual gets a manifest entry with fields such as:
- source path
- checksum
- manufacturer
- family
- model applicability
- document type
- revision
- publication date
- language
- priority
- processing status

### Stage 1: pre-bucketing
Assign manuals to buckets before parsing or LLM extraction.

Core bucket dimensions:
- manufacturer
- product family
- model or applicability
- document type
- revision authority
- publication date or revision order

### Stage 2: OCR and normalization
Normalize scanned or inconsistent PDFs before parsing.

### Stage 3: parsing
Use a layout-aware parser first and save:
- markdown
- structured JSON
- heading tree
- table outputs
- page map
- parse metadata
- quality notes

### Stage 4: canonical sectioning
Split parsed content into meaningful sections such as:
- safety
- installation
- configuration
- startup
- shutdown
- maintenance
- troubleshooting
- specifications
- parts
- revision notes

### Stage 5: OpenAI structured extraction
Convert parsed sections into canonical records.

### Stage 6: OpenAI wiki compilation
Generate human-readable Markdown artifacts.

### Stage 7: review and publish
Stage outputs and open PRs into FlowCommander.

## Wiki output strategy

Generated wiki artifacts should eventually publish into FlowCommander under a dedicated subtree such as:

```text
repo-wiki/
  knowledge/
    manufacturers/
    families/
    procedures/
    specs/
    troubleshooting/
    source-index/
    _manifests/
    _sources/
    _publish-log/
```

### Page types
1. Source pages
   - one page per source manual
   - include metadata, revision, parser used, extraction quality notes, section index, and unresolved issues

2. Compiled topic pages
   - startup procedures
   - alarm references
   - specs
   - troubleshooting
   - include normalized content, applicability, citations, and contradiction notes

3. Family overview pages
   - controller family overviews
   - pump family overviews
   - model group overviews

## Data model concepts

Even if the first implementation is filesystem-first, the architecture should center on explicit entities:
- `document`
- `document_version`
- `bucket_assignment`
- `parse_run`
- `section`
- `extracted_record`
- `contradiction_candidate`
- `compiled_page`
- `publish_run`

Example record types:
- `procedure`
- `procedure_step`
- `warning`
- `spec_value`
- `alarm_definition`
- `troubleshooting_entry`
- `part_reference`
- `applicability`
- `revision_note`
- `supersession_candidate`
- `contradiction_candidate`

Every extracted record should preserve provenance:
- source document id
- source page range
- source heading
- revision or date
- parser version
- extraction version
- confidence
- bucket context

## Key docs

- `AGENTS.md` — repo-specific agent operating conventions
- `docs/agent-workflow.md` — two-repo working model and local workflow
- `docs/publish-contract.md` — downstream FlowCommander publish boundary
- `docs/repo-structure.md` — current scaffold and planned artifact layout

## Contradiction and supersession

Version 1 should not try to resolve contradictions across the whole corpus.
It should only compare records inside scoped buckets.

Suggested precedence:
1. service bulletin or addendum
2. revised manual
3. original manual
4. quick start or supplemental guide
5. non-authoritative material

Compare only when these overlap:
- manufacturer
- family
- applicability
- claim type
- topic subject

Outputs should include:
- contradiction candidate records
- supersession candidate records
- wiki notes showing competing claims and sources

## Initial roadmap shape

### Epic A: repo bootstrap and architecture foundation
- initialize Python project and dependency management
- add architecture docs
- add local config and env examples
- add CI baseline
- add artifact and data directory conventions

### Epic B: intake manifest and pre-bucketing
- define manifest schema
- build import CLI
- implement bucket taxonomy
- add manual override support
- add checksum and path dedupe

### Epic C: OCR and normalization
- integrate OCR pipeline
- record OCR metadata
- save normalized outputs
- support selective OCR
- add rerun safety

### Epic D: layout-aware parsing
- integrate Docling primary parser
- define parse artifact format
- add parser quality scoring
- add fallback parser lane
- record parse provenance

### Epic E: OpenAI inference foundation
- implement OpenAI client wrapper
- add model and config abstraction
- add secrets and env handling
- add request logging and cost tracking
- add retry and rate-limit handling
- add batch JSONL builder
- add batch submission and polling
- add output and error reconciliation

### Epic F: extraction schemas and structured extraction engine
- define JSON schemas for record types
- build section-to-record extraction prompts
- validate model outputs against schema
- add repair path for invalid output
- save extraction records with provenance
- add confidence and diagnostics

### Epic G: LLM wiki compilation engine
- generate source pages
- generate compiled topic pages
- generate family overview pages
- generate contradiction note pages
- add frontmatter and generation metadata

### Epic H: publish contract and FlowCommander PR integration
- define target folder contract
- generate publish manifests
- implement git and PR publish workflow
- validate scoped output
- add publish logs and rollback guidance

### Epic I: contradiction and supersession workflow
- define contradiction candidate schema
- implement bucket-scoped comparison rules
- render contradiction review outputs
- add precedence metadata
- add review hooks for future phases

### Epic J: evaluation and first corpus onboarding
- create parser benchmark fixture set
- build parser score rubric
- build extraction eval harness
- onboard first real manufacturer bucket
- produce first end-to-end publish PR into FlowCommander

## Suggested first build sequence
1. bootstrap repo, docs, CI, and config
2. manifest and pre-bucketing
3. OCR and parser benchmark harness
4. OpenAI inference foundation
5. extraction schemas and structured extraction
6. wiki compiler
7. publish integration
8. contradiction workflow and first real corpus rollout

## Acceptance criteria for v1
Knowledge Forge v1 is successful when:
- a manual can be registered and bucketed
- a manual can be normalized and parsed into structured artifacts
- the OpenAI extraction pipeline can convert parsed sections into valid canonical records
- those records preserve provenance
- the OpenAI compilation pipeline can generate human-readable wiki pages
- the system can open a clean PR into FlowCommander `repo-wiki`
- contradiction candidates are surfaced inside relevant buckets
- failed batches and invalid outputs can be retried without corrupting prior approved outputs

## Proposed repo shape

```text
knowledge-forge/
  README.md
  docs/
    architecture.md
    roadmap.md
    publish-contract.md
    bucket-taxonomy.md
    extraction-schemas.md
    inference-layer.md
  src/
    knowledge_forge/
      intake/
      bucketing/
      normalize/
      parse/
      sectioning/
      inference/
      extract/
      compile/
      publish/
      evaluation/
  tests/
  data/
    manifests/
    raw/
    normalized/
    parsed/
    extracted/
    compiled/
    publish/
  scripts/
  .github/
    workflows/
```

## For Opus

Use this repo to create the roadmap, epics, and incremental issue stack. Do not try to build everything in one issue. Break the work into clean, testable phases that Codex can ship with focused PRs.

## For Codex later

Work the next canonical roadmap issue in `TNwkrk/knowledge-forge`. Follow the architecture docs and publish contract. Make the smallest complete implementation that satisfies the issue cleanly, with tests and docs where needed. Preserve provenance, rerun safety, and the separation between digestion and FlowCommander publish integration.
