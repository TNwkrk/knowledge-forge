# Knowledge Forge — Roadmap

## Source material coverage

The pipeline must support the full field-service knowledge corpus, not just OEM
manuals. See `README.md § Source material scope` for the complete taxonomy.
Phases below should be read with that broader scope in mind:

- **Phases 1–4** are format-agnostic in design but PDF-first in current implementation. As operational document classes (SOPs, checklists, inspection and service forms, drawings, controller screenshots, correspondence) begin arriving from FlowCommander promotions, each intake and parsing stage must extend — not gate — on them.
- **Phase 6** extraction schemas currently target manual-style records. Schemas for operational documents (SOPs, checklists, safety/permit records, inspection forms, service forms) are in scope from intake forward, not deferred to a "future" phase.
- **Phase 10** first-corpus onboarding should include at least one non-manual source family (e.g. an SOP, inspection form, or checklist) to validate broader intake coverage.

## Two intake paths

Knowledge Forge accepts source material along two intake paths, both of which
must be supported by the pipeline:

1. **Curated source packs** (bootstrap + controlled operator corpus) —
   `config/source-packs/*.yaml` manifests manually assembled by the Knowledge
   Forge operator. This is the current primary path and how the Rockwell
   corpus onboards.
2. **Promoted candidate source packs from FlowCommander** (steady state) —
   explicit editorial promotions pushed by FlowCommander office / admin staff.
   These carry FC-side scope metadata (customer / site / station / work-order
   context), document-class hints, promoter rationale, and back-references to
   FlowCommander artifact IDs. Knowledge Forge applies guardrails and may
   reject or downgrade low-signal promotions, with rejections surfaced back to
   the FC promoter.

The FlowCommander-side half of this contract is canonical in
[`FlowCommander/docs/operational-intake-model.md`](https://github.com/TNwkrk/FlowCommander/blob/main/docs/operational-intake-model.md).
The Knowledge Forge-side ingest contract for promoted packs is tracked as its
own workstream in the phase table below.

## Scoped publishing is a future extension

The current publish contract covers global reference material only (controller
family, fault codes, symptoms, workflow guidance, contradictions, supersessions,
source-index). Customer / site / station / asset-scoped publishing is a
**future extension** of the publish contract, not an assumed capability. Do not
generate scoped pages until the FlowCommander-side contract is extended.

## Phased implementation plan

Each phase maps to one or more GitHub epics. Issues within each phase are sequenced so they can be worked one at a time without guessing context.

Each issue should also carry a concrete acceptance signal before implementation
starts. Prefer one or more of:
- commands that must pass
- schema or contract validation
- expected generated artifact shape
- docs-consistency checks when the issue is primarily operational or structural

If a future issue cannot name an executable check yet, its acceptance criteria
should still say what will be inspected or compared to determine completion.

---

### Phase 1 — Repo bootstrap and architecture foundation

**Epic A**

Goal: A working Python project with src layout, dev tooling, CI, and data directory conventions. All planning docs committed.

| Issue | Summary |
|---|---|
| A-1 | Initialize Python project with pyproject.toml, src layout, and core dependencies |
| A-2 | Add dev tooling: ruff, pytest, pre-commit config |
| A-3 | Add GitHub Actions CI for lint and test |
| A-4 | Add data directory conventions, .gitignore patterns, and env example |

Exit criteria: `pip install -e .` works, CI passes on empty test suite, data dirs are documented.

---

### Phase 2 — Intake manifest and pre-bucketing

**Epic B**

Goal: Source documents can be registered, checksummed, and assigned to buckets before any heavy processing. The manifest schema supports the full document type vocabulary (manuals, bulletins, SOPs, checklists, datasheets, drawings, field forms, training material, and others).

| Issue | Summary |
|---|---|
| B-1 | Define manifest schema and document data model |
| B-2 | Build manual import CLI (register, list, inspect) |
| B-3 | Define bucket taxonomy and implement automatic assignment |
| B-4 | Add checksum deduplication and intake rerun safety |

Exit criteria: A PDF can be registered, manifested, bucketed, and re-registered without duplication. The manifest schema supports `document_type` values beyond just manuals.

---

### Phase 3 — OCR and normalization

**Epic C**

Goal: Scanned PDFs are normalized with OCRmyPDF before parsing. Metadata is tracked.

| Issue | Summary |
|---|---|
| C-1 | Integrate OCRmyPDF normalization pipeline |
| C-2 | Add OCR metadata tracking and selective OCR bypass |

Exit criteria: A scanned PDF produces a text-layer-enhanced normalized PDF with metadata recorded.

---

### Phase 4 — Layout-aware parsing

**Epic D**

Goal: Docling (primary) and a fallback parser produce structured parse artifacts.

| Issue | Summary |
|---|---|
| D-1 | Integrate Docling as primary parser with structured output |
| D-2 | Define parse artifact format and quality scoring |
| D-3 | Add fallback parser lane (MinerU or Marker) |
| D-4 | Implement canonical sectioning logic |

Exit criteria: A normalized PDF produces markdown, structured JSON, heading tree, tables, page map, and quality score. Sections are typed and bounded. Section types include safety, installation, configuration, startup, shutdown, maintenance, troubleshooting, specifications, parts, revision notes, workflow/SOP/checklist, inspection/commissioning, wiring/drawings/diagrams, and addenda/bulletins.

---

### Phase 5 — OpenAI inference foundation

**Epic E**

Goal: A robust OpenAI client layer with config, logging, cost tracking, retry, and batch support.

| Issue | Summary |
|---|---|
| E-1 | Build OpenAI client wrapper with config and secrets management |
| E-2 | Add request logging, token tracking, and cost accounting |
| E-3 | Build batch JSONL builder and batch submission workflow |
| E-4 | Add batch polling, result ingestion, and error reconciliation |

Exit criteria: A prompt can be sent in direct mode with logged cost. A batch of prompts can be submitted, polled, and ingested with error handling.

---

### Phase 6 — Extraction schemas and structured extraction engine

**Epic F**

Goal: Parsed sections are converted into schema-validated canonical records with provenance.

| Issue | Summary |
|---|---|
| F-1 | Define JSON schemas for all extraction record types |
| F-2 | Build section-to-record extraction engine with prompt templates |
| F-3 | Add schema validation, repair path, and confidence scoring |
| F-4 | Attach provenance metadata to all extraction records |

Exit criteria: A parsed section produces valid extraction records with full provenance. Invalid outputs are repaired or flagged.

---

### Phase 7 — LLM wiki compilation engine

**Epic G**

Goal: Extracted records are compiled into Markdown wiki pages.

| Issue | Summary |
|---|---|
| G-1 | Build source page generator (one page per manual) |
| G-2 | Build compiled topic page generator with cross-source citations |
| G-3 | Build family overview and index page generator |
| G-4 | Add contradiction note rendering to wiki pages |

Exit criteria: A bucket of extracted records produces source pages, topic pages, overview pages, and contradiction notes in Markdown with frontmatter.

---

### Phase 8 — Publish contract and FlowCommander PR integration

**Epic H**

Goal: Compiled wiki output is validated and published into FlowCommander via PR.

| Issue | Summary |
|---|---|
| H-1 | Implement publish contract validation and target folder structure |
| H-2 | Build Git and GitHub PR publish workflow |
| H-3 | Add publish manifests and logging |

Exit criteria: Compiled output is staged, validated against the publish contract, and pushed as a PR into the FlowCommander `repo-wiki/knowledge/` subtree.

---

### Phase 9 — Contradiction and supersession workflow

**Epic I**

Goal: Records within a bucket are compared for contradictions and supersession.

| Issue | Summary |
|---|---|
| I-1 | Define contradiction candidate schema and bucket-scoped comparison rules |
| I-2 | Implement supersession analysis with precedence metadata |
| I-3 | Render contradiction review outputs and add review hooks |

Exit criteria: Overlapping records within a bucket produce contradiction and supersession candidates with precedence metadata and review-ready output.

---

### Phase 10 — Evaluation and first corpus onboarding

**Epic J**

Goal: Quality tooling exists and the first real manufacturer bucket runs end-to-end. At least one non-manual source family (SOP, checklist, or field form) is also tested.

| Issue | Summary |
|---|---|
| J-1 | Build parser evaluation harness with benchmark fixtures |
| J-2 | Build extraction evaluation harness |
| J-3 | Onboard first real manufacturer bucket end-to-end |

Exit criteria: Parser and extraction quality are measurable. A real manufacturer bucket produces a publish-ready PR into FlowCommander. Evaluation fixtures include at least one non-manual document type.

---

## Build sequence

```
Phase 1 ──► Phase 2 ──► Phase 3 ──► Phase 4
                                       │
                                       ▼
                          Phase 5 (can overlap with Phase 3-4)
                                       │
                                       ▼
                          Phase 6 ──► Phase 7 ──► Phase 8
                                       │
                                       ▼
                                    Phase 9
                                       │
                                       ▼
                                    Phase 10
```

Phase 5 (OpenAI inference foundation) can be started as soon as the repo is bootstrapped. It does not depend on the parser being complete, only on having the client and batch infrastructure ready before extraction begins.

## v1 acceptance criteria

Knowledge Forge v1 is complete when:
- A manual can be registered and bucketed
- A manual can be normalized and parsed into structured artifacts
- The OpenAI extraction pipeline converts parsed sections into valid canonical records with provenance
- The OpenAI compilation pipeline generates human-readable wiki pages
- The system opens a clean PR into FlowCommander `repo-wiki`
- Contradiction candidates are surfaced inside relevant buckets
- Failed batches and invalid outputs can be retried without corrupting prior approved outputs
