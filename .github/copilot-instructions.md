# GitHub Copilot instructions for `TNwkrk/knowledge-forge`

This repo is a staged manual-digestion pipeline, not the FlowCommander product repo.
Optimize for correctness, provenance, rerun safety, and boundary discipline.

## What this repo owns
- manual intake and manifests
- pre-bucketing
- OCR and normalization
- parsing and sectioning
- OpenAI-backed extraction and wiki compilation
- contradiction and supersession analysis
- publish staging and PR-based publishing into FlowCommander

## Hard boundaries
- Work one roadmap issue at a time.
- Keep diffs tightly scoped to the issue's acceptance signal.
- Do not casually widen scope into later roadmap phases just because the structure is obvious.
- Preserve the Knowledge Forge -> FlowCommander boundary.
- Stage outputs in Knowledge Forge first.
- Publish to FlowCommander only by pull request.
- Do not treat local downstream FlowCommander access as permission to make casual downstream edits.
- Do not turn this repo into a generic app, retrieval system, or hosted runtime.

## Roadmap and phase discipline
- Read the issue, `AGENTS.md`, `README.md`, and the relevant docs before proposing implementation.
- Confirm the current roadmap phase and stay inside it.
- Flag phase-boundary violations explicitly.
- Flag hidden scope expansion explicitly.
- If a PR appears to deliver more than the issue or phase should allow, call that out.
- Prefer the smallest complete fix over a redesign.

## Publish-boundary discipline
- Knowledge Forge is the heavy digestion and compilation system.
- FlowCommander is the downstream publication target for approved outputs.
- Generated artifacts should flow through local staging and publish manifests before downstream publication.
- Review for accidental cross-repo writes, direct downstream mutations, or assumptions that bypass PR review.
- Review for publish-contract drift whenever `publish/`, staged artifacts, or downstream path assumptions change.

## Provenance, artifacts, and rerun safety
- Preserve provenance on extracted and compiled outputs.
- Be suspicious of changes that lose or weaken source linkage, page ranges, headings, revision context, parser version, extraction version, confidence, or bucket context.
- Review for idempotency and rerun safety at every stage.
- Prefer content-addressed or checksum-aware behavior over duplicate-prone behavior.
- Staged artifacts should be reproducible, reviewable, and safe to rerun.
- Approved outputs must not be overwritten casually.

## What to look for in review
Prioritize these over style nits:
- correctness bugs
- roadmap or phase sequencing violations
- accidental cross-repo boundary violations
- weak provenance handling
- missing validation for changed behavior
- contract drift between code, docs, and staged artifact shape
- idempotency or rerun-safety regressions
- docs or implementation mismatch
- hidden scope creep
- unsafe assumptions about manifests, parsing outputs, extraction outputs, compiled pages, publish staging, or downstream repo structure

## Testing and validation expectations
- Call out missing tests only when the changed behavior actually needs them.
- Prefer focused validation tied to the touched stage or contract.
- Report exactly what validation was run.
- Do not claim tests passed unless they were actually run.
- If only docs or scaffolding changed, say so plainly and use docs-consistency or smoke checks honestly.

## Guidance for follow-up fix passes
When asked to apply review findings:
- keep the fix set narrow and complete
- preserve the issue and phase boundary
- avoid opportunistic refactors unless they are required to make the fix safe
- update docs when behavior, structure, boundaries, or acceptance criteria changed
- keep PR summaries honest about what is fixed versus still out of scope

## Review tone
- prioritize correctness, maintainability, and boundary discipline over stylistic preferences
- do not flood reviews with low-value nitpicks
- flag material risk clearly and early
- when suggesting a fix, prefer the smallest complete fix that matches the current architecture and repo docs
