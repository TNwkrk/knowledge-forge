# Knowledge Forge — Publish Contract

## Overview

This document defines the boundary between Knowledge Forge and FlowCommander.
Knowledge Forge never directly mutates FlowCommander. All output flows through
a staged publish step that opens a pull request for human review.

This is both a GitHub workflow rule and a local working rule. Access to a
local FlowCommander clone is for inspection and publish preparation, not for
casual hand-edits from intermediate output.

## Authoritative target contract

**The downstream shape is owned by FlowCommander, not Knowledge Forge.** The
canonical contract for directory layout, frontmatter, and body structure of
published pages lives in the FlowCommander repo at:

`docs/knowledge-forge-publish-contract.md` in `TNwkrk/FlowCommander`

Local reference path when the downstream clone is available:
`/Users/taylor/development/FlowCommander/docs/knowledge-forge-publish-contract.md`

Knowledge Forge conforms to that contract. This document summarizes the
implications for this repository and must stay aligned with the FlowCommander
contract. If the two drift, the FlowCommander contract wins.

## Target repository

- **Repo:** `TNwkrk/FlowCommander` (or the configured target)
- **Branch:** PRs are opened against the default branch
- **Subtree:** `repo-wiki/knowledge/`

Knowledge Forge only writes inside `repo-wiki/knowledge/`. It never touches
any other FlowCommander path.

## Target folder structure

The downstream target is the FlowCommander digest-type taxonomy:

```
repo-wiki/
  knowledge/
    controllers/              # controller family / model field guidance
    fault-codes/              # panel indicators and alarm-code references
    symptoms/                 # symptom-first troubleshooting pages
    workflow-guidance/        # SOPs, PM, startup/shutdown, seasonal procedures
    contradictions/           # contradiction pages with competing claims
    supersessions/            # supersession notices
    source-index/             # one provenance page per source document
    _manifests/               # {publish_run_id}.json per run
    _publish-log/             # append-only publish log per run
    _sources/                 # per-source metadata snapshots
```

> **Deprecated internal taxonomy.** Earlier iterations of this document
> described a `manufacturers/`, `procedures/`, `specs/`, `troubleshooting/`,
> `parts/`, `safety/` target layout. That taxonomy is a **compile-stage
> intermediate under `data/compiled/`, not a publish target.** The publish
> stage must re-compile into the digest-type directories above. See the
> FlowCommander contract for the mapping (for example: procedure records →
> `workflow-guidance/`; alarm definitions → `fault-codes/`; troubleshooting
> entries → `symptoms/`; spec values fold into the relevant `controllers/`
> page).

## Slug conventions

- Lowercase, hyphen-separated
- Derived from manifest fields, not invented
- Stable across reruns (same input → same slug)

## File format

Auto-generated Markdown files include YAML frontmatter conforming to the
FlowCommander digest schema plus the Knowledge Forge provenance fields:

```yaml
---
title: "Human readable title"
digest_type: controller | fault-code | symptom | workflow-guidance | contradiction | supersession
slug: stable-slug
status: draft | approved | superseded
source_documents:
  - title: "Source document title"
    attachment_id: "uuid-or-null"
    locator: "page 12, section 4.3"
knowledge_record_ids: []
tags: []
cross_links: []
generated_by: knowledge-forge
publish_run: "{publish_run_id}"
generated_at: "2026-01-15T10:30:00Z"
extraction_version: "{version}"
compilation_version: "{version}"
# plus page-type specific add-ons: controller_models, fault_code, symptom_key,
# workflow_key, contradiction_key, superseded_slug, replacement_slug, etc.
---
```

Digest body sections, in order:

1. `## Summary`
2. `## Field Guidance`
3. `## Source Citations`
4. `## Related Pages`

Source-index pages follow the source-index frontmatter defined in the
FlowCommander contract.

## Publish manifest

Each publish run produces `_manifests/{publish_run_id}.json`:

```json
{
  "publish_run_id": "kf-YYYYMMDD-NNN",
  "generated_at": "2026-01-15T10:30:00Z",
  "knowledge_forge_version": "x.y.z",
  "source_documents": ["doc-001", "doc-002"],
  "buckets": ["bucket-id"],
  "files_written": ["controllers/allen-bradley-controllogix-family.md"],
  "files_updated": [],
  "files_removed": [],
  "extraction_version": "...",
  "compilation_version": "..."
}
```

## PR conventions

- Branch: `knowledge-forge/{publish_run_id}`
- Title: `[Knowledge Forge] Publish {bucket_description}`
- Labels: `knowledge-forge`, `auto-generated`
- Body: summary, source documents, counts of new/updated/removed files, link
  to the publish manifest, any warnings

## Validation rules

Before opening a PR, the publish workflow validates:

1. All files under `repo-wiki/knowledge/`
2. Only the allowed directories listed above are used
3. Digest pages include all required frontmatter (schema fields + generated-by
   provenance fields + page-type add-ons)
4. Digest pages include the four required body sections in order
5. Slugs match expected derivation from manifest fields
6. No two files claim the same canonical identity
7. `files_removed` paths were previously published by Knowledge Forge
8. Pages without `generated_by: knowledge-forge` are not overwritten

## Rollback guidance

If a published PR needs to be reverted:

1. Revert the merge commit in FlowCommander
2. `_publish-log/` records exactly what was added, updated, or removed
3. Re-publish from the same or corrected extraction by creating a new
   publish run

## Idempotency

Re-running publish for the same extraction outputs produces identical file
content. Publish run IDs are unique; file contents are deterministic given the
same extraction input.

## Current implementation notes

The compile-stage directories under `data/compiled/` may still use the older
document-provenance-first layout (`overview-pages/`, `topic-pages/`,
`contradiction-notes/`). That is intentional and remains an internal
intermediate only.

The publish boundary now does the downstream rewrite explicitly:

- `source-pages/` stage into `source-index/`
- `topic-pages/` stage into the digest taxonomy under `controllers/`,
  `fault-codes/`, `symptoms/`, and `workflow-guidance/`
- `contradiction-notes/` stage into `contradictions/`
- `overview-pages/` remain internal and are not staged into
  `repo-wiki/knowledge/`

`kf publish validate` now validates the staged output against the FlowCommander
digest contract rather than the deprecated Knowledge Forge internal taxonomy.

Known limitation to keep explicit for future reruns:

- Some digest metadata is still inferred from current compile-stage bucket/topic
  pages rather than from a final one-record-per-digest compiler. This is enough
  to validate and stage the correct downstream shape for publish-boundary work,
  but it does not by itself claim that the future rerun has perfect final digest
  granularity.
