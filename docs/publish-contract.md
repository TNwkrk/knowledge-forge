# Knowledge Forge — Publish Contract

## Overview

This document defines the boundary between Knowledge Forge and FlowCommander. Knowledge Forge never directly mutates FlowCommander. All output flows through a staged publish step that opens a pull request for human review.

## Target repository

- **Repo:** `TNwkrk/FlowCommander` (or the configured target)
- **Branch:** PRs are opened against the default branch
- **Subtree:** `repo-wiki/knowledge/`

Knowledge Forge only writes inside `repo-wiki/knowledge/`. It never touches any other FlowCommander path.

## Target folder structure

```
repo-wiki/
  knowledge/
    manufacturers/
      {manufacturer_slug}/
        _index.md                    # manufacturer overview
        {family_slug}/
          _index.md                  # family overview
          {model_slug}/
            _index.md                # model overview
    procedures/
      {procedure_slug}.md            # compiled procedure pages
    specs/
      {spec_slug}.md                 # compiled spec pages
    troubleshooting/
      {topic_slug}.md                # compiled troubleshooting pages
    source-index/
      {doc_id}.md                    # one page per source manual
    _manifests/
      {publish_run_id}.json          # publish manifest for this run
    _sources/
      {doc_id}.json                  # source metadata snapshot
    _publish-log/
      {publish_run_id}.json          # log of what was published and when
```

## Slug conventions

- Lowercase, hyphen-separated: `honeywell`, `grundfos-cr-series`
- Derived from manifest fields, not invented
- Stable across reruns (same input → same slug)

## File format

All published Markdown files include YAML frontmatter:

```yaml
---
title: "Page Title"
generated_by: knowledge-forge
publish_run: "{publish_run_id}"
source_documents:
  - doc_id: "{doc_id}"
    revision: "{revision}"
    manufacturer: "{manufacturer}"
    family: "{family}"
generated_at: "2024-01-15T10:30:00Z"
extraction_version: "{version}"
compilation_version: "{version}"
---
```

## Publish manifest

Each publish run produces a manifest at `_manifests/{publish_run_id}.json`:

```json
{
  "publish_run_id": "kf-20240115-001",
  "generated_at": "2024-01-15T10:30:00Z",
  "knowledge_forge_version": "0.1.0",
  "source_documents": ["doc-001", "doc-002"],
  "buckets": ["honeywell/dc1000"],
  "files_written": [
    "manufacturers/honeywell/dc1000/_index.md",
    "source-index/doc-001.md"
  ],
  "files_updated": [],
  "files_removed": [],
  "extraction_version": "0.1.0",
  "compilation_version": "0.1.0"
}
```

## PR conventions

### Branch naming

```
knowledge-forge/{publish_run_id}
```

Example: `knowledge-forge/kf-20240115-001`

### PR title

```
[Knowledge Forge] Publish {bucket_description}
```

Example: `[Knowledge Forge] Publish Honeywell DC1000 service manual extraction`

### PR body

The PR body includes:
- Summary of what was processed
- List of source documents
- Count of new, updated, and removed files
- Link back to the publish manifest
- Any warnings (low confidence, contradiction candidates)

### Labels

PRs are labeled with:
- `knowledge-forge`
- `auto-generated`

## Validation rules

Before opening a PR, the publish workflow validates:

1. **Scoped output** — all files are under `repo-wiki/knowledge/`
2. **No orphan removals** — files are only removed if they were previously published by Knowledge Forge (tracked via publish manifests)
3. **Frontmatter present** — every Markdown file has valid YAML frontmatter with required fields
4. **Slug stability** — slugs match the expected derivation from manifest fields
5. **No duplication** — no two files claim the same canonical identity

## Rollback guidance

If a published PR needs to be reverted:

1. Revert the merge commit in FlowCommander
2. The publish log in `_publish-log/` records exactly what was added, updated, or removed
3. Knowledge Forge can re-publish from the same or corrected extraction by creating a new publish run

## Idempotency

Re-running the publish workflow for the same extraction outputs produces the same PR content. Publish run IDs are unique, but file content is deterministic given the same extraction input.

## Future considerations

- Automated PR approval workflows based on confidence thresholds
- Webhook notification to FlowCommander on publish
- Supabase sync after PR merge (owned by FlowCommander, not Knowledge Forge)
