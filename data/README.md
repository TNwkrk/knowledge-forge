# Knowledge Forge Local Data Directory

This directory is the local working area for generated Knowledge Forge
artifacts. It is intentionally gitignored except for this README and
`.gitkeep`.

Current expectations:
- source files and parsed outputs stay local by default
- generated artifacts are staged here before any future publish workflow exists
- reviewable publish-ready content should be assembled here first, not written
  directly into the FlowCommander repository

Planned working subdirectories:
- `manifests/`
- `raw/`
- `normalized/`
- `parsed/`
- `sections/`
- `extracted/`
- `compiled/`
- `publish/`
- `inference_logs/`
- `evaluation/`

FlowCommander-facing guidance:
- The downstream publish target is FlowCommander `repo-wiki/knowledge/`.
- Use the local FlowCommander clone at
  `/Users/taylor/development/FlowCommander` for inspection when it exists.
- Preserve provenance and reviewability so future PR-based publishing can show
  where each generated artifact came from.
