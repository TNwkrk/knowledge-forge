# Codex Project Configuration

This folder contains repo-local Codex configuration for Knowledge Forge.

- `config.toml` sets project-scoped Codex defaults for this repository.
- Durable repo behavior belongs in checked-in repo guidance such as `AGENTS.md`
  and the docs in `docs/`.
- Keep this directory team-shareable. Do not commit secrets, tokens,
  machine-local overrides, or copied content from `~/.codex`.
- Put private or temporary Codex settings in your user-level Codex config
  instead of this folder.
