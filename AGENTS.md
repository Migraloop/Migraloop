## Agent skills

### Issue tracker

Issues live in GitHub Issues via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`CONTEXT.md` + `docs/adr/` at repo root). See `docs/agents/domain.md`.

## Cursor Cloud specific instructions

This repo currently has no application dependencies. Cloud agents can work directly with the committed skills under `.agents/skills/`.

When app dependencies are added, update `.cursor/environment.json`:

- `install`: run the repo's dependency setup (for example `pnpm install`)
- `start`: start any long-lived services the agent needs (for example `docker compose up -d`)

Keep heavy one-off builds out of `install`; document task-specific commands here instead.
