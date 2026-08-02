## Agent skills

### Issue tracker

Issues live in GitHub Issues via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`CONTEXT.md` + `docs/adr/` at repo root). See `docs/agents/domain.md`.

### Handbook

Operator/Developer product handbook duties (three locales, hard guards vs soft obligation). See `docs/agents/handbook.md`.

### Implement closeout (repo overlay)

When closing out **`/implement`**, if a PR already exists for the branch: work is not done until **every** check on that PR is green. Attribute reds before retrying; do not claim success without an explicit `CI: pass` / `CI: fail` / `CI: blocked` / `CI: pending-timeout` line. No PR → skip this gate. Do **not** patch upstream `.agents/skills/implement`. See `docs/agents/implement-ci-gate.md`.

## Cursor Cloud specific instructions

This repo currently has no application dependencies. Cloud agents can work directly with the committed skills under `.agents/skills/`.

When app dependencies are added, update `.cursor/environment.json`:

- `install`: run the repo's dependency setup (for example `pnpm install`)
- `start`: start any long-lived services the agent needs (for example `docker compose up -d`)

Keep heavy one-off builds out of `install`; document task-specific commands here instead.
