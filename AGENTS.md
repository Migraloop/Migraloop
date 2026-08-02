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

Cloud `install`/`start` (`.cursor/environment.json`) set up nested-friendly Docker for **Local Sync Lab**:

- `install`: `cargo fetch` plus `.cursor/cloud-dind-install.sh` — installs `docker.io`, Compose v2, and `fuse-overlayfs`; writes `.cursor/daemon.json` (`storage-driver: fuse-overlayfs`, containerd snapshotter disabled); pre-warms Lab images (`postgres:16`, `mongo:7`, `gvenzl/oracle-free:23-slim`).
- `start`: `.cursor/cloud-dind-start.sh` — starts `dockerd` with that recipe (Cloud VMs have no systemd Docker unit).

Default overlay/overlayfs DinD fails Lab image extract with whiteout `EPERM`. Do not invent session-local storage-driver workarounds; use the baked recipe. After `start`, `migraloop lab up` / `lab status` should yield a ready Lab Fixture. Matrix evidence (pass/fail per recipe): implementing PR for GitHub issue #107.

Keep heavy one-off app builds out of `install` (Lab images pre-warm is intentional). Agents can work directly with committed skills under `.agents/skills/`.
