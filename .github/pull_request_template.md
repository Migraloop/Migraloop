## Summary

<!-- What changed and why -->

## Handbook checklist

- [ ] Matching handbook pages updated in **all three locales** (`handbook/en`, `handbook/zh-TW`, `handbook/zh-CN`), or this change has no Operator/Developer-visible doc impact
- [ ] If the Operator CLI subcommand surface changed: refreshed `ci/handbook/cli-surface.txt` **and** `cli-and-config.md` in all three locales
- [ ] If extending a high-signal surface: added/updated rows in `ci/handbook/touchpoints.json`
- [ ] If using the `docs-not-needed` exemption: added the PR label **and** a rationale line in the PR body:
      `docs-not-needed: <why there is no Operator/Developer-visible doc impact>`

## Test plan

- [ ] `cargo test -p handbook-guard`
- [ ] `git diff --name-only origin/main...HEAD > /tmp/changed.txt && cargo run -p handbook-guard -- check --handbook handbook --changed-paths-file /tmp/changed.txt`
