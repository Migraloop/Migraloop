## Summary

<!-- What changed and why -->

## Handbook checklist

- [ ] Matching handbook pages updated in **all three locales** (`handbook/en`, `handbook/zh-TW`, `handbook/zh-CN`), or this change has no Operator/Developer-visible doc impact
- [ ] If the Operator CLI subcommand surface changed: refreshed `ci/handbook/cli-surface.txt` **and** `cli-and-config.md` in all three locales
- [ ] If extending a high-signal surface: added/updated rows in `ci/handbook/touchpoints.json`
- [ ] If using the `docs-not-needed` exemption: added the PR label **and** a rationale line in the PR body:
      `docs-not-needed: <why there is no Operator/Developer-visible doc impact>`

## Test plan

Release Quality Gate (every PR): Handbook guard + `rqg-unit` + `rqg-integration` + `rqg-perf`. Lab Scenarios stay manual—do not run the Lab catalog as CI. Local env parity and the capability → Lab Scenario → CI twin ladder: `handbook/*/developer-local-setup.md`.

- [ ] `cargo test --workspace --exclude migraloop-app --exclude handbook-guard` (`rqg-unit`)
- [ ] With CI-parity Postgres/Mongo (`MIGRALOOP_TEST_ADMIN_URL`, `MIGRALOOP_TEST_MONGO_HOST` / `PORT`): `cargo test -p migraloop-app` (`rqg-integration`)
- [ ] When touching Sync→Delivery / perf-sensitive paths: `bash ci/rqg/run_direct_pipeline_microbench.sh` (`rqg-perf`)
- [ ] `cargo test -p handbook-guard`
- [ ] `git diff --name-only origin/main...HEAD > /tmp/changed.txt && cargo run -p handbook-guard -- check --handbook handbook --changed-paths-file /tmp/changed.txt`
