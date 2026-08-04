# Developer local setup

Clone, build, run the Platform Store, and exercise tests in this modular Rust monorepo. Product usage docs for Operators live in the other handbook chapters—this page is the Developer path.

## Prerequisites

- Rust toolchain matching `rust-toolchain.toml` (stable)
- Docker / Docker Compose (Platform Store and optional integration dependencies)
- Git
- **Optional (live Oracle Source):** Oracle Instant Client Basic or Basic Light on the machine that runs `migraloop`, with `LD_LIBRARY_PATH` pointing at the Instant Client directory. Required for real-host Initial Load and LogMiner (OCI); not needed for `host: contract` / `stub` CI slices. For Source TLS (TCPS), also mount an Instant Client wallet and set `spec.source.tls` (`enabled` + `walletLocation`)—see [Security](security.md).
- **Optional (contract/stub CI slices):** point `MIGRALOOP_CONTRACT_SOURCE_CATALOG` at a JSON file of harness catalog tables for schema discovery + Initial Load, and `MIGRALOOP_INJECT_LOGMINER_CONTENTS` at Incremental LogMiner contents when needed (optional `rs_id` / `ssn` ordering keys for same-SCN multi-row streams; see [Source System](source-system.md) / [CLI and config](cli-and-config.md)). Named scenario fixtures belong in those inject files for tests—not in the shipped product path.

## Clone and build

```bash
git clone https://github.com/Migraloop/Migraloop.git
cd Migraloop
cargo build -p migraloop-app
```

Workspace members: `crates/app` (binary `migraloop`), `cli`, `runtime`, `capture`, `platform-store`, `transform`, `delivery`, `types`, plus `ci/handbook` (Handbook guard).

## Platform Store via compose

```bash
docker compose up -d platform-store
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
# Local cleartext is fine. Production store TLS: add ?sslmode=require&sslrootcert=/path/to/ca.pem
cargo run -p migraloop-app -- migrate
cargo run -p migraloop-app -- status
```

Full two-container stack (store + app `run`):

```bash
docker compose up -d --build
```

Default compose credentials (`migraloop` / `migraloop`) are for local development only.

## Local Sync Lab Fixture

Disposable Oracle + MongoDB + Platform Store + app for **manual** Sync→Delivery verification (ADR-0025). Distinct from the **Release Quality Gate** / CI contract-stub harness: operators choose Lab Scenarios; do **not** treat the Scenario catalog as a CI suite or add a job that runs the entire catalog as a release gate.

```bash
cargo build -p migraloop-app
./target/debug/migraloop lab up
./target/debug/migraloop lab status   # Fixture ready + Scenario run active/leftover/(none)
./target/debug/migraloop lab scenario list
# Scenario apply/sync needs Instant Client: export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
./target/debug/migraloop lab scenario run rt-project
./target/debug/migraloop lab scenario run rt-filter
./target/debug/migraloop lab scenario run rt-field-ops
./target/debug/migraloop lab scenario run rt-equilookup
./target/debug/migraloop lab scenario run rt-union
./target/debug/migraloop lab scenario run rt-unwind
./target/debug/migraloop lab scenario run rt-distinct-addtoset
./target/debug/migraloop lab scenario run transform-pipeline
./target/debug/migraloop lab scenario run concurrent-source-workload
./target/debug/migraloop lab scenario run bulk-load
./target/debug/migraloop lab scenario run idempotent-redelivery
./target/debug/migraloop lab scenario run pause-resume
./target/debug/migraloop lab scenario run remove-pipeline
./target/debug/migraloop lab scenario run change-pipeline
./target/debug/migraloop lab scenario run poison-quarantine
./target/debug/migraloop lab scenario run schema-change-pause
./target/debug/migraloop lab scenario run source-alignment
./target/debug/migraloop lab scenario run drift-check
./target/debug/migraloop lab scenario run bounded-backpressure
./target/debug/migraloop lab scenario run observability-surface
./target/debug/migraloop lab scenario run platform-store-guardrails
./target/debug/migraloop lab scenario run initial-load-throttled
./target/debug/migraloop lab scenario run backward-compatible-upgrades
# lab status names leftover Namespace after keep-on-finish; also inspect with base / derived / target.
# Re-run wipes Namespace first; or: lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Defaults after bring-up: Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`, Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`, MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`. Lab Compose also injects those Lab-only secrets into the Fixture `app` for continuous Sync. Lab bring-up does not apply sample Deployments/Pipelines. Requires Docker Compose; `lab up` builds `target/debug/migraloop` when missing and packs it via `lab/Dockerfile` (Ubuntu 24.04 base for host glibc). Lab Compose uses `network_mode: host`. First Oracle boot can take several minutes. Nested Docker whiteout extract failures: use dockerd `storage-driver: fuse-overlayfs` or `vfs` (containerd snapshotter disabled). On **Cursor Cloud**, environment `install`/`start` already configures `fuse-overlayfs` and pre-warms Lab images—run `migraloop lab up` after the session starts. See [CLI & Config](cli-and-config.md) (`lab`) and [Deployment](deployment.md).

### DB-level restore / load escape hatch

For loads **outside** Scenario recipes (SQL/JS/dumps into Lab Oracle and/or Lab Mongo), use `lab/escape-hatch/` against the disposable Fixture connection details from `migraloop lab status`. This is not a Lab Scenario (`recipe.yaml` / `lab scenario run`) and not the Release Quality Gate.

```bash
./target/debug/migraloop lab up
docker compose -f lab/compose.yaml -p migraloop-lab exec -T oracle \
  sqlplus -s SYNC_USER/lab_oracle@FREEPDB1 < lab/escape-hatch/oracle-load.sql
docker compose -f lab/compose.yaml -p migraloop-lab exec -T mongo \
  mongosh --quiet --host 127.0.0.1 -u migraloop -p lab_mongo \
  --authenticationDatabase admin lab < lab/escape-hatch/mongo-load.js
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=lab_oracle MONGO_PASSWORD=lab_mongo
export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop apply -f lab/escape-hatch/deployment.yaml
./target/debug/migraloop status
./target/debug/migraloop base --table LAB_ESCAPE_CUSTOMERS
```

Operator-facing detail: [Deployment](deployment.md). CLI-seam coverage: always-on package checks plus ignored Fixture flow in `crates/app/tests/cli_lab_escape_hatch.rs`.

### Authoring a Lab Scenario (feature-time coverage)

Completeness ladder for a shipped first-class capability (ADR-0025 + ADR-0028): **capability → Lab Scenario → non-ignored contract-path CI twin**. A capability is incomplete until both the manual Lab Scenario and a Release Quality Gate twin exist. Use this repeatable path while building a feature:

1. Create `lab/scenarios/<id>/` with:
   - `recipe.yaml` — recipe-driven runner interface (also catalog metadata): `id`, `summary`, **Scenario Namespace** (`source_tables`, `target_collections`, `deployment`, `pipelines`), `workload` (`concurrency`: `serial`|`parallel`, ordered `steps`), `checks.correctness`, optional equal-weight `thresholds` (`max_settle_ms`, `max_lag`, `max_duration_ms`, `min_rows_per_s`)
   - `deployment.yaml` — real product Deployment config (same format Operators apply), bound only to Lab Fixture engines (`127.0.0.1` / `localhost` Oracle + Mongo endpoints from `migraloop lab status`). Scenario `run` refuses non-Lab / production engine targets before apply/sync.
2. Implement a Scenario adapter (Namespace prepare/Source workload/correctness/remove) in the `crates/cli/src/lab_scenario/` module and register the Scenario id. Recipe `workload` / `checks` / `thresholds` are the recipe-driven runner interface — do not duplicate threshold values as Rust constants.
3. Confirm `migraloop lab scenario list` shows the new id and **summary from `recipe.yaml`**. Selectable catalog = registered Scenario adapters that have both recipe + deployment files under `--lab-dir`.
4. Manually verify with `migraloop lab scenario run <id>` on a Lab Fixture. Keep always-on CLI-seam tests for list/control-plane behavior; full Fixture runs stay `#[ignore]` — not Release Quality Gate evidence.
5. Add a **non-ignored** contract-path CI twin under `crates/app/tests` (prefer extending existing CLI/`migraloop-app` seams on contract/stub + Platform Store/Mongo). Update the Lab↔CI matrix in `docs/rqg/CI_TWIN_COVERAGE.md`. Do **not** un-ignore Lab Scenario / Fixture / live Oracle tests to “satisfy” the gate, and do **not** add a CI job that runs the Lab Scenario catalog.

Recipe conventions and a short checklist also live in `lab/scenarios/README.md`. Shipped-capability Lab gaps: `lab/scenarios/COVERAGE.md` (also summarized by `lab scenario list`). CI twin rows for the same capabilities: `docs/rqg/CI_TWIN_COVERAGE.md`.

### Adding a Source or Target engine (Developer checklist)

New Source System or Target System kinds plug in at stable interfaces. Do **not** reshape Sync, Rich Transform, Delivery, Deployment runtime, or Platform Store concepts for a new engine—implement the seam, document Operator prerequisites, then complete the capability ladder (ADR-0024 / ADR-0025 / ADR-0028).

1. **Implement the engine interface**
   - Source: `SourceEngine` / `IncrementalCaptureSession` in `crates/capture` (schema discovery, Initial Load chunks, Incremental Capture resume, prerequisites check, alignment reads, schema-change classification inputs).
   - Target: `TargetEngine` in `crates/delivery` (upsert Managed fields by Output Identity, delete by identity, list/read helpers for Drift Check / inspect).
   - Wire the adapter through Deployment runtime factory helpers (`source_engine_from_connection` / `target_engine_from_deployment`). Those factories return the `SourceEngine` / `TargetEngine` interfaces (not concrete Oracle/Mongo types at the call site). Runtime Sync/Delivery must keep depending on the interfaces.
   - Default Operator CLI `apply` / `run` / `sync` still constructs the v1 Oracle LogMiner and MongoDB adapters via those factories. For in-process seam tests, full Incremental Sync also accepts injected engines (`run_incremental_sync_with_engines`) so Fake adapters can exercise the production Sync path without Oracle-kind string gates.
   - Keep Rich Transform / Affect Analysis on platform-managed Base/Derived data only—never use the new engine as transform compute.
2. **Prerequisites and handbook**
   - Document engine-specific Source Prerequisites / Required Privileges (or Target Delivery grants) in the matching Operator chapters ([Source System](source-system.md) / [Target System](target-system.md)) in **all three locales**.
   - Fail fast at apply/sync when prerequisites are unmet; do not auto-mutate customer Source/Target settings by default.
3. **Lab Scenario**
   - Add a selectable Lab Scenario under `lab/scenarios/<id>/` that exercises the new engine on the real product path (same recipe-driven runner checklist above). Namespace-isolate Lab-only bindings.
4. **CI contract twin**
   - Add a **non-ignored** contract-path twin under `crates/app/tests` and update `docs/rqg/CI_TWIN_COVERAGE.md`. Prefer contract/stub + Platform Store/Mongo (or an in-memory fake that implements the same interface)—do not treat the Lab catalog as CI.
5. **Packaging guards**
   - Preserve the modular monorepo + single `migraloop` binary (ADR-0024). Do not introduce a second Platform Store engine (ADR-0001).

In-memory `FakeSource` / `FakeTarget` adapters exist for seam tests (including injected full Incremental Sync); they are not a second production engine.

## Release Quality Gate

Every PR/push must keep four parallel checks green (ADR-0011, ADR-0028). Handbook guard stays its own workflow; the other three jobs live in `.github/workflows/release-quality-gate.yml`. Call the automated surface **Release Quality Gate** / **contract-path CI twin**—never “Mock Lab,” and never treat Local Sync Lab as the gate.

| Check | What it runs | Local reproduction |
| --- | --- | --- |
| **Handbook guard** | `cargo test -p handbook-guard` plus the handbook check entrypoint | See the Handbook guard section below |
| **rqg-unit** | Workspace crate tests excluding `migraloop-app` and `handbook-guard` (no Postgres/Mongo) | `cargo test --workspace --exclude migraloop-app --exclude handbook-guard` |
| **rqg-integration** | Non-ignored `migraloop-app` tests (correctness, contract, fault, capability CI twins) | CI-parity env below, then `cargo test -p migraloop-app` |
| **rqg-perf** | Fixed Direct Pipeline microbench on contract/stub vs committed baseline (`allowed_regression_pct` ~55 for hosted-runner noise; up to 3 attempts) | CI-parity env below, then `bash ci/rqg/run_direct_pipeline_microbench.sh` |

`rqg-integration` and `rqg-perf` use the same service credentials as CI. Set these before those cargo/bash commands:

| Variable | CI / local parity value |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` | `127.0.0.1` |
| `MIGRALOOP_TEST_MONGO_PORT` | `27017` |

MongoDB for those jobs expects root auth `deliver_user` / `mongo-secret-value` (`authSource=admin`)—the same defaults app integration tests hardcode. Example local services:

```bash
docker compose up -d platform-store   # Postgres 16; admin URL above
docker run -d --name migraloop-rqg-mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=deliver_user \
  -e MONGO_INITDB_ROOT_PASSWORD=mongo-secret-value \
  mongo:7
# Some schema/Delivery probes also need: pip install pymongo
export MIGRALOOP_TEST_ADMIN_URL=postgres://migraloop:migraloop@127.0.0.1:5432/postgres
export MIGRALOOP_TEST_MONGO_HOST=127.0.0.1
export MIGRALOOP_TEST_MONGO_PORT=27017
```

Default `cargo test -p migraloop-app` skips `#[ignore]` Lab Fixture / Lab Scenario / live Oracle tests and the `rqg-perf`-only microbench—keep it that way. Lab Scenario `bulk-load` stays **manual**; it is not the performance gate (`ci/rqg/` owns `rqg-perf`). Matrix of shipped Lab capabilities → non-ignored CI twin evidence: `docs/rqg/CI_TWIN_COVERAGE.md`.

## Tests

Unit/crate tests (also covered by `rqg-unit` when run workspace-wide as above):

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

App integration tests under `crates/app/tests` expect a reachable Postgres (and often MongoDB) via the env table in [Release Quality Gate](#release-quality-gate):

```bash
cargo test -p migraloop-app
```

Live Oracle Direct Pipeline seam (ignored by default; requires Instant Client + a prepared Source with Prerequisites):

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
export MIGRALOOP_LIVE_ORACLE_HOST=...
export MIGRALOOP_LIVE_ORACLE_PORT=1521
export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
export ORACLE_PASSWORD=...
cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
```

Lab Fixture lifecycle seam (ignored by default; requires Docker Compose + Lab Oracle image):

```bash
cargo test -p migraloop-app --test cli_lab_fixture -- --ignored --nocapture
```

Lab Scenario Direct Pipeline, Rich Transform `project`/`filter`/`addFields`/`rename`/`remove`/`equiLookup`/`union`/`unwind`, multi-table Transform Pipeline (`groupBy` sum/count/min/max/avg), concurrent Source workload, bulk-load, idempotent-redelivery, pause-resume, remove-pipeline, change-pipeline, poison-quarantine, schema-change-pause, source-alignment, drift-check, bounded-backpressure, observability-surface, platform-store-guardrails, backward-compatible-upgrades, and initial-load-throttled seams (ignored by default; requires Docker Lab Fixture + Instant Client). These are **manual Lab** seams—not Release Quality Gate evidence and not something to wire into CI:

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

Lab DB-level escape-hatch load then product status/inspect (ignored by default; requires Docker Lab Fixture + Instant Client):

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_escape_hatch -- --ignored --nocapture
```

See [Source System](source-system.md) for the Operator apply/sync/inspect verification steps.

## Handbook guard (docs CI seam)

When changing Operator/Developer-visible behavior or handbook pages, run the same entrypoint CI uses (this is the Handbook guard check alongside the Release Quality Gate jobs—not a substitute for `rqg-unit` / `rqg-integration` / `rqg-perf`):

```bash
cargo test -p handbook-guard
cargo run -p handbook-guard -- check \
  --handbook handbook \
  --touchpoints ci/handbook/touchpoints.json \
  --cli-source crates/cli/src/lib.rs \
  --cli-surface ci/handbook/cli-surface.txt
```

Locale trees under `handbook/en`, `handbook/zh-TW`, and `handbook/zh-CN` must stay path-isomorphic. English is canonical.

## Layout reminders

| Path | Audience |
| --- | --- |
| `handbook/` | Operators + Developers (this portal) |
| `CONTEXT.md`, `docs/adr/` | Domain glossary and engineering ADRs |
| `docs/agents/` | Agent skill contracts |
| `ci/handbook/` | Machine config for handbook guards |

## Related chapters

- Operator progressive path: [Start here](start-here.md)
- Compose install shape: [Deployment](deployment.md)
- CLI surface: [CLI & Config reference](cli-and-config.md)
- Source / Target Operator contracts: [Source System](source-system.md), [Target System](target-system.md)
