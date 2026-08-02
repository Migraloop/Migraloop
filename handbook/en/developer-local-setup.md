# Developer local setup

Clone, build, run the Platform Store, and exercise tests in this modular Rust monorepo. Product usage docs for Operators live in the other handbook chapters—this page is the Developer path.

## Prerequisites

- Rust toolchain matching `rust-toolchain.toml` (stable)
- Docker / Docker Compose (Platform Store and optional integration dependencies)
- Git
- **Optional (live Oracle Source):** Oracle Instant Client Basic or Basic Light on the machine that runs `migraloop`, with `LD_LIBRARY_PATH` pointing at the Instant Client directory. Required for real-host Initial Load and LogMiner (OCI); not needed for `host: contract` / `stub` CI slices.

## Clone and build

```bash
git clone https://github.com/Migraloop/Migraloop.git
cd Migraloop
cargo build -p migraloop-app
```

Workspace members: `crates/app` (binary `migraloop`), `cli`, `capture`, `platform-store`, `transform`, `delivery`, plus `ci/handbook` (Handbook guard).

## Platform Store via compose

```bash
docker compose up -d platform-store
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
cargo run -p migraloop-app -- migrate
cargo run -p migraloop-app -- status
```

Full two-container stack (store + app `run`):

```bash
docker compose up -d --build
```

Default compose credentials (`migraloop` / `migraloop`) are for local development only.

## Local Sync Lab Fixture

Disposable Oracle + MongoDB + Platform Store + app for manual Sync→Delivery verification (not CI):

```bash
cargo build -p migraloop-app
./target/debug/migraloop lab up
./target/debug/migraloop lab status
./target/debug/migraloop lab scenario list
# Scenario apply/sync needs Instant Client: export LD_LIBRARY_PATH=/path/to/instantclient
./target/debug/migraloop lab scenario run direct-pipeline
./target/debug/migraloop lab scenario run transform-pipeline
# Inspect leftover Scenario Namespace with migraloop base / derived / target, or mutate Lab DBs directly.
# Re-run wipes Namespace first; or: lab scenario remove <id> / run --auto-remove
./target/debug/migraloop lab down
```

Defaults after bring-up: Platform Store `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`, Oracle `SYNC_USER` / `lab_oracle` @ `FREEPDB1`, MongoDB URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin`. Lab bring-up does not apply sample Deployments/Pipelines. Requires Docker Compose; `lab up` builds `target/debug/migraloop` when missing and packs it via `lab/Dockerfile` (Ubuntu 24.04 base for host glibc). Lab Compose uses `network_mode: host`. First Oracle boot can take several minutes. Nested Docker whiteout extract failures: use dockerd `storage-driver: vfs`. See [CLI & Config](cli-and-config.md) (`lab`) and [Deployment](deployment.md).

## Tests

Unit/crate tests:

```bash
cargo test -p migraloop-capture
cargo test -p migraloop-transform
cargo test -p migraloop-cli
```

App integration tests under `crates/app/tests` expect a reachable Postgres (and often MongoDB) via:

| Variable | Default habit |
| --- | --- |
| `MIGRALOOP_TEST_ADMIN_URL` | `postgres://migraloop:migraloop@127.0.0.1:5432/postgres` |
| `MIGRALOOP_TEST_MONGO_HOST` / `MIGRALOOP_TEST_MONGO_PORT` | `127.0.0.1` / `27017` |

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

Lab Scenario Direct Pipeline and multi-table Transform Pipeline seams (ignored by default; requires Docker Lab Fixture + Instant Client):

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
```

See [Source System](source-system.md) for the Operator apply/sync/inspect verification steps.

## Handbook guard (docs CI seam)

When changing Operator/Developer-visible behavior or handbook pages, run the same entrypoint CI uses:

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
