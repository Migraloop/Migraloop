# Developer local setup

Clone, build, run the Platform Store, and exercise tests in this modular Rust monorepo. Product usage docs for Operators live in the other handbook chapters—this page is the Developer path.

## Prerequisites

- Rust toolchain matching `rust-toolchain.toml` (stable)
- Docker / Docker Compose (Platform Store and optional integration dependencies)
- Git

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
