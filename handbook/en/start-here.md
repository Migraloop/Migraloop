# Start here

Short progressive path from install to a first Pipeline and Sync Health / Delivery Health checks. Functional chapters hold the detail—use this page as the spine, not a second manual.

## Who this is for

- **Operators** install and run a **Deployment**, author **Pipelines**, and monitor health.
- **Developers** setting up the monorepo should jump to [Developer local setup](developer-local-setup.md). Adding a Source or Target engine uses the checklist there (interface + prerequisites/docs + Lab Scenario + CI contract twin)—not a rewrite of Sync / Rich Transform / Delivery / runtime.

v1 ships the first engine pair **Oracle → MongoDB**. One Deployment pairs exactly one Source System with one Target System.

## 1. Install the app and Platform Store

Default install is **one compose stack, two containers**: the `migraloop` app and the PostgreSQL **Platform Store**.

```bash
docker compose up -d --build
```

Compose sets `MIGRALOOP_PLATFORM_STORE_URL` for the app. The app entrypoint runs `migraloop run` (migrate on startup, continuously run Incremental Capture → Affect Analysis → Delivery for applied Deployments/Pipelines, serve Prometheus `/metrics` on port `9090`, then stay alive). Source/Target secret refs used by Pipelines (for example `ORACLE_PASSWORD` / `MONGO_PASSWORD`) must be present in the app process environment so continuous Sync can open Source and Deliver.

For a disposable **Local Sync Lab** Fixture (Oracle + MongoDB + Platform Store + app, no default Deployment/Pipelines): `migraloop lab up` / `status` / `down`. `lab status` reports Fixture readiness and which Scenario Namespace is active or leftover (or `(none)`). Selectable **Lab Scenarios** (catalogued via `lab/scenarios/<id>/recipe.yaml`; runs go through the recipe-driven runner on the real product path—recipe `product_path` steps share Namespace lifecycle + prepare/apply/sync; `namespace.lifecycle` drives wipe/seed (optional mutate SQL); thin hooks handle rare escapes; `checks.correctness` is executable; poison / delay / fail-after / queue-capacity Lab escapes use typed SyncOptions CLI flags, and Initial Load throttle / pause / store-delay knobs use typed ApplyOptions CLI flags (not process env as the primary adapter); for example `migraloop lab scenario list` / `run direct-pipeline` / `run rt-project` / `run rt-filter` / `run rt-field-ops` / `run rt-equilookup` / `run rt-union` / `run rt-unwind` / `run rt-distinct-addtoset` / `run transform-pipeline` (groupBy sum/count/min/max/avg) / `run concurrent-source-workload` / `run change-ordering` / `run bulk-load` / `run idempotent-redelivery` / `run pause-resume` / `run remove-pipeline` / `run change-pipeline` / `run poison-quarantine` / `run schema-change-pause` / `run source-alignment` / `run drift-check` / `run bounded-backpressure` / `run observability-surface` / `run platform-store-guardrails` / `run backward-compatible-upgrades` / `run initial-load-throttled`) exercise real apply/sync inside a Scenario Namespace (Lab pauses Fixture `app` for exclusive host Sync during each run, then resumes it); Scenario `run` refuses non-Lab / production Source/Target engine bindings before apply/sync. Re-run wipes that Namespace first, and `scenario remove` / `--auto-remove` cover cleanup. For DB-level restore/load outside Scenario recipes, use `lab/escape-hatch/` with Lab connection details, then ordinary `apply` / `status` / inspect—still not the Release Quality Gate. Manual verification (ADR-0025). Nested Docker / **Cursor Cloud** storage-driver notes (`fuse-overlayfs` or `vfs`): [Developer local setup](developer-local-setup.md) and [Deployment](deployment.md). See also [CLI & Config reference](cli-and-config.md).

Details: [Deployment](deployment.md) · flags and env: [CLI & Config reference](cli-and-config.md) · secrets/TLS: [Security](security.md)

## 2. Prepare Source System and Target System

Before apply/sync:

1. Satisfy Oracle **Source Prerequisites** (supplemental logging, redo retention) and **Required Privileges** — [Source System](source-system.md).
2. Provide a MongoDB **Target System** the Delivery account can write — [Target System](target-system.md).
3. Supply Source System / Target System passwords only via secret references (`fromEnv` / `fromFile` / `fromDockerSecret`) — [Security](security.md).

## 3. Apply a Deployment with a first Pipeline

Write a declarative YAML/JSON Deployment (`apiVersion: migraloop.dev/v1`, `kind: Deployment`) with `spec.source`, `spec.target`, and at least one Pipeline. Then:

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=...   # names must match your secret refs
export MONGO_PASSWORD=...

migraloop apply -f deployment.yaml
```

`apply` validates config, checks Source Prerequisites when Pipelines reference tables, runs **Initial Load** into **Base Datasets** as needed, and records Pipelines in the Platform Store.

- Direct Pipeline (one source table → Target Binding): [Pipeline](pipeline.md)
- Transform Pipeline (declarative Rich Transform + Output Identity): [Rich Transform](rich-transform.md)

## 4. Continuous Sync (and optional one-shot catch-up)

Steady-state Sync is the long-running app: after `apply`, the compose/`migraloop run` instance continuously resumes **Incremental Capture** (Oracle LogMiner) into Base Datasets from durable checkpoints, maintains Derived Datasets for Transform Pipelines, and **Delivers** Managed fields to MongoDB—no external sync scheduler required.

One-shot catch-up (Lab scenarios, operator-driven drain, or when `run` is not the active path):

```bash
migraloop sync
```

`sync` runs the same Incremental Capture → Affect Analysis → Delivery path once and exits. Prefer the running app for continuous Sync; keep `sync` for Lab and catch-up.

## 5. Check Sync Health and Delivery Health

```bash
migraloop status
```

Read Platform Store health, Deployments, Pipelines, Base Dataset cutover/lag, **Sync Health**, and **Delivery Health**. For deeper inspection:

```bash
migraloop base --table ORDERS
migraloop target --collection orders
migraloop derived --pipeline orders_by_customer   # Transform Pipelines
```

How to interpret signals: [Observability](observability.md) · day-2 ops: [Operations](operations.md)

## Chapter map

| Next topic | Chapter |
| --- | --- |
| Pairing Source + Target, install shape | [Deployment](deployment.md) |
| Oracle connection, prerequisites, types | [Source System](source-system.md) |
| MongoDB Target Binding / Managed Columns | [Target System](target-system.md) |
| Direct / Transform Pipelines | [Pipeline](pipeline.md) |
| Declarative operators & Affect Analysis | [Rich Transform](rich-transform.md) |
| Health, status, metrics contract | [Observability](observability.md) |
| Schema / poison / backpressure / upgrades | [Operations](operations.md) |
| Commands, flags, config fields, env | [CLI & Config reference](cli-and-config.md) |
| Secrets-by-reference and TLS | [Security](security.md) |
| Clone, build, test locally | [Developer local setup](developer-local-setup.md) |
