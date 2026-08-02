# Start here

Short progressive path from install to a first Pipeline and Sync Health / Delivery Health checks. Functional chapters hold the detail—use this page as the spine, not a second manual.

## Who this is for

- **Operators** install and run a **Deployment**, author **Pipelines**, and monitor health.
- **Developers** setting up the monorepo should jump to [Developer local setup](developer-local-setup.md).

v1 ships the first engine pair **Oracle → MongoDB**. One Deployment pairs exactly one Source System with one Target System.

## 1. Install the app and Platform Store

Default install is **one compose stack, two containers**: the `migraloop` app and the PostgreSQL **Platform Store**.

```bash
docker compose up -d --build
```

Compose sets `MIGRALOOP_PLATFORM_STORE_URL` for the app. The app entrypoint runs `migraloop run` (migrate on startup, then stay alive).

For a disposable **Local Sync Lab** Fixture (Oracle + MongoDB + Platform Store + app, no default Deployment/Pipelines): `migraloop lab up` / `status` / `down`. Selectable **Lab Scenarios** (catalogued via `lab/scenarios/<id>/recipe.yaml`; for example `migraloop lab scenario list` / `run direct-pipeline` / `run transform-pipeline` / `run concurrent-source-workload` / `run bulk-load`) exercise real apply/sync inside a Scenario Namespace; re-run wipes that Namespace first, and `scenario remove` / `--auto-remove` cover cleanup. Manual verification (ADR-0025)—not the Release Quality Gate. See [Deployment](deployment.md), [CLI & Config reference](cli-and-config.md), and [Developer local setup](developer-local-setup.md) (feature-time authoring path).

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

## 4. Run Incremental Capture and Delivery

```bash
migraloop sync
```

`sync` resumes **Incremental Capture** (Oracle LogMiner) into Base Datasets from durable checkpoints, maintains Derived Datasets for Transform Pipelines, and **Delivers** Managed fields to MongoDB.

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
