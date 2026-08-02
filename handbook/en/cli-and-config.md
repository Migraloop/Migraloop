# CLI & Config reference

Commands, flags, environment variables, and Deployment config fields.

## Operator CLI subcommands

The `migraloop` Operator CLI currently exposes these subcommands:

- `migrate` — apply Platform Store schema migrations
- `apply` — apply a declarative Deployment config
- `status` — report Platform Store health, Deployments, Pipelines, and Base Datasets
- `base` — inspect Base Dataset rows for a Source table
- `target` — inspect Target documents for a Pipeline collection
- `derived` — inspect Derived Dataset rows for a Transform Pipeline
- `sync` — run Incremental Capture into Base Datasets, then Delivery
- `run` — migrate on startup, then keep the app process alive

## Public environment contract

- `MIGRALOOP_PLATFORM_STORE_URL` — Platform Store connection URL used by Operator CLI commands

_Stub chapter — content deepens in a later handbook ticket._
