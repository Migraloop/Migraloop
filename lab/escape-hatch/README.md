# Lab DB-level restore / load escape hatch

Operator samples for loading data **directly** into disposable Lab Oracle and Lab Mongo (issue #87 / ADR-0025), then continuing on the ordinary product `apply` / `status` / inspect / `sync` path.

This is **not** a Lab Scenario package: there is no `recipe.yaml`, and you do **not** run `migraloop lab scenario …` for this flow. It is also **not** part of the Release Quality Gate / CI catalog.

Canonical Operator steps live in the handbook:

- [Deployment](../../handbook/en/deployment.md) (Local Sync Lab Fixture — DB-level restore/load)
- [Developer local setup](../../handbook/en/developer-local-setup.md)
- [CLI & Config](../../handbook/en/cli-and-config.md)

Files:

| File | Role |
| --- | --- |
| `oracle-load.sql` | Create/load `LAB_ESCAPE_CUSTOMERS` + table supplemental logging |
| `mongo-load.js` | Seed `lab_escape_manual` for Target-side inspect |
| `deployment.yaml` | Lab-only Deployment so product apply/sync/inspect can follow an Oracle load |
