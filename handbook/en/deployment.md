# Deployment

A **Deployment** pairs exactly **one Source System** with exactly **one Target System** and hosts one or more **Pipelines** between them. Wanting a different database pair means another Deployment—not multi-database fan-in inside one Deployment.

## Install shape (v1)

Default production-shaped install is **one install, two containers**:

| Service | Role |
| --- | --- |
| `platform-store` | Bundled PostgreSQL **Platform Store** (product-locked engine) |
| `app` | `migraloop` binary (`Dockerfile` builds release `migraloop-app`) |

Bring the stack up from the repo root:

```bash
docker compose up -d --build
```

Compose wires `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` into the app and runs `migraloop run`. Tune Postgres volumes/resources as needed; do not replace the store engine.

For Operator CLI on the host (against published store port `5432`):

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # if you are not using `run`
migraloop apply -f deployment.yaml
migraloop status
```

## Local Sync Lab Fixture

For manual end-to-end verification on a disposable real stack (ADR-0025), use the **Local Sync Lab** Fixture. It extends the real install shape with Lab-provisioned Oracle and MongoDB beside Platform Store + app:

```bash
migraloop lab up      # from repo root (or pass --lab-dir)
migraloop lab status  # Fixture readiness + connection details; no default Pipeline
migraloop lab down    # remove containers and volumes
```

Compose definition: `lab/compose.yaml` (project `migraloop-lab`). Lab Oracle init enables ARCHIVELOG and database supplemental logging for LogMiner; it does **not** pre-apply any Deployment or Pipelines—those come from a Lab Scenario or your own `migraloop apply`. Distinct from the default two-container install above and from the contract/stub harness used in CI.

Resource note: Lab Oracle (Free) typically needs several GB RAM and a few minutes on first image pull/boot.

## Runtime model

- v1 runs **one active app instance** (internally concurrent) plus the Platform Store.
- All durable Deployment state (Pipelines, Base/Derived Datasets, checkpoints) lives in the Platform Store so a replacement instance can resume.
- Automatic multi-instance failover is later; active processing remains single-leader (not multi-writer).

## Declaring a Deployment

Config is YAML or JSON. Essential top-level fields:

```yaml
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: db.example.com
    port: 1521
    database: ORCL
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    timezone: Asia/Taipei          # optional; naive DATE/TIMESTAMP fallback
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines: []                    # see Pipeline chapter
```

v1 requires `source.kind: oracle` and `target.kind: mongodb`. Passwords must be secret references—never plaintext.

Apply with `migraloop apply -f <file>`. Empty `pipelines` applies Deployment metadata only (no capture yet).

## Related chapters

- Source connection and prerequisites: [Source System](source-system.md)
- Target Binding and Delivery: [Target System](target-system.md)
- Pipelines inside the Deployment: [Pipeline](pipeline.md)
- Full field/flag list: [CLI & Config reference](cli-and-config.md)
