# Security

How Operators supply credentials and protect connections for Source System, Target System, and Platform Store.

## Secrets by reference

Credentials must **not** live as plaintext in Pipeline/Deployment documents or as resolved secret values in Platform Store rows (ADR-0006). v1 accepts secrets from:

| Reference form | Config shape | Resolution |
| --- | --- | --- |
| Environment variable | `password: { fromEnv: NAME }` | `std::env` at apply/sync time |
| Mounted file | `password: { fromFile: /path/to/secret }` | File contents (trailing newlines stripped) |
| Docker secret | `password: { fromDockerSecret: name }` | `/run/secrets/<name>` |

Exactly **one** of `fromEnv`, `fromFile`, or `fromDockerSecret` must be set. Plaintext password strings fail config validation with a clear error.

Example:

```yaml
password:
  fromEnv: ORACLE_PASSWORD
```

External secret managers (Vault / cloud KMS) can be added later; they are not required for a production-safe v1 if you inject env or files at runtime.

Platform Store URL in compose may embed a local password for the bundled lab-style store—treat production store credentials the same way you protect any Postgres DSN (env / orchestrator secrets), and do not paste Source/Target passwords into YAML.

**Local Sync Lab** disposable defaults (`migraloop lab up`) are intentionally local-dev friendly and printed after bring-up (`ORACLE_PASSWORD=lab_oracle`, `MONGO_PASSWORD=lab_mongo`, Platform Store `migraloop`/`migraloop`); `migraloop lab status` also surfaces those Lab-only connection details alongside active/leftover Scenario Namespace state. Lab Compose injects the same Lab-only secrets into the Fixture `app` so continuous `migraloop run` Sync can open Source and Deliver. Lab Scenario runs and Namespace cleanup (`migraloop lab scenario run direct-pipeline|rt-project|rt-filter|rt-field-ops|rt-equilookup|rt-union|rt-unwind|rt-distinct-addtoset|transform-pipeline (groupBy sum/count/min/max/avg)|concurrent-source-workload|bulk-load|idempotent-redelivery|pause-resume|remove-pipeline|change-pipeline|poison-quarantine|schema-change-pause|source-alignment|drift-check|bounded-backpressure|observability-surface|platform-store-guardrails|backward-compatible-upgrades|initial-load-throttled …`, `remove`, `--auto-remove`) reuse those same Lab-only secret references for real `apply`/`sync` and Fixture DB cleanup against the disposable stack (Scenario recipes under `lab/scenarios/<id>/`). The DB-level restore/load escape hatch (`lab/escape-hatch/`) uses the same printed Lab credentials for compose-exec sqlplus/mongosh (or dump tools) against the disposable engines only—then ordinary `apply` / `sync`; it is not a Scenario and must not target customer/production databases. Use Lab secrets only for Lab; never point Lab commands or Scenario configs at customer production databases. The CLI enforces that rule on Scenario `run`: Source/Target bindings that are not Lab Fixture engines are refused before apply/sync — ordinary `migraloop apply` / `migraloop sync` remains the path for real Deployments. Nested Docker / **Cursor Cloud** Lab bring-up storage-driver notes: [Developer local setup](developer-local-setup.md) and [Deployment](deployment.md).

## TLS / Connection Security

TLS is **supported** for Source, Target, and Platform Store connections and **recommended in production** (ADR-0017). Cleartext remains allowed for local/dev or explicitly chosen setups in v1—the product does not hard-fail every non-TLS connection. When TLS is requested, misconfiguration fails clearly at apply/run with **no silent cleartext fallback**.

### Source / Target (`spec.source.tls` / `spec.target.tls`)

Optional block on each system. Omit the block (or set `enabled: false`) for cleartext Lab/dev.

| Field | Source (Oracle) | Target (MongoDB) | Notes |
| --- | --- | --- | --- |
| `enabled` | yes to require TCPS | yes to require Mongo TLS | Default when omitted: disabled (cleartext allowed) |
| `caFile` | **invalid** (apply rejects; use `walletLocation`) | CA path (`tlsCAFile`) | Filesystem path only—never paste PEM into YAML or `password` |
| `walletLocation` | Instant Client wallet directory | **invalid** (apply rejects) | Oracle `MY_WALLET_DIRECTORY` |
| `insecureSkipVerify` | optional (`SSL_SERVER_DN_MATCH=no`) | optional (allow invalid certs) | Dev/lab only; keep `false` in production |

Example (paths are references, not secret bodies):

```yaml
source:
  # ...
  tls:
    enabled: true
    walletLocation: /etc/oracle/wallet
target:
  # ...
  tls:
    enabled: true
    caFile: /etc/migraloop/certs/mongo-ca.pem
```

`migraloop status` surfaces non-secret TLS flags/paths (`tls=enabled|disabled`, `caFile=…`, `walletLocation=…`) and never prints PEM bodies or passwords.

### Platform Store

Set TLS on `MIGRALOOP_PLATFORM_STORE_URL` with Postgres libpq-style query params:

| Param | Purpose |
| --- | --- |
| `sslmode=require` / `verify-ca` / `verify-full` | Require TLS (no cleartext fallback) |
| `sslmode=prefer` / `disable` (or omit) | Cleartext-friendly local/dev |
| `sslrootcert=/path/to/ca.pem` | CA file for verification modes |

Example: `postgres://migraloop:***@db:5432/migraloop?sslmode=require&sslrootcert=/run/certs/pg-ca.pem`

Operator guidance:

- Prefer TLS-capable connection paths for Oracle, MongoDB, and Postgres in production networks
- Keep secret material and certificate PEM bodies out of shell history and committed config—use mounted paths and secret references
- Limit Required Privileges on Source/Target accounts (concrete grants below)

## Required Privileges (pointer)

ADR-0016: document and prefer the minimum rights sufficient to run—not DBA/admin-only-by-default. Concrete engine grants live with the connection chapters:

| Account | Chapter | Covers |
| --- | --- | --- |
| Oracle Source sync user | [Source System → Required Privileges](source-system.md#required-privileges) | Initial Load, LogMiner Incremental Capture, Prerequisites probes, schema discovery; required vs Lab-only vs DBA-applied Prerequisites DDL |
| MongoDB Target Delivery user | [Target System → Required Privileges](target-system.md#required-privileges-target) | Delivery upsert/delete, Target inspection; `readWrite` vs collection-scoped custom role; Lab root is not the production default |

Wire those accounts into Deployment config with secret references only (`fromEnv` / `fromFile` / `fromDockerSecret`)—never plaintext passwords in YAML.

## Public env surface

| Variable | Sensitivity |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Contains store credentials when using password DSNs—inject via orchestrator secrets |
| Names used in `fromEnv` | Secret values—never commit |

## Related chapters

- Config shapes: [CLI & Config reference](cli-and-config.md)
- Install defaults: [Deployment](deployment.md)
- Local compose passwords: [Developer local setup](developer-local-setup.md)
- Oracle sync grants: [Source System](source-system.md#required-privileges)
- MongoDB Delivery grants: [Target System](target-system.md#required-privileges-target)
