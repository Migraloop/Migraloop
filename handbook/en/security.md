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

**Local Sync Lab** disposable defaults (`migraloop lab up`) are intentionally local-dev friendly and printed after bring-up (`ORACLE_PASSWORD=lab_oracle`, `MONGO_PASSWORD=lab_mongo`, Platform Store `migraloop`/`migraloop`); `migraloop lab status` also surfaces those Lab-only connection details alongside active/leftover Scenario Namespace state. Lab Scenario runs and Namespace cleanup (`migraloop lab scenario run direct-pipeline|rt-project|rt-filter|transform-pipeline|concurrent-source-workload|bulk-load|idempotent-redelivery …`, `remove`, `--auto-remove`) reuse those same Lab-only secret references for real `apply`/`sync` and Fixture DB cleanup against the disposable stack (Scenario recipes under `lab/scenarios/<id>/`). The DB-level restore/load escape hatch (`lab/escape-hatch/`) uses the same printed Lab credentials for compose-exec sqlplus/mongosh (or dump tools) against the disposable engines only—then ordinary `apply` / `sync`; it is not a Scenario and must not target customer/production databases. Use Lab secrets only for Lab; never point Lab commands or Scenario configs at customer production databases. The CLI enforces that rule on Scenario `run`: Source/Target bindings that are not Lab Fixture engines are refused before apply/sync — ordinary `migraloop apply` / `migraloop sync` remains the path for real Deployments.

## TLS / Connection Security

TLS is **supported** for Source, Target, and Platform Store connections and **recommended in production** (ADR-0017). Cleartext remains allowed for local/dev or explicitly chosen setups in v1—the product does not hard-fail every non-TLS connection.

Operator guidance:

- Prefer TLS-capable connection paths for Oracle, MongoDB, and Postgres in production networks
- Keep secret material out of shell history and committed config
- Limit Required Privileges on Source/Target accounts (see [Source System](source-system.md) and [Target System](target-system.md))

## Public env surface

| Variable | Sensitivity |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Contains store credentials when using password DSNs—inject via orchestrator secrets |
| Names used in `fromEnv` | Secret values—never commit |

## Related chapters

- Config shapes: [CLI & Config reference](cli-and-config.md)
- Install defaults: [Deployment](deployment.md)
- Local compose passwords: [Developer local setup](developer-local-setup.md)
