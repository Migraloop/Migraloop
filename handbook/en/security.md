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
