# Source System

A **Source System** is the user database the platform captures from. v1 ships **Oracle** with **LogMiner** for **Incremental Capture**. Connection identity is `kind` + host/port/database/username plus a password secret reference.

## Connection shape

Under `spec.source` in the Deployment config:

| Field | Meaning |
| --- | --- |
| `kind` | Must be `oracle` in v1 |
| `host` | Oracle host. Special values `contract` or `stub` select the in-process LogMiner contract harness (tests / local slices)—not a real Oracle |
| `port` | TCP port (typically `1521`) |
| `database` | Service / database name |
| `username` | Sync account (minimum Required Privileges; not admin-only-by-default) |
| `password` | Secret reference: `fromEnv`, `fromFile`, or `fromDockerSecret` |
| `timezone` | Optional IANA name or Oracle-style offset (`+09:00`). Used when naive DATE/TIMESTAMP must be interpreted and the Source DB timezone is unreadable |

Real Oracle hosts use the **OCI** path for both **Initial Load** (schema discovery + snapshot) and **LogMiner Incremental Capture**. Without Oracle Instant Client / OCI libraries in the runtime, apply/sync fail fast naming LogMiner/OCI—there is no silent fallback to the stub catalog. Install Instant Client (Basic or Basic Light) and set `LD_LIBRARY_PATH` to its directory before running the app against a live Source.

On a live Source, Pipeline `source.schema` selects the Oracle owner; when omitted, the platform uses the Source `username` (uppercased) as the default schema. The contract/stub harness ignores schema and continues to use its fixture catalog for CI slices only—not as the Lab/real-path definition of truth.

## Source Prerequisites (Oracle / LogMiner)

Before **Initial Load** or **Incremental Capture**, the platform validates **Source Prerequisites** and **fails fast** with a clear error when they are unmet (ADR-0021). The platform **does not** automatically alter Source System settings to satisfy these checks.

### 1. Database supplemental logging

Enable minimum supplemental logging at the database level:

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

Without this, LogMiner cannot reliably reconstruct change vectors.

### 2. Table-level key supplemental logging

For every table referenced by a Pipeline, enable PRIMARY KEY or ALL COLUMNS supplemental logging:

```sql
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;
-- or, when the table has no usable PK / needs full before-images:
ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Missing table-level logging leads to incomplete or incorrect Incremental Capture for that table.

### 3. Sufficient redo / archive retention

Retain redo (online + archived) for at least **24 hours** so Initial Load overlap, Incremental Capture lag, and process restart resume can still read needed change history. Configure archive destination retention / FRA policy for your Oracle edition.

Live OCI probes require **ARCHIVELOG** mode. They report the available archived-redo span when known; if that span is still shorter than 24 hours (for example a freshly provisioned Lab Source) but `db_recovery_file_dest` or `log_archive_dest_1` is configured, the probe treats that as meeting the documented floor. **NOARCHIVELOG** fails fast. If redo is aged out before the platform consumes it, changes are lost—the platform refuses to run rather than capture incompletely.

### Operator workflow

1. Apply the SQL above (or equivalent) on the Source System as a DBA / privileged operator.
2. Confirm grants for the sync user (see Required Privileges below).
3. Run `migraloop apply` / `migraloop sync`. Unmet prerequisites produce a pre-run failure naming what is missing.
4. Fix the named Oracle settings, then re-run. The platform never auto-runs `ALTER DATABASE` / `ALTER TABLE` to "fix" failures.

**Local Sync Lab:** `migraloop lab up` provisions a disposable Oracle Source that already satisfies database-level prerequisites for Lab use (ARCHIVELOG + database supplemental logging + sync-user grants). Table-level supplemental logging still applies when Lab Scenarios (or you) create Pipeline-referenced tables—for example `migraloop lab scenario run direct-pipeline` prepares its Scenario Namespace table with `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, then runs real `apply` / LogMiner `sync` (host Instant Client / `LD_LIBRARY_PATH` required). Re-running the same Scenario drops and recreates that Namespace table; `lab scenario remove` clears it without a run. Lab does not mutate customer/production databases.

### Contract LogMiner harness (tests / local slices)

When Source `host` is `contract` or `stub`, Incremental Capture uses the in-process **LogMiner contract harness**. Prerequisite probes for that harness are env-driven (read-only; never mutate a database):

| Variable | Meaning | Default |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` for database supplemental logging | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`, empty, or comma-separated tables with PK/ALL logging | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | Reported redo retention in hours | `72` |

## Required Privileges

The sync account needs rights sufficient for **Initial Load**, **Incremental Capture** (LogMiner session and related dictionary/redo reads), schema discovery for Pipeline-referenced tables, and alignment-style reads—not superuser as the only supported mode (ADR-0016).

In practice the account must be able to:

- `SELECT` the tables (and schemas) referenced by Pipelines for Initial Load
- Open LogMiner / read redo contents views required for Incremental Capture
- Read data-dictionary metadata needed for supplemental-logging and schema probes

Grant the narrowest set that satisfies those duties on your Oracle edition. Admin/DBA may work for labs but must not be the documented production default.

## Supported Source Types (v1)

After schema discovery, Sync converts only an allow-listed Oracle type set into the Platform Store (ADR-0018, ADR-0023):

- **Allow-list:** `NUMBER` (precision/scale rules), `FLOAT` / `BINARY_FLOAT` / `BINARY_DOUBLE`, `CHAR` / `NCHAR` / `VARCHAR2` / `NVARCHAR2`, `DATE`, `TIMESTAMP` (including WITH TIME ZONE / LOCAL TIME ZONE), `RAW` (size-capped), and nullable forms of the above.
- **Out of scope:** `BLOB`, `CLOB`, `NCLOB`, `BFILE`, `LONG` / `LONG RAW`, `XMLType`, object types, nested tables / VARRAYs, `ROWID` / `UROWID`, and other exotic types.

Unsupported columns are **omitted** from the Base Dataset (the table still syncs); omission is visible in `migraloop status`. A Pipeline that requires an unsupported column cannot use it—never silent coercion.

**NUMBER:** mapped to precision-preserving Mongo types (`NumberLong` / `Decimal128`) when safe. Schema-unsafe NUMBER columns must be resolved at configure time via Pipeline `fields` (`as: string` or `as: omit`)—not quarantined row-by-row at runtime.

**Temporals:** platform-internal UTC. Timezone-aware values become absolute instants. Naive DATE/TIMESTAMP use the Source DB timezone when readable, else the configured Source `timezone`.

## Which tables are captured

Sync selects tables by **Pipeline references**—not whole-schema mirror. Each included table gets one shared **Base Dataset** (full supported-type row) reused by every Pipeline that needs it. Adding a newly referenced table runs **table-level Initial Load** for that table only.

## Live Oracle verification (CLI operator seam)

Against a real Oracle Source (with Instant Client installed and Source Prerequisites satisfied), operators verify Sync→Delivery without mocks:

1. Point `spec.source.host` / `port` / `database` / `username` at the live Source (not `contract`/`stub`).
2. `migraloop apply -f deployment.yaml` — Initial Load reads live tables into Base Datasets and Delivers Direct Pipelines to MongoDB.
3. Mutate Source rows (`INSERT` / `UPDATE` / `DELETE`) with supplemental logging enabled.
4. `migraloop sync` — LogMiner (OCI) Incremental Capture applies changes; Managed fields on MongoDB reflect them.
5. Inspect with `migraloop status`, `migraloop base --table <TABLE>`, and `migraloop target --collection <NAME>`.

Developers can also run the gated seam test when a live Oracle is available:

```bash
export LD_LIBRARY_PATH=/path/to/instantclient
export MIGRALOOP_LIVE_ORACLE_HOST=127.0.0.1
export MIGRALOOP_LIVE_ORACLE_PORT=1521
export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
export ORACLE_PASSWORD=...
cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
```

## Related chapters

- Pairing with Target: [Deployment](deployment.md)
- Pipelines that reference tables: [Pipeline](pipeline.md)
- Secrets and TLS: [Security](security.md)
- Instant Client on a Developer machine: [Developer local setup](developer-local-setup.md)
