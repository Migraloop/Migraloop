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
| `timezone` | Optional IANA name or Oracle-style offset (`+09:00` / `±HH:MM`). Accepted at `apply`. Used when naive DATE/TIMESTAMP must be interpreted and the Source DB timezone is unreadable |
| `tls` | Optional. Set `enabled: true` for TCPS; use `walletLocation` for an Instant Client wallet directory (`caFile` is rejected—Oracle does not use PEM CA files here). Paths only—never PEM inline. See [Security](security.md) |

Real Oracle hosts use the **OCI** path for both **Initial Load** (schema discovery + chunked snapshot) and **LogMiner Incremental Capture**. Initial Load reads PK-ordered `OFFSET`/`FETCH` windows (bounded by `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE`) rather than one unbounded full-table slam; see [Operations](operations.md) and [CLI & Config](cli-and-config.md). Without Oracle Instant Client / OCI libraries in the runtime, apply/sync fail fast naming LogMiner/OCI—there is no silent fallback to the stub catalog. When `tls.enabled: true`, the connect string uses TCPS and misconfig fails clearly (no silent cleartext fallback). Install Instant Client (Basic or Basic Light) and set `LD_LIBRARY_PATH` to its directory before running the app against a live Source.

LogMiner Incremental Capture projects `RS_ID` and `SSN` (with SCN) so multiple contents rows that share one SCN stay distinct, ordered, and resume-safe after a process restart or bounded capture window—Platform Store dedupe and checkpoints must not skip unapplied same-SCN peers (prefer duplicates over gaps; see [Operations](operations.md)).

On a live Source, Pipeline `source.schema` selects the Oracle owner; when omitted, the platform uses the Source `username` (uppercased) as the default schema. The contract/stub harness ignores schema and uses an **injected contract Source catalog** for CI slices only (`MIGRALOOP_CONTRACT_SOURCE_CATALOG` JSON for schema discovery + Initial Load; `MIGRALOOP_INJECT_LOGMINER_CONTENTS` for Incremental Capture)—not an in-binary business-table catalog, not the Lab/real-path definition of truth, and not a supported production Source mechanism.

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
3. Run `migraloop apply` / continuous `migraloop run` (or one-shot `migraloop sync`). Unmet prerequisites produce a pre-run failure naming what is missing.
4. Fix the named Oracle settings, then re-run. The platform never auto-runs `ALTER DATABASE` / `ALTER TABLE` to "fix" failures.

**Local Sync Lab:** `migraloop lab up` provisions a disposable Oracle Source that already satisfies database-level prerequisites for Lab use (ARCHIVELOG + database supplemental logging + sync-user grants). `migraloop lab status` reports Fixture readiness and names any active or leftover Scenario Namespace (or `(none)`). Table-level supplemental logging still applies when recipe-driven Lab Scenario runs (or you) create Pipeline-referenced tables—for example `migraloop lab scenario run direct-pipeline`, `rt-project`, `rt-filter`, `rt-field-ops`, `rt-equilookup`, `rt-union`, `rt-unwind`, `rt-distinct-addtoset`, `transform-pipeline` (groupBy sum/count/min/max/avg), `concurrent-source-workload`, `bulk-load`, `idempotent-redelivery`, `pause-resume`, `remove-pipeline`, `change-pipeline`, `poison-quarantine`, `schema-change-pause`, `source-alignment`, `drift-check`, `bounded-backpressure`, `observability-surface`, or `platform-store-guardrails` / `backward-compatible-upgrades` / `initial-load-throttled` (each packaged under `lab/scenarios/<id>/` with `recipe.yaml`) prepares its Scenario Namespace table(s) with `SUPPLEMENTAL LOG DATA (ALL) COLUMNS`, then runs real `apply` (and LogMiner `sync` when the Scenario drives Incremental Capture; host Instant Client / `LD_LIBRARY_PATH` required). Re-running the same Scenario drops and recreates those Namespace tables; `lab scenario remove` clears them without a run. For DB-level restore/load outside Scenario recipes, use `lab/escape-hatch/oracle-load.sql` (includes table supplemental logging) against Lab connection details, then ordinary `apply` / `sync` — not a second Scenario model and not CI. Lab does not mutate customer/production databases — Scenario `run` refuses Source/Target bindings that are not Lab Fixture engines before apply/sync — and Scenario catalog runs are manual (not a Release Quality Gate / CI suite). Nested Docker / **Cursor Cloud** dockerd storage-driver notes (`fuse-overlayfs` or `vfs`): [Developer local setup](developer-local-setup.md) and [Deployment](deployment.md).

### Contract LogMiner harness (tests / local slices)

When Source `host` is `contract` or `stub`, Incremental Capture uses the in-process **LogMiner contract harness**. Prerequisite probes for that harness are env-driven (read-only; never mutate a database):

| Variable | Meaning | Default |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` for database supplemental logging | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all` (every table currently in the contract Source catalog), empty, or comma-separated tables with PK/ALL logging | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | Reported redo retention in hours | `72` |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | Path to a JSON file of contract catalog tables for schema discovery + Initial Load (CI / local slices only). Required for harness hosts that need tables; unset means an empty catalog | unset (empty catalog) |

## Required Privileges

Concrete grants for the Oracle sync account (ADR-0016). **DBA / SYSDBA is not required** for Sync, Prerequisites probes, or Delivery pairing—use a dedicated sync user with the minimum below. Source Prerequisites DDL (`ALTER DATABASE` / `ALTER TABLE` supplemental logging, ARCHIVELOG) is applied by a DBA separately; it is **not** part of the sync account’s Required Privileges.

### Required for v1 (Initial Load + LogMiner Incremental Capture + Prerequisites)

Grant these to the Deployment `spec.source.username` (replace `SYNC_USER` and table names):

```sql
-- Session
GRANT CREATE SESSION TO SYNC_USER;
GRANT ALTER SESSION TO SYNC_USER;

-- Initial Load + alignment-style reads (repeat per Pipeline-referenced table)
GRANT SELECT ON <schema>.<table> TO SYNC_USER;

-- LogMiner Incremental Capture
GRANT LOGMINING TO SYNC_USER;              -- when the privilege exists on your edition
GRANT SELECT ANY TRANSACTION TO SYNC_USER;
GRANT EXECUTE_CATALOG_ROLE TO SYNC_USER;   -- DBMS_LOGMNR / DBMS_LOGMNR_D
GRANT SELECT_CATALOG_ROLE TO SYNC_USER;    -- dictionary + V$ used by capture and probes
```

`SELECT_CATALOG_ROLE` covers the dictionary / fixed views the product reads for schema discovery, supplemental-logging probes, redo-retention probes, and LogMiner contents, including:

| Object | Used for |
| --- | --- |
| `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS` | Schema discovery + primary-key identity |
| `ALL_LOG_GROUPS` | Table-level supplemental-logging Prerequisites |
| `V$DATABASE` | ARCHIVELOG / DB supplemental logging / current SCN |
| `V$ARCHIVED_LOG`, `V$PARAMETER` | Redo retention / archive-destination Prerequisites |
| `V$LOGMNR_CONTENTS` (and related LogMiner fixed views) | Incremental Capture |

If your security policy forbids `SELECT_CATALOG_ROLE` / `EXECUTE_CATALOG_ROLE`, grant the equivalent narrower set instead (same capabilities):

```sql
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO SYNC_USER;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO SYNC_USER;
GRANT SELECT ON V_$DATABASE TO SYNC_USER;
GRANT SELECT ON V_$ARCHIVED_LOG TO SYNC_USER;
GRANT SELECT ON V_$LOG TO SYNC_USER;
GRANT SELECT ON V_$LOGFILE TO SYNC_USER;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO SYNC_USER;
GRANT SELECT ON V_$PARAMETER TO SYNC_USER;
-- plus SELECT on Pipeline tables and CREATE/ALTER SESSION as above
```

Dictionary views on the narrower path:

- `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS` / `ALL_CONS_COLUMNS` — normally readable for tables the sync user already `SELECT`s (no extra grant once table `SELECT` is in place).
- `ALL_LOG_GROUPS` — required for table-level supplemental-logging Prerequisites. If those rows are not visible without catalog access on your edition, keep `SELECT_CATALOG_ROLE` (or your DBA’s equivalent dictionary grant); the `V_$…` list alone does not replace it.

Edition notes: `LOGMINING` may be absent on some older editions—use the `EXECUTE` grants on `DBMS_LOGMNR` / `DBMS_LOGMNR_D` plus the `V_$…` selects. Exact role names can vary by Oracle version; the capability list above is the contract.

### Optional / not required for production Sync

| Grant | Status |
| --- | --- |
| `CREATE TABLE`, `UNLIMITED TABLESPACE` | **Lab-only** — Local Sync Lab Scenarios create Namespace tables as `SYNC_USER`. Production sync accounts do **not** need DDL. |
| `DBA`, `SYSDBA`, `SELECT ANY TABLE` | **Not required.** Labs or break-glass may use them; they must not be the documented production default. |
| Source Prerequisites DDL (ARCHIVELOG, supplemental logging) | Applied by a **DBA** account — see Source Prerequisites above — not by the sync user. |

**Local Sync Lab:** `lab/oracle/init/01-lab-source-prerequisites.sh` grants the required set plus Lab DDL (`CREATE TABLE` / `UNLIMITED TABLESPACE`) so Scenarios can own Namespace objects. Treat Lab grants as a superset of production Required Privileges, not as the production default.

Secrets and TLS for this account: [Security](security.md). Target Delivery account: [Target System](target-system.md).

## Supported Source Types (v1)

After schema discovery, Sync converts only an allow-listed Oracle type set into the Platform Store (ADR-0018, ADR-0023):

- **Allow-list:** `NUMBER` (precision/scale rules), `FLOAT` / `BINARY_FLOAT` / `BINARY_DOUBLE`, `CHAR` / `NCHAR` / `VARCHAR2` / `NVARCHAR2`, `DATE`, `TIMESTAMP` (including WITH TIME ZONE / LOCAL TIME ZONE), `RAW` (size-capped), and nullable forms of the above.
- **Out of scope:** `BLOB`, `CLOB`, `NCLOB`, `BFILE`, `LONG` / `LONG RAW`, `XMLType`, object types, nested tables / VARRAYs, `ROWID` / `UROWID`, and other exotic types.

Unsupported columns are **omitted** from the Base Dataset (the table still syncs); omission is visible in `migraloop status`. A Pipeline that requires an unsupported column cannot use it—never silent coercion.

**NUMBER:** mapped to precision-preserving Mongo types (`NumberLong` / `Decimal128`) when safe. Schema-unsafe NUMBER columns must be resolved at configure time via Pipeline `fields` (`as: string` or `as: omit`)—not quarantined row-by-row at runtime.

**Temporals:** platform-internal UTC. Timezone-aware values become absolute instants. Naive DATE/TIMESTAMP use the Source DB timezone when readable, else the configured Source `timezone` (IANA name or Oracle-style `±HH:MM`).

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

## Adding another Source engine (Developers)

v1 ships Oracle LogMiner only. A new Source kind implements `SourceEngine` (plus prerequisites/docs, a Lab Scenario, and a CI contract twin) without reshaping Sync / Rich Transform / Delivery / runtime concepts—see [Developer local setup — Adding a Source or Target engine](developer-local-setup.md#adding-a-source-or-target-engine-developer-checklist).

## Related chapters

- Pairing with Target: [Deployment](deployment.md)
- Pipelines that reference tables: [Pipeline](pipeline.md)
- Secrets, TLS, and privilege pointers: [Security](security.md#required-privileges-pointer)
- MongoDB Delivery grants: [Target System](target-system.md#required-privileges-target)
- Instant Client on a Developer machine: [Developer local setup](developer-local-setup.md)
