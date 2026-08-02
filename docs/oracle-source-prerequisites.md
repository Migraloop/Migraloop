# Oracle Source Prerequisites

Before Initial Load or Incremental Capture runs against an Oracle Source System, the platform validates **Source Prerequisites** and fails fast with a clear error when they are unmet (ADR-0021). The platform **does not** automatically alter Source System settings to satisfy these checks.

v1 Incremental Capture uses LogMiner. The checks below are the minimum documented requirements for correct capture.

## Required settings

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

Retain redo (online + archived) for at least **24 hours** so Initial Load overlap, Incremental Capture lag, and process restart resume can still read needed change history. Configure archive destination retention / FRA policy accordingly for your Oracle edition.

If redo is aged out before the platform consumes it, changes are silently lost — the platform refuses to run rather than capture incompletely.

## Operator workflow

1. Apply the SQL above (or equivalent) on the Source System as a DBA / privileged operator.
2. Confirm grants for the sync user (see ADR-0016 / Required Privileges).
3. Run `migraloop apply` / `migraloop sync`. Unmet prerequisites produce a pre-run failure naming what is missing.
4. Fix the named Oracle settings, then re-run. The platform never auto-runs `ALTER DATABASE` / `ALTER TABLE` to "fix" failures.

## Stub Source (tests / early slices)

Until real OCI probes land with LogMiner (#13), the stub Source simulates prerequisite state via environment variables:

| Variable | Meaning | Default |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` for database supplemental logging | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`, empty, or comma-separated tables with PK/ALL logging | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | Reported redo retention in hours | `72` |

These are read-only probe inputs for fail-fast coverage; they do not mutate any database.