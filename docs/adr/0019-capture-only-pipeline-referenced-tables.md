# Sync selects tables by Pipeline use; each Base keeps full supported rows

Which Source **tables** enter Sync is determined by Pipeline references in the Deployment—not whole-schema mirroring. When a newly referenced table has no Base Dataset, run a **table-level Initial Load** for that table only; existing Bases stay incremental.

Once a table is included, its Base Dataset stores the **full row** for all **Supported Source Types** on that table—even if current Pipelines use only a subset of fields. We do not project Bases down to “only columns this transform touches,” so a later Pipeline can reuse the same Base without column-level backfill. If a table also has unsupported-type columns (e.g. BLOB per ADR-0018), the table is still synced and those columns are **omitted**, with that omission visible in status/docs—not a whole-table reject.
