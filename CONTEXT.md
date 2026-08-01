# DB Sync Platform

An open-source platform for continuous database-to-database synchronization, with first-class rich transforms that shape multi-table data into derived outputs. Transform compute runs against platform-managed data, not the user's source or target databases. The platform owns applying changes to user-configured target tables.

## Language

**Sync**:
Continuous capture of changes from a source database into platform-managed Base Datasets. Latency and resumability are first-class. Sync alone does not imply the user's target table has been updated—that is Delivery.
_Avoid_: Replication (unless referring to the underlying mechanism), mirror-only (implies zero transform capability)

**Base Dataset**:
A platform-managed copy of a source table or collection, kept aligned by Sync, close to the source shape. It is the unit Rich Transforms may read; it is not the user's source or target database.
_Avoid_: Raw table (ambiguous), source mirror, target table

**Rich Transform**:
A user-defined, high-expressiveness transformation (Mongo aggregation–like) that can combine multiple platform-managed datasets into a new data shape. It reads only platform-managed data—never the user's source or target DB as a compute engine. It may be slower than Sync, but must still be performance-oriented.
_Avoid_: Thin mapping, light transform, ETL job (too generic)

**Derived Dataset**:
The platform-managed output produced by a Rich Transform; a dataset the platform materializes and maintains, not a verbatim copy of a single source table.
_Avoid_: View (implies non-materialized / DB-native only), sink table (implementation-flavored)

**Target Binding**:
The user's configuration that maps a platform dataset (Base or Derived) to a specific table/collection in a user target database—whether the path is direct (Base → target) or via Rich Transform (Derived → target).
_Avoid_: Destination (vague), sink (implementation-flavored)

**Delivery**:
The platform-owned process that applies insert/update/delete to the bound target table so the user does not implement write logic themselves. Writing to the target for Delivery is allowed; using the target as Rich Transform input/compute is not.
_Avoid_: Load job (too batch-flavored), sync (overloaded—Sync is capture into the platform)

**Sync Health**:
Whether capture from source into a Base Dataset is caught up and applying successfully (lag, checkpoints, capture/apply failures). Necessary but not sufficient to claim the Base Dataset matches the source.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Source Alignment Check**:
A non-real-time, resource-gated verification that a Base Dataset matches its source. Required before the platform may treat that Base Dataset as a reliable baseline for Drift Check. Must keep source reads lightweight and run only when the source has enough spare capacity.
_Avoid_: Trusting Sync Health alone, full table dump

**Delivery Health**:
Whether the change stream for a Target Binding is caught up and applying successfully (lag, checkpoints, apply failures). Manual edits on the target are out of scope for this signal.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Drift Check**:
A non-real-time, resource-gated verification that the target table matches the platform's expected dataset for that binding (Base or Derived). Uses the platform dataset as baseline only when Source Alignment (for Bases) or equivalent Derived correctness guarantees hold. Detects manual deletes/edits and other target divergence without using the target as transform compute.
_Avoid_: Sync check (ambiguous), audit (too vague)
