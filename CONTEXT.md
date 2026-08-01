# DB Sync Platform

An open-source platform for continuous database-to-database synchronization, with first-class rich transforms that shape multi-table data into derived outputs. Transform compute runs against platform-managed data, not the user's source or target databases. The platform owns applying changes to user-configured target tables.

The product must support **many database engine kinds** over time (multiple source kinds and multiple target kinds). The domain model stays engine-agnostic so new engines plug in without reshaping Sync, Rich Transform, Delivery, or checks. The first shipping pair is **Oracle → MongoDB**; that pair is a vertical slice, not the ceiling.

A single **Deployment** connects **one Source System to one Target System** and may contain **many Pipelines**. Wanting a different database pair means another Deployment (source/target swapped or replaced), not multi-database fan-in inside one Deployment.

## Language

**Source System**:
A user database instance the platform captures from (e.g. an Oracle database). Identified by engine kind plus connection identity. The platform must support multiple engine kinds over time.
_Avoid_: Source DB (fine colloquially; prefer Source System when distinguishing from Target System)

**Target System**:
A user database instance the platform delivers into (e.g. a MongoDB deployment). Identified by engine kind plus connection identity. The platform must support multiple engine kinds over time.
_Avoid_: Destination database

**Deployment**:
One running configuration that pairs exactly one Source System with exactly one Target System and hosts one or more Pipelines between them.
_Avoid_: Cluster (infra-flavored), pipeline (a Deployment contains many Pipelines)

**Sync**:
Continuous one-way capture of changes from a Source System into platform-managed Base Datasets. Latency and resumability are first-class. Sync alone does not imply the Target System has been updated—that is Delivery. Reverse flow is not a product feature; users who need the opposite direction create a separate Deployment with source and target swapped. Capture mechanics are engine-specific; the Sync concept is not.
_Avoid_: Replication (unless referring to the underlying mechanism), mirror-only (implies zero transform capability), bidirectional sync, active-active

**Base Dataset**:
A platform-managed copy of a source table or collection, kept aligned by Sync, close to the source shape. It is the unit Rich Transforms and direct Pipelines may read; it is not the user's source or target database. Within a Deployment, each source table/collection has at most one Base Dataset, shared by every Pipeline that needs it—never captured or stored once per Pipeline.
_Avoid_: Raw table (ambiguous), source mirror, target table, per-pipeline copy of the same source table

**Rich Transform**:
A user-defined, high-expressiveness transformation (Mongo aggregation–like) that can combine multiple platform-managed datasets into a new data shape. It reads only platform-managed data—never the user's source or target DB as a compute engine. It may be slower than Sync, but must still be performance-oriented.
_Avoid_: Thin mapping, light transform, ETL job (too generic)

**Derived Dataset**:
The platform-managed output produced by a Rich Transform; a dataset the platform materializes and maintains, not a verbatim copy of a single source table.
_Avoid_: View (implies non-materialized / DB-native only), sink table (implementation-flavored)

**Pipeline**:
A user-defined flow inside a Deployment that produces one target table/collection. Examples: source table A → target table A (direct); source tables A+B → target table B (via Rich Transform). A Deployment has many Pipelines; a Pipeline does not define the Source/Target System pair—that is the Deployment.
_Avoid_: The whole system config, Deployment, single global job

**Target Binding**:
The part of a Pipeline that maps its output dataset (Base or Derived) to a specific table/collection in the Target System. It declares Managed Columns/fields; the target table/collection may have additional fields outside the binding.
_Avoid_: Destination (vague), sink (implementation-flavored)

**Managed Columns**:
The columns/fields declared by a Pipeline's Target Binding as owned by the platform. Delivery, Drift Check, and auto-repair apply only to these. Extra fields on the target table/collection are out of scope.
_Avoid_: All columns, full document ownership (unless the binding literally lists every field)

**Delivery**:
The platform-owned process that applies insert/update/delete to Managed Columns on the bound target table/collection so the user does not implement write logic themselves. Writing to the target for Delivery is allowed; using the target as Rich Transform input/compute is not.
_Avoid_: Load job (too batch-flavored), sync (overloaded—Sync is capture into the platform)

**Sync Health**:
Whether capture from source into a Base Dataset is caught up and applying successfully (lag, checkpoints, capture/apply failures). Necessary but not sufficient to claim the Base Dataset matches the source.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Source Alignment Check**:
A non-real-time, resource-gated verification that a Base Dataset matches its source. Required before the platform may treat that Base Dataset as a reliable baseline for Drift Check. Must keep source reads lightweight and run only when the source has enough spare capacity. When misalignment is found, the platform repairs the Base Dataset from source using data already required for the check where possible—not by writing to the source.
_Avoid_: Trusting Sync Health alone, full table dump

**Delivery Health**:
Whether the change stream for a Pipeline's Target Binding is caught up and applying successfully (lag, checkpoints, apply failures). Edits to non-Managed Columns are irrelevant to this signal.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Drift Check**:
A non-real-time, resource-gated verification that Managed Columns on the target match the platform's expected dataset for that Pipeline. Uses the platform dataset as baseline only when Source Alignment (for Bases) or equivalent Derived correctness guarantees hold. By default, detected drift on Managed Columns is auto-repaired back to the Pipeline's expected values; non-Managed Columns are ignored. Auto-repair must not imply extra source load beyond what alignment/verification already requires.
_Avoid_: Sync check (ambiguous), audit (too vague), preserving manual edits on Managed Columns
