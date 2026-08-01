# DB Sync Platform

An open-source platform for continuous database-to-database synchronization, with first-class rich transforms that shape multi-table data into derived outputs.

## Language

**Sync**:
Continuous alignment of base data from a source database into a target database (extract + load of base tables/collections). Latency and resumability are first-class.
_Avoid_: Replication (unless referring to the underlying mechanism), mirror-only (implies zero transform capability)

**Base Dataset**:
A source table or collection that Sync keeps aligned on the target as close to the source shape as the connection allows.
_Avoid_: Raw table (ambiguous), source mirror

**Rich Transform**:
A user-defined, high-expressiveness transformation (Mongo aggregation–like) that can combine multiple Base Datasets into a new data shape. It is allowed to be slower than Sync, but must still be performance-oriented—not a best-effort batch afterthought.
_Avoid_: Thin mapping, light transform, ETL job (too generic)

**Derived Dataset**:
The output produced by a Rich Transform; a dataset the platform materializes and maintains, not a verbatim copy of a single source table.
_Avoid_: View (implies non-materialized / DB-native only), sink table (implementation-flavored)
