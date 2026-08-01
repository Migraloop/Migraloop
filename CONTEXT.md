# DB Sync Platform

An open-source platform for continuous database-to-database synchronization, with first-class rich transforms that shape multi-table data into derived outputs. Transform compute runs against platform-managed data, not the user's source or target databases.

## Language

**Sync**:
Continuous capture of changes from a source database into platform-managed Base Datasets, with delivery toward a user target as a separate concern. Latency and resumability are first-class.
_Avoid_: Replication (unless referring to the underlying mechanism), mirror-only (implies zero transform capability)

**Base Dataset**:
A platform-managed copy of a source table or collection, kept aligned by Sync, close to the source shape. It is the unit Rich Transforms may read; it is not the user's source or target database.
_Avoid_: Raw table (ambiguous), source mirror, target table

**Rich Transform**:
A user-defined, high-expressiveness transformation (Mongo aggregation–like) that can combine multiple platform-managed datasets into a new data shape. It reads only platform-managed data—never the user's source or target DB. It may be slower than Sync, but must still be performance-oriented.
_Avoid_: Thin mapping, light transform, ETL job (too generic)

**Derived Dataset**:
The platform-managed output produced by a Rich Transform; a dataset the platform materializes and maintains, not a verbatim copy of a single source table.
_Avoid_: View (implies non-materialized / DB-native only), sink table (implementation-flavored)
