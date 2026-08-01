# Sync captures only Pipeline-referenced tables/columns, with table-level Initial Load

Initial Load and Incremental Capture include only Source tables/columns actually referenced by Pipelines in the Deployment. We do not default to whole-schema mirroring.

When a Pipeline is added at runtime and references a table that does not yet have a Base Dataset, the platform performs a **table-level Initial Load for that table only** (then overlaps into Incremental Capture with no-gap cutover). Already-synced Base Datasets are left on incremental paths and are not reloaded. Shared Base Datasets remain one per source table for all Pipelines that need them.
