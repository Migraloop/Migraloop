# Sync captures only Pipeline-referenced tables and columns

Initial Load and Incremental Capture include only Source tables/columns actually referenced by Pipelines in the Deployment (Direct or Transform inputs). We do not default to whole-schema or whole-database mirroring. This limits source load, Platform Store size, and required privileges.
