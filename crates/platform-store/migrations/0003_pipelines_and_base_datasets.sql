-- Pipelines belonging to a Deployment, and Base Datasets produced by Sync Initial Load.

CREATE TABLE pipelines (
    deployment_name TEXT NOT NULL REFERENCES deployments (name) ON DELETE CASCADE,
    name TEXT NOT NULL,
    mode TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_name, name),
    CONSTRAINT pipelines_mode_check CHECK (mode IN ('direct'))
);

CREATE TABLE base_datasets (
    deployment_name TEXT NOT NULL REFERENCES deployments (name) ON DELETE CASCADE,
    source_table TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    columns_json TEXT NOT NULL,
    omitted_columns_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_name, source_schema, source_table),
    CONSTRAINT base_datasets_status_check CHECK (status IN ('initial_load_complete'))
);

CREATE TABLE base_rows (
    deployment_name TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL,
    row_ordinal INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (deployment_name, source_schema, source_table, row_ordinal),
    FOREIGN KEY (deployment_name, source_schema, source_table)
        REFERENCES base_datasets (deployment_name, source_schema, source_table)
        ON DELETE CASCADE
);
