-- Transform Pipeline mode, persisted Rich Transform + Output Identity,
-- and Derived Datasets materialized by Rich Transforms.

ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_mode_check;
ALTER TABLE pipelines
    ADD CONSTRAINT pipelines_mode_check CHECK (mode IN ('direct', 'transform'));

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS output_identity_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS transform_json TEXT NOT NULL DEFAULT 'null';

CREATE TABLE IF NOT EXISTS derived_datasets (
    deployment_name TEXT NOT NULL REFERENCES deployments (name) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL,
    output_identity_json TEXT NOT NULL,
    columns_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_name, pipeline_name),
    CONSTRAINT derived_datasets_status_check CHECK (status IN ('materialized')),
    FOREIGN KEY (deployment_name, pipeline_name)
        REFERENCES pipelines (deployment_name, name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS derived_rows (
    deployment_name TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    row_ordinal INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (deployment_name, pipeline_name, row_ordinal),
    FOREIGN KEY (deployment_name, pipeline_name)
        REFERENCES derived_datasets (deployment_name, pipeline_name)
        ON DELETE CASCADE
);
