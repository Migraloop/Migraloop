-- Maintenance State for Rich Transform operators that need value-level
-- Affect Analysis (distinct / addToSet). Not created for simple groupBy
-- aggregations that Derived + change already suffice for.

CREATE TABLE IF NOT EXISTS maintenance_states (
    deployment_name TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_name, pipeline_name),
    FOREIGN KEY (deployment_name, pipeline_name)
        REFERENCES pipelines (deployment_name, name)
        ON DELETE CASCADE
);
