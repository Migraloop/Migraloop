-- Schema Change impact pause records (ADR-0009 / issue #23).
-- Blocking Source DDL warns and pauses affected Pipeline(s). Distinct from
-- poison_quarantine (ADR-0015): stream-wide blockers, not single-row failures.

CREATE TABLE IF NOT EXISTS schema_change_impacts (
    deployment_name TEXT NOT NULL REFERENCES deployments (name) ON DELETE CASCADE,
    pipeline_name TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL,
    change_id TEXT NOT NULL,
    capture_position BIGINT NOT NULL,
    ddl_summary TEXT NOT NULL,
    impact TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    warned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deployment_name, pipeline_name, change_id)
);

CREATE INDEX IF NOT EXISTS schema_change_impacts_pipeline_active_idx
    ON schema_change_impacts (deployment_name, pipeline_name)
    WHERE status = 'active';
