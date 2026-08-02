-- Poison Change quarantine (ADR-0015 / issue #22).
-- After bounded retries, a single failing change/Output Identity is quarantined
-- so the Pipeline can continue. Quarantined keys stay unhealthy / not aligned.

CREATE TABLE IF NOT EXISTS poison_quarantine (
    deployment_name TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL,
    change_id TEXT NOT NULL,
    capture_position BIGINT NOT NULL,
    output_identity_json JSONB NOT NULL,
    stage TEXT NOT NULL DEFAULT 'delivery',
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'quarantined',
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deployment_name, pipeline_name, change_id)
);

CREATE INDEX IF NOT EXISTS poison_quarantine_pipeline_active_idx
    ON poison_quarantine (deployment_name, pipeline_name)
    WHERE status = 'quarantined';
