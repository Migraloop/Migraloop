-- Durable Deployment configuration (non-secret) with secret references only.

CREATE TABLE deployments (
    name TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL,
    source_host TEXT NOT NULL,
    source_port INTEGER NOT NULL,
    source_database TEXT NOT NULL,
    source_username TEXT NOT NULL,
    source_password_ref_kind TEXT NOT NULL,
    source_password_ref_value TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_host TEXT NOT NULL,
    target_port INTEGER NOT NULL,
    target_database TEXT NOT NULL,
    target_username TEXT NOT NULL,
    target_password_ref_kind TEXT NOT NULL,
    target_password_ref_value TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT deployments_source_password_ref_kind_check
        CHECK (source_password_ref_kind IN ('env', 'file')),
    CONSTRAINT deployments_target_password_ref_kind_check
        CHECK (target_password_ref_kind IN ('env', 'file'))
);
