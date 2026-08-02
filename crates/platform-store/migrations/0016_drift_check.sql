-- Drift Check durable status on Pipelines (issue #25).
-- Non-realtime, resource-gated verification that Managed fields on Target match
-- the platform expected dataset. Default auto-repair restores Managed fields via
-- Delivery upsert; non-Managed Target fields are ignored / never overwritten.

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS drift_status TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS drift_checked_rows INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS drift_mismatched_rows INTEGER NOT NULL DEFAULT 0;

-- unknown | ok | partial (partial = budget truncated; not yet a full check)
ALTER TABLE pipelines
    DROP CONSTRAINT IF EXISTS pipelines_drift_status_check;

ALTER TABLE pipelines
    ADD CONSTRAINT pipelines_drift_status_check
    CHECK (drift_status IN ('unknown', 'ok', 'partial'));
