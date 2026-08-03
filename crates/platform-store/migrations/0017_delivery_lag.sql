-- Delivery Health lag under Downstream backpressure (ADR-0020 / issue #26).
-- Counts remaining pending Delivery work in the current Incremental window
-- (mirrors Base Dataset sync_lag). Default 0 = caught up / unknown.

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS delivery_lag INTEGER NOT NULL DEFAULT 0;
