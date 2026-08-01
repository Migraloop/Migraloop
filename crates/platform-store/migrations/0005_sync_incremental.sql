-- Incremental Capture progress on Base Datasets (Sync Health basics).

ALTER TABLE base_datasets
    DROP CONSTRAINT IF EXISTS base_datasets_status_check;

ALTER TABLE base_datasets
    ADD CONSTRAINT base_datasets_status_check
    CHECK (status IN ('initial_load_complete', 'incremental'));

ALTER TABLE base_datasets
    ADD COLUMN sync_applied_changes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE base_datasets
    ADD COLUMN sync_health TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE base_datasets
    DROP CONSTRAINT IF EXISTS base_datasets_sync_health_check;

ALTER TABLE base_datasets
    ADD CONSTRAINT base_datasets_sync_health_check
    CHECK (sync_health IN ('unknown', 'ok'));
