-- Expand Sync Health durable labels beyond unknown|ok (issue #174 / ADR-0008).
-- Typed assembly still derives Lagging from lag even if an older store row says ok.

ALTER TABLE base_datasets
    DROP CONSTRAINT IF EXISTS base_datasets_sync_health_check;

ALTER TABLE base_datasets
    ADD CONSTRAINT base_datasets_sync_health_check
    CHECK (sync_health IN ('unknown', 'ok', 'lagging', 'failed'));
