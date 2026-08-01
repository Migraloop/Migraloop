-- Target Binding on Pipelines, Output Identity (source PK) on Base Datasets,
-- and Delivery status for operator-visible progress.

ALTER TABLE pipelines
    ADD COLUMN target_collection TEXT NOT NULL DEFAULT '',
    ADD COLUMN delivery_status TEXT NOT NULL DEFAULT 'not_configured';

ALTER TABLE pipelines
    DROP CONSTRAINT IF EXISTS pipelines_delivery_status_check;

ALTER TABLE pipelines
    ADD CONSTRAINT pipelines_delivery_status_check
    CHECK (delivery_status IN ('not_configured', 'pending', 'delivered'));

ALTER TABLE base_datasets
    ADD COLUMN primary_key_json TEXT NOT NULL DEFAULT '[]';
