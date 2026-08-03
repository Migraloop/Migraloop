-- Chunked / pausable Initial Load (issue #124): durable in-progress/paused status +
-- Operator-visible resume cursor (PK of last persisted row). Resume offset is
-- durable row_count; cursor_json is observational/status surface.

ALTER TABLE base_datasets
    DROP CONSTRAINT IF EXISTS base_datasets_status_check;

ALTER TABLE base_datasets
    ADD CONSTRAINT base_datasets_status_check
    CHECK (status IN (
        'initial_load_complete',
        'incremental',
        'initial_load_in_progress',
        'initial_load_paused'
    ));

ALTER TABLE base_datasets
    ADD COLUMN IF NOT EXISTS initial_load_cursor_json TEXT;
