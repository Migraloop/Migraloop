-- Source Alignment Check durable status on Base Datasets (issue #24).
-- Non-realtime, resource-gated verification that Base matches Source; required
-- before treating Base as a Drift baseline. Repair writes Base only — never Source.

ALTER TABLE base_datasets
    ADD COLUMN IF NOT EXISTS source_alignment TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS source_alignment_checked_rows INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_alignment_mismatched_rows INTEGER NOT NULL DEFAULT 0;

-- unknown | aligned | partial (partial = budget truncated; not yet a full baseline)
ALTER TABLE base_datasets
    DROP CONSTRAINT IF EXISTS base_datasets_source_alignment_check;

ALTER TABLE base_datasets
    ADD CONSTRAINT base_datasets_source_alignment_check
    CHECK (source_alignment IN ('unknown', 'aligned', 'partial'));
