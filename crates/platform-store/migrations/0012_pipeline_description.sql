-- Optional Operator-facing Pipeline description (metadata-only; ADR-0007 / issue #21).
-- Description changes do not rebuild Derived Datasets or re-Deliver.

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
