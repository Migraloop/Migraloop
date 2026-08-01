-- Basic Delivery progress: how many Output Identity applies Delivery has performed.

ALTER TABLE pipelines
    ADD COLUMN delivery_applied_changes INTEGER NOT NULL DEFAULT 0;
