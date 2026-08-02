-- Durable Operator pause for Pipelines (ADR-0007 / issue #19).
-- Pause stops Delivery/processing for that Pipeline; Base Capture may continue
-- when other Pipelines share the Base Dataset. Resume continues from durable state.

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS paused BOOLEAN NOT NULL DEFAULT FALSE;
