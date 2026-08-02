-- Initial↔Incremental cutover: low-watermark + checkpoint + applied change dedupe (ADR-0004).

ALTER TABLE base_datasets
    ADD COLUMN capture_low_watermark BIGINT,
    ADD COLUMN capture_checkpoint BIGINT;

CREATE TABLE applied_source_changes (
    deployment_name TEXT NOT NULL,
    source_schema TEXT NOT NULL DEFAULT '',
    source_table TEXT NOT NULL,
    change_id TEXT NOT NULL,
    position BIGINT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (deployment_name, source_schema, source_table, change_id),
    FOREIGN KEY (deployment_name, source_schema, source_table)
        REFERENCES base_datasets (deployment_name, source_schema, source_table)
        ON DELETE CASCADE
);

CREATE INDEX applied_source_changes_position_idx
    ON applied_source_changes (deployment_name, source_schema, source_table, position);
