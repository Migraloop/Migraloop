-- Source/Deployment timezone for naive DATE/TIMESTAMP (ADR-0022)
-- and Pipeline field mapping overrides for unsafe NUMBER (ADR-0023).

ALTER TABLE deployments
    ADD COLUMN source_timezone TEXT NOT NULL DEFAULT '';

ALTER TABLE pipelines
    ADD COLUMN field_mappings_json TEXT NOT NULL DEFAULT '{}';
