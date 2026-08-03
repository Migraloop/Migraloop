-- Non-secret TLS settings for Source / Target connections (ADR-0017 / issue #123).
-- Certificate PEM bodies and passwords stay out of these columns; only paths/flags.

ALTER TABLE deployments
    ADD COLUMN source_tls_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN target_tls_json TEXT NOT NULL DEFAULT '{}';
