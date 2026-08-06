-- Align poison_quarantine with schema_change_impacts: Deployment delete must
-- remove orphan quarantine rows (Lab Namespace wipe / Operator remove).
-- Table was created without an FK in 0013; add cascade after clearing orphans.

DELETE FROM poison_quarantine pq
WHERE NOT EXISTS (
    SELECT 1 FROM deployments d WHERE d.name = pq.deployment_name
);

ALTER TABLE poison_quarantine
    ADD CONSTRAINT poison_quarantine_deployment_name_fkey
    FOREIGN KEY (deployment_name) REFERENCES deployments (name)
    ON DELETE CASCADE;
