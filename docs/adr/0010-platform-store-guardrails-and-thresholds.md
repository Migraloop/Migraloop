# Platform Store has minimum config guardrails and warn thresholds

The bundled PostgreSQL Platform Store ships with safe defaults and product-enforced minimums so users cannot set resources absurdly too low. The platform monitors store health (especially free disk) against a safe threshold and warns when crossed. Critical exhaustion must not be silent: alert and pause affected work rather than hammering a full disk. Full built-in backup-to-object-storage is not required for v1; documented Postgres backup remains the user’s responsibility alongside these guardrails.
