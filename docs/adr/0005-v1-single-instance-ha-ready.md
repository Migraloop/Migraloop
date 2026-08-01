# v1 is single-instance, HA-ready—not automatic failover

Production v1 runs one active app instance (internally concurrent) with PostgreSQL Platform Store. All durable sync/transform/delivery state belongs in the Platform Store so another instance can resume the same Deployment later. We do not ship automatic leader election/failover in v1, and we do not allow multi-writer active-active processing. This keeps v1 shippable while avoiding architecture debt that would block active-passive HA next.
