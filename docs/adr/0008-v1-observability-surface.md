# v1 Observability Surface is health, logs, and Prometheus metrics

Production v1 must expose structured logs, Sync/Delivery Health (lag, checkpoints, errors), per-Pipeline status, Prometheus metrics, and alertable failure counters. This is enough to operate a single-instance Deployment. Full distributed tracing and vendor APM bindings are deferred.
