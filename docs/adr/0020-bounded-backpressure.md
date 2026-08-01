# Downstream slowness uses bounded queues and backpressure

When Platform Store apply, Derived maintenance, or Target Delivery cannot keep up, the platform applies **backpressure** through bounded queues and slowed capture/apply. Lag is exposed via Sync/Delivery Health and Prometheus metrics. Unbounded buffering until OOM is rejected. Auto-pausing a Pipeline merely because the target is slow is not the v1 default—operators see lag and act; pause remains for true blockers (e.g. unblockable DDL).
