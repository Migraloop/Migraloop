# Oracle Incremental Capture starts with LogMiner, stays pluggable

Incremental Capture must remain a pluggable concern so the platform can support multiple mechanisms per source engine over time. For Oracle, v1 uses **LogMiner** to avoid forcing GoldenGate licensing on users. The domain (Sync, Base Dataset, Affect Analysis, Delivery) depends on normalized change events, not on LogMiner specifically—future Oracle capture options can be added behind the same boundary.
