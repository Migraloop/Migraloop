# Implement the platform in Rust, including Oracle LogMiner via OCI

Performance is a primary product requirement. We implement the v1 app in **Rust** (single process, internally concurrent) and access Oracle LogMiner through Oracle OCI bindings (e.g. ODPI-C / rust-oracle), calling the same `DBMS_LOGMNR` / contents views a JDBC client would. We accept a less paved CDC ecosystem than Java in exchange for runtime control and long-running performance characteristics. Go was rejected as the performance-first choice; a Java-only core was rejected for the same reason.
