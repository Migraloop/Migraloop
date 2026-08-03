//! Oracle LogMiner Incremental Capture (ADR-0003, ADR-0013).
//!
//! Product Incremental Capture reads normalized LogMiner contents and maps them
//! to [`ChangeEvent`] values. The domain never depends on stub table catalogs.
//!
//! Backends:
//! - **contract** — in-process LogMiner contents harness for tests / local slices
//! - **oci** — DBMS_LOGMNR via Oracle OCI (requires Instant Client at runtime)

mod contents;
mod contract;
mod inject;
mod oci;
mod source;

pub use contents::{
    change_events_from_logminer_contents, logminer_change_id, LogMinerContent, LogMinerOperation,
};
pub use contract::{named_scenario_logminer_contents, ContractLogMiner};
pub use inject::{
    load_injected_logminer_contents, LogMinerInjectError, INJECT_LOGMINER_CONTENTS_ENV,
};
pub use oci::{
    oci_logminer_session_sql, OciLogMiner, DBMS_LOGMNR_END_LOGMNR, DBMS_LOGMNR_START_LOGMNR,
    V_LOGMNR_CONTENTS_QUERY,
};
pub use source::{
    open_oracle_incremental_capture, IncrementalCapture, OracleSourceConnect,
};
