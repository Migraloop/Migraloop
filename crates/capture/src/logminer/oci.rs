//! Oracle OCI LogMiner adapter (ADR-0013).
//!
//! Production Incremental Capture starts a `DBMS_LOGMNR` session over OCI and
//! reads reconstructed change vectors from `V$LOGMNR_CONTENTS`. This module
//! holds the session SQL surface and connection entry points. Linking/running
//! against Instant Client is required; without it the adapter fails fast with
//! a clear operator error (contract harness covers CI / local slices).

use crate::oracle_prerequisites::OracleSourcePrerequisiteState;
use crate::{CaptureError, CapturePosition, ChangeEvent};

use super::source::OracleSourceConnect;

/// SQL used to start a LogMiner session for Incremental Capture from an SCN.
///
/// Kept as named constants so the OCI binding layer (ODPI-C / rust-oracle) calls
/// the same surface a JDBC client would, without leaking DBMS details into CLI.
pub const DBMS_LOGMNR_START_LOGMNR: &str = "BEGIN DBMS_LOGMNR.START_LOGMNR(\
    STARTSCN => :start_scn, \
    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE \
); END;";

/// Contents query projecting the fields mapped by [`super::contents`].
pub const V_LOGMNR_CONTENTS_QUERY: &str = "SELECT SCN, OPERATION, SEG_OWNER, TABLE_NAME, \
     SQL_REDO, ROW_ID, CS_INFO \
     FROM V$LOGMNR_CONTENTS \
     WHERE SEG_NAME = :table_name \
       AND SCN >= :start_scn \
       AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') \
     ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN";

pub const DBMS_LOGMNR_END_LOGMNR: &str = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";

/// OCI-backed LogMiner Incremental Capture.
///
/// Constructed for non-contract Oracle Source hosts. Until Instant Client + OCI
/// bindings are available in the runtime environment, [`Self::fetch_changes`]
/// and [`Self::probe_prerequisites`] fail fast rather than silently falling
/// back to stub fixtures.
#[derive(Debug, Clone)]
pub struct OciLogMiner {
    connect: OracleSourceConnect,
    #[allow(dead_code)]
    password: String,
}

impl OciLogMiner {
    pub fn new(connect: OracleSourceConnect, password: String) -> Self {
        Self { connect, password }
    }

    pub fn mechanism_label(&self) -> &'static str {
        "LogMiner (OCI)"
    }

    pub fn fetch_changes(
        &self,
        _table: &str,
        _from_position: CapturePosition,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        Err(CaptureError::OciUnavailable {
            host: self.connect.host.clone(),
            detail: "Oracle Instant Client / OCI LogMiner bindings are not available in this runtime; \
                     use Source host `contract` (or `stub`) for the LogMiner contract harness, \
                     or install Instant Client for real OCI capture"
                .to_string(),
        })
    }

    pub fn probe_prerequisites(&self) -> Result<OracleSourcePrerequisiteState, CaptureError> {
        Err(CaptureError::OciUnavailable {
            host: self.connect.host.clone(),
            detail: "cannot probe Oracle Source Prerequisites via OCI LogMiner without Instant Client; \
                     the platform does not auto-alter Source settings"
                .to_string(),
        })
    }
}
