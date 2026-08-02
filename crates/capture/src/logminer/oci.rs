//! Oracle OCI LogMiner adapter (ADR-0013).
//!
//! Production Incremental Capture starts a `DBMS_LOGMNR` session over OCI and
//! reconstructs supplemental-logged row images into [`super::LogMinerContent`]
//! (identity + after_image). The platform maps those contents to
//! [`crate::ChangeEvent`]; it does **not** parse `SQL_REDO` text.
//!
//! Until Oracle Instant Client / OCI bindings are linked into the runtime, this
//! adapter fails fast. Operator-seam and CI coverage for issue #13 use the
//! LogMiner **contract** harness (`host: contract` / `stub`).

use crate::oracle_prerequisites::OracleSourcePrerequisiteState;
use crate::{CaptureError, CapturePosition, ChangeEvent};

use super::source::OracleSourceConnect;

/// Start a LogMiner session for Incremental Capture from an SCN.
///
/// Bound parameters: `:start_scn`.
pub const DBMS_LOGMNR_START_LOGMNR: &str = "BEGIN DBMS_LOGMNR.START_LOGMNR(\
    STARTSCN => :start_scn, \
    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE \
); END;";

/// Contents query projecting fields the OCI binding reconstructs into
/// [`super::LogMinerContent`].
///
/// Bound parameters: `:table_name`, `:start_scn`.
/// Column values / PK identity come from supplemental logging reconstruction in
/// the OCI layer — not from scraping `SQL_REDO`.
pub const V_LOGMNR_CONTENTS_QUERY: &str = "SELECT SCN, OPERATION, SEG_OWNER, TABLE_NAME \
     FROM V$LOGMNR_CONTENTS \
     WHERE SEG_NAME = :table_name \
       AND SCN >= :start_scn \
       AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') \
     ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN";

pub const DBMS_LOGMNR_END_LOGMNR: &str = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";

/// Documented OCI session steps for a future Instant Client binding.
pub fn oci_logminer_session_sql() -> [&'static str; 3] {
    [
        DBMS_LOGMNR_START_LOGMNR,
        V_LOGMNR_CONTENTS_QUERY,
        DBMS_LOGMNR_END_LOGMNR,
    ]
}

/// OCI-backed LogMiner Incremental Capture.
///
/// Constructed for non-contract Oracle Source hosts. Without Instant Client,
/// [`Self::fetch_changes`] and [`Self::probe_prerequisites`] fail fast rather
/// than falling back to a stub change catalog.
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
        // Keep the session SQL surface referenced so the adapter documents the
        // real OCI work remaining (bindings + supplemental-log reconstruction).
        let _sql = oci_logminer_session_sql();
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
