//! Pluggable Oracle Incremental Capture backends behind LogMiner (ADR-0003).

use crate::oracle_prerequisites::OracleSourcePrerequisiteState;
use crate::{CaptureError, CapturePosition, ChangeEvent};

use super::contract::ContractLogMiner;
use super::oci::OciLogMiner;

/// Non-secret Oracle Source connection identity used to select a capture backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleSourceConnect {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
}

impl OracleSourceConnect {
    pub fn is_contract_harness(&self) -> bool {
        let host = self.host.trim();
        host.eq_ignore_ascii_case("contract") || host.eq_ignore_ascii_case("stub")
    }
}

/// Opened Incremental Capture handle for an Oracle Source System.
#[derive(Debug, Clone)]
pub enum IncrementalCapture {
    Contract(ContractLogMiner),
    Oci(OciLogMiner),
}

impl IncrementalCapture {
    /// Operator-visible mechanism label (always LogMiner-backed).
    pub fn mechanism_label(&self) -> &'static str {
        match self {
            Self::Contract(c) => c.mechanism_label(),
            Self::Oci(o) => o.mechanism_label(),
        }
    }

    pub fn fetch_changes(
        &self,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        match self {
            Self::Contract(c) => c.fetch_changes(table, from_position),
            Self::Oci(o) => o.fetch_changes(table, from_position),
        }
    }

    pub fn probe_prerequisites(&self) -> Result<OracleSourcePrerequisiteState, CaptureError> {
        match self {
            Self::Contract(c) => Ok(c.probe_prerequisites()),
            Self::Oci(o) => o.probe_prerequisites(),
        }
    }
}

/// Open LogMiner-backed Incremental Capture for the given Oracle Source.
///
/// - `host: contract` / `host: stub` → contract LogMiner harness
/// - any other host → OCI LogMiner adapter (fails fast without Instant Client)
pub fn open_oracle_incremental_capture(
    source: &OracleSourceConnect,
    password: &str,
) -> Result<IncrementalCapture, CaptureError> {
    if source.is_contract_harness() {
        Ok(IncrementalCapture::Contract(ContractLogMiner::default()))
    } else {
        Ok(IncrementalCapture::Oci(OciLogMiner::new(
            source.clone(),
            password.to_string(),
        )))
    }
}
