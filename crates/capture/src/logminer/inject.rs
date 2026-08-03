//! Test/Lab injection of LogMiner contents for contract Incremental Capture.
//!
//! On `contract`/`stub` hosts the product path loads **only** this inject file
//! (empty when unset)—named scenario Incremental rows are not baked in
//! (issue #120). Tests may include backlog rows here for bounded backpressure
//! (ADR-0020 / issue #26). Not a production Operator control.

use std::fs;
use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

use super::contents::{logminer_content_order, LogMinerContent, LogMinerOperation};
use crate::CaptureError;

/// Env var: path to a JSON file of LogMiner contents for the contract harness
/// Incremental stream (sole contents source on the product path).
pub const INJECT_LOGMINER_CONTENTS_ENV: &str = "MIGRALOOP_INJECT_LOGMINER_CONTENTS";

#[derive(Debug, Error)]
pub enum LogMinerInjectError {
    #[error("logminer contents inject file error: {0}")]
    Detail(String),
}

impl From<LogMinerInjectError> for CaptureError {
    fn from(err: LogMinerInjectError) -> Self {
        CaptureError::ContractCatalog {
            detail: err.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct InjectFile {
    contents: Vec<InjectContent>,
}

#[derive(Debug, Deserialize)]
struct InjectContent {
    scn: u64,
    operation: InjectOp,
    #[serde(default)]
    seg_owner: Option<String>,
    table_name: String,
    identity: std::collections::BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    after_image: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    /// Optional LogMiner `RS_ID` for same-SCN multi-row identity (issue #143).
    #[serde(default)]
    rs_id: Option<String>,
    /// Optional LogMiner `SSN` for same-SCN multi-row identity (issue #143).
    #[serde(default)]
    ssn: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum InjectOp {
    Insert,
    Update,
    Delete,
}

impl InjectOp {
    fn as_logminer(self) -> LogMinerOperation {
        match self {
            Self::Insert => LogMinerOperation::Insert,
            Self::Update => LogMinerOperation::Update,
            Self::Delete => LogMinerOperation::Delete,
        }
    }
}

/// Load injected LogMiner contents from [`INJECT_LOGMINER_CONTENTS_ENV`] when set.
///
/// Returns an empty list when the env var is unset.
pub fn load_injected_logminer_contents() -> Result<Vec<LogMinerContent>, LogMinerInjectError> {
    let Some(path) = std::env::var_os(INJECT_LOGMINER_CONTENTS_ENV) else {
        return Ok(Vec::new());
    };
    load_logminer_contents_file(Path::new(&path))
}

/// Load LogMiner contents from a JSON inject file.
pub fn load_logminer_contents_file(
    path: &Path,
) -> Result<Vec<LogMinerContent>, LogMinerInjectError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        LogMinerInjectError::Detail(format!("failed to read {}: {err}", path.display()))
    })?;
    let file: InjectFile = serde_json::from_str(&raw).map_err(|err| {
        LogMinerInjectError::Detail(format!(
            "invalid logminer contents inject JSON {}: {err}",
            path.display()
        ))
    })?;

    let mut out = Vec::with_capacity(file.contents.len());
    for entry in file.contents {
        out.push(
            LogMinerContent::new(
                entry.scn,
                entry.operation.as_logminer(),
                entry.seg_owner.unwrap_or_else(|| "APP".to_string()),
                entry.table_name,
                entry.identity,
                entry.after_image,
            )
            .with_order(entry.rs_id.unwrap_or_default(), entry.ssn.unwrap_or(0)),
        );
    }
    out.sort_by(logminer_content_order);
    Ok(out)
}
