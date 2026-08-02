//! Capture from a Source System into Base Datasets.
//!
//! Incremental Capture for Oracle is LogMiner-backed (ADR-0003 / ADR-0013):
//! contract harness for local/CI slices, OCI adapter for real Oracle hosts.
//! Initial Load / schema discovery use the live Oracle Source on real hosts
//! (fixture catalog remains for `host: contract` / `stub` only).

mod logminer;
mod oracle_connect;
mod oracle_prerequisites;
mod oracle_source;
mod oracle_types;

pub use logminer::{
    change_events_from_logminer_contents, logminer_change_id, open_oracle_incremental_capture,
    ContractLogMiner, IncrementalCapture, LogMinerContent, LogMinerOperation, OciLogMiner,
    OracleSourceConnect, DBMS_LOGMNR_END_LOGMNR, DBMS_LOGMNR_START_LOGMNR, V_LOGMNR_CONTENTS_QUERY,
};

// Re-export session helper used by the OCI adapter surface.
pub use logminer::oci_logminer_session_sql;
pub use oracle_connect::{oracle_connect_string, resolve_oracle_schema};
pub use oracle_prerequisites::{
    check_oracle_source_prerequisites, probe_oracle_source_prerequisites_stub,
    OracleSourcePrerequisiteState, PrerequisiteError, MIN_REDO_RETENTION_HOURS,
};
pub use oracle_source::{discover_source_schema, initial_load_for_source};
pub use oracle_types::{
    aware_temporal_to_utc, classify_number, is_allow_listed_oracle_type, naive_temporal_to_utc,
    normalize_oracle_type, resolve_temporal_timezone, NumberMongoMapping, ResolvedTimezone,
    TypeError, DECIMAL128_MAX_PRECISION, INT64_SAFE_PRECISION, RAW_SIZE_CAP_BYTES,
};

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "capture";

/// Oracle SCN / LogMiner capture position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CapturePosition(pub u64);

impl CapturePosition {
    pub fn as_i64(self) -> i64 {
        self.0 as i64
    }

    pub fn from_i64(value: i64) -> Option<Self> {
        if value < 0 {
            None
        } else {
            Some(Self(value as u64))
        }
    }
}

impl std::fmt::Display for CapturePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
pub enum CaptureError {
    #[error("unknown stub Source table: {0}")]
    UnknownTable(String),
    #[error(
        "Oracle LogMiner (OCI) unavailable for Source host {host}: {detail}"
    )]
    OciUnavailable { host: String, detail: String },
    #[error(transparent)]
    Type(#[from] TypeError),
}

/// Kind of Incremental Capture change from the Source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

/// One Incremental Capture change for a Source table row (by Output Identity / PK).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub table: String,
    pub op: ChangeOp,
    /// Primary-key columns locating the row (Direct Pipeline Output Identity).
    pub identity: BTreeMap<String, serde_json::Value>,
    /// Full source row for insert/update (may include unsupported columns to omit).
    /// `None` for delete.
    pub row: Option<BTreeMap<String, serde_json::Value>>,
    /// Capture position of this change (Oracle SCN from LogMiner).
    pub position: CapturePosition,
    /// Stable id for Platform Store dedupe of overlapping/replayed applies.
    pub change_id: String,
}

/// Column metadata discovered from the Source schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceColumn {
    pub name: String,
    pub oracle_type: String,
    /// True when the column is on the v1 allow-list (ADR-0018).
    pub supported: bool,
    /// Declared precision for NUMBER (None = unconstrained / unknown).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,
    /// Declared scale for NUMBER.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
    /// Declared size for RAW/CHAR-like types when relevant to allow-list caps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i32>,
}

impl SourceColumn {
    pub fn is_temporal_naive(&self) -> bool {
        // DATE, TIMESTAMP, and TIMESTAMP WITH LOCAL TIME ZONE are wall-clock /
        // session-local and need DB or configured timezone (ADR-0022).
        matches!(
            normalize_oracle_type(&self.oracle_type).as_str(),
            "DATE" | "TIMESTAMP" | "TIMESTAMP WITH LOCAL TIME ZONE"
        )
    }

    pub fn is_temporal_aware(&self) -> bool {
        matches!(
            normalize_oracle_type(&self.oracle_type).as_str(),
            "TIMESTAMP WITH TIME ZONE"
        )
    }

    pub fn is_number(&self) -> bool {
        normalize_oracle_type(&self.oracle_type) == "NUMBER"
    }
}

/// Result of a table-level Initial Load from the Source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitialLoadSnapshot {
    pub table: String,
    /// Low-watermark capture position established BEFORE this snapshot (ADR-0004).
    pub low_watermark: CapturePosition,
    /// Source primary-key column names; Direct Pipeline Output Identity defaults from these.
    pub primary_key: Vec<String>,
    pub columns: Vec<SourceColumn>,
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
}

impl InitialLoadSnapshot {
    pub fn supported_columns(&self) -> Vec<&SourceColumn> {
        self.columns.iter().filter(|c| c.supported).collect()
    }

    pub fn omitted_columns(&self) -> Vec<&SourceColumn> {
        self.columns.iter().filter(|c| !c.supported).collect()
    }
}

/// CUSTOMERS stub low-watermark (established before snapshot).
pub const CUSTOMERS_LOW_WATERMARK: CapturePosition = CapturePosition(1000);

/// Stub Source DB timezone when readable.
///
/// Set `MIGRALOOP_STUB_DB_TIMEZONE` (IANA name) to simulate a readable DB zone.
/// When unset, the stub reports no readable DB timezone (user must set source.timezone
/// for naive DATE/TIMESTAMP).
pub fn stub_db_timezone() -> Option<String> {
    std::env::var("MIGRALOOP_STUB_DB_TIMEZONE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Discover stub Source schema for a table (no temporal normalization).
pub fn source_schema_stub(table: &str) -> Result<Vec<SourceColumn>, CaptureError> {
    Ok(raw_fixture(table)?.columns)
}

fn raw_fixture(table: &str) -> Result<InitialLoadSnapshot, CaptureError> {
    match table {
        "CUSTOMERS" => Ok(customers_fixture()),
        "ORDERS" => Ok(orders_fixture()),
        "EVENTS" => Ok(events_fixture()),
        "ACCOUNTS" => Ok(accounts_fixture()),
        other => Err(CaptureError::UnknownTable(other.to_string())),
    }
}

/// Perform Initial Load for a table from the stub/fixture Source.
///
/// Establishes a low-watermark capture position first, then reads the snapshot.
/// Incremental Capture must start from that watermark so cutover overlaps
/// (prefer duplicates over gaps; ADR-0004).
///
/// Temporal values in the returned snapshot are normalized to UTC ISO-8601 strings
/// when `configured_timezone` / readable DB timezone allow (ADR-0022).
pub fn initial_load_stub(
    table: &str,
    configured_timezone: Option<&str>,
) -> Result<InitialLoadSnapshot, CaptureError> {
    let mut snapshot = raw_fixture(table)?;
    normalize_snapshot_temporals(&mut snapshot, configured_timezone)?;
    Ok(snapshot)
}

/// Normalize temporal fields in an Incremental change row to UTC (ADR-0022).
pub fn normalize_change_temporals(
    columns: &[SourceColumn],
    row: &mut BTreeMap<String, serde_json::Value>,
    configured_timezone: Option<&str>,
) -> Result<(), CaptureError> {
    let tz_needed = columns.iter().any(|c| c.supported && c.is_temporal_naive());
    let tz = if tz_needed {
        Some(resolve_temporal_timezone(
            stub_db_timezone().as_deref(),
            configured_timezone,
        )?)
    } else {
        None
    };

    for column in columns.iter().filter(|c| c.supported) {
        let Some(value) = row.get_mut(&column.name) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if column.is_temporal_naive() {
            let raw = value
                .as_str()
                .ok_or_else(|| {
                    TypeError::InvalidTemporal(value.to_string(), "expected string".into())
                })?
                .to_string();
            let utc = naive_temporal_to_utc(&raw, tz.expect("tz resolved"))?;
            *value = serde_json::Value::String(utc.to_rfc3339());
        } else if column.is_temporal_aware() {
            let raw = value
                .as_str()
                .ok_or_else(|| {
                    TypeError::InvalidTemporal(value.to_string(), "expected string".into())
                })?
                .to_string();
            let utc = aware_temporal_to_utc(&raw)?;
            *value = serde_json::Value::String(utc.to_rfc3339());
        }
    }
    Ok(())
}

pub(crate) fn normalize_snapshot_temporals(
    snapshot: &mut InitialLoadSnapshot,
    configured_timezone: Option<&str>,
) -> Result<(), CaptureError> {
    for row in &mut snapshot.rows {
        normalize_change_temporals(&snapshot.columns, row, configured_timezone)?;
    }
    Ok(())
}

fn customers_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "CUSTOMERS".to_string(),
        // Watermark first: Incremental from 1000 will include the Alice→Alicia update
        // that this snapshot still shows as Alice (classic mid-window change).
        low_watermark: CUSTOMERS_LOW_WATERMARK,
        primary_key: vec!["ID".to_string()],
        columns: vec![
            number_col("ID", 10, 0, true),
            col("NAME", "VARCHAR2", true),
            col("EMAIL", "VARCHAR2", true),
            number_col("ACTIVE", 1, 0, true),
            col("BIO", "BLOB", false),
        ],
        // Include unsupported BIO values so Initial Load must actively omit them.
        // Carol is in the snapshot AND as an Incremental INSERT after the low-watermark
        // (overlap duplicate absorbed idempotently; ADR-0004).
        rows: vec![
            row(&[
                ("ID", json_num(1)),
                ("NAME", json_str("Alice")),
                ("EMAIL", json_str("alice@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-alice")),
            ]),
            row(&[
                ("ID", json_num(2)),
                ("NAME", json_str("Bob")),
                ("EMAIL", json_str("bob@example.com")),
                ("ACTIVE", json_num(0)),
                ("BIO", json_str("blob-bytes-bob")),
            ]),
            row(&[
                ("ID", json_num(3)),
                ("NAME", json_str("Carol")),
                ("EMAIL", json_str("carol@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-carol")),
            ]),
        ],
    }
}

fn orders_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "ORDERS".to_string(),
        low_watermark: CapturePosition(500),
        primary_key: vec!["ORDER_ID".to_string()],
        columns: vec![
            number_col("ORDER_ID", 10, 0, true),
            number_col("CUSTOMER_ID", 10, 0, true),
            // Safe decimal → Decimal128 (not IEEE double).
            number_col("AMOUNT", 12, 2, true),
            // Unused by sum(AMOUNT) Affect Analysis scenarios (issue #17).
            col("ADDRESS", "VARCHAR2", true),
        ],
        rows: vec![
            row(&[
                ("ORDER_ID", json_num(100)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("42.50")),
                ("ADDRESS", json_str("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json_num(101)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("10.00")),
                ("ADDRESS", json_str("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json_num(200)),
                ("CUSTOMER_ID", json_num(2)),
                ("AMOUNT", json_str("5.00")),
                ("ADDRESS", json_str("2 Side Rd")),
            ]),
        ],
    }
}

fn events_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "EVENTS".to_string(),
        low_watermark: CapturePosition(2000),
        primary_key: vec!["EVENT_ID".to_string()],
        columns: vec![
            number_col("EVENT_ID", 10, 0, true),
            col("NAME", "VARCHAR2", true),
            // Naive DATE — needs DB timezone or source.timezone (ADR-0022).
            col("OCCURRED_AT", "DATE", true),
            col("AWARE_AT", "TIMESTAMP WITH TIME ZONE", true),
        ],
        rows: vec![row(&[
            ("EVENT_ID", json_num(1)),
            ("NAME", json_str("kickoff")),
            // Naive wall-clock; interpretation depends on timezone resolution.
            ("OCCURRED_AT", json_str("2024-01-15T10:30:00")),
            ("AWARE_AT", json_str("2024-01-15T10:30:00+09:00")),
        ])],
    }
}

fn accounts_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "ACCOUNTS".to_string(),
        low_watermark: CapturePosition(3000),
        primary_key: vec!["ACCOUNT_ID".to_string()],
        columns: vec![
            number_col("ACCOUNT_ID", 10, 0, true),
            col("NAME", "VARCHAR2", true),
            // Safe Long.
            number_col("BALANCE_CENTS", 18, 0, true),
            // Safe Decimal128.
            number_col("RATE", 10, 4, true),
            // Unsafe declared precision (>34) — must string or omit at apply (ADR-0023).
            number_col("HUGE_AMOUNT", 38, 10, true),
            // Unconstrained NUMBER — unsafe (never default IEEE double).
            SourceColumn {
                name: "LEGACY_NUM".to_string(),
                oracle_type: "NUMBER".to_string(),
                supported: true,
                precision: None,
                scale: None,
                size: None,
            },
        ],
        rows: vec![row(&[
            ("ACCOUNT_ID", json_num(1)),
            ("NAME", json_str("primary")),
            ("BALANCE_CENTS", json_num(123456789012345678)),
            ("RATE", json_str("1.2500")),
            ("HUGE_AMOUNT", json_str("123456789012345678901234567890.1234567890")),
            ("LEGACY_NUM", json_str("999")),
        ])],
    }
}

fn col(name: &str, oracle_type: &str, supported: bool) -> SourceColumn {
    let size = None;
    let supported = supported && is_allow_listed_oracle_type(oracle_type, size);
    SourceColumn {
        name: name.to_string(),
        oracle_type: oracle_type.to_string(),
        supported,
        precision: None,
        scale: None,
        size,
    }
}

fn number_col(name: &str, precision: i32, scale: i32, supported: bool) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        oracle_type: "NUMBER".to_string(),
        supported: supported && is_allow_listed_oracle_type("NUMBER", None),
        precision: Some(precision),
        scale: Some(scale),
        size: None,
    }
}

fn row(pairs: &[(&str, serde_json::Value)]) -> BTreeMap<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn json_str(value: &str) -> serde_json::Value {
    serde_json::Value::String(value.to_string())
}

fn json_num(value: i64) -> serde_json::Value {
    serde_json::Value::Number(value.into())
}

/// Emit Incremental Capture changes via the LogMiner contract harness.
///
/// Retained for unit-test convenience. Product CLI uses
/// [`open_oracle_incremental_capture`] so Incremental Capture is always
/// LogMiner-backed (contract or OCI).
pub fn incremental_changes_stub(
    table: &str,
    from_position: CapturePosition,
) -> Result<Vec<ChangeEvent>, CaptureError> {
    match table {
        "CUSTOMERS" | "ORDERS" | "EVENTS" | "ACCOUNTS" => {
            ContractLogMiner::default().fetch_changes(table, from_position)
        }
        other => Err(CaptureError::UnknownTable(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_load_establishes_low_watermark_before_snapshot_rows() {
        let snapshot =
            initial_load_stub("CUSTOMERS", None).expect("customers snapshot");
        assert_eq!(snapshot.low_watermark, CUSTOMERS_LOW_WATERMARK);
        assert!(snapshot
            .rows
            .iter()
            .any(|r| r.get("NAME") == Some(&json_str("Alice"))));
        assert!(!snapshot
            .rows
            .iter()
            .any(|r| r.get("NAME") == Some(&json_str("Alicia"))));
    }

    #[test]
    fn incremental_from_low_watermark_includes_overlap_window() {
        let changes = incremental_changes_stub("CUSTOMERS", CUSTOMERS_LOW_WATERMARK)
            .expect("incremental from watermark");
        assert_eq!(changes.len(), 3);
        assert!(changes.iter().any(|c| c.change_id.contains("1050")));
    }

    #[test]
    fn incremental_after_last_change_is_empty() {
        let changes =
            incremental_changes_stub("CUSTOMERS", CapturePosition(1071)).expect("past end");
        assert!(changes.is_empty());
    }

    #[test]
    fn events_initial_load_normalizes_naive_date_with_configured_timezone() {
        let snapshot = initial_load_stub("EVENTS", Some("America/New_York")).expect("events");
        let occurred = snapshot.rows[0]
            .get("OCCURRED_AT")
            .and_then(|v| v.as_str())
            .expect("OCCURRED_AT");
        assert_eq!(occurred, "2024-01-15T15:30:00+00:00");
        let aware = snapshot.rows[0]
            .get("AWARE_AT")
            .and_then(|v| v.as_str())
            .expect("AWARE_AT");
        assert_eq!(aware, "2024-01-15T01:30:00+00:00");
    }

    #[test]
    fn events_initial_load_fails_without_timezone_when_db_unreadable() {
        std::env::remove_var("MIGRALOOP_STUB_DB_TIMEZONE");
        let err = initial_load_stub("EVENTS", None).expect_err("needs tz");
        assert!(err.to_string().contains("timezone"));
    }

    #[test]
    fn blob_is_not_allow_listed() {
        let snapshot = initial_load_stub("CUSTOMERS", None).unwrap();
        let bio = snapshot.columns.iter().find(|c| c.name == "BIO").unwrap();
        assert!(!bio.supported);
        assert!(!is_allow_listed_oracle_type("BLOB", None));
    }
}
