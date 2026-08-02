//! Capture from a Source System into Base Datasets.
//!
//! v1 early slices use a stub/fixture Source; Oracle LogMiner lands in a later ticket.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "capture";

/// Stub stand-in for an Oracle SCN / LogMiner capture position.
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
    /// Capture position of this change (stub SCN).
    pub position: CapturePosition,
    /// Stable id for Platform Store dedupe of overlapping/replayed applies.
    pub change_id: String,
}

/// Column metadata discovered from the Source schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceColumn {
    pub name: String,
    pub oracle_type: String,
    pub supported: bool,
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

/// Perform Initial Load for a table from the stub/fixture Source.
///
/// Establishes a low-watermark capture position first, then reads the snapshot.
/// Incremental Capture must start from that watermark so cutover overlaps
/// (prefer duplicates over gaps; ADR-0004).
pub fn initial_load_stub(table: &str) -> Result<InitialLoadSnapshot, CaptureError> {
    match table {
        "CUSTOMERS" => Ok(customers_fixture()),
        "ORDERS" => Ok(orders_fixture()),
        other => Err(CaptureError::UnknownTable(other.to_string())),
    }
}

fn customers_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "CUSTOMERS".to_string(),
        // Watermark first: Incremental from 1000 will include the Alice→Alicia update
        // that this snapshot still shows as Alice (classic mid-window change).
        low_watermark: CUSTOMERS_LOW_WATERMARK,
        primary_key: vec!["ID".to_string()],
        columns: vec![
            col("ID", "NUMBER", true),
            col("NAME", "VARCHAR2", true),
            col("EMAIL", "VARCHAR2", true),
            col("ACTIVE", "NUMBER", true),
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
            col("ORDER_ID", "NUMBER", true),
            col("CUSTOMER_ID", "NUMBER", true),
            col("AMOUNT", "NUMBER", true),
        ],
        rows: vec![row(&[
            ("ORDER_ID", json_num(100)),
            ("CUSTOMER_ID", json_num(1)),
            ("AMOUNT", json_num(42)),
        ])],
    }
}

fn col(name: &str, oracle_type: &str, supported: bool) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        oracle_type: oracle_type.to_string(),
        supported,
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

/// Emit stub Incremental Capture changes at or after `from_position` (inclusive).
///
/// Cutover starts from the Initial Load low-watermark so the overlap window is
/// included. Later syncs pass the last applied checkpoint+1 semantics via
/// exclusive filtering in the caller when desired; this stub filters
/// `position >= from_position`.
pub fn incremental_changes_stub(
    table: &str,
    from_position: CapturePosition,
) -> Result<Vec<ChangeEvent>, CaptureError> {
    match table {
        "CUSTOMERS" => Ok(customers_incremental_fixture()
            .into_iter()
            .filter(|change| change.position >= from_position)
            .collect()),
        "ORDERS" => Ok(Vec::new()),
        other => Err(CaptureError::UnknownTable(other.to_string())),
    }
}

fn customers_incremental_fixture() -> Vec<ChangeEvent> {
    // Positions are after CUSTOMERS_LOW_WATERMARK (1000). The Alice→Alicia update at
    // 1050 is the classic "snapshot saw old value; Incremental Capture overlap must
    // not gap" case.
    vec![
        ChangeEvent {
            table: "CUSTOMERS".to_string(),
            op: ChangeOp::Update,
            identity: row(&[("ID", json_num(1))]),
            row: Some(row(&[
                ("ID", json_num(1)),
                ("NAME", json_str("Alicia")),
                ("EMAIL", json_str("alicia@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-alicia")),
            ])),
            position: CapturePosition(1050),
            change_id: "customers-scn-1050-update-1".to_string(),
        },
        ChangeEvent {
            table: "CUSTOMERS".to_string(),
            op: ChangeOp::Insert,
            identity: row(&[("ID", json_num(3))]),
            row: Some(row(&[
                ("ID", json_num(3)),
                ("NAME", json_str("Carol")),
                ("EMAIL", json_str("carol@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-carol")),
            ])),
            position: CapturePosition(1060),
            change_id: "customers-scn-1060-insert-3".to_string(),
        },
        ChangeEvent {
            table: "CUSTOMERS".to_string(),
            op: ChangeOp::Delete,
            identity: row(&[("ID", json_num(2))]),
            row: None,
            position: CapturePosition(1070),
            change_id: "customers-scn-1070-delete-2".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_load_establishes_low_watermark_before_snapshot_rows() {
        let snapshot = initial_load_stub("CUSTOMERS").expect("customers snapshot");
        assert_eq!(snapshot.low_watermark, CUSTOMERS_LOW_WATERMARK);
        assert!(snapshot.rows.iter().any(|r| r.get("NAME") == Some(&json_str("Alice"))));
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
}
