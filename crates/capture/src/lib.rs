//! Capture from a Source System into Base Datasets.
//!
//! v1 early slices use a stub/fixture Source; Oracle LogMiner lands in a later ticket.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "capture";

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

/// Perform Initial Load for a table from the stub/fixture Source.
///
/// The stub exposes CUSTOMERS (with an unsupported BLOB) and ORDERS so capture
/// scope can be verified: only Pipeline-referenced tables are loaded.
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
        primary_key: vec!["ID".to_string()],
        columns: vec![
            col("ID", "NUMBER", true),
            col("NAME", "VARCHAR2", true),
            col("EMAIL", "VARCHAR2", true),
            col("ACTIVE", "NUMBER", true),
            col("BIO", "BLOB", false),
        ],
        // Include unsupported BIO values so Initial Load must actively omit them.
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
        ],
    }
}

fn orders_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "ORDERS".to_string(),
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

/// Emit stub Incremental Capture changes for a table after Initial Load.
///
/// Happy-path fixture for Direct Pipeline: update, insert, and delete against
/// CUSTOMERS. Does not model no-gap cutover or resumable checkpoints.
pub fn incremental_changes_stub(table: &str) -> Result<Vec<ChangeEvent>, CaptureError> {
    match table {
        "CUSTOMERS" => Ok(customers_incremental_fixture()),
        "ORDERS" => Ok(Vec::new()),
        other => Err(CaptureError::UnknownTable(other.to_string())),
    }
}

fn customers_incremental_fixture() -> Vec<ChangeEvent> {
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
        },
        ChangeEvent {
            table: "CUSTOMERS".to_string(),
            op: ChangeOp::Delete,
            identity: row(&[("ID", json_num(2))]),
            row: None,
        },
    ]
}
