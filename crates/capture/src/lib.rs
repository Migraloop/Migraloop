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
        columns: vec![
            col("ID", "NUMBER", true),
            col("NAME", "VARCHAR2", true),
            col("EMAIL", "VARCHAR2", true),
            col("ACTIVE", "NUMBER", true),
            col("BIO", "BLOB", false),
        ],
        rows: vec![
            row(&[
                ("ID", json_num(1)),
                ("NAME", json_str("Alice")),
                ("EMAIL", json_str("alice@example.com")),
                ("ACTIVE", json_num(1)),
            ]),
            row(&[
                ("ID", json_num(2)),
                ("NAME", json_str("Bob")),
                ("EMAIL", json_str("bob@example.com")),
                ("ACTIVE", json_num(0)),
            ]),
        ],
    }
}

fn orders_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "ORDERS".to_string(),
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
