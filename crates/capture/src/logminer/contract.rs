//! Contract Oracle LogMiner harness.
//!
//! Serves structured LogMiner contents (the shape OCI would return from
//! `V$LOGMNR_CONTENTS` after supplemental-logging reconstruction). Used when
//! Source host is `contract` or `stub` so operator-seam tests can exercise the
//! LogMiner product path without Instant Client.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::oracle_prerequisites::{
    probe_oracle_source_prerequisites_stub, OracleSourcePrerequisiteState,
};
use crate::{CaptureError, CapturePosition, ChangeEvent, CUSTOMERS_LOW_WATERMARK};

use super::contents::{
    change_events_from_logminer_contents, LogMinerContent, LogMinerOperation,
};

/// In-process LogMiner contents provider for contract / stub Source hosts.
#[derive(Debug, Clone)]
pub struct ContractLogMiner {
    contents: Vec<LogMinerContent>,
}

impl Default for ContractLogMiner {
    fn default() -> Self {
        Self::with_default_fixtures()
    }
}

impl ContractLogMiner {
    pub fn with_default_fixtures() -> Self {
        let mut contents = customers_logminer_fixture();
        contents.extend(orders_logminer_fixture());
        Self { contents }
    }

    pub fn with_contents(contents: Vec<LogMinerContent>) -> Self {
        Self { contents }
    }

    pub fn mechanism_label(&self) -> &'static str {
        "LogMiner (contract)"
    }

    pub fn fetch_changes(
        &self,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        // Unknown tables yield empty incremental streams (same as prior stub for
        // ORDERS/EVENTS/ACCOUNTS). Initial Load still owns table existence checks.
        Ok(change_events_from_logminer_contents(
            &self.contents,
            table,
            from_position,
        ))
    }

    pub fn probe_prerequisites(&self) -> OracleSourcePrerequisiteState {
        // Contract harness reuses the env-driven read-only probe (never mutates Source).
        probe_oracle_source_prerequisites_stub()
    }
}

fn customers_logminer_fixture() -> Vec<LogMinerContent> {
    // Positions after CUSTOMERS_LOW_WATERMARK (1000). Alice→Alicia at 1050 is the
    // classic mid-window cutover overlap case (ADR-0004).
    let _ = CUSTOMERS_LOW_WATERMARK;
    vec![
        LogMinerContent {
            scn: 1050,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(1))]),
            after_image: Some(row(&[
                ("ID", json_num(1)),
                ("NAME", json_str("Alicia")),
                ("EMAIL", json_str("alicia@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-alicia")),
            ])),
        },
        LogMinerContent {
            scn: 1060,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(3))]),
            after_image: Some(row(&[
                ("ID", json_num(3)),
                ("NAME", json_str("Carol")),
                ("EMAIL", json_str("carol@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-carol")),
            ])),
        },
        LogMinerContent {
            scn: 1070,
            operation: LogMinerOperation::Delete,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(2))]),
            after_image: None,
        },
    ]
}

fn orders_logminer_fixture() -> Vec<LogMinerContent> {
    // After ORDERS low-watermark (500):
    // 1) ADDRESS-only update (unused by sum(AMOUNT) Affect Analysis)
    // 2) AMOUNT update for customer 1 (used field → recompute that Output Identity)
    // 3) CUSTOMER_ID group-key move order 200: customer 2 → 3 (old+new identities; #18)
    vec![
        LogMinerContent {
            scn: 510,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(100))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(100)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("42.50")),
                ("ADDRESS", json_str("1 Main Ave")),
            ])),
        },
        LogMinerContent {
            scn: 520,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(100))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(100)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("50.00")),
                ("ADDRESS", json_str("1 Main Ave")),
            ])),
        },
        LogMinerContent {
            scn: 530,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(200))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(200)),
                ("CUSTOMER_ID", json_num(3)),
                ("AMOUNT", json_str("5.00")),
                ("ADDRESS", json_str("2 Side Rd")),
            ])),
        },
    ]
}

fn row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn json_str(value: &str) -> Value {
    Value::String(value.to_string())
}

fn json_num(value: i64) -> Value {
    Value::Number(value.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_emits_overlap_window_from_low_watermark() {
        let miner = ContractLogMiner::default();
        let changes = miner
            .fetch_changes("CUSTOMERS", CUSTOMERS_LOW_WATERMARK)
            .expect("fetch");
        assert_eq!(changes.len(), 3);
        assert!(changes[0].change_id.contains("logminer:CUSTOMERS:1050"));
    }

    #[test]
    fn contract_past_end_is_empty() {
        let miner = ContractLogMiner::default();
        let changes = miner
            .fetch_changes("CUSTOMERS", CapturePosition(1071))
            .expect("fetch");
        assert!(changes.is_empty());
    }
}
