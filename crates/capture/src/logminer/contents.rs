//! Normalized LogMiner contents → platform [`ChangeEvent`].
//!
//! Mirrors the fields Incremental Capture needs from `V$LOGMNR_CONTENTS`
//! (SCN, OPERATION, SEG_OWNER, TABLE_NAME, and reconstructed row images from
//! supplemental logging) without parsing SQL_REDO text.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CapturePosition, ChangeEvent, ChangeOp};

/// LogMiner DML operation (subset of `V$LOGMNR_CONTENTS.OPERATION`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogMinerOperation {
    Insert,
    Update,
    Delete,
}

impl LogMinerOperation {
    pub fn as_change_op(self) -> ChangeOp {
        match self {
            Self::Insert => ChangeOp::Insert,
            Self::Update => ChangeOp::Update,
            Self::Delete => ChangeOp::Delete,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// One reconstructed change from a LogMiner contents stream.
///
/// `identity` is the primary-key column map (from supplemental logging).
/// `after_image` is the full after-row for INSERT/UPDATE; `None` for DELETE.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogMinerContent {
    pub scn: u64,
    pub operation: LogMinerOperation,
    pub seg_owner: String,
    pub table_name: String,
    pub identity: BTreeMap<String, Value>,
    pub after_image: Option<BTreeMap<String, Value>>,
}

/// Stable Platform Store dedupe id derived from LogMiner SCN + row identity.
pub fn logminer_change_id(content: &LogMinerContent) -> String {
    let mut identity_parts: Vec<String> = content
        .identity
        .iter()
        .map(|(k, v)| format!("{k}={}", value_key(v)))
        .collect();
    identity_parts.sort();
    format!(
        "logminer:{}:{}:{}:{}",
        content.table_name,
        content.scn,
        content.operation.as_str(),
        identity_parts.join(",")
    )
}

fn value_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

/// Map LogMiner contents at or after `from_position` into platform change events.
///
/// Filters by table name (case-insensitive) and `scn >= from_position`.
/// When `limit` is `Some(n)`, returns at most `n` events (bounded Incremental window).
pub fn change_events_from_logminer_contents(
    contents: &[LogMinerContent],
    table: &str,
    from_position: CapturePosition,
) -> Vec<ChangeEvent> {
    change_events_from_logminer_contents_limited(contents, table, from_position, None)
}

/// Like [`change_events_from_logminer_contents`] with an optional max event count.
pub fn change_events_from_logminer_contents_limited(
    contents: &[LogMinerContent],
    table: &str,
    from_position: CapturePosition,
    limit: Option<usize>,
) -> Vec<ChangeEvent> {
    let iter = contents
        .iter()
        .filter(|row| row.table_name.eq_ignore_ascii_case(table))
        .filter(|row| CapturePosition(row.scn) >= from_position)
        .map(|row| ChangeEvent {
            table: row.table_name.clone(),
            op: row.operation.as_change_op(),
            identity: row.identity.clone(),
            row: row.after_image.clone(),
            position: CapturePosition(row.scn),
            change_id: logminer_change_id(row),
        });
    match limit {
        Some(n) => iter.take(n).collect(),
        None => iter.collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn num(v: i64) -> Value {
        Value::Number(v.into())
    }

    fn s(v: &str) -> Value {
        Value::String(v.to_string())
    }

    #[test]
    fn maps_insert_update_delete_preserving_scn_and_identity() {
        let rows = vec![
            LogMinerContent {
                scn: 1050,
                operation: LogMinerOperation::Update,
                seg_owner: "APP".into(),
                table_name: "CUSTOMERS".into(),
                identity: BTreeMap::from([("ID".into(), num(1))]),
                after_image: Some(BTreeMap::from([
                    ("ID".into(), num(1)),
                    ("NAME".into(), s("Alicia")),
                ])),
            },
            LogMinerContent {
                scn: 1070,
                operation: LogMinerOperation::Delete,
                seg_owner: "APP".into(),
                table_name: "CUSTOMERS".into(),
                identity: BTreeMap::from([("ID".into(), num(2))]),
                after_image: None,
            },
        ];

        let events = change_events_from_logminer_contents(&rows, "CUSTOMERS", CapturePosition(1000));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].op, ChangeOp::Update);
        assert_eq!(events[0].position, CapturePosition(1050));
        assert!(events[0].change_id.starts_with("logminer:CUSTOMERS:1050:UPDATE:"));
        assert_eq!(events[1].op, ChangeOp::Delete);
        assert!(events[1].row.is_none());
    }

    #[test]
    fn filters_by_from_position_and_table() {
        let rows = vec![LogMinerContent {
            scn: 900,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".into(),
            table_name: "CUSTOMERS".into(),
            identity: BTreeMap::from([("ID".into(), num(9))]),
            after_image: Some(BTreeMap::from([("ID".into(), num(9))])),
        }];
        let events = change_events_from_logminer_contents(&rows, "CUSTOMERS", CapturePosition(1000));
        assert!(events.is_empty());
        let other = change_events_from_logminer_contents(&rows, "ORDERS", CapturePosition(1));
        assert!(other.is_empty());
    }
}
