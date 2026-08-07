//! Normalized LogMiner contents → platform [`ChangeEvent`].
//!
//! Mirrors the fields Incremental Capture needs from `V$LOGMNR_CONTENTS`
//! (SCN, OPERATION, SEG_OWNER, TABLE_NAME, RS_ID, SSN, and reconstructed row
//! images). OCI INSERT reconstruction may parse `SQL_REDO` (#252); UPDATE/DELETE
//! still use `MINE_VALUE` images.

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
/// `rs_id` / `ssn` are LogMiner ordering keys that distinguish multiple rows at one SCN.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogMinerContent {
    pub scn: u64,
    pub operation: LogMinerOperation,
    pub seg_owner: String,
    pub table_name: String,
    pub identity: BTreeMap<String, Value>,
    pub after_image: Option<BTreeMap<String, Value>>,
    /// LogMiner `RS_ID` (row change identifier within an SCN).
    #[serde(default)]
    pub rs_id: String,
    /// LogMiner `SSN` (SQL sequence number within an `RS_ID`).
    #[serde(default)]
    pub ssn: u32,
}

impl LogMinerContent {
    /// Build a content row with empty LogMiner ordering keys.
    ///
    /// Prefer setting [`Self::rs_id`] / [`Self::ssn`] for same-SCN multi-row streams.
    pub fn new(
        scn: u64,
        operation: LogMinerOperation,
        seg_owner: impl Into<String>,
        table_name: impl Into<String>,
        identity: BTreeMap<String, Value>,
        after_image: Option<BTreeMap<String, Value>>,
    ) -> Self {
        Self {
            scn,
            operation,
            seg_owner: seg_owner.into(),
            table_name: table_name.into(),
            identity,
            after_image,
            rs_id: String::new(),
            ssn: 0,
        }
    }

    /// Attach LogMiner ordering keys (RS_ID / SSN).
    pub fn with_order(mut self, rs_id: impl Into<String>, ssn: u32) -> Self {
        self.rs_id = rs_id.into();
        self.ssn = ssn;
        self
    }
}

/// Compare LogMiner contents in capture order: SCN, then RS_ID, then SSN, then table.
pub fn logminer_content_order(a: &LogMinerContent, b: &LogMinerContent) -> std::cmp::Ordering {
    a.scn
        .cmp(&b.scn)
        .then_with(|| a.rs_id.cmp(&b.rs_id))
        .then_with(|| a.ssn.cmp(&b.ssn))
        .then_with(|| a.table_name.cmp(&b.table_name))
}

/// Stable Platform Store dedupe id derived from LogMiner SCN + ordering key + row identity.
///
/// Includes `RS_ID`/`SSN` so distinct LogMiner rows that share one SCN (and even the
/// same operation + PK) remain unique — SCN+op+identity alone is not enough.
pub fn logminer_change_id(content: &LogMinerContent) -> String {
    let mut identity_parts: Vec<String> = content
        .identity
        .iter()
        .map(|(k, v)| format!("{k}={}", value_key(v)))
        .collect();
    identity_parts.sort();
    format!(
        "logminer:{}:{}:{}:{}:{}:{}",
        content.table_name,
        content.scn,
        content.operation.as_str(),
        identity_parts.join(","),
        content.rs_id,
        content.ssn
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

/// Count matching LogMiner contents at or after `from_position` without materializing events.
pub fn count_logminer_contents(
    contents: &[LogMinerContent],
    table: &str,
    from_position: CapturePosition,
) -> usize {
    contents
        .iter()
        .filter(|row| row.table_name.eq_ignore_ascii_case(table))
        .filter(|row| CapturePosition(row.scn) >= from_position)
        .count()
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
            LogMinerContent::new(
                1050,
                LogMinerOperation::Update,
                "APP",
                "CUSTOMERS",
                BTreeMap::from([("ID".into(), num(1))]),
                Some(BTreeMap::from([
                    ("ID".into(), num(1)),
                    ("NAME".into(), s("Alicia")),
                ])),
            )
            .with_order("0x000001.00000001.0001", 1),
            LogMinerContent::new(
                1070,
                LogMinerOperation::Delete,
                "APP",
                "CUSTOMERS",
                BTreeMap::from([("ID".into(), num(2))]),
                None,
            )
            .with_order("0x000001.00000002.0001", 1),
        ];

        let events = change_events_from_logminer_contents(&rows, "CUSTOMERS", CapturePosition(1000));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].op, ChangeOp::Update);
        assert_eq!(events[0].position, CapturePosition(1050));
        assert!(events[0].change_id.starts_with("logminer:CUSTOMERS:1050:UPDATE:"));
        assert!(events[0].change_id.contains("0x000001.00000001.0001:1"));
        assert_eq!(events[1].op, ChangeOp::Delete);
        assert!(events[1].row.is_none());
    }

    #[test]
    fn filters_by_from_position_and_table() {
        let rows = vec![LogMinerContent::new(
            900,
            LogMinerOperation::Insert,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), num(9))]),
            Some(BTreeMap::from([("ID".into(), num(9))])),
        )];
        let events = change_events_from_logminer_contents(&rows, "CUSTOMERS", CapturePosition(1000));
        assert!(events.is_empty());
        let other = change_events_from_logminer_contents(&rows, "ORDERS", CapturePosition(1));
        assert!(other.is_empty());
    }

    #[test]
    fn same_scn_rows_get_distinct_change_ids_from_rs_id_ssn() {
        let a = LogMinerContent::new(
            1050,
            LogMinerOperation::Update,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), num(1))]),
            Some(BTreeMap::from([
                ("ID".into(), num(1)),
                ("NAME".into(), s("A1")),
            ])),
        )
        .with_order("0xAAA", 1);
        let b = LogMinerContent::new(
            1050,
            LogMinerOperation::Update,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), num(1))]),
            Some(BTreeMap::from([
                ("ID".into(), num(1)),
                ("NAME".into(), s("A2")),
            ])),
        )
        .with_order("0xAAA", 2);

        let id_a = logminer_change_id(&a);
        let id_b = logminer_change_id(&b);
        assert_ne!(
            id_a, id_b,
            "same SCN+op+identity must still be distinct via RS_ID/SSN"
        );
        assert!(id_a.ends_with(":0xAAA:1"));
        assert!(id_b.ends_with(":0xAAA:2"));
    }

    #[test]
    fn same_scn_events_preserve_rs_id_ssn_order() {
        let rows = vec![
            LogMinerContent::new(
                1050,
                LogMinerOperation::Update,
                "APP",
                "CUSTOMERS",
                BTreeMap::from([("ID".into(), num(1))]),
                Some(BTreeMap::from([
                    ("ID".into(), num(1)),
                    ("NAME".into(), s("A2")),
                ])),
            )
            .with_order("0xBBB", 2),
            LogMinerContent::new(
                1050,
                LogMinerOperation::Insert,
                "APP",
                "CUSTOMERS",
                BTreeMap::from([("ID".into(), num(4))]),
                Some(BTreeMap::from([
                    ("ID".into(), num(4)),
                    ("NAME".into(), s("Dana")),
                ])),
            )
            .with_order("0xCCC", 1),
            LogMinerContent::new(
                1050,
                LogMinerOperation::Update,
                "APP",
                "CUSTOMERS",
                BTreeMap::from([("ID".into(), num(1))]),
                Some(BTreeMap::from([
                    ("ID".into(), num(1)),
                    ("NAME".into(), s("A1")),
                ])),
            )
            .with_order("0xBBB", 1),
        ];
        let mut ordered = rows;
        ordered.sort_by(logminer_content_order);

        let events =
            change_events_from_logminer_contents(&ordered, "CUSTOMERS", CapturePosition(1000));
        assert_eq!(events.len(), 3);
        assert_eq!(
            events[0].row.as_ref().unwrap().get("NAME"),
            Some(&s("A1"))
        );
        assert_eq!(
            events[1].row.as_ref().unwrap().get("NAME"),
            Some(&s("A2"))
        );
        assert_eq!(
            events[2].row.as_ref().unwrap().get("NAME"),
            Some(&s("Dana"))
        );
    }
}
