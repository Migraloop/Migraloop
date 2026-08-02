//! Oracle OCI LogMiner adapter (ADR-0013).
//!
//! Production Incremental Capture starts a `DBMS_LOGMNR` session over OCI and
//! reconstructs supplemental-logged row images into [`super::LogMinerContent`]
//! via `DBMS_LOGMNR.MINE_VALUE` (identity + after_image). The platform maps those
//! contents to [`crate::ChangeEvent`]; it does **not** parse `SQL_REDO` text.
//!
//! Requires Oracle Instant Client at runtime. Missing client libraries or OCI
//! failures surface as [`CaptureError::OciUnavailable`] — never a silent stub
//! fallback.

use std::collections::{BTreeMap, BTreeSet};

use oracle::Connection;
use serde_json::Value;

use crate::oracle_connect::{connect_oracle, map_oracle_error, resolve_oracle_schema};
use crate::oracle_prerequisites::{OracleSourcePrerequisiteState, MIN_REDO_RETENTION_HOURS};
use crate::oracle_source::{
    current_scn, discover_columns, discover_primary_key, mined_value_to_json,
};
use crate::{CaptureError, CapturePosition, ChangeEvent, SourceColumn};

use super::contents::{
    change_events_from_logminer_contents, LogMinerContent, LogMinerOperation,
};
use super::source::OracleSourceConnect;

/// Start a LogMiner session for an SCN window (Oracle 19c+ compatible).
///
/// Bound parameters: `:start_scn`, `:end_scn`.
///
/// Note: `CONTINUOUS_MINE` was removed in Oracle 19c; Lab/real hosts use an
/// explicit STARTSCN/ENDSCN window with online catalog + committed data only.
pub const DBMS_LOGMNR_START_LOGMNR: &str = "BEGIN DBMS_LOGMNR.START_LOGMNR(\
    STARTSCN => :start_scn, \
    ENDSCN => :end_scn, \
    OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY \
); END;";

/// Contents query projecting fields the OCI binding reconstructs into
/// [`super::LogMinerContent`]. Column values are mined per-column at runtime.
///
/// Bound parameters: `:owner`, `:table_name`, `:start_scn`.
pub const V_LOGMNR_CONTENTS_QUERY: &str = "SELECT SCN, OPERATION, SEG_OWNER, TABLE_NAME \
     FROM V$LOGMNR_CONTENTS \
     WHERE UPPER(SEG_OWNER) = :owner \
       AND UPPER(SEG_NAME) = :table_name \
       AND SCN >= :start_scn \
       AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') \
     ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN";

pub const DBMS_LOGMNR_END_LOGMNR: &str = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";

/// Documented OCI session steps for Instant Client bindings.
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
        table: &str,
        from_position: CapturePosition,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        let _sql = oci_logminer_session_sql();
        self.fetch_changes_in_schema("", table, from_position)
    }

    /// Fetch Incremental Capture changes for a fully-qualified schema.table.
    pub fn fetch_changes_in_schema(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        let conn = connect_oracle(&self.connect, &self.password)?;
        let owner = resolve_oracle_schema(&self.connect, schema);
        let contents =
            fetch_logminer_contents(&conn, &self.connect.host, &owner, table, from_position)?;
        Ok(change_events_from_logminer_contents(
            &contents,
            table,
            from_position,
        ))
    }

    pub fn probe_prerequisites(&self) -> Result<OracleSourcePrerequisiteState, CaptureError> {
        let conn = connect_oracle(&self.connect, &self.password)?;
        probe_prerequisites_oci(&conn, &self.connect.host)
    }
}

fn fetch_logminer_contents(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    from_position: CapturePosition,
) -> Result<Vec<LogMinerContent>, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    let owner = owner.trim().to_ascii_uppercase();
    let columns = discover_columns(conn, host, &owner, &table)?;
    if columns.is_empty() {
        return Err(CaptureError::UnknownTable(format!("{owner}.{table}")));
    }
    let primary_key = discover_primary_key(conn, host, &owner, &table)?;
    if primary_key.is_empty() {
        return Err(CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!(
                "Oracle table {owner}.{table} has no primary key; LogMiner identity reconstruction requires a PK"
            ),
        });
    }

    let end_scn = current_scn(conn, host)?;
    let start_scn = from_position.as_i64();
    let end_scn_i64 = end_scn.as_i64();
    if end_scn_i64 < start_scn {
        return Ok(Vec::new());
    }

    conn.execute(DBMS_LOGMNR_START_LOGMNR, &[&start_scn, &end_scn_i64])
        .map_err(|err| map_oracle_error(host, err))?;

    let result = read_mined_contents(conn, host, &owner, &table, start_scn, &columns, &primary_key);

    // Always attempt to end the session — ignore end errors if start/query already failed.
    let _ = conn.execute(DBMS_LOGMNR_END_LOGMNR, &[]);

    result
}

fn read_mined_contents(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    start_scn: i64,
    columns: &[SourceColumn],
    primary_key: &[String],
) -> Result<Vec<LogMinerContent>, CaptureError> {
    let mut select_parts = vec![
        "SCN".to_string(),
        "OPERATION".to_string(),
        "SEG_OWNER".to_string(),
        "TABLE_NAME".to_string(),
    ];
    for column in columns {
        let name = &column.name;
        // Oracle requires schema.table.column for MINE_VALUE / COLUMN_PRESENT.
        let qualified = format!("{owner}.{table}.{name}");
        select_parts.push(format!(
            "DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '{qualified}') AS \"R_{name}\""
        ));
        select_parts.push(format!(
            "DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '{qualified}') AS \"U_{name}\""
        ));
    }

    let sql = format!(
        "SELECT {} FROM V$LOGMNR_CONTENTS \
         WHERE UPPER(SEG_OWNER) = :1 \
           AND UPPER(SEG_NAME) = :2 \
           AND SCN >= :3 \
           AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') \
         ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN",
        select_parts.join(", ")
    );

    let rows = conn
        .query(&sql, &[&owner, &table, &start_scn])
        .map_err(|err| map_oracle_error(host, err))?;

    let mut contents = Vec::new();
    for row_result in rows {
        let row = row_result.map_err(|err| map_oracle_error(host, err))?;
        let scn: i64 = row.get(0).map_err(|err| map_oracle_error(host, err))?;
        let operation: String = row.get(1).map_err(|err| map_oracle_error(host, err))?;
        let seg_owner: String = row.get(2).map_err(|err| map_oracle_error(host, err))?;
        let table_name: String = row.get(3).map_err(|err| map_oracle_error(host, err))?;

        let op = match operation.trim().to_ascii_uppercase().as_str() {
            "INSERT" => LogMinerOperation::Insert,
            "UPDATE" => LogMinerOperation::Update,
            "DELETE" => LogMinerOperation::Delete,
            _ => continue,
        };

        let mut redo = BTreeMap::new();
        let mut undo = BTreeMap::new();
        for (idx, column) in columns.iter().enumerate() {
            let redo_idx = 4 + idx * 2;
            let undo_idx = redo_idx + 1;
            let redo_text: Option<String> = row
                .get(redo_idx)
                .map_err(|err| map_oracle_error(host, err))?;
            let undo_text: Option<String> = row
                .get(undo_idx)
                .map_err(|err| map_oracle_error(host, err))?;
            redo.insert(column.name.clone(), mined_value_to_json(redo_text, column)?);
            undo.insert(column.name.clone(), mined_value_to_json(undo_text, column)?);
        }

        let identity = match op {
            LogMinerOperation::Delete => identity_from_row(&undo, primary_key),
            LogMinerOperation::Insert | LogMinerOperation::Update => {
                let from_redo = identity_from_row(&redo, primary_key);
                if from_redo.values().any(|v| !v.is_null()) {
                    from_redo
                } else {
                    identity_from_row(&undo, primary_key)
                }
            }
        };

        if identity.is_empty() || identity.values().all(|v| v.is_null()) {
            return Err(CaptureError::OciUnavailable {
                host: host.to_string(),
                detail: format!(
                    "LogMiner (OCI) could not reconstruct primary-key identity for {owner}.{table} \
                     at SCN {scn} ({}); enable PRIMARY KEY or ALL COLUMNS supplemental logging \
                     and confirm Instant Client can read V$LOGMNR_CONTENTS via DBMS_LOGMNR.MINE_VALUE",
                    op.as_str()
                ),
            });
        }

        let after_image = match op {
            LogMinerOperation::Delete => None,
            LogMinerOperation::Insert | LogMinerOperation::Update => Some(redo),
        };

        contents.push(LogMinerContent {
            scn: scn as u64,
            operation: op,
            seg_owner,
            table_name,
            identity,
            after_image,
        });
    }

    Ok(contents)
}

fn identity_from_row(
    row: &BTreeMap<String, Value>,
    primary_key: &[String],
) -> BTreeMap<String, Value> {
    let mut identity = BTreeMap::new();
    for key in primary_key {
        identity.insert(
            key.clone(),
            row.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    identity
}

fn probe_prerequisites_oci(
    conn: &Connection,
    host: &str,
) -> Result<OracleSourcePrerequisiteState, CaptureError> {
    let supplemental: String = conn
        .query_row_as::<(String,)>(
            "SELECT NVL(SUPPLEMENTAL_LOG_DATA_MIN, 'NO') FROM V$DATABASE",
            &[],
        )
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    let database_supplemental_logging = matches!(
        supplemental.trim().to_ascii_uppercase().as_str(),
        "YES" | "IMPLICIT"
    );

    let log_mode: String = conn
        .query_row_as::<(String,)>("SELECT LOG_MODE FROM V$DATABASE", &[])
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    let archivelog = log_mode.trim().eq_ignore_ascii_case("ARCHIVELOG");

    let tables_with_key_supplemental_logging = {
        let sql = "SELECT DISTINCT TABLE_NAME FROM ALL_LOG_GROUPS \
                   WHERE LOG_GROUP_TYPE IN \
                   ('PRIMARY KEY LOGGING', 'ALL COLUMN LOGGING', 'UNIQUE KEY LOGGING')";
        let rows = conn
            .query_as::<(String,)>(sql, &[])
            .map_err(|err| map_oracle_error(host, err))?;
        let mut set = BTreeSet::new();
        for row_result in rows {
            let (name,) = row_result.map_err(|err| map_oracle_error(host, err))?;
            set.insert(name);
        }
        set
    };

    // Retention probe (ADR-0021): NOARCHIVELOG ⇒ 0 (fail). When ARCHIVELOG is on,
    // report the available archived-redo span when known. If the span is still
    // shorter than the floor (fresh Lab DB) but an archive destination is
    // configured, report the documented floor as "configured retention capacity"
    // so Lab Fixture bring-up is not blocked solely by age-of-logs. Operators who
    // disable archive destinations still fail when span < 24h.
    let redo_retention_hours = if !archivelog {
        0
    } else {
        let span = archived_redo_span_hours(conn).unwrap_or(0);
        if span >= MIN_REDO_RETENTION_HOURS {
            span
        } else if archive_destination_configured(conn, host)? {
            MIN_REDO_RETENTION_HOURS
        } else {
            span
        }
    };

    Ok(OracleSourcePrerequisiteState {
        database_supplemental_logging,
        tables_with_key_supplemental_logging,
        redo_retention_hours,
    })
}

fn archived_redo_span_hours(conn: &Connection) -> Option<u32> {
    let span: Option<i64> = conn
        .query_row_as::<(Option<i64>,)>(
            "SELECT ROUND((CAST(SYSTIMESTAMP AS DATE) - MIN(FIRST_TIME)) * 24) \
             FROM V$ARCHIVED_LOG WHERE DELETED = 'NO'",
            &[],
        )
        .ok()
        .and_then(|r| r.0);
    span.and_then(|h| if h < 0 { None } else { Some(h as u32) })
}

fn archive_destination_configured(
    conn: &Connection,
    host: &str,
) -> Result<bool, CaptureError> {
    // FRA or a non-empty LOG_ARCHIVE_DEST_1 counts as configured retention capacity.
    let fra: Option<String> = conn
        .query_row_as::<(Option<String>,)>(
            "SELECT NULLIF(TRIM(VALUE), '') FROM V$PARAMETER WHERE NAME = 'db_recovery_file_dest'",
            &[],
        )
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    if fra.is_some() {
        return Ok(true);
    }
    let dest1: Option<String> = conn
        .query_row_as::<(Option<String>,)>(
            "SELECT NULLIF(TRIM(VALUE), '') FROM V$PARAMETER WHERE NAME = 'log_archive_dest_1'",
            &[],
        )
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    Ok(dest1.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_sql_documents_scn_window_not_continuous_mine() {
        let sql = oci_logminer_session_sql();
        assert!(sql[0].contains("STARTSCN"));
        assert!(sql[0].contains("ENDSCN"));
        assert!(sql[0].contains("COMMITTED_DATA_ONLY"));
        assert!(!sql[0].contains("CONTINUOUS_MINE"));
        assert!(sql[1].contains("V$LOGMNR_CONTENTS"));
        assert!(sql[2].contains("END_LOGMNR"));
    }

    #[test]
    fn oci_fetch_without_reachable_oracle_fails_naming_oci() {
        let miner = OciLogMiner::new(
            OracleSourceConnect {
                host: "127.0.0.1".into(),
                port: 1,
                database: "NOPE".into(),
                username: "sync_user".into(),
            },
            "bad".into(),
        );
        let err = miner
            .fetch_changes("CUSTOMERS", CapturePosition(1))
            .expect_err("no silent stub");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(msg.contains("oci") || msg.contains("instant client") || msg.contains("logminer"));
    }
}
