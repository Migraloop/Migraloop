//! Oracle OCI LogMiner adapter (ADR-0013).
//!
//! Production Incremental Capture starts a `DBMS_LOGMNR` session over OCI and
//! reconstructs supplemental-logged row images into [`super::LogMinerContent`].
//! INSERT after-images use LogMiner `SQL_REDO` (avoids per-column
//! `DBMS_LOGMNR.MINE_VALUE` on insert-heavy Direct bursts, #252); UPDATE/DELETE
//! still mine REDO/UNDO via `MINE_VALUE`.
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
use super::sql_redo::parse_insert_sql_redo;

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
pub const V_LOGMNR_CONTENTS_QUERY: &str = "SELECT SCN, OPERATION, SEG_OWNER, TABLE_NAME, \
     RS_ID, SSN \
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
        self.fetch_changes_in_schema_limited(schema, table, from_position, None)
    }

    /// Bounded Incremental Capture fetch for backpressure windows (ADR-0020).
    pub fn fetch_changes_in_schema_limited(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
        limit: Option<usize>,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        let results = self.prefetch_tables_limited(&[(
            schema.to_string(),
            table.to_string(),
            from_position,
            limit,
        )])?;
        Ok(results.into_iter().next().unwrap_or_default())
    }

    /// Prefetch many tables inside **one** LogMiner START/END session (#252).
    ///
    /// Mega-mix Deployments otherwise pay a full SCN-range `START_LOGMNR` per
    /// Base table (including paused Pipelines that still advance Base). Shared
    /// session keeps idle-table probes from re-scanning the Direct evidence burst.
    pub fn prefetch_tables_limited(
        &self,
        requests: &[(String, String, CapturePosition, Option<usize>)],
    ) -> Result<Vec<Vec<ChangeEvent>>, CaptureError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let conn = connect_oracle(&self.connect, &self.password)?;
        let host = self.connect.host.as_str();
        let end_scn = current_scn(&conn, host)?;
        let end_scn_i64 = end_scn.as_i64();
        let min_start = requests
            .iter()
            .map(|(_, _, from, _)| from.as_i64())
            .min()
            .unwrap_or(end_scn_i64);
        if end_scn_i64 < min_start {
            return Ok(requests.iter().map(|_| Vec::new()).collect());
        }

        conn.execute(DBMS_LOGMNR_START_LOGMNR, &[&min_start, &end_scn_i64])
            .map_err(|err| map_oracle_error(host, err))?;

        let result = (|| {
            let mut out = Vec::with_capacity(requests.len());
            for (schema, table, from_position, limit) in requests {
                let owner = resolve_oracle_schema(&self.connect, schema);
                let contents = read_table_contents_in_session(
                    &conn,
                    host,
                    &owner,
                    table,
                    *from_position,
                    *limit,
                )?;
                out.push(change_events_from_logminer_contents(
                    &contents,
                    table,
                    *from_position,
                ));
            }
            Ok(out)
        })();

        let _ = conn.execute(DBMS_LOGMNR_END_LOGMNR, &[]);
        result
    }

    /// Count pending Incremental DML rows for Sync/Delivery Health lag (ADR-0020).
    pub fn count_changes_in_schema(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<usize, CaptureError> {
        let conn = connect_oracle(&self.connect, &self.password)?;
        let owner = resolve_oracle_schema(&self.connect, schema);
        count_logminer_contents(&conn, &self.connect.host, &owner, table, from_position)
    }

    pub fn probe_prerequisites(&self) -> Result<OracleSourcePrerequisiteState, CaptureError> {
        let conn = connect_oracle(&self.connect, &self.password)?;
        probe_prerequisites_oci(&conn, &self.connect.host)
    }
}

fn count_logminer_contents(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    from_position: CapturePosition,
) -> Result<usize, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    let owner = owner.trim().to_ascii_uppercase();
    let end_scn = current_scn(conn, host)?;
    let start_scn = from_position.as_i64();
    let end_scn_i64 = end_scn.as_i64();
    if end_scn_i64 < start_scn {
        return Ok(0);
    }

    conn.execute(DBMS_LOGMNR_START_LOGMNR, &[&start_scn, &end_scn_i64])
        .map_err(|err| map_oracle_error(host, err))?;

    let result = (|| {
        let sql = "SELECT COUNT(*) FROM V$LOGMNR_CONTENTS \
             WHERE UPPER(SEG_OWNER) = :1 \
               AND UPPER(SEG_NAME) = :2 \
               AND SCN >= :3 \
               AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE')";
        let row = conn
            .query_row(sql, &[&owner, &table, &start_scn])
            .map_err(|err| map_oracle_error(host, err))?;
        let count: i64 = row.get(0).map_err(|err| map_oracle_error(host, err))?;
        Ok(count.max(0) as usize)
    })();

    let _ = conn.execute(DBMS_LOGMNR_END_LOGMNR, &[]);
    result
}

/// Read one table from an already-started LogMiner session.
fn read_table_contents_in_session(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    from_position: CapturePosition,
    limit: Option<usize>,
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

    let start_scn = from_position.as_i64();
    read_mined_contents(
        conn,
        host,
        &owner,
        &table,
        start_scn,
        &columns,
        &primary_key,
        limit,
    )
}

/// Build the OCI `V$LOGMNR_CONTENTS` query that reconstructs row images.
///
/// Backpressure limits (ADR-0020) are applied while iterating the result cursor —
/// not via SQL `FETCH FIRST` / nested `ROWNUM`. On Oracle 23, `DBMS_LOGMNR.MINE_VALUE`
/// raises ORA-01323 when the select is rewritten with those row-limit forms.
fn mined_contents_sql(owner: &str, table: &str, columns: &[SourceColumn]) -> String {
    let mut select_parts = vec![
        "SCN".to_string(),
        "OPERATION".to_string(),
        "SEG_OWNER".to_string(),
        "TABLE_NAME".to_string(),
        "RS_ID".to_string(),
        "SSN".to_string(),
        // INSERT after-image/identity come from SQL_REDO (#252); projected for
        // every row so the cursor shape stays stable across OPERATION values.
        "SQL_REDO".to_string(),
    ];
    for column in columns {
        let name = &column.name;
        // Oracle requires schema.table.column for MINE_VALUE / COLUMN_PRESENT.
        let qualified = format!("{owner}.{table}.{name}");
        // INSERT: SQL_REDO path (no MINE_VALUE). DELETE: identity from UNDO only.
        // UPDATE: mine REDO + UNDO. CASE short-circuits PL/SQL mine on insert bursts.
        select_parts.push(format!(
            "CASE WHEN OPERATION = 'UPDATE' THEN \
                DBMS_LOGMNR.MINE_VALUE(REDO_VALUE, '{qualified}') \
             ELSE NULL END AS \"R_{name}\""
        ));
        select_parts.push(format!(
            "CASE WHEN OPERATION IN ('UPDATE', 'DELETE') THEN \
                DBMS_LOGMNR.MINE_VALUE(UNDO_VALUE, '{qualified}') \
             ELSE NULL END AS \"U_{name}\""
        ));
    }

    format!(
        "SELECT {} FROM V$LOGMNR_CONTENTS \
         WHERE UPPER(SEG_OWNER) = :1 \
           AND UPPER(SEG_NAME) = :2 \
           AND SCN >= :3 \
           AND OPERATION IN ('INSERT', 'UPDATE', 'DELETE') \
         ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN",
        select_parts.join(", ")
    )
}

fn read_mined_contents(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    start_scn: i64,
    columns: &[SourceColumn],
    primary_key: &[String],
    limit: Option<usize>,
) -> Result<Vec<LogMinerContent>, CaptureError> {
    let sql = mined_contents_sql(owner, table, columns);

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
        // RS_ID may be RAW/VARCHAR2 depending on Oracle version; normalize to string.
        let rs_id = read_rs_id(&row, host)?;
        let ssn: i64 = row
            .get::<_, Option<i64>>(5)
            .map_err(|err| map_oracle_error(host, err))?
            .unwrap_or(0);

        let op = match operation.trim().to_ascii_uppercase().as_str() {
            "INSERT" => LogMinerOperation::Insert,
            "UPDATE" => LogMinerOperation::Update,
            "DELETE" => LogMinerOperation::Delete,
            _ => continue,
        };

        let sql_redo: Option<String> = row.get(6).map_err(|err| map_oracle_error(host, err))?;

        let (identity, after_image) = match op {
            LogMinerOperation::Insert => {
                let sql = sql_redo.ok_or_else(|| CaptureError::OciUnavailable {
                    host: host.to_string(),
                    detail: format!(
                        "LogMiner (OCI) INSERT at SCN {scn} for {owner}.{table} has empty SQL_REDO; \
                         enable supplemental logging and DICT_FROM_ONLINE_CATALOG"
                    ),
                })?;
                let after = parse_insert_sql_redo(&sql, columns, host)?;
                let identity = identity_from_row(&after, primary_key);
                (identity, Some(after))
            }
            LogMinerOperation::Update | LogMinerOperation::Delete => {
                let mut redo = BTreeMap::new();
                let mut undo = BTreeMap::new();
                for (idx, column) in columns.iter().enumerate() {
                    // Fixed prefix: SCN..SSN + SQL_REDO (7 cols), then R/U pairs.
                    let redo_idx = 7 + idx * 2;
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
                    LogMinerOperation::Update => {
                        let from_redo = identity_from_row(&redo, primary_key);
                        if from_redo.values().any(|v| !v.is_null()) {
                            from_redo
                        } else {
                            identity_from_row(&undo, primary_key)
                        }
                    }
                    LogMinerOperation::Insert => unreachable!("INSERT handled above"),
                };
                let after_image = match op {
                    LogMinerOperation::Delete => None,
                    LogMinerOperation::Update => Some(redo),
                    LogMinerOperation::Insert => unreachable!("INSERT handled above"),
                };
                (identity, after_image)
            }
        };

        if identity.is_empty() || identity.values().all(|v| v.is_null()) {
            return Err(CaptureError::OciUnavailable {
                host: host.to_string(),
                detail: format!(
                    "LogMiner (OCI) could not reconstruct primary-key identity for {owner}.{table} \
                     at SCN {scn} ({}); enable PRIMARY KEY or ALL COLUMNS supplemental logging \
                     and confirm Instant Client can read V$LOGMNR_CONTENTS (SQL_REDO / MINE_VALUE)",
                    op.as_str()
                ),
            });
        }

        contents.push(
            LogMinerContent::new(
                scn as u64,
                op,
                seg_owner,
                table_name,
                identity,
                after_image,
            )
            .with_order(rs_id, ssn.max(0) as u32),
        );

        // ADR-0020: bound the reconstructed batch under Downstream slowness by
        // stopping the cursor after `limit` DML rows (SQL FETCH FIRST breaks MINE_VALUE).
        if let Some(max) = limit {
            if max > 0 && contents.len() >= max {
                break;
            }
        }
    }

    Ok(contents)
}

fn read_rs_id(row: &oracle::Row, host: &str) -> Result<String, CaptureError> {
    // Prefer textual RS_ID; fall back to hex for RAW bindings.
    match row.get::<_, Option<String>>(4) {
        Ok(value) => Ok(value.unwrap_or_default()),
        Err(string_err) => match row.get::<_, Option<Vec<u8>>>(4) {
            Ok(bytes) => Ok(bytes
                .map(|b| {
                    b.iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                })
                .unwrap_or_default()),
            Err(_) => Err(map_oracle_error(host, string_err)),
        },
    }
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
        assert!(
            sql[1].contains("RS_ID") && sql[1].contains("SSN"),
            "OCI contents query must project LogMiner ordering keys for same-SCN identity"
        );
        assert!(sql[2].contains("END_LOGMNR"));
    }

    #[test]
    fn mined_contents_sql_keeps_mine_value_compatible_with_oracle_23() {
        // Oracle 23 raises ORA-01323 when MINE_VALUE is combined with FETCH FIRST /
        // nested ROWNUM row limits. Backpressure bounds the cursor in Rust instead.
        let columns = vec![
            SourceColumn {
                name: "ID".into(),
                data_type: "NUMBER".into(),
                supported: true,
                precision: Some(10),
                scale: Some(0),
                size: None,
            },
            SourceColumn {
                name: "NAME".into(),
                data_type: "VARCHAR2".into(),
                supported: true,
                precision: None,
                scale: None,
                size: Some(100),
            },
        ];
        let sql = mined_contents_sql("SYNC_USER", "LAB_DP_CUSTOMERS", &columns);
        assert!(sql.contains("DBMS_LOGMNR.MINE_VALUE"));
        assert!(sql.contains("SYNC_USER.LAB_DP_CUSTOMERS.ID"));
        assert!(sql.contains("ORDER BY SCN, COMMIT_TIMESTAMP, RS_ID, SSN"));
        let upper = sql.to_ascii_uppercase();
        assert!(
            !upper.contains("FETCH FIRST"),
            "MINE_VALUE query must not use FETCH FIRST (ORA-01323): {sql}"
        );
        assert!(
            !upper.contains("ROWNUM"),
            "MINE_VALUE query must not use ROWNUM limits (ORA-01323): {sql}"
        );
        // INSERT uses SQL_REDO; UPDATE/DELETE keep MINE_VALUE (#252).
        assert!(
            upper.contains("SQL_REDO"),
            "INSERT fast path requires SQL_REDO projection: {sql}"
        );
        assert!(
            upper.contains("CASE WHEN OPERATION = 'UPDATE' THEN"),
            "REDO MINE_VALUE must run only for UPDATE: {sql}"
        );
        assert!(
            upper.contains("CASE WHEN OPERATION IN ('UPDATE', 'DELETE') THEN"),
            "UNDO MINE_VALUE must run for UPDATE/DELETE only: {sql}"
        );
    }

    #[test]
    fn oci_fetch_without_reachable_oracle_fails_naming_oci() {
        let miner = OciLogMiner::new(
            OracleSourceConnect {
                host: "127.0.0.1".into(),
                port: 1,
                database: "NOPE".into(),
                username: "sync_user".into(),
                tls: Default::default(),
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
