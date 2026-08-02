//! Live Oracle schema discovery and Initial Load (OCI).
//!
//! Contract/stub hosts keep the in-process fixture catalog for CI. Real hosts
//! always read the Source System over OCI — never the stub business-table catalog.

use std::collections::BTreeMap;

use oracle::Connection;
use serde_json::Value;

use crate::oracle_connect::{connect_oracle, map_oracle_error, resolve_oracle_schema};
use crate::oracle_types::{is_allow_listed_oracle_type, normalize_oracle_type};
use crate::{
    initial_load_stub, normalize_snapshot_temporals, source_schema_stub, CaptureError,
    CapturePosition, InitialLoadSnapshot, OracleSourceConnect, SourceColumn,
};

/// Discover Source columns for a Pipeline-referenced table.
///
/// - `host: contract` / `stub` → fixture catalog (tests / local slices)
/// - any other host → live Oracle data dictionary via OCI
pub fn discover_source_schema(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, CaptureError> {
    if source.is_contract_harness() {
        return source_schema_stub(table);
    }
    let conn = connect_oracle(source, password)?;
    let owner = resolve_oracle_schema(source, schema);
    discover_columns(&conn, &source.host, &owner, table)
}

/// Table-level Initial Load: low-watermark SCN first, then snapshot rows (ADR-0004).
pub fn initial_load_for_source(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
    configured_timezone: Option<&str>,
) -> Result<InitialLoadSnapshot, CaptureError> {
    if source.is_contract_harness() {
        return initial_load_stub(table, configured_timezone);
    }
    let conn = connect_oracle(source, password)?;
    let owner = resolve_oracle_schema(source, schema);
    let mut snapshot = initial_load_oci(&conn, &source.host, &owner, table)?;
    // Prefer configured Source timezone; otherwise readable DBTIMEZONE.
    let db_tz = read_db_timezone(&conn, &source.host).ok().flatten();
    let tz = configured_timezone.or(db_tz.as_deref());
    normalize_snapshot_temporals(&mut snapshot, tz)?;
    Ok(snapshot)
}

fn initial_load_oci(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
) -> Result<InitialLoadSnapshot, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    if table.is_empty() {
        return Err(CaptureError::UnknownTable(table));
    }

    // ADR-0004: establish low-watermark BEFORE reading snapshot rows.
    let low_watermark = current_scn(conn, host)?;
    let columns = discover_columns(conn, host, owner, &table)?;
    if columns.is_empty() {
        return Err(CaptureError::UnknownTable(format!("{owner}.{table}")));
    }
    let primary_key = discover_primary_key(conn, host, owner, &table)?;
    if primary_key.is_empty() {
        return Err(CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!(
                "Oracle table {owner}.{table} has no primary key; Direct Pipeline Output Identity requires a PK"
            ),
        });
    }

    let select_cols: Vec<String> = columns
        .iter()
        .map(|c| format!("TO_CHAR(\"{}\") AS \"{}\"", c.name, c.name))
        .collect();
    let sql = format!(
        "SELECT {} FROM \"{}\".\"{}\"",
        select_cols.join(", "),
        owner,
        table
    );

    let rows = conn.query(&sql, &[]).map_err(|err| map_oracle_error(host, err))?;
    let mut out_rows = Vec::new();
    for row_result in rows {
        let row = row_result.map_err(|err| map_oracle_error(host, err))?;
        let mut values = Vec::with_capacity(columns.len());
        for idx in 0..columns.len() {
            let v: Option<String> = row.get(idx).map_err(|err| map_oracle_error(host, err))?;
            values.push(v);
        }
        out_rows.push(values_to_json(values, &columns)?);
    }

    Ok(InitialLoadSnapshot {
        table,
        low_watermark,
        primary_key,
        columns,
        rows: out_rows,
    })
}

pub(crate) fn current_scn(conn: &Connection, host: &str) -> Result<CapturePosition, CaptureError> {
    let scn: i64 = conn
        .query_row_as::<(i64,)>("SELECT CURRENT_SCN FROM V$DATABASE", &[])
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    CapturePosition::from_i64(scn).ok_or_else(|| CaptureError::OciUnavailable {
        host: host.to_string(),
        detail: format!("V$DATABASE.CURRENT_SCN is not a valid capture position ({scn})"),
    })
}

pub(crate) fn discover_columns(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    let sql = "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH, DATA_LENGTH \
               FROM ALL_TAB_COLUMNS \
               WHERE OWNER = :1 AND TABLE_NAME = :2 \
               ORDER BY COLUMN_ID";
    let rows = conn
        .query_as::<(
            String,
            String,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        )>(sql, &[&owner, &table])
        .map_err(|err| map_oracle_error(host, err))?;

    let mut columns = Vec::new();
    for row_result in rows {
        let (column_name, data_type, data_precision, data_scale, char_length, data_length) =
            row_result.map_err(|err| map_oracle_error(host, err))?;
        let size = column_size(&data_type, char_length, data_length);
        let supported = is_allow_listed_oracle_type(&data_type, size);
        columns.push(SourceColumn {
            name: column_name,
            oracle_type: data_type,
            supported,
            precision: data_precision,
            scale: data_scale,
            size,
        });
    }
    Ok(columns)
}

pub(crate) fn discover_primary_key(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
) -> Result<Vec<String>, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    let sql = "SELECT ACC.COLUMN_NAME \
               FROM ALL_CONSTRAINTS AC \
               JOIN ALL_CONS_COLUMNS ACC \
                 ON AC.OWNER = ACC.OWNER \
                AND AC.CONSTRAINT_NAME = ACC.CONSTRAINT_NAME \
               WHERE AC.CONSTRAINT_TYPE = 'P' \
                 AND AC.OWNER = :1 \
                 AND AC.TABLE_NAME = :2 \
               ORDER BY ACC.POSITION";
    let rows = conn
        .query_as::<(String,)>(sql, &[&owner, &table])
        .map_err(|err| map_oracle_error(host, err))?;
    let mut pk = Vec::new();
    for row_result in rows {
        let (column_name,) = row_result.map_err(|err| map_oracle_error(host, err))?;
        pk.push(column_name);
    }
    Ok(pk)
}

fn read_db_timezone(conn: &Connection, host: &str) -> Result<Option<String>, CaptureError> {
    let tz: String = conn
        .query_row_as::<(String,)>("SELECT DBTIMEZONE FROM DUAL", &[])
        .map_err(|err| map_oracle_error(host, err))?
        .0;
    let tz = tz.trim().to_string();
    if tz.is_empty() {
        Ok(None)
    } else {
        Ok(Some(tz))
    }
}

fn column_size(
    data_type: &str,
    char_length: Option<i32>,
    data_length: Option<i32>,
) -> Option<i32> {
    let normalized = normalize_oracle_type(data_type);
    match normalized.as_str() {
        "RAW" | "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" => {
            if char_length.unwrap_or(0) > 0 {
                char_length
            } else {
                data_length
            }
        }
        _ => None,
    }
}

fn values_to_json(
    values: Vec<Option<String>>,
    columns: &[SourceColumn],
) -> Result<BTreeMap<String, Value>, CaptureError> {
    let mut map = BTreeMap::new();
    for (column, value) in columns.iter().zip(values.into_iter()) {
        map.insert(column.name.clone(), text_to_json(value, column)?);
    }
    Ok(map)
}

fn text_to_json(text: Option<String>, column: &SourceColumn) -> Result<Value, CaptureError> {
    let Some(text) = text else {
        return Ok(Value::Null);
    };
    let normalized = normalize_oracle_type(&column.oracle_type);
    match normalized.as_str() {
        "NUMBER" | "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => parse_number_json(&text),
        _ => Ok(Value::String(text)),
    }
}

fn parse_number_json(text: &str) -> Result<Value, CaptureError> {
    let trimmed = text.trim();
    if let Ok(n) = trimmed.parse::<i64>() {
        return Ok(Value::Number(n.into()));
    }
    if let Ok(n) = trimmed.parse::<u64>() {
        return Ok(Value::Number(n.into()));
    }
    // Keep decimal precision as string — Delivery maps via schema rules.
    Ok(Value::String(trimmed.to_string()))
}

/// Convert a LogMiner `MINE_VALUE` string into JSON using column metadata.
pub(crate) fn mined_value_to_json(
    text: Option<String>,
    column: &SourceColumn,
) -> Result<Value, CaptureError> {
    text_to_json(text, column)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_host_initial_load_still_uses_fixture_catalog() {
        let source = OracleSourceConnect {
            host: "contract".into(),
            port: 1521,
            database: "ORCL".into(),
            username: "sync_user".into(),
        };
        let snapshot = initial_load_for_source(&source, "unused", "APP", "CUSTOMERS", None)
            .expect("contract initial load");
        assert_eq!(snapshot.table, "CUSTOMERS");
        assert!(!snapshot.rows.is_empty());
    }

    #[test]
    fn real_host_without_reachable_oracle_names_oci_or_instant_client() {
        let source = OracleSourceConnect {
            host: "127.0.0.1".into(),
            port: 1,
            database: "NOPE".into(),
            username: "sync_user".into(),
        };
        let err = initial_load_for_source(&source, "bad", "APP", "CUSTOMERS", None)
            .expect_err("must not fall back to stub catalog");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("oci") || msg.contains("logminer") || msg.contains("instant client"),
            "expected OCI/LogMiner failure, got: {msg}"
        );
        assert!(
            !msg.contains("unknown stub"),
            "must not silently use stub catalog: {msg}"
        );
    }
}
