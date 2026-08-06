//! Live Oracle schema discovery and Initial Load (OCI).
//!
//! Contract/stub hosts use the process [`crate::ContractSourceCatalog`] loaded
//! only from inject/override (no in-binary business fixtures; issue #120).
//! Real hosts always read the Source System over OCI — never a hard-coded
//! business-table catalog.

use std::collections::BTreeMap;

use oracle::Connection;
use serde_json::Value;

use crate::oracle_connect::{connect_oracle, map_oracle_error, resolve_oracle_schema};
use crate::oracle_types::{is_allow_listed_oracle_type, normalize_oracle_type};
use crate::{
    load_contract_source_catalog, normalize_snapshot_temporals, CaptureError, CapturePosition,
    InitialLoadChunk, InitialLoadChunkOptions, InitialLoadSnapshot, OracleSourceConnect,
    SourceColumn,
};

/// Discover Source columns for a Pipeline-referenced table.
///
/// - `host: contract` / `stub` → contract Source catalog (CI / local slices)
/// - any other host → live Oracle data dictionary via OCI
pub fn discover_source_schema(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, CaptureError> {
    if source.is_contract_harness() {
        return load_contract_source_catalog()?.schema(table);
    }
    let conn = connect_oracle(source, password)?;
    let owner = resolve_oracle_schema(source, schema);
    discover_columns(&conn, &source.host, &owner, table)
}

/// Table-level Initial Load: low-watermark SCN first, then snapshot rows (ADR-0004).
///
/// Assembles the full snapshot via the chunked path so the normal Source read is
/// never one unbounded slam (issue #124). Prefer [`initial_load_chunk_for_source`]
/// at the CLI/Deployment seam for streaming persist.
pub fn initial_load_for_source(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
    configured_timezone: Option<&str>,
) -> Result<InitialLoadSnapshot, CaptureError> {
    const ASSEMBLE_CHUNK: usize = 1000;
    let mut rows = Vec::new();
    let mut offset = 0usize;
    let mut established = None;
    let mut meta: Option<InitialLoadChunk> = None;
    loop {
        let chunk = initial_load_chunk_for_source(
            source,
            password,
            schema,
            table,
            configured_timezone,
            &InitialLoadChunkOptions {
                chunk_size: ASSEMBLE_CHUNK,
                offset,
                established_watermark: established,
            },
        )?;
        established = Some(chunk.low_watermark);
        offset = offset.saturating_add(chunk.rows.len());
        let exhausted = chunk.exhausted;
        rows.extend(chunk.rows.iter().cloned());
        if meta.is_none() {
            meta = Some(chunk);
        }
        if exhausted {
            break;
        }
    }
    let meta = meta.ok_or_else(|| CaptureError::ContractCatalog {
        detail: format!("Initial Load produced no chunks for table {table}"),
    })?;
    Ok(InitialLoadSnapshot {
        table: meta.table,
        low_watermark: meta.low_watermark,
        primary_key: meta.primary_key,
        columns: meta.columns,
        rows,
    })
}

/// Bounded Initial Load chunk: watermark first (or reuse durable), then at most
/// `chunk_size` rows ordered by primary key (issue #124 / ADR-0004).
pub fn initial_load_chunk_for_source(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
    configured_timezone: Option<&str>,
    options: &InitialLoadChunkOptions,
) -> Result<InitialLoadChunk, CaptureError> {
    if options.chunk_size == 0 {
        return Err(CaptureError::ContractCatalog {
            detail: "Initial Load chunk_size must be >= 1".to_string(),
        });
    }
    if source.is_contract_harness() {
        return load_contract_source_catalog()?.initial_load_chunk(
            table,
            configured_timezone,
            options,
        );
    }
    let conn = connect_oracle(source, password)?;
    let owner = resolve_oracle_schema(source, schema);
    let mut chunk = initial_load_chunk_oci(&conn, &source.host, &owner, table, options)?;
    let db_tz = read_db_timezone(&conn, &source.host).ok().flatten();
    let tz = configured_timezone.or(db_tz.as_deref());
    let mut snapshot = InitialLoadSnapshot {
        table: chunk.table.clone(),
        low_watermark: chunk.low_watermark,
        primary_key: chunk.primary_key.clone(),
        columns: chunk.columns.clone(),
        rows: chunk.rows,
    };
    normalize_snapshot_temporals(&mut snapshot, tz)?;
    chunk.rows = snapshot.rows;
    chunk.cursor_pk = chunk.rows.last().map(|last| {
        chunk
            .primary_key
            .iter()
            .map(|pk| last.get(pk).cloned().unwrap_or(Value::Null))
            .collect()
    });
    if chunk.rows.is_empty() {
        chunk.exhausted = true;
    }
    Ok(chunk)
}

/// Resource-gated Source read for Source Alignment Check (issue #24).
///
/// Reads at most `max_rows` Source rows (never a full slam by default). Does not
/// write the Source. Contract/stub hosts sample the contract catalog; live hosts
/// use bounded `FETCH FIRST` over OCI (plus one peek row for truncation — no
/// full-table `COUNT(*)`).
pub fn alignment_check_read_for_source(
    source: &OracleSourceConnect,
    password: &str,
    schema: &str,
    table: &str,
    max_rows: u32,
    configured_timezone: Option<&str>,
) -> Result<crate::AlignmentCheckSample, CaptureError> {
    if max_rows == 0 {
        return Err(CaptureError::ContractCatalog {
            detail: "Source Alignment Check max_rows must be >= 1".to_string(),
        });
    }
    if source.is_contract_harness() {
        let snapshot = load_contract_source_catalog()?.initial_load(table, configured_timezone)?;
        let total = snapshot.rows.len();
        let take = (max_rows as usize).min(total);
        let truncated = total > take;
        return Ok(crate::AlignmentCheckSample {
            table: snapshot.table,
            primary_key: snapshot.primary_key,
            columns: snapshot.columns,
            rows: snapshot.rows.into_iter().take(take).collect(),
            truncated,
            source_row_count: Some(total),
        });
    }
    let conn = connect_oracle(source, password)?;
    let owner = resolve_oracle_schema(source, schema);
    let mut sample = alignment_check_read_oci(&conn, &source.host, &owner, table, max_rows)?;
    let db_tz = read_db_timezone(&conn, &source.host).ok().flatten();
    let tz = configured_timezone.or(db_tz.as_deref());
    // Reuse Initial Load temporal normalization on the sampled rows.
    let mut snapshot = InitialLoadSnapshot {
        table: sample.table.clone(),
        low_watermark: CapturePosition(0),
        primary_key: sample.primary_key.clone(),
        columns: sample.columns.clone(),
        rows: sample.rows,
    };
    normalize_snapshot_temporals(&mut snapshot, tz)?;
    sample.rows = snapshot.rows;
    Ok(sample)
}

fn alignment_check_read_oci(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    max_rows: u32,
) -> Result<crate::AlignmentCheckSample, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    if table.is_empty() {
        return Err(CaptureError::UnknownTable(table));
    }
    let columns = discover_columns(conn, host, owner, &table)?;
    if columns.is_empty() {
        return Err(CaptureError::UnknownTable(format!("{owner}.{table}")));
    }
    let primary_key = discover_primary_key(conn, host, owner, &table)?;
    if primary_key.is_empty() {
        return Err(CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!(
                "Oracle table {owner}.{table} has no primary key; Source Alignment Check requires a PK"
            ),
        });
    }

    let select_cols: Vec<String> = columns
        .iter()
        .map(|c| format!("TO_CHAR(\"{}\") AS \"{}\"", c.name, c.name))
        .collect();
    // ORDER BY PK for a stable resource-gated window. Fetch one extra row to
    // detect truncation without a full-table COUNT(*) slam (CONTEXT.md).
    let order_cols: Vec<String> = primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect();
    let fetch_limit = u64::from(max_rows).saturating_add(1);
    let sql = format!(
        "SELECT {} FROM \"{}\".\"{}\" ORDER BY {} FETCH FIRST {} ROWS ONLY",
        select_cols.join(", "),
        owner,
        table,
        order_cols.join(", "),
        fetch_limit
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

    let truncated = out_rows.len() as u64 > u64::from(max_rows);
    if truncated {
        out_rows.truncate(max_rows as usize);
    }
    Ok(crate::AlignmentCheckSample {
        table,
        primary_key,
        columns,
        rows: out_rows,
        truncated,
        // Live OCI avoids COUNT(*); contract catalog still reports known totals.
        source_row_count: None,
    })
}

fn initial_load_chunk_oci(
    conn: &Connection,
    host: &str,
    owner: &str,
    table: &str,
    options: &InitialLoadChunkOptions,
) -> Result<InitialLoadChunk, CaptureError> {
    let table = table.trim().to_ascii_uppercase();
    if table.is_empty() {
        return Err(CaptureError::UnknownTable(table));
    }

    // ADR-0004: establish low-watermark BEFORE reading the first chunk; resume reuses it.
    let low_watermark = if let Some(wm) = options.established_watermark {
        wm
    } else {
        current_scn(conn, host)?
    };
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
    let order_cols: Vec<String> = primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect();

    // PK-ordered OFFSET/FETCH window: bounded read, correct resume via durable row_count.
    // Fetch one extra row to detect exhaustion without COUNT(*).
    let fetch_limit = (options.chunk_size as u64).saturating_add(1);
    let offset = options.offset as u64;
    let sql = format!(
        "SELECT {} FROM \"{}\".\"{}\" ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
        select_cols.join(", "),
        owner,
        table,
        order_cols.join(", "),
        offset,
        fetch_limit
    );

    let rows = conn
        .query(&sql, &[])
        .map_err(|err| map_oracle_error(host, err))?;

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

    let exhausted = out_rows.len() <= options.chunk_size;
    if !exhausted {
        out_rows.truncate(options.chunk_size);
    }
    let cursor_pk = out_rows.last().map(|last| {
        primary_key
            .iter()
            .map(|pk| last.get(pk).cloned().unwrap_or(Value::Null))
            .collect()
    });

    Ok(InitialLoadChunk {
        table,
        low_watermark,
        primary_key,
        columns,
        rows: out_rows,
        cursor_pk,
        exhausted,
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
            data_type,
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
    let normalized = normalize_oracle_type(&column.data_type);
    match normalized.as_str() {
        "NUMBER" => parse_number_json(&text, column.scale),
        "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => parse_number_json(&text, None),
        _ => Ok(Value::String(text)),
    }
}

fn parse_number_json(text: &str, scale: Option<i32>) -> Result<Value, CaptureError> {
    let trimmed = text.trim();
    // Non-zero scale NUMBER must stay a decimal string so addToSet / Delivery /
    // Lab inspect keep ADR-0023 precision (Oracle often returns `10` for 10.00).
    if let Some(scale) = scale.filter(|s| *s > 0) {
        return Ok(Value::String(format_decimal_with_scale(trimmed, scale as usize)));
    }
    if let Ok(n) = trimmed.parse::<i64>() {
        return Ok(Value::Number(n.into()));
    }
    if let Ok(n) = trimmed.parse::<u64>() {
        return Ok(Value::Number(n.into()));
    }
    // Keep decimal precision as string — Delivery maps via schema rules.
    Ok(Value::String(trimmed.to_string()))
}

/// Format a numeric text to exactly `scale` fractional digits (no IEEE double).
fn format_decimal_with_scale(text: &str, scale: usize) -> String {
    let negative = text.starts_with('-');
    let body = text.trim_start_matches('+').trim_start_matches('-');
    let (whole, frac) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };
    let whole = if whole.is_empty() { "0" } else { whole };
    let mut frac_digits: String = frac.chars().filter(|c| c.is_ascii_digit()).collect();
    if frac_digits.len() > scale {
        frac_digits.truncate(scale);
    } else {
        while frac_digits.len() < scale {
            frac_digits.push('0');
        }
    }
    if negative {
        format!("-{whole}.{frac_digits}")
    } else {
        format!("{whole}.{frac_digits}")
    }
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
    use crate::{
        clear_contract_source_catalog_override, col, number_col, set_contract_source_catalog_override,
        snapshot, ContractSourceCatalog,
    };
    use serde_json::json;

    #[test]
    fn number_with_scale_preserves_fractional_string() {
        assert_eq!(
            parse_number_json("10", Some(2)).unwrap(),
            Value::String("10.00".into())
        );
        assert_eq!(
            parse_number_json("42.5", Some(2)).unwrap(),
            Value::String("42.50".into())
        );
        assert_eq!(
            parse_number_json("7.00", Some(2)).unwrap(),
            Value::String("7.00".into())
        );
        // scale 0 / absent still prefers JSON integers.
        assert_eq!(
            parse_number_json("10", Some(0)).unwrap(),
            Value::Number(10.into())
        );
    }

    #[test]
    fn contract_host_initial_load_without_inject_rejects_former_fixture_table() {
        clear_contract_source_catalog_override();
        let source = OracleSourceConnect {
            host: "contract".into(),
            port: 1521,
            database: "ORCL".into(),
            username: "sync_user".into(),
            tls: Default::default(),
        };
        let err = initial_load_for_source(&source, "unused", "APP", "CUSTOMERS", None)
            .expect_err("no in-binary fixture catalog");
        assert!(
            err.to_string().contains("unknown Source table"),
            "got: {err}"
        );
    }

    #[test]
    fn contract_alignment_check_read_respects_max_rows_budget() {
        clear_contract_source_catalog_override();
        set_contract_source_catalog_override(ContractSourceCatalog::with_default_fixtures());
        let source = OracleSourceConnect {
            host: "stub".into(),
            port: 1521,
            database: "STUB".into(),
            username: "sync_user".into(),
            tls: Default::default(),
        };
        let sample = alignment_check_read_for_source(&source, "unused", "APP", "CUSTOMERS", 1, None)
            .expect("gated alignment read");
        clear_contract_source_catalog_override();
        assert_eq!(sample.rows.len(), 1);
        assert!(sample.truncated);
        assert_eq!(sample.source_row_count, Some(3));
        assert_eq!(sample.rows[0].get("NAME"), Some(&json!("Alice")));
    }

    #[test]
    fn contract_host_discovers_and_loads_injected_non_fixture_table() {
        clear_contract_source_catalog_override();
        let mut catalog = ContractSourceCatalog::empty();
        let mut row = BTreeMap::new();
        row.insert("WID".into(), json!(7));
        row.insert("LABEL".into(), json!("gamma"));
        row.insert("PHOTO".into(), json!("blob"));
        catalog.insert(snapshot(
            "WIDGETS",
            9000,
            &["WID"],
            vec![
                number_col("WID", 10, 0, true),
                col("LABEL", "VARCHAR2", true),
                col("PHOTO", "BLOB", false),
            ],
            vec![row],
        ));
        set_contract_source_catalog_override(catalog);

        let source = OracleSourceConnect {
            host: "contract".into(),
            port: 1521,
            database: "ORCL".into(),
            username: "sync_user".into(),
            tls: Default::default(),
        };
        let columns = discover_source_schema(&source, "unused", "APP", "WIDGETS")
            .expect("discover injected table");
        assert!(columns.iter().any(|c| c.name == "LABEL" && c.supported));
        assert!(columns.iter().any(|c| c.name == "PHOTO" && !c.supported));

        let loaded = initial_load_for_source(&source, "unused", "APP", "WIDGETS", None)
            .expect("initial load injected table");
        assert_eq!(loaded.table, "WIDGETS");
        assert_eq!(loaded.rows[0].get("LABEL"), Some(&json!("gamma")));
        assert_eq!(loaded.omitted_columns()[0].name, "PHOTO");

        clear_contract_source_catalog_override();
    }

    #[test]
    fn real_host_without_reachable_oracle_names_oci_or_instant_client() {
        clear_contract_source_catalog_override();
        let source = OracleSourceConnect {
            host: "127.0.0.1".into(),
            port: 1,
            database: "NOPE".into(),
            username: "sync_user".into(),
            tls: Default::default(),
        };
        let err = initial_load_for_source(&source, "bad", "APP", "CUSTOMERS", None)
            .expect_err("must not fall back to stub catalog");
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("oci") || msg.contains("logminer") || msg.contains("instant client"),
            "expected OCI/LogMiner failure, got: {msg}"
        );
        assert!(
            !msg.contains("unknown source table"),
            "must not silently use contract catalog: {msg}"
        );
    }
}
