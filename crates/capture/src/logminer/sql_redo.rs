//! Parse LogMiner `SQL_REDO` for INSERT after-images (#252).
//!
//! Under `DICT_FROM_ONLINE_CATALOG` + supplemental logging, Oracle emits INSERT
//! `SQL_REDO` like:
//! `insert into "OWNER"."TABLE"("ID","NAME") values ('10','hello');`
//! Numbers are quoted; SQL string escapes use doubled single quotes; SQL NULL
//! is the bare token `NULL`. Parsing that text avoids per-column
//! `DBMS_LOGMNR.MINE_VALUE` on insert-heavy Direct bursts (mega-mix).

use std::collections::BTreeMap;

use serde_json::Value;

use crate::oracle_source::mined_value_to_json;
use crate::{CaptureError, SourceColumn};

/// Reconstruct an INSERT after-image from LogMiner `SQL_REDO`.
pub fn parse_insert_sql_redo(
    sql_redo: &str,
    columns: &[SourceColumn],
    host: &str,
) -> Result<BTreeMap<String, Value>, CaptureError> {
    let trimmed = sql_redo.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("insert into ") {
        return Err(sql_redo_error(
            host,
            format!("expected INSERT SQL_REDO, got: {sql_redo}"),
        ));
    }

    let (col_names, values_sql) = split_insert_columns_and_values(trimmed).map_err(|detail| {
        sql_redo_error(host, format!("{detail}; sql_redo={sql_redo}"))
    })?;

    if col_names.len() != values_sql.len() {
        return Err(sql_redo_error(
            host,
            format!(
                "SQL_REDO column/value arity mismatch ({} cols vs {} values); sql_redo={sql_redo}",
                col_names.len(),
                values_sql.len()
            ),
        ));
    }

    let meta: BTreeMap<&str, &SourceColumn> = columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let mut after = BTreeMap::new();
    for (name, raw) in col_names.iter().zip(values_sql.iter()) {
        let Some(column) = meta.get(name.as_str()).copied() else {
            // Ignore columns not in the discovered supported set.
            continue;
        };
        let text = match raw.as_str() {
            "NULL" => None,
            other => Some(other.to_string()),
        };
        after.insert(name.clone(), mined_value_to_json(text, column)?);
    }

    if after.is_empty() {
        return Err(sql_redo_error(
            host,
            format!("SQL_REDO INSERT produced no supported columns; sql_redo={sql_redo}"),
        ));
    }
    Ok(after)
}

fn sql_redo_error(host: &str, detail: String) -> CaptureError {
    CaptureError::OciUnavailable { host: host.to_string(), detail }
}

/// Split `insert into "O"."T"("C1","C2") values ('v1','v2')` into column names
/// and decoded value tokens (`NULL` or unescaped string contents).
fn split_insert_columns_and_values(sql: &str) -> Result<(Vec<String>, Vec<String>), String> {
    let lower = sql.to_ascii_lowercase();
    let values_kw = lower
        .find(" values ")
        .ok_or_else(|| "SQL_REDO INSERT missing VALUES clause".to_string())?;
    let before_values = &sql[..values_kw];
    let after_values = sql[values_kw + " values ".len()..].trim();

    let cols_open = before_values
        .rfind('(')
        .ok_or_else(|| "SQL_REDO INSERT missing column list".to_string())?;
    let cols_close = before_values
        .rfind(')')
        .ok_or_else(|| "SQL_REDO INSERT missing column list close".to_string())?;
    if cols_close < cols_open {
        return Err("SQL_REDO INSERT column list bounds inverted".to_string());
    }
    let cols_body = &before_values[cols_open + 1..cols_close];
    let col_names = parse_quoted_ident_list(cols_body)?;

    let values_body = strip_outer_parens(after_values)
        .ok_or_else(|| "SQL_REDO INSERT VALUES missing parentheses".to_string())?;
    let values = parse_sql_value_list(values_body)?;
    Ok((col_names, values))
}

fn strip_outer_parens(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

fn parse_quoted_ident_list(body: &str) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    let mut rest = body.trim();
    while !rest.is_empty() {
        rest = rest.trim_start();
        if !rest.starts_with('"') {
            return Err(format!("expected quoted identifier in column list, got: {body}"));
        }
        let end = rest[1..]
            .find('"')
            .map(|i| i + 1)
            .ok_or_else(|| format!("unterminated quoted identifier in: {body}"))?;
        names.push(rest[1..end].to_string());
        rest = rest[end + 1..].trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if !rest.is_empty() {
            return Err(format!("unexpected trailing column-list text: {rest}"));
        }
    }
    if names.is_empty() {
        return Err("empty SQL_REDO column list".to_string());
    }
    Ok(names)
}

fn parse_sql_value_list(body: &str) -> Result<Vec<String>, String> {
    let mut values = Vec::new();
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        if i + 4 <= bytes.len()
            && body[i..i + 4].eq_ignore_ascii_case("NULL")
            && (i + 4 == bytes.len()
                || matches!(bytes[i + 4] as char, ',' | ' ' | '\t' | '\n' | '\r'))
        {
            values.push("NULL".to_string());
            i += 4;
        } else if bytes[i] == b'\'' {
            i += 1;
            let mut out = String::new();
            loop {
                if i >= bytes.len() {
                    return Err(format!("unterminated string in VALUES: {body}"));
                }
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                out.push(bytes[i] as char);
                i += 1;
            }
            values.push(out);
        } else {
            return Err(format!("unexpected VALUES token at offset {i}: {body}"));
        }
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
        } else if i < bytes.len() {
            return Err(format!("unexpected trailing VALUES text: {}", &body[i..]));
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<SourceColumn> {
        vec![
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
        ]
    }

    #[test]
    fn parses_oracle_insert_sql_redo_numbers_strings_null_and_quotes() {
        let columns = cols();
        let row = parse_insert_sql_redo(
            r#"insert into "SYNC_USER"."LAB_SQLREDO_PROBE"("ID","NAME") values ('10','hello');"#,
            &columns,
            "lab",
        )
        .expect("parse");
        assert_eq!(row["ID"], serde_json::json!(10));
        assert_eq!(row["NAME"], serde_json::json!("hello"));

        let escaped = parse_insert_sql_redo(
            r#"insert into "SYNC_USER"."T"("ID","NAME") values ('11','a''b')"#,
            &columns,
            "lab",
        )
        .expect("escaped");
        assert_eq!(escaped["NAME"], serde_json::json!("a'b"));

        let null_name = parse_insert_sql_redo(
            r#"insert into "SYNC_USER"."T"("ID","NAME") values ('12',NULL)"#,
            &columns,
            "lab",
        )
        .expect("null");
        assert!(null_name["NAME"].is_null());
    }

    #[test]
    fn rejects_non_insert_sql_redo() {
        let err = parse_insert_sql_redo(
            r#"update "SYNC_USER"."T" set "NAME" = 'x' where "ID" = '1';"#,
            &cols(),
            "lab",
        )
        .expect_err("update");
        assert!(err.to_string().to_ascii_lowercase().contains("insert"));
    }
}
