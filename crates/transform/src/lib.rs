//! Rich Transform operators and Affect Analysis.
//!
//! v1 MVP: declarative project + filter (eq) over Base Dataset rows.
//! Free-form scripts and unanalyzable operators are rejected at parse time.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "transform";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransformError {
    #[error("Transform Pipeline rejects free-form scripts; use declarative analyzable operators only (project/filter)")]
    FreeFormScript,
    #[error("unsupported Rich Transform operator: {0}; v1 MVP allows only project and filter")]
    UnsupportedOperator(String),
    #[error("invalid Rich Transform: {0}")]
    Invalid(String),
}

/// One analyzable Rich Transform operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransformOp {
    Project { fields: Vec<String> },
    FilterEq { field: String, value: Value },
}

/// Parse one declarative transform step JSON object into an analyzable operator.
///
/// Accepted shapes:
/// - `{ "project": { "fields": [...] } }`
/// - `{ "filter": { "field": "...", "eq": ... } }`
/// Rejected shapes (clear errors):
/// - `{ "script": "..." }` / `{ "function": "..." }`
/// - any other operator object
/// - malformed project/filter (reported as invalid, not unsupported)
pub fn parse_transform_step_value(step: &Value) -> Result<TransformOp, TransformError> {
    let obj = step.as_object().ok_or_else(|| {
        TransformError::Invalid("each transform step must be an object".to_string())
    })?;

    if obj.contains_key("script") || obj.contains_key("function") || obj.contains_key("$function")
    {
        return Err(TransformError::FreeFormScript);
    }

    if obj.contains_key("project") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "project step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_project(obj.get("project").expect("project key"));
    }

    if obj.contains_key("filter") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "filter step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_filter(obj.get("filter").expect("filter key"));
    }

    let name = obj
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "(empty)".to_string());
    Err(TransformError::UnsupportedOperator(name))
}

fn parse_project(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("project must be an object with fields".to_string())
    })?;
    let fields_value = obj.get("fields").ok_or_else(|| {
        TransformError::Invalid("project.fields is required".to_string())
    })?;
    let fields_arr = fields_value.as_array().ok_or_else(|| {
        TransformError::Invalid("project.fields must be an array of field names".to_string())
    })?;
    if fields_arr.is_empty() {
        return Err(TransformError::Invalid(
            "project.fields must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(fields_arr.len());
    for entry in fields_arr {
        let name = entry.as_str().ok_or_else(|| {
            TransformError::Invalid("project.fields entries must be strings".to_string())
        })?;
        if name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "project.fields entries must not be empty".to_string(),
            ));
        }
        fields.push(name.to_string());
    }
    if obj.keys().any(|k| k != "fields") {
        return Err(TransformError::Invalid(
            "project only supports fields".to_string(),
        ));
    }
    Ok(TransformOp::Project { fields })
}

fn parse_filter(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("filter must be an object with field and eq".to_string())
    })?;
    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("filter.field is required".to_string()))?;
    if field.trim().is_empty() {
        return Err(TransformError::Invalid(
            "filter.field must not be empty".to_string(),
        ));
    }
    let eq = obj.get("eq").ok_or_else(|| {
        TransformError::Invalid("filter.eq is required".to_string())
    })?;
    if obj.keys().any(|k| k != "field" && k != "eq") {
        return Err(TransformError::Invalid(
            "filter only supports field and eq".to_string(),
        ));
    }
    Ok(TransformOp::FilterEq {
        field: field.to_string(),
        value: eq.clone(),
    })
}

/// Parse a list of declarative transform step JSON values.
pub fn parse_transform_steps(steps: &[Value]) -> Result<Vec<TransformOp>, TransformError> {
    if steps.is_empty() {
        return Err(TransformError::Invalid(
            "transform must declare at least one operator".to_string(),
        ));
    }
    let mut ops = Vec::with_capacity(steps.len());
    for (index, step) in steps.iter().enumerate() {
        match parse_transform_step_value(step) {
            Ok(op) => ops.push(op),
            Err(TransformError::Invalid(msg)) => {
                return Err(TransformError::Invalid(format!(
                    "step {}: {msg}",
                    index + 1
                )));
            }
            Err(other) => return Err(other),
        }
    }
    Ok(ops)
}

/// Field names present in Derived output after applying ops (from project steps).
///
/// When no project is present, returns `None` (Derived keeps Base field shape).
pub fn derived_projected_fields(ops: &[TransformOp]) -> Option<Vec<String>> {
    let mut fields = None;
    for op in ops {
        if let TransformOp::Project { fields: projected } = op {
            fields = Some(projected.clone());
        }
    }
    fields
}

/// Evaluate a Rich Transform over Base rows, producing Derived rows.
pub fn evaluate_transform(
    ops: &[TransformOp],
    rows: &[Map<String, Value>],
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let mut current = rows.to_vec();
    for op in ops {
        current = apply_op(op, current)?;
    }
    Ok(current)
}

fn apply_op(
    op: &TransformOp,
    rows: Vec<Map<String, Value>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    match op {
        TransformOp::Project { fields } => Ok(rows
            .into_iter()
            .map(|row| {
                let mut projected = Map::new();
                for field in fields {
                    if let Some(value) = row.get(field) {
                        projected.insert(field.clone(), value.clone());
                    }
                }
                projected
            })
            .collect()),
        TransformOp::FilterEq { field, value } => Ok(rows
            .into_iter()
            .filter(|row| row.get(field).is_some_and(|v| json_values_eq(v, value)))
            .collect()),
    }
}

/// Equality that treats JSON numbers with the same numeric value as equal
/// (e.g. YAML `1` vs platform NUMBER `1`).
fn json_values_eq(left: &Value, right: &Value) -> bool {
    if left == right {
        return true;
    }
    match (as_f64(left), as_f64(right)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn rejects_script_steps() {
        let steps = vec![json!({"script": "return true"})];
        assert_eq!(
            parse_transform_steps(&steps).unwrap_err(),
            TransformError::FreeFormScript
        );
    }

    #[test]
    fn malformed_project_is_invalid_not_unsupported() {
        let steps = vec![json!({"project": {"fields": []}})];
        let err = parse_transform_steps(&steps).unwrap_err();
        assert!(
            matches!(err, TransformError::Invalid(_)),
            "expected Invalid, got {err:?}"
        );
        assert!(!err.to_string().to_ascii_lowercase().contains("unsupported"));
    }

    #[test]
    fn project_then_filter_eq() {
        let ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME", "ACTIVE"]}}),
            json!({"filter": {"field": "ACTIVE", "eq": 1}}),
        ])
        .unwrap();
        let rows = vec![
            row(&[
                ("ID", json!(1)),
                ("NAME", json!("Alice")),
                ("EMAIL", json!("a@x")),
                ("ACTIVE", json!(1)),
            ]),
            row(&[
                ("ID", json!(2)),
                ("NAME", json!("Bob")),
                ("EMAIL", json!("b@x")),
                ("ACTIVE", json!(0)),
            ]),
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("NAME"), Some(&json!("Alice")));
        assert!(!out[0].contains_key("EMAIL"));
    }
}
