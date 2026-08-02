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

/// Declarative transform step as authored in Deployment config (YAML/JSON).
///
/// Accepted shapes:
/// - `{ project: { fields: [...] } }`
/// - `{ filter: { field, eq } }`
/// Rejected shapes (clear errors):
/// - `{ script: "..." }` / `{ function: "..." }`
/// - any other operator object
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TransformStepSpec {
    Project {
        project: ProjectStepSpec,
    },
    Filter {
        filter: FilterStepSpec,
    },
    Script {
        script: String,
    },
    Function {
        function: String,
    },
    Other(Map<String, Value>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectStepSpec {
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterStepSpec {
    pub field: String,
    pub eq: Value,
}

/// Parse declarative transform steps into analyzable operators.
pub fn parse_transform_steps(steps: &[TransformStepSpec]) -> Result<Vec<TransformOp>, TransformError> {
    if steps.is_empty() {
        return Err(TransformError::Invalid(
            "transform must declare at least one operator".to_string(),
        ));
    }
    let mut ops = Vec::with_capacity(steps.len());
    for step in steps {
        ops.push(parse_step(step)?);
    }
    Ok(ops)
}

fn parse_step(step: &TransformStepSpec) -> Result<TransformOp, TransformError> {
    match step {
        TransformStepSpec::Project { project } => {
            if project.fields.is_empty() {
                return Err(TransformError::Invalid(
                    "project.fields must not be empty".to_string(),
                ));
            }
            for field in &project.fields {
                if field.trim().is_empty() {
                    return Err(TransformError::Invalid(
                        "project.fields entries must not be empty".to_string(),
                    ));
                }
            }
            Ok(TransformOp::Project {
                fields: project.fields.clone(),
            })
        }
        TransformStepSpec::Filter { filter } => {
            if filter.field.trim().is_empty() {
                return Err(TransformError::Invalid(
                    "filter.field must not be empty".to_string(),
                ));
            }
            Ok(TransformOp::FilterEq {
                field: filter.field.clone(),
                value: filter.eq.clone(),
            })
        }
        TransformStepSpec::Script { .. } | TransformStepSpec::Function { .. } => {
            Err(TransformError::FreeFormScript)
        }
        TransformStepSpec::Other(map) => {
            let name = map
                .keys()
                .next()
                .cloned()
                .unwrap_or_else(|| "(empty)".to_string());
            let lower = name.to_ascii_lowercase();
            if lower == "script" || lower == "function" || lower == "$function" {
                return Err(TransformError::FreeFormScript);
            }
            Err(TransformError::UnsupportedOperator(name))
        }
    }
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
        let steps = vec![TransformStepSpec::Script {
            script: "return true".to_string(),
        }];
        assert_eq!(
            parse_transform_steps(&steps).unwrap_err(),
            TransformError::FreeFormScript
        );
    }

    #[test]
    fn project_then_filter_eq() {
        let ops = vec![
            TransformOp::Project {
                fields: vec!["ID".into(), "NAME".into(), "ACTIVE".into()],
            },
            TransformOp::FilterEq {
                field: "ACTIVE".into(),
                value: json!(1),
            },
        ];
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
