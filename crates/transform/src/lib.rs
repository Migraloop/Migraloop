//! Rich Transform operators and Affect Analysis.
//!
//! Declarative project, filter (eq), and groupBy (sum) over Base Dataset rows.
//! Free-form scripts and unanalyzable operators are rejected at parse time.
//! Affect Analysis skips Derived recompute when only unused Base fields change.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "transform";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransformError {
    #[error("Transform Pipeline rejects free-form scripts; use declarative analyzable operators only (project/filter/groupBy)")]
    FreeFormScript,
    #[error("unsupported Rich Transform operator: {0}; v1 allows project, filter, and groupBy")]
    UnsupportedOperator(String),
    #[error("invalid Rich Transform: {0}")]
    Invalid(String),
}

/// One aggregation inside a groupBy operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSpec {
    pub op: AggregateOp,
    pub field: String,
    #[serde(rename = "as")]
    pub as_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateOp {
    Sum,
}

/// One analyzable Rich Transform operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransformOp {
    Project { fields: Vec<String> },
    FilterEq { field: String, value: Value },
    GroupBy {
        keys: Vec<String>,
        aggregates: Vec<AggregateSpec>,
    },
}

/// Kind of Base change for Affect Analysis (mirrors capture ChangeOp without coupling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaseChangeKind {
    Insert,
    Update,
    Delete,
}

/// Result of Affect Analysis for one Base change against a Rich Transform.
#[derive(Debug, Clone, PartialEq)]
pub enum AffectOutcome {
    /// Only unused fields changed; Derived must not recompute for this transform.
    SkipUnusedFields,
    /// Recompute Derived for these Output Identity maps (group key → value).
    Recompute {
        identities: Vec<Map<String, Value>>,
    },
}

/// Parse one declarative transform step JSON object into an analyzable operator.
///
/// Accepted shapes:
/// - `{ "project": { "fields": [...] } }`
/// - `{ "filter": { "field": "...", "eq": ... } }`
/// - `{ "groupBy": { "keys": [...], "aggregates": [{ "op": "sum", "field": "...", "as": "..." }] } }`
/// Rejected shapes (clear errors):
/// - `{ "script": "..." }` / `{ "function": "..." }`
/// - any other operator object
/// - malformed project/filter/groupBy (reported as invalid, not unsupported)
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

    if obj.contains_key("groupBy") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "groupBy step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_group_by(obj.get("groupBy").expect("groupBy key"));
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

fn parse_group_by(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "groupBy must be an object with keys and aggregates".to_string(),
        )
    })?;
    let keys_value = obj.get("keys").ok_or_else(|| {
        TransformError::Invalid("groupBy.keys is required".to_string())
    })?;
    let keys_arr = keys_value.as_array().ok_or_else(|| {
        TransformError::Invalid("groupBy.keys must be an array of field names".to_string())
    })?;
    if keys_arr.is_empty() {
        return Err(TransformError::Invalid(
            "groupBy.keys must not be empty".to_string(),
        ));
    }
    let mut keys = Vec::with_capacity(keys_arr.len());
    for entry in keys_arr {
        let name = entry.as_str().ok_or_else(|| {
            TransformError::Invalid("groupBy.keys entries must be strings".to_string())
        })?;
        if name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "groupBy.keys entries must not be empty".to_string(),
            ));
        }
        keys.push(name.to_string());
    }

    let aggregates_value = obj.get("aggregates").ok_or_else(|| {
        TransformError::Invalid("groupBy.aggregates is required".to_string())
    })?;
    let aggregates_arr = aggregates_value.as_array().ok_or_else(|| {
        TransformError::Invalid("groupBy.aggregates must be an array".to_string())
    })?;
    if aggregates_arr.is_empty() {
        return Err(TransformError::Invalid(
            "groupBy.aggregates must not be empty".to_string(),
        ));
    }
    let mut aggregates = Vec::with_capacity(aggregates_arr.len());
    for (index, entry) in aggregates_arr.iter().enumerate() {
        aggregates.push(parse_aggregate(entry, index)?);
    }

    if obj.keys().any(|k| k != "keys" && k != "aggregates") {
        return Err(TransformError::Invalid(
            "groupBy only supports keys and aggregates".to_string(),
        ));
    }
    Ok(TransformOp::GroupBy { keys, aggregates })
}

fn parse_aggregate(value: &Value, index: usize) -> Result<AggregateSpec, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(format!(
            "groupBy.aggregates[{index}] must be an object"
        ))
    })?;
    let op_raw = obj
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            TransformError::Invalid(format!("groupBy.aggregates[{index}].op is required"))
        })?;
    let op = match op_raw {
        "sum" => AggregateOp::Sum,
        other => {
            return Err(TransformError::Invalid(format!(
                "groupBy.aggregates[{index}].op {other:?} is unsupported; v1 allows sum"
            )));
        }
    };
    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            TransformError::Invalid(format!(
                "groupBy.aggregates[{index}].field is required"
            ))
        })?;
    if field.trim().is_empty() {
        return Err(TransformError::Invalid(format!(
            "groupBy.aggregates[{index}].field must not be empty"
        )));
    }
    let as_name = obj
        .get("as")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            TransformError::Invalid(format!("groupBy.aggregates[{index}].as is required"))
        })?;
    if as_name.trim().is_empty() {
        return Err(TransformError::Invalid(format!(
            "groupBy.aggregates[{index}].as must not be empty"
        )));
    }
    if obj
        .keys()
        .any(|k| k != "op" && k != "field" && k != "as")
    {
        return Err(TransformError::Invalid(format!(
            "groupBy.aggregates[{index}] only supports op, field, and as"
        )));
    }
    Ok(AggregateSpec {
        op,
        field: field.to_string(),
        as_name: as_name.to_string(),
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

/// Field names present in Derived output after applying ops.
///
/// When no project/groupBy is present, returns `None` (Derived keeps Base field shape).
pub fn derived_projected_fields(ops: &[TransformOp]) -> Option<Vec<String>> {
    let mut fields = None;
    for op in ops {
        match op {
            TransformOp::Project { fields: projected } => {
                fields = Some(projected.clone());
            }
            TransformOp::GroupBy { keys, aggregates } => {
                let mut names = keys.clone();
                for agg in aggregates {
                    names.push(agg.as_name.clone());
                }
                fields = Some(names);
            }
            TransformOp::FilterEq { .. } => {}
        }
    }
    fields
}

/// Base fields this Rich Transform depends on for Affect Analysis.
pub fn used_base_fields(ops: &[TransformOp]) -> BTreeSet<String> {
    let mut used = BTreeSet::new();
    for op in ops {
        match op {
            TransformOp::Project { fields } => {
                used.extend(fields.iter().cloned());
            }
            TransformOp::FilterEq { field, .. } => {
                used.insert(field.clone());
            }
            TransformOp::GroupBy { keys, aggregates } => {
                used.extend(keys.iter().cloned());
                for agg in aggregates {
                    used.insert(agg.field.clone());
                }
            }
        }
    }
    used
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

/// Recompute Derived rows for specific Output Identities from current Base rows.
///
/// For groupBy, identities are group-key maps. Returns one Derived row per identity
/// that still has input rows; identities with empty groups are omitted (caller deletes).
pub fn evaluate_transform_for_identities(
    ops: &[TransformOp],
    base_rows: &[Map<String, Value>],
    identities: &[Map<String, Value>],
) -> Result<Vec<Map<String, Value>>, TransformError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let Some(group_keys) = group_by_keys(ops) else {
        // Non-groupBy transforms: filter full evaluation to matching Output Identity.
        let all = evaluate_transform(ops, base_rows)?;
        return Ok(all
            .into_iter()
            .filter(|row| identities.iter().any(|id| identity_matches_row(id, row)))
            .collect());
    };

    let mut filtered = Vec::new();
    for row in base_rows {
        if identities.iter().any(|id| row_matches_group_keys(row, id, &group_keys)) {
            filtered.push(row.clone());
        }
    }
    evaluate_transform(ops, &filtered)
}

fn group_by_keys(ops: &[TransformOp]) -> Option<Vec<String>> {
    ops.iter().rev().find_map(|op| match op {
        TransformOp::GroupBy { keys, .. } => Some(keys.clone()),
        _ => None,
    })
}

fn row_matches_group_keys(
    row: &Map<String, Value>,
    identity: &Map<String, Value>,
    keys: &[String],
) -> bool {
    keys.iter().all(|key| match (row.get(key), identity.get(key)) {
        (Some(a), Some(b)) => json_values_eq(a, b),
        _ => false,
    })
}

/// Affect Analysis: which Output Identities (if any) need Derived recompute.
///
/// `pre_apply` is the Base row before applying the change (required for Update/Delete).
/// `after` is the change after-image (required for Insert/Update).
pub fn analyze_affect(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
) -> Result<AffectOutcome, TransformError> {
    let used = used_base_fields(ops);
    if used.is_empty() {
        return Ok(AffectOutcome::SkipUnusedFields);
    }

    let group_keys = group_by_keys(ops);

    match kind {
        BaseChangeKind::Insert => {
            let after = after.ok_or_else(|| {
                TransformError::Invalid("Insert Affect Analysis requires after-image".into())
            })?;
            let identity = identity_from_row(after, group_keys.as_deref(), &used)?;
            Ok(AffectOutcome::Recompute {
                identities: vec![identity],
            })
        }
        BaseChangeKind::Delete => {
            let pre = pre_apply.ok_or_else(|| {
                TransformError::Invalid(
                    "Delete Affect Analysis requires pre-apply Base row".into(),
                )
            })?;
            let identity = identity_from_row(pre, group_keys.as_deref(), &used)?;
            Ok(AffectOutcome::Recompute {
                identities: vec![identity],
            })
        }
        BaseChangeKind::Update => {
            let pre = pre_apply.ok_or_else(|| {
                TransformError::Invalid(
                    "Update Affect Analysis requires pre-apply Base row".into(),
                )
            })?;
            let after = after.ok_or_else(|| {
                TransformError::Invalid("Update Affect Analysis requires after-image".into())
            })?;
            let changed = changed_fields(pre, after);
            if changed.is_empty() || changed.is_disjoint(&used) {
                return Ok(AffectOutcome::SkipUnusedFields);
            }
            // Always derive after-image identity, and always derive pre-apply identity
            // (required for group-key moves: old + new Output Identities). Do not depend
            // on reading the prior key after Base has already been overwritten.
            let after_id = identity_from_row(after, group_keys.as_deref(), &used)?;
            let pre_id = identity_from_row(pre, group_keys.as_deref(), &used)?;
            let mut identities = vec![after_id];
            if !identities
                .iter()
                .any(|existing| identity_maps_eq(existing, &pre_id))
            {
                identities.push(pre_id);
            }
            Ok(AffectOutcome::Recompute { identities })
        }
    }
}

fn identity_from_row(
    row: &Map<String, Value>,
    group_keys: Option<&[String]>,
    used: &BTreeSet<String>,
) -> Result<Map<String, Value>, TransformError> {
    if let Some(keys) = group_keys {
        let mut identity = Map::new();
        for key in keys {
            let value = row.get(key).cloned().ok_or_else(|| {
                TransformError::Invalid(format!(
                    "Base row missing groupBy key {key} for Affect Analysis"
                ))
            })?;
            identity.insert(key.clone(), value);
        }
        return Ok(identity);
    }
    // Non-groupBy: Output Identity is typically source PK fields still present after project.
    // Affect Analysis still keys off used fields for skip decisions; identity map uses
    // all used fields present on the row as a best-effort locator for recompute filter.
    let mut identity = Map::new();
    for key in used {
        if let Some(value) = row.get(key) {
            identity.insert(key.clone(), value.clone());
        }
    }
    if identity.is_empty() {
        return Err(TransformError::Invalid(
            "cannot derive Affect Analysis identity from Base row".into(),
        ));
    }
    Ok(identity)
}

fn changed_fields(pre: &Map<String, Value>, after: &Map<String, Value>) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    names.extend(pre.keys().cloned());
    names.extend(after.keys().cloned());
    names
        .into_iter()
        .filter(|name| match (pre.get(name), after.get(name)) {
            (Some(a), Some(b)) => !json_values_eq(a, b),
            (None, Some(_)) | (Some(_), None) => true,
            (None, None) => false,
        })
        .collect()
}

fn identity_maps_eq(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .all(|(k, v)| right.get(k).is_some_and(|other| json_values_eq(v, other)))
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
        TransformOp::GroupBy { keys, aggregates } => apply_group_by(keys, aggregates, rows),
    }
}

fn apply_group_by(
    keys: &[String],
    aggregates: &[AggregateSpec],
    rows: Vec<Map<String, Value>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let mut groups: BTreeMap<Vec<ValueKey>, Vec<Map<String, Value>>> = BTreeMap::new();
    for row in rows {
        let mut key_parts = Vec::with_capacity(keys.len());
        for key in keys {
            let value = row.get(key).cloned().unwrap_or(Value::Null);
            key_parts.push(ValueKey(value));
        }
        groups.entry(key_parts).or_default().push(row);
    }

    let mut out = Vec::with_capacity(groups.len());
    for (key_parts, group_rows) in groups {
        let mut derived = Map::new();
        for (index, key) in keys.iter().enumerate() {
            derived.insert(key.clone(), key_parts[index].0.clone());
        }
        for agg in aggregates {
            let value = match agg.op {
                AggregateOp::Sum => sum_field(&group_rows, &agg.field)?,
            };
            derived.insert(agg.as_name.clone(), value);
        }
        out.push(derived);
    }
    Ok(out)
}

/// Precision-preserving sum (ADR-0023): scaled integer arithmetic, never IEEE double.
fn sum_field(rows: &[Map<String, Value>], field: &str) -> Result<Value, TransformError> {
    let mut total_units: i128 = 0;
    let mut max_scale: usize = 0;
    let mut saw_any = false;
    for row in rows {
        let Some(value) = row.get(field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let (units, scale) = parse_decimal_units(value).ok_or_else(|| {
            TransformError::Invalid(format!(
                "groupBy sum field {field} is not numeric: {value}"
            ))
        })?;
        saw_any = true;
        if scale > max_scale {
            let factor = ten_pow_i128(scale - max_scale)?;
            total_units = total_units
                .checked_mul(factor)
                .ok_or_else(|| TransformError::Invalid("groupBy sum overflow".into()))?;
            max_scale = scale;
        }
        let aligned = if scale < max_scale {
            let factor = ten_pow_i128(max_scale - scale)?;
            units
                .checked_mul(factor)
                .ok_or_else(|| TransformError::Invalid("groupBy sum overflow".into()))?
        } else {
            units
        };
        total_units = total_units
            .checked_add(aligned)
            .ok_or_else(|| TransformError::Invalid("groupBy sum overflow".into()))?;
    }
    if !saw_any {
        return Ok(Value::Number(serde_json::Number::from(0)));
    }
    Ok(format_decimal_units(total_units, max_scale))
}

fn parse_decimal_units(value: &Value) -> Option<(i128, usize)> {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Some((i128::from(i), 0));
            }
            if let Some(u) = n.as_u64() {
                return Some((i128::from(u), 0));
            }
            // JSON numbers that are not integers arrive as text via to_string.
            parse_decimal_text(&n.to_string())
        }
        Value::String(s) => parse_decimal_text(s),
        _ => None,
    }
}

fn parse_decimal_text(raw: &str) -> Option<(i128, usize)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let negative = trimmed.starts_with('-');
    let body = trimmed.trim_start_matches(['+', '-']);
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{int_part}{frac_part}");
    if digits.is_empty() {
        return None;
    }
    let mut units: i128 = digits.parse().ok()?;
    if negative {
        units = -units;
    }
    Some((units, frac_part.len()))
}

fn ten_pow_i128(exp: usize) -> Result<i128, TransformError> {
    let mut result: i128 = 1;
    for _ in 0..exp {
        result = result
            .checked_mul(10)
            .ok_or_else(|| TransformError::Invalid("groupBy sum scale overflow".into()))?;
    }
    Ok(result)
}

fn format_decimal_units(units: i128, scale: usize) -> Value {
    if scale == 0 {
        if units >= i64::MIN as i128 && units <= i64::MAX as i128 {
            return Value::Number(serde_json::Number::from(units as i64));
        }
        return Value::String(units.to_string());
    }
    let negative = units < 0;
    let abs = units.unsigned_abs();
    let factor = 10_u128.pow(scale as u32);
    let int_part = abs / factor;
    let frac_part = abs % factor;
    let frac = format!("{frac_part:0>scale$}");
    let text = if negative {
        format!("-{int_part}.{frac}")
    } else {
        format!("{int_part}.{frac}")
    };
    Value::String(text)
}

/// Equality that treats JSON numbers with the same numeric value as equal
/// (e.g. YAML `1` vs platform NUMBER `1`). Prefer decimal-unit compare over IEEE.
pub fn json_values_eq(left: &Value, right: &Value) -> bool {
    if left == right {
        return true;
    }
    match (parse_decimal_units(left), parse_decimal_units(right)) {
        (Some((a_units, a_scale)), Some((b_units, b_scale))) => {
            let scale = a_scale.max(b_scale);
            match (
                scale_units(a_units, a_scale, scale),
                scale_units(b_units, b_scale, scale),
            ) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }
        _ => false,
    }
}

fn scale_units(units: i128, from_scale: usize, to_scale: usize) -> Option<i128> {
    if from_scale > to_scale {
        return None;
    }
    let factor = ten_pow_i128(to_scale - from_scale).ok()?;
    units.checked_mul(factor)
}

/// Whether every key in `identity` matches the corresponding field on `row`
/// (numeric-aware).
pub fn identity_matches_row(identity: &Map<String, Value>, row: &Map<String, Value>) -> bool {
    identity
        .iter()
        .all(|(key, expected)| row.get(key).is_some_and(|v| json_values_eq(v, expected)))
}

#[derive(Debug, Clone)]
struct ValueKey(Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        json_values_eq(&self.0, &other.0)
    }
}

impl Eq for ValueKey {}

impl PartialOrd for ValueKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValueKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        value_ord(&self.0).cmp(&value_ord(&other.0))
    }
}

fn value_ord(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => format!("b{b}"),
        Value::Number(n) => format!("n{n}"),
        Value::String(s) => {
            if let Ok(f) = s.parse::<f64>() {
                format!("n{f}")
            } else {
                format!("s{s}")
            }
        }
        other => format!("x{other}"),
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

    #[test]
    fn group_by_sum_totals() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        let rows = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
                ("ADDRESS", json!("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
                ("ADDRESS", json!("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
                ("ADDRESS", json!("2 Side Rd")),
            ]),
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out.len(), 2);
        let c1 = out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(1)))
            .unwrap();
        assert_eq!(c1.get("TOTAL_AMOUNT"), Some(&json!("52.50")));
    }

    #[test]
    fn affect_analysis_skips_unused_address_update() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
            ("ADDRESS", json!("1 Main St")),
        ]);
        let after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        let outcome = analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after)).unwrap();
        assert_eq!(outcome, AffectOutcome::SkipUnusedFields);
    }

    #[test]
    fn group_by_sum_preserves_decimal_precision_without_ieee_double() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        // Classic binary-float trap: 0.1 + 0.2 != 0.3 in IEEE double.
        let rows = vec![
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", json!("0.10"))]),
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", json!("0.20"))]),
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out[0].get("TOTAL_AMOUNT"), Some(&json!("0.30")));
    }

    #[test]
    fn affect_analysis_recomputes_on_amount_update() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        let after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        let outcome = analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after)).unwrap();
        match outcome {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("CUSTOMER_ID"), Some(&json!(1)));
            }
            other => panic!("expected Recompute, got {other:?}"),
        }
    }

    #[test]
    fn affect_analysis_recomputes_old_and_new_identities_on_group_key_change() {
        // US38 / issue #18: group-key change must use pre-apply Base visibility so both
        // old and new Output Identities are returned — never only the after-image key.
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        let pre = row(&[
            ("ORDER_ID", json!(200)),
            ("CUSTOMER_ID", json!(2)),
            ("AMOUNT", json!("5.00")),
            ("ADDRESS", json!("2 Side Rd")),
        ]);
        let after = row(&[
            ("ORDER_ID", json!(200)),
            ("CUSTOMER_ID", json!(3)),
            ("AMOUNT", json!("5.00")),
            ("ADDRESS", json!("2 Side Rd")),
        ]);
        let outcome = analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after)).unwrap();
        match outcome {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 2, "expected old+new identities, got {identities:?}");
                let ids: BTreeSet<i64> = identities
                    .iter()
                    .filter_map(|id| id.get("CUSTOMER_ID")?.as_i64())
                    .collect();
                assert_eq!(ids, BTreeSet::from([2, 3]));
            }
            other => panic!("expected Recompute, got {other:?}"),
        }
    }

    #[test]
    fn evaluate_for_identities_omits_empty_old_group_after_key_move() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        // After moving order 200 from customer 2 → 3, Base no longer has customer 2 rows.
        let base_rows = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("50.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(3)),
                ("AMOUNT", json!("5.00")),
            ]),
        ];
        let identities = vec![
            row(&[("CUSTOMER_ID", json!(3))]),
            row(&[("CUSTOMER_ID", json!(2))]),
        ];
        let out = evaluate_transform_for_identities(&ops, &base_rows, &identities).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("CUSTOMER_ID"), Some(&json!(3)));
        assert_eq!(out[0].get("TOTAL_AMOUNT"), Some(&json!("5.00")));
    }

    #[test]
    fn evaluate_for_identities_adjusts_old_group_when_sibling_rows_remain() {
        // Spec "adjusts/removes": when the old group still has other Base rows, recompute
        // must return an adjusted sum for the old identity (not omit it).
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        // Moved order 100 from customer 1 → 3; order 101 remains on customer 1.
        let base_rows = vec![
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(3)),
                ("AMOUNT", json!("50.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
            ]),
        ];
        let identities = vec![
            row(&[("CUSTOMER_ID", json!(3))]),
            row(&[("CUSTOMER_ID", json!(1))]),
        ];
        let out = evaluate_transform_for_identities(&ops, &base_rows, &identities).unwrap();
        assert_eq!(out.len(), 2);
        let c1 = out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(1)))
            .expect("old identity customer 1 must be adjusted, not removed");
        assert_eq!(c1.get("TOTAL_AMOUNT"), Some(&json!("10.00")));
        let c3 = out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(3)))
            .expect("new identity customer 3");
        assert_eq!(c3.get("TOTAL_AMOUNT"), Some(&json!("50.00")));
    }
}
