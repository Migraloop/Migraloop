//! Rich Transform operators and Affect Analysis.
//!
//! Declarative project, addFields, rename, remove, filter (eq), and groupBy (sum)
//! over Base Dataset rows. Free-form scripts and unanalyzable operators are rejected
//! at parse time. Affect Analysis skips Derived recompute when only unused Base
//! fields change.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "transform";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransformError {
    #[error("Transform Pipeline rejects free-form scripts; use declarative analyzable operators only (project/addFields/rename/remove/filter/groupBy)")]
    FreeFormScript,
    #[error("unsupported Rich Transform operator: {0}; v1 allows project, addFields, rename, remove, filter, and groupBy")]
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

/// Source for one `addFields` entry: literal JSON or copy from an existing field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AddFieldSource {
    Literal(Value),
    Field(String),
}

/// One field added by `addFields`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddFieldSpec {
    #[serde(rename = "as")]
    pub as_name: String,
    pub source: AddFieldSource,
}

/// One rename mapping (`from` → `to`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenameSpec {
    pub from: String,
    pub to: String,
}

/// One analyzable Rich Transform operator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TransformOp {
    Project { fields: Vec<String> },
    AddFields { fields: Vec<AddFieldSpec> },
    Rename { fields: Vec<RenameSpec> },
    Remove { fields: Vec<String> },
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
/// - `{ "addFields": { "fields": [{ "as": "...", "value": ... } | { "as": "...", "field": "..." }] } }`
/// - `{ "rename": { "fields": [{ "from": "...", "to": "..." }] } }`
/// - `{ "remove": { "fields": [...] } }`
/// - `{ "filter": { "field": "...", "eq": ... } }`
/// - `{ "groupBy": { "keys": [...], "aggregates": [{ "op": "sum", "field": "...", "as": "..." }] } }`
/// Rejected shapes (clear errors):
/// - `{ "script": "..." }` / `{ "function": "..." }`
/// - any other operator object
/// - malformed operators (reported as invalid, not unsupported)
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

    if obj.contains_key("addFields") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "addFields step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_add_fields(obj.get("addFields").expect("addFields key"));
    }

    if obj.contains_key("rename") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "rename step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_rename(obj.get("rename").expect("rename key"));
    }

    if obj.contains_key("remove") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "remove step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_remove(obj.get("remove").expect("remove key"));
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

fn parse_add_fields(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("addFields must be an object with fields".to_string())
    })?;
    let fields_value = obj.get("fields").ok_or_else(|| {
        TransformError::Invalid("addFields.fields is required".to_string())
    })?;
    let fields_arr = fields_value.as_array().ok_or_else(|| {
        TransformError::Invalid("addFields.fields must be an array".to_string())
    })?;
    if fields_arr.is_empty() {
        return Err(TransformError::Invalid(
            "addFields.fields must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(fields_arr.len());
    for (index, entry) in fields_arr.iter().enumerate() {
        fields.push(parse_add_field_spec(entry, index)?);
    }
    if obj.keys().any(|k| k != "fields") {
        return Err(TransformError::Invalid(
            "addFields only supports fields".to_string(),
        ));
    }
    Ok(TransformOp::AddFields { fields })
}

fn parse_add_field_spec(value: &Value, index: usize) -> Result<AddFieldSpec, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(format!("addFields.fields[{index}] must be an object"))
    })?;
    let as_name = obj
        .get("as")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            TransformError::Invalid(format!("addFields.fields[{index}].as is required"))
        })?;
    if as_name.trim().is_empty() {
        return Err(TransformError::Invalid(format!(
            "addFields.fields[{index}].as must not be empty"
        )));
    }
    let has_value = obj.contains_key("value");
    let has_field = obj.contains_key("field");
    let source = match (has_value, has_field) {
        (true, false) => AddFieldSource::Literal(obj.get("value").expect("value").clone()),
        (false, true) => {
            let field = obj
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    TransformError::Invalid(format!(
                        "addFields.fields[{index}].field must be a string"
                    ))
                })?;
            if field.trim().is_empty() {
                return Err(TransformError::Invalid(format!(
                    "addFields.fields[{index}].field must not be empty"
                )));
            }
            AddFieldSource::Field(field.to_string())
        }
        (true, true) => {
            return Err(TransformError::Invalid(format!(
                "addFields.fields[{index}] must set exactly one of value or field"
            )));
        }
        (false, false) => {
            return Err(TransformError::Invalid(format!(
                "addFields.fields[{index}] requires value or field"
            )));
        }
    };
    if obj
        .keys()
        .any(|k| k != "as" && k != "value" && k != "field")
    {
        return Err(TransformError::Invalid(format!(
            "addFields.fields[{index}] only supports as, value, and field"
        )));
    }
    Ok(AddFieldSpec {
        as_name: as_name.to_string(),
        source,
    })
}

fn parse_rename(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("rename must be an object with fields".to_string())
    })?;
    let fields_value = obj.get("fields").ok_or_else(|| {
        TransformError::Invalid("rename.fields is required".to_string())
    })?;
    let fields_arr = fields_value.as_array().ok_or_else(|| {
        TransformError::Invalid("rename.fields must be an array".to_string())
    })?;
    if fields_arr.is_empty() {
        return Err(TransformError::Invalid(
            "rename.fields must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(fields_arr.len());
    for (index, entry) in fields_arr.iter().enumerate() {
        let entry_obj = entry.as_object().ok_or_else(|| {
            TransformError::Invalid(format!("rename.fields[{index}] must be an object"))
        })?;
        let from = entry_obj
            .get("from")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                TransformError::Invalid(format!("rename.fields[{index}].from is required"))
            })?;
        if from.trim().is_empty() {
            return Err(TransformError::Invalid(format!(
                "rename.fields[{index}].from must not be empty"
            )));
        }
        let to = entry_obj
            .get("to")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                TransformError::Invalid(format!("rename.fields[{index}].to is required"))
            })?;
        if to.trim().is_empty() {
            return Err(TransformError::Invalid(format!(
                "rename.fields[{index}].to must not be empty"
            )));
        }
        if entry_obj.keys().any(|k| k != "from" && k != "to") {
            return Err(TransformError::Invalid(format!(
                "rename.fields[{index}] only supports from and to"
            )));
        }
        fields.push(RenameSpec {
            from: from.to_string(),
            to: to.to_string(),
        });
    }
    if obj.keys().any(|k| k != "fields") {
        return Err(TransformError::Invalid(
            "rename only supports fields".to_string(),
        ));
    }
    Ok(TransformOp::Rename { fields })
}

fn parse_remove(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("remove must be an object with fields".to_string())
    })?;
    let fields_value = obj.get("fields").ok_or_else(|| {
        TransformError::Invalid("remove.fields is required".to_string())
    })?;
    let fields_arr = fields_value.as_array().ok_or_else(|| {
        TransformError::Invalid("remove.fields must be an array of field names".to_string())
    })?;
    if fields_arr.is_empty() {
        return Err(TransformError::Invalid(
            "remove.fields must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(fields_arr.len());
    for entry in fields_arr {
        let name = entry.as_str().ok_or_else(|| {
            TransformError::Invalid("remove.fields entries must be strings".to_string())
        })?;
        if name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "remove.fields entries must not be empty".to_string(),
            ));
        }
        fields.push(name.to_string());
    }
    if obj.keys().any(|k| k != "fields") {
        return Err(TransformError::Invalid(
            "remove only supports fields".to_string(),
        ));
    }
    Ok(TransformOp::Remove { fields })
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

/// Derived Managed field names given Base column names (handles open remove/rename/addFields).
pub fn derived_output_field_names(ops: &[TransformOp], base_field_names: &[String]) -> Vec<String> {
    let mut fields: Option<Vec<String>> = None;
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
            TransformOp::AddFields { fields: adds } => {
                let mut cur = fields.unwrap_or_else(|| base_field_names.to_vec());
                for spec in adds {
                    if !cur.iter().any(|n| n == &spec.as_name) {
                        cur.push(spec.as_name.clone());
                    }
                }
                fields = Some(cur);
            }
            TransformOp::Rename { fields: renames } => {
                let mut cur = fields.unwrap_or_else(|| base_field_names.to_vec());
                for spec in renames {
                    if let Some(pos) = cur.iter().position(|n| n == &spec.from) {
                        cur[pos] = spec.to.clone();
                    }
                }
                fields = Some(cur);
            }
            TransformOp::Remove { fields: remove } => {
                let mut cur = fields.unwrap_or_else(|| base_field_names.to_vec());
                cur.retain(|n| !remove.iter().any(|r| r == n));
                fields = Some(cur);
            }
            TransformOp::FilterEq { .. } => {}
        }
    }
    fields.unwrap_or_else(|| base_field_names.to_vec())
}

/// Base-field dependency mode for Affect Analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AffectDeps {
    /// Output depends only on these Base fields (closed by project/groupBy).
    Closed(BTreeSet<String>),
    /// Passthrough Base fields except `unused`; `extra` are always-used deps (filter/copies).
    Open {
        unused: BTreeSet<String>,
        extra: BTreeSet<String>,
    },
}

fn resolve_field_deps(
    lineage: &BTreeMap<String, BTreeSet<String>>,
    closed: bool,
    removed: &BTreeSet<String>,
    field: &str,
) -> BTreeSet<String> {
    if let Some(deps) = lineage.get(field) {
        return deps.clone();
    }
    if closed || removed.contains(field) {
        return BTreeSet::new();
    }
    BTreeSet::from([field.to_string()])
}

fn affect_deps(ops: &[TransformOp]) -> AffectDeps {
    let mut lineage: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut closed = false;
    let mut removed: BTreeSet<String> = BTreeSet::new();
    let mut filter_used: BTreeSet<String> = BTreeSet::new();

    for op in ops {
        match op {
            TransformOp::Project { fields } => {
                let mut next = BTreeMap::new();
                for field in fields {
                    next.insert(
                        field.clone(),
                        resolve_field_deps(&lineage, closed, &removed, field),
                    );
                }
                lineage = next;
                closed = true;
                removed.clear();
            }
            TransformOp::FilterEq { field, .. } => {
                filter_used.extend(resolve_field_deps(&lineage, closed, &removed, field));
            }
            TransformOp::AddFields { fields } => {
                for spec in fields {
                    let deps = match &spec.source {
                        AddFieldSource::Literal(_) => BTreeSet::new(),
                        AddFieldSource::Field(src) => {
                            resolve_field_deps(&lineage, closed, &removed, src)
                        }
                    };
                    lineage.insert(spec.as_name.clone(), deps);
                    removed.remove(&spec.as_name);
                }
            }
            TransformOp::Rename { fields } => {
                for spec in fields {
                    let deps = resolve_field_deps(&lineage, closed, &removed, &spec.from);
                    lineage.remove(&spec.from);
                    if !closed {
                        removed.insert(spec.from.clone());
                    }
                    lineage.insert(spec.to.clone(), deps);
                    removed.remove(&spec.to);
                }
            }
            TransformOp::Remove { fields } => {
                for field in fields {
                    lineage.remove(field);
                    removed.insert(field.clone());
                }
            }
            TransformOp::GroupBy { keys, aggregates } => {
                let mut next = BTreeMap::new();
                for key in keys {
                    next.insert(
                        key.clone(),
                        resolve_field_deps(&lineage, closed, &removed, key),
                    );
                }
                for agg in aggregates {
                    next.insert(
                        agg.as_name.clone(),
                        resolve_field_deps(&lineage, closed, &removed, &agg.field),
                    );
                }
                lineage = next;
                closed = true;
                removed.clear();
            }
        }
    }

    let mut explicit_used: BTreeSet<String> = lineage.values().flatten().cloned().collect();
    explicit_used.extend(filter_used);

    if closed {
        AffectDeps::Closed(explicit_used)
    } else {
        let unused = removed
            .into_iter()
            .filter(|name| !explicit_used.contains(name))
            .collect();
        AffectDeps::Open {
            unused,
            extra: explicit_used,
        }
    }
}

/// Base fields this Rich Transform depends on for Affect Analysis.
///
/// For open (passthrough) transforms this is only the *explicit* dependency set
/// (filter / addFields copies / rename sources). Removed fields are unused; other
/// passthrough Base fields still affect output and are handled in [`analyze_affect`].
pub fn used_base_fields(ops: &[TransformOp]) -> BTreeSet<String> {
    match affect_deps(ops) {
        AffectDeps::Closed(used) => used,
        AffectDeps::Open { extra, .. } => extra,
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
    let deps = affect_deps(ops);
    let group_keys = group_by_keys(ops);

    match kind {
        BaseChangeKind::Insert => {
            let after = after.ok_or_else(|| {
                TransformError::Invalid("Insert Affect Analysis requires after-image".into())
            })?;
            let identity = identity_from_row(ops, after, group_keys.as_deref())?;
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
            let identity = identity_from_row(ops, pre, group_keys.as_deref())?;
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
            let skip = match &deps {
                AffectDeps::Closed(used) => used.is_empty() || changed.is_disjoint(used),
                AffectDeps::Open { unused, .. } => {
                    changed.is_empty() || changed.is_subset(unused)
                }
            };
            if skip {
                return Ok(AffectOutcome::SkipUnusedFields);
            }
            let after_id = identity_from_row(ops, after, group_keys.as_deref())?;
            // groupBy key moves need pre-apply + after identities. Row-grain transforms
            // (project/addFields/rename/remove/filter) keep a single after-image identity —
            // shaped field renames must not invent a second delete identity.
            if group_keys.is_some() {
                let pre_id = identity_from_row(ops, pre, group_keys.as_deref())?;
                let mut identities = vec![after_id];
                if !identities
                    .iter()
                    .any(|existing| identity_maps_eq(existing, &pre_id))
                {
                    identities.push(pre_id);
                }
                Ok(AffectOutcome::Recompute { identities })
            } else {
                Ok(AffectOutcome::Recompute {
                    identities: vec![after_id],
                })
            }
        }
    }
}

fn identity_from_row(
    ops: &[TransformOp],
    row: &Map<String, Value>,
    group_keys: Option<&[String]>,
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
    // Non-groupBy: shape the Base row through project/addFields/rename/remove so
    // recompute identity keys match Derived field names (rename-safe).
    let identity = shape_row_for_identity(ops, row)?;
    if identity.is_empty() {
        return Err(TransformError::Invalid(
            "cannot derive Affect Analysis identity from Base row".into(),
        ));
    }
    Ok(identity)
}

/// Apply field-shaping operators (not filter/groupBy) so identity keys match Derived.
fn shape_row_for_identity(
    ops: &[TransformOp],
    row: &Map<String, Value>,
) -> Result<Map<String, Value>, TransformError> {
    let mut current = vec![row.clone()];
    for op in ops {
        match op {
            TransformOp::FilterEq { .. } | TransformOp::GroupBy { .. } => {}
            other => {
                current = apply_op(other, current)?;
            }
        }
    }
    Ok(current.into_iter().next().unwrap_or_default())
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
        TransformOp::AddFields { fields } => Ok(rows
            .into_iter()
            .map(|mut row| {
                for spec in fields {
                    let value = match &spec.source {
                        AddFieldSource::Literal(v) => v.clone(),
                        AddFieldSource::Field(src) => {
                            row.get(src).cloned().unwrap_or(Value::Null)
                        }
                    };
                    row.insert(spec.as_name.clone(), value);
                }
                row
            })
            .collect()),
        TransformOp::Rename { fields } => Ok(rows
            .into_iter()
            .map(|mut row| {
                for spec in fields {
                    if let Some(value) = row.remove(&spec.from) {
                        row.insert(spec.to.clone(), value);
                    }
                }
                row
            })
            .collect()),
        TransformOp::Remove { fields } => Ok(rows
            .into_iter()
            .map(|mut row| {
                for field in fields {
                    row.remove(field);
                }
                row
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

    #[test]
    fn add_fields_rename_remove_evaluate_and_skip_unused() {
        let ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME", "EMAIL", "ACTIVE"]}}),
            json!({"remove": {"fields": ["EMAIL"]}}),
            json!({"rename": {"fields": [{"from": "NAME", "to": "customerName"}]}}),
            json!({
                "addFields": {
                    "fields": [
                        {"as": "source", "value": "oracle"},
                        {"as": "displayName", "field": "customerName"}
                    ]
                }
            }),
            json!({"filter": {"field": "ACTIVE", "eq": 1}}),
        ])
        .unwrap();

        let rows = vec![
            row(&[
                ("ID", json!(1)),
                ("NAME", json!("Alice")),
                ("EMAIL", json!("a@x")),
                ("ACTIVE", json!(1)),
                ("NOTES", json!("unused")),
            ]),
            row(&[
                ("ID", json!(2)),
                ("NAME", json!("Bob")),
                ("EMAIL", json!("b@x")),
                ("ACTIVE", json!(0)),
                ("NOTES", json!("unused")),
            ]),
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("customerName"), Some(&json!("Alice")));
        assert_eq!(out[0].get("displayName"), Some(&json!("Alice")));
        assert_eq!(out[0].get("source"), Some(&json!("oracle")));
        assert!(!out[0].contains_key("EMAIL"));
        assert!(!out[0].contains_key("NAME"));
        assert!(!out[0].contains_key("NOTES"));

        let base_names = vec![
            "ID".into(),
            "NAME".into(),
            "EMAIL".into(),
            "ACTIVE".into(),
            "NOTES".into(),
        ];
        let managed = derived_output_field_names(&ops, &base_names);
        assert!(managed.contains(&"customerName".to_string()));
        assert!(managed.contains(&"displayName".to_string()));
        assert!(managed.contains(&"source".to_string()));
        assert!(!managed.iter().any(|n| n == "EMAIL" || n == "NAME" || n == "NOTES"));

        // EMAIL was removed (and NOTES never projected): unused-field update must skip.
        let pre = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alice")),
            ("EMAIL", json!("a@x")),
            ("ACTIVE", json!(1)),
            ("NOTES", json!("unused")),
        ]);
        let after_email = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alice")),
            ("EMAIL", json!("new@x")),
            ("ACTIVE", json!(1)),
            ("NOTES", json!("unused")),
        ]);
        assert_eq!(
            analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_email)).unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // NAME is used (via rename + addFields copy): must recompute; identity uses output names.
        let after_name = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alicia")),
            ("EMAIL", json!("a@x")),
            ("ACTIVE", json!(1)),
            ("NOTES", json!("unused")),
        ]);
        match analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_name)).unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("ID"), Some(&json!(1)));
                assert_eq!(identities[0].get("customerName"), Some(&json!("Alicia")));
                assert_eq!(identities[0].get("displayName"), Some(&json!("Alicia")));
                assert!(!identities[0].contains_key("NAME"));
                assert!(!identities[0].contains_key("EMAIL"));
            }
            other => panic!("expected Recompute, got {other:?}"),
        }
    }

    #[test]
    fn remove_only_skips_removed_field_updates_in_open_passthrough() {
        let ops = parse_transform_steps(&[json!({"remove": {"fields": ["ADDRESS"]}})]).unwrap();
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("AMOUNT", json!("42.50")),
            ("ADDRESS", json!("1 Main St")),
        ]);
        let after_addr = row(&[
            ("ORDER_ID", json!(100)),
            ("AMOUNT", json!("42.50")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        assert_eq!(
            analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_addr)).unwrap(),
            AffectOutcome::SkipUnusedFields
        );
        let after_amount = row(&[
            ("ORDER_ID", json!(100)),
            ("AMOUNT", json!("50.00")),
            ("ADDRESS", json!("1 Main St")),
        ]);
        assert!(matches!(
            analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_amount)).unwrap(),
            AffectOutcome::Recompute { .. }
        ));
    }

    #[test]
    fn malformed_add_fields_rename_remove_are_invalid_not_unsupported() {
        for step in [
            json!({"addFields": {"fields": []}}),
            json!({"rename": {"fields": [{"from": "A"}]}}),
            json!({"remove": {"fields": []}}),
        ] {
            let err = parse_transform_steps(&[step]).unwrap_err();
            assert!(
                matches!(err, TransformError::Invalid(_)),
                "expected Invalid, got {err:?}"
            );
            assert!(!err.to_string().to_ascii_lowercase().contains("unsupported"));
        }
    }
}
