//! Rich Transform operators and Affect Analysis.
//!
//! Declarative project, addFields, rename, remove, filter (eq), equiLookup,
//! unwind, union, groupBy (sum/count/min/max/avg), distinct, and addToSet over
//! Base Dataset rows. Free-form scripts and unanalyzable operators (including
//! `$lookup` / `$unwind` / `$unionWith` shorthands) are rejected at parse time.
//! Affect Analysis skips Derived recompute when only unused Base fields change,
//! and (with Maintenance State) when distinct/addToSet value-level semantics
//! prove no Derived change.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "transform";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransformError {
    #[error("Transform Pipeline rejects free-form scripts; use declarative analyzable operators only (project/addFields/rename/remove/filter/equiLookup/unwind/union/groupBy/distinct/addToSet)")]
    FreeFormScript,
    #[error("unsupported Rich Transform operator: {0}; v1 allows project, addFields, rename, remove, filter, equiLookup, unwind, union, groupBy, distinct, and addToSet")]
    UnsupportedOperator(String),
    #[error("invalid Rich Transform: {0}")]
    Invalid(String),
}

/// Secondary Base Dataset referenced by an `equiLookup` or `union` step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecondaryBaseRef {
    pub table: String,
    pub schema: Option<String>,
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
    Count,
    Min,
    Max,
    Avg,
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
    /// Left-outer equijoin against another Base Dataset (embedded match array).
    EquiLookup {
        from: String,
        from_schema: Option<String>,
        local_field: String,
        foreign_field: String,
        as_name: String,
    },
    /// Expand an array field into one Derived row per element (1→N grain).
    ///
    /// Object elements are merged into the parent (path removed) so Output
    /// Identity can key Delivery on unwound fields. Scalar elements replace
    /// the path value (Mongo-style). Missing / null / empty arrays emit no rows.
    Unwind { path: String },
    /// Concatenate another Base Dataset into the stream (SQL UNION ALL / Mongo
    /// `$unionWith` without a nested pipeline). Primary `source.table` rows
    /// (after prior steps) are followed by `from` Base rows; later steps shape
    /// both sides. Optional `from_schema` overrides the secondary schema.
    Union {
        from: String,
        from_schema: Option<String>,
    },
    GroupBy {
        keys: Vec<String>,
        aggregates: Vec<AggregateSpec>,
    },
    /// One Derived row per unique combination of `fields` (SQL DISTINCT).
    Distinct { fields: Vec<String> },
    /// Group by `keys`; collect unique non-null values of `field` into array `as_name`.
    AddToSet {
        keys: Vec<String>,
        field: String,
        as_name: String,
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
    /// Value-level semantics (distinct/addToSet + Maintenance State) prove no Derived change.
    SkipValueUnchanged,
    /// Recompute Derived for these Output Identity maps (group key → value).
    Recompute {
        identities: Vec<Map<String, Value>>,
    },
}

/// One Maintenance State entry: refcount for a distinct identity or an addToSet member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceEntry {
    /// Distinct fields, or addToSet group keys.
    pub identity: Map<String, Value>,
    /// `None` for distinct; `Some(member)` for addToSet value membership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    pub refcount: i64,
}

/// Platform-internal Maintenance State for operators that need value-level Affect Analysis.
///
/// Created only when [`requires_maintenance_state`] is true (distinct / addToSet).
/// Simple groupBy sum/count/min/max/avg must not invent this structure.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceState {
    pub entries: Vec<MaintenanceEntry>,
}

/// Parse one declarative transform step JSON object into an analyzable operator.
///
/// Accepted shapes:
/// - `{ "project": { "fields": [...] } }`
/// - `{ "addFields": { "fields": [{ "as": "...", "value": ... } | { "as": "...", "field": "..." }] } }`
/// - `{ "rename": { "fields": [{ "from": "...", "to": "..." }] } }`
/// - `{ "remove": { "fields": [...] } }`
/// - `{ "filter": { "field": "...", "eq": ... } }`
/// - `{ "equiLookup": { "from": "...", "localField": "...", "foreignField": "...", "as": "...", "fromSchema?": "..." } }`
/// - `{ "unwind": { "path": "..." } }`
/// - `{ "union": { "from": "...", "fromSchema?": "..." } }`
/// - `{ "groupBy": { "keys": [...], "aggregates": [{ "op": "sum"|"count"|"min"|"max"|"avg", "field": "...", "as": "..." }] } }`
/// - `{ "distinct": { "fields": [...] } }`
/// - `{ "addToSet": { "keys": [...], "field": "...", "as": "..." } }`
/// Rejected shapes (clear errors):
/// - `{ "script": "..." }` / `{ "function": "..." }`
/// - `{ "$lookup": ... }` (use declarative `equiLookup`)
/// - `{ "$unwind": ... }` (use declarative `unwind`)
/// - `{ "$unionWith": ... }` (use declarative `union`)
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

    if obj.contains_key("$lookup") {
        return Err(TransformError::Invalid(
            "$lookup is not supported; use declarative equiLookup (from/localField/foreignField/as) so Affect Analysis stays correct".to_string(),
        ));
    }

    if obj.contains_key("$unwind") {
        return Err(TransformError::Invalid(
            "$unwind is not supported; use declarative unwind ({ path }) so Affect Analysis stays correct".to_string(),
        ));
    }

    if obj.contains_key("$unionWith") {
        return Err(TransformError::Invalid(
            "$unionWith is not supported; use declarative union ({ from }) so Affect Analysis stays correct".to_string(),
        ));
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

    if obj.contains_key("equiLookup") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "equiLookup step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_equi_lookup(obj.get("equiLookup").expect("equiLookup key"));
    }

    if obj.contains_key("unwind") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "unwind step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_unwind(obj.get("unwind").expect("unwind key"));
    }

    if obj.contains_key("union") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "union step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_union(obj.get("union").expect("union key"));
    }

    if obj.contains_key("groupBy") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "groupBy step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_group_by(obj.get("groupBy").expect("groupBy key"));
    }

    if obj.contains_key("distinct") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "distinct step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_distinct(obj.get("distinct").expect("distinct key"));
    }

    if obj.contains_key("addToSet") {
        if obj.len() != 1 {
            return Err(TransformError::Invalid(
                "addToSet step must not mix other operators in the same step".to_string(),
            ));
        }
        return parse_add_to_set(obj.get("addToSet").expect("addToSet key"));
    }

    let name = obj
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "(empty)".to_string());
    Err(TransformError::UnsupportedOperator(name))
}

fn parse_union(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("union must be an object with from".to_string())
    })?;

    // Reject free-form / Mongo $unionWith extensions (nested pipeline, coll alias).
    for banned in ["pipeline", "let", "coll", "$unionWith"] {
        if obj.contains_key(banned) {
            return Err(TransformError::Invalid(format!(
                "union does not support `{banned}`; only declarative from (optional fromSchema)"
            )));
        }
    }

    let from = obj
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("union.from is required".to_string()))?
        .trim();
    if from.is_empty() {
        return Err(TransformError::Invalid(
            "union.from must not be empty".to_string(),
        ));
    }

    let from_schema = match obj.get("fromSchema") {
        None => None,
        Some(Value::Null) => None,
        Some(v) => {
            let s = v.as_str().ok_or_else(|| {
                TransformError::Invalid("union.fromSchema must be a string".to_string())
            })?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
    };

    for key in obj.keys() {
        if !matches!(key.as_str(), "from" | "fromSchema") {
            return Err(TransformError::Invalid(format!(
                "union only supports from and fromSchema (unknown `{key}`)"
            )));
        }
    }

    Ok(TransformOp::Union {
        from: from.to_string(),
        from_schema,
    })
}

fn parse_unwind(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("unwind must be an object with path".to_string())
    })?;

    for banned in [
        "preserveNullAndEmptyArrays",
        "includeArrayIndex",
        "pipeline",
        "let",
    ] {
        if obj.contains_key(banned) {
            return Err(TransformError::Invalid(format!(
                "unwind does not support `{banned}`; only declarative path (missing/null/empty arrays emit no rows)"
            )));
        }
    }

    let path_raw = obj
        .get("path")
        .ok_or_else(|| TransformError::Invalid("unwind.path is required".to_string()))?
        .as_str()
        .ok_or_else(|| TransformError::Invalid("unwind.path must be a string".to_string()))?
        .trim();
    if path_raw.is_empty() {
        return Err(TransformError::Invalid(
            "unwind.path must not be empty".to_string(),
        ));
    }
    let path = path_raw
        .strip_prefix('$')
        .unwrap_or(path_raw)
        .to_string();
    if path.is_empty() {
        return Err(TransformError::Invalid(
            "unwind.path must not be empty".to_string(),
        ));
    }

    for key in obj.keys() {
        if key != "path" {
            return Err(TransformError::Invalid(format!(
                "unwind only supports path (unknown `{key}`)"
            )));
        }
    }

    Ok(TransformOp::Unwind { path })
}

fn parse_equi_lookup(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "equiLookup must be an object with from, localField, foreignField, and as".to_string(),
        )
    })?;

    // Reject free-form / Mongo pipeline-style lookup extensions.
    for banned in ["pipeline", "let", "asOf", "$lookup"] {
        if obj.contains_key(banned) {
            return Err(TransformError::Invalid(format!(
                "equiLookup does not support `{banned}`; only declarative from/localField/foreignField/as (optional fromSchema)"
            )));
        }
    }

    let from = obj
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("equiLookup.from is required".to_string()))?
        .trim();
    if from.is_empty() {
        return Err(TransformError::Invalid(
            "equiLookup.from must not be empty".to_string(),
        ));
    }

    let local_field = obj
        .get("localField")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("equiLookup.localField is required".to_string()))?
        .trim();
    if local_field.is_empty() {
        return Err(TransformError::Invalid(
            "equiLookup.localField must not be empty".to_string(),
        ));
    }

    let foreign_field = obj
        .get("foreignField")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("equiLookup.foreignField is required".to_string()))?
        .trim();
    if foreign_field.is_empty() {
        return Err(TransformError::Invalid(
            "equiLookup.foreignField must not be empty".to_string(),
        ));
    }

    let as_name = obj
        .get("as")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("equiLookup.as is required".to_string()))?
        .trim();
    if as_name.is_empty() {
        return Err(TransformError::Invalid(
            "equiLookup.as must not be empty".to_string(),
        ));
    }

    let from_schema = match obj.get("fromSchema") {
        None => None,
        Some(Value::Null) => None,
        Some(v) => {
            let s = v.as_str().ok_or_else(|| {
                TransformError::Invalid("equiLookup.fromSchema must be a string".to_string())
            })?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
    };

    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "from" | "localField" | "foreignField" | "as" | "fromSchema"
        ) {
            return Err(TransformError::Invalid(format!(
                "equiLookup only supports from, localField, foreignField, as, and fromSchema (unknown `{key}`)"
            )));
        }
    }

    Ok(TransformOp::EquiLookup {
        from: from.to_string(),
        from_schema,
        local_field: local_field.to_string(),
        foreign_field: foreign_field.to_string(),
        as_name: as_name.to_string(),
    })
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
        "count" => AggregateOp::Count,
        "min" => AggregateOp::Min,
        "max" => AggregateOp::Max,
        "avg" => AggregateOp::Avg,
        other => {
            return Err(TransformError::Invalid(format!(
                "groupBy.aggregates[{index}].op {other:?} is unsupported; v1 allows sum, count, min, max, avg"
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

fn parse_distinct(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("distinct must be an object with fields".to_string())
    })?;
    let fields_value = obj.get("fields").ok_or_else(|| {
        TransformError::Invalid("distinct.fields is required".to_string())
    })?;
    let fields_arr = fields_value.as_array().ok_or_else(|| {
        TransformError::Invalid("distinct.fields must be an array of field names".to_string())
    })?;
    if fields_arr.is_empty() {
        return Err(TransformError::Invalid(
            "distinct.fields must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(fields_arr.len());
    for entry in fields_arr {
        let name = entry.as_str().ok_or_else(|| {
            TransformError::Invalid("distinct.fields entries must be strings".to_string())
        })?;
        if name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "distinct.fields entries must not be empty".to_string(),
            ));
        }
        fields.push(name.to_string());
    }
    if obj.keys().any(|k| k != "fields") {
        return Err(TransformError::Invalid(
            "distinct only supports fields".to_string(),
        ));
    }
    Ok(TransformOp::Distinct { fields })
}

fn parse_add_to_set(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "addToSet must be an object with keys, field, and as".to_string(),
        )
    })?;
    let keys_value = obj.get("keys").ok_or_else(|| {
        TransformError::Invalid("addToSet.keys is required".to_string())
    })?;
    let keys_arr = keys_value.as_array().ok_or_else(|| {
        TransformError::Invalid("addToSet.keys must be an array of field names".to_string())
    })?;
    if keys_arr.is_empty() {
        return Err(TransformError::Invalid(
            "addToSet.keys must not be empty".to_string(),
        ));
    }
    let mut keys = Vec::with_capacity(keys_arr.len());
    for entry in keys_arr {
        let name = entry.as_str().ok_or_else(|| {
            TransformError::Invalid("addToSet.keys entries must be strings".to_string())
        })?;
        if name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "addToSet.keys entries must not be empty".to_string(),
            ));
        }
        keys.push(name.to_string());
    }
    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("addToSet.field is required".to_string()))?;
    if field.trim().is_empty() {
        return Err(TransformError::Invalid(
            "addToSet.field must not be empty".to_string(),
        ));
    }
    let as_name = obj
        .get("as")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransformError::Invalid("addToSet.as is required".to_string()))?;
    if as_name.trim().is_empty() {
        return Err(TransformError::Invalid(
            "addToSet.as must not be empty".to_string(),
        ));
    }
    if obj
        .keys()
        .any(|k| k != "keys" && k != "field" && k != "as")
    {
        return Err(TransformError::Invalid(
            "addToSet only supports keys, field, and as".to_string(),
        ));
    }
    Ok(TransformOp::AddToSet {
        keys,
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
    let ms_ops = ops
        .iter()
        .filter(|op| matches!(op, TransformOp::Distinct { .. } | TransformOp::AddToSet { .. }))
        .count();
    if ms_ops > 1 {
        return Err(TransformError::Invalid(
            "v1 allows at most one distinct or addToSet operator per transform".to_string(),
        ));
    }
    let has_unwind = ops
        .iter()
        .any(|op| matches!(op, TransformOp::Unwind { .. }));
    if has_unwind && ms_ops > 0 {
        return Err(TransformError::Invalid(
            "v1 does not allow unwind together with distinct or addToSet".to_string(),
        ));
    }
    let has_union = ops
        .iter()
        .any(|op| matches!(op, TransformOp::Union { .. }));
    if has_union && ms_ops > 0 {
        return Err(TransformError::Invalid(
            "v1 does not allow union together with distinct or addToSet".to_string(),
        ));
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
            TransformOp::Distinct { fields: distinct } => {
                fields = Some(distinct.clone());
            }
            TransformOp::AddToSet { keys, as_name, .. } => {
                let mut names = keys.clone();
                names.push(as_name.clone());
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
            TransformOp::EquiLookup { as_name, .. } => {
                let mut cur = fields.unwrap_or_else(|| base_field_names.to_vec());
                if !cur.iter().any(|n| n == as_name) {
                    cur.push(as_name.clone());
                }
                fields = Some(cur);
            }
            TransformOp::Unwind { path } => {
                // Object-element flatten removes `path`; element field names appear
                // at evaluation time and are unioned from Derived rows by callers.
                let mut cur = fields.unwrap_or_else(|| base_field_names.to_vec());
                cur.retain(|n| n != path);
                fields = Some(cur);
            }
            TransformOp::Union { .. } => {
                // Concatenation does not rename fields; secondary columns may add
                // names at evaluation time and are merged by callers.
            }
            TransformOp::FilterEq { .. } => {}
        }
    }
    fields.unwrap_or_else(|| base_field_names.to_vec())
}

/// Secondary Base tables referenced by `equiLookup` / `union` steps (capture / Initial Load scope).
pub fn secondary_base_refs(ops: &[TransformOp]) -> Vec<SecondaryBaseRef> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for op in ops {
        let (from, from_schema) = match op {
            TransformOp::EquiLookup {
                from,
                from_schema,
                ..
            }
            | TransformOp::Union {
                from,
                from_schema,
            } => (from, from_schema),
            _ => continue,
        };
        let key = (
            from_schema.clone().unwrap_or_default(),
            from.to_ascii_uppercase(),
        );
        if seen.insert(key) {
            out.push(SecondaryBaseRef {
                table: from.clone(),
                schema: from_schema.clone(),
            });
        }
    }
    out
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
            TransformOp::EquiLookup {
                local_field,
                as_name,
                ..
            } => {
                // Output array depends on the local join key (foreign deps handled
                // separately via analyze_affect_on_base).
                let deps = resolve_field_deps(&lineage, closed, &removed, local_field);
                lineage.insert(as_name.clone(), deps);
                removed.remove(as_name);
            }
            TransformOp::Union { .. } => {
                // Primary-side field lineage is unchanged; secondary Base deps are
                // analyzed via analyze_affect_on_base when `from` changes.
            }
            TransformOp::Unwind { path } => {
                // Expanding `path` keeps its Base deps used; object flatten drops
                // the path field from output while element fields inherit those deps.
                let deps = resolve_field_deps(&lineage, closed, &removed, path);
                lineage.remove(path);
                removed.insert(path.clone());
                filter_used.extend(deps.iter().cloned());
                for entry_deps in lineage.values_mut() {
                    entry_deps.extend(deps.iter().cloned());
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
            TransformOp::Distinct { fields } => {
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
            TransformOp::AddToSet {
                keys,
                field,
                as_name,
            } => {
                let mut next = BTreeMap::new();
                for key in keys {
                    next.insert(
                        key.clone(),
                        resolve_field_deps(&lineage, closed, &removed, key),
                    );
                }
                next.insert(
                    as_name.clone(),
                    resolve_field_deps(&lineage, closed, &removed, field),
                );
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

/// Evaluate a Rich Transform over primary Base rows, producing Derived rows.
///
/// Single-Base transforms may omit secondary Bases. `equiLookup` / `union`
/// require the matching secondary table rows in [`evaluate_transform_with_bases`].
pub fn evaluate_transform(
    ops: &[TransformOp],
    rows: &[Map<String, Value>],
) -> Result<Vec<Map<String, Value>>, TransformError> {
    evaluate_transform_with_bases(ops, rows, &BTreeMap::new())
}

/// Evaluate a Rich Transform with secondary Base Datasets for `equiLookup` / `union`.
///
/// `secondary_bases` is keyed by Base table name (`equiLookup.from` / `union.from`).
pub fn evaluate_transform_with_bases(
    ops: &[TransformOp],
    primary_rows: &[Map<String, Value>],
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let mut current = primary_rows.to_vec();
    for op in ops {
        current = apply_op(op, current, secondary_bases)?;
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
    evaluate_transform_for_identities_with_bases(ops, base_rows, &BTreeMap::new(), identities)
}

/// Like [`evaluate_transform_for_identities`] with secondary Bases for `equiLookup` / `union`.
pub fn evaluate_transform_for_identities_with_bases(
    ops: &[TransformOp],
    primary_rows: &[Map<String, Value>],
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
    identities: &[Map<String, Value>],
) -> Result<Vec<Map<String, Value>>, TransformError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    // `union` concatenates secondary Bases into the stream — group-key filtering of
    // primary rows alone would drop secondary contributors. Full eval then filter.
    let has_union = ops
        .iter()
        .any(|op| matches!(op, TransformOp::Union { .. }));
    let Some(group_keys) = output_grouping_keys(ops) else {
        // Row-grain transforms: filter full evaluation to matching Output Identity.
        let all = evaluate_transform_with_bases(ops, primary_rows, secondary_bases)?;
        return Ok(all
            .into_iter()
            .filter(|row| identities.iter().any(|id| identity_matches_row(id, row)))
            .collect());
    };

    if has_union {
        let all = evaluate_transform_with_bases(ops, primary_rows, secondary_bases)?;
        return Ok(all
            .into_iter()
            .filter(|row| {
                identities
                    .iter()
                    .any(|id| row_matches_group_keys(row, id, &group_keys))
            })
            .collect());
    }

    let mut filtered = Vec::new();
    for row in primary_rows {
        if identities.iter().any(|id| row_matches_group_keys(row, id, &group_keys)) {
            filtered.push(row.clone());
        }
    }
    evaluate_transform_with_bases(ops, &filtered, secondary_bases)
}

/// Grouping / distinct keys that define Output Identity grain for incremental recompute.
fn output_grouping_keys(ops: &[TransformOp]) -> Option<Vec<String>> {
    ops.iter().rev().find_map(|op| match op {
        TransformOp::GroupBy { keys, .. } => Some(keys.clone()),
        TransformOp::Distinct { fields } => Some(fields.clone()),
        TransformOp::AddToSet { keys, .. } => Some(keys.clone()),
        _ => None,
    })
}

/// Whether this transform requires Maintenance State for correct value-level Affect Analysis.
///
/// True for `distinct` / `addToSet`. False for simple groupBy sum/count/min/max/avg and
/// row-grain operators — those must not invent blind side tables.
pub fn requires_maintenance_state(ops: &[TransformOp]) -> bool {
    ops.iter()
        .any(|op| matches!(op, TransformOp::Distinct { .. } | TransformOp::AddToSet { .. }))
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

/// Affect Analysis for a change on the Pipeline's primary Base Dataset.
///
/// `pre_apply` is the Base row before applying the change (required for Update/Delete).
/// `after` is the change after-image (required for Insert/Update).
///
/// For multi-Base `equiLookup` Pipelines, prefer [`analyze_affect_on_base`] so foreign
/// Base changes resolve the correct primary Output Identities. Pipelines with `unwind`
/// after `equiLookup` should use [`analyze_affect_on_base_with_bases`] so expansion
/// can read secondary Bases.
pub fn analyze_affect(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
) -> Result<AffectOutcome, TransformError> {
    analyze_primary_affect(ops, kind, pre_apply, after, &BTreeMap::new())
}

/// Affect Analysis when the changed Base table is known (primary, equiLookup `from`,
/// or `union.from`).
///
/// `primary_table` is the Pipeline `source.table`. `primary_rows` are current primary
/// Base rows (needed to resolve Output Identities when a foreign Base changes).
///
/// For `unwind` (optionally after `equiLookup`), prefer [`analyze_affect_on_base_with_bases`]
/// so 1→N Output Identities expand from secondary Bases.
pub fn analyze_affect_on_base(
    ops: &[TransformOp],
    changed_table: &str,
    primary_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    primary_rows: &[Map<String, Value>],
) -> Result<AffectOutcome, TransformError> {
    analyze_affect_on_base_with_bases(
        ops,
        changed_table,
        primary_table,
        kind,
        pre_apply,
        after,
        primary_rows,
        &BTreeMap::new(),
    )
}

/// Like [`analyze_affect_on_base`] with secondary Bases for `equiLookup` / `union` / `unwind`.
pub fn analyze_affect_on_base_with_bases(
    ops: &[TransformOp],
    changed_table: &str,
    primary_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    primary_rows: &[Map<String, Value>],
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<AffectOutcome, TransformError> {
    if table_names_eq(changed_table, primary_table) {
        return analyze_primary_affect(ops, kind, pre_apply, after, secondary_bases);
    }
    if is_equi_lookup_from_table(ops, changed_table) {
        return analyze_foreign_equi_lookup_affect(
            ops,
            changed_table,
            kind,
            pre_apply,
            after,
            primary_rows,
            secondary_bases,
        );
    }
    if is_union_from_table(ops, changed_table) {
        return analyze_union_secondary_affect(ops, changed_table, kind, pre_apply, after);
    }
    // Unknown table name — fall back to primary-side analysis (legacy callers).
    analyze_primary_affect(ops, kind, pre_apply, after, secondary_bases)
}

fn analyze_primary_affect(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<AffectOutcome, TransformError> {
    analyze_primary_affect_inner(ops, kind, pre_apply, after, None, secondary_bases)
}

/// Affect Analysis with Maintenance State for value-level distinct/addToSet skips.
///
/// Callers must pass the *pre-change* Maintenance State. Update refcounts via
/// [`maintain_state_for_change`] after deciding the outcome (including skips).
pub fn analyze_affect_with_maintenance(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    state: &MaintenanceState,
) -> Result<AffectOutcome, TransformError> {
    analyze_primary_affect_inner(ops, kind, pre_apply, after, Some(state), &BTreeMap::new())
}

fn analyze_primary_affect_inner(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    state: Option<&MaintenanceState>,
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<AffectOutcome, TransformError> {
    let deps = affect_deps(ops);
    let group_keys = output_grouping_keys(ops);

    match kind {
        BaseChangeKind::Insert => {
            let after = after.ok_or_else(|| {
                TransformError::Invalid("Insert Affect Analysis requires after-image".into())
            })?;
            if expands_output_grain(ops) {
                return Ok(AffectOutcome::Recompute {
                    identities: expand_primary_to_output_rows(ops, after, secondary_bases)?,
                });
            }
            // Shape through prefix ops so distinct/addToSet keys match Maintenance State.
            let Some(shaped_after) = shape_row_before_ms_operator(ops, after)? else {
                // Filtered out before distinct/addToSet — no Derived change.
                return Ok(AffectOutcome::SkipValueUnchanged);
            };
            let identity = identity_from_row(ops, &shaped_after, group_keys.as_deref())?;
            if let Some(outcome) = value_level_affect(
                ops,
                kind,
                None,
                Some(&shaped_after),
                state,
                &identity,
                None,
            )? {
                return Ok(outcome);
            }
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
            if expands_output_grain(ops) {
                return Ok(AffectOutcome::Recompute {
                    identities: expand_primary_to_output_rows(ops, pre, secondary_bases)?,
                });
            }
            let Some(shaped_pre) = shape_row_before_ms_operator(ops, pre)? else {
                return Ok(AffectOutcome::SkipValueUnchanged);
            };
            let identity = identity_from_row(ops, &shaped_pre, group_keys.as_deref())?;
            if let Some(outcome) = value_level_affect(
                ops,
                kind,
                Some(&shaped_pre),
                None,
                state,
                &identity,
                None,
            )? {
                return Ok(outcome);
            }
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
            if expands_output_grain(ops) {
                let mut identities = expand_primary_to_output_rows(ops, pre, secondary_bases)?;
                for id in expand_primary_to_output_rows(ops, after, secondary_bases)? {
                    push_unique_identity(&mut identities, id);
                }
                return Ok(AffectOutcome::Recompute { identities });
            }
            // For Maintenance State ops, shape both images so keys/values match refcounts.
            let (pre_for_id, after_for_id) = if requires_maintenance_state(ops) {
                let shaped_pre = shape_row_before_ms_operator(ops, pre)?;
                let shaped_after = shape_row_before_ms_operator(ops, after)?;
                match (shaped_pre, shaped_after) {
                    (None, None) => return Ok(AffectOutcome::SkipValueUnchanged),
                    (None, Some(after_shaped)) => {
                        let after_id =
                            identity_from_row(ops, &after_shaped, group_keys.as_deref())?;
                        if let Some(outcome) = value_level_affect(
                            ops,
                            BaseChangeKind::Insert,
                            None,
                            Some(&after_shaped),
                            state,
                            &after_id,
                            None,
                        )? {
                            return Ok(outcome);
                        }
                        return Ok(AffectOutcome::Recompute {
                            identities: vec![after_id],
                        });
                    }
                    (Some(pre_shaped), None) => {
                        let pre_id = identity_from_row(ops, &pre_shaped, group_keys.as_deref())?;
                        if let Some(outcome) = value_level_affect(
                            ops,
                            BaseChangeKind::Delete,
                            Some(&pre_shaped),
                            None,
                            state,
                            &pre_id,
                            None,
                        )? {
                            return Ok(outcome);
                        }
                        return Ok(AffectOutcome::Recompute {
                            identities: vec![pre_id],
                        });
                    }
                    (Some(pre_shaped), Some(after_shaped)) => (pre_shaped, after_shaped),
                }
            } else {
                (pre.clone(), after.clone())
            };
            let after_id = identity_from_row(ops, &after_for_id, group_keys.as_deref())?;
            // groupBy/distinct/addToSet key moves need pre-apply + after identities.
            // Row-grain transforms keep a single after-image identity.
            if group_keys.is_some() {
                let pre_id = identity_from_row(ops, &pre_for_id, group_keys.as_deref())?;
                if let Some(outcome) = value_level_affect(
                    ops,
                    kind,
                    Some(&pre_for_id),
                    Some(&after_for_id),
                    state,
                    &after_id,
                    Some(&pre_id),
                )? {
                    return Ok(outcome);
                }
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

/// Final Derived grain expands 1→N via `unwind` (and is not collapsed by groupBy/…).
fn expands_output_grain(ops: &[TransformOp]) -> bool {
    output_grouping_keys(ops).is_none()
        && ops.iter().any(|op| matches!(op, TransformOp::Unwind { .. }))
}

fn expand_primary_to_output_rows(
    ops: &[TransformOp],
    primary: &Map<String, Value>,
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    evaluate_transform_with_bases(ops, std::slice::from_ref(primary), secondary_bases)
}

fn push_unique_identity(identities: &mut Vec<Map<String, Value>>, identity: Map<String, Value>) {
    if !identities
        .iter()
        .any(|existing| identity_maps_eq(existing, &identity))
    {
        identities.push(identity);
    }
}

/// Shape a Base row through operators before `distinct` / `addToSet` (filter/project/…).
///
/// Returns `None` when prefix ops drop the row (e.g. filter miss) — no MS contribution.
fn shape_row_before_ms_operator(
    ops: &[TransformOp],
    row: &Map<String, Value>,
) -> Result<Option<Map<String, Value>>, TransformError> {
    if !requires_maintenance_state(ops) {
        return Ok(Some(row.clone()));
    }
    let (prefix, _) = split_at_ms_operator(ops)?;
    if prefix.is_empty() {
        return Ok(Some(row.clone()));
    }
    let out = evaluate_transform_with_bases(prefix, &[row.clone()], &BTreeMap::new())?;
    Ok(out.into_iter().next())
}

/// Value-level Affect Analysis using Maintenance State refcounts.
///
/// Returns `Some(SkipValueUnchanged)` or `Some(Recompute{…})` when MS applies;
/// `None` when the caller should use the default recompute path (no MS / no state).
fn value_level_affect(
    ops: &[TransformOp],
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    state: Option<&MaintenanceState>,
    after_identity: &Map<String, Value>,
    pre_identity: Option<&Map<String, Value>>,
) -> Result<Option<AffectOutcome>, TransformError> {
    if !requires_maintenance_state(ops) {
        return Ok(None);
    }
    let Some(state) = state else {
        // Without state, fall back to always-recompute (correct, not optimal).
        return Ok(None);
    };

    if let Some(TransformOp::Distinct { .. }) = ms_operator(ops) {
        return Ok(Some(distinct_value_level_affect(
            kind,
            state,
            after_identity,
            pre_identity,
        )));
    }
    if let Some(TransformOp::AddToSet { field, .. }) = ms_operator(ops) {
        return Ok(Some(add_to_set_value_level_affect(
            kind,
            state,
            field,
            pre_apply,
            after,
            after_identity,
            pre_identity,
        )));
    }
    Ok(None)
}

fn ms_operator(ops: &[TransformOp]) -> Option<&TransformOp> {
    ops.iter()
        .find(|op| matches!(op, TransformOp::Distinct { .. } | TransformOp::AddToSet { .. }))
}

fn distinct_value_level_affect(
    kind: BaseChangeKind,
    state: &MaintenanceState,
    after_identity: &Map<String, Value>,
    pre_identity: Option<&Map<String, Value>>,
) -> AffectOutcome {
    match kind {
        BaseChangeKind::Insert => {
            if state_refcount(state, after_identity, None) > 0 {
                AffectOutcome::SkipValueUnchanged
            } else {
                AffectOutcome::Recompute {
                    identities: vec![after_identity.clone()],
                }
            }
        }
        BaseChangeKind::Delete => {
            if state_refcount(state, after_identity, None) > 1 {
                AffectOutcome::SkipValueUnchanged
            } else {
                AffectOutcome::Recompute {
                    identities: vec![after_identity.clone()],
                }
            }
        }
        BaseChangeKind::Update => {
            let pre_id = pre_identity.unwrap_or(after_identity);
            let mut identities = Vec::new();
            // Leaving the old identity: only when this was the last contributor.
            if !identity_maps_eq(pre_id, after_identity)
                && state_refcount(state, pre_id, None) <= 1
            {
                identities.push(pre_id.clone());
            }
            // Entering a new identity: only when it was absent before.
            if !identity_maps_eq(pre_id, after_identity)
                && state_refcount(state, after_identity, None) == 0
            {
                identities.push(after_identity.clone());
            }
            // Same-identity updates cannot change distinct Derived output.
            if identities.is_empty() {
                AffectOutcome::SkipValueUnchanged
            } else {
                AffectOutcome::Recompute { identities }
            }
        }
    }
}

fn add_to_set_value_level_affect(
    kind: BaseChangeKind,
    state: &MaintenanceState,
    field: &str,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    after_identity: &Map<String, Value>,
    pre_identity: Option<&Map<String, Value>>,
) -> AffectOutcome {
    let after_value = after.and_then(|row| {
        row.get(field)
            .cloned()
            .filter(|v| !v.is_null())
    });
    let pre_value = pre_apply.and_then(|row| {
        row.get(field)
            .cloned()
            .filter(|v| !v.is_null())
    });

    match kind {
        BaseChangeKind::Insert => match after_value {
            None => AffectOutcome::SkipValueUnchanged,
            Some(ref v) if state_refcount(state, after_identity, Some(v)) > 0 => {
                AffectOutcome::SkipValueUnchanged
            }
            Some(_) => AffectOutcome::Recompute {
                identities: vec![after_identity.clone()],
            },
        },
        BaseChangeKind::Delete => match pre_value {
            None => AffectOutcome::SkipValueUnchanged,
            Some(ref v) if state_refcount(state, after_identity, Some(v)) > 1 => {
                AffectOutcome::SkipValueUnchanged
            }
            Some(_) => AffectOutcome::Recompute {
                identities: vec![after_identity.clone()],
            },
        },
        BaseChangeKind::Update => {
            let pre_id = pre_identity.unwrap_or(after_identity);
            let mut identities = Vec::new();
            let key_moved = !identity_maps_eq(pre_id, after_identity);
            let value_changed = match (&pre_value, &after_value) {
                (Some(a), Some(b)) => !json_values_eq(a, b),
                (None, Some(_)) | (Some(_), None) => true,
                (None, None) => false,
            };

            if key_moved {
                // Old group: remove old value from set if last ref.
                if let Some(ref v) = pre_value {
                    if state_refcount(state, pre_id, Some(v)) <= 1 {
                        identities.push(pre_id.clone());
                    }
                }
                // New group: add value if new to that set.
                if let Some(ref v) = after_value {
                    if state_refcount(state, after_identity, Some(v)) == 0
                        && !identities
                            .iter()
                            .any(|id| identity_maps_eq(id, after_identity))
                    {
                        identities.push(after_identity.clone());
                    }
                }
            } else if value_changed {
                let mut set_changes = false;
                if let Some(ref v) = pre_value {
                    if state_refcount(state, after_identity, Some(v)) <= 1 {
                        set_changes = true;
                    }
                }
                if let Some(ref v) = after_value {
                    if state_refcount(state, after_identity, Some(v)) == 0 {
                        set_changes = true;
                    }
                }
                if set_changes {
                    identities.push(after_identity.clone());
                }
            }

            if identities.is_empty() {
                AffectOutcome::SkipValueUnchanged
            } else {
                AffectOutcome::Recompute { identities }
            }
        }
    }
}

fn state_refcount(
    state: &MaintenanceState,
    identity: &Map<String, Value>,
    value: Option<&Value>,
) -> i64 {
    state
        .entries
        .iter()
        .find(|e| {
            identity_maps_eq(&e.identity, identity)
                && match (value, &e.value) {
                    (None, None) => true,
                    (Some(a), Some(b)) => json_values_eq(a, b),
                    _ => false,
                }
        })
        .map(|e| e.refcount)
        .unwrap_or(0)
}

/// Build Maintenance State from current Base rows for distinct / addToSet.
pub fn build_maintenance_state(
    ops: &[TransformOp],
    rows: &[Map<String, Value>],
) -> Result<MaintenanceState, TransformError> {
    let mut state = MaintenanceState::default();
    if !requires_maintenance_state(ops) {
        return Ok(state);
    }
    // Evaluate shaping ops before the MS operator so keys/fields match operator inputs.
    let (prefix, ms_op) = split_at_ms_operator(ops)?;
    let shaped = if prefix.is_empty() {
        rows.to_vec()
    } else {
        evaluate_transform_with_bases(prefix, rows, &BTreeMap::new())?
    };
    match ms_op {
        TransformOp::Distinct { fields } => {
            for row in &shaped {
                let identity = identity_map_from_fields(fields, row);
                bump_state(&mut state, identity, None, 1);
            }
        }
        TransformOp::AddToSet { keys, field, .. } => {
            for row in &shaped {
                let identity = identity_map_from_fields(keys, row);
                if let Some(value) = row.get(field).cloned().filter(|v| !v.is_null()) {
                    bump_state(&mut state, identity, Some(value), 1);
                }
            }
        }
        _ => {}
    }
    Ok(state)
}

/// Apply a Base change to Maintenance State refcounts (call after Affect Analysis).
pub fn maintain_state_for_change(
    ops: &[TransformOp],
    state: &mut MaintenanceState,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
) -> Result<(), TransformError> {
    if !requires_maintenance_state(ops) {
        return Ok(());
    }
    let (prefix, ms_op) = split_at_ms_operator(ops)?;
    let shape = |row: &Map<String, Value>| -> Result<Option<Map<String, Value>>, TransformError> {
        if prefix.is_empty() {
            return Ok(Some(row.clone()));
        }
        let out = evaluate_transform_with_bases(prefix, &[row.clone()], &BTreeMap::new())?;
        Ok(out.into_iter().next())
    };

    match ms_op {
        TransformOp::Distinct { fields } => match kind {
            BaseChangeKind::Insert => {
                let after = after.ok_or_else(|| {
                    TransformError::Invalid("Insert Maintenance State requires after-image".into())
                })?;
                if let Some(shaped) = shape(after)? {
                    bump_state(state, identity_map_from_fields(fields, &shaped), None, 1);
                }
            }
            BaseChangeKind::Delete => {
                let pre = pre_apply.ok_or_else(|| {
                    TransformError::Invalid(
                        "Delete Maintenance State requires pre-apply Base row".into(),
                    )
                })?;
                if let Some(shaped) = shape(pre)? {
                    bump_state(state, identity_map_from_fields(fields, &shaped), None, -1);
                }
            }
            BaseChangeKind::Update => {
                let pre = pre_apply.ok_or_else(|| {
                    TransformError::Invalid(
                        "Update Maintenance State requires pre-apply Base row".into(),
                    )
                })?;
                let after = after.ok_or_else(|| {
                    TransformError::Invalid("Update Maintenance State requires after-image".into())
                })?;
                if let Some(pre_shaped) = shape(pre)? {
                    bump_state(
                        state,
                        identity_map_from_fields(fields, &pre_shaped),
                        None,
                        -1,
                    );
                }
                if let Some(after_shaped) = shape(after)? {
                    bump_state(
                        state,
                        identity_map_from_fields(fields, &after_shaped),
                        None,
                        1,
                    );
                }
            }
        },
        TransformOp::AddToSet { keys, field, .. } => match kind {
            BaseChangeKind::Insert => {
                let after = after.ok_or_else(|| {
                    TransformError::Invalid("Insert Maintenance State requires after-image".into())
                })?;
                if let Some(shaped) = shape(after)? {
                    if let Some(value) = shaped.get(field).cloned().filter(|v| !v.is_null()) {
                        bump_state(
                            state,
                            identity_map_from_fields(keys, &shaped),
                            Some(value),
                            1,
                        );
                    }
                }
            }
            BaseChangeKind::Delete => {
                let pre = pre_apply.ok_or_else(|| {
                    TransformError::Invalid(
                        "Delete Maintenance State requires pre-apply Base row".into(),
                    )
                })?;
                if let Some(shaped) = shape(pre)? {
                    if let Some(value) = shaped.get(field).cloned().filter(|v| !v.is_null()) {
                        bump_state(
                            state,
                            identity_map_from_fields(keys, &shaped),
                            Some(value),
                            -1,
                        );
                    }
                }
            }
            BaseChangeKind::Update => {
                let pre = pre_apply.ok_or_else(|| {
                    TransformError::Invalid(
                        "Update Maintenance State requires pre-apply Base row".into(),
                    )
                })?;
                let after = after.ok_or_else(|| {
                    TransformError::Invalid("Update Maintenance State requires after-image".into())
                })?;
                if let Some(pre_shaped) = shape(pre)? {
                    if let Some(value) = pre_shaped.get(field).cloned().filter(|v| !v.is_null()) {
                        bump_state(
                            state,
                            identity_map_from_fields(keys, &pre_shaped),
                            Some(value),
                            -1,
                        );
                    }
                }
                if let Some(after_shaped) = shape(after)? {
                    if let Some(value) = after_shaped.get(field).cloned().filter(|v| !v.is_null()) {
                        bump_state(
                            state,
                            identity_map_from_fields(keys, &after_shaped),
                            Some(value),
                            1,
                        );
                    }
                }
            }
        },
        _ => {}
    }
    Ok(())
}

fn split_at_ms_operator(
    ops: &[TransformOp],
) -> Result<(&[TransformOp], &TransformOp), TransformError> {
    let idx = ops
        .iter()
        .position(|op| matches!(op, TransformOp::Distinct { .. } | TransformOp::AddToSet { .. }))
        .ok_or_else(|| {
            TransformError::Invalid(
                "Maintenance State requested but transform has no distinct/addToSet".into(),
            )
        })?;
    Ok((&ops[..idx], &ops[idx]))
}

fn identity_map_from_fields(fields: &[String], row: &Map<String, Value>) -> Map<String, Value> {
    let mut identity = Map::new();
    for field in fields {
        identity.insert(
            field.clone(),
            row.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    identity
}

fn bump_state(
    state: &mut MaintenanceState,
    identity: Map<String, Value>,
    value: Option<Value>,
    delta: i64,
) {
    let entry_matches = |e: &MaintenanceEntry| {
        identity_maps_eq(&e.identity, &identity)
            && match (&value, &e.value) {
                (None, None) => true,
                (Some(a), Some(b)) => json_values_eq(a, b),
                _ => false,
            }
    };
    if let Some(pos) = state.entries.iter().position(entry_matches) {
        state.entries[pos].refcount += delta;
        if state.entries[pos].refcount <= 0 {
            state.entries.remove(pos);
        }
    } else if delta > 0 {
        state.entries.push(MaintenanceEntry {
            identity,
            value,
            refcount: delta,
        });
    }
}

fn analyze_foreign_equi_lookup_affect(
    ops: &[TransformOp],
    changed_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    primary_rows: &[Map<String, Value>],
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<AffectOutcome, TransformError> {
    let lookups: Vec<&TransformOp> = ops
        .iter()
        .filter(|op| match op {
            TransformOp::EquiLookup { from, as_name, .. } => {
                table_names_eq(from, changed_table) && equi_lookup_as_survives(ops, as_name)
            }
            _ => false,
        })
        .collect();
    if lookups.is_empty() {
        // equiLookup `as` was removed/projected away — foreign fields unused.
        return Ok(AffectOutcome::SkipUnusedFields);
    }

    let mut join_values: Vec<Value> = Vec::new();
    match kind {
        BaseChangeKind::Insert => {
            let after = after.ok_or_else(|| {
                TransformError::Invalid("Insert Affect Analysis requires after-image".into())
            })?;
            for op in &lookups {
                if let TransformOp::EquiLookup { foreign_field, .. } = op {
                    if let Some(v) = after.get(foreign_field) {
                        push_unique_join_value(&mut join_values, v.clone());
                    }
                }
            }
        }
        BaseChangeKind::Delete => {
            let pre = pre_apply.ok_or_else(|| {
                TransformError::Invalid(
                    "Delete Affect Analysis requires pre-apply Base row".into(),
                )
            })?;
            for op in &lookups {
                if let TransformOp::EquiLookup { foreign_field, .. } = op {
                    if let Some(v) = pre.get(foreign_field) {
                        push_unique_join_value(&mut join_values, v.clone());
                    }
                }
            }
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
            // Full foreign rows are embedded — any field change can affect Derived.
            if changed_fields(pre, after).is_empty() {
                return Ok(AffectOutcome::SkipUnusedFields);
            }
            for op in &lookups {
                if let TransformOp::EquiLookup { foreign_field, .. } = op {
                    if let Some(v) = pre.get(foreign_field) {
                        push_unique_join_value(&mut join_values, v.clone());
                    }
                    if let Some(v) = after.get(foreign_field) {
                        push_unique_join_value(&mut join_values, v.clone());
                    }
                }
            }
        }
    }

    if join_values.is_empty() {
        return Ok(AffectOutcome::Recompute {
            identities: Vec::new(),
        });
    }

    let mut matching_primaries = Vec::new();
    for op in &lookups {
        let TransformOp::EquiLookup { local_field, from, .. } = op else {
            continue;
        };
        for primary in primary_rows {
            // localField is read from the shaped stream (after prior project/rename/…),
            // not raw Base column names — match Mongo-style stage ordering.
            let shaped = shape_row_until_equi_lookup(ops, primary, from)?;
            let Some(local_val) = shaped.get(local_field) else {
                continue;
            };
            if !join_values.iter().any(|jv| json_values_eq(jv, local_val)) {
                continue;
            }
            if !matching_primaries
                .iter()
                .any(|existing: &Map<String, Value>| identity_maps_eq(existing, primary))
            {
                matching_primaries.push(primary.clone());
            }
        }
    }

    if expands_output_grain(ops) {
        return expand_foreign_unwind_identities(
            ops,
            changed_table,
            kind,
            pre_apply,
            after,
            &matching_primaries,
            secondary_bases,
        );
    }

    let group_keys = output_grouping_keys(ops);
    let mut identities = Vec::new();
    for primary in &matching_primaries {
        let identity = identity_from_row(ops, primary, group_keys.as_deref())?;
        push_unique_identity(&mut identities, identity);
    }
    Ok(AffectOutcome::Recompute { identities })
}

/// Expand matching primaries through equiLookup+unwind with pre/after foreign Bases
/// so disappeared Output Identities are included for Delivery delete.
fn expand_foreign_unwind_identities(
    ops: &[TransformOp],
    changed_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
    matching_primaries: &[Map<String, Value>],
    secondary_after: &BTreeMap<String, Vec<Map<String, Value>>>,
) -> Result<AffectOutcome, TransformError> {
    let mut identities = Vec::new();
    let secondary_pre = foreign_secondary_pre_image(
        secondary_after,
        changed_table,
        kind,
        pre_apply,
        after,
    )?;
    for primary in matching_primaries {
        if let Some(pre_bases) = &secondary_pre {
            for id in expand_primary_to_output_rows(ops, primary, pre_bases)? {
                push_unique_identity(&mut identities, id);
            }
        }
        for id in expand_primary_to_output_rows(ops, primary, secondary_after)? {
            push_unique_identity(&mut identities, id);
        }
    }
    Ok(AffectOutcome::Recompute { identities })
}

fn foreign_secondary_pre_image(
    secondary_after: &BTreeMap<String, Vec<Map<String, Value>>>,
    changed_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
) -> Result<Option<BTreeMap<String, Vec<Map<String, Value>>>>, TransformError> {
    match kind {
        BaseChangeKind::Insert => Ok(None),
        BaseChangeKind::Delete => {
            let pre = pre_apply.ok_or_else(|| {
                TransformError::Invalid(
                    "Delete Affect Analysis requires pre-apply Base row".into(),
                )
            })?;
            let mut pre_bases = secondary_after.clone();
            let rows = secondary_table_rows_mut(&mut pre_bases, changed_table);
            rows.push(pre.clone());
            Ok(Some(pre_bases))
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
            let mut pre_bases = secondary_after.clone();
            let rows = secondary_table_rows_mut(&mut pre_bases, changed_table);
            rows.retain(|row| !identity_maps_eq(row, after));
            rows.push(pre.clone());
            Ok(Some(pre_bases))
        }
    }
}

fn secondary_table_rows_mut<'a>(
    secondary: &'a mut BTreeMap<String, Vec<Map<String, Value>>>,
    table: &str,
) -> &'a mut Vec<Map<String, Value>> {
    if let Some(key) = secondary
        .keys()
        .find(|name| table_names_eq(name, table))
        .cloned()
    {
        return secondary.get_mut(&key).expect("secondary key");
    }
    secondary.entry(table.to_string()).or_default()
}

/// Shape a primary Base row through operators before the target `equiLookup.from` step.
fn shape_row_until_equi_lookup(
    ops: &[TransformOp],
    row: &Map<String, Value>,
    from_table: &str,
) -> Result<Map<String, Value>, TransformError> {
    let mut current = vec![row.clone()];
    let empty = BTreeMap::new();
    for op in ops {
        match op {
            TransformOp::EquiLookup { from, .. } if table_names_eq(from, from_table) => {
                break;
            }
            TransformOp::FilterEq { .. }
            | TransformOp::GroupBy { .. }
            | TransformOp::Unwind { .. }
            | TransformOp::Union { .. } => {}
            TransformOp::EquiLookup { .. } => {
                // Prior equiLookup against another table still needs secondary Bases;
                // for join-key matching we only need field shaping, so skip.
            }
            other => {
                current = apply_op(other, current, &empty)?;
            }
        }
    }
    Ok(current.into_iter().next().unwrap_or_default())
}

fn push_unique_join_value(values: &mut Vec<Value>, value: Value) {
    if !values.iter().any(|existing| json_values_eq(existing, &value)) {
        values.push(value);
    }
}

fn table_names_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn is_equi_lookup_from_table(ops: &[TransformOp], table: &str) -> bool {
    ops.iter().any(|op| match op {
        TransformOp::EquiLookup { from, .. } => table_names_eq(from, table),
        _ => false,
    })
}

fn is_union_from_table(ops: &[TransformOp], table: &str) -> bool {
    ops.iter().any(|op| match op {
        TransformOp::Union { from, .. } => table_names_eq(from, table),
        _ => false,
    })
}

/// Affect Analysis when a `union.from` secondary Base changes.
///
/// Secondary rows enter the stream at the matching `union` step; only operators
/// after that step shape the contributed row (Mongo `$unionWith` without pipeline).
fn analyze_union_secondary_affect(
    ops: &[TransformOp],
    changed_table: &str,
    kind: BaseChangeKind,
    pre_apply: Option<&Map<String, Value>>,
    after: Option<&Map<String, Value>>,
) -> Result<AffectOutcome, TransformError> {
    let Some(idx) = ops.iter().position(|op| match op {
        TransformOp::Union { from, .. } => table_names_eq(from, changed_table),
        _ => false,
    }) else {
        return Ok(AffectOutcome::SkipUnusedFields);
    };
    let suffix = &ops[idx + 1..];
    analyze_primary_affect(suffix, kind, pre_apply, after, &BTreeMap::new())
}

/// Whether an equiLookup `as` field (possibly renamed) still contributes to Derived output.
fn equi_lookup_as_survives(ops: &[TransformOp], as_name: &str) -> bool {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for op in ops {
        match op {
            TransformOp::EquiLookup {
                as_name: name, ..
            } if name == as_name => {
                names.insert(name.clone());
            }
            TransformOp::Project { fields } => {
                names.retain(|n| fields.iter().any(|f| f == n));
            }
            TransformOp::Remove { fields } => {
                for field in fields {
                    names.remove(field);
                }
            }
            TransformOp::Rename { fields } => {
                for spec in fields {
                    if names.remove(&spec.from) {
                        names.insert(spec.to.clone());
                    }
                }
            }
            TransformOp::GroupBy { keys, aggregates } => {
                names.retain(|n| {
                    keys.iter().any(|k| k == n)
                        || aggregates.iter().any(|a| a.as_name == *n || a.field == *n)
                });
            }
            TransformOp::Distinct { fields } => {
                names.retain(|n| fields.iter().any(|f| f == n));
            }
            TransformOp::AddToSet { keys, as_name, .. } => {
                names.retain(|n| keys.iter().any(|k| k == n) || n == as_name);
            }
            TransformOp::Unwind { path: _ } => {
                // Object flatten removes `path` from the row shape, but Derived still
                // depends on the lookup array — keep names so foreign Affect runs.
            }
            _ => {}
        }
    }
    !names.is_empty()
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
                    "Base row missing grouping key {key} for Affect Analysis"
                ))
            })?;
            identity.insert(key.clone(), value);
        }
        return Ok(identity);
    }
    // Row-grain: shape the Base row through project/addFields/rename/remove so
    // recompute identity keys match Derived field names (rename-safe).
    let identity = shape_row_for_identity(ops, row)?;
    if identity.is_empty() {
        return Err(TransformError::Invalid(
            "cannot derive Affect Analysis identity from Base row".into(),
        ));
    }
    Ok(identity)
}

/// Apply field-shaping operators (not filter/groupBy/distinct/addToSet/equiLookup/unwind/union)
/// so identity keys match Derived.
///
/// `equiLookup` / `unwind` / `union` are skipped: Output Identity for non-expanding
/// row-grain is on primary-side fields; joining/expanding/concatenating would
/// require secondary Bases and is handled by multi-Base Affect paths instead.
fn shape_row_for_identity(
    ops: &[TransformOp],
    row: &Map<String, Value>,
) -> Result<Map<String, Value>, TransformError> {
    let mut current = vec![row.clone()];
    let empty = BTreeMap::new();
    for op in ops {
        match op {
            TransformOp::FilterEq { .. }
            | TransformOp::GroupBy { .. }
            | TransformOp::Distinct { .. }
            | TransformOp::AddToSet { .. }
            | TransformOp::EquiLookup { .. }
            | TransformOp::Unwind { .. }
            | TransformOp::Union { .. } => {}
            other => {
                current = apply_op(other, current, &empty)?;
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
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
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
        TransformOp::EquiLookup {
            from,
            local_field,
            foreign_field,
            as_name,
            ..
        } => apply_equi_lookup(rows, secondary_bases, from, local_field, foreign_field, as_name),
        TransformOp::Unwind { path } => apply_unwind(path, rows),
        TransformOp::Union { from, .. } => apply_union(rows, secondary_bases, from),
        TransformOp::GroupBy { keys, aggregates } => apply_group_by(keys, aggregates, rows),
        TransformOp::Distinct { fields } => Ok(apply_distinct(fields, rows)),
        TransformOp::AddToSet {
            keys,
            field,
            as_name,
        } => apply_add_to_set(keys, field, as_name, rows),
    }
}

fn apply_union(
    rows: Vec<Map<String, Value>>,
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
    from: &str,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let secondary_rows = secondary_bases
        .get(from)
        .or_else(|| {
            secondary_bases
                .iter()
                .find(|(name, _)| table_names_eq(name, from))
                .map(|(_, rows)| rows)
        })
        .ok_or_else(|| {
            TransformError::Invalid(format!(
                "union.from Base Dataset `{from}` was not loaded for evaluation"
            ))
        })?;
    let mut out = rows;
    out.extend(secondary_rows.iter().cloned());
    Ok(out)
}

fn apply_unwind(
    path: &str,
    rows: Vec<Map<String, Value>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let mut out = Vec::new();
    for mut row in rows {
        match row.remove(path) {
            None | Some(Value::Null) => {
                // Missing / null → no Derived rows (preserveNullAndEmptyArrays unsupported).
            }
            Some(Value::Array(items)) => {
                if items.is_empty() {
                    continue;
                }
                for item in items {
                    let mut expanded = row.clone();
                    match item {
                        Value::Object(obj) => {
                            // Flatten object elements so Output Identity can key
                            // Delivery on unwound fields (e.g. ORDER_ID).
                            for (key, value) in obj {
                                expanded.insert(key, value);
                            }
                        }
                        other => {
                            expanded.insert(path.to_string(), other);
                        }
                    }
                    out.push(expanded);
                }
            }
            Some(other) => {
                return Err(TransformError::Invalid(format!(
                    "unwind.path `{path}` must be an array; got {other}"
                )));
            }
        }
    }
    Ok(out)
}

fn apply_distinct(fields: &[String], rows: Vec<Map<String, Value>>) -> Vec<Map<String, Value>> {
    let mut seen: BTreeSet<Vec<ValueKey>> = BTreeSet::new();
    let mut out = Vec::new();
    for row in rows {
        let key_parts: Vec<ValueKey> = fields
            .iter()
            .map(|field| ValueKey(row.get(field).cloned().unwrap_or(Value::Null)))
            .collect();
        if seen.insert(key_parts.clone()) {
            let mut derived = Map::new();
            for (index, field) in fields.iter().enumerate() {
                derived.insert(field.clone(), key_parts[index].0.clone());
            }
            out.push(derived);
        }
    }
    out
}

fn apply_add_to_set(
    keys: &[String],
    field: &str,
    as_name: &str,
    rows: Vec<Map<String, Value>>,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let mut groups: BTreeMap<Vec<ValueKey>, BTreeSet<ValueKey>> = BTreeMap::new();
    for row in rows {
        let mut key_parts = Vec::with_capacity(keys.len());
        for key in keys {
            key_parts.push(ValueKey(row.get(key).cloned().unwrap_or(Value::Null)));
        }
        let values = groups.entry(key_parts).or_default();
        if let Some(value) = row.get(field).cloned().filter(|v| !v.is_null()) {
            values.insert(ValueKey(value));
        }
    }
    let mut out = Vec::with_capacity(groups.len());
    for (key_parts, values) in groups {
        let mut derived = Map::new();
        for (index, key) in keys.iter().enumerate() {
            derived.insert(key.clone(), key_parts[index].0.clone());
        }
        let arr: Vec<Value> = values.into_iter().map(|v| v.0).collect();
        derived.insert(as_name.to_string(), Value::Array(arr));
        out.push(derived);
    }
    Ok(out)
}

fn apply_equi_lookup(
    rows: Vec<Map<String, Value>>,
    secondary_bases: &BTreeMap<String, Vec<Map<String, Value>>>,
    from: &str,
    local_field: &str,
    foreign_field: &str,
    as_name: &str,
) -> Result<Vec<Map<String, Value>>, TransformError> {
    let foreign_rows = secondary_bases
        .get(from)
        .or_else(|| {
            secondary_bases
                .iter()
                .find(|(name, _)| table_names_eq(name, from))
                .map(|(_, rows)| rows)
        })
        .ok_or_else(|| {
            TransformError::Invalid(format!(
                "equiLookup.from Base Dataset `{from}` was not loaded for evaluation"
            ))
        })?;

    Ok(rows
        .into_iter()
        .map(|mut row| {
            let matches: Vec<Value> = match row.get(local_field) {
                Some(local_val) => foreign_rows
                    .iter()
                    .filter(|foreign| {
                        foreign
                            .get(foreign_field)
                            .is_some_and(|fv| json_values_eq(fv, local_val))
                    })
                    .map(|foreign| Value::Object(foreign.clone()))
                    .collect(),
                None => Vec::new(),
            };
            row.insert(as_name.to_string(), Value::Array(matches));
            row
        })
        .collect())
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
                AggregateOp::Count => count_field(&group_rows, &agg.field),
                AggregateOp::Min => min_field(&group_rows, &agg.field)?,
                AggregateOp::Max => max_field(&group_rows, &agg.field)?,
                AggregateOp::Avg => avg_field(&group_rows, &agg.field)?,
            };
            derived.insert(agg.as_name.clone(), value);
        }
        out.push(derived);
    }
    Ok(out)
}

/// Count non-null values of `field` in the group (SQL `COUNT(field)` semantics).
fn count_field(rows: &[Map<String, Value>], field: &str) -> Value {
    let count = rows
        .iter()
        .filter(|row| row.get(field).is_some_and(|v| !v.is_null()))
        .count();
    Value::Number(serde_json::Number::from(count as u64))
}

/// Precision-preserving min (ADR-0023): compare via scaled integer units.
fn min_field(rows: &[Map<String, Value>], field: &str) -> Result<Value, TransformError> {
    extreme_field(rows, field, Extreme::Min)
}

/// Precision-preserving max (ADR-0023): compare via scaled integer units.
fn max_field(rows: &[Map<String, Value>], field: &str) -> Result<Value, TransformError> {
    extreme_field(rows, field, Extreme::Max)
}

enum Extreme {
    Min,
    Max,
}

fn extreme_field(
    rows: &[Map<String, Value>],
    field: &str,
    extreme: Extreme,
) -> Result<Value, TransformError> {
    let mut best: Option<(i128, usize, Value)> = None;
    for row in rows {
        let Some(value) = row.get(field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let (units, scale) = parse_decimal_units(value).ok_or_else(|| {
            TransformError::Invalid(format!(
                "groupBy min/max field {field} is not numeric: {value}"
            ))
        })?;
        match &mut best {
            None => best = Some((units, scale, value.clone())),
            Some((best_units, best_scale, best_value)) => {
                let target_scale = (*best_scale).max(scale);
                let aligned = scale_units(units, scale, target_scale).ok_or_else(|| {
                    TransformError::Invalid("groupBy min/max scale overflow".into())
                })?;
                let best_aligned =
                    scale_units(*best_units, *best_scale, target_scale).ok_or_else(|| {
                        TransformError::Invalid("groupBy min/max scale overflow".into())
                    })?;
                let take = match extreme {
                    Extreme::Min => aligned < best_aligned,
                    Extreme::Max => aligned > best_aligned,
                };
                if take {
                    *best_units = units;
                    *best_scale = scale;
                    *best_value = value.clone();
                }
            }
        }
    }
    Ok(best.map(|(_, _, v)| v).unwrap_or(Value::Null))
}

/// Precision-preserving avg (ADR-0023): scaled sum / non-null count, never IEEE double.
fn avg_field(rows: &[Map<String, Value>], field: &str) -> Result<Value, TransformError> {
    let mut total_units: i128 = 0;
    let mut max_scale: usize = 0;
    let mut count: i128 = 0;
    for row in rows {
        let Some(value) = row.get(field) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let (units, scale) = parse_decimal_units(value).ok_or_else(|| {
            TransformError::Invalid(format!(
                "groupBy avg field {field} is not numeric: {value}"
            ))
        })?;
        count += 1;
        if scale > max_scale {
            let factor = ten_pow_i128(scale - max_scale)?;
            total_units = total_units
                .checked_mul(factor)
                .ok_or_else(|| TransformError::Invalid("groupBy avg overflow".into()))?;
            max_scale = scale;
        }
        let aligned = if scale < max_scale {
            let factor = ten_pow_i128(max_scale - scale)?;
            units
                .checked_mul(factor)
                .ok_or_else(|| TransformError::Invalid("groupBy avg overflow".into()))?
        } else {
            units
        };
        total_units = total_units
            .checked_add(aligned)
            .ok_or_else(|| TransformError::Invalid("groupBy avg overflow".into()))?;
    }
    if count == 0 {
        return Ok(Value::Null);
    }
    // Divide in fixed-point with enough extra scale for a stable decimal quotient.
    // Result scale = max_scale + AVG_EXTRA_SCALE, then trim trailing zeros down to
    // the input scale so money-like values keep two places when exact.
    const AVG_EXTRA_SCALE: usize = 4;
    let dividend_scale = max_scale + AVG_EXTRA_SCALE;
    let factor = ten_pow_i128(AVG_EXTRA_SCALE)?;
    let dividend = total_units
        .checked_mul(factor)
        .ok_or_else(|| TransformError::Invalid("groupBy avg overflow".into()))?;
    let quotient = dividend / count;
    let remainder = dividend % count;
    // Round half away from zero on the final digit.
    let mut rounded = quotient;
    if remainder.abs() * 2 >= count {
        rounded += if dividend >= 0 { 1 } else { -1 };
    }
    Ok(trim_decimal_units(rounded, dividend_scale, max_scale))
}

/// Format scaled units, trimming trailing fractional zeros down to `min_scale`
/// so money-like inputs (scale 2) keep `"20.00"` while still dropping pure
/// padding from the avg extra scale.
fn trim_decimal_units(units: i128, scale: usize, min_scale: usize) -> Value {
    let mut u = units;
    let mut s = scale;
    while s > min_scale && u % 10 == 0 {
        u /= 10;
        s -= 1;
    }
    format_decimal_units(u, s)
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
    fn group_by_accepts_count_min_max_avg() {
        // Issue #126: parser must accept the remaining v1 groupBy aggregate ops.
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [
                    {"op": "count", "field": "ORDER_ID", "as": "ORDER_COUNT"},
                    {"op": "min", "field": "AMOUNT", "as": "MIN_AMOUNT"},
                    {"op": "max", "field": "AMOUNT", "as": "MAX_AMOUNT"},
                    {"op": "avg", "field": "AMOUNT", "as": "AVG_AMOUNT"}
                ]
            }
        })])
        .unwrap();
        match &ops[0] {
            TransformOp::GroupBy { aggregates, .. } => {
                assert_eq!(aggregates.len(), 4);
                assert_eq!(aggregates[0].op, AggregateOp::Count);
                assert_eq!(aggregates[1].op, AggregateOp::Min);
                assert_eq!(aggregates[2].op, AggregateOp::Max);
                assert_eq!(aggregates[3].op, AggregateOp::Avg);
            }
            other => panic!("expected GroupBy, got {other:?}"),
        }
    }

    #[test]
    fn group_by_count_min_max_avg_totals() {
        // Known-good literals: customer 1 has amounts 10.00 + 30.00 → count 2, min 10, max 30, avg 20.
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [
                    {"op": "count", "field": "ORDER_ID", "as": "ORDER_COUNT"},
                    {"op": "min", "field": "AMOUNT", "as": "MIN_AMOUNT"},
                    {"op": "max", "field": "AMOUNT", "as": "MAX_AMOUNT"},
                    {"op": "avg", "field": "AMOUNT", "as": "AVG_AMOUNT"},
                    {"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}
                ]
            }
        })])
        .unwrap();
        let rows = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
                ("ADDRESS", json!("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("30.00")),
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
        let c1 = out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(1)))
            .expect("customer 1");
        assert_eq!(c1.get("ORDER_COUNT"), Some(&json!(2)));
        assert_eq!(c1.get("MIN_AMOUNT"), Some(&json!("10.00")));
        assert_eq!(c1.get("MAX_AMOUNT"), Some(&json!("30.00")));
        assert_eq!(c1.get("AVG_AMOUNT"), Some(&json!("20.00")));
        assert_eq!(c1.get("TOTAL_AMOUNT"), Some(&json!("40.00")));
        let c2 = out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(2)))
            .expect("customer 2");
        assert_eq!(c2.get("ORDER_COUNT"), Some(&json!(1)));
        assert_eq!(c2.get("MIN_AMOUNT"), Some(&json!("5.00")));
        assert_eq!(c2.get("MAX_AMOUNT"), Some(&json!("5.00")));
        assert_eq!(c2.get("AVG_AMOUNT"), Some(&json!("5.00")));
    }

    #[test]
    fn group_by_count_skips_null_field_values() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "count", "field": "AMOUNT", "as": "AMOUNT_COUNT"}]
            }
        })])
        .unwrap();
        let rows = vec![
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", json!("10.00"))]),
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", Value::Null)]),
            row(&[("CUSTOMER_ID", json!(1))]), // missing field
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out[0].get("AMOUNT_COUNT"), Some(&json!(1)));
    }

    #[test]
    fn group_by_avg_preserves_decimal_precision_without_ieee_double() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "avg", "field": "AMOUNT", "as": "AVG_AMOUNT"}]
            }
        })])
        .unwrap();
        // 0.10 + 0.20 → avg 0.15 exactly (not IEEE 0.15000000000000002).
        let rows = vec![
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", json!("0.10"))]),
            row(&[("CUSTOMER_ID", json!(1)), ("AMOUNT", json!("0.20"))]),
        ];
        let out = evaluate_transform(&ops, &rows).unwrap();
        assert_eq!(out[0].get("AVG_AMOUNT"), Some(&json!("0.15")));
    }

    fn rich_groupby_ops() -> Vec<TransformOp> {
        parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [
                    {"op": "count", "field": "ORDER_ID", "as": "ORDER_COUNT"},
                    {"op": "min", "field": "AMOUNT", "as": "MIN_AMOUNT"},
                    {"op": "max", "field": "AMOUNT", "as": "MAX_AMOUNT"},
                    {"op": "avg", "field": "AMOUNT", "as": "AVG_AMOUNT"},
                    {"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}
                ]
            }
        })])
        .unwrap()
    }

    #[test]
    fn rich_groupby_affect_skips_unused_address_update() {
        // Issue #126: unused-field changes must not trigger recompute for count/min/max/avg.
        let ops = rich_groupby_ops();
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
            ("ADDRESS", json!("1 Main St")),
        ]);
        let after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        assert_eq!(
            analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after)).unwrap(),
            AffectOutcome::SkipUnusedFields
        );
    }

    #[test]
    fn rich_groupby_affect_recomputes_on_amount_or_order_id_update() {
        let ops = rich_groupby_ops();
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        let after_amount = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("40.00")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        match analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_amount)).unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("CUSTOMER_ID"), Some(&json!(1)));
            }
            other => panic!("expected Recompute on AMOUNT, got {other:?}"),
        }

        let after_order_id = row(&[
            ("ORDER_ID", json!(199)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        assert!(matches!(
            analyze_affect(&ops, BaseChangeKind::Update, Some(&pre), Some(&after_order_id)).unwrap(),
            AffectOutcome::Recompute { .. }
        ));
    }

    #[test]
    fn rich_groupby_incremental_identities_stay_correct_for_insert_update_delete() {
        // No Maintenance State: per-identity recompute from Base must match full eval.
        let ops = rich_groupby_ops();
        let mut base = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("30.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
            ]),
        ];

        // Insert order 102 for customer 1: count 3, min 10, max 50, avg 30, sum 90.
        base.push(row(&[
            ("ORDER_ID", json!(102)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
        ]));
        let after_insert = evaluate_transform_for_identities(
            &ops,
            &base,
            &[row(&[("CUSTOMER_ID", json!(1))])],
        )
        .unwrap();
        assert_eq!(after_insert.len(), 1);
        assert_eq!(after_insert[0].get("ORDER_COUNT"), Some(&json!(3)));
        assert_eq!(after_insert[0].get("MIN_AMOUNT"), Some(&json!("10.00")));
        assert_eq!(after_insert[0].get("MAX_AMOUNT"), Some(&json!("50.00")));
        assert_eq!(after_insert[0].get("AVG_AMOUNT"), Some(&json!("30.00")));
        assert_eq!(after_insert[0].get("TOTAL_AMOUNT"), Some(&json!("90.00")));

        // Update order 100 amount 10→20: min 20, max 50, avg 100/3 kept exact at extra scale.
        base[0] = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("20.00")),
        ]);
        let after_update = evaluate_transform_for_identities(
            &ops,
            &base,
            &[row(&[("CUSTOMER_ID", json!(1))])],
        )
        .unwrap();
        assert_eq!(after_update[0].get("ORDER_COUNT"), Some(&json!(3)));
        assert_eq!(after_update[0].get("MIN_AMOUNT"), Some(&json!("20.00")));
        assert_eq!(after_update[0].get("MAX_AMOUNT"), Some(&json!("50.00")));
        assert_eq!(after_update[0].get("TOTAL_AMOUNT"), Some(&json!("100.00")));
        // 100 / 3 = 33.3̅ → precision-preserving fixed-point (not IEEE double).
        assert_eq!(after_update[0].get("AVG_AMOUNT"), Some(&json!("33.333333")));

        // Delete last remaining row of customer 2 → identity omitted (caller deletes).
        base.retain(|r| r.get("CUSTOMER_ID") != Some(&json!(2)));
        let after_delete = evaluate_transform_for_identities(
            &ops,
            &base,
            &[row(&[("CUSTOMER_ID", json!(2))])],
        )
        .unwrap();
        assert!(
            after_delete.is_empty(),
            "empty group must be omitted so Delivery can delete the Output Identity"
        );
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

    #[test]
    fn equi_lookup_parse_evaluate_and_affect_both_bases() {
        let ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME"]}}),
            json!({
                "equiLookup": {
                    "from": "ORDERS",
                    "localField": "ID",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            }),
        ])
        .unwrap();
        match &ops[1] {
            TransformOp::EquiLookup {
                from,
                local_field,
                foreign_field,
                as_name,
                from_schema,
            } => {
                assert_eq!(from, "ORDERS");
                assert_eq!(local_field, "ID");
                assert_eq!(foreign_field, "CUSTOMER_ID");
                assert_eq!(as_name, "orders");
                assert!(from_schema.is_none());
            }
            other => panic!("expected EquiLookup, got {other:?}"),
        }
        assert_eq!(
            secondary_base_refs(&ops),
            vec![SecondaryBaseRef {
                table: "ORDERS".into(),
                schema: None,
            }]
        );

        let customers = vec![
            row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]),
            row(&[("ID", json!(2)), ("NAME", json!("Bob")), ("EMAIL", json!("b@x"))]),
        ];
        let orders = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
            ]),
        ];
        let mut secondary = BTreeMap::new();
        secondary.insert("ORDERS".to_string(), orders.clone());

        let out = evaluate_transform_with_bases(&ops, &customers, &secondary).unwrap();
        assert_eq!(out.len(), 2);
        let alice = out
            .iter()
            .find(|r| r.get("ID") == Some(&json!(1)))
            .expect("Alice");
        assert_eq!(alice.get("NAME"), Some(&json!("Alice")));
        let alice_orders = alice
            .get("orders")
            .and_then(|v| v.as_array())
            .expect("orders array");
        assert_eq!(alice_orders.len(), 2);
        assert!(!alice.contains_key("EMAIL"));

        let bob = out
            .iter()
            .find(|r| r.get("ID") == Some(&json!(2)))
            .expect("Bob");
        assert_eq!(
            bob.get("orders").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(1)
        );

        let managed = derived_output_field_names(&ops, &["ID".into(), "NAME".into(), "EMAIL".into()]);
        assert!(managed.contains(&"orders".to_string()));
        assert!(managed.contains(&"ID".to_string()));
        assert!(used_base_fields(&ops).contains("ID"));

        // Primary unused EMAIL (projected away) skips.
        let pre = row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]);
        let after_email = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alice")),
            ("EMAIL", json!("new@x")),
        ]);
        assert_eq!(
            analyze_affect_on_base(
                &ops,
                "CUSTOMERS",
                "CUSTOMERS",
                BaseChangeKind::Update,
                Some(&pre),
                Some(&after_email),
                &customers,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // Primary NAME change recomputes customer 1.
        let after_name = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alicia")),
            ("EMAIL", json!("a@x")),
        ]);
        match analyze_affect_on_base(
            &ops,
            "CUSTOMERS",
            "CUSTOMERS",
            BaseChangeKind::Update,
            Some(&pre),
            Some(&after_name),
            &customers,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("ID"), Some(&json!(1)));
            }
            other => panic!("expected Recompute, got {other:?}"),
        }

        // Foreign ORDERS amount update recomputes matching customer identity.
        let order_pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let order_after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
        ]);
        match analyze_affect_on_base(
            &ops,
            "ORDERS",
            "CUSTOMERS",
            BaseChangeKind::Update,
            Some(&order_pre),
            Some(&order_after),
            &customers,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("ID"), Some(&json!(1)));
            }
            other => panic!("expected foreign Recompute, got {other:?}"),
        }

        let mut orders_after = orders;
        orders_after[0] = order_after;
        secondary.insert("ORDERS".to_string(), orders_after);
        let recomputed = evaluate_transform_for_identities_with_bases(
            &ops,
            &customers,
            &secondary,
            &[row(&[("ID", json!(1))])],
        )
        .unwrap();
        assert_eq!(recomputed.len(), 1);
        let amounts: Vec<_> = recomputed[0]
            .get("orders")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|o| o.get("AMOUNT"))
            .collect();
        assert!(amounts.contains(&&json!("50.00")));
        assert!(amounts.contains(&&json!("10.00")));
    }

    #[test]
    fn dollar_lookup_and_equi_lookup_pipeline_fail_clearly() {
        let err = parse_transform_steps(&[json!({
            "$lookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }
        })])
        .unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("$lookup") && msg.contains("equilookup"),
            "expected clear $lookup → equiLookup guidance, got: {err}"
        );

        let err = parse_transform_steps(&[json!({
            "equiLookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders",
                "pipeline": [{"$match": {}}]
            }
        })])
        .unwrap_err();
        assert!(
            matches!(err, TransformError::Invalid(_)),
            "pipeline-style equiLookup must be Invalid, got {err:?}"
        );
        assert!(err.to_string().to_ascii_lowercase().contains("pipeline"));
    }

    #[test]
    fn equi_lookup_foreign_affect_uses_shaped_local_field() {
        // rename ID → customerId, then equiLookup on customerId — foreign Affect must
        // still resolve primary Output Identities.
        let ops = parse_transform_steps(&[
            json!({"rename": {"fields": [{"from": "ID", "to": "customerId"}]}}),
            json!({
                "equiLookup": {
                    "from": "ORDERS",
                    "localField": "customerId",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            }),
            json!({"rename": {"fields": [{"from": "orders", "to": "orderList"}]}}),
        ])
        .unwrap();
        let customers = vec![row(&[("ID", json!(1)), ("NAME", json!("Alice"))])];
        let order_pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let order_after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
        ]);
        match analyze_affect_on_base(
            &ops,
            "ORDERS",
            "CUSTOMERS",
            BaseChangeKind::Update,
            Some(&order_pre),
            Some(&order_after),
            &customers,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("customerId"), Some(&json!(1)));
            }
            other => panic!("expected shaped-localField Recompute, got {other:?}"),
        }
    }

    #[test]
    fn equi_lookup_removed_as_skips_foreign_affect() {
        let ops = parse_transform_steps(&[
            json!({
                "equiLookup": {
                    "from": "ORDERS",
                    "localField": "ID",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            }),
            json!({"remove": {"fields": ["orders"]}}),
        ])
        .unwrap();
        let customers = vec![row(&[("ID", json!(1)), ("NAME", json!("Alice"))])];
        let order_pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let order_after = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
        ]);
        assert_eq!(
            analyze_affect_on_base(
                &ops,
                "ORDERS",
                "CUSTOMERS",
                BaseChangeKind::Update,
                Some(&order_pre),
                Some(&order_after),
                &customers,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );
    }

    #[test]
    fn distinct_and_add_to_set_parse_and_evaluate() {
        // Issue #128: declarative distinct / addToSet.
        let distinct_ops = parse_transform_steps(&[json!({
            "distinct": { "fields": ["CUSTOMER_ID"] }
        })])
        .unwrap();
        match &distinct_ops[0] {
            TransformOp::Distinct { fields } => assert_eq!(fields, &["CUSTOMER_ID".to_string()]),
            other => panic!("expected Distinct, got {other:?}"),
        }

        let add_ops = parse_transform_steps(&[json!({
            "addToSet": {
                "keys": ["CUSTOMER_ID"],
                "field": "AMOUNT",
                "as": "AMOUNTS"
            }
        })])
        .unwrap();
        match &add_ops[0] {
            TransformOp::AddToSet {
                keys,
                field,
                as_name,
            } => {
                assert_eq!(keys, &["CUSTOMER_ID".to_string()]);
                assert_eq!(field, "AMOUNT");
                assert_eq!(as_name, "AMOUNTS");
            }
            other => panic!("expected AddToSet, got {other:?}"),
        }

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
                ("ORDER_ID", json!(102)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
                ("ADDRESS", json!("1 Main St")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
                ("ADDRESS", json!("2 Side Rd")),
            ]),
        ];

        let distinct_out = evaluate_transform(&distinct_ops, &rows).unwrap();
        assert_eq!(distinct_out.len(), 2);
        assert!(distinct_out
            .iter()
            .any(|r| r.get("CUSTOMER_ID") == Some(&json!(1))));
        assert!(distinct_out
            .iter()
            .any(|r| r.get("CUSTOMER_ID") == Some(&json!(2))));

        let add_out = evaluate_transform(&add_ops, &rows).unwrap();
        let c1 = add_out
            .iter()
            .find(|r| r.get("CUSTOMER_ID") == Some(&json!(1)))
            .expect("customer 1");
        let amounts = c1.get("AMOUNTS").and_then(|v| v.as_array()).expect("array");
        assert_eq!(amounts.len(), 2);
        assert!(amounts.iter().any(|v| v == &json!("10.00")));
        assert!(amounts.iter().any(|v| v == &json!("42.50")));
    }

    #[test]
    fn group_by_sum_does_not_require_maintenance_state() {
        let ops = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        assert!(!requires_maintenance_state(&ops));
        assert!(build_maintenance_state(&ops, &[]).unwrap().entries.is_empty());
    }

    #[test]
    fn distinct_value_level_affect_skips_duplicate_keys_via_maintenance_state() {
        let ops = parse_transform_steps(&[json!({
            "distinct": { "fields": ["CUSTOMER_ID"] }
        })])
        .unwrap();
        assert!(requires_maintenance_state(&ops));

        let base = vec![
            row(&[("ORDER_ID", json!(100)), ("CUSTOMER_ID", json!(1))]),
            row(&[("ORDER_ID", json!(101)), ("CUSTOMER_ID", json!(1))]),
            row(&[("ORDER_ID", json!(200)), ("CUSTOMER_ID", json!(2))]),
        ];
        let mut state = build_maintenance_state(&ops, &base).unwrap();
        assert_eq!(state_refcount(&state, &row(&[("CUSTOMER_ID", json!(1))]), None), 2);
        assert_eq!(state_refcount(&state, &row(&[("CUSTOMER_ID", json!(2))]), None), 1);

        // Unused ADDRESS-style field is not in distinct.fields — skip unused.
        let pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("ADDRESS", json!("1 Main St")),
        ]);
        let after_addr = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("ADDRESS", json!("1 Main Ave")),
        ]);
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Update,
                Some(&pre),
                Some(&after_addr),
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // Duplicate CUSTOMER_ID insert: value-level skip (already counted).
        let dup = row(&[("ORDER_ID", json!(103)), ("CUSTOMER_ID", json!(1))]);
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Insert,
                None,
                Some(&dup),
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipValueUnchanged
        );
        maintain_state_for_change(&ops, &mut state, BaseChangeKind::Insert, None, Some(&dup))
            .unwrap();
        assert_eq!(state_refcount(&state, &row(&[("CUSTOMER_ID", json!(1))]), None), 3);

        // New CUSTOMER_ID insert: recompute.
        let neu = row(&[("ORDER_ID", json!(300)), ("CUSTOMER_ID", json!(3))]);
        match analyze_affect_with_maintenance(
            &ops,
            BaseChangeKind::Insert,
            None,
            Some(&neu),
            &state,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("CUSTOMER_ID"), Some(&json!(3)));
            }
            other => panic!("expected Recompute for new distinct key, got {other:?}"),
        }
        maintain_state_for_change(&ops, &mut state, BaseChangeKind::Insert, None, Some(&neu))
            .unwrap();

        // Delete non-last contributor: skip.
        let del_dup = row(&[("ORDER_ID", json!(103)), ("CUSTOMER_ID", json!(1))]);
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Delete,
                Some(&del_dup),
                None,
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipValueUnchanged
        );
        maintain_state_for_change(&ops, &mut state, BaseChangeKind::Delete, Some(&del_dup), None)
            .unwrap();

        // Delete last contributor for customer 2: recompute (caller deletes identity).
        let del_last = row(&[("ORDER_ID", json!(200)), ("CUSTOMER_ID", json!(2))]);
        match analyze_affect_with_maintenance(
            &ops,
            BaseChangeKind::Delete,
            Some(&del_last),
            None,
            &state,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities[0].get("CUSTOMER_ID"), Some(&json!(2)));
            }
            other => panic!("expected Recompute for last distinct delete, got {other:?}"),
        }
    }

    #[test]
    fn add_to_set_value_level_affect_skips_duplicate_members() {
        let ops = parse_transform_steps(&[json!({
            "addToSet": {
                "keys": ["CUSTOMER_ID"],
                "field": "AMOUNT",
                "as": "AMOUNTS"
            }
        })])
        .unwrap();
        let base = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(102)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
            ]),
        ];
        let mut state = build_maintenance_state(&ops, &base).unwrap();
        assert_eq!(
            state_refcount(
                &state,
                &row(&[("CUSTOMER_ID", json!(1))]),
                Some(&json!("42.50"))
            ),
            2
        );

        // Insert duplicate AMOUNT already in set → skip Derived.
        let dup = row(&[
            ("ORDER_ID", json!(103)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
        ]);
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Insert,
                None,
                Some(&dup),
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipValueUnchanged
        );
        maintain_state_for_change(&ops, &mut state, BaseChangeKind::Insert, None, Some(&dup))
            .unwrap();

        // Insert new AMOUNT → recompute.
        let neu = row(&[
            ("ORDER_ID", json!(104)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("7.00")),
        ]);
        assert!(matches!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Insert,
                None,
                Some(&neu),
                &state,
            )
            .unwrap(),
            AffectOutcome::Recompute { .. }
        ));

        // AMOUNT change that keeps the set unchanged (42.50→10.00 while both remain):
        // pre count(42.50)=2 → still 1 after; after count(10.00)=1 → still present.
        // Wait: after maintain of neu we'd have 7.00; rebuild for clarity.
        let mut state = build_maintenance_state(&ops, &base).unwrap();
        let pre = row(&[
            ("ORDER_ID", json!(102)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let after = row(&[
            ("ORDER_ID", json!(102)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
        ]);
        // 42.50 refcount 2 → 1 (still in set); 10.00 refcount 1 → 2 (already in set) → skip.
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Update,
                Some(&pre),
                Some(&after),
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipValueUnchanged
        );
        maintain_state_for_change(
            &ops,
            &mut state,
            BaseChangeKind::Update,
            Some(&pre),
            Some(&after),
        )
        .unwrap();

        // Change last 42.50 → 50.00: remove 42.50 from set, add 50.00 → recompute.
        let pre2 = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let after2 = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("50.00")),
        ]);
        assert!(matches!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Update,
                Some(&pre2),
                Some(&after2),
                &state,
            )
            .unwrap(),
            AffectOutcome::Recompute { .. }
        ));
    }

    #[test]
    fn add_to_set_affect_shapes_through_rename_prefix() {
        // Prefix rename must align Affect Analysis keys/values with Maintenance State.
        let ops = parse_transform_steps(&[
            json!({
                "rename": {
                    "fields": [
                        {"from": "CUSTOMER_ID", "to": "CUST"},
                        {"from": "AMOUNT", "to": "AMT"}
                    ]
                }
            }),
            json!({
                "addToSet": {
                    "keys": ["CUST"],
                    "field": "AMT",
                    "as": "AMTS"
                }
            }),
        ])
        .unwrap();
        let base = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
        ];
        let state = build_maintenance_state(&ops, &base).unwrap();
        assert_eq!(
            state_refcount(&state, &row(&[("CUST", json!(1))]), Some(&json!("42.50"))),
            1
        );

        // Duplicate AMOUNT after rename shaping → value-level skip.
        let dup = row(&[
            ("ORDER_ID", json!(102)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        assert_eq!(
            analyze_affect_with_maintenance(
                &ops,
                BaseChangeKind::Insert,
                None,
                Some(&dup),
                &state,
            )
            .unwrap(),
            AffectOutcome::SkipValueUnchanged
        );

        // New AMOUNT → recompute shaped identity CUST=1.
        let neu = row(&[
            ("ORDER_ID", json!(103)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("7.00")),
        ]);
        match analyze_affect_with_maintenance(
            &ops,
            BaseChangeKind::Insert,
            None,
            Some(&neu),
            &state,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities[0].get("CUST"), Some(&json!(1)));
            }
            other => panic!("expected Recompute after rename+addToSet, got {other:?}"),
        }
    }

    #[test]
    fn unwind_parse_evaluate_flatten_and_affect() {
        let ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME"]}}),
            json!({
                "equiLookup": {
                    "from": "ORDERS",
                    "localField": "ID",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            }),
            json!({"unwind": {"path": "orders"}}),
        ])
        .unwrap();
        match &ops[2] {
            TransformOp::Unwind { path } => assert_eq!(path, "orders"),
            other => panic!("expected Unwind, got {other:?}"),
        }

        let customers = vec![
            row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]),
            row(&[("ID", json!(2)), ("NAME", json!("Bob")), ("EMAIL", json!("b@x"))]),
        ];
        let orders = vec![
            row(&[
                ("ORDER_ID", json!(100)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("42.50")),
            ]),
            row(&[
                ("ORDER_ID", json!(101)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
            ]),
            row(&[
                ("ORDER_ID", json!(200)),
                ("CUSTOMER_ID", json!(2)),
                ("AMOUNT", json!("5.00")),
            ]),
        ];
        let mut secondary = BTreeMap::new();
        secondary.insert("ORDERS".to_string(), orders.clone());

        let out = evaluate_transform_with_bases(&ops, &customers, &secondary).unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| !r.contains_key("orders")));
        let alice_100 = out
            .iter()
            .find(|r| r.get("ORDER_ID") == Some(&json!(100)))
            .expect("order 100");
        assert_eq!(alice_100.get("NAME"), Some(&json!("Alice")));
        assert_eq!(alice_100.get("AMOUNT"), Some(&json!("42.50")));
        assert_eq!(alice_100.get("ID"), Some(&json!(1)));

        let managed = derived_output_field_names(
            &ops,
            &["ID".into(), "NAME".into(), "EMAIL".into()],
        );
        assert!(!managed.contains(&"orders".to_string()));
        assert!(managed.contains(&"ID".to_string()));

        // Primary unused EMAIL skips.
        let pre = row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]);
        let after_email = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alice")),
            ("EMAIL", json!("new@x")),
        ]);
        assert_eq!(
            analyze_affect_on_base_with_bases(
                &ops,
                "CUSTOMERS",
                "CUSTOMERS",
                BaseChangeKind::Update,
                Some(&pre),
                Some(&after_email),
                &customers,
                &secondary,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // Primary NAME change expands to both unwound order identities.
        let after_name = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alicia")),
            ("EMAIL", json!("a@x")),
        ]);
        match analyze_affect_on_base_with_bases(
            &ops,
            "CUSTOMERS",
            "CUSTOMERS",
            BaseChangeKind::Update,
            Some(&pre),
            Some(&after_name),
            &customers,
            &secondary,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                // pre + after images (NAME Alice→Alicia) both expand; ORDER_IDs overlap.
                assert!(identities.len() >= 2);
                assert!(identities.iter().any(|id| id.get("ORDER_ID") == Some(&json!(100))));
                assert!(identities.iter().any(|id| id.get("ORDER_ID") == Some(&json!(101))));
                assert!(identities.iter().any(|id| id.get("NAME") == Some(&json!("Alicia"))));
            }
            other => panic!("expected Recompute for NAME, got {other:?}"),
        }

        // Foreign order delete must include disappeared ORDER_ID for Delivery delete.
        let order_pre = row(&[
            ("ORDER_ID", json!(100)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("42.50")),
        ]);
        let mut orders_after_delete = orders.clone();
        orders_after_delete.retain(|r| r.get("ORDER_ID") != Some(&json!(100)));
        secondary.insert("ORDERS".to_string(), orders_after_delete);
        match analyze_affect_on_base_with_bases(
            &ops,
            "ORDERS",
            "CUSTOMERS",
            BaseChangeKind::Delete,
            Some(&order_pre),
            None,
            &customers,
            &secondary,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert!(
                    identities.iter().any(|id| id.get("ORDER_ID") == Some(&json!(100))),
                    "deleted order identity must be present, got {identities:?}"
                );
                assert!(identities.iter().any(|id| id.get("ORDER_ID") == Some(&json!(101))));
            }
            other => panic!("expected foreign delete Recompute, got {other:?}"),
        }

        // Scalar unwind (literal array via addFields).
        let scalar_ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID"]}}),
            json!({
                "addFields": {
                    "fields": [{"as": "tags", "value": ["a", "b"]}]
                }
            }),
            json!({"unwind": {"path": "$tags"}}),
        ])
        .unwrap();
        let scalar_out = evaluate_transform(&scalar_ops, &[row(&[("ID", json!(1))])]).unwrap();
        assert_eq!(scalar_out.len(), 2);
        assert_eq!(scalar_out[0].get("tags"), Some(&json!("a")));
        assert_eq!(scalar_out[1].get("tags"), Some(&json!("b")));
    }

    #[test]
    fn dollar_unwind_and_unsupported_forms_fail_clearly() {
        let err = parse_transform_steps(&[json!({
            "$unwind": "$orders"
        })])
        .unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("$unwind") && msg.contains("unwind"),
            "expected clear $unwind → unwind guidance, got: {err}"
        );

        let err = parse_transform_steps(&[json!({
            "unwind": {
                "path": "orders",
                "preserveNullAndEmptyArrays": true
            }
        })])
        .unwrap_err();
        assert!(
            matches!(err, TransformError::Invalid(_)),
            "preserveNullAndEmptyArrays must be Invalid, got {err:?}"
        );
        assert!(err
            .to_string()
            .to_ascii_lowercase()
            .contains("preservenullandemptyarrays"));

        let err = parse_transform_steps(&[
            json!({"unwind": {"path": "orders"}}),
            json!({"distinct": {"fields": ["ORDER_ID"]}}),
        ])
        .unwrap_err();
        assert!(
            err.to_string().to_ascii_lowercase().contains("distinct")
                || err.to_string().to_ascii_lowercase().contains("addtoset"),
            "unwind+distinct must fail clearly, got: {err}"
        );
    }

    #[test]
    fn union_parse_evaluate_and_affect_both_bases() {
        // Issue #130: declarative multi-Base union.
        let ops = parse_transform_steps(&[
            json!({
                "union": {
                    "from": "WEST_CUSTOMERS"
                }
            }),
            json!({"project": {"fields": ["ID", "NAME"]}}),
        ])
        .unwrap();
        match &ops[0] {
            TransformOp::Union { from, from_schema } => {
                assert_eq!(from, "WEST_CUSTOMERS");
                assert!(from_schema.is_none());
            }
            other => panic!("expected Union, got {other:?}"),
        }
        assert_eq!(
            secondary_base_refs(&ops),
            vec![SecondaryBaseRef {
                table: "WEST_CUSTOMERS".into(),
                schema: None,
            }]
        );

        let east = vec![
            row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]),
            row(&[("ID", json!(2)), ("NAME", json!("Bob")), ("EMAIL", json!("b@x"))]),
        ];
        let west = vec![row(&[
            ("ID", json!(10)),
            ("NAME", json!("Zoe")),
            ("EMAIL", json!("z@x")),
        ])];
        let mut secondary = BTreeMap::new();
        secondary.insert("WEST_CUSTOMERS".to_string(), west.clone());

        let out = evaluate_transform_with_bases(&ops, &east, &secondary).unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|r| r.contains_key("ID") && r.contains_key("NAME")));
        assert!(out.iter().all(|r| !r.contains_key("EMAIL")));
        assert!(out.iter().any(|r| r.get("NAME") == Some(&json!("Alice"))));
        assert!(out.iter().any(|r| r.get("NAME") == Some(&json!("Zoe"))));

        // Primary unused EMAIL (projected away after union) skips.
        let pre = row(&[("ID", json!(1)), ("NAME", json!("Alice")), ("EMAIL", json!("a@x"))]);
        let after_email = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alice")),
            ("EMAIL", json!("new@x")),
        ]);
        assert_eq!(
            analyze_affect_on_base(
                &ops,
                "EAST_CUSTOMERS",
                "EAST_CUSTOMERS",
                BaseChangeKind::Update,
                Some(&pre),
                Some(&after_email),
                &east,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // Primary NAME change recomputes identity 1.
        let after_name = row(&[
            ("ID", json!(1)),
            ("NAME", json!("Alicia")),
            ("EMAIL", json!("a@x")),
        ]);
        match analyze_affect_on_base(
            &ops,
            "EAST_CUSTOMERS",
            "EAST_CUSTOMERS",
            BaseChangeKind::Update,
            Some(&pre),
            Some(&after_name),
            &east,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("ID"), Some(&json!(1)));
            }
            other => panic!("expected primary Recompute, got {other:?}"),
        }

        // Secondary WEST unused EMAIL skips (project after union).
        let west_pre = row(&[("ID", json!(10)), ("NAME", json!("Zoe")), ("EMAIL", json!("z@x"))]);
        let west_email = row(&[
            ("ID", json!(10)),
            ("NAME", json!("Zoe")),
            ("EMAIL", json!("zoe@x")),
        ]);
        assert_eq!(
            analyze_affect_on_base(
                &ops,
                "WEST_CUSTOMERS",
                "EAST_CUSTOMERS",
                BaseChangeKind::Update,
                Some(&west_pre),
                Some(&west_email),
                &east,
            )
            .unwrap(),
            AffectOutcome::SkipUnusedFields
        );

        // Secondary WEST NAME change recomputes identity 10.
        let west_name = row(&[
            ("ID", json!(10)),
            ("NAME", json!("Zora")),
            ("EMAIL", json!("z@x")),
        ]);
        match analyze_affect_on_base(
            &ops,
            "WEST_CUSTOMERS",
            "EAST_CUSTOMERS",
            BaseChangeKind::Update,
            Some(&west_pre),
            Some(&west_name),
            &east,
        )
        .unwrap()
        {
            AffectOutcome::Recompute { identities } => {
                assert_eq!(identities.len(), 1);
                assert_eq!(identities[0].get("ID"), Some(&json!(10)));
            }
            other => panic!("expected secondary Recompute, got {other:?}"),
        }

        secondary.insert("WEST_CUSTOMERS".to_string(), vec![west_name]);
        let recomputed = evaluate_transform_for_identities_with_bases(
            &ops,
            &east,
            &secondary,
            &[row(&[("ID", json!(10)), ("NAME", json!("Zora"))])],
        )
        .unwrap();
        assert_eq!(recomputed.len(), 1);
        assert_eq!(recomputed[0].get("NAME"), Some(&json!("Zora")));
    }

    #[test]
    fn dollar_union_with_and_unsupported_forms_fail_clearly() {
        let err = parse_transform_steps(&[json!({
            "$unionWith": {
                "coll": "WEST_CUSTOMERS"
            }
        })])
        .unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("$unionwith") && msg.contains("union"),
            "expected clear $unionWith → union guidance, got: {err}"
        );

        let err = parse_transform_steps(&[json!({
            "union": {
                "from": "WEST_CUSTOMERS",
                "pipeline": [{"$match": {}}]
            }
        })])
        .unwrap_err();
        assert!(
            matches!(err, TransformError::Invalid(_)),
            "pipeline-style union must be Invalid, got {err:?}"
        );
        assert!(err.to_string().to_ascii_lowercase().contains("pipeline"));

        let err = parse_transform_steps(&[
            json!({"union": {"from": "WEST_CUSTOMERS"}}),
            json!({"distinct": {"fields": ["ID"]}}),
        ])
        .unwrap_err();
        assert!(
            err.to_string().to_ascii_lowercase().contains("distinct")
                || err.to_string().to_ascii_lowercase().contains("addtoset"),
            "union+distinct must fail clearly, got: {err}"
        );
    }
}
