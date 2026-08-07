//! Aggregation / SQL-like Rich Transform authoring (preferred DX).
//!
//! Accepts MongoDB Aggregation–shaped stages and thin SQL-ish aliases as the
//! preferred declarative authoring form beside classic steps (Upgrade
//! Compatibility). Every accepted shape normalizes to the same [`TransformOp`]
//! IR so Affect Analysis and evaluation stay unchanged.
//!
//! Free-form scripts and unanalyzable Aggregation extensions (`pipeline`, `let`,
//! multi-predicate `$match`, expression `$project`, …) remain rejected.

use serde_json::{Map, Value};

use crate::{
    parse_add_fields, parse_add_to_set, parse_distinct, parse_equi_lookup, parse_filter,
    parse_group_by, parse_project, parse_remove, parse_rename, parse_union, parse_unwind,
    AddFieldSource, AddFieldSpec, AggregateOp, AggregateSpec, RenameSpec, TransformError,
    TransformOp,
};

fn is_classic_fields_body(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|o| o.contains_key("fields") && o.keys().all(|k| k == "fields"))
}

/// Try parsing one step as Aggregation-like / SQL-alias DX.
///
/// Returns `None` when the step is not an Aggregation/SQL-alias shape (caller
/// continues with the current declarative form).
pub(crate) fn try_parse_aggregation_step(
    obj: &Map<String, Value>,
) -> Result<Option<TransformOp>, TransformError> {
    if obj.len() != 1 {
        // Mixed keys are invalid for every step form; let the caller report.
        return Ok(None);
    }
    let (name, value) = obj.iter().next().expect("len == 1");
    let op = match name.as_str() {
        "$project" | "select" => Some(parse_project_dx(value)?),
        "$match" | "where" => Some(parse_match_dx(value)?),
        "$addFields" | "$set" => Some(parse_add_fields_dx(value)?),
        "$unset" => Some(parse_unset_dx(value)?),
        "$rename" => Some(parse_rename_dx(value)?),
        "$lookup" | "join" => Some(parse_lookup_dx(value)?),
        "$unwind" => Some(parse_unwind_dx(value)?),
        "$unionWith" => Some(parse_union_with_dx(value)?),
        "$group" => Some(parse_group_dx(value)?),
        _ => None,
    };
    Ok(op)
}

fn parse_project_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Classic body: { fields: [...] } (also allowed under $project / select).
    if is_classic_fields_body(value) {
        return parse_project(value);
    }
    // Aggregation inclusion map: { ID: 1, NAME: true } — inclusion only.
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$project/select must be an object with fields or an inclusion map".to_string(),
        )
    })?;
    if obj.is_empty() {
        return Err(TransformError::Invalid(
            "$project inclusion map must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(obj.len());
    for (key, flag) in obj {
        if key.starts_with('$') {
            return Err(TransformError::Invalid(format!(
                "$project does not support operator `{key}`; use inclusion (field: 1) or fields: [...]"
            )));
        }
        let include = match flag {
            Value::Bool(true) => true,
            Value::Number(n) if n.as_i64() == Some(1) || n.as_u64() == Some(1) => true,
            Value::Bool(false) => {
                return Err(TransformError::Invalid(
                    "$project exclusions (0/false) are not supported; list included fields only"
                        .to_string(),
                ));
            }
            Value::Number(n) if n.as_i64() == Some(0) || n.as_u64() == Some(0) => {
                return Err(TransformError::Invalid(
                    "$project exclusions (0/false) are not supported; list included fields only"
                        .to_string(),
                ));
            }
            other => {
                return Err(TransformError::Invalid(format!(
                    "$project field `{key}` must be inclusion 1/true (got {other}); expressions are not supported"
                )));
            }
        };
        if include {
            if key.trim().is_empty() {
                return Err(TransformError::Invalid(
                    "$project field names must not be empty".to_string(),
                ));
            }
            fields.push(key.clone());
        }
    }
    if fields.is_empty() {
        return Err(TransformError::Invalid(
            "$project must include at least one field".to_string(),
        ));
    }
    Ok(TransformOp::Project { fields })
}

fn parse_match_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Classic filter body: { field, eq }
    if value.as_object().is_some_and(|o| {
        o.contains_key("field") && o.contains_key("eq") && o.keys().all(|k| k == "field" || k == "eq")
    }) {
        return parse_filter(value);
    }
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$match/where must be an object with one equality predicate".to_string(),
        )
    })?;
    if obj.len() != 1 {
        return Err(TransformError::Invalid(
            "$match/where supports exactly one equality predicate (no $and/$or/multi-field)"
                .to_string(),
        ));
    }
    let (field, pred) = obj.iter().next().expect("len == 1");
    if field.starts_with('$') {
        return Err(TransformError::Invalid(format!(
            "$match does not support operator `{field}`; use {{ field: value }} equality only"
        )));
    }
    if field.trim().is_empty() {
        return Err(TransformError::Invalid(
            "$match field must not be empty".to_string(),
        ));
    }
    let eq_value = match pred {
        Value::Object(inner) => {
            if inner.len() == 1 && inner.contains_key("$eq") {
                inner.get("$eq").expect("$eq").clone()
            } else {
                return Err(TransformError::Invalid(
                    "$match supports only equality; use { field: value } or { field: { $eq: value } }"
                        .to_string(),
                ));
            }
        }
        other => other.clone(),
    };
    Ok(TransformOp::FilterEq {
        field: field.clone(),
        value: eq_value,
    })
}

fn parse_add_fields_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Classic body: { fields: [{ as, value|field }] }
    if is_classic_fields_body(value) {
        return parse_add_fields(value);
    }
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$addFields/$set must be an object map of new fields or { fields: [...] }".to_string(),
        )
    })?;
    if obj.is_empty() {
        return Err(TransformError::Invalid(
            "$addFields/$set must declare at least one field".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(obj.len());
    for (as_name, source_val) in obj {
        if as_name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "$addFields field name must not be empty".to_string(),
            ));
        }
        if as_name.starts_with('$') {
            return Err(TransformError::Invalid(format!(
                "$addFields does not support operator `{as_name}`"
            )));
        }
        let source = match source_val {
            Value::String(s) if s.starts_with('$') => {
                let field = s.strip_prefix('$').unwrap_or(s.as_str()).trim();
                if field.is_empty() {
                    return Err(TransformError::Invalid(format!(
                        "$addFields `{as_name}` field ref must not be empty"
                    )));
                }
                AddFieldSource::Field(field.to_string())
            }
            Value::Object(inner)
                if inner.len() == 1 && inner.contains_key("$literal") =>
            {
                AddFieldSource::Literal(inner.get("$literal").expect("$literal").clone())
            }
            Value::Object(inner) if inner.keys().any(|k| k.starts_with('$')) => {
                return Err(TransformError::Invalid(format!(
                    "$addFields `{as_name}` supports only $literal or \"$field\" refs; expressions are not supported"
                )));
            }
            other => AddFieldSource::Literal(other.clone()),
        };
        fields.push(AddFieldSpec {
            as_name: as_name.clone(),
            source,
        });
    }
    Ok(TransformOp::AddFields { fields })
}

fn parse_unset_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Classic remove body
    if is_classic_fields_body(value) {
        return parse_remove(value);
    }
    let fields = match value {
        Value::String(s) => {
            let name = s.trim();
            if name.is_empty() {
                return Err(TransformError::Invalid(
                    "$unset field must not be empty".to_string(),
                ));
            }
            vec![name.to_string()]
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err(TransformError::Invalid(
                    "$unset array must not be empty".to_string(),
                ));
            }
            let mut fields = Vec::with_capacity(arr.len());
            for entry in arr {
                let name = entry.as_str().ok_or_else(|| {
                    TransformError::Invalid("$unset entries must be strings".to_string())
                })?;
                if name.trim().is_empty() {
                    return Err(TransformError::Invalid(
                        "$unset entries must not be empty".to_string(),
                    ));
                }
                fields.push(name.to_string());
            }
            fields
        }
        _ => {
            return Err(TransformError::Invalid(
                "$unset must be a field name, an array of names, or { fields: [...] }".to_string(),
            ));
        }
    };
    Ok(TransformOp::Remove { fields })
}

fn parse_rename_dx(value: &Value) -> Result<TransformOp, TransformError> {
    if is_classic_fields_body(value) {
        return parse_rename(value);
    }
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$rename must be a from→to map or { fields: [{ from, to }] }".to_string(),
        )
    })?;
    if obj.is_empty() {
        return Err(TransformError::Invalid(
            "$rename map must not be empty".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(obj.len());
    for (from, to_val) in obj {
        if from.trim().is_empty() {
            return Err(TransformError::Invalid(
                "$rename from must not be empty".to_string(),
            ));
        }
        let to = to_val.as_str().ok_or_else(|| {
            TransformError::Invalid(format!("$rename `{from}` target must be a string"))
        })?;
        if to.trim().is_empty() {
            return Err(TransformError::Invalid(format!(
                "$rename `{from}` target must not be empty"
            )));
        }
        fields.push(RenameSpec {
            from: from.clone(),
            to: to.to_string(),
        });
    }
    Ok(TransformOp::Rename { fields })
}

fn parse_lookup_dx(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$lookup/join must be an object with from, localField, foreignField, and as"
                .to_string(),
        )
    })?;
    for banned in ["pipeline", "let", "asOf"] {
        if obj.contains_key(banned) {
            return Err(TransformError::Invalid(format!(
                "$lookup/join does not support `{banned}`; only declarative equijoin from/localField/foreignField/as (optional fromSchema) so Affect Analysis stays correct"
            )));
        }
    }
    // Reuse equiLookup validation (same required keys).
    parse_equi_lookup(value)
}

fn parse_unwind_dx(value: &Value) -> Result<TransformOp, TransformError> {
    match value {
        Value::String(s) => {
            let path_raw = s.trim();
            if path_raw.is_empty() {
                return Err(TransformError::Invalid(
                    "$unwind path must not be empty".to_string(),
                ));
            }
            let path = path_raw
                .strip_prefix('$')
                .unwrap_or(path_raw)
                .to_string();
            if path.is_empty() {
                return Err(TransformError::Invalid(
                    "$unwind path must not be empty".to_string(),
                ));
            }
            Ok(TransformOp::Unwind { path })
        }
        Value::Object(_) => parse_unwind(value),
        _ => Err(TransformError::Invalid(
            "$unwind must be a path string or { path }".to_string(),
        )),
    }
}

fn parse_union_with_dx(value: &Value) -> Result<TransformOp, TransformError> {
    match value {
        Value::String(s) => {
            let from = s.trim();
            if from.is_empty() {
                return Err(TransformError::Invalid(
                    "$unionWith collection must not be empty".to_string(),
                ));
            }
            Ok(TransformOp::Union {
                from: from.to_string(),
                from_schema: None,
            })
        }
        Value::Object(obj) => {
            for banned in ["pipeline", "let"] {
                if obj.contains_key(banned) {
                    return Err(TransformError::Invalid(format!(
                        "$unionWith does not support `{banned}`; only coll/from (optional fromSchema) so Affect Analysis stays correct"
                    )));
                }
            }
            // Mongo uses `coll`; we also accept `from` for symmetry with `union`.
            if obj.contains_key("from") && !obj.contains_key("coll") {
                return parse_union(value);
            }
            let from = obj
                .get("coll")
                .or_else(|| obj.get("from"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    TransformError::Invalid(
                        "$unionWith requires coll or from (Base Dataset table name)".to_string(),
                    )
                })?
                .trim();
            if from.is_empty() {
                return Err(TransformError::Invalid(
                    "$unionWith collection must not be empty".to_string(),
                ));
            }
            let from_schema = match obj.get("fromSchema") {
                None => None,
                Some(Value::Null) => None,
                Some(v) => {
                    let s = v.as_str().ok_or_else(|| {
                        TransformError::Invalid(
                            "$unionWith.fromSchema must be a string".to_string(),
                        )
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
                if !matches!(key.as_str(), "coll" | "from" | "fromSchema") {
                    return Err(TransformError::Invalid(format!(
                        "$unionWith only supports coll/from and fromSchema (unknown `{key}`)"
                    )));
                }
            }
            if obj.contains_key("coll") && obj.contains_key("from") {
                return Err(TransformError::Invalid(
                    "$unionWith accepts only one of coll or from".to_string(),
                ));
            }
            Ok(TransformOp::Union {
                from: from.to_string(),
                from_schema,
            })
        }
        _ => Err(TransformError::Invalid(
            "$unionWith must be a collection name string or { coll|from }".to_string(),
        )),
    }
}

fn field_ref(value: &Value, ctx: &str) -> Result<String, TransformError> {
    let raw = value.as_str().ok_or_else(|| {
        TransformError::Invalid(format!("{ctx} must be a field name or \"$field\" ref"))
    })?;
    let field = raw.strip_prefix('$').unwrap_or(raw).trim();
    if field.is_empty() {
        return Err(TransformError::Invalid(format!(
            "{ctx} field ref must not be empty"
        )));
    }
    if field.contains('.') {
        return Err(TransformError::Invalid(format!(
            "{ctx} does not support dotted paths ({field:?}); use a single Base/Derived field"
        )));
    }
    Ok(field.to_string())
}

fn parse_group_id_keys(id: &Value) -> Result<Vec<String>, TransformError> {
    match id {
        Value::String(_) => Ok(vec![field_ref(id, "$group._id")?]),
        Value::Array(arr) => {
            if arr.is_empty() {
                return Err(TransformError::Invalid(
                    "$group._id array must not be empty".to_string(),
                ));
            }
            let mut keys = Vec::with_capacity(arr.len());
            for entry in arr {
                keys.push(field_ref(entry, "$group._id")?);
            }
            Ok(keys)
        }
        Value::Object(obj) => {
            if obj.is_empty() {
                return Err(TransformError::Invalid(
                    "$group._id object must not be empty".to_string(),
                ));
            }
            let mut keys = Vec::with_capacity(obj.len());
            for (alias, path) in obj {
                let field = field_ref(path, "$group._id")?;
                if alias != &field {
                    return Err(TransformError::Invalid(format!(
                        "$group._id alias `{alias}` must match field `{field}` (v1 keeps group keys as Base field names)"
                    )));
                }
                keys.push(field);
            }
            Ok(keys)
        }
        Value::Null => Err(TransformError::Invalid(
            "$group._id: null (whole-collection group) is not supported".to_string(),
        )),
        _ => Err(TransformError::Invalid(
            "$group._id must be \"$field\", [\"$a\",\"$b\"], or { FIELD: \"$FIELD\" }".to_string(),
        )),
    }
}

fn parse_group_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Classic groupBy / distinct / addToSet bodies reuse existing parsers when present.
    if let Some(obj) = value.as_object() {
        if obj.contains_key("keys") && obj.contains_key("aggregates") {
            return parse_group_by(value);
        }
        if obj.contains_key("fields")
            && !obj.contains_key("_id")
            && !obj.contains_key("keys")
            && obj.keys().all(|k| k == "fields")
        {
            return parse_distinct(value);
        }
        if obj.contains_key("keys")
            && obj.contains_key("field")
            && obj.contains_key("as")
            && !obj.contains_key("_id")
        {
            return parse_add_to_set(value);
        }
    }

    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("$group must be an object with _id".to_string())
    })?;
    let id = obj.get("_id").ok_or_else(|| {
        TransformError::Invalid("$group._id is required".to_string())
    })?;
    let keys = parse_group_id_keys(id)?;

    let mut aggregates: Vec<AggregateSpec> = Vec::new();
    let mut add_to_set: Option<(String, String)> = None; // (field, as_name)

    for (as_name, acc) in obj.iter().filter(|(k, _)| k.as_str() != "_id") {
        if as_name.trim().is_empty() {
            return Err(TransformError::Invalid(
                "$group accumulator output name must not be empty".to_string(),
            ));
        }
        let acc_obj = acc.as_object().ok_or_else(|| {
            TransformError::Invalid(format!(
                "$group `{as_name}` must be an accumulator object like {{ $sum: \"$AMOUNT\" }}"
            ))
        })?;
        if acc_obj.len() != 1 {
            return Err(TransformError::Invalid(format!(
                "$group `{as_name}` must have exactly one accumulator operator"
            )));
        }
        let (op_name, op_arg) = acc_obj.iter().next().expect("len == 1");
        match op_name.as_str() {
            "$sum" | "$min" | "$max" | "$avg" | "$count" => {
                if add_to_set.is_some() {
                    return Err(TransformError::Invalid(
                        "$group cannot mix $addToSet with other aggregations in v1; use addToSet alone or groupBy aggregates"
                            .to_string(),
                    ));
                }
                let field = field_ref(op_arg, &format!("$group `{as_name}`"))?;
                let op = match op_name.as_str() {
                    "$sum" => AggregateOp::Sum,
                    "$min" => AggregateOp::Min,
                    "$max" => AggregateOp::Max,
                    "$avg" => AggregateOp::Avg,
                    "$count" => AggregateOp::Count,
                    _ => unreachable!(),
                };
                aggregates.push(AggregateSpec {
                    op,
                    field,
                    as_name: as_name.clone(),
                });
            }
            "$addToSet" => {
                if !aggregates.is_empty() || add_to_set.is_some() {
                    return Err(TransformError::Invalid(
                        "$group supports at most one $addToSet and cannot mix it with $sum/$count/$min/$max/$avg in v1"
                            .to_string(),
                    ));
                }
                let field = field_ref(op_arg, &format!("$group `{as_name}`"))?;
                add_to_set = Some((field, as_name.clone()));
            }
            other => {
                return Err(TransformError::Invalid(format!(
                    "$group accumulator {other:?} is unsupported; v1 allows $sum, $count, $min, $max, $avg, $addToSet"
                )));
            }
        }
    }

    if let Some((field, as_name)) = add_to_set {
        return Ok(TransformOp::AddToSet {
            keys,
            field,
            as_name,
        });
    }
    if aggregates.is_empty() {
        // SQL DISTINCT / unique keys only.
        return Ok(TransformOp::Distinct { fields: keys });
    }
    Ok(TransformOp::GroupBy { keys, aggregates })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        analyze_base_change, evaluate_transform_with_bases, BaseChangeContext, BaseChangeKind,
        parse_transform_steps,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    fn row(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// Normalize IR so map-iteration order (serde_json BTreeMap) does not fail
    /// semantic equivalence against classic arrays.
    fn normalize_ops(mut ops: Vec<TransformOp>) -> Vec<TransformOp> {
        for op in &mut ops {
            match op {
                TransformOp::Project { fields } => fields.sort(),
                TransformOp::AddFields { fields } => {
                    fields.sort_by(|a, b| a.as_name.cmp(&b.as_name));
                }
                TransformOp::Rename { fields } => {
                    fields.sort_by(|a, b| a.from.cmp(&b.from));
                }
                TransformOp::Remove { fields } => fields.sort(),
                TransformOp::GroupBy { keys, aggregates } => {
                    keys.sort();
                    aggregates.sort_by(|a, b| a.as_name.cmp(&b.as_name));
                }
                TransformOp::Distinct { fields } => fields.sort(),
                TransformOp::AddToSet { keys, .. } => keys.sort(),
                _ => {}
            }
        }
        ops
    }

    fn assert_ops_eq(old: &[Value], new: &[Value]) {
        let old_ops = normalize_ops(parse_transform_steps(old).expect("old form"));
        let new_ops = normalize_ops(parse_transform_steps(new).expect("new DX form"));
        assert_eq!(old_ops, new_ops, "IR must match for old vs Aggregation DX");
    }

    #[test]
    fn project_filter_field_ops_equivalence() {
        assert_ops_eq(
            &[
                json!({"project": {"fields": ["ID", "NAME", "STATUS", "EMAIL"]}}),
                json!({"filter": {"field": "STATUS", "eq": "OPEN"}}),
                json!({"addFields": {"fields": [
                    {"as": "currency", "value": "USD"},
                    {"as": "displayName", "field": "NAME"}
                ]}}),
                json!({"rename": {"fields": [{"from": "NAME", "to": "customerName"}]}}),
                json!({"remove": {"fields": ["EMAIL"]}}),
            ],
            &[
                json!({"$project": {"ID": 1, "NAME": 1, "STATUS": 1, "EMAIL": 1}}),
                json!({"$match": {"STATUS": "OPEN"}}),
                json!({"$addFields": {
                    "currency": {"$literal": "USD"},
                    "displayName": "$NAME"
                }}),
                json!({"$rename": {"NAME": "customerName"}}),
                json!({"$unset": ["EMAIL"]}),
            ],
        );
        // Order-preserving Aggregation body (fields array under $project).
        assert_ops_eq(
            &[json!({"project": {"fields": ["ID", "NAME", "STATUS", "EMAIL"]}})],
            &[json!({"$project": {"fields": ["ID", "NAME", "STATUS", "EMAIL"]}})],
        );
        // SQL-ish aliases
        assert_ops_eq(
            &[
                json!({"project": {"fields": ["ID", "NAME"]}}),
                json!({"filter": {"field": "STATUS", "eq": "OPEN"}}),
            ],
            &[
                json!({"select": {"fields": ["ID", "NAME"]}}),
                json!({"where": {"field": "STATUS", "eq": "OPEN"}}),
            ],
        );
    }

    #[test]
    fn join_lookup_unwind_union_groupby_equivalence() {
        assert_ops_eq(
            &[json!({
                "equiLookup": {
                    "from": "ORDERS",
                    "localField": "ID",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            })],
            &[json!({
                "$lookup": {
                    "from": "ORDERS",
                    "localField": "ID",
                    "foreignField": "CUSTOMER_ID",
                    "as": "orders"
                }
            })],
        );
        assert_ops_eq(
            &[json!({"equiLookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }})],
            &[json!({"join": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }})],
        );
        assert_ops_eq(
            &[json!({"unwind": {"path": "orders"}})],
            &[json!({"$unwind": "$orders"})],
        );
        assert_ops_eq(
            &[json!({"union": {"from": "WEST_CUSTOMERS"}})],
            &[json!({"$unionWith": {"coll": "WEST_CUSTOMERS"}})],
        );
        assert_ops_eq(
            &[json!({"union": {"from": "WEST_CUSTOMERS"}})],
            &[json!({"$unionWith": "WEST_CUSTOMERS"})],
        );
        assert_ops_eq(
            &[json!({
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
            })],
            &[json!({
                "$group": {
                    "_id": "$CUSTOMER_ID",
                    "ORDER_COUNT": {"$count": "$ORDER_ID"},
                    "MIN_AMOUNT": {"$min": "$AMOUNT"},
                    "MAX_AMOUNT": {"$max": "$AMOUNT"},
                    "AVG_AMOUNT": {"$avg": "$AMOUNT"},
                    "TOTAL_AMOUNT": {"$sum": "$AMOUNT"}
                }
            })],
        );
        assert_ops_eq(
            &[json!({"distinct": {"fields": ["CUSTOMER_ID"]}})],
            &[json!({"$group": {"_id": "$CUSTOMER_ID"}})],
        );
        assert_ops_eq(
            &[json!({
                "addToSet": {
                    "keys": ["CUSTOMER_ID"],
                    "field": "AMOUNT",
                    "as": "AMOUNTS"
                }
            })],
            &[json!({
                "$group": {
                    "_id": "$CUSTOMER_ID",
                    "AMOUNTS": {"$addToSet": "$AMOUNT"}
                }
            })],
        );
    }

    #[test]
    fn aggregation_dx_affect_and_eval_match_classic() {
        let classic = parse_transform_steps(&[json!({
            "groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL_AMOUNT"}]
            }
        })])
        .unwrap();
        let agg = parse_transform_steps(&[json!({
            "$group": {
                "_id": "$CUSTOMER_ID",
                "TOTAL_AMOUNT": {"$sum": "$AMOUNT"}
            }
        })])
        .unwrap();
        assert_eq!(classic, agg);

        let rows = vec![
            row(&[
                ("ORDER_ID", json!(1)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("10.00")),
                ("ADDRESS", json!("x")),
            ]),
            row(&[
                ("ORDER_ID", json!(2)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("5.00")),
                ("ADDRESS", json!("y")),
            ]),
        ];
        let secondary = BTreeMap::new();
        let classic_out = evaluate_transform_with_bases(&classic, &rows, &secondary).unwrap();
        let agg_out = evaluate_transform_with_bases(&agg, &rows, &secondary).unwrap();
        assert_eq!(classic_out, agg_out);

        let after_address_only = row(&[
            ("ORDER_ID", json!(1)),
            ("CUSTOMER_ID", json!(1)),
            ("AMOUNT", json!("10.00")),
            ("ADDRESS", json!("changed")),
        ]);
        let ctx = BaseChangeContext {
            changed_base: "ORDERS",
            primary_base: "ORDERS",
            kind: BaseChangeKind::Update,
            before: Some(&rows[0]),
            after: Some(&after_address_only),
            primary_rows: &rows,
            secondary_bases: &secondary,
        };
        let analysis_classic = analyze_base_change(&classic, &ctx, None).unwrap();
        let analysis_agg = analyze_base_change(&agg, &ctx, None).unwrap();
        assert_eq!(analysis_classic.outcome, analysis_agg.outcome);
    }

    #[test]
    fn project_match_and_lookup_affect_eval_match_classic() {
        let classic_pf = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME", "ACTIVE"]}}),
            json!({"filter": {"field": "ACTIVE", "eq": 1}}),
        ])
        .unwrap();
        let agg_pf = parse_transform_steps(&[
            json!({"$project": {"fields": ["ID", "NAME", "ACTIVE"]}}),
            json!({"$match": {"ACTIVE": 1}}),
        ])
        .unwrap();
        assert_eq!(classic_pf, agg_pf);

        let rows = vec![
            row(&[("ID", json!(1)), ("NAME", json!("A")), ("ACTIVE", json!(1)), ("EMAIL", json!("a"))]),
            row(&[("ID", json!(2)), ("NAME", json!("B")), ("ACTIVE", json!(0)), ("EMAIL", json!("b"))]),
        ];
        let secondary = BTreeMap::new();
        assert_eq!(
            evaluate_transform_with_bases(&classic_pf, &rows, &secondary).unwrap(),
            evaluate_transform_with_bases(&agg_pf, &rows, &secondary).unwrap()
        );
        let after_email = row(&[
            ("ID", json!(1)),
            ("NAME", json!("A")),
            ("ACTIVE", json!(1)),
            ("EMAIL", json!("changed")),
        ]);
        let ctx = BaseChangeContext {
            changed_base: "CUSTOMERS",
            primary_base: "CUSTOMERS",
            kind: BaseChangeKind::Update,
            before: Some(&rows[0]),
            after: Some(&after_email),
            primary_rows: &rows,
            secondary_bases: &secondary,
        };
        assert_eq!(
            analyze_base_change(&classic_pf, &ctx, None).unwrap().outcome,
            analyze_base_change(&agg_pf, &ctx, None).unwrap().outcome
        );

        let classic_join = parse_transform_steps(&[json!({
            "equiLookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }
        })])
        .unwrap();
        let agg_join = parse_transform_steps(&[json!({
            "$lookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }
        })])
        .unwrap();
        assert_eq!(classic_join, agg_join);
        let customers = vec![row(&[("ID", json!(1)), ("NAME", json!("A"))])];
        let mut secondary_orders = BTreeMap::new();
        secondary_orders.insert(
            "ORDERS".to_string(),
            vec![row(&[
                ("ORDER_ID", json!(10)),
                ("CUSTOMER_ID", json!(1)),
                ("AMOUNT", json!("5.00")),
            ])],
        );
        assert_eq!(
            evaluate_transform_with_bases(&classic_join, &customers, &secondary_orders).unwrap(),
            evaluate_transform_with_bases(&agg_join, &customers, &secondary_orders).unwrap()
        );
    }

    #[test]
    fn free_form_aggregation_extensions_still_rejected() {
        let err = parse_transform_steps(&[json!({
            "$lookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders",
                "pipeline": [{"$match": {}}]
            }
        })])
        .unwrap_err();
        assert!(err.to_string().to_ascii_lowercase().contains("pipeline"));

        let err = parse_transform_steps(&[json!({
            "$match": {"STATUS": {"$gt": 1}}
        })])
        .unwrap_err();
        assert!(
            matches!(err, TransformError::Invalid(_)),
            "non-eq $match must be Invalid, got {err:?}"
        );

        let err = parse_transform_steps(&[json!({"script": "return true"})]).unwrap_err();
        assert_eq!(err, TransformError::FreeFormScript);

        let err = parse_transform_steps(&[json!({
            "$unionWith": {
                "coll": "WEST",
                "pipeline": []
            }
        })])
        .unwrap_err();
        assert!(err.to_string().to_ascii_lowercase().contains("pipeline"));

        let err = parse_transform_steps(&[json!({
            "$unwind": {
                "path": "$orders",
                "preserveNullAndEmptyArrays": true
            }
        })])
        .unwrap_err();
        assert!(err
            .to_string()
            .to_ascii_lowercase()
            .contains("preservenullandemptyarrays"));
    }

    #[test]
    fn classic_form_still_parses_unchanged() {
        // Issue #233: classic steps remain Upgrade Compatible after Aggregation DX contract.
        let ops = parse_transform_steps(&[
            json!({"project": {"fields": ["ID", "NAME"]}}),
            json!({"filter": {"field": "STATUS", "eq": "OPEN"}}),
            json!({"groupBy": {
                "keys": ["CUSTOMER_ID"],
                "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL"}]
            }}),
        ])
        .unwrap();
        assert_eq!(ops.len(), 3);
        assert!(matches!(ops[0], TransformOp::Project { .. }));
        assert!(matches!(ops[1], TransformOp::FilterEq { .. }));
        assert!(matches!(ops[2], TransformOp::GroupBy { .. }));
    }
}
