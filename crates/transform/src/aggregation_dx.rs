//! Aggregation-only Rich Transform authoring (ADR-0030).
//!
//! Accepts MongoDB Aggregation–shaped stages as the sole declarative authoring
//! surface. Every accepted shape normalizes to the same [`TransformOp`] IR so
//! Affect Analysis and evaluation stay unchanged.
//!
//! Classic step names and SQL-ish aliases (`select` / `where` / `join`) are
//! rejected. Free-form scripts and unanalyzable Aggregation extensions
//! (`pipeline`, `let`, multi-predicate `$match`, expression `$project`, …)
//! remain rejected.

use serde_json::{Map, Value};

use crate::{
    parse_equi_lookup, parse_project, parse_union, parse_unwind, AddFieldSource, AddFieldSpec,
    AggregateOp, AggregateSpec, RenameSpec, TransformError, TransformOp,
};

/// `$project` may use `{ fields: [...] }` (order-preserving Aggregation body).
fn is_project_fields_array_body(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|o| o.contains_key("fields") && o.keys().all(|k| k == "fields"))
}

/// Try parsing one step as an Aggregation `$…` stage.
///
/// Returns `None` when the step is not a supported Aggregation stage name
/// (caller reports [`TransformError::UnsupportedOperator`]).
pub(crate) fn try_parse_aggregation_step(
    obj: &Map<String, Value>,
) -> Result<Option<TransformOp>, TransformError> {
    if obj.len() != 1 {
        // Mixed keys are invalid for every step form; let the caller report.
        return Ok(None);
    }
    let (name, value) = obj.iter().next().expect("len == 1");
    let op = match name.as_str() {
        "$project" => Some(parse_project_dx(value)?),
        "$match" => Some(parse_match_dx(value)?),
        "$addFields" | "$set" => Some(parse_add_fields_dx(value)?),
        "$unset" => Some(parse_unset_dx(value)?),
        "$rename" => Some(parse_rename_dx(value)?),
        "$lookup" => Some(parse_lookup_dx(value)?),
        "$unwind" => Some(parse_unwind_dx(value)?),
        "$unionWith" => Some(parse_union_with_dx(value)?),
        "$group" => Some(parse_group_dx(value)?),
        _ => None,
    };
    Ok(op)
}

fn parse_project_dx(value: &Value) -> Result<TransformOp, TransformError> {
    // Order-preserving Aggregation body: { fields: [...] }.
    if is_project_fields_array_body(value) {
        return parse_project(value);
    }
    // Aggregation inclusion map: { ID: 1, NAME: true } — inclusion only.
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$project must be an object with fields or an inclusion map".to_string(),
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
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$match must be an object with one equality predicate".to_string(),
        )
    })?;
    if obj.contains_key("field") && obj.contains_key("eq") {
        return Err(TransformError::Invalid(
            "$match does not accept classic { field, eq }; use { FIELD: value } equality"
                .to_string(),
        ));
    }
    if obj.len() != 1 {
        return Err(TransformError::Invalid(
            "$match supports exactly one equality predicate (no $and/$or/multi-field)".to_string(),
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
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid(
            "$addFields/$set must be an object map of new fields".to_string(),
        )
    })?;
    if obj.contains_key("fields") && obj.keys().all(|k| k == "fields") {
        return Err(TransformError::Invalid(
            "$addFields does not accept classic { fields: [...] }; use a field map (as: \"$field\"|literal)"
                .to_string(),
        ));
    }
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
        Value::Object(obj)
            if obj.contains_key("fields") && obj.keys().all(|k| k == "fields") =>
        {
            return Err(TransformError::Invalid(
                "$unset does not accept classic { fields: [...] }; use a field name or array of names"
                    .to_string(),
            ));
        }
        _ => {
            return Err(TransformError::Invalid(
                "$unset must be a field name or an array of names".to_string(),
            ));
        }
    };
    Ok(TransformOp::Remove { fields })
}

fn parse_rename_dx(value: &Value) -> Result<TransformOp, TransformError> {
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("$rename must be a from→to map".to_string())
    })?;
    if obj.contains_key("fields") && obj.keys().all(|k| k == "fields") {
        return Err(TransformError::Invalid(
            "$rename does not accept classic { fields: [{ from, to }] }; use a { FROM: TO } map"
                .to_string(),
        ));
    }
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
            "$lookup must be an object with from, localField, foreignField, and as".to_string(),
        )
    })?;
    for banned in ["pipeline", "let", "asOf"] {
        if obj.contains_key(banned) {
            return Err(TransformError::Invalid(format!(
                "$lookup does not support `{banned}`; only declarative equijoin from/localField/foreignField/as (optional fromSchema) so Affect Analysis stays correct"
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
    let obj = value.as_object().ok_or_else(|| {
        TransformError::Invalid("$group must be an object with _id".to_string())
    })?;
    if !obj.contains_key("_id") {
        if obj.contains_key("keys") || obj.contains_key("aggregates") || obj.contains_key("fields")
        {
            return Err(TransformError::Invalid(
                "$group requires `_id`; classic groupBy/distinct/addToSet bodies are not accepted — use `_id: \"$KEY\"` and `$sum`/`$count`/`$addToSet`/…"
                    .to_string(),
            ));
        }
        return Err(TransformError::Invalid(
            "$group._id is required".to_string(),
        ));
    }
    let id = obj.get("_id").expect("_id present");
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
                        "$group cannot mix $addToSet with other aggregations in v1; use $addToSet alone or $sum/$count/$min/$max/$avg"
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

    /// Normalize IR so map-iteration order does not fail semantic equality.
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

    fn parse_ops(steps: &[Value]) -> Vec<TransformOp> {
        normalize_ops(parse_transform_steps(steps).expect("Aggregation form"))
    }

    #[test]
    fn project_match_field_ops_parse_to_analyzable_ir() {
        let ops = parse_ops(&[
            json!({"$project": {"ID": 1, "NAME": 1, "STATUS": 1, "EMAIL": 1}}),
            json!({"$match": {"STATUS": "OPEN"}}),
            json!({"$addFields": {
                "currency": {"$literal": "USD"},
                "displayName": "$NAME"
            }}),
            json!({"$rename": {"NAME": "customerName"}}),
            json!({"$unset": ["EMAIL"]}),
        ]);
        assert_eq!(ops.len(), 5);
        assert!(matches!(ops[0], TransformOp::Project { .. }));
        assert!(matches!(ops[1], TransformOp::FilterEq { .. }));
        assert!(matches!(ops[2], TransformOp::AddFields { .. }));
        assert!(matches!(ops[3], TransformOp::Rename { .. }));
        assert!(matches!(ops[4], TransformOp::Remove { .. }));

        // Order-preserving Aggregation body (fields array under $project).
        let fields_body = parse_ops(&[json!({"$project": {"fields": ["ID", "NAME", "STATUS", "EMAIL"]}})]);
        let inclusion = parse_ops(&[json!({"$project": {"ID": 1, "NAME": 1, "STATUS": 1, "EMAIL": 1}})]);
        assert_eq!(fields_body, inclusion);
    }

    #[test]
    fn lookup_unwind_union_group_parse_to_analyzable_ir() {
        let lookup = parse_ops(&[json!({
            "$lookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }
        })]);
        assert!(matches!(lookup[0], TransformOp::EquiLookup { .. }));

        let unwind = parse_ops(&[json!({"$unwind": "$orders"})]);
        assert_eq!(
            unwind,
            parse_ops(&[json!({"$unwind": {"path": "orders"}})])
        );

        let union_coll = parse_ops(&[json!({"$unionWith": {"coll": "WEST_CUSTOMERS"}})]);
        let union_str = parse_ops(&[json!({"$unionWith": "WEST_CUSTOMERS"})]);
        assert_eq!(union_coll, union_str);
        assert!(matches!(union_coll[0], TransformOp::Union { .. }));

        let group = parse_ops(&[json!({
            "$group": {
                "_id": "$CUSTOMER_ID",
                "ORDER_COUNT": {"$count": "$ORDER_ID"},
                "MIN_AMOUNT": {"$min": "$AMOUNT"},
                "MAX_AMOUNT": {"$max": "$AMOUNT"},
                "AVG_AMOUNT": {"$avg": "$AMOUNT"},
                "TOTAL_AMOUNT": {"$sum": "$AMOUNT"}
            }
        })]);
        assert!(matches!(group[0], TransformOp::GroupBy { .. }));

        let distinct = parse_ops(&[json!({"$group": {"_id": "$CUSTOMER_ID"}})]);
        assert!(matches!(distinct[0], TransformOp::Distinct { .. }));

        let add_to_set = parse_ops(&[json!({
            "$group": {
                "_id": "$CUSTOMER_ID",
                "AMOUNTS": {"$addToSet": "$AMOUNT"}
            }
        })]);
        assert!(matches!(add_to_set[0], TransformOp::AddToSet { .. }));
    }

    #[test]
    fn aggregation_group_affect_and_eval_stay_correct() {
        let agg = parse_transform_steps(&[json!({
            "$group": {
                "_id": "$CUSTOMER_ID",
                "TOTAL_AMOUNT": {"$sum": "$AMOUNT"}
            }
        })])
        .unwrap();

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
        let out = evaluate_transform_with_bases(&agg, &rows, &secondary).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("CUSTOMER_ID"), Some(&json!(1)));
        assert_eq!(out[0].get("TOTAL_AMOUNT"), Some(&json!("15.00")));

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
        let analysis = analyze_base_change(&agg, &ctx, None).unwrap();
        assert_eq!(analysis.outcome, crate::AffectOutcome::SkipUnusedFields);
    }

    #[test]
    fn project_match_and_lookup_affect_eval_stay_correct() {
        let agg_pf = parse_transform_steps(&[
            json!({"$project": {"fields": ["ID", "NAME", "ACTIVE"]}}),
            json!({"$match": {"ACTIVE": 1}}),
        ])
        .unwrap();

        let rows = vec![
            row(&[("ID", json!(1)), ("NAME", json!("A")), ("ACTIVE", json!(1)), ("EMAIL", json!("a"))]),
            row(&[("ID", json!(2)), ("NAME", json!("B")), ("ACTIVE", json!(0)), ("EMAIL", json!("b"))]),
        ];
        let secondary = BTreeMap::new();
        let out = evaluate_transform_with_bases(&agg_pf, &rows, &secondary).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get("NAME"), Some(&json!("A")));
        assert!(out[0].get("EMAIL").is_none());

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
            analyze_base_change(&agg_pf, &ctx, None).unwrap().outcome,
            crate::AffectOutcome::SkipUnusedFields
        );

        let agg_join = parse_transform_steps(&[json!({
            "$lookup": {
                "from": "ORDERS",
                "localField": "ID",
                "foreignField": "CUSTOMER_ID",
                "as": "orders"
            }
        })])
        .unwrap();
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
        let joined =
            evaluate_transform_with_bases(&agg_join, &customers, &secondary_orders).unwrap();
        assert_eq!(joined.len(), 1);
        assert!(joined[0].get("orders").is_some());
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
    fn classic_step_names_are_rejected() {
        // ADR-0030 / issue #250: classic step-name authoring removed (no read-compat).
        for (step, name) in [
            (
                json!({"project": {"fields": ["ID", "NAME"]}}),
                "project",
            ),
            (
                json!({"filter": {"field": "STATUS", "eq": "OPEN"}}),
                "filter",
            ),
            (
                json!({"addFields": {"fields": [{"as": "x", "value": 1}]}}),
                "addFields",
            ),
            (
                json!({"rename": {"fields": [{"from": "A", "to": "B"}]}}),
                "rename",
            ),
            (json!({"remove": {"fields": ["EMAIL"]}}), "remove"),
            (
                json!({
                    "equiLookup": {
                        "from": "ORDERS",
                        "localField": "ID",
                        "foreignField": "CUSTOMER_ID",
                        "as": "orders"
                    }
                }),
                "equiLookup",
            ),
            (json!({"unwind": {"path": "orders"}}), "unwind"),
            (json!({"union": {"from": "WEST"}}), "union"),
            (
                json!({
                    "groupBy": {
                        "keys": ["CUSTOMER_ID"],
                        "aggregates": [{"op": "sum", "field": "AMOUNT", "as": "TOTAL"}]
                    }
                }),
                "groupBy",
            ),
            (json!({"distinct": {"fields": ["CUSTOMER_ID"]}}), "distinct"),
            (
                json!({
                    "addToSet": {
                        "keys": ["CUSTOMER_ID"],
                        "field": "AMOUNT",
                        "as": "AMOUNTS"
                    }
                }),
                "addToSet",
            ),
        ] {
            let err = parse_transform_steps(&[step]).expect_err(name);
            match &err {
                TransformError::UnsupportedOperator(got) => {
                    assert_eq!(got, name, "unsupported operator name for classic `{name}`");
                }
                other => panic!("classic `{name}` must be UnsupportedOperator, got {other:?}"),
            }
            let msg = err.to_string();
            assert!(
                msg.contains("$project") || msg.contains("$match") || msg.contains("$group"),
                "reject message must point Operators at Aggregation `$…` forms, got: {msg}"
            );
            assert!(
                !msg.contains("classic steps") && !msg.contains("select/where/join"),
                "reject message must not advertise removed classic/SQL-ish surfaces, got: {msg}"
            );
        }
    }

    #[test]
    fn sql_ish_aliases_are_rejected() {
        // ADR-0030 / issue #250: select/where/join aliases removed.
        for (step, name) in [
            (json!({"select": {"fields": ["ID", "NAME"]}}), "select"),
            (
                json!({"where": {"field": "STATUS", "eq": "OPEN"}}),
                "where",
            ),
            (
                json!({
                    "join": {
                        "from": "ORDERS",
                        "localField": "ID",
                        "foreignField": "CUSTOMER_ID",
                        "as": "orders"
                    }
                }),
                "join",
            ),
        ] {
            let err = parse_transform_steps(&[step]).expect_err(name);
            match &err {
                TransformError::UnsupportedOperator(got) => {
                    assert_eq!(got, name, "unsupported operator name for alias `{name}`");
                }
                other => panic!("SQL-ish `{name}` must be UnsupportedOperator, got {other:?}"),
            }
        }
    }

    #[test]
    fn unsupported_aggregation_stages_still_reject_clearly() {
        for (step, needle) in [
            (json!({"$sort": {"AMOUNT": 1}}), "$sort"),
            (json!({"$limit": 10}), "$limit"),
            (json!({"$facet": {}}), "$facet"),
        ] {
            let err = parse_transform_steps(&[step]).expect_err(needle);
            match &err {
                TransformError::UnsupportedOperator(got) => {
                    assert_eq!(got, needle);
                }
                other => panic!("{needle} must be UnsupportedOperator, got {other:?}"),
            }
            assert!(
                !err.to_string().contains("silent"),
                "unsupported Aggregation stages must fail clearly"
            );
        }
    }
}
