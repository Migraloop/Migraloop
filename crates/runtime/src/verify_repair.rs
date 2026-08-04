//! Shared verify-and-repair internals for Source Alignment Check and Drift Check
//! (issue #183 / #168).
//!
//! Alignment and Drift remain distinct Operator verbs and domain concepts in
//! [`crate::lifecycle`]. This module owns the isomorphic pieces — read budget,
//! field-subset equality, and JSON compare normalization — so budget/equality
//! bugs are fixed once without merging the two checks.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

/// Resolve a resource-gate budget: `0` means the check's default max rows.
pub(crate) fn effective_max_rows(max_rows: u32, default: u32) -> u32 {
    if max_rows == 0 {
        default
    } else {
        max_rows
    }
}

/// Persisted check status: full window → `ok_label`, truncated → `"partial"`.
pub(crate) fn persisted_status(truncated: bool, ok_label: &'static str) -> &'static str {
    if truncated {
        "partial"
    } else {
        ok_label
    }
}

/// Operator detect line: mismatches → `bad_label`, else same as [`persisted_status`].
pub(crate) fn detect_status(
    mismatched: i32,
    truncated: bool,
    ok_label: &'static str,
    bad_label: &'static str,
) -> &'static str {
    if mismatched > 0 {
        bad_label
    } else {
        persisted_status(truncated, ok_label)
    }
}

/// Counts from walking expected identities against an indexed actual side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MismatchRepair<T> {
    pub mismatched: i32,
    pub repaired: i32,
    pub repairs: Vec<T>,
}

/// For each expected identity: when absent or unequal in `actual_by_id`, count a
/// mismatch and collect a repair payload. Matching identities produce no repair.
pub(crate) fn collect_mismatched_repairs<K, E, A, R>(
    expected_items: impl IntoIterator<Item = (K, E)>,
    actual_by_id: &BTreeMap<K, A>,
    eq: impl Fn(&E, &A) -> bool,
    into_repair: impl Fn(E) -> R,
) -> MismatchRepair<R>
where
    K: Ord,
{
    let mut mismatched = 0i32;
    let mut repaired = 0i32;
    let mut repairs = Vec::new();
    for (key, expected) in expected_items {
        let needs_repair = match actual_by_id.get(&key) {
            Some(actual) if eq(&expected, actual) => false,
            _ => true,
        };
        if needs_repair {
            mismatched += 1;
            repaired += 1;
            repairs.push(into_repair(expected));
        }
    }
    MismatchRepair {
        mismatched,
        repaired,
        repairs,
    }
}

/// Field-subset equality over a key set. Missing on both sides counts as equal.
/// Values compare via [`json_values_equal`] (Mongo Extended JSON–aware).
pub(crate) fn maps_equal_on_keys(
    left: &Map<String, Value>,
    right: &Map<String, Value>,
    keys: impl IntoIterator<Item = impl AsRef<str>>,
) -> bool {
    for key in keys {
        let name = key.as_ref();
        if !optional_json_values_equal(left.get(name), right.get(name)) {
            return false;
        }
    }
    true
}

/// Managed-field subset equality when the actual side is a Target document.
/// Non-listed keys on the document are ignored.
pub(crate) fn document_fields_match(
    actual_doc: &Value,
    expected_fields: &Map<String, Value>,
    keys: &[&str],
) -> bool {
    for key in keys {
        if !optional_json_values_equal(expected_fields.get(*key), actual_doc.get(*key)) {
            return false;
        }
    }
    true
}

fn optional_json_values_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (Some(l), Some(r)) => json_values_equal(l, r),
        (None, None) => true,
        _ => false,
    }
}

/// Equality after [`normalize_json_for_compare`] (plain JSON and Mongo Extended JSON).
pub(crate) fn json_values_equal(left: &Value, right: &Value) -> bool {
    normalize_json_for_compare(left) == normalize_json_for_compare(right)
}

/// Collapse Mongo Extended JSON number/date wrappers so Alignment and Drift
/// share one compare path.
pub(crate) fn normalize_json_for_compare(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            if let Some(n) = map.get("$numberLong").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<i64>() {
                    return Value::Number(parsed.into());
                }
            }
            if let Some(n) = map.get("$numberInt").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<i64>() {
                    return Value::Number(parsed.into());
                }
            }
            if let Some(n) = map.get("$numberDouble").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(parsed) {
                        return Value::Number(num);
                    }
                }
            }
            if let Some(n) = map.get("$numberDecimal").and_then(|v| v.as_str()) {
                return Value::String(n.to_string());
            }
            if let Some(d) = map.get("$date") {
                return normalize_json_for_compare(d);
            }
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), normalize_json_for_compare(v));
            }
            Value::Object(out)
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                Value::Number(u.into())
            } else {
                value.clone()
            }
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(normalize_json_for_compare).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn effective_max_rows_zero_means_default() {
        assert_eq!(effective_max_rows(0, 1000), 1000);
        assert_eq!(effective_max_rows(25, 1000), 25);
    }

    #[test]
    fn persisted_and_detect_status_labels() {
        assert_eq!(persisted_status(false, "aligned"), "aligned");
        assert_eq!(persisted_status(true, "aligned"), "partial");
        assert_eq!(persisted_status(false, "ok"), "ok");
        assert_eq!(detect_status(0, false, "ok", "drifted"), "ok");
        assert_eq!(detect_status(0, true, "ok", "drifted"), "partial");
        assert_eq!(detect_status(2, false, "aligned", "misaligned"), "misaligned");
    }

    #[test]
    fn json_values_equal_collapses_mongo_extended_number_long() {
        let plain = json!(42);
        let extended = json!({ "$numberLong": "42" });
        assert!(json_values_equal(&plain, &extended));
        assert!(!json_values_equal(&plain, &json!(43)));
    }

    #[test]
    fn maps_equal_on_keys_ignores_keys_outside_supported_set() {
        let left = json!({ "ID": 1, "NAME": "a", "EXTRA": "x" })
            .as_object()
            .unwrap()
            .clone();
        let right = json!({ "ID": 1, "NAME": "a", "EXTRA": "y" })
            .as_object()
            .unwrap()
            .clone();
        let keys = ["ID", "NAME"];
        assert!(maps_equal_on_keys(&left, &right, keys));
        assert!(!maps_equal_on_keys(
            &left,
            &right,
            ["ID", "NAME", "EXTRA"]
        ));
    }

    #[test]
    fn document_fields_match_ignores_non_managed_target_keys() {
        let expected = json!({ "NAME": "Alice" }).as_object().unwrap().clone();
        let target_ok = json!({
            "NAME": "Alice",
            "manual_note": "keep me",
            "_id": "doc-1",
        });
        let target_mismatched = json!({
            "NAME": "Bob",
            "manual_note": "keep me",
        });
        assert!(document_fields_match(&target_ok, &expected, &["NAME"]));
        assert!(!document_fields_match(
            &target_mismatched,
            &expected,
            &["NAME"]
        ));
    }

    #[test]
    fn collect_mismatched_repairs_only_collects_unequal_or_missing() {
        let mut actual = BTreeMap::new();
        actual.insert("1".to_string(), json!({"v": 1}));
        actual.insert("2".to_string(), json!({"v": 9}));

        let expected = vec![
            ("1".to_string(), json!({"v": 1})),
            ("2".to_string(), json!({"v": 2})),
            ("3".to_string(), json!({"v": 3})),
        ];
        let outcome = collect_mismatched_repairs(
            expected,
            &actual,
            |e, a| e == a,
            |e| e,
        );
        assert_eq!(outcome.mismatched, 2);
        assert_eq!(outcome.repaired, 2);
        assert_eq!(
            outcome.repairs,
            vec![json!({"v": 2}), json!({"v": 3})]
        );
    }
}
