//! Executable Lab Scenario correctness vocabulary (#205 / ADR-0025).
//!
//! `checks.correctness` is a small data-driven inspect vocabulary (Managed field
//! present/absent, field-key absence, substring / amount mentions, row /
//! document counts, status text). The recipe runner executes these checks at
//! `assert` (and settle loops may re-evaluate them). Rare escapes (poison
//! status, schema DDL bridges, pause/resume CLI, settle orchestration) stay in
//! thin ProductPathHooks — not duplicate isomorphic inspect arms.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::lab::{lab_migraloop_bin, LAB_PLATFORM_STORE_URL};
use crate::CliError;

use super::recipe::ScenarioRecipe;
use super::run_product_cli;

/// Where a correctness check inspects (product CLI verb / status).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CorrectnessSurface {
    Base,
    Derived,
    Target,
    Status,
}

/// One Managed field/value pair for present/absent expectations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct FieldValue {
    pub(crate) field: String,
    pub(crate) value: String,
}

/// One runnable correctness check driven from `recipe.yaml` (#205).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub(crate) struct CorrectnessCheck {
    pub(crate) surface: CorrectnessSurface,
    /// Required for `surface: base`.
    #[serde(default)]
    pub(crate) table: Option<String>,
    /// Required for `surface: derived`.
    #[serde(default)]
    pub(crate) pipeline: Option<String>,
    /// Required for `surface: target`.
    #[serde(default)]
    pub(crate) collection: Option<String>,
    #[serde(default)]
    pub(crate) present: Vec<FieldValue>,
    #[serde(default)]
    pub(crate) absent: Vec<FieldValue>,
    /// Field keys that must not appear as Managed keys in inspect output.
    #[serde(default)]
    pub(crate) field_absent: Vec<String>,
    #[serde(default)]
    pub(crate) contains: Vec<String>,
    #[serde(default)]
    pub(crate) not_contains: Vec<String>,
    /// TOTAL_AMOUNT-style values (integer or decimal string variants).
    #[serde(default)]
    pub(crate) amount_present: Vec<String>,
    #[serde(default)]
    pub(crate) amount_absent: Vec<String>,
    /// Base inspect `rows=N`.
    #[serde(default)]
    pub(crate) row_count: Option<u64>,
    /// Target inspect `documents: N`.
    #[serde(default)]
    pub(crate) document_count: Option<u64>,
}

impl Default for CorrectnessSurface {
    fn default() -> Self {
        Self::Base
    }
}

/// Cache key for one inspect fetch (dedupe when multiple expectations share a surface).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct InspectKey {
    pub(crate) surface: CorrectnessSurface,
    pub(crate) identity: String,
}

impl InspectKey {
    pub(crate) fn from_check(check: &CorrectnessCheck) -> Result<Self, String> {
        let identity = match check.surface {
            CorrectnessSurface::Base => check
                .table
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| "surface=base requires non-empty `table`".to_string())?
                .to_string(),
            CorrectnessSurface::Derived => check
                .pipeline
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| "surface=derived requires non-empty `pipeline`".to_string())?
                .to_string(),
            CorrectnessSurface::Target => check
                .collection
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| "surface=target requires non-empty `collection`".to_string())?
                .to_string(),
            CorrectnessSurface::Status => String::new(),
        };
        Ok(Self {
            surface: check.surface,
            identity,
        })
    }
}

/// Validate runnable correctness vocabulary for a recipe (#205).
///
/// Every check must be a complete runnable inspect expectation. When
/// `product_path` is set, fail fast with an explicit product_path-oriented
/// message if checks are missing — the assert step needs something to execute.
pub(crate) fn validate_runnable_correctness(
    path_display: &str,
    checks: &[CorrectnessCheck],
    has_product_path: bool,
) -> Result<(), CliError> {
    if checks.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} must declare checks.correctness{}",
            if has_product_path {
                " (product_path assert requires runnable checks; issue #205)"
            } else {
                ""
            }
        )));
    }
    for (idx, check) in checks.iter().enumerate() {
        validate_one_check(path_display, idx, check)?;
    }
    Ok(())
}

fn validate_one_check(
    path_display: &str,
    idx: usize,
    check: &CorrectnessCheck,
) -> Result<(), CliError> {
    InspectKey::from_check(check).map_err(|err| {
        CliError::Failed(format!(
            "Lab Scenario recipe {path_display} checks.correctness[{idx}]: {err}"
        ))
    })?;
    if !check_has_expectation(check) {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} checks.correctness[{idx}] must declare \
             at least one runnable expectation \
             (present/absent/field_absent/contains/not_contains/amount_*/row_count/document_count)"
        )));
    }
    if check.row_count.is_some() && check.surface != CorrectnessSurface::Base {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} checks.correctness[{idx}] \
             row_count is only valid for surface=base"
        )));
    }
    if check.document_count.is_some() && check.surface != CorrectnessSurface::Target {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} checks.correctness[{idx}] \
             document_count is only valid for surface=target"
        )));
    }
    Ok(())
}

fn check_has_expectation(check: &CorrectnessCheck) -> bool {
    !check.present.is_empty()
        || !check.absent.is_empty()
        || !check.field_absent.is_empty()
        || !check.contains.is_empty()
        || !check.not_contains.is_empty()
        || !check.amount_present.is_empty()
        || !check.amount_absent.is_empty()
        || check.row_count.is_some()
        || check.document_count.is_some()
}

/// Evaluate expectations against one inspect (or status) text blob.
pub(crate) fn expect_satisfied(inspect: &str, check: &CorrectnessCheck) -> Result<(), String> {
    for fv in &check.present {
        if !field_value_present(inspect, &fv.field, &fv.value) {
            return Err(format!(
                "expected present {}={} on {:?}/{}",
                fv.field,
                fv.value,
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for fv in &check.absent {
        if field_value_present(inspect, &fv.field, &fv.value) {
            return Err(format!(
                "expected absent {}={} on {:?}/{}",
                fv.field,
                fv.value,
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for field in &check.field_absent {
        if inspect_mentions_field_key(inspect, field) {
            return Err(format!(
                "expected field key `{field}` absent on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for needle in &check.contains {
        if !text_contains(inspect, needle, check.surface) {
            return Err(format!(
                "expected contains `{needle}` on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for needle in &check.not_contains {
        if text_contains(inspect, needle, check.surface) {
            return Err(format!(
                "expected not_contains `{needle}` on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for amount in &check.amount_present {
        if !inspect_mentions_amount(inspect, amount) {
            return Err(format!(
                "expected amount_present `{amount}` on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    for amount in &check.amount_absent {
        if inspect_mentions_amount(inspect, amount) {
            return Err(format!(
                "expected amount_absent `{amount}` on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    if let Some(expected) = check.row_count {
        let actual = parse_inspect_row_count(inspect).ok_or_else(|| {
            format!(
                "could not parse rows= on {:?}/{}",
                check.surface,
                surface_identity(check)
            )
        })?;
        if actual != expected {
            return Err(format!(
                "expected row_count={expected} got {actual} on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    if let Some(expected) = check.document_count {
        let actual = parse_target_document_count(inspect).ok_or_else(|| {
            format!(
                "could not parse documents: on {:?}/{}",
                check.surface,
                surface_identity(check)
            )
        })?;
        if actual != expected {
            return Err(format!(
                "expected document_count={expected} got {actual} on {:?}/{}",
                check.surface,
                surface_identity(check)
            ));
        }
    }
    Ok(())
}

fn surface_identity(check: &CorrectnessCheck) -> String {
    InspectKey::from_check(check)
        .map(|k| {
            if k.identity.is_empty() {
                "status".to_string()
            } else {
                k.identity
            }
        })
        .unwrap_or_else(|_| "?".to_string())
}

/// Evaluate already-fetched inspect blobs against a check list (settle loops).
pub(crate) fn evaluate_fetched(
    checks: &[CorrectnessCheck],
    fetched: &HashMap<InspectKey, String>,
) -> Result<(), String> {
    for check in checks {
        let key = InspectKey::from_check(check)?;
        let inspect = fetched.get(&key).ok_or_else(|| {
            format!(
                "missing inspect fetch for {:?}/{}",
                key.surface, key.identity
            )
        })?;
        expect_satisfied(inspect, check)?;
    }
    Ok(())
}

/// True when every recipe correctness check passes against `fetched`.
pub(crate) fn fetched_satisfies(
    checks: &[CorrectnessCheck],
    fetched: &HashMap<InspectKey, String>,
) -> bool {
    evaluate_fetched(checks, fetched).is_ok()
}

/// Fetch inspect/status output for one check key via the real product CLI.
pub(crate) async fn fetch_inspect(
    key: &InspectKey,
) -> Result<String, CliError> {
    let bin = lab_migraloop_bin();
    match key.surface {
        CorrectnessSurface::Base => {
            run_product_cli(
                &bin,
                &[
                    "base",
                    "--platform-store-url",
                    LAB_PLATFORM_STORE_URL,
                    "--table",
                    &key.identity,
                ],
            )
            .await
        }
        CorrectnessSurface::Derived => {
            run_product_cli(
                &bin,
                &[
                    "derived",
                    "--platform-store-url",
                    LAB_PLATFORM_STORE_URL,
                    "--pipeline",
                    &key.identity,
                ],
            )
            .await
        }
        CorrectnessSurface::Target => {
            run_product_cli(
                &bin,
                &[
                    "target",
                    "--platform-store-url",
                    LAB_PLATFORM_STORE_URL,
                    "--collection",
                    &key.identity,
                ],
            )
            .await
        }
        CorrectnessSurface::Status => {
            run_product_cli(
                &bin,
                &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
            )
            .await
        }
    }
}

/// Fetch all distinct inspect surfaces needed by `checks`.
pub(crate) async fn fetch_all(
    checks: &[CorrectnessCheck],
) -> Result<HashMap<InspectKey, String>, CliError> {
    let mut fetched = HashMap::new();
    for check in checks {
        let key = InspectKey::from_check(check).map_err(CliError::Failed)?;
        if fetched.contains_key(&key) {
            continue;
        }
        let text = fetch_inspect(&key).await?;
        fetched.insert(key, text);
    }
    Ok(fetched)
}

/// Execute recipe `checks.correctness` against live product inspect/status output.
pub(crate) async fn execute_recipe_correctness(
    _lab_dir: &Path,
    recipe: &ScenarioRecipe,
) -> Result<(), CliError> {
    let checks = &recipe.checks.correctness;
    if checks.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario `{}` product_path assert requires runnable checks.correctness (#205)",
            recipe.id
        )));
    }
    let fetched = fetch_all(checks).await?;
    evaluate_fetched(checks, &fetched).map_err(|err| {
        let mut detail = format!("correctness checks failed for `{}`: {err}", recipe.id);
        for (key, text) in &fetched {
            detail.push_str(&format!(
                "\n--- {:?}/{} ---\n{text}",
                key.surface, key.identity
            ));
        }
        CliError::Failed(detail)
    })
}

/// Format one check for recipe-interface printing.
pub(crate) fn format_check_summary(check: &CorrectnessCheck) -> String {
    let mut parts = Vec::new();
    parts.push(format!("surface={:?}", check.surface).to_ascii_lowercase());
    match check.surface {
        CorrectnessSurface::Base => {
            if let Some(t) = &check.table {
                parts.push(format!("table={t}"));
            }
        }
        CorrectnessSurface::Derived => {
            if let Some(p) = &check.pipeline {
                parts.push(format!("pipeline={p}"));
            }
        }
        CorrectnessSurface::Target => {
            if let Some(c) = &check.collection {
                parts.push(format!("collection={c}"));
            }
        }
        CorrectnessSurface::Status => {}
    }
    if !check.present.is_empty() {
        parts.push(format!("present={}", check.present.len()));
    }
    if !check.absent.is_empty() {
        parts.push(format!("absent={}", check.absent.len()));
    }
    if !check.field_absent.is_empty() {
        parts.push(format!("field_absent={}", check.field_absent.join(",")));
    }
    if !check.contains.is_empty() {
        parts.push(format!("contains={}", check.contains.len()));
    }
    if !check.not_contains.is_empty() {
        parts.push(format!("not_contains={}", check.not_contains.len()));
    }
    if !check.amount_present.is_empty() {
        parts.push(format!("amount_present={}", check.amount_present.join(",")));
    }
    if !check.amount_absent.is_empty() {
        parts.push(format!("amount_absent={}", check.amount_absent.join(",")));
    }
    if let Some(n) = check.row_count {
        parts.push(format!("row_count={n}"));
    }
    if let Some(n) = check.document_count {
        parts.push(format!("document_count={n}"));
    }
    parts.join(" ")
}

/// Present check with decimal-string tolerance (`15` also matches `15.0` / `15.00`).
fn field_value_present(inspect: &str, field: &str, value: &str) -> bool {
    if managed_field_present(inspect, field, value) {
        return true;
    }
    // Tolerate common inspect decimal renderings of whole/half values.
    if value.contains('.') {
        // `17.5` ↔ `17.50`
        if !value.ends_with('0') {
            if managed_field_present(inspect, field, &format!("{value}0")) {
                return true;
            }
        }
    } else if value.chars().all(|c| c.is_ascii_digit() || c == '-') {
        if managed_field_present(inspect, field, &format!("{value}.0"))
            || managed_field_present(inspect, field, &format!("{value}.00"))
        {
            return true;
        }
    }
    false
}

fn text_contains(inspect: &str, needle: &str, surface: CorrectnessSurface) -> bool {
    if surface == CorrectnessSurface::Status {
        inspect
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
    } else {
        inspect.contains(needle)
    }
}

/// Managed field presence in Base/Target/Derived JSON inspect output.
pub(crate) fn managed_field_present(inspect: &str, field: &str, value: &str) -> bool {
    let lower_field = field.to_ascii_lowercase();
    let patterns = [
        format!("\"{field}\": \"{value}\""),
        format!("\"{field}\":\"{value}\""),
        format!("\"{lower_field}\": \"{value}\""),
        format!("\"{lower_field}\":\"{value}\""),
    ];
    if patterns.iter().any(|p| inspect.contains(p.as_str())) {
        return true;
    }
    numeric_field_present(inspect, field, value)
        || numeric_field_present(inspect, &lower_field, value)
}

fn numeric_field_present(inspect: &str, field: &str, value: &str) -> bool {
    for spaced in [format!("\"{field}\": {value}"), format!("\"{field}\":{value}")] {
        let mut start = 0;
        while let Some(rel) = inspect[start..].find(&spaced) {
            let abs = start + rel;
            let after = abs + spaced.len();
            let boundary_ok = inspect
                .as_bytes()
                .get(after)
                .map(|b| !b.is_ascii_digit())
                .unwrap_or(true);
            if boundary_ok {
                return true;
            }
            start = abs + 1;
        }
    }
    false
}

/// Managed NAME field presence (Direct Pipeline convenience).
pub(crate) fn managed_name_present(inspect: &str, name: &str) -> bool {
    managed_field_present(inspect, "NAME", name)
}

/// Amount-like values may appear as integers or decimal strings in inspect output.
pub(crate) fn inspect_mentions_amount(inspect: &str, amount: &str) -> bool {
    managed_field_present(inspect, "TOTAL_AMOUNT", amount)
        || managed_field_present(inspect, "TOTAL_AMOUNT", &format!("{amount}.00"))
        || managed_field_present(inspect, "TOTAL_AMOUNT", &format!("{amount}.0"))
}

/// True when inspect output exposes a Managed field key (not merely a substring value).
pub(crate) fn inspect_mentions_field_key(inspect: &str, field: &str) -> bool {
    let lower = field.to_ascii_lowercase();
    inspect.contains(&format!("\"{field}\""))
        || inspect.contains(&format!("\"{lower}\""))
        || inspect.contains(&format!("'{field}'"))
        || inspect.contains(&format!("'{lower}'"))
}

/// True when inspect output exposes an EMAIL Managed field key.
pub(crate) fn inspect_mentions_email_field(inspect: &str) -> bool {
    inspect_mentions_field_key(inspect, "EMAIL")
}

pub(crate) fn parse_inspect_row_count(inspect: &str) -> Option<u64> {
    for line in inspect.lines() {
        if let Some(idx) = line.find("rows=") {
            let digits: String = line[idx + "rows=".len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !digits.is_empty() {
                return digits.parse().ok();
            }
        }
    }
    None
}

pub(crate) fn parse_target_document_count(inspect: &str) -> Option<u64> {
    for line in inspect.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("documents:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_name_check() -> CorrectnessCheck {
        CorrectnessCheck {
            surface: CorrectnessSurface::Base,
            table: Some("LAB_DP_CUSTOMERS".into()),
            present: vec![
                FieldValue {
                    field: "NAME".into(),
                    value: "Alicia".into(),
                },
                FieldValue {
                    field: "NAME".into(),
                    value: "Carol".into(),
                },
            ],
            absent: vec![FieldValue {
                field: "NAME".into(),
                value: "Bob".into(),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn expect_satisfied_passes_managed_present_absent() {
        let inspect = r#"{"ID": 1, "NAME": "Alicia"} {"ID": 3, "NAME": "Carol"}"#;
        expect_satisfied(inspect, &base_name_check()).expect("should pass");
    }

    #[test]
    fn expect_satisfied_fails_when_absent_value_present() {
        let inspect = r#"{"NAME": "Alicia"} {"NAME": "Bob"} {"NAME": "Carol"}"#;
        let err = expect_satisfied(inspect, &base_name_check()).expect_err("Bob must fail");
        assert!(err.contains("absent"), "err={err}");
    }

    #[test]
    fn expect_satisfied_field_absent_and_amounts() {
        let check = CorrectnessCheck {
            surface: CorrectnessSurface::Derived,
            pipeline: Some("lab-rp-customers".into()),
            present: vec![FieldValue {
                field: "NAME".into(),
                value: "Alicia".into(),
            }],
            field_absent: vec!["EMAIL".into()],
            amount_present: vec!["35".into()],
            amount_absent: vec!["30".into()],
            ..Default::default()
        };
        let inspect = r#"{"NAME": "Alicia", "TOTAL_AMOUNT": 35}"#;
        expect_satisfied(inspect, &check).expect("pass");
        let with_email = r#"{"NAME": "Alicia", "EMAIL": "x", "TOTAL_AMOUNT": 35}"#;
        let err = expect_satisfied(with_email, &check).expect_err("EMAIL");
        assert!(err.contains("EMAIL"), "err={err}");
    }

    #[test]
    fn expect_satisfied_row_and_document_counts() {
        let base = CorrectnessCheck {
            surface: CorrectnessSurface::Base,
            table: Some("LAB_BL_ITEMS".into()),
            row_count: Some(100_000),
            ..Default::default()
        };
        expect_satisfied("Base Dataset LAB_BL_ITEMS rows=100000", &base).expect("rows");
        let target = CorrectnessCheck {
            surface: CorrectnessSurface::Target,
            collection: Some("lab_bl_items".into()),
            document_count: Some(100_000),
            ..Default::default()
        };
        expect_satisfied("documents: 100000", &target).expect("docs");
    }

    #[test]
    fn validate_runnable_correctness_rejects_empty_expectation() {
        let checks = vec![CorrectnessCheck {
            surface: CorrectnessSurface::Base,
            table: Some("T".into()),
            ..Default::default()
        }];
        let err = validate_runnable_correctness("demo.yaml", &checks, true)
            .expect_err("empty expect");
        assert!(
            err.to_string().contains("runnable expectation"),
            "err={err}"
        );
    }

    #[test]
    fn validate_runnable_correctness_fails_fast_for_product_path_without_checks() {
        let err = validate_runnable_correctness("demo.yaml", &[], true)
            .expect_err("product_path needs checks");
        assert!(
            err.to_string().contains("product_path assert requires runnable checks"),
            "err={err}"
        );
    }

    #[test]
    fn validate_runnable_correctness_rejects_missing_table() {
        let checks = vec![CorrectnessCheck {
            surface: CorrectnessSurface::Base,
            present: vec![FieldValue {
                field: "NAME".into(),
                value: "A".into(),
            }],
            ..Default::default()
        }];
        let err = validate_runnable_correctness("demo.yaml", &checks, true).expect_err("table");
        assert!(err.to_string().contains("table"), "err={err}");
    }

    #[test]
    fn validate_runnable_correctness_accepts_status_contains() {
        let checks = vec![CorrectnessCheck {
            surface: CorrectnessSurface::Status,
            contains: vec!["Delivery Health: unhealthy".into()],
            ..Default::default()
        }];
        validate_runnable_correctness("demo.yaml", &checks, true).expect("ok");
    }

    #[test]
    fn evaluate_fetched_aggregates_surfaces() {
        let checks = vec![
            base_name_check(),
            CorrectnessCheck {
                surface: CorrectnessSurface::Target,
                collection: Some("lab_dp_customers".into()),
                present: vec![FieldValue {
                    field: "NAME".into(),
                    value: "Alicia".into(),
                }],
                ..Default::default()
            },
        ];
        let mut fetched = HashMap::new();
        fetched.insert(
            InspectKey {
                surface: CorrectnessSurface::Base,
                identity: "LAB_DP_CUSTOMERS".into(),
            },
            r#"{"NAME": "Alicia"} {"NAME": "Carol"}"#.into(),
        );
        fetched.insert(
            InspectKey {
                surface: CorrectnessSurface::Target,
                identity: "lab_dp_customers".into(),
            },
            r#"{"NAME": "Alicia"}"#.into(),
        );
        evaluate_fetched(&checks, &fetched).expect("ok");
        assert!(fetched_satisfies(&checks, &fetched));
    }
}
