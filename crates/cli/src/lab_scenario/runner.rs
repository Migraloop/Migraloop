//! Recipe-driven Lab Scenario runner (issues #157, #173 / ADR-0025).
//!
//! When `workload.product_path` is set, the runner executes shared product-path
//! steps from recipe data (prepare / apply / mutate / sync / assert). Scenario
//! hooks supply Namespace seeds, rare escapes, and correctness asserts.
//! Adapters return measured metrics + correctness; the runner evaluates
//! `recipe.yaml` thresholds as equal-weight fail axes and builds the report.

use crate::CliError;

use super::recipe::{ProductPathStepKind, ScenarioRecipe, ScenarioRecipeThresholds};

/// Measured metrics from a Scenario adapter (thresholds come from the recipe).
#[derive(Debug, Clone)]
pub(crate) struct ScenarioMetrics {
    pub settle_ms: Option<u128>,
    pub lag: Option<i32>,
    pub rows_per_s: Option<f64>,
    pub duration_ms: Option<u128>,
    pub rows_applied: u64,
    pub capture_path_note: String,
}

/// Outcome from a Scenario-specific adapter (seeds/workload/escapes + correctness).
#[derive(Debug, Clone)]
pub(crate) struct AdapterOutcome {
    pub correctness: bool,
    /// Correctness-oriented detail; runner may append threshold detail.
    pub detail: String,
    pub metrics: ScenarioMetrics,
}

/// Report built by the recipe-driven runner (correctness + threshold axes).
#[derive(Debug, Clone)]
pub(crate) struct ScenarioReport {
    pub correctness: bool,
    pub rows_applied: u64,
    pub detail: String,
    pub capture_path_note: String,
    /// Settle duration after concurrent Source changes (contention Scenario).
    pub settle_ms: Option<u128>,
    /// Scenario-defined max settle threshold that can fail the run (equal weight).
    pub max_settle_ms: Option<u128>,
    /// Observed Sync Health lag after catch-up (bulk-load).
    pub lag: Option<i32>,
    /// Scenario-defined max lag threshold (bulk-load).
    pub max_lag: Option<i32>,
    /// Scenario-defined minimum throughput threshold (bulk-load).
    pub min_rows_per_s: Option<f64>,
    /// Scenario-defined max duration threshold (bulk-load).
    pub max_duration_ms: Option<u128>,
    /// Measured throughput used for threshold comparison when set.
    pub measured_rows_per_s: Option<f64>,
    /// Measured duration used for threshold comparison when set.
    pub measured_duration_ms: Option<u128>,
    /// Operational threshold outcome; `true` when the Scenario defines none.
    pub thresholds_ok: bool,
}

/// Summarize recipe workload / checks / threshold axes for the runner interface.
pub(crate) fn recipe_interface_summary(recipe: &ScenarioRecipe) -> String {
    let mut axes = Vec::new();
    if recipe.thresholds.max_settle_ms.is_some() {
        axes.push("max_settle_ms");
    }
    if recipe.thresholds.max_lag.is_some() {
        axes.push("max_lag");
    }
    if recipe.thresholds.max_duration_ms.is_some() {
        axes.push("max_duration_ms");
    }
    if recipe.thresholds.min_rows_per_s.is_some() {
        axes.push("min_rows_per_s");
    }
    let axes = if axes.is_empty() {
        "none".to_string()
    } else {
        axes.join(",")
    };
    let product_path = match &recipe.workload.product_path {
        Some(pp) => format!("product_path.steps={}", pp.steps.len()),
        None => "product_path=none".to_string(),
    };
    format!(
        "workload.concurrency={} workload.steps={} {product_path} \
         checks.correctness={} thresholds=[{axes}]",
        recipe.workload.concurrency,
        recipe.workload.steps.len(),
        recipe.checks.correctness.len(),
    )
}

/// Ordered product-path plan from recipe data, when the Scenario opts in (#173).
pub(crate) fn product_path_plan(recipe: &ScenarioRecipe) -> Option<&[ProductPathStepKind]> {
    recipe
        .workload
        .product_path
        .as_ref()
        .map(|pp| pp.steps.as_slice())
}

/// Evaluate recipe.yaml thresholds against measured metrics (equal weight with correctness).
pub(crate) fn evaluate_recipe_thresholds(
    thresholds: &ScenarioRecipeThresholds,
    metrics: &ScenarioMetrics,
) -> (bool, String) {
    let mut failed = Vec::new();
    if let (Some(max_settle_ms), Some(settle_ms)) = (thresholds.max_settle_ms, metrics.settle_ms) {
        if settle_ms > max_settle_ms {
            failed.push(format!(
                "settle_ms={settle_ms} exceeded max_settle_ms={max_settle_ms}"
            ));
        }
    }
    if let (Some(max_lag), Some(lag)) = (thresholds.max_lag, metrics.lag) {
        if lag > max_lag {
            failed.push(format!("lag={lag} exceeded max_lag={max_lag}"));
        }
    }
    if let (Some(max_duration_ms), Some(duration_ms)) =
        (thresholds.max_duration_ms, metrics.duration_ms)
    {
        if duration_ms > max_duration_ms {
            failed.push(format!(
                "duration_ms={duration_ms} exceeded max_duration_ms={max_duration_ms}"
            ));
        }
    }
    if let (Some(min_rows_per_s), Some(rows_per_s)) =
        (thresholds.min_rows_per_s, metrics.rows_per_s)
    {
        if rows_per_s < min_rows_per_s {
            failed.push(format!(
                "rows_per_s={rows_per_s:.2} below min_rows_per_s={min_rows_per_s:.2}"
            ));
        }
    }
    if failed.is_empty() {
        (true, String::new())
    } else {
        (false, format!("threshold: {}", failed.join("; ")))
    }
}

/// Apply threshold evaluation and build a ScenarioReport from an adapter outcome.
pub(crate) fn report_from_adapter_outcome(
    recipe: &ScenarioRecipe,
    outcome: AdapterOutcome,
) -> ScenarioReport {
    let (thresholds_ok, threshold_detail) =
        evaluate_recipe_thresholds(&recipe.thresholds, &outcome.metrics);
    let mut detail = outcome.detail;
    if !thresholds_ok {
        if detail.is_empty() {
            detail = threshold_detail;
        } else if !threshold_detail.is_empty() && !detail.contains("threshold:") {
            detail = format!("{detail}; {threshold_detail}");
        } else if !threshold_detail.is_empty() && !detail.contains(&threshold_detail) {
            // Adapter already mentioned threshold context; keep both when distinct.
            detail = format!("{detail}; {threshold_detail}");
        }
    }
    ScenarioReport {
        correctness: outcome.correctness,
        rows_applied: outcome.metrics.rows_applied,
        detail,
        capture_path_note: outcome.metrics.capture_path_note,
        settle_ms: outcome.metrics.settle_ms,
        max_settle_ms: recipe.thresholds.max_settle_ms,
        lag: outcome.metrics.lag,
        max_lag: recipe.thresholds.max_lag,
        min_rows_per_s: recipe.thresholds.min_rows_per_s,
        max_duration_ms: recipe.thresholds.max_duration_ms,
        measured_rows_per_s: outcome.metrics.rows_per_s,
        measured_duration_ms: outcome.metrics.duration_ms,
        thresholds_ok,
    }
}

fn print_recipe_interface(recipe: &ScenarioRecipe) {
    println!("Lab Scenario: {}", recipe.id);
    println!(
        "Scenario Namespace: tables={} collections={} deployment={} pipelines={}",
        recipe.namespace.source_tables.join(","),
        recipe.namespace.target_collections.join(","),
        recipe.namespace.deployment,
        if recipe.namespace.pipelines.is_empty() {
            "(none)".to_string()
        } else {
            recipe.namespace.pipelines.join(",")
        }
    );
    println!("Lab Scenario recipe interface: {}", recipe_interface_summary(recipe));
    if !recipe.workload.steps.is_empty() {
        println!("Lab Scenario workload.steps:");
        for (idx, step) in recipe.workload.steps.iter().enumerate() {
            println!("  {}. {step}", idx + 1);
        }
    }
    if let Some(product_path) = &recipe.workload.product_path {
        println!("Lab Scenario workload.product_path:");
        for (idx, step) in product_path.steps.iter().enumerate() {
            let label = match step {
                ProductPathStepKind::PrepareNamespace => "prepare_namespace",
                ProductPathStepKind::ProductApply => "product_apply",
                ProductPathStepKind::Mutate => "mutate",
                ProductPathStepKind::ProductSync => "product_sync",
                ProductPathStepKind::Assert => "assert",
            };
            println!("  {}. {label}", idx + 1);
        }
        println!(
            "Lab Scenario product_path.apply: require_initial_load={} \
             require_delivery={} require_derived={}",
            product_path.apply.require_initial_load,
            product_path.apply.require_delivery,
            product_path.apply.require_derived
        );
        println!(
            "Lab Scenario product_path.sync: require_logminer={} allow_fail={}",
            product_path.sync.require_logminer, product_path.sync.allow_fail
        );
    }
    if !recipe.checks.correctness.is_empty() {
        println!("Lab Scenario checks.correctness:");
        for check in &recipe.checks.correctness {
            println!("  - {check}");
        }
    }
    let t = &recipe.thresholds;
    if t.max_settle_ms.is_some()
        || t.max_lag.is_some()
        || t.max_duration_ms.is_some()
        || t.min_rows_per_s.is_some()
    {
        print!("Lab Scenario thresholds:");
        if let Some(v) = t.max_settle_ms {
            print!(" max_settle_ms={v}");
        }
        if let Some(v) = t.max_lag {
            print!(" max_lag={v}");
        }
        if let Some(v) = t.max_duration_ms {
            print!(" max_duration_ms={v}");
        }
        if let Some(v) = t.min_rows_per_s {
            print!(" min_rows_per_s={v}");
        }
        println!();
    }
}

/// Run a Scenario through the recipe-driven path:
/// 1. Print id / namespace / workload / product_path / checks / thresholds from the recipe
/// 2. Call adapter (full adapt_* or shared product-path hooks)
/// 3. Evaluate thresholds from recipe against adapter metrics
/// 4. Build ScenarioReport
pub(crate) async fn run_recipe_driven<F, Fut>(
    recipe: &ScenarioRecipe,
    adapter: F,
) -> Result<ScenarioReport, CliError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<AdapterOutcome, CliError>>,
{
    print_recipe_interface(recipe);
    let outcome = adapter().await?;
    Ok(report_from_adapter_outcome(recipe, outcome))
}
