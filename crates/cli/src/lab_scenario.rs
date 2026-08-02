//! Lab Scenario catalog, run orchestration, and Namespace cleanup
//! (issues #60–#66, #63 / ADR-0025).
//!
//! Lab-specific machinery: catalog listing from on-disk Scenario recipes
//! (`lab/scenarios/<id>/recipe.yaml`), Scenario Namespace lifecycle
//! (prepare / re-run wipe / manual remove / opt-in auto-remove), Source workload
//! driving (including recipe-authored intra-Scenario concurrency and ~100k bulk
//! Source inserts), one-at-a-time lock, result reporting with equal-weight
//! correctness and operational metric thresholds (lag, throughput, duration),
//! and shipped-capability coverage visibility (`lab/scenarios/COVERAGE.md`).
//! Apply / Sync / inspect use the real product CLI path.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::lab::{
    ensure_fixture_ready_for_scenario, lab_migraloop_bin, mongosh_in_mongo, sqlplus_in_oracle,
    LAB_MONGO_PASSWORD_DEFAULT, LAB_MONGO_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT,
    LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_USER, LAB_PLATFORM_STORE_URL,
};
use crate::CliError;
use migraloop_platform_store::delete_deployment;

const LOCK_FILE_NAME: &str = ".migraloop-scenario.lock";

const DIRECT_PIPELINE_ID: &str = "direct-pipeline";
const DIRECT_PIPELINE_TABLE: &str = "LAB_DP_CUSTOMERS";
const DIRECT_PIPELINE_COLLECTION: &str = "lab_dp_customers";
const DIRECT_PIPELINE_DEPLOYMENT: &str = "lab-direct-pipeline";

const TRANSFORM_PIPELINE_ID: &str = "transform-pipeline";
const TRANSFORM_CUSTOMERS_TABLE: &str = "LAB_TP_CUSTOMERS";
const TRANSFORM_ORDERS_TABLE: &str = "LAB_TP_ORDERS";
const TRANSFORM_CUSTOMERS_COLLECTION: &str = "lab_tp_customers";
const TRANSFORM_ORDER_TOTALS_COLLECTION: &str = "lab_tp_order_totals";
const TRANSFORM_ORDER_TOTALS_PIPELINE: &str = "lab-tp-order-totals";
const TRANSFORM_PIPELINE_DEPLOYMENT: &str = "lab-transform-pipeline";

const CONCURRENT_SOURCE_WORKLOAD_ID: &str = "concurrent-source-workload";
const CONCURRENT_CUSTOMERS_TABLE: &str = "LAB_CW_CUSTOMERS";
const CONCURRENT_ORDERS_TABLE: &str = "LAB_CW_ORDERS";
const CONCURRENT_CUSTOMERS_COLLECTION: &str = "lab_cw_customers";
const CONCURRENT_ORDER_TOTALS_COLLECTION: &str = "lab_cw_order_totals";
const CONCURRENT_ORDER_TOTALS_PIPELINE: &str = "lab-cw-order-totals";
const CONCURRENT_SOURCE_WORKLOAD_DEPLOYMENT: &str = "lab-concurrent-source-workload";
/// Fail-able settle threshold after concurrent Source changes (US21 / US47).
/// Must stay aligned with `lab/scenarios/concurrent-source-workload/recipe.yaml`.
const CONCURRENT_MAX_SETTLE_MS: u128 = 300_000;
const CONCURRENT_SETTLE_POLL: Duration = Duration::from_secs(2);

const BULK_LOAD_ID: &str = "bulk-load";
const BULK_LOAD_TABLE: &str = "LAB_BL_ITEMS";
const BULK_LOAD_COLLECTION: &str = "lab_bl_items";
const BULK_LOAD_DEPLOYMENT: &str = "lab-bulk-load";

const RT_PROJECT_ID: &str = "rt-project";
const RT_PROJECT_TABLE: &str = "LAB_RP_CUSTOMERS";
const RT_PROJECT_COLLECTION: &str = "lab_rp_customers";
const RT_PROJECT_PIPELINE: &str = "lab-rp-customers";
const RT_PROJECT_DEPLOYMENT: &str = "lab-rt-project";

const RT_FILTER_ID: &str = "rt-filter";
const RT_FILTER_TABLE: &str = "LAB_RF_CUSTOMERS";
const RT_FILTER_COLLECTION: &str = "lab_rf_customers";
const RT_FILTER_PIPELINE: &str = "lab-rf-customers";
const RT_FILTER_DEPLOYMENT: &str = "lab-rt-filter";

/// Bulk-load thresholds must stay aligned with `lab/scenarios/bulk-load/recipe.yaml`.
/// Default bulk volume for the Lab Scenario (US17 — on the order of 100k).
const BULK_LOAD_ROW_COUNT: u64 = 100_000;
/// Fail-able Sync Health lag after Delivery catch-up (US21).
const BULK_LOAD_MAX_LAG: i32 = 0;
/// Fail-able end-to-end duration for bulk Initial Load + Delivery (US21 / US47).
const BULK_LOAD_MAX_DURATION_MS: u128 = 600_000;
/// Fail-able minimum throughput (rows/s) for the bulk load (US21).
const BULK_LOAD_MIN_ROWS_PER_S: f64 = 50.0;
/// Poll interval while waiting for Delivery/Health catch-up after bulk Initial Load (US47).
const BULK_LOAD_SETTLE_POLL: Duration = Duration::from_secs(2);

#[derive(Debug, Subcommand)]
pub enum ScenarioCommand {
    /// List selectable Lab Scenarios in the catalog
    List {
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
    },
    /// Run a Lab Scenario by id (one Scenario at a time)
    Run {
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `rt-project`, `rt-filter`, `bulk-load`)
        scenario: String,
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
        /// After a successful run, fully remove the Scenario Namespace (opt-in; default keep)
        #[arg(long)]
        auto_remove: bool,
    },
    /// Fully remove a Scenario Namespace without starting a run
    Remove {
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `rt-project`, `rt-filter`, `bulk-load`)
        scenario: String,
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
    },
}

pub async fn run_scenario_command(command: ScenarioCommand) -> Result<(), CliError> {
    match command {
        ScenarioCommand::List { lab_dir } => {
            // Validate --lab-dir early so operators get the same path contract as up/status/run.
            let compose = lab_dir.join("compose.yaml");
            if !compose.is_file() {
                return Err(CliError::Failed(format!(
                    "Lab compose file not found at {} \
                     (pass --lab-dir pointing at the repo `lab/` directory, or run from the repo root)",
                    compose.display()
                )));
            }
            scenario_list(&lab_dir)?;
            Ok(())
        }
        ScenarioCommand::Run {
            scenario,
            lab_dir,
            auto_remove,
        } => scenario_run(&scenario, &lab_dir, auto_remove).await,
        ScenarioCommand::Remove { scenario, lab_dir } => {
            scenario_remove(&scenario, &lab_dir).await
        }
    }
}

/// Registered Scenario runners (feature-time implementations in this module).
/// Selectable catalog = these ids ∩ complete on-disk recipe packages.
fn registered_scenario_ids() -> &'static [&'static str] {
    &[
        DIRECT_PIPELINE_ID,
        TRANSFORM_PIPELINE_ID,
        RT_PROJECT_ID,
        RT_FILTER_ID,
        CONCURRENT_SOURCE_WORKLOAD_ID,
        BULK_LOAD_ID,
    ]
}

/// Shipped first-class capabilities that must have a selectable Scenario before
/// the Lab may claim catalog-complete (ADR-0025 / issue #66). Keep aligned with
/// `lab/scenarios/COVERAGE.md`.
fn shipped_capability_scenario_requirements() -> &'static [(&'static str, &'static str)] {
    &[
        (DIRECT_PIPELINE_ID, "Direct Pipeline Initial Load + insert/update/delete"),
        (
            TRANSFORM_PIPELINE_ID,
            "multi-table Transform Pipeline (groupBy/sum)",
        ),
        (RT_PROJECT_ID, "Rich Transform project"),
        (RT_FILTER_ID, "Rich Transform filter"),
        (
            CONCURRENT_SOURCE_WORKLOAD_ID,
            "intra-Scenario concurrent Source workload",
        ),
        (BULK_LOAD_ID, "bulk load (~100k) with metric thresholds"),
    ]
}

/// Gaps among shipped capabilities that lack a selectable Scenario in `catalog`.
fn shipped_capability_coverage_gaps(catalog_ids: &[String]) -> Vec<&'static str> {
    shipped_capability_scenario_requirements()
        .iter()
        .filter(|(id, _)| !catalog_ids.iter().any(|listed| listed == *id))
        .map(|(_, label)| *label)
        .collect()
}

#[derive(Debug, Deserialize)]
struct ScenarioRecipe {
    id: String,
    summary: String,
    namespace: ScenarioRecipeNamespace,
    #[serde(default = "default_deployment_config")]
    deployment_config: String,
    workload: ScenarioRecipeWorkload,
    checks: ScenarioRecipeChecks,
    /// Documented fail-able metric axes (equal weight with correctness). Kept on
    /// the recipe seam for authoring; runners keep matching constants today.
    #[serde(default)]
    #[allow(dead_code)]
    thresholds: ScenarioRecipeThresholds,
}

fn default_deployment_config() -> String {
    "deployment.yaml".to_string()
}

#[derive(Debug, Deserialize)]
struct ScenarioRecipeNamespace {
    source_tables: Vec<String>,
    target_collections: Vec<String>,
    deployment: String,
    /// Pipeline identities inside the Scenario Namespace (authoring metadata).
    #[serde(default)]
    #[allow(dead_code)]
    pipelines: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ScenarioRecipeWorkload {
    concurrency: String,
    #[serde(default)]
    steps: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ScenarioRecipeChecks {
    #[serde(default)]
    correctness: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[allow(dead_code)]
struct ScenarioRecipeThresholds {
    max_settle_ms: Option<u128>,
    max_lag: Option<i32>,
    max_duration_ms: Option<u128>,
    min_rows_per_s: Option<f64>,
}

fn load_recipe(path: &Path) -> Result<ScenarioRecipe, CliError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        CliError::Failed(format!(
            "failed to read Lab Scenario recipe {}: {err}",
            path.display()
        ))
    })?;
    let recipe: ScenarioRecipe = serde_yaml::from_str(&raw).map_err(|err| {
        CliError::Failed(format!(
            "failed to parse Lab Scenario recipe {}: {err}",
            path.display()
        ))
    })?;
    if recipe.id.is_empty() || recipe.summary.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must set non-empty `id` and `summary`",
            path.display()
        )));
    }
    if recipe.namespace.source_tables.is_empty()
        || recipe.namespace.target_collections.is_empty()
        || recipe.namespace.deployment.is_empty()
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare namespace.source_tables, \
             target_collections, and deployment",
            path.display()
        )));
    }
    match recipe.workload.concurrency.as_str() {
        "serial" | "parallel" => {}
        other => {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {} workload.concurrency must be \
                 `serial` or `parallel` (got `{other}`)",
                path.display()
            )));
        }
    }
    if recipe.workload.steps.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare workload.steps",
            path.display()
        )));
    }
    if recipe.checks.correctness.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare checks.correctness",
            path.display()
        )));
    }
    Ok(recipe)
}

/// Selectable catalog: registered runners that have `recipe.yaml` + deployment
/// config under `lab_dir/scenarios/<id>/`. Summaries come from the recipe file.
fn load_selectable_catalog(lab_dir: &Path) -> Result<Vec<(String, String)>, CliError> {
    let mut entries = Vec::new();
    for id in registered_scenario_ids() {
        let scenario_dir = lab_dir.join("scenarios").join(id);
        let recipe_path = scenario_dir.join("recipe.yaml");
        if !recipe_path.is_file() {
            // Not selectable yet — recipe package incomplete (feature-time authoring in progress).
            continue;
        }
        let recipe = load_recipe(&recipe_path)?;
        if recipe.id != *id {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {} has id `{}` but lives under scenarios/{id}/ \
                 (directory name must match recipe id)",
                recipe_path.display(),
                recipe.id
            )));
        }
        let deployment_path = scenario_dir.join(&recipe.deployment_config);
        if !deployment_path.is_file() {
            return Err(CliError::Failed(format!(
                "Lab Scenario `{id}` recipe references missing deployment config {} \
                 (expected under lab/scenarios/{id}/)",
                deployment_path.display()
            )));
        }
        entries.push((recipe.id, recipe.summary));
    }
    Ok(entries)
}

fn scenario_list(lab_dir: &Path) -> Result<(), CliError> {
    let catalog = load_selectable_catalog(lab_dir)?;
    println!("Lab Scenarios:");
    if catalog.is_empty() {
        println!(
            "  (none — add lab/scenarios/<id>/recipe.yaml + deployment.yaml \
and register a runner; see lab/scenarios/README.md)"
        );
    } else {
        for (id, summary) in &catalog {
            println!("  {id}  {summary}");
        }
    }
    let ids: Vec<String> = catalog.iter().map(|(id, _)| id.clone()).collect();
    let gaps = shipped_capability_coverage_gaps(&ids);
    if gaps.is_empty() {
        println!(
            "Catalog coverage: complete for shipped capabilities \
(see lab/scenarios/COVERAGE.md). Not a CI suite — pick Scenarios individually (ADR-0025)."
        );
    } else {
        println!(
            "Catalog coverage: incomplete for shipped capabilities \
(see lab/scenarios/COVERAGE.md):"
        );
        for gap in gaps {
            println!("  missing: {gap}");
        }
        println!(
            "Do not claim catalog-complete until every shipped capability has a \
selectable Scenario (ADR-0025)."
        );
    }
    Ok(())
}

fn unknown_or_incomplete_scenario_error(scenario: &str, lab_dir: &Path) -> CliError {
    if registered_scenario_ids().contains(&scenario) {
        CliError::Failed(format!(
            "Lab Scenario `{scenario}` is not selectable under {}: \
             add scenarios/{scenario}/recipe.yaml and deployment.yaml \
             (see lab/scenarios/README.md). Run `migraloop lab scenario list`.",
            lab_dir.display()
        ))
    } else {
        CliError::Failed(format!(
            "Unknown Lab Scenario `{scenario}`. Run `migraloop lab scenario list`."
        ))
    }
}

async fn scenario_run(
    scenario: &str,
    lab_dir: &Path,
    auto_remove: bool,
) -> Result<(), CliError> {
    let catalog = load_selectable_catalog(lab_dir)?;
    let entry = catalog
        .iter()
        .find(|(id, _)| id == scenario)
        .cloned()
        .ok_or_else(|| unknown_or_incomplete_scenario_error(scenario, lab_dir))?;

    // One-at-a-time check before Fixture probes so CI can assert rejection without Docker.
    let lock_path = lab_dir.join(LOCK_FILE_NAME);
    if let Some(existing) = read_active_lock(&lock_path)? {
        return Err(CliError::Failed(format!(
            "Lab Scenario run rejected: another Scenario is active \
             (`{}` since unix {})",
            existing.scenario, existing.started_at_unix
        )));
    }

    // Lab-only outcome probe: exercise equal-weight fail axes through the real CLI
    // report/exit path without Docker (issue #63 / PRD #55 metrics tests).
    if let Ok(probe) = std::env::var("MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE") {
        return emit_scenario_outcome_probe(&entry.0, &probe);
    }

    ensure_fixture_ready_for_scenario(lab_dir).await?;

    let lock = ScenarioLock::acquire(&lock_path, scenario)?;
    let started = Instant::now();
    // Catalog membership already validated; dispatch by id.
    let result = match scenario {
        DIRECT_PIPELINE_ID => run_direct_pipeline(lab_dir).await,
        TRANSFORM_PIPELINE_ID => run_transform_pipeline(lab_dir).await,
        RT_PROJECT_ID => run_rt_project(lab_dir).await,
        RT_FILTER_ID => run_rt_filter(lab_dir).await,
        CONCURRENT_SOURCE_WORKLOAD_ID => run_concurrent_source_workload(lab_dir).await,
        BULK_LOAD_ID => run_bulk_load(lab_dir).await,
        _ => Err(CliError::Failed(format!(
            "Lab Scenario `{scenario}` is listed but has no runner"
        ))),
    };
    let duration = started.elapsed();

    match result {
        Ok(report) => {
            let passed = report.correctness && report.thresholds_ok;
            let mut namespace_removed = false;
            if auto_remove && passed {
                // Opt-in cleanup after success only — failures keep Namespace for debug (US35).
                remove_scenario_namespace(scenario, lab_dir).await?;
                namespace_removed = true;
            }
            drop(lock);
            print_scenario_report(&entry.0, true, duration, &report, namespace_removed);
            if passed {
                Ok(())
            } else {
                // US36: name correctness vs threshold (or both) — equal-weight fail axes.
                let kind = scenario_failure_kind(report.correctness, report.thresholds_ok);
                Err(CliError::Failed(format!(
                    "Lab Scenario {kind} failed: {}",
                    report.detail
                )))
            }
        }
        Err(err) => {
            drop(lock);
            let report = ScenarioReport {
                correctness: false,
                rows_applied: 0,
                detail: err.to_string(),
                capture_path_note: String::new(),
                settle_ms: None,
                max_settle_ms: None,
                lag: None,
                max_lag: None,
                min_rows_per_s: None,
                max_duration_ms: None,
                measured_rows_per_s: None,
                measured_duration_ms: None,
                thresholds_ok: true,
            };
            print_scenario_report(&entry.0, false, duration, &report, false);
            Err(err)
        }
    }
}

async fn scenario_remove(scenario: &str, lab_dir: &Path) -> Result<(), CliError> {
    let catalog = load_selectable_catalog(lab_dir)?;
    catalog
        .iter()
        .find(|(id, _)| id == scenario)
        .ok_or_else(|| unknown_or_incomplete_scenario_error(scenario, lab_dir))?;

    let lock_path = lab_dir.join(LOCK_FILE_NAME);
    if let Some(existing) = read_active_lock(&lock_path)? {
        return Err(CliError::Failed(format!(
            "Lab Scenario remove rejected: another Scenario is active \
             (`{}` since unix {})",
            existing.scenario, existing.started_at_unix
        )));
    }

    ensure_fixture_ready_for_scenario(lab_dir).await?;

    let lock = ScenarioLock::acquire(&lock_path, scenario)?;
    remove_scenario_namespace(scenario, lab_dir).await?;
    drop(lock);

    println!("Lab Scenario Namespace removed: {scenario}");
    Ok(())
}

async fn remove_scenario_namespace(scenario: &str, lab_dir: &Path) -> Result<(), CliError> {
    match scenario {
        DIRECT_PIPELINE_ID => remove_direct_pipeline_namespace(lab_dir).await,
        TRANSFORM_PIPELINE_ID => remove_transform_pipeline_namespace(lab_dir).await,
        RT_PROJECT_ID => remove_rt_project_namespace(lab_dir).await,
        RT_FILTER_ID => remove_rt_filter_namespace(lab_dir).await,
        CONCURRENT_SOURCE_WORKLOAD_ID => remove_concurrent_source_namespace(lab_dir).await,
        BULK_LOAD_ID => remove_bulk_load_namespace(lab_dir).await,
        _ => Err(CliError::Failed(format!(
            "Lab Scenario `{scenario}` is listed but has no Namespace remove path"
        ))),
    }
}

struct ScenarioReport {
    correctness: bool,
    rows_applied: u64,
    detail: String,
    capture_path_note: String,
    /// Settle duration after concurrent Source changes (contention Scenario).
    settle_ms: Option<u128>,
    /// Scenario-defined max settle threshold that can fail the run (equal weight).
    max_settle_ms: Option<u128>,
    /// Observed Sync Health lag after catch-up (bulk-load).
    lag: Option<i32>,
    /// Scenario-defined max lag threshold (bulk-load).
    max_lag: Option<i32>,
    /// Scenario-defined minimum throughput threshold (bulk-load).
    min_rows_per_s: Option<f64>,
    /// Scenario-defined max duration threshold (bulk-load).
    max_duration_ms: Option<u128>,
    /// Measured throughput used for threshold comparison when set.
    measured_rows_per_s: Option<f64>,
    /// Measured duration used for threshold comparison when set.
    measured_duration_ms: Option<u128>,
    /// Operational threshold outcome; `true` when the Scenario defines none.
    thresholds_ok: bool,
}

fn scenario_failure_kind(correctness: bool, thresholds_ok: bool) -> &'static str {
    match (correctness, thresholds_ok) {
        (false, false) => "correctness and threshold",
        (false, true) => "correctness",
        (true, false) => "threshold",
        (true, true) => "scenario",
    }
}

/// Emit a synthetic bulk-load Scenario outcome through the same report + exit
/// contract as a real run (CLI-seam metrics/correctness fail-axis verification).
fn emit_scenario_outcome_probe(scenario: &str, probe: &str) -> Result<(), CliError> {
    if scenario != BULK_LOAD_ID {
        return Err(CliError::Failed(format!(
            "MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE is only supported for `{BULK_LOAD_ID}` \
             (got `{scenario}`)"
        )));
    }
    let report = match probe {
        "threshold-fail" => {
            let sample = BulkLoadMetricSample {
                lag: 0,
                max_lag: BULK_LOAD_MAX_LAG,
                duration_ms: BULK_LOAD_MAX_DURATION_MS + 1,
                max_duration_ms: BULK_LOAD_MAX_DURATION_MS,
                rows_per_s: BULK_LOAD_MIN_ROWS_PER_S / 2.0,
                min_rows_per_s: BULK_LOAD_MIN_ROWS_PER_S,
            };
            let (_, detail) = evaluate_bulk_load_thresholds(&sample);
            ScenarioReport {
                correctness: true,
                rows_applied: BULK_LOAD_ROW_COUNT,
                detail,
                capture_path_note: "Initial Load".to_string(),
                settle_ms: None,
                max_settle_ms: None,
                lag: Some(sample.lag),
                max_lag: Some(sample.max_lag),
                min_rows_per_s: Some(sample.min_rows_per_s),
                max_duration_ms: Some(sample.max_duration_ms),
                measured_rows_per_s: Some(sample.rows_per_s),
                measured_duration_ms: Some(sample.duration_ms),
                thresholds_ok: false,
            }
        }
        "correctness-fail" => ScenarioReport {
            correctness: false,
            rows_applied: BULK_LOAD_ROW_COUNT - 1,
            detail: format!(
                "correctness: expected rows={BULK_LOAD_ROW_COUNT} \
base_rows={} target_rows={}",
                BULK_LOAD_ROW_COUNT - 1,
                BULK_LOAD_ROW_COUNT - 1
            ),
            capture_path_note: "Initial Load".to_string(),
            settle_ms: None,
            max_settle_ms: None,
            lag: Some(0),
            max_lag: Some(BULK_LOAD_MAX_LAG),
            min_rows_per_s: Some(BULK_LOAD_MIN_ROWS_PER_S),
            max_duration_ms: Some(BULK_LOAD_MAX_DURATION_MS),
            measured_rows_per_s: Some(800.0),
            measured_duration_ms: Some(120_000),
            thresholds_ok: true,
        },
        other => {
            return Err(CliError::Failed(format!(
                "Unknown MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE `{other}` \
                 (expected `threshold-fail` or `correctness-fail`)"
            )));
        }
    };

    let duration = Duration::from_millis(report.measured_duration_ms.unwrap_or(0) as u64);
    print_scenario_report(scenario, true, duration, &report, false);
    let kind = scenario_failure_kind(report.correctness, report.thresholds_ok);
    Err(CliError::Failed(format!(
        "Lab Scenario {kind} failed: {}",
        report.detail
    )))
}

fn report_defines_thresholds(report: &ScenarioReport) -> bool {
    report.max_settle_ms.is_some()
        || report.max_lag.is_some()
        || report.min_rows_per_s.is_some()
        || report.max_duration_ms.is_some()
}

/// Bulk-load metric sample for equal-weight threshold evaluation (US21 / US36).
#[derive(Debug, Clone, PartialEq)]
struct BulkLoadMetricSample {
    lag: i32,
    max_lag: i32,
    duration_ms: u128,
    max_duration_ms: u128,
    rows_per_s: f64,
    min_rows_per_s: f64,
}

/// Evaluate Scenario-defined lag / duration / throughput thresholds independently
/// of row-level correctness.
fn evaluate_bulk_load_thresholds(sample: &BulkLoadMetricSample) -> (bool, String) {
    let mut failed = Vec::new();
    if sample.lag > sample.max_lag {
        failed.push(format!(
            "lag={} exceeded max_lag={}",
            sample.lag, sample.max_lag
        ));
    }
    if sample.duration_ms > sample.max_duration_ms {
        failed.push(format!(
            "duration_ms={} exceeded max_duration_ms={}",
            sample.duration_ms, sample.max_duration_ms
        ));
    }
    if sample.rows_per_s < sample.min_rows_per_s {
        failed.push(format!(
            "rows_per_s={:.2} below min_rows_per_s={:.2}",
            sample.rows_per_s, sample.min_rows_per_s
        ));
    }
    if failed.is_empty() {
        (true, String::new())
    } else {
        (false, format!("threshold: {}", failed.join("; ")))
    }
}

fn print_scenario_report(
    scenario: &str,
    overall_pass: bool,
    duration: Duration,
    report: &ScenarioReport,
    namespace_removed: bool,
) {
    print!(
        "{}",
        format_scenario_report(scenario, overall_pass, duration, report, namespace_removed)
    );
}

fn format_scenario_report(
    scenario: &str,
    overall_pass: bool,
    duration: Duration,
    report: &ScenarioReport,
    namespace_removed: bool,
) -> String {
    let duration_ms = report
        .measured_duration_ms
        .unwrap_or_else(|| duration.as_millis());
    let rows_per_s = report.measured_rows_per_s.unwrap_or_else(|| {
        if duration.as_secs_f64() > 0.0 {
            report.rows_applied as f64 / duration.as_secs_f64()
        } else {
            report.rows_applied as f64
        }
    });
    let outcome = if overall_pass && report.correctness && report.thresholds_ok {
        "PASS"
    } else {
        "FAIL"
    };
    let mut out = String::new();
    out.push('\n');
    out.push_str(&format!("Lab Scenario: {outcome}\n"));
    out.push_str(&format!("  scenario={scenario}\n"));
    out.push_str(&format!(
        "  correctness={}\n",
        if report.correctness { "pass" } else { "fail" }
    ));
    if report_defines_thresholds(report) {
        out.push_str(&format!(
            "  thresholds={}\n",
            if report.thresholds_ok { "pass" } else { "fail" }
        ));
        if let Some(settle_ms) = report.settle_ms {
            out.push_str(&format!("  settle_ms={settle_ms}\n"));
        }
        if let Some(max_settle_ms) = report.max_settle_ms {
            out.push_str(&format!("  max_settle_ms={max_settle_ms}\n"));
        }
        if let Some(lag) = report.lag {
            out.push_str(&format!("  lag={lag}\n"));
        }
        if let Some(max_lag) = report.max_lag {
            out.push_str(&format!("  max_lag={max_lag}\n"));
        }
        if let Some(min_rows_per_s) = report.min_rows_per_s {
            out.push_str(&format!("  min_rows_per_s={min_rows_per_s:.2}\n"));
        }
        if let Some(max_duration_ms) = report.max_duration_ms {
            out.push_str(&format!("  max_duration_ms={max_duration_ms}\n"));
        }
    }
    out.push_str(&format!("  duration_ms={duration_ms}\n"));
    out.push_str(&format!("  rows_applied={}\n", report.rows_applied));
    out.push_str(&format!("  rows_per_s={rows_per_s:.2}\n"));
    if !report.capture_path_note.is_empty() {
        out.push_str(&format!("  capture={}\n", report.capture_path_note));
    }
    if !report.detail.is_empty() && outcome == "FAIL" {
        out.push_str(&format!("  detail={}\n", report.detail));
    }
    if namespace_removed {
        out.push_str("  namespace=removed (--auto-remove)\n");
    } else {
        out.push_str(
            "  namespace=left in place (inspect with `migraloop base` / `migraloop derived` / `migraloop target`)\n",
        );
    }
    out
}

async fn run_direct_pipeline(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {DIRECT_PIPELINE_ID}");
    println!("Scenario Namespace: table={DIRECT_PIPELINE_TABLE} \
collection={DIRECT_PIPELINE_COLLECTION} deployment={DIRECT_PIPELINE_DEPLOYMENT}");

    prepare_direct_pipeline_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, DIRECT_PIPELINE_ID)?;
    let bin = lab_migraloop_bin();

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    let base_after_apply = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            DIRECT_PIPELINE_TABLE,
        ],
    )
    .await?;
    if !(base_after_apply.contains("Alice") && base_after_apply.contains("Bob")) {
        return Err(CliError::Failed(format!(
            "Initial Load Base check failed (expected Alice and Bob):\n{base_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source insert/update/delete...");
    mutate_direct_pipeline_source(lab_dir).await?;

    println!("Lab Scenario: sync Incremental Capture + Delivery via real product path...");
    let sync_out = run_product_cli(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };

    let base_after = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            DIRECT_PIPELINE_TABLE,
        ],
    )
    .await?;
    let target_after = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            DIRECT_PIPELINE_COLLECTION,
        ],
    )
    .await?;

    let base_ok = managed_name_present(&base_after, "Alicia")
        && managed_name_present(&base_after, "Carol")
        && !managed_name_present(&base_after, "Bob");
    let target_ok = managed_name_present(&target_after, "Alicia")
        && managed_name_present(&target_after, "Carol")
        && !managed_name_present(&target_after, "Bob");

    // Throughput signal from product apply/sync Delivery lines (not a hand-counted recipe).
    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(base_ok && target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after insert/update/delete.\nBase:\n{base_after}\nTarget:\n{target_after}"
        )));
    }

    println!("Lab Scenario: correctness checks passed (Base + Target Managed outcomes)");
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: None,
        max_lag: None,
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

/// Managed NAME field presence in Base/Target JSON inspect output.
fn managed_name_present(inspect: &str, name: &str) -> bool {
    managed_field_present(inspect, "NAME", name)
}

/// Sum Delivery document ops reported on the product CLI path.
fn count_delivery_ops(product_out: &str) -> u64 {
    let mut total = 0u64;
    for line in product_out.lines() {
        if let Some(n) = parse_parenthetical_documents(line) {
            total += n;
            continue;
        }
        total += parse_labeled_u64(line, "upserts=");
        total += parse_labeled_u64(line, "deletes=");
    }
    total
}

fn parse_parenthetical_documents(line: &str) -> Option<u64> {
    // Delivery complete: Pipeline … (N documents)
    let start = line.find('(')?;
    let rest = &line[start + 1..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() || !rest[digits.len()..].starts_with(" documents)") {
        return None;
    }
    digits.parse().ok()
}

fn parse_labeled_u64(line: &str, label: &str) -> u64 {
    let Some(idx) = line.find(label) else {
        return 0;
    };
    line[idx + label.len()..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

fn deployment_config_path(lab_dir: &Path, scenario_id: &str) -> Result<PathBuf, CliError> {
    let path = lab_dir
        .join("scenarios")
        .join(scenario_id)
        .join("deployment.yaml");
    if !path.is_file() {
        return Err(CliError::Failed(format!(
            "Lab Scenario deployment config not found at {} \
             (expected under the repo `lab/scenarios/{scenario_id}/` directory)",
            path.display()
        )));
    }
    Ok(path)
}

async fn run_transform_pipeline(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {TRANSFORM_PIPELINE_ID}");
    println!(
        "Scenario Namespace: tables={TRANSFORM_CUSTOMERS_TABLE},{TRANSFORM_ORDERS_TABLE} \
collections={TRANSFORM_CUSTOMERS_COLLECTION},{TRANSFORM_ORDER_TOTALS_COLLECTION} \
deployment={TRANSFORM_PIPELINE_DEPLOYMENT}"
    );

    prepare_transform_pipeline_namespace(lab_dir).await?;
    println!(
        "Lab Scenario: Scenario Namespace prepared (multi-table schema + seed + supplemental logging)"
    );

    let config_path = deployment_config_path(lab_dir, TRANSFORM_PIPELINE_ID)?;
    let bin = lab_migraloop_bin();

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.to_ascii_lowercase().contains("derived")
        || apply_out.contains(TRANSFORM_ORDER_TOTALS_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
        )));
    }

    let customers_base = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            TRANSFORM_CUSTOMERS_TABLE,
        ],
    )
    .await?;
    let orders_base = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            TRANSFORM_ORDERS_TABLE,
        ],
    )
    .await?;
    if !(managed_field_present(&customers_base, "NAME", "Alice")
        && managed_field_present(&customers_base, "NAME", "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load customers Base check failed (expected Alice and Bob):\n{customers_base}"
        )));
    }
    if !(managed_field_present(&orders_base, "AMOUNT", "10")
        && managed_field_present(&orders_base, "AMOUNT", "20")
        && managed_field_present(&orders_base, "AMOUNT", "5"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load orders Base check failed (expected amounts 10/20/5):\n{orders_base}"
        )));
    }

    let derived_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            TRANSFORM_ORDER_TOTALS_PIPELINE,
        ],
    )
    .await?;
    // Seed totals: customer 1 = 10+20=30, customer 2 = 5.
    if !(inspect_mentions_amount(&derived_after_apply, "30")
        && inspect_mentions_amount(&derived_after_apply, "5"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load Derived check failed (expected totals 30 and 5):\n{derived_after_apply}"
        )));
    }

    println!("Lab Scenario: driving multi-table Source insert/update/delete...");
    mutate_transform_pipeline_source(lab_dir).await?;

    println!("Lab Scenario: sync Incremental Capture + Delivery via real product path...");
    let sync_out = run_product_cli(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };

    let customers_base_after = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            TRANSFORM_CUSTOMERS_TABLE,
        ],
    )
    .await?;
    let derived_after = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            TRANSFORM_ORDER_TOTALS_PIPELINE,
        ],
    )
    .await?;
    let customers_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            TRANSFORM_CUSTOMERS_COLLECTION,
        ],
    )
    .await?;
    let totals_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            TRANSFORM_ORDER_TOTALS_COLLECTION,
        ],
    )
    .await?;

    // Customers Direct: Alicia + Carol present, Bob deleted.
    let customers_base_ok = managed_field_present(&customers_base_after, "NAME", "Alicia")
        && managed_field_present(&customers_base_after, "NAME", "Carol")
        && !managed_field_present(&customers_base_after, "NAME", "Bob");
    let customers_target_ok = managed_field_present(&customers_target, "NAME", "Alicia")
        && managed_field_present(&customers_target, "NAME", "Carol")
        && !managed_field_present(&customers_target, "NAME", "Bob");
    // Orders Transform after mutate: cust1=20+15=35, cust2=50 (order 1 deleted, order 3 updated).
    let derived_ok = inspect_mentions_amount(&derived_after, "35")
        && inspect_mentions_amount(&derived_after, "50")
        && !inspect_mentions_amount(&derived_after, "30");
    let totals_target_ok = inspect_mentions_amount(&totals_target, "35")
        && inspect_mentions_amount(&totals_target, "50")
        && !inspect_mentions_amount(&totals_target, "30");

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(customers_base_ok && customers_target_ok && derived_ok && totals_target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after multi-table insert/update/delete.\n\
Customers Base:\n{customers_base_after}\nCustomers Target:\n{customers_target}\n\
Derived:\n{derived_after}\nOrder totals Target:\n{totals_target}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (Base + Derived + Target Managed outcomes)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: None,
        max_lag: None,
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

/// Managed field presence in Base/Target/Derived JSON inspect output.
fn managed_field_present(inspect: &str, field: &str, value: &str) -> bool {
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
    // Numeric JSON without quotes — require a non-digit boundary so "5" ≠ "50".
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

/// Amount-like values may appear as integers or decimal strings in inspect output.
fn inspect_mentions_amount(inspect: &str, amount: &str) -> bool {
    managed_field_present(inspect, "TOTAL_AMOUNT", amount)
        || managed_field_present(inspect, "TOTAL_AMOUNT", &format!("{amount}.00"))
        || managed_field_present(inspect, "TOTAL_AMOUNT", &format!("{amount}.0"))
}

/// Fully remove Transform Pipeline Scenario Namespace (Source tables, Target
/// collections, Platform Store Deployment + cascaded Bases/Derived/Pipelines).
/// Idempotent.
async fn remove_transform_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (tables={TRANSFORM_CUSTOMERS_TABLE},{TRANSFORM_ORDERS_TABLE}, \
          collections={TRANSFORM_CUSTOMERS_COLLECTION},{TRANSFORM_ORDER_TOTALS_COLLECTION}, \
          deployment={TRANSFORM_PIPELINE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {TRANSFORM_ORDERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {TRANSFORM_CUSTOMERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace tables \
                 {TRANSFORM_CUSTOMERS_TABLE}/{TRANSFORM_ORDERS_TABLE}:\n{err}"
            ))
        })?;

    for collection in [
        TRANSFORM_CUSTOMERS_COLLECTION,
        TRANSFORM_ORDER_TOTALS_COLLECTION,
    ] {
        let js = format!("db.getCollection('{collection}').drop()");
        mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Mongo Scenario Namespace collection {collection}:\n{err}"
            ))
        })?;
    }

    delete_deployment(LAB_PLATFORM_STORE_URL, TRANSFORM_PIPELINE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{TRANSFORM_PIPELINE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_transform_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    // Re-run wipe-before-recreate: fully remove leftovers, then create fresh Namespace.
    remove_transform_pipeline_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {TRANSFORM_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200)\n\
);\n\
ALTER TABLE {TRANSFORM_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
CREATE TABLE {TRANSFORM_ORDERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  CUSTOMER_ID NUMBER(10) NOT NULL,\n\
  AMOUNT NUMBER(10) NOT NULL,\n\
  NOTE VARCHAR2(200)\n\
);\n\
ALTER TABLE {TRANSFORM_ORDERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {TRANSFORM_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (1, 'Alice', 'alice@example.com');\n\
INSERT INTO {TRANSFORM_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (2, 'Bob', 'bob@example.com');\n\
INSERT INTO {TRANSFORM_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (1, 1, 10, 'seed-a');\n\
INSERT INTO {TRANSFORM_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (2, 1, 20, 'seed-b');\n\
INSERT INTO {TRANSFORM_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (3, 2, 5, 'seed-c');\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare Transform Pipeline Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_transform_pipeline_source(lab_dir: &Path) -> Result<(), CliError> {
    // Multi-table Source workload:
    // - customers: UPDATE Alice→Alicia, INSERT Carol, DELETE Bob
    // - orders: INSERT amount 15 for cust 1, UPDATE cust 2 amount 5→50, DELETE order 1
    // Expected order totals after sync: cust1=35 (20+15), cust2=50
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {TRANSFORM_CUSTOMERS_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {TRANSFORM_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (3, 'Carol', 'carol@example.com');\n\
DELETE FROM {TRANSFORM_CUSTOMERS_TABLE} WHERE ID = 2;\n\
INSERT INTO {TRANSFORM_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (4, 1, 15, 'new');\n\
UPDATE {TRANSFORM_ORDERS_TABLE} SET AMOUNT = 50, NOTE = 'bumped' WHERE ID = 3;\n\
DELETE FROM {TRANSFORM_ORDERS_TABLE} WHERE ID = 1;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive multi-table Source insert/update/delete for Lab Scenario:\n{err}"
            ))
        })
}

async fn run_rt_project(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {RT_PROJECT_ID}");
    println!(
        "Scenario Namespace: table={RT_PROJECT_TABLE} \
collection={RT_PROJECT_COLLECTION} deployment={RT_PROJECT_DEPLOYMENT}"
    );

    prepare_rt_project_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, RT_PROJECT_ID)?;
    let bin = lab_migraloop_bin();

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.to_ascii_lowercase().contains("derived")
        || apply_out.contains(RT_PROJECT_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
        )));
    }

    let derived_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_PROJECT_PIPELINE,
        ],
    )
    .await?;
    // project keeps ID/NAME only — EMAIL must not appear as Managed Derived field.
    if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
        && managed_field_present(&derived_after_apply, "NAME", "Bob")
        && !inspect_mentions_email_field(&derived_after_apply))
    {
        return Err(CliError::Failed(format!(
            "Initial Load project Derived check failed \
(expected Alice/Bob NAME, no EMAIL Managed field):\n{derived_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source insert/update/delete...");
    mutate_rt_project_source(lab_dir).await?;

    println!("Lab Scenario: sync Incremental Capture + Delivery via real product path...");
    let sync_out = run_product_cli(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };

    let derived_after = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_PROJECT_PIPELINE,
        ],
    )
    .await?;
    let target_after = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            RT_PROJECT_COLLECTION,
        ],
    )
    .await?;

    let derived_ok = managed_field_present(&derived_after, "NAME", "Alicia")
        && managed_field_present(&derived_after, "NAME", "Carol")
        && !managed_field_present(&derived_after, "NAME", "Bob")
        && !inspect_mentions_email_field(&derived_after);
    let target_ok = managed_field_present(&target_after, "NAME", "Alicia")
        && managed_field_present(&target_after, "NAME", "Carol")
        && !managed_field_present(&target_after, "NAME", "Bob")
        && !inspect_mentions_email_field(&target_after);

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(derived_ok && target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after project insert/update/delete.\n\
Derived:\n{derived_after}\nTarget:\n{target_after}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (projected Derived + Target Managed outcomes)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: None,
        max_lag: None,
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

/// True when inspect output exposes an EMAIL Managed field key (not merely the substring).
fn inspect_mentions_email_field(inspect: &str) -> bool {
    inspect.contains("\"EMAIL\"")
        || inspect.contains("\"email\"")
        || inspect.contains("'EMAIL'")
        || inspect.contains("'email'")
}

async fn remove_rt_project_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={RT_PROJECT_TABLE}, collection={RT_PROJECT_COLLECTION}, \
          deployment={RT_PROJECT_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_PROJECT_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace table {RT_PROJECT_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{RT_PROJECT_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {RT_PROJECT_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, RT_PROJECT_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{RT_PROJECT_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_rt_project_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_rt_project_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {RT_PROJECT_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200)\n\
);\n\
ALTER TABLE {RT_PROJECT_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {RT_PROJECT_TABLE} (ID, NAME, EMAIL) VALUES (1, 'Alice', 'alice@example.com');\n\
INSERT INTO {RT_PROJECT_TABLE} (ID, NAME, EMAIL) VALUES (2, 'Bob', 'bob@example.com');\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare rt-project Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_rt_project_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_PROJECT_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {RT_PROJECT_TABLE} (ID, NAME, EMAIL) VALUES (3, 'Carol', 'carol@example.com');\n\
DELETE FROM {RT_PROJECT_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source insert/update/delete for rt-project Lab Scenario:\n{err}"
            ))
        })
}

async fn run_rt_filter(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {RT_FILTER_ID}");
    println!(
        "Scenario Namespace: table={RT_FILTER_TABLE} \
collection={RT_FILTER_COLLECTION} deployment={RT_FILTER_DEPLOYMENT}"
    );

    prepare_rt_filter_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, RT_FILTER_ID)?;
    let bin = lab_migraloop_bin();

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.to_ascii_lowercase().contains("derived")
        || apply_out.contains(RT_FILTER_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
        )));
    }

    let derived_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_FILTER_PIPELINE,
        ],
    )
    .await?;
    // filter ACTIVE==1: Alice only (Bob ACTIVE=0 excluded).
    if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
        && !managed_field_present(&derived_after_apply, "NAME", "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load filter Derived check failed (expected Alice only):\n{derived_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source insert/update/delete...");
    mutate_rt_filter_source(lab_dir).await?;

    println!("Lab Scenario: sync Incremental Capture + Delivery via real product path...");
    let sync_out = run_product_cli(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };

    let derived_after = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_FILTER_PIPELINE,
        ],
    )
    .await?;
    let target_after = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            RT_FILTER_COLLECTION,
        ],
    )
    .await?;

    // After mutate: Alicia ACTIVE=1, Carol ACTIVE=1, Bob deleted → both active names present.
    let derived_ok = managed_field_present(&derived_after, "NAME", "Alicia")
        && managed_field_present(&derived_after, "NAME", "Carol")
        && !managed_field_present(&derived_after, "NAME", "Bob");
    let target_ok = managed_field_present(&target_after, "NAME", "Alicia")
        && managed_field_present(&target_after, "NAME", "Carol")
        && !managed_field_present(&target_after, "NAME", "Bob");

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(derived_ok && target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after filter insert/update/delete.\n\
Derived:\n{derived_after}\nTarget:\n{target_after}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (filtered Derived + Target Managed outcomes)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: None,
        max_lag: None,
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

async fn remove_rt_filter_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={RT_FILTER_TABLE}, collection={RT_FILTER_COLLECTION}, \
          deployment={RT_FILTER_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_FILTER_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace table {RT_FILTER_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{RT_FILTER_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {RT_FILTER_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, RT_FILTER_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{RT_FILTER_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_rt_filter_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_rt_filter_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {RT_FILTER_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1) NOT NULL\n\
);\n\
ALTER TABLE {RT_FILTER_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {RT_FILTER_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {RT_FILTER_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare rt-filter Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_rt_filter_source(lab_dir: &Path) -> Result<(), CliError> {
    // Alicia stays ACTIVE=1; Carol ACTIVE=1 inserted; inactive Bob deleted.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_FILTER_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {RT_FILTER_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {RT_FILTER_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source insert/update/delete for rt-filter Lab Scenario:\n{err}"
            ))
        })
}

async fn run_concurrent_source_workload(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {CONCURRENT_SOURCE_WORKLOAD_ID}");
    println!(
        "Scenario Namespace: tables={CONCURRENT_CUSTOMERS_TABLE},{CONCURRENT_ORDERS_TABLE} \
collections={CONCURRENT_CUSTOMERS_COLLECTION},{CONCURRENT_ORDER_TOTALS_COLLECTION} \
deployment={CONCURRENT_SOURCE_WORKLOAD_DEPLOYMENT}"
    );
    println!(
        "Lab Scenario: recipe uses intra-Scenario parallel Source sessions \
(not a second concurrent Scenario run); max_settle_ms={CONCURRENT_MAX_SETTLE_MS}"
    );

    prepare_concurrent_source_namespace(lab_dir).await?;
    println!(
        "Lab Scenario: Scenario Namespace prepared (multi-table schema + seed + supplemental logging)"
    );

    let config_path = deployment_config_path(lab_dir, CONCURRENT_SOURCE_WORKLOAD_ID)?;
    let bin = lab_migraloop_bin();

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.to_ascii_lowercase().contains("derived")
        || apply_out.contains(CONCURRENT_ORDER_TOTALS_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
        )));
    }

    let customers_base = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            CONCURRENT_CUSTOMERS_TABLE,
        ],
    )
    .await?;
    let orders_base = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            CONCURRENT_ORDERS_TABLE,
        ],
    )
    .await?;
    if !(managed_field_present(&customers_base, "NAME", "Alice")
        && managed_field_present(&customers_base, "NAME", "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load customers Base check failed (expected Alice and Bob):\n{customers_base}"
        )));
    }
    if !(managed_field_present(&orders_base, "AMOUNT", "10")
        && managed_field_present(&orders_base, "AMOUNT", "20")
        && managed_field_present(&orders_base, "AMOUNT", "5"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load orders Base check failed (expected amounts 10/20/5):\n{orders_base}"
        )));
    }

    let derived_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            CONCURRENT_ORDER_TOTALS_PIPELINE,
        ],
    )
    .await?;
    // Seed totals: customer 1 = 10+20=30, customer 2 = 5.
    if !(inspect_mentions_amount(&derived_after_apply, "30")
        && inspect_mentions_amount(&derived_after_apply, "5"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load Derived check failed (expected totals 30 and 5):\n{derived_after_apply}"
        )));
    }

    println!(
        "Lab Scenario: driving concurrent Source workload \
(parallel customers + orders sessions)..."
    );
    mutate_concurrent_source_workload(lab_dir).await?;

    // US47: wait until Delivery catches up within Scenario thresholds before final asserts.
    println!(
        "Lab Scenario: settling Incremental Capture + Delivery within max_settle_ms={CONCURRENT_MAX_SETTLE_MS}..."
    );
    let settle_started = Instant::now();
    let mut sync_out = String::new();
    let mut capture_note = String::new();
    let mut last_detail = String::new();

    loop {
        let settle_ms = settle_started.elapsed().as_millis();
        if settle_ms > CONCURRENT_MAX_SETTLE_MS {
            return Ok(ScenarioReport {
                correctness: false,
                rows_applied: count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out),
                detail: format!(
                    "threshold: concurrent Source changes did not settle within \
max_settle_ms={CONCURRENT_MAX_SETTLE_MS} (elapsed settle_ms={settle_ms}). {last_detail}"
                ),
                capture_path_note: capture_note,
                settle_ms: Some(settle_ms),
                max_settle_ms: Some(CONCURRENT_MAX_SETTLE_MS),
                lag: None,
                max_lag: None,
                min_rows_per_s: None,
                max_duration_ms: None,
                measured_rows_per_s: None,
                measured_duration_ms: None,
                thresholds_ok: false,
            });
        }

        sync_out = run_product_cli(
            &bin,
            &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        )
        .await?;
        if sync_out.to_ascii_lowercase().contains("logminer") {
            capture_note = "LogMiner".to_string();
        } else if capture_note.is_empty() {
            return Err(CliError::Failed(format!(
                "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
            )));
        }

        let customers_base_after = run_product_cli(
            &bin,
            &[
                "base",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--table",
                CONCURRENT_CUSTOMERS_TABLE,
            ],
        )
        .await?;
        let derived_after = run_product_cli(
            &bin,
            &[
                "derived",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--pipeline",
                CONCURRENT_ORDER_TOTALS_PIPELINE,
            ],
        )
        .await?;
        let customers_target = run_product_cli(
            &bin,
            &[
                "target",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--collection",
                CONCURRENT_CUSTOMERS_COLLECTION,
            ],
        )
        .await?;
        let totals_target = run_product_cli(
            &bin,
            &[
                "target",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--collection",
                CONCURRENT_ORDER_TOTALS_COLLECTION,
            ],
        )
        .await?;

        // Customers Direct: Alicia + Carol present, Bob deleted.
        let customers_base_ok = managed_field_present(&customers_base_after, "NAME", "Alicia")
            && managed_field_present(&customers_base_after, "NAME", "Carol")
            && !managed_field_present(&customers_base_after, "NAME", "Bob");
        let customers_target_ok = managed_field_present(&customers_target, "NAME", "Alicia")
            && managed_field_present(&customers_target, "NAME", "Carol")
            && !managed_field_present(&customers_target, "NAME", "Bob");
        // After concurrent mutate: cust1=20+5+10=35, cust2=5+15+30=50.
        let derived_ok = inspect_mentions_amount(&derived_after, "35")
            && inspect_mentions_amount(&derived_after, "50")
            && !inspect_mentions_amount(&derived_after, "30");
        let totals_target_ok = inspect_mentions_amount(&totals_target, "35")
            && inspect_mentions_amount(&totals_target, "50")
            && !inspect_mentions_amount(&totals_target, "30");

        if customers_base_ok && customers_target_ok && derived_ok && totals_target_ok {
            break;
        }

        last_detail = format!(
            "correctness not yet settled.\n\
Customers Base:\n{customers_base_after}\nCustomers Target:\n{customers_target}\n\
Derived:\n{derived_after}\nOrder totals Target:\n{totals_target}"
        );
        tokio::time::sleep(CONCURRENT_SETTLE_POLL).await;
    }

    let settle_ms = settle_started.elapsed().as_millis();
    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);
    println!(
        "Lab Scenario: correctness checks passed after concurrent Source settle \
(settle_ms={settle_ms}, Base + Derived + Target Managed outcomes)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }

    // Threshold can fail even when Managed outcomes are already correct (US21 / US36).
    if settle_ms > CONCURRENT_MAX_SETTLE_MS {
        return Ok(ScenarioReport {
            correctness: true,
            rows_applied,
            detail: format!(
                "threshold: settle_ms={settle_ms} exceeded max_settle_ms={CONCURRENT_MAX_SETTLE_MS} \
after concurrent Source changes reached correct Target/Derived outcomes"
            ),
            capture_path_note: capture_note,
            settle_ms: Some(settle_ms),
            max_settle_ms: Some(CONCURRENT_MAX_SETTLE_MS),
            lag: None,
            max_lag: None,
            min_rows_per_s: None,
            max_duration_ms: None,
            measured_rows_per_s: None,
            measured_duration_ms: None,
            thresholds_ok: false,
        });
    }

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: Some(settle_ms),
        max_settle_ms: Some(CONCURRENT_MAX_SETTLE_MS),
        lag: None,
        max_lag: None,
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

/// Fully remove concurrent-source-workload Scenario Namespace. Idempotent.
async fn remove_concurrent_source_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (tables={CONCURRENT_CUSTOMERS_TABLE},{CONCURRENT_ORDERS_TABLE}, \
          collections={CONCURRENT_CUSTOMERS_COLLECTION},{CONCURRENT_ORDER_TOTALS_COLLECTION}, \
          deployment={CONCURRENT_SOURCE_WORKLOAD_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {CONCURRENT_ORDERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {CONCURRENT_CUSTOMERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace tables \
                 {CONCURRENT_CUSTOMERS_TABLE}/{CONCURRENT_ORDERS_TABLE}:\n{err}"
            ))
        })?;

    for collection in [
        CONCURRENT_CUSTOMERS_COLLECTION,
        CONCURRENT_ORDER_TOTALS_COLLECTION,
    ] {
        let js = format!("db.getCollection('{collection}').drop()");
        mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Mongo Scenario Namespace collection {collection}:\n{err}"
            ))
        })?;
    }

    delete_deployment(LAB_PLATFORM_STORE_URL, CONCURRENT_SOURCE_WORKLOAD_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{CONCURRENT_SOURCE_WORKLOAD_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_concurrent_source_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_concurrent_source_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {CONCURRENT_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200)\n\
);\n\
ALTER TABLE {CONCURRENT_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
CREATE TABLE {CONCURRENT_ORDERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  CUSTOMER_ID NUMBER(10) NOT NULL,\n\
  AMOUNT NUMBER(10) NOT NULL,\n\
  NOTE VARCHAR2(200)\n\
);\n\
ALTER TABLE {CONCURRENT_ORDERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {CONCURRENT_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (1, 'Alice', 'alice@example.com');\n\
INSERT INTO {CONCURRENT_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (2, 'Bob', 'bob@example.com');\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (1, 1, 10, 'seed-a');\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (2, 1, 20, 'seed-b');\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (3, 2, 5, 'seed-c');\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare concurrent Source workload Scenario Namespace:\n{err}"
            ))
        })
}

/// Intra-Scenario concurrent Source workload: three parallel sqlplus sessions.
///
/// Expected after all commits (deterministic finals; PK ranges do not overlap):
/// - customers: Alicia + Carol (Bob deleted)
/// - order totals: cust1=35 (20+5+10), cust2=50 (5+15+30)
async fn mutate_concurrent_source_workload(lab_dir: &Path) -> Result<(), CliError> {
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");

    // Session A — customers Direct path (serial steps inside one session).
    let sql_customers = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {CONCURRENT_CUSTOMERS_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {CONCURRENT_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (3, 'Carol', 'carol@example.com');\n\
DELETE FROM {CONCURRENT_CUSTOMERS_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    // Session B — parallel inserts/delete on orders for CUSTOMER_ID=1.
    let sql_orders_cust1 = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (10, 1, 5, 'par-a');\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (11, 1, 10, 'par-b');\n\
DELETE FROM {CONCURRENT_ORDERS_TABLE} WHERE ID = 1;\n\
COMMIT;\n\
EXIT;\n"
    );
    // Session C — parallel inserts on orders for CUSTOMER_ID=2 (contention on Derived totals).
    let sql_orders_cust2 = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (20, 2, 15, 'par-c');\n\
INSERT INTO {CONCURRENT_ORDERS_TABLE} (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES (21, 2, 30, 'par-d');\n\
COMMIT;\n\
EXIT;\n"
    );

    let (customers, orders_a, orders_b) = tokio::join!(
        sqlplus_in_oracle(lab_dir, &connect, &sql_customers),
        sqlplus_in_oracle(lab_dir, &connect, &sql_orders_cust1),
        sqlplus_in_oracle(lab_dir, &connect, &sql_orders_cust2),
    );

    customers.map_err(|err| {
        CliError::Failed(format!(
            "Failed concurrent customers Source session for Lab Scenario:\n{err}"
        ))
    })?;
    orders_a.map_err(|err| {
        CliError::Failed(format!(
            "Failed concurrent orders (customer 1) Source session for Lab Scenario:\n{err}"
        ))
    })?;
    orders_b.map_err(|err| {
        CliError::Failed(format!(
            "Failed concurrent orders (customer 2) Source session for Lab Scenario:\n{err}"
        ))
    })?;

    Ok(())
}

async fn run_bulk_load(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {BULK_LOAD_ID}");
    println!(
        "Scenario Namespace: table={BULK_LOAD_TABLE} collection={BULK_LOAD_COLLECTION} \
deployment={BULK_LOAD_DEPLOYMENT}"
    );
    println!(
        "Lab Scenario: bulk Source volume rows={BULK_LOAD_ROW_COUNT}; \
thresholds max_lag={BULK_LOAD_MAX_LAG} max_duration_ms={BULK_LOAD_MAX_DURATION_MS} \
min_rows_per_s={BULK_LOAD_MIN_ROWS_PER_S}"
    );

    prepare_bulk_load_namespace(lab_dir).await?;
    println!(
        "Lab Scenario: Scenario Namespace prepared \
(schema + ~{BULK_LOAD_ROW_COUNT} Source inserts + supplemental logging)"
    );

    let config_path = deployment_config_path(lab_dir, BULK_LOAD_ID)?;
    let bin = lab_migraloop_bin();
    let load_started = Instant::now();

    println!("Lab Scenario: apply Deployment via real product path (Initial Load of bulk volume)...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_path.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    // US47: wait until Delivery/Health catch up within duration threshold before final asserts.
    println!(
        "Lab Scenario: settling bulk Delivery / Sync Health within \
max_duration_ms={BULK_LOAD_MAX_DURATION_MS}..."
    );
    let mut last_detail = String::new();
    let mut settled = false;
    let (base_rows, target_rows, lag) = loop {
        let measured_duration_ms = load_started.elapsed().as_millis();
        let base_after = run_product_cli(
            &bin,
            &[
                "base",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--table",
                BULK_LOAD_TABLE,
            ],
        )
        .await?;
        let target_after = run_product_cli(
            &bin,
            &[
                "target",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--collection",
                BULK_LOAD_COLLECTION,
            ],
        )
        .await?;
        let status_out = run_product_cli(
            &bin,
            &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        )
        .await?;

        let base_rows = parse_inspect_row_count(&base_after).ok_or_else(|| {
            CliError::Failed(format!(
                "Lab Scenario could not parse Base row count after bulk Initial Load:\n{base_after}"
            ))
        })?;
        let target_rows = parse_target_document_count(&target_after).ok_or_else(|| {
            CliError::Failed(format!(
                "Lab Scenario could not parse Target document count after bulk Delivery:\n{target_after}"
            ))
        })?;
        let lag = parse_sync_lag_for_table(&status_out, BULK_LOAD_TABLE).ok_or_else(|| {
            CliError::Failed(format!(
                "Lab Scenario could not parse Sync Health lag for {BULK_LOAD_TABLE}:\n{status_out}"
            ))
        })?;

        let rows_ok = base_rows == BULK_LOAD_ROW_COUNT && target_rows == BULK_LOAD_ROW_COUNT;
        let lag_ok = lag <= BULK_LOAD_MAX_LAG;
        if rows_ok && lag_ok {
            settled = true;
            break (base_rows, target_rows, lag);
        }

        last_detail = format!(
            "bulk Delivery/Health not yet caught up \
(base_rows={base_rows} target_rows={target_rows} lag={lag}).\n\
Base:\n{base_after}\nTarget:\n{target_after}\nStatus:\n{status_out}"
        );
        if measured_duration_ms > BULK_LOAD_MAX_DURATION_MS {
            break (base_rows, target_rows, lag);
        }
        tokio::time::sleep(BULK_LOAD_SETTLE_POLL).await;
    };

    let measured_duration_ms = load_started.elapsed().as_millis();
    let measured_rows_per_s = if load_started.elapsed().as_secs_f64() > 0.0 {
        BULK_LOAD_ROW_COUNT as f64 / load_started.elapsed().as_secs_f64()
    } else {
        BULK_LOAD_ROW_COUNT as f64
    };

    let rows_applied = count_delivery_ops(&apply_out).max(base_rows);
    // Correctness is row-level Managed outcomes; lag/duration/throughput are threshold axes.
    let correctness =
        base_rows == BULK_LOAD_ROW_COUNT && target_rows == BULK_LOAD_ROW_COUNT;
    let sample = BulkLoadMetricSample {
        lag,
        max_lag: BULK_LOAD_MAX_LAG,
        duration_ms: measured_duration_ms,
        max_duration_ms: BULK_LOAD_MAX_DURATION_MS,
        rows_per_s: measured_rows_per_s,
        min_rows_per_s: BULK_LOAD_MIN_ROWS_PER_S,
    };
    let (thresholds_ok, mut threshold_detail) = evaluate_bulk_load_thresholds(&sample);
    if !settled && !thresholds_ok && !last_detail.is_empty() {
        threshold_detail = format!(
            "{threshold_detail} (bulk Delivery/Health settle incomplete within \
max_duration_ms={BULK_LOAD_MAX_DURATION_MS}). {last_detail}"
        );
    }

    let mut detail_parts = Vec::new();
    if !correctness {
        detail_parts.push(format!(
            "correctness: expected rows={BULK_LOAD_ROW_COUNT} \
base_rows={base_rows} target_rows={target_rows}"
        ));
    }
    if !thresholds_ok {
        detail_parts.push(threshold_detail);
    }
    let detail = detail_parts.join("; ");

    if correctness && thresholds_ok {
        println!(
            "Lab Scenario: correctness and metric thresholds passed \
(base/target rows={BULK_LOAD_ROW_COUNT}, lag={lag}, \
duration_ms={measured_duration_ms}, rows_per_s={measured_rows_per_s:.2})"
        );
    } else if correctness {
        println!(
            "Lab Scenario: correctness passed; metric thresholds failed \
(lag={lag}, duration_ms={measured_duration_ms}, rows_per_s={measured_rows_per_s:.2})"
        );
    } else {
        println!(
            "Lab Scenario: correctness failed (base_rows={base_rows} target_rows={target_rows}); \
metrics lag={lag} duration_ms={measured_duration_ms} rows_per_s={measured_rows_per_s:.2}"
        );
    }

    Ok(ScenarioReport {
        correctness,
        rows_applied,
        detail,
        capture_path_note: "Initial Load".to_string(),
        settle_ms: None,
        max_settle_ms: None,
        lag: Some(lag),
        max_lag: Some(BULK_LOAD_MAX_LAG),
        min_rows_per_s: Some(BULK_LOAD_MIN_ROWS_PER_S),
        max_duration_ms: Some(BULK_LOAD_MAX_DURATION_MS),
        measured_rows_per_s: Some(measured_rows_per_s),
        measured_duration_ms: Some(measured_duration_ms),
        thresholds_ok,
    })
}

/// Fully remove bulk-load Scenario Namespace. Idempotent.
async fn remove_bulk_load_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={BULK_LOAD_TABLE}, collection={BULK_LOAD_COLLECTION}, \
          deployment={BULK_LOAD_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {BULK_LOAD_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace table {BULK_LOAD_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{BULK_LOAD_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {BULK_LOAD_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, BULK_LOAD_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{BULK_LOAD_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_bulk_load_namespace(lab_dir: &Path) -> Result<(), CliError> {
    // Re-run wipe-before-recreate: fully remove leftovers, then create fresh Namespace.
    remove_bulk_load_namespace(lab_dir).await?;

    // Bulk insert via CONNECT BY before apply so Initial Load exercises ~100k volume (US17).
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {BULK_LOAD_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  VALUE NUMBER(10) NOT NULL\n\
);\n\
ALTER TABLE {BULK_LOAD_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {BULK_LOAD_TABLE} (ID, NAME, VALUE)\n\
SELECT LEVEL, 'item-' || LEVEL, MOD(LEVEL, 100)\n\
FROM dual\n\
CONNECT BY LEVEL <= {BULK_LOAD_ROW_COUNT};\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare bulk-load Scenario Namespace (~{BULK_LOAD_ROW_COUNT} inserts):\n{err}"
            ))
        })
}

fn parse_inspect_row_count(inspect: &str) -> Option<u64> {
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

fn parse_target_document_count(inspect: &str) -> Option<u64> {
    for line in inspect.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("documents:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

/// Parse `Sync Health: ... lag=N ...` for the Base Dataset line matching `table`.
fn parse_sync_lag_for_table(status_out: &str, table: &str) -> Option<i32> {
    let mut lines = status_out.lines().peekable();
    while let Some(line) = lines.next() {
        if line.contains("Base Dataset:") && line.contains(table) {
            // Sync Health may appear on following indented lines.
            for _ in 0..8 {
                let Some(next) = lines.next() else {
                    break;
                };
                if let Some(lag) = parse_labeled_i32(next, "lag=") {
                    return Some(lag);
                }
                if next.contains("Base Dataset:") {
                    break;
                }
            }
        }
    }
    None
}

fn parse_labeled_i32(line: &str, label: &str) -> Option<i32> {
    let idx = line.find(label)?;
    let rest = &line[idx + label.len()..];
    let digits: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '-')
        .collect();
    if digits.is_empty() || digits == "-" {
        return None;
    }
    digits.parse().ok()
}

/// Fully remove Direct Pipeline Scenario Namespace (Source table, Target collection,
/// Platform Store Deployment + cascaded Bases/Pipelines). Idempotent.
async fn remove_direct_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={DIRECT_PIPELINE_TABLE}, collection={DIRECT_PIPELINE_COLLECTION}, \
          deployment={DIRECT_PIPELINE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {DIRECT_PIPELINE_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Oracle Scenario Namespace table {DIRECT_PIPELINE_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{DIRECT_PIPELINE_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {DIRECT_PIPELINE_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, DIRECT_PIPELINE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{DIRECT_PIPELINE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_direct_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    // Re-run wipe-before-recreate: fully remove leftovers, then create fresh Namespace.
    remove_direct_pipeline_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {DIRECT_PIPELINE_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {DIRECT_PIPELINE_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {DIRECT_PIPELINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {DIRECT_PIPELINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare Direct Pipeline Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_direct_pipeline_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {DIRECT_PIPELINE_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {DIRECT_PIPELINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {DIRECT_PIPELINE_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source insert/update/delete for Lab Scenario:\n{err}"
            ))
        })
}

async fn run_product_cli(bin: &Path, args: &[&str]) -> Result<String, CliError> {
    let output = Command::new(bin)
        .args(args)
        .env(LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT)
        .env(LAB_MONGO_PASSWORD_ENV, LAB_MONGO_PASSWORD_DEFAULT)
        .env("MIGRALOOP_PLATFORM_STORE_URL", LAB_PLATFORM_STORE_URL)
        .output()
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "failed to run `{} {}`: {err}",
                bin.display(),
                args.join(" ")
            ))
        })?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(CliError::Failed(format!(
            "`{} {}` failed:\n{text}\n\
Hint: Scenario apply/sync needs Oracle Instant Client on the host \
(`LD_LIBRARY_PATH`) and a ready Lab Fixture (`migraloop lab up`).",
            bin.display(),
            args.join(" ")
        )));
    }
    // Echo product output so operators see real apply/sync progress.
    print!("{text}");
    Ok(text)
}

#[derive(Debug, Serialize, Deserialize)]
struct LockFile {
    scenario: String,
    pid: u32,
    started_at_unix: u64,
}

fn read_active_lock(path: &Path) -> Result<Option<LockFile>, CliError> {
    if !path.is_file() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path).map_err(|err| {
        CliError::Failed(format!(
            "failed to read Scenario lock {}: {err}",
            path.display()
        ))
    })?;
    let parsed: LockFile = serde_json::from_str(&raw).map_err(|err| {
        CliError::Failed(format!(
            "failed to parse Scenario lock {}: {err}",
            path.display()
        ))
    })?;
    if process_is_alive(parsed.pid) {
        Ok(Some(parsed))
    } else {
        // Stale lock from a crashed runner — clear so a new run can proceed.
        let _ = fs::remove_file(path);
        Ok(None)
    }
}

fn process_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // Unix: signal 0 checks existence without killing.
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct ScenarioLock {
    path: PathBuf,
}

impl ScenarioLock {
    fn acquire(path: &Path, scenario: &str) -> Result<Self, CliError> {
        // Drop stale locks (dead pid) before exclusive create.
        if let Some(existing) = read_active_lock(path)? {
            return Err(CliError::Failed(format!(
                "Lab Scenario rejected: another Scenario is active \
                 (`{}` since unix {})",
                existing.scenario, existing.started_at_unix
            )));
        }
        let started_at_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let body = LockFile {
            scenario: scenario.to_string(),
            pid: std::process::id(),
            started_at_unix,
        };
        let json = serde_json::to_string_pretty(&body).map_err(|err| {
            CliError::Failed(format!("failed to serialize Scenario lock: {err}"))
        })?;
        // Exclusive create closes the TOCTOU race between runners.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    if let Ok(Some(existing)) = read_active_lock(path) {
                        return CliError::Failed(format!(
                            "Lab Scenario rejected: another Scenario is active \
                             (`{}` since unix {})",
                            existing.scenario, existing.started_at_unix
                        ));
                    }
                }
                CliError::Failed(format!(
                    "failed to acquire Scenario lock {}: {err}",
                    path.display()
                ))
            })?;
        file.write_all(format!("{json}\n").as_bytes())
            .map_err(|err| {
                CliError::Failed(format!(
                    "failed to write Scenario lock {}: {err}",
                    path.display()
                ))
            })?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

impl Drop for ScenarioLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn selectable_catalog_loads_bulk_load_recipe_from_repo_lab() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab");
        let catalog = load_selectable_catalog(&lab).expect("load repo Lab catalog");
        let bulk = catalog
            .iter()
            .find(|(id, _)| id == BULK_LOAD_ID)
            .expect("catalog must include bulk-load");
        assert!(
            bulk.1.contains("100k"),
            "recipe summary must mention 100k, got {}",
            bulk.1
        );
        assert_eq!(
            catalog.len(),
            registered_scenario_ids().len(),
            "repo Lab recipes must cover every registered Scenario runner"
        );
    }

    #[test]
    fn repo_lab_catalog_is_complete_for_shipped_capabilities() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab");
        let catalog = load_selectable_catalog(&lab).expect("load repo Lab catalog");
        let ids: Vec<String> = catalog.iter().map(|(id, _)| id.clone()).collect();
        let gaps = shipped_capability_coverage_gaps(&ids);
        assert!(
            gaps.is_empty(),
            "shipped capability gaps must be empty before catalog-complete claim; gaps={gaps:?}"
        );
        assert!(
            ids.iter().any(|id| id == RT_PROJECT_ID),
            "catalog must include rt-project for shipped Rich Transform project"
        );
        assert!(
            ids.iter().any(|id| id == RT_FILTER_ID),
            "catalog must include rt-filter for shipped Rich Transform filter"
        );
        let coverage = lab.join("scenarios/COVERAGE.md");
        let body = fs::read_to_string(&coverage).expect("COVERAGE.md");
        assert!(
            body.contains("Catalog-complete for currently shipped"),
            "COVERAGE.md must state catalog-complete only when shipped surface is covered"
        );
        for (id, _) in shipped_capability_scenario_requirements() {
            assert!(
                body.contains(id),
                "COVERAGE.md must document Scenario `{id}`"
            );
        }
    }

    #[test]
    fn coverage_gaps_surface_missing_shipped_scenarios() {
        let gaps = shipped_capability_coverage_gaps(&[DIRECT_PIPELINE_ID.to_string()]);
        assert!(
            gaps.iter().any(|g| g.contains("Rich Transform project")),
            "missing rt-project must be a visible gap; gaps={gaps:?}"
        );
        assert!(
            gaps.iter().any(|g| g.contains("Rich Transform filter")),
            "missing rt-filter must be a visible gap; gaps={gaps:?}"
        );
    }

    #[test]
    fn recipe_loader_requires_namespace_workload_and_checks() {
        let dir = std::env::temp_dir().join(format!(
            "migraloop-recipe-validate-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).expect("temp recipe dir");
        let path = dir.join("recipe.yaml");
        fs::write(
            &path,
            "id: demo\nsummary: demo\nnamespace:\n  source_tables: []\n  target_collections: [c]\n  deployment: d\nworkload:\n  concurrency: serial\n  steps: [prepare]\nchecks:\n  correctness: [ok]\n",
        )
        .expect("write");
        let err = load_recipe(&path).expect_err("empty source_tables must fail");
        let _ = fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("source_tables"),
            "err={err}"
        );
    }

    #[test]
    fn bulk_thresholds_fail_independently_of_correctness() {
        // Metrics miss the bar while row-level correctness would pass (US21 / US36).
        let (ok, detail) = evaluate_bulk_load_thresholds(&BulkLoadMetricSample {
            lag: 0,
            max_lag: 0,
            duration_ms: 900_000,
            max_duration_ms: 600_000,
            rows_per_s: 10.0,
            min_rows_per_s: 50.0,
        });
        assert!(!ok, "threshold sample must fail");
        assert!(detail.contains("threshold:"), "detail={detail}");
        assert!(detail.contains("duration_ms"), "detail={detail}");
        assert!(detail.contains("rows_per_s"), "detail={detail}");

        let report = ScenarioReport {
            correctness: true,
            rows_applied: BULK_LOAD_ROW_COUNT,
            detail: detail.clone(),
            capture_path_note: "Initial Load".to_string(),
            settle_ms: None,
            max_settle_ms: None,
            lag: Some(0),
            max_lag: Some(0),
            min_rows_per_s: Some(50.0),
            max_duration_ms: Some(600_000),
            measured_rows_per_s: Some(10.0),
            measured_duration_ms: Some(900_000),
            thresholds_ok: false,
        };
        assert_eq!(
            scenario_failure_kind(report.correctness, report.thresholds_ok),
            "threshold"
        );
        let rendered = format_scenario_report(
            BULK_LOAD_ID,
            true,
            Duration::from_millis(900_000),
            &report,
            false,
        );
        assert!(rendered.contains("Lab Scenario: FAIL"), "{rendered}");
        assert!(rendered.contains("correctness=pass"), "{rendered}");
        assert!(rendered.contains("thresholds=fail"), "{rendered}");
        assert!(rendered.contains("lag=0"), "{rendered}");
        assert!(rendered.contains("duration_ms=900000"), "{rendered}");
        assert!(rendered.contains("rows_per_s=10.00"), "{rendered}");
        assert!(
            rendered.contains("detail=threshold:"),
            "CLI report seam must surface threshold detail; got:\n{rendered}"
        );
    }

    #[test]
    fn correctness_fail_even_when_metrics_pass() {
        // Row counts wrong, but lag/duration/throughput would pass (US36).
        let (ok, detail) = evaluate_bulk_load_thresholds(&BulkLoadMetricSample {
            lag: 0,
            max_lag: 0,
            duration_ms: 120_000,
            max_duration_ms: 600_000,
            rows_per_s: 800.0,
            min_rows_per_s: 50.0,
        });
        assert!(ok, "metrics should pass, detail={detail}");

        let report = ScenarioReport {
            correctness: false,
            rows_applied: 99_000,
            detail: format!(
                "correctness: expected rows={BULK_LOAD_ROW_COUNT} base_rows=99000 target_rows=99000"
            ),
            capture_path_note: "Initial Load".to_string(),
            settle_ms: None,
            max_settle_ms: None,
            lag: Some(0),
            max_lag: Some(0),
            min_rows_per_s: Some(50.0),
            max_duration_ms: Some(600_000),
            measured_rows_per_s: Some(800.0),
            measured_duration_ms: Some(120_000),
            thresholds_ok: true,
        };
        assert_eq!(
            scenario_failure_kind(report.correctness, report.thresholds_ok),
            "correctness"
        );
        let rendered = format_scenario_report(
            BULK_LOAD_ID,
            true,
            Duration::from_millis(120_000),
            &report,
            false,
        );
        assert!(rendered.contains("Lab Scenario: FAIL"), "{rendered}");
        assert!(rendered.contains("correctness=fail"), "{rendered}");
        assert!(rendered.contains("thresholds=pass"), "{rendered}");
        assert!(rendered.contains("lag=0"), "{rendered}");
        assert!(rendered.contains("detail=correctness:"), "{rendered}");
        assert!(
            rendered.contains("namespace=left in place"),
            "fail keeps Namespace for inspect; got:\n{rendered}"
        );
    }

    #[test]
    fn parse_sync_lag_and_row_counts_from_cli_shaped_output() {
        let status = "\
Base Dataset: LAB_BL_ITEMS status=ready rows=100000 columns=[ID, NAME, VALUE] omittedUnsupported=[(none)]\n\
  Initial Load complete\n\
  Cutover: low-watermark=1 checkpoint=1\n\
  Sync Health: ok appliedChanges=0 lag=0 checkpoint=1\n\
Base Dataset: OTHER status=ready rows=2 columns=[ID] omittedUnsupported=[(none)]\n\
  Sync Health: ok appliedChanges=1 lag=3 checkpoint=9\n";
        assert_eq!(parse_sync_lag_for_table(status, BULK_LOAD_TABLE), Some(0));
        assert_eq!(parse_sync_lag_for_table(status, "OTHER"), Some(3));

        let base = "Base Dataset: LAB_BL_ITEMS status=ready rows=100000 columns=[ID]";
        assert_eq!(parse_inspect_row_count(base), Some(100_000));
        let target = "documents: 100000\n{\"_id\": 1}\n";
        assert_eq!(parse_target_document_count(target), Some(100_000));
    }
}
