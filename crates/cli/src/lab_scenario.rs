//! Lab Scenario catalog, run orchestration, and Namespace cleanup
//! (issues #60–#66, #63, #85, #86 / ADR-0025).
//!
//! Lab-specific machinery: catalog listing from on-disk Scenario recipes
//! (`lab/scenarios/<id>/recipe.yaml`), Scenario Namespace lifecycle
//! (prepare / re-run wipe / manual remove / opt-in auto-remove), Source workload
//! driving (including recipe-authored intra-Scenario concurrency and ~100k bulk
//! Source inserts), one-at-a-time lock, refusal of non-Lab / production engine
//! bindings before apply/sync (US44), result reporting with equal-weight
//! correctness and operational metric thresholds (lag, throughput, duration),
//! and shipped-capability coverage visibility (`lab/scenarios/COVERAGE.md`).
//! Apply / Sync / inspect use the real product CLI path. Idempotent re-delivery
//! (#86) resets Pipeline Delivery status in Platform Store so a second real
//! `apply` re-Delivers the same Output Identities (at-least-once / upsert).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::config::load_deployment_config;
use crate::lab::{
    ensure_fixture_ready_for_scenario, lab_migraloop_bin, mongosh_in_mongo, sqlplus_in_oracle,
    LAB_MONGO_DATABASE, LAB_MONGO_HOST, LAB_MONGO_PASSWORD_DEFAULT, LAB_MONGO_PASSWORD_ENV,
    LAB_MONGO_PORT, LAB_MONGO_USER, LAB_ORACLE_HOST, LAB_ORACLE_PASSWORD_DEFAULT,
    LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PORT, LAB_ORACLE_SERVICE, LAB_ORACLE_USER,
    LAB_PLATFORM_STORE_URL,
};
use crate::CliError;
use migraloop_platform_store::{delete_deployment, update_pipeline_delivery_status};

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

const RT_FIELD_OPS_ID: &str = "rt-field-ops";
const RT_FIELD_OPS_TABLE: &str = "LAB_RFO_CUSTOMERS";
const RT_FIELD_OPS_COLLECTION: &str = "lab_rfo_customers";
const RT_FIELD_OPS_PIPELINE: &str = "lab-rfo-customers";
const RT_FIELD_OPS_DEPLOYMENT: &str = "lab-rt-field-ops";

const RT_EQUILOOKUP_ID: &str = "rt-equilookup";
const RT_EQUILOOKUP_CUSTOMERS_TABLE: &str = "LAB_REL_CUSTOMERS";
const RT_EQUILOOKUP_ORDERS_TABLE: &str = "LAB_REL_ORDERS";
const RT_EQUILOOKUP_COLLECTION: &str = "lab_rel_customers";
const RT_EQUILOOKUP_PIPELINE: &str = "lab-rel-customers";
const RT_EQUILOOKUP_DEPLOYMENT: &str = "lab-rt-equilookup";

const RT_DISTINCT_ADDTOSET_ID: &str = "rt-distinct-addtoset";
const RT_DISTINCT_ADDTOSET_TABLE: &str = "LAB_RDA_ORDERS";
const RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION: &str = "lab_rda_distinct_customers";
const RT_DISTINCT_ADDTOSET_ADD_COLLECTION: &str = "lab_rda_amounts_by_customer";
const RT_DISTINCT_ADDTOSET_DISTINCT_PIPELINE: &str = "lab-rda-distinct-customers";
const RT_DISTINCT_ADDTOSET_ADD_PIPELINE: &str = "lab-rda-amounts-by-customer";
const RT_DISTINCT_ADDTOSET_DEPLOYMENT: &str = "lab-rt-distinct-addtoset";

const IDEMPOTENT_REDELIVERY_ID: &str = "idempotent-redelivery";
const IDEMPOTENT_REDELIVERY_TABLE: &str = "LAB_IR_CUSTOMERS";
const IDEMPOTENT_REDELIVERY_COLLECTION: &str = "lab_ir_customers";
const IDEMPOTENT_REDELIVERY_PIPELINE: &str = "lab-ir-customers";
const IDEMPOTENT_REDELIVERY_DEPLOYMENT: &str = "lab-idempotent-redelivery";
/// Non-Managed Target field planted before re-Delivery to show Managed-only upsert.
const IDEMPOTENT_REDELIVERY_OPERATOR_NOTE: &str = "lab-keep-across-redelivery";

const PAUSE_RESUME_ID: &str = "pause-resume";
const PAUSE_RESUME_CUSTOMERS_TABLE: &str = "LAB_PR_CUSTOMERS";
const PAUSE_RESUME_ORDERS_TABLE: &str = "LAB_PR_ORDERS";
const PAUSE_RESUME_CUSTOMERS_COLLECTION: &str = "lab_pr_customers";
const PAUSE_RESUME_ORDERS_COLLECTION: &str = "lab_pr_orders";
const PAUSE_RESUME_CUSTOMERS_PIPELINE: &str = "lab-pr-customers";
const PAUSE_RESUME_ORDERS_PIPELINE: &str = "lab-pr-orders";
const PAUSE_RESUME_DEPLOYMENT: &str = "lab-pause-resume";

const REMOVE_PIPELINE_ID: &str = "remove-pipeline";
const REMOVE_PIPELINE_CUSTOMERS_TABLE: &str = "LAB_RP_CUSTOMERS";
const REMOVE_PIPELINE_CUSTOMERS_COLLECTION: &str = "lab_rp_customers";
const REMOVE_PIPELINE_REPORTING_COLLECTION: &str = "lab_rp_customers_reporting";
const REMOVE_PIPELINE_CUSTOMERS_PIPELINE: &str = "lab-rp-customers";
const REMOVE_PIPELINE_REPORTING_PIPELINE: &str = "lab-rp-customers-reporting";
const REMOVE_PIPELINE_DEPLOYMENT: &str = "lab-remove-pipeline";

const CHANGE_PIPELINE_ID: &str = "change-pipeline";
const CHANGE_PIPELINE_CUSTOMERS_TABLE: &str = "LAB_CP_CUSTOMERS";
const CHANGE_PIPELINE_ACTIVE_COLLECTION: &str = "lab_cp_active_customers";
const CHANGE_PIPELINE_REPORTING_COLLECTION: &str = "lab_cp_customers_reporting";
const CHANGE_PIPELINE_ACTIVE_PIPELINE: &str = "lab-cp-active-customers";
const CHANGE_PIPELINE_REPORTING_PIPELINE: &str = "lab-cp-customers-reporting";
const CHANGE_PIPELINE_DEPLOYMENT: &str = "lab-change-pipeline";
const CHANGE_PIPELINE_SEMANTIC_CONFIG: &str = "deployment-semantic.yaml";
const CHANGE_PIPELINE_METADATA_CONFIG: &str = "deployment-metadata.yaml";

const POISON_QUARANTINE_ID: &str = "poison-quarantine";
const POISON_QUARANTINE_TABLE: &str = "LAB_PQ_CUSTOMERS";
const POISON_QUARANTINE_COLLECTION: &str = "lab_pq_customers";
const POISON_QUARANTINE_PIPELINE: &str = "lab-pq-customers";
const POISON_QUARANTINE_DEPLOYMENT: &str = "lab-poison-quarantine";
/// Lab orchestration: force Delivery failure for Output Identity 1 so quarantine runs.
const POISON_QUARANTINE_IDENTITY: &str = "1";
const POISON_QUARANTINE_MAX_ATTEMPTS: &str = "2";

const SCHEMA_CHANGE_PAUSE_ID: &str = "schema-change-pause";
const SCHEMA_CHANGE_PAUSE_TABLE: &str = "LAB_SC_CUSTOMERS";
const SCHEMA_CHANGE_PAUSE_COLLECTION: &str = "lab_sc_customers";
const SCHEMA_CHANGE_PAUSE_PIPELINE: &str = "lab-sc-customers";
const SCHEMA_CHANGE_PAUSE_DEPLOYMENT: &str = "lab-schema-change-pause";

const SOURCE_ALIGNMENT_ID: &str = "source-alignment";
const SOURCE_ALIGNMENT_TABLE: &str = "LAB_SA_CUSTOMERS";
const SOURCE_ALIGNMENT_COLLECTION: &str = "lab_sa_customers";
const SOURCE_ALIGNMENT_PIPELINE: &str = "lab-sa-customers";
const SOURCE_ALIGNMENT_DEPLOYMENT: &str = "lab-source-alignment";

const DRIFT_CHECK_ID: &str = "drift-check";
const DRIFT_CHECK_TABLE: &str = "LAB_DC_CUSTOMERS";
const DRIFT_CHECK_COLLECTION: &str = "lab_dc_customers";
const DRIFT_CHECK_PIPELINE: &str = "lab-dc-customers";
const DRIFT_CHECK_DEPLOYMENT: &str = "lab-drift-check";
const DRIFT_CHECK_EXTRA_FIELD: &str = "keep-me-non-managed";

const BOUNDED_BACKPRESSURE_ID: &str = "bounded-backpressure";
const BOUNDED_BACKPRESSURE_TABLE: &str = "LAB_BP_CUSTOMERS";
const BOUNDED_BACKPRESSURE_COLLECTION: &str = "lab_bp_customers";
const BOUNDED_BACKPRESSURE_PIPELINE: &str = "lab-bp-customers";
const BOUNDED_BACKPRESSURE_DEPLOYMENT: &str = "lab-bounded-backpressure";
/// Lab orchestration: tiny Incremental window + Downstream Delivery delay.
const BOUNDED_BACKPRESSURE_CAPACITY: &str = "2";
const BOUNDED_BACKPRESSURE_DELAY_MS: &str = "80";
const BOUNDED_BACKPRESSURE_FAIL_AFTER: &str = "1";
const BOUNDED_BACKPRESSURE_BACKLOG: i64 = 20;

const OBSERVABILITY_SURFACE_ID: &str = "observability-surface";
const OBSERVABILITY_SURFACE_TABLE: &str = "LAB_OBS_CUSTOMERS";
const OBSERVABILITY_SURFACE_COLLECTION: &str = "lab_obs_customers";
const OBSERVABILITY_SURFACE_PIPELINE: &str = "lab-obs-customers";
const OBSERVABILITY_SURFACE_DEPLOYMENT: &str = "lab-observability-surface";
const OBSERVABILITY_SURFACE_CAPACITY: &str = "2";
const OBSERVABILITY_SURFACE_DELAY_MS: &str = "80";
const OBSERVABILITY_SURFACE_FAIL_AFTER: &str = "1";
const OBSERVABILITY_SURFACE_BACKLOG: i64 = 20;

const PLATFORM_STORE_GUARDRAILS_ID: &str = "platform-store-guardrails";
const PLATFORM_STORE_GUARDRAILS_TABLE: &str = "LAB_GUARD_CUSTOMERS";
const PLATFORM_STORE_GUARDRAILS_COLLECTION: &str = "lab_guard_customers";
const PLATFORM_STORE_GUARDRAILS_DEPLOYMENT: &str = "lab-platform-store-guardrails";
/// 512 MiB — below the 1 GiB product warn threshold (ADR-0010).
const PLATFORM_STORE_GUARDRAILS_LOW_DISK_BYTES: &str = "536870912";

const BACKWARD_COMPATIBLE_UPGRADES_ID: &str = "backward-compatible-upgrades";
const BACKWARD_COMPATIBLE_UPGRADES_TABLE: &str = "LAB_UPG_CUSTOMERS";
const BACKWARD_COMPATIBLE_UPGRADES_COLLECTION: &str = "lab_upg_customers";
const BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT: &str = "lab-backward-compatible-upgrades";
const BACKWARD_COMPATIBLE_UPGRADES_OLDER_CONFIG: &str = "deployment-v1.0.0.yaml";

const INITIAL_LOAD_THROTTLED_ID: &str = "initial-load-throttled";
const INITIAL_LOAD_THROTTLED_TABLE: &str = "LAB_IL_ITEMS";
const INITIAL_LOAD_THROTTLED_COLLECTION: &str = "lab_il_items";
const INITIAL_LOAD_THROTTLED_DEPLOYMENT: &str = "lab-initial-load-throttled";
/// Non-trivial Source volume for chunked Initial Load (issue #124).
const INITIAL_LOAD_THROTTLED_ROW_COUNT: i64 = 500;
const INITIAL_LOAD_THROTTLED_CHUNK_SIZE: &str = "50";
const INITIAL_LOAD_THROTTLED_RATE: &str = "200";
const INITIAL_LOAD_THROTTLED_PAUSE_AFTER: &str = "2";
const INITIAL_LOAD_THROTTLED_STORE_DELAY_MS: &str = "20";

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
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `remove-pipeline`, `bulk-load`)
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
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `remove-pipeline`, `bulk-load`)
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
        RT_FIELD_OPS_ID,
        RT_EQUILOOKUP_ID,
        RT_DISTINCT_ADDTOSET_ID,
        CONCURRENT_SOURCE_WORKLOAD_ID,
        BULK_LOAD_ID,
        IDEMPOTENT_REDELIVERY_ID,
        PAUSE_RESUME_ID,
        REMOVE_PIPELINE_ID,
        CHANGE_PIPELINE_ID,
        POISON_QUARANTINE_ID,
        SCHEMA_CHANGE_PAUSE_ID,
        SOURCE_ALIGNMENT_ID,
        DRIFT_CHECK_ID,
        BOUNDED_BACKPRESSURE_ID,
        OBSERVABILITY_SURFACE_ID,
        PLATFORM_STORE_GUARDRAILS_ID,
        BACKWARD_COMPATIBLE_UPGRADES_ID,
        INITIAL_LOAD_THROTTLED_ID,
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
            "multi-table Transform Pipeline (groupBy sum/count/min/max/avg)",
        ),
        (RT_PROJECT_ID, "Rich Transform project"),
        (RT_FILTER_ID, "Rich Transform filter"),
        (
            RT_FIELD_OPS_ID,
            "Rich Transform addFields/rename/remove",
        ),
        (RT_EQUILOOKUP_ID, "Rich Transform equiLookup"),
        (
            RT_DISTINCT_ADDTOSET_ID,
            "Rich Transform distinct/addToSet with Maintenance State",
        ),
        (
            CONCURRENT_SOURCE_WORKLOAD_ID,
            "intra-Scenario concurrent Source workload",
        ),
        (BULK_LOAD_ID, "bulk load (~100k) with metric thresholds"),
        (
            IDEMPOTENT_REDELIVERY_ID,
            "idempotent re-delivery / duplicate-safe Delivery",
        ),
        (
            PAUSE_RESUME_ID,
            "Pipeline pause/resume CLI verbs",
        ),
        (
            REMOVE_PIPELINE_ID,
            "Pipeline remove CLI verb",
        ),
        (
            CHANGE_PIPELINE_ID,
            "Pipeline revision change via apply (Derived rebuild / metadata-only skip)",
        ),
        (
            POISON_QUARANTINE_ID,
            "Poison Change quarantine on Operator status",
        ),
        (
            SCHEMA_CHANGE_PAUSE_ID,
            "Blocking DDL Schema Change warn+pause",
        ),
        (
            SOURCE_ALIGNMENT_ID,
            "Source Alignment Check for Base Datasets",
        ),
        (
            DRIFT_CHECK_ID,
            "Drift Check with Managed-field auto-repair",
        ),
        (
            BOUNDED_BACKPRESSURE_ID,
            "Bounded backpressure with visible lag",
        ),
        (
            OBSERVABILITY_SURFACE_ID,
            "Observability Surface (logs, health, Prometheus)",
        ),
        (
            PLATFORM_STORE_GUARDRAILS_ID,
            "Platform Store Guardrails and warn-only disk thresholds",
        ),
        (
            BACKWARD_COMPATIBLE_UPGRADES_ID,
            "Backward-compatible upgrades / Platform Store migrations",
        ),
        (
            INITIAL_LOAD_THROTTLED_ID,
            "Chunked / rate-limited / pausable Initial Load with backoff",
        ),
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
    Ok(load_selectable_recipes(lab_dir)?
        .into_iter()
        .map(|recipe| (recipe.id, recipe.summary))
        .collect())
}

/// Load complete selectable Scenario recipes (id matches directory + deployment config present).
fn load_selectable_recipes(lab_dir: &Path) -> Result<Vec<ScenarioRecipe>, CliError> {
    let mut recipes = Vec::new();
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
        recipes.push(recipe);
    }
    Ok(recipes)
}

/// Active Lab Scenario id from the one-at-a-time lock (live PID only).
pub(crate) fn active_scenario_run(lab_dir: &Path) -> Result<Option<String>, CliError> {
    let lock_path = lab_dir.join(LOCK_FILE_NAME);
    Ok(read_active_lock(&lock_path)?.map(|lock| lock.scenario))
}

/// Scenario ids whose Namespace Deployment is still present in the Platform Store
/// while no run is active for that Scenario (leftover after keep-on-finish).
///
/// `present_deployments` are Deployment names currently in the Lab Platform Store.
/// When `active` is set, that Scenario is treated as the active run, not a leftover.
pub(crate) fn leftover_scenario_namespaces(
    lab_dir: &Path,
    present_deployments: &[String],
    active: Option<&str>,
) -> Result<Vec<String>, CliError> {
    let recipes = load_selectable_recipes(lab_dir)?;
    let present: std::collections::BTreeSet<&str> =
        present_deployments.iter().map(String::as_str).collect();
    let mut leftovers = Vec::new();
    for recipe in recipes {
        if active.is_some_and(|id| id == recipe.id) {
            continue;
        }
        if present.contains(recipe.namespace.deployment.as_str()) {
            leftovers.push(recipe.id);
        }
    }
    leftovers.sort();
    Ok(leftovers)
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

    // US44 / issue #85: refuse non-Lab / production engine bindings before apply/sync.
    // Runs before Fixture probes so CI can assert isolation without Docker.
    ensure_lab_fixture_engines_for_scenario(lab_dir, scenario)?;

    ensure_fixture_ready_for_scenario(lab_dir).await?;

    let lock = ScenarioLock::acquire(&lock_path, scenario)?;
    let started = Instant::now();
    // Catalog membership already validated; dispatch by id.
    let result = match scenario {
        DIRECT_PIPELINE_ID => run_direct_pipeline(lab_dir).await,
        TRANSFORM_PIPELINE_ID => run_transform_pipeline(lab_dir).await,
        RT_PROJECT_ID => run_rt_project(lab_dir).await,
        RT_FILTER_ID => run_rt_filter(lab_dir).await,
        RT_FIELD_OPS_ID => run_rt_field_ops(lab_dir).await,
        RT_EQUILOOKUP_ID => run_rt_equilookup(lab_dir).await,
        RT_DISTINCT_ADDTOSET_ID => run_rt_distinct_addtoset(lab_dir).await,
        CONCURRENT_SOURCE_WORKLOAD_ID => run_concurrent_source_workload(lab_dir).await,
        BULK_LOAD_ID => run_bulk_load(lab_dir).await,
        IDEMPOTENT_REDELIVERY_ID => run_idempotent_redelivery(lab_dir).await,
        PAUSE_RESUME_ID => run_pause_resume(lab_dir).await,
        REMOVE_PIPELINE_ID => run_remove_pipeline(lab_dir).await,
        CHANGE_PIPELINE_ID => run_change_pipeline(lab_dir).await,
        POISON_QUARANTINE_ID => run_poison_quarantine(lab_dir).await,
        SCHEMA_CHANGE_PAUSE_ID => run_schema_change_pause(lab_dir).await,
        SOURCE_ALIGNMENT_ID => run_source_alignment(lab_dir).await,
        DRIFT_CHECK_ID => run_drift_check(lab_dir).await,
        BOUNDED_BACKPRESSURE_ID => run_bounded_backpressure(lab_dir).await,
        OBSERVABILITY_SURFACE_ID => run_observability_surface(lab_dir).await,
        PLATFORM_STORE_GUARDRAILS_ID => run_platform_store_guardrails(lab_dir).await,
        BACKWARD_COMPATIBLE_UPGRADES_ID => run_backward_compatible_upgrades(lab_dir).await,
        INITIAL_LOAD_THROTTLED_ID => run_initial_load_throttled(lab_dir).await,
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
        RT_FIELD_OPS_ID => remove_rt_field_ops_namespace(lab_dir).await,
        RT_EQUILOOKUP_ID => remove_rt_equilookup_namespace(lab_dir).await,
        RT_DISTINCT_ADDTOSET_ID => remove_rt_distinct_addtoset_namespace(lab_dir).await,
        CONCURRENT_SOURCE_WORKLOAD_ID => remove_concurrent_source_namespace(lab_dir).await,
        BULK_LOAD_ID => remove_bulk_load_namespace(lab_dir).await,
        IDEMPOTENT_REDELIVERY_ID => remove_idempotent_redelivery_namespace(lab_dir).await,
        PAUSE_RESUME_ID => remove_pause_resume_namespace(lab_dir).await,
        REMOVE_PIPELINE_ID => remove_remove_pipeline_namespace(lab_dir).await,
        CHANGE_PIPELINE_ID => remove_change_pipeline_namespace(lab_dir).await,
        POISON_QUARANTINE_ID => remove_poison_quarantine_namespace(lab_dir).await,
        SCHEMA_CHANGE_PAUSE_ID => remove_schema_change_pause_namespace(lab_dir).await,
        SOURCE_ALIGNMENT_ID => remove_source_alignment_namespace(lab_dir).await,
        DRIFT_CHECK_ID => remove_drift_check_namespace(lab_dir).await,
        BOUNDED_BACKPRESSURE_ID => remove_bounded_backpressure_namespace(lab_dir).await,
        OBSERVABILITY_SURFACE_ID => remove_observability_surface_namespace(lab_dir).await,
        PLATFORM_STORE_GUARDRAILS_ID => remove_platform_store_guardrails_namespace(lab_dir).await,
        BACKWARD_COMPATIBLE_UPGRADES_ID => {
            remove_backward_compatible_upgrades_namespace(lab_dir).await
        }
        INITIAL_LOAD_THROTTLED_ID => remove_initial_load_throttled_namespace(lab_dir).await,
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
    scenario_config_path(lab_dir, scenario_id, "deployment.yaml")
}

fn scenario_config_path(
    lab_dir: &Path,
    scenario_id: &str,
    filename: &str,
) -> Result<PathBuf, CliError> {
    let path = lab_dir
        .join("scenarios")
        .join(scenario_id)
        .join(filename);
    if !path.is_file() {
        return Err(CliError::Failed(format!(
            "Lab Scenario config not found at {} \
             (expected under the repo `lab/scenarios/{scenario_id}/` directory)",
            path.display()
        )));
    }
    Ok(path)
}

/// Loopback hostnames that identify Lab Fixture engines on the operator machine.
fn is_lab_loopback_host(host: &str) -> bool {
    matches!(
        host.trim().to_ascii_lowercase().as_str(),
        "127.0.0.1" | "localhost"
    )
}

/// Enforce that Scenario Source/Target bindings are Lab Fixture engines only (US44 / #85).
///
/// Lab Scenario apply/sync must never drive customer/production databases. Ordinary
/// `migraloop apply` / `sync` remains the path for real Deployments.
fn ensure_lab_fixture_engines_for_scenario(
    lab_dir: &Path,
    scenario: &str,
) -> Result<(), CliError> {
    let recipe = load_selectable_recipes(lab_dir)?
        .into_iter()
        .find(|recipe| recipe.id == scenario)
        .ok_or_else(|| unknown_or_incomplete_scenario_error(scenario, lab_dir))?;
    let config_path = lab_dir
        .join("scenarios")
        .join(scenario)
        .join(&recipe.deployment_config);
    let doc = load_deployment_config(&config_path)?;

    let source = &doc.spec.source;
    let target = &doc.spec.target;
    let mut mismatches = Vec::new();

    if source.kind != "oracle" {
        mismatches.push(format!(
            "source.kind=`{}` (Lab Fixture Source is oracle)",
            source.kind
        ));
    }
    if !is_lab_loopback_host(&source.host) {
        mismatches.push(format!(
            "source.host=`{}` (Lab Fixture Source is {LAB_ORACLE_HOST} / localhost)",
            source.host
        ));
    }
    if source.port != i32::from(LAB_ORACLE_PORT) {
        mismatches.push(format!(
            "source.port={} (Lab Fixture Source is {LAB_ORACLE_PORT})",
            source.port
        ));
    }
    if source.database != LAB_ORACLE_SERVICE {
        mismatches.push(format!(
            "source.database=`{}` (Lab Fixture Source is {LAB_ORACLE_SERVICE})",
            source.database
        ));
    }
    if source.username != LAB_ORACLE_USER {
        mismatches.push(format!(
            "source.username=`{}` (Lab Fixture Source is {LAB_ORACLE_USER})",
            source.username
        ));
    }

    if target.kind != "mongodb" {
        mismatches.push(format!(
            "target.kind=`{}` (Lab Fixture Target is mongodb)",
            target.kind
        ));
    }
    if !is_lab_loopback_host(&target.host) {
        mismatches.push(format!(
            "target.host=`{}` (Lab Fixture Target is {LAB_MONGO_HOST} / localhost)",
            target.host
        ));
    }
    if target.port != i32::from(LAB_MONGO_PORT) {
        mismatches.push(format!(
            "target.port={} (Lab Fixture Target is {LAB_MONGO_PORT})",
            target.port
        ));
    }
    if target.database != LAB_MONGO_DATABASE {
        mismatches.push(format!(
            "target.database=`{}` (Lab Fixture Target is {LAB_MONGO_DATABASE})",
            target.database
        ));
    }
    if target.username != LAB_MONGO_USER {
        mismatches.push(format!(
            "target.username=`{}` (Lab Fixture Target is {LAB_MONGO_USER})",
            target.username
        ));
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    Err(CliError::Failed(format!(
        "Lab Scenario run refused: Source/Target must be Lab-provisioned Fixture engines only \
         (Local Sync Lab will not apply/sync against customer or production databases). \
         Non-Lab engine binding(s): {}. \
         Restore Scenario configs to Lab Fixture endpoints (`migraloop lab status`), \
         or use ordinary `migraloop apply` / `migraloop sync` for real Deployments.",
        mismatches.join("; ")
    )))
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
    // Orders Transform after mutate: cust1=20+15 → sum 35 / count 2 / min 15 / max 20 / avg 17.5;
    // cust2=50 (order 1 deleted, order 3 updated).
    let derived_ok = inspect_mentions_amount(&derived_after, "35")
        && inspect_mentions_amount(&derived_after, "50")
        && !inspect_mentions_amount(&derived_after, "30")
        && managed_field_present(&derived_after, "ORDER_COUNT", "2")
        && managed_field_present(&derived_after, "ORDER_COUNT", "1")
        && (managed_field_present(&derived_after, "MIN_AMOUNT", "15")
            || managed_field_present(&derived_after, "MIN_AMOUNT", "15.00"))
        && (managed_field_present(&derived_after, "MAX_AMOUNT", "20")
            || managed_field_present(&derived_after, "MAX_AMOUNT", "20.00"))
        && (managed_field_present(&derived_after, "AVG_AMOUNT", "17.5")
            || managed_field_present(&derived_after, "AVG_AMOUNT", "17.50"));
    let totals_target_ok = inspect_mentions_amount(&totals_target, "35")
        && inspect_mentions_amount(&totals_target, "50")
        && !inspect_mentions_amount(&totals_target, "30")
        && managed_field_present(&totals_target, "ORDER_COUNT", "2")
        && managed_field_present(&totals_target, "ORDER_COUNT", "1")
        && (managed_field_present(&totals_target, "MIN_AMOUNT", "15")
            || managed_field_present(&totals_target, "MIN_AMOUNT", "15.00"))
        && (managed_field_present(&totals_target, "MAX_AMOUNT", "20")
            || managed_field_present(&totals_target, "MAX_AMOUNT", "20.00"))
        && (managed_field_present(&totals_target, "AVG_AMOUNT", "17.5")
            || managed_field_present(&totals_target, "AVG_AMOUNT", "17.50"));

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

    // After mutate: Alicia + flipped Bob + Carol ACTIVE=1; Dana ACTIVE=0 stays filtered out.
    let derived_ok = managed_field_present(&derived_after, "NAME", "Alicia")
        && managed_field_present(&derived_after, "NAME", "Bob")
        && managed_field_present(&derived_after, "NAME", "Carol")
        && !managed_field_present(&derived_after, "NAME", "Dana");
    let target_ok = managed_field_present(&target_after, "NAME", "Alicia")
        && managed_field_present(&target_after, "NAME", "Bob")
        && managed_field_present(&target_after, "NAME", "Carol")
        && !managed_field_present(&target_after, "NAME", "Dana");

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
    // Alicia stays ACTIVE=1; Bob flips 0→1 (must enter Derived); Carol ACTIVE=1;
    // Dana ACTIVE=0 must stay filtered out.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_FILTER_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
UPDATE {RT_FILTER_TABLE} SET ACTIVE = 1 WHERE ID = 2;\n\
INSERT INTO {RT_FILTER_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
INSERT INTO {RT_FILTER_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (4, 'Dana', 'dana@example.com', 0);\n\
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


async fn run_rt_field_ops(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {RT_FIELD_OPS_ID}");
    println!(
        "Scenario Namespace: table={RT_FIELD_OPS_TABLE} \
collection={RT_FIELD_OPS_COLLECTION} deployment={RT_FIELD_OPS_DEPLOYMENT}"
    );

    prepare_rt_field_ops_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, RT_FIELD_OPS_ID)?;
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
        || apply_out.contains(RT_FIELD_OPS_PIPELINE))
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
            RT_FIELD_OPS_PIPELINE,
        ],
    )
    .await?;
    // project+remove+rename+addFields+filter: Alice only; EMAIL gone; customerName/displayName/source present.
    if !(managed_field_present(&derived_after_apply, "customerName", "Alice")
        && managed_field_present(&derived_after_apply, "displayName", "Alice")
        && managed_field_present(&derived_after_apply, "source", "oracle")
        && !managed_field_present(&derived_after_apply, "customerName", "Bob")
        && !inspect_mentions_email_field(&derived_after_apply))
    {
        return Err(CliError::Failed(format!(
            "Initial Load field-ops Derived check failed \
(expected Alice customerName/displayName/source, no EMAIL, no Bob):\n{derived_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source insert/update/delete...");
    mutate_rt_field_ops_source(lab_dir).await?;

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
            RT_FIELD_OPS_PIPELINE,
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
            RT_FIELD_OPS_COLLECTION,
        ],
    )
    .await?;

    let derived_ok = managed_field_present(&derived_after, "customerName", "Alicia")
        && managed_field_present(&derived_after, "displayName", "Alicia")
        && managed_field_present(&derived_after, "customerName", "Carol")
        && managed_field_present(&derived_after, "source", "oracle")
        && !managed_field_present(&derived_after, "customerName", "Bob")
        && !inspect_mentions_email_field(&derived_after);
    let target_ok = managed_field_present(&target_after, "customerName", "Alicia")
        && managed_field_present(&target_after, "displayName", "Alicia")
        && managed_field_present(&target_after, "customerName", "Carol")
        && managed_field_present(&target_after, "source", "oracle")
        && !managed_field_present(&target_after, "customerName", "Bob")
        && !inspect_mentions_email_field(&target_after);

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(derived_ok && target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after field-ops insert/update/delete.\n\
Derived:\n{derived_after}\nTarget:\n{target_after}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (addFields/rename/remove Derived + Target Managed outcomes)"
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

async fn remove_rt_field_ops_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={RT_FIELD_OPS_TABLE}, collection={RT_FIELD_OPS_COLLECTION}, \
          deployment={RT_FIELD_OPS_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_FIELD_OPS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table {RT_FIELD_OPS_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{RT_FIELD_OPS_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {RT_FIELD_OPS_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, RT_FIELD_OPS_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{RT_FIELD_OPS_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_rt_field_ops_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_rt_field_ops_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {RT_FIELD_OPS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1) NOT NULL\n\
);\n\
ALTER TABLE {RT_FIELD_OPS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {RT_FIELD_OPS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {RT_FIELD_OPS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare rt-field-ops Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_rt_field_ops_source(lab_dir: &Path) -> Result<(), CliError> {
    // EMAIL-only update is unused after remove; NAME rename path must still update;
    // Carol ACTIVE=1 enters; Bob deleted.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_FIELD_OPS_TABLE} SET EMAIL = 'alice-only@example.com' WHERE ID = 1;\n\
UPDATE {RT_FIELD_OPS_TABLE} SET NAME = 'Alicia' WHERE ID = 1;\n\
INSERT INTO {RT_FIELD_OPS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {RT_FIELD_OPS_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source insert/update/delete for rt-field-ops Lab Scenario:\n{err}"
            ))
        })
}

async fn run_rt_equilookup(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {RT_EQUILOOKUP_ID}");
    println!(
        "Scenario Namespace: tables={RT_EQUILOOKUP_CUSTOMERS_TABLE},{RT_EQUILOOKUP_ORDERS_TABLE} \
collection={RT_EQUILOOKUP_COLLECTION} deployment={RT_EQUILOOKUP_DEPLOYMENT}"
    );

    prepare_rt_equilookup_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, RT_EQUILOOKUP_ID)?;
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
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.contains(RT_EQUILOOKUP_CUSTOMERS_TABLE)
        && apply_out.contains(RT_EQUILOOKUP_ORDERS_TABLE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply must Initial Load both equiLookup Bases:\n{apply_out}"
        )));
    }
    if !(apply_out.to_ascii_lowercase().contains("derived")
        || apply_out.contains(RT_EQUILOOKUP_PIPELINE))
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
            RT_EQUILOOKUP_PIPELINE,
        ],
    )
    .await?;
    if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
        && derived_after_apply.contains("orders")
        && (derived_after_apply.contains("42.50") || derived_after_apply.contains("42.5"))
        && managed_field_present(&derived_after_apply, "NAME", "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load equiLookup Derived check failed \
(expected Alice/Bob with embedded orders):\n{derived_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source primary + foreign mutations...");
    mutate_rt_equilookup_source(lab_dir).await?;

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
            RT_EQUILOOKUP_PIPELINE,
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
            RT_EQUILOOKUP_COLLECTION,
        ],
    )
    .await?;

    let derived_ok = managed_field_present(&derived_after, "NAME", "Alicia")
        && derived_after.contains("orders")
        && (derived_after.contains("50.00") || derived_after.contains("50"))
        && !derived_after.contains("42.50")
        && managed_field_present(&derived_after, "NAME", "Bob");
    let target_ok = managed_field_present(&target_after, "NAME", "Alicia")
        && target_after.contains("orders")
        && (target_after.contains("50.00") || target_after.contains("50"))
        && managed_field_present(&target_after, "NAME", "Bob");

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(derived_ok && target_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after equiLookup primary/foreign updates.\n\
Derived:\n{derived_after}\nTarget:\n{target_after}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (equiLookup multi-Base Derived + Target Managed outcomes)"
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

async fn remove_rt_equilookup_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (tables={RT_EQUILOOKUP_CUSTOMERS_TABLE},{RT_EQUILOOKUP_ORDERS_TABLE}, \
          collection={RT_EQUILOOKUP_COLLECTION}, deployment={RT_EQUILOOKUP_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_EQUILOOKUP_ORDERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_EQUILOOKUP_CUSTOMERS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace tables for rt-equilookup:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{RT_EQUILOOKUP_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {RT_EQUILOOKUP_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, RT_EQUILOOKUP_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{RT_EQUILOOKUP_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_rt_equilookup_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_rt_equilookup_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {RT_EQUILOOKUP_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200)\n\
);\n\
ALTER TABLE {RT_EQUILOOKUP_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
CREATE TABLE {RT_EQUILOOKUP_ORDERS_TABLE} (\n\
  ORDER_ID NUMBER(10) PRIMARY KEY,\n\
  CUSTOMER_ID NUMBER(10) NOT NULL,\n\
  AMOUNT NUMBER(12,2) NOT NULL\n\
);\n\
ALTER TABLE {RT_EQUILOOKUP_ORDERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {RT_EQUILOOKUP_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (1, 'Alice', 'alice@example.com');\n\
INSERT INTO {RT_EQUILOOKUP_CUSTOMERS_TABLE} (ID, NAME, EMAIL) VALUES (2, 'Bob', 'bob@example.com');\n\
INSERT INTO {RT_EQUILOOKUP_ORDERS_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT) VALUES (100, 1, 42.50);\n\
INSERT INTO {RT_EQUILOOKUP_ORDERS_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT) VALUES (101, 1, 10.00);\n\
INSERT INTO {RT_EQUILOOKUP_ORDERS_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT) VALUES (200, 2, 5.00);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare rt-equilookup Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_rt_equilookup_source(lab_dir: &Path) -> Result<(), CliError> {
    // Primary NAME change + foreign AMOUNT change must both refresh the joined Derived.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_EQUILOOKUP_CUSTOMERS_TABLE} SET NAME = 'Alicia' WHERE ID = 1;\n\
UPDATE {RT_EQUILOOKUP_ORDERS_TABLE} SET AMOUNT = 50.00 WHERE ORDER_ID = 100;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for rt-equilookup Lab Scenario:\n{err}"
            ))
        })
}

async fn run_rt_distinct_addtoset(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {RT_DISTINCT_ADDTOSET_ID}");
    println!(
        "Scenario Namespace: table={RT_DISTINCT_ADDTOSET_TABLE} \
collections={RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION},{RT_DISTINCT_ADDTOSET_ADD_COLLECTION} \
deployment={RT_DISTINCT_ADDTOSET_DEPLOYMENT}"
    );

    prepare_rt_distinct_addtoset_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, RT_DISTINCT_ADDTOSET_ID)?;
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
    if !(apply_out.contains(RT_DISTINCT_ADDTOSET_DISTINCT_PIPELINE)
        && apply_out.contains(RT_DISTINCT_ADDTOSET_ADD_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply must materialize both distinct and addToSet Pipelines:\n{apply_out}"
        )));
    }

    let distinct_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_DISTINCT_ADDTOSET_DISTINCT_PIPELINE,
        ],
    )
    .await?;
    let add_after_apply = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_DISTINCT_ADDTOSET_ADD_PIPELINE,
        ],
    )
    .await?;
    if !(distinct_after_apply.contains("\"CUSTOMER_ID\": 1")
        && distinct_after_apply.contains("\"CUSTOMER_ID\": 2")
        && add_after_apply.contains("42.50")
        && add_after_apply.contains("10.00")
        && add_after_apply.contains("5.00"))
    {
        return Err(CliError::Failed(format!(
            "Initial Load distinct/addToSet Derived check failed.\n\
distinct:\n{distinct_after_apply}\naddToSet:\n{add_after_apply}"
        )));
    }

    println!("Lab Scenario: driving Source mutations (unused/duplicate/new/key-move)...");
    mutate_rt_distinct_addtoset_source(lab_dir).await?;

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

    let distinct_after = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_DISTINCT_ADDTOSET_DISTINCT_PIPELINE,
        ],
    )
    .await?;
    let add_after = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            RT_DISTINCT_ADDTOSET_ADD_PIPELINE,
        ],
    )
    .await?;
    let distinct_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION,
        ],
    )
    .await?;
    let add_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            RT_DISTINCT_ADDTOSET_ADD_COLLECTION,
        ],
    )
    .await?;

    let distinct_ok = distinct_after.contains("\"CUSTOMER_ID\": 1")
        && distinct_after.contains("\"CUSTOMER_ID\": 3")
        && !distinct_after.contains("\"CUSTOMER_ID\": 2")
        && distinct_target.contains("\"CUSTOMER_ID\": 1")
        && distinct_target.contains("\"CUSTOMER_ID\": 3")
        && !distinct_target.contains("\"CUSTOMER_ID\": 2");
    let add_ok = add_after.contains("7.00")
        && add_after.contains("42.50")
        && add_after.contains("10.00")
        && add_after.contains("\"CUSTOMER_ID\": 3")
        && add_after.contains("5.00")
        && !add_after.contains("\"CUSTOMER_ID\": 2")
        && add_target.contains("7.00")
        && add_target.contains("\"CUSTOMER_ID\": 3");

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);

    if !(distinct_ok && add_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after distinct/addToSet Source mutations.\n\
distinct Derived:\n{distinct_after}\ndistinct Target:\n{distinct_target}\n\
addToSet Derived:\n{add_after}\naddToSet Target:\n{add_target}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed (distinct + addToSet Derived/Target outcomes)"
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

async fn remove_rt_distinct_addtoset_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={RT_DISTINCT_ADDTOSET_TABLE}, \
          collections={RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION},{RT_DISTINCT_ADDTOSET_ADD_COLLECTION}, \
          deployment={RT_DISTINCT_ADDTOSET_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {RT_DISTINCT_ADDTOSET_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for rt-distinct-addtoset:\n{err}"
            ))
        })?;

    for collection in [
        RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION,
        RT_DISTINCT_ADDTOSET_ADD_COLLECTION,
    ] {
        let js = format!("db.getCollection('{collection}').drop()");
        mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Mongo Scenario Namespace collection {collection}:\n{err}"
            ))
        })?;
    }

    delete_deployment(LAB_PLATFORM_STORE_URL, RT_DISTINCT_ADDTOSET_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{RT_DISTINCT_ADDTOSET_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_rt_distinct_addtoset_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_rt_distinct_addtoset_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {RT_DISTINCT_ADDTOSET_TABLE} (\n\
  ORDER_ID NUMBER(10) PRIMARY KEY,\n\
  CUSTOMER_ID NUMBER(10) NOT NULL,\n\
  AMOUNT NUMBER(12,2) NOT NULL,\n\
  ADDRESS VARCHAR2(200)\n\
);\n\
ALTER TABLE {RT_DISTINCT_ADDTOSET_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {RT_DISTINCT_ADDTOSET_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (100, 1, 42.50, '1 Main St');\n\
INSERT INTO {RT_DISTINCT_ADDTOSET_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (101, 1, 10.00, '1 Main St');\n\
INSERT INTO {RT_DISTINCT_ADDTOSET_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (200, 2, 5.00, '2 Side Rd');\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare rt-distinct-addtoset Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_rt_distinct_addtoset_source(lab_dir: &Path) -> Result<(), CliError> {
    // Unused ADDRESS; duplicate CUSTOMER_ID+AMOUNT; new AMOUNT; last-row key move 2→3.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {RT_DISTINCT_ADDTOSET_TABLE} SET ADDRESS = '1 Main Ave' WHERE ORDER_ID = 100;\n\
INSERT INTO {RT_DISTINCT_ADDTOSET_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (102, 1, 42.50, '1 Main Ave');\n\
INSERT INTO {RT_DISTINCT_ADDTOSET_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (103, 1, 7.00, '1 Main Ave');\n\
UPDATE {RT_DISTINCT_ADDTOSET_TABLE} SET CUSTOMER_ID = 3 WHERE ORDER_ID = 200;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for rt-distinct-addtoset Lab Scenario:\n{err}"
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

/// Issue #86 / PRD #55 US49: exercise at-least-once Delivery via duplicate-safe re-Delivery
/// on the real product apply path inside a Scenario Namespace.
async fn run_idempotent_redelivery(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {IDEMPOTENT_REDELIVERY_ID}");
    println!(
        "Scenario Namespace: table={IDEMPOTENT_REDELIVERY_TABLE} \
collection={IDEMPOTENT_REDELIVERY_COLLECTION} deployment={IDEMPOTENT_REDELIVERY_DEPLOYMENT}"
    );

    prepare_idempotent_redelivery_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, IDEMPOTENT_REDELIVERY_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: driving Source insert/update/delete...");
    mutate_idempotent_redelivery_source(lab_dir).await?;

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

    let target_before = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            IDEMPOTENT_REDELIVERY_COLLECTION,
        ],
    )
    .await?;
    let docs_before = parse_target_document_count(&target_before).ok_or_else(|| {
        CliError::Failed(format!(
            "Lab Scenario could not parse Target document count before re-Delivery:\n{target_before}"
        ))
    })?;
    let before_ok = managed_name_present(&target_before, "Alicia")
        && managed_name_present(&target_before, "Carol")
        && !managed_name_present(&target_before, "Bob");
    if !before_ok || docs_before != 2 {
        return Err(CliError::Failed(format!(
            "pre-redelivery Managed Target baseline failed (expected Alicia+Carol, Bob absent, documents=2):\n{target_before}"
        )));
    }

    // Plant a non-Managed field so re-Delivery proves Managed-only upsert (US48-adjacent / US49).
    println!(
        "Lab Scenario: planting non-Managed Target field before duplicate-safe re-Delivery..."
    );
    plant_idempotent_redelivery_operator_note(lab_dir).await?;

    // Lab orchestration only: mark Pipeline Delivery pending so the next real `apply`
    // re-Delivers current Base Output Identities (at-least-once / upsert-by-identity).
    println!(
        "Lab Scenario: resetting Pipeline Delivery status to force duplicate-safe re-Delivery..."
    );
    update_pipeline_delivery_status(
        LAB_PLATFORM_STORE_URL,
        IDEMPOTENT_REDELIVERY_DEPLOYMENT,
        IDEMPOTENT_REDELIVERY_PIPELINE,
        "pending",
    )
    .await
    .map_err(|err| {
        CliError::Failed(format!(
            "Failed to reset Pipeline Delivery status for re-Delivery exercise:\n{err}"
        ))
    })?;

    println!("Lab Scenario: re-apply via real product path (duplicate-safe re-Delivery)...");
    let reapply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !reapply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario re-apply must perform Delivery (duplicate-safe re-Delivery):\n{reapply_out}"
        )));
    }
    // Must not reload existing Base on re-apply (ADR-0019) — re-Delivery of current Base only.
    if reapply_out.contains("Initial Load")
        || reapply_out.to_ascii_lowercase().contains("initial_load")
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario re-apply must not reload Base (expected Delivery-only re-run):\n{reapply_out}"
        )));
    }
    let redelivery_ops = count_delivery_ops(&reapply_out);
    if redelivery_ops < 2 {
        return Err(CliError::Failed(format!(
            "Lab Scenario re-apply must re-Deliver current Base Output Identities \
             (expected ≥2 Delivery ops, got {redelivery_ops}):\n{reapply_out}"
        )));
    }

    let base_after = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            IDEMPOTENT_REDELIVERY_TABLE,
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
            IDEMPOTENT_REDELIVERY_COLLECTION,
        ],
    )
    .await?;
    let docs_after = parse_target_document_count(&target_after).ok_or_else(|| {
        CliError::Failed(format!(
            "Lab Scenario could not parse Target document count after re-Delivery:\n{target_after}"
        ))
    })?;

    let base_ok = managed_name_present(&base_after, "Alicia")
        && managed_name_present(&base_after, "Carol")
        && !managed_name_present(&base_after, "Bob");
    let target_ok = managed_name_present(&target_after, "Alicia")
        && managed_name_present(&target_after, "Carol")
        && !managed_name_present(&target_after, "Bob");
    let count_ok = docs_after == docs_before && docs_after == 2;
    let note_ok = target_after.contains(IDEMPOTENT_REDELIVERY_OPERATOR_NOTE);

    let rows_applied = count_delivery_ops(&apply_out)
        + count_delivery_ops(&sync_out)
        + count_delivery_ops(&reapply_out);

    if !(base_ok && target_ok && count_ok && note_ok) {
        return Err(CliError::Failed(format!(
            "correctness checks failed after duplicate-safe re-Delivery \
             (base_ok={base_ok} target_ok={target_ok} count_ok={count_ok} \
             docs_before={docs_before} docs_after={docs_after} note_ok={note_ok}).\n\
             Base:\n{base_after}\nTarget:\n{target_after}"
        )));
    }

    println!(
        "Lab Scenario: correctness checks passed \
         (Managed outcomes stable; document count={docs_after}; non-Managed field preserved)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) and Delivery complete");
    }
    println!("Lab Scenario: duplicate-safe re-Delivery complete on real product apply path");

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

/// Issue #19 / ADR-0007: pause stops Delivery for one Pipeline; resume catch-up
/// from durable Base; other Pipelines keep Delivering on the real product path.
async fn run_pause_resume(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {PAUSE_RESUME_ID}");
    println!(
        "Scenario Namespace: tables={PAUSE_RESUME_CUSTOMERS_TABLE},{PAUSE_RESUME_ORDERS_TABLE} \
collections={PAUSE_RESUME_CUSTOMERS_COLLECTION},{PAUSE_RESUME_ORDERS_COLLECTION} \
deployment={PAUSE_RESUME_DEPLOYMENT}"
    );

    prepare_pause_resume_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, PAUSE_RESUME_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: pause Pipeline {PAUSE_RESUME_CUSTOMERS_PIPELINE} via CLI...");
    let pause_out = run_product_cli(
        &bin,
        &[
            "pause",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            PAUSE_RESUME_CUSTOMERS_PIPELINE,
            "--deployment",
            PAUSE_RESUME_DEPLOYMENT,
        ],
    )
    .await?;
    if !pause_out.to_ascii_lowercase().contains("paused") {
        return Err(CliError::Failed(format!(
            "Lab Scenario pause did not report paused Pipeline:\n{pause_out}"
        )));
    }

    println!("Lab Scenario: driving Source mutations on both Namespace tables...");
    mutate_pause_resume_source(lab_dir).await?;

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
    if sync_out.contains(&format!("Delivery complete: Pipeline {PAUSE_RESUME_CUSTOMERS_PIPELINE}"))
    {
        return Err(CliError::Failed(format!(
            "paused Pipeline must not Deliver during sync:\n{sync_out}"
        )));
    }
    if !(sync_out.contains(&format!("Delivery complete: Pipeline {PAUSE_RESUME_ORDERS_PIPELINE}"))
        || sync_out.contains(PAUSE_RESUME_ORDERS_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "unaffected Pipeline must still Deliver during sync:\n{sync_out}"
        )));
    }

    let customers_target_paused = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            PAUSE_RESUME_CUSTOMERS_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&customers_target_paused, "Alice")
        && managed_name_present(&customers_target_paused, "Bob")
        && !managed_name_present(&customers_target_paused, "Alicia"))
    {
        return Err(CliError::Failed(format!(
            "while paused, customers Target must retain Initial Load (Alice/Bob, not Alicia):\n{customers_target_paused}"
        )));
    }

    let orders_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            PAUSE_RESUME_ORDERS_COLLECTION,
        ],
    )
    .await?;
    if !(orders_target.contains("50.00") || orders_target.contains("\"50\"")) {
        return Err(CliError::Failed(format!(
            "unaffected orders Pipeline must Deliver Incremental update:\n{orders_target}"
        )));
    }

    println!("Lab Scenario: resume Pipeline {PAUSE_RESUME_CUSTOMERS_PIPELINE} (catch-up Delivery)...");
    let resume_out = run_product_cli(
        &bin,
        &[
            "resume",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            PAUSE_RESUME_CUSTOMERS_PIPELINE,
            "--deployment",
            PAUSE_RESUME_DEPLOYMENT,
        ],
    )
    .await?;
    if !(resume_out.to_ascii_lowercase().contains("resum")
        && resume_out.to_ascii_lowercase().contains("delivery"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario resume must catch up Delivery from durable state:\n{resume_out}"
        )));
    }

    let customers_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            PAUSE_RESUME_CUSTOMERS_COLLECTION,
        ],
    )
    .await?;
    let customers_ok = managed_name_present(&customers_target, "Alicia")
        && managed_name_present(&customers_target, "Carol")
        && !managed_name_present(&customers_target, "Bob");
    if !customers_ok {
        return Err(CliError::Failed(format!(
            "after resume, customers Target must match durable Base (Alicia+Carol, Bob absent):\n{customers_target}"
        )));
    }

    let status_out = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if status_out
        .lines()
        .any(|line| line.contains(PAUSE_RESUME_CUSTOMERS_PIPELINE) && line.contains("paused"))
    {
        return Err(CliError::Failed(format!(
            "status must not keep customers Pipeline paused after resume:\n{status_out}"
        )));
    }
    if !status_out.contains(PAUSE_RESUME_ORDERS_PIPELINE) {
        return Err(CliError::Failed(format!(
            "status must still list unaffected orders Pipeline:\n{status_out}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out)
        + count_delivery_ops(&sync_out)
        + count_delivery_ops(&resume_out);

    println!(
        "Lab Scenario: correctness checks passed \
         (pause stopped customers Delivery; resume catch-up; orders unaffected)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) complete");
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

async fn remove_pause_resume_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (tables={PAUSE_RESUME_CUSTOMERS_TABLE},{PAUSE_RESUME_ORDERS_TABLE}, \
          collections={PAUSE_RESUME_CUSTOMERS_COLLECTION},{PAUSE_RESUME_ORDERS_COLLECTION}, \
          deployment={PAUSE_RESUME_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {PAUSE_RESUME_CUSTOMERS_TABLE} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {PAUSE_RESUME_ORDERS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace tables for pause-resume:\n{err}"
            ))
        })?;

    let js = format!(
        "db.getCollection('{PAUSE_RESUME_CUSTOMERS_COLLECTION}').drop();\n\
db.getCollection('{PAUSE_RESUME_ORDERS_COLLECTION}').drop();"
    );
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collections for pause-resume:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, PAUSE_RESUME_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{PAUSE_RESUME_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_pause_resume_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_pause_resume_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {PAUSE_RESUME_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {PAUSE_RESUME_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {PAUSE_RESUME_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {PAUSE_RESUME_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
CREATE TABLE {PAUSE_RESUME_ORDERS_TABLE} (\n\
  ORDER_ID NUMBER(10) PRIMARY KEY,\n\
  CUSTOMER_ID NUMBER(10) NOT NULL,\n\
  AMOUNT NUMBER(12,2) NOT NULL,\n\
  ADDRESS VARCHAR2(200)\n\
);\n\
ALTER TABLE {PAUSE_RESUME_ORDERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {PAUSE_RESUME_ORDERS_TABLE} (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) \
VALUES (100, 1, 42.50, '1 Main St');\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare pause-resume Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_pause_resume_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {PAUSE_RESUME_CUSTOMERS_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {PAUSE_RESUME_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {PAUSE_RESUME_CUSTOMERS_TABLE} WHERE ID = 2;\n\
UPDATE {PAUSE_RESUME_ORDERS_TABLE} SET AMOUNT = 50.00, ADDRESS = '1 Main Ave' WHERE ORDER_ID = 100;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for pause-resume:\n{err}"
            ))
        })
}

async fn run_poison_quarantine(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {POISON_QUARANTINE_ID}");
    println!(
        "Scenario Namespace: table={POISON_QUARANTINE_TABLE} \
collection={POISON_QUARANTINE_COLLECTION} deployment={POISON_QUARANTINE_DEPLOYMENT}"
    );

    prepare_poison_quarantine_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, POISON_QUARANTINE_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: driving Source mutations (update/insert/delete)...");
    mutate_poison_quarantine_source(lab_dir).await?;

    println!(
        "Lab Scenario: sync Incremental Capture + Delivery with poison injection \
         for Output Identity {POISON_QUARANTINE_IDENTITY}..."
    );
    let sync_out = run_product_cli_with_env(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[
            (
                "MIGRALOOP_DELIVERY_POISON_IDENTITIES",
                POISON_QUARANTINE_IDENTITY,
            ),
            (
                "MIGRALOOP_POISON_MAX_ATTEMPTS",
                POISON_QUARANTINE_MAX_ATTEMPTS,
            ),
        ],
    )
    .await?;
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };
    let sync_lower = sync_out.to_ascii_lowercase();
    if !(sync_lower.contains("quarantine") && sync_lower.contains("alert")) {
        return Err(CliError::Failed(format!(
            "sync must quarantine poison identity with an ALERT:\n{sync_out}"
        )));
    }
    if sync_lower
        .lines()
        .any(|line| line.contains("paused") && line.contains(POISON_QUARANTINE_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "poison quarantine must not pause the Pipeline:\n{sync_out}"
        )));
    }

    let target_out = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            POISON_QUARANTINE_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&target_out, "Alice")
        && !managed_name_present(&target_out, "Alicia")
        && managed_name_present(&target_out, "Carol")
        && !managed_name_present(&target_out, "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Target must keep Alice for quarantined identity 1, Deliver Carol, delete Bob:\n{target_out}"
        )));
    }

    let status_out = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let status_lower = status_out.to_ascii_lowercase();
    if !(status_out.contains("Delivery Health: unhealthy")
        && status_out.contains(POISON_QUARANTINE_PIPELINE)
        && status_lower.contains("quarantine")
        && (status_out.contains("identity=1") || status_lower.contains("identity=1")))
    {
        return Err(CliError::Failed(format!(
            "status must show Delivery Health unhealthy with quarantined identity=1:\n{status_out}"
        )));
    }
    if !(status_lower.contains("unhealthy") || status_lower.contains("not aligned")) {
        return Err(CliError::Failed(format!(
            "quarantined keys must be marked unhealthy/not aligned:\n{status_out}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (poison identity quarantined; Pipeline continued; status unhealthy)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) complete");
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

async fn remove_poison_quarantine_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={POISON_QUARANTINE_TABLE}, collection={POISON_QUARANTINE_COLLECTION}, \
          deployment={POISON_QUARANTINE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {POISON_QUARANTINE_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for poison-quarantine:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{POISON_QUARANTINE_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for poison-quarantine:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, POISON_QUARANTINE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{POISON_QUARANTINE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_poison_quarantine_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_poison_quarantine_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {POISON_QUARANTINE_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {POISON_QUARANTINE_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {POISON_QUARANTINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {POISON_QUARANTINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare poison-quarantine Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_poison_quarantine_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {POISON_QUARANTINE_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {POISON_QUARANTINE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {POISON_QUARANTINE_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for poison-quarantine:\n{err}"
            ))
        })
}


async fn run_bounded_backpressure(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {BOUNDED_BACKPRESSURE_ID}");
    println!(
        "Scenario Namespace: table={BOUNDED_BACKPRESSURE_TABLE} \
collection={BOUNDED_BACKPRESSURE_COLLECTION} deployment={BOUNDED_BACKPRESSURE_DEPLOYMENT}"
    );

    prepare_bounded_backpressure_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, BOUNDED_BACKPRESSURE_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    println!(
        "Lab Scenario: inserting Source backlog ({BOUNDED_BACKPRESSURE_BACKLOG} rows)..."
    );
    insert_bounded_backpressure_backlog(lab_dir).await?;

    println!(
        "Lab Scenario: sync under Downstream delay with queue capacity={} \
(fail after {} durable checkpoint)...",
        BOUNDED_BACKPRESSURE_CAPACITY, BOUNDED_BACKPRESSURE_FAIL_AFTER
    );
    let (slow_ok, slow_out) = run_product_cli_allow_fail(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[
            (
                "MIGRALOOP_SYNC_QUEUE_CAPACITY",
                BOUNDED_BACKPRESSURE_CAPACITY,
            ),
            (
                "MIGRALOOP_DELIVERY_DELAY_MS",
                BOUNDED_BACKPRESSURE_DELAY_MS,
            ),
            (
                "MIGRALOOP_SYNC_FAIL_AFTER_CHANGES",
                BOUNDED_BACKPRESSURE_FAIL_AFTER,
            ),
        ],
    )
    .await?;
    if slow_ok {
        return Err(CliError::Failed(format!(
            "expected mid-sync stop under Downstream slowness (FAIL_AFTER), got success:\n{slow_out}"
        )));
    }
    if !slow_out.to_ascii_lowercase().contains("logminer") {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{slow_out}"
        )));
    }
    let slow_lower = slow_out.to_ascii_lowercase();
    if !(slow_out.contains("Backpressure:") || slow_lower.contains("backpressure")) {
        return Err(CliError::Failed(format!(
            "expected Backpressure signal while Downstream is slow:\n{slow_out}"
        )));
    }
    let mut peak_depth = 0i32;
    for line in slow_out.lines() {
        if let Some(rest) = line.split("queue_depth=").nth(1) {
            if let Some(n) = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<i32>().ok())
            {
                peak_depth = peak_depth.max(n);
            }
        }
    }
    if peak_depth <= 0 || peak_depth > 2 {
        return Err(CliError::Failed(format!(
            "queue_depth must stay within capacity=2 under backpressure, peak={peak_depth}:\n{slow_out}"
        )));
    }
    if slow_lower
        .lines()
        .any(|line| line.contains("paused") && line.contains(BOUNDED_BACKPRESSURE_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "backpressure must not pause the Pipeline for Downstream slowness:\n{slow_out}"
        )));
    }

    let status_mid = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let sync_lag = parse_sync_lag_for_table(&status_mid, BOUNDED_BACKPRESSURE_TABLE).ok_or_else(
        || {
            CliError::Failed(format!(
                "could not parse Sync Health lag under backpressure:\n{status_mid}"
            ))
        },
    )?;
    if sync_lag < 10 {
        return Err(CliError::Failed(format!(
            "Sync Health lag must reflect Source backlog under backpressure (not only window remainder), got {sync_lag}:\n{status_mid}"
        )));
    }
    let delivery_lag = parse_delivery_lag_for_pipeline(&status_mid, BOUNDED_BACKPRESSURE_PIPELINE)
        .ok_or_else(|| {
            CliError::Failed(format!(
                "could not parse Delivery Health lag under backpressure:\n{status_mid}"
            ))
        })?;
    if delivery_lag < 10 {
        return Err(CliError::Failed(format!(
            "Delivery Health lag must reflect Downstream backlog under delay, got {delivery_lag}:\n{status_mid}"
        )));
    }
    if status_mid.contains("Delivery Health: paused") {
        return Err(CliError::Failed(format!(
            "default must not pause Pipeline for mere Downstream slowness:\n{status_mid}"
        )));
    }

    println!("Lab Scenario: catch-up sync without Downstream delay...");
    let catch_out = run_product_cli_with_env(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[(
            "MIGRALOOP_SYNC_QUEUE_CAPACITY",
            BOUNDED_BACKPRESSURE_CAPACITY,
        )],
    )
    .await?;
    let capture_note = if catch_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario catch-up sync must use real LogMiner path:\n{catch_out}"
        )));
    };

    let status_after = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let sync_lag_after =
        parse_sync_lag_for_table(&status_after, BOUNDED_BACKPRESSURE_TABLE).unwrap_or(-1);
    let delivery_lag_after =
        parse_delivery_lag_for_pipeline(&status_after, BOUNDED_BACKPRESSURE_PIPELINE).unwrap_or(-1);
    if sync_lag_after != 0 || delivery_lag_after != 0 {
        return Err(CliError::Failed(format!(
            "lag must return to 0 after catch-up (sync={sync_lag_after}, delivery={delivery_lag_after}):\n{status_after}"
        )));
    }
    if status_after.contains("Delivery Health: paused") {
        return Err(CliError::Failed(format!(
            "Pipeline must remain unpaused after backpressure catch-up:\n{status_after}"
        )));
    }

    let target_out = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            BOUNDED_BACKPRESSURE_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&target_out, "User100")
        && managed_name_present(&target_out, "User119"))
    {
        return Err(CliError::Failed(format!(
            "Target must receive backlog rows User100..User119 after catch-up:\n{target_out}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out)
        + count_delivery_ops(&slow_out)
        + count_delivery_ops(&catch_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (bounded backpressure; visible lag; catch-up; Pipeline not paused)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: Some(sync_lag_after),
        max_lag: Some(0),
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

async fn run_initial_load_throttled(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {INITIAL_LOAD_THROTTLED_ID}");
    println!(
        "Scenario Namespace: table={INITIAL_LOAD_THROTTLED_TABLE} \
collection={INITIAL_LOAD_THROTTLED_COLLECTION} deployment={INITIAL_LOAD_THROTTLED_DEPLOYMENT}"
    );
    prepare_initial_load_throttled_namespace(lab_dir).await?;

    let config_path = deployment_config_path(lab_dir, INITIAL_LOAD_THROTTLED_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!(
        "Lab Scenario: apply with chunk_size={INITIAL_LOAD_THROTTLED_CHUNK_SIZE} \
pause_after={INITIAL_LOAD_THROTTLED_PAUSE_AFTER}..."
    );
    let paused_out = run_product_cli_with_env(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
        &[
            (
                "MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE",
                INITIAL_LOAD_THROTTLED_CHUNK_SIZE,
            ),
            (
                "MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS",
                INITIAL_LOAD_THROTTLED_PAUSE_AFTER,
            ),
        ],
    )
    .await?;
    if !(paused_out.contains("Initial Load paused")
        || paused_out.contains("initial_load_paused")
        || paused_out.contains("\"event\":\"initial_load_paused\""))
    {
        return Err(CliError::Failed(format!(
            "expected Initial Load pause after bounded chunks:\n{paused_out}"
        )));
    }
    let progress_paused = paused_out
        .lines()
        .filter(|l| l.contains("initial_load_progress") || l.contains("Initial Load progress"))
        .count();
    if progress_paused < 2 {
        return Err(CliError::Failed(format!(
            "expected >=2 Initial Load progress signals before pause, got {progress_paused}:\n{paused_out}"
        )));
    }

    let status_paused = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !(status_paused.contains("status=initial_load_paused")
        || status_paused.contains("Initial Load paused"))
    {
        return Err(CliError::Failed(format!(
            "expected durable Initial Load paused status:\n{status_paused}"
        )));
    }
    if !status_paused.contains("low-watermark=") {
        return Err(CliError::Failed(format!(
            "expected cutover low-watermark after paused chunked load:\n{status_paused}"
        )));
    }

    println!(
        "Lab Scenario: resume apply with rate_limit={INITIAL_LOAD_THROTTLED_RATE}/s \
and store delay for backoff..."
    );
    let resume_out = run_product_cli_with_env(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
        &[
            (
                "MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE",
                INITIAL_LOAD_THROTTLED_CHUNK_SIZE,
            ),
            (
                "MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC",
                INITIAL_LOAD_THROTTLED_RATE,
            ),
            (
                "MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS",
                INITIAL_LOAD_THROTTLED_STORE_DELAY_MS,
            ),
        ],
    )
    .await?;
    if !(resume_out.contains("Initial Load complete")
        || resume_out.contains("\"event\":\"initial_load_complete\""))
    {
        return Err(CliError::Failed(format!(
            "expected Initial Load complete after resume:\n{resume_out}"
        )));
    }
    if !(resume_out.contains("rate_limit=")
        || resume_out.contains("rate_limit_rows_per_sec"))
    {
        return Err(CliError::Failed(format!(
            "expected Operator-visible rate_limit on resume apply:\n{resume_out}"
        )));
    }
    if !(resume_out.contains("Initial Load backoff")
        || resume_out.contains("initial_load_backoff"))
    {
        return Err(CliError::Failed(format!(
            "expected Initial Load backoff under store pressure:\n{resume_out}"
        )));
    }

    let status_after = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !(status_after.contains("status=initial_load_complete")
        && status_after.contains(&format!("rows={INITIAL_LOAD_THROTTLED_ROW_COUNT}")))
    {
        return Err(CliError::Failed(format!(
            "expected complete Base with {INITIAL_LOAD_THROTTLED_ROW_COUNT} rows:\n{status_after}"
        )));
    }

    let capture_note = if resume_out.to_ascii_lowercase().contains("oci") {
        "Initial Load (chunked OCI)".to_string()
    } else {
        "Initial Load (chunked)".to_string()
    };
    println!(
        "Lab Scenario: {INITIAL_LOAD_THROTTLED_ID} checks passed \
         (chunked progress; pause/resume; rate_limit; backoff; watermark retained)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied: INITIAL_LOAD_THROTTLED_ROW_COUNT as u64,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: Some(0),
        max_lag: Some(0),
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

async fn remove_initial_load_throttled_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={INITIAL_LOAD_THROTTLED_TABLE}, collection={INITIAL_LOAD_THROTTLED_COLLECTION}, \
          deployment={INITIAL_LOAD_THROTTLED_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {INITIAL_LOAD_THROTTLED_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for initial-load-throttled:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{INITIAL_LOAD_THROTTLED_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for initial-load-throttled:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, INITIAL_LOAD_THROTTLED_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{INITIAL_LOAD_THROTTLED_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_initial_load_throttled_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_initial_load_throttled_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {INITIAL_LOAD_THROTTLED_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  LABEL VARCHAR2(100) NOT NULL\n\
);\n\
ALTER TABLE {INITIAL_LOAD_THROTTLED_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {INITIAL_LOAD_THROTTLED_TABLE} (ID, LABEL)\n\
SELECT LEVEL, 'item' || LEVEL FROM DUAL CONNECT BY LEVEL <= {INITIAL_LOAD_THROTTLED_ROW_COUNT};\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare initial-load-throttled Scenario Namespace:\n{err}"
            ))
        })
}

async fn remove_bounded_backpressure_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={BOUNDED_BACKPRESSURE_TABLE}, collection={BOUNDED_BACKPRESSURE_COLLECTION}, \
          deployment={BOUNDED_BACKPRESSURE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {BOUNDED_BACKPRESSURE_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for bounded-backpressure:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{BOUNDED_BACKPRESSURE_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for bounded-backpressure:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, BOUNDED_BACKPRESSURE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{BOUNDED_BACKPRESSURE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_bounded_backpressure_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_bounded_backpressure_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {BOUNDED_BACKPRESSURE_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {BOUNDED_BACKPRESSURE_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {BOUNDED_BACKPRESSURE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {BOUNDED_BACKPRESSURE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare bounded-backpressure Scenario Namespace:\n{err}"
            ))
        })
}

async fn insert_bounded_backpressure_backlog(lab_dir: &Path) -> Result<(), CliError> {
    let mut inserts = String::new();
    for i in 0..BOUNDED_BACKPRESSURE_BACKLOG {
        let id = 100 + i;
        inserts.push_str(&format!(
            "INSERT INTO {BOUNDED_BACKPRESSURE_TABLE} (ID, NAME, EMAIL, ACTIVE) \
VALUES ({id}, 'User{id}', 'user{id}@example.com', 1);\n"
        ));
    }
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
{inserts}\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to insert Source backlog for bounded-backpressure:\n{err}"
            ))
        })
}

async fn run_observability_surface(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {OBSERVABILITY_SURFACE_ID}");
    println!(
        "Scenario Namespace: table={OBSERVABILITY_SURFACE_TABLE} \
collection={OBSERVABILITY_SURFACE_COLLECTION} deployment={OBSERVABILITY_SURFACE_DEPLOYMENT}"
    );

    prepare_observability_surface_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, OBSERVABILITY_SURFACE_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !(apply_out.contains("\"event\":\"initial_load_complete\"")
        || apply_out.contains("\"event\": \"initial_load_complete\"")
        || apply_out.contains("\"event\":\"delivery_complete\"")
        || apply_out.contains("\"event\": \"delivery_complete\""))
    {
        return Err(CliError::Failed(format!(
            "expected structured Initial Load / Delivery operator events on apply:\n{apply_out}"
        )));
    }

    println!(
        "Lab Scenario: inserting Source backlog ({OBSERVABILITY_SURFACE_BACKLOG} rows)..."
    );
    insert_observability_surface_backlog(lab_dir).await?;

    println!(
        "Lab Scenario: sync under Downstream delay with queue capacity={} \
(fail after {} durable checkpoint)...",
        OBSERVABILITY_SURFACE_CAPACITY, OBSERVABILITY_SURFACE_FAIL_AFTER
    );
    let (slow_ok, slow_out) = run_product_cli_allow_fail(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[
            (
                "MIGRALOOP_SYNC_QUEUE_CAPACITY",
                OBSERVABILITY_SURFACE_CAPACITY,
            ),
            (
                "MIGRALOOP_DELIVERY_DELAY_MS",
                OBSERVABILITY_SURFACE_DELAY_MS,
            ),
            (
                "MIGRALOOP_SYNC_FAIL_AFTER_CHANGES",
                OBSERVABILITY_SURFACE_FAIL_AFTER,
            ),
        ],
    )
    .await?;
    if slow_ok {
        return Err(CliError::Failed(format!(
            "expected mid-sync stop under Downstream slowness (FAIL_AFTER), got success:\n{slow_out}"
        )));
    }
    if !slow_out.to_ascii_lowercase().contains("logminer") {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{slow_out}"
        )));
    }
    if !(slow_out.contains("\"event\":\"backpressure\"")
        || slow_out.contains("\"event\": \"backpressure\""))
    {
        return Err(CliError::Failed(format!(
            "expected structured backpressure event JSON:\n{slow_out}"
        )));
    }
    if !(slow_out.contains("\"event\":\"incremental_capture\"")
        || slow_out.contains("\"event\": \"incremental_capture\""))
    {
        return Err(CliError::Failed(format!(
            "expected structured incremental_capture event JSON:\n{slow_out}"
        )));
    }

    let status_mid = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !(status_mid.contains("Sync Health:")
        && status_mid.contains("Delivery Health:")
        && status_mid.contains(&format!("Pipeline: {OBSERVABILITY_SURFACE_PIPELINE}")))
    {
        return Err(CliError::Failed(format!(
            "status must include Sync Health, Delivery Health, and Pipeline status:\n{status_mid}"
        )));
    }
    let sync_lag = parse_sync_lag_for_table(&status_mid, OBSERVABILITY_SURFACE_TABLE).ok_or_else(
        || {
            CliError::Failed(format!(
                "could not parse Sync Health lag under Observability Surface probe:\n{status_mid}"
            ))
        },
    )?;
    if sync_lag < 10 {
        return Err(CliError::Failed(format!(
            "Sync Health lag must reflect Source backlog, got {sync_lag}:\n{status_mid}"
        )));
    }
    let delivery_lag =
        parse_delivery_lag_for_pipeline(&status_mid, OBSERVABILITY_SURFACE_PIPELINE).ok_or_else(
            || {
                CliError::Failed(format!(
                    "could not parse Delivery Health lag under Observability Surface probe:\n{status_mid}"
                ))
            },
        )?;
    if delivery_lag < 10 {
        return Err(CliError::Failed(format!(
            "Delivery Health lag must reflect Downstream backlog, got {delivery_lag}:\n{status_mid}"
        )));
    }

    println!("Lab Scenario: scraping Prometheus /metrics via migraloop run...");
    let metrics_body = scrape_run_metrics(&bin, LAB_PLATFORM_STORE_URL).await?;
    if !(metrics_body.contains("migraloop_sync_lag")
        && metrics_body.contains("migraloop_delivery_lag")
        && (metrics_body.contains("migraloop_quarantined_changes")
            || metrics_body.contains("migraloop_failures")))
    {
        return Err(CliError::Failed(format!(
            "Prometheus /metrics must expose lag and failure counters:\n{metrics_body}"
        )));
    }
    let metric_sync_lag = parse_prometheus_gauge(&metrics_body, "migraloop_sync_lag").ok_or_else(
        || {
            CliError::Failed(format!(
                "could not parse migraloop_sync_lag from metrics:\n{metrics_body}"
            ))
        },
    )?;
    if metric_sync_lag < 1.0 {
        return Err(CliError::Failed(format!(
            "migraloop_sync_lag must reflect backlog, got {metric_sync_lag}:\n{metrics_body}"
        )));
    }

    println!("Lab Scenario: catch-up sync without Downstream delay...");
    let catch_out = run_product_cli_with_env(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[(
            "MIGRALOOP_SYNC_QUEUE_CAPACITY",
            OBSERVABILITY_SURFACE_CAPACITY,
        )],
    )
    .await?;
    let capture_note = if catch_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario catch-up sync must use real LogMiner path:\n{catch_out}"
        )));
    };

    let status_after = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let sync_lag_after =
        parse_sync_lag_for_table(&status_after, OBSERVABILITY_SURFACE_TABLE).unwrap_or(-1);
    let delivery_lag_after =
        parse_delivery_lag_for_pipeline(&status_after, OBSERVABILITY_SURFACE_PIPELINE).unwrap_or(-1);
    if sync_lag_after != 0 || delivery_lag_after != 0 {
        return Err(CliError::Failed(format!(
            "lag must return to 0 after catch-up (sync={sync_lag_after}, delivery={delivery_lag_after}):\n{status_after}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out)
        + count_delivery_ops(&slow_out)
        + count_delivery_ops(&catch_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (structured logs; Sync/Delivery Health; Prometheus lag/failures)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: capture_note,
        settle_ms: None,
        max_settle_ms: None,
        lag: Some(sync_lag_after),
        max_lag: Some(0),
        min_rows_per_s: None,
        max_duration_ms: None,
        measured_rows_per_s: None,
        measured_duration_ms: None,
        thresholds_ok: true,
    })
}

async fn remove_observability_surface_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={OBSERVABILITY_SURFACE_TABLE}, collection={OBSERVABILITY_SURFACE_COLLECTION}, \
          deployment={OBSERVABILITY_SURFACE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {OBSERVABILITY_SURFACE_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for observability-surface:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{OBSERVABILITY_SURFACE_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for observability-surface:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, OBSERVABILITY_SURFACE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{OBSERVABILITY_SURFACE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_observability_surface_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_observability_surface_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {OBSERVABILITY_SURFACE_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {OBSERVABILITY_SURFACE_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {OBSERVABILITY_SURFACE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {OBSERVABILITY_SURFACE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare observability-surface Scenario Namespace:\n{err}"
            ))
        })
}

async fn insert_observability_surface_backlog(lab_dir: &Path) -> Result<(), CliError> {
    let mut inserts = String::new();
    for i in 0..OBSERVABILITY_SURFACE_BACKLOG {
        let id = 100 + i;
        inserts.push_str(&format!(
            "INSERT INTO {OBSERVABILITY_SURFACE_TABLE} (ID, NAME, EMAIL, ACTIVE) \
VALUES ({id}, 'User{id}', 'user{id}@example.com', 1);\n"
        ));
    }
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
{inserts}\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to insert Source backlog for observability-surface:\n{err}"
            ))
        })
}

async fn run_platform_store_guardrails(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {PLATFORM_STORE_GUARDRAILS_ID}");
    println!(
        "Scenario Namespace: table={PLATFORM_STORE_GUARDRAILS_TABLE} \
collection={PLATFORM_STORE_GUARDRAILS_COLLECTION} deployment={PLATFORM_STORE_GUARDRAILS_DEPLOYMENT}"
    );

    prepare_platform_store_guardrails_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, PLATFORM_STORE_GUARDRAILS_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: status without disk inject (bundled settings must pass guardrails)...");
    let status_ok = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !status_ok.contains("Platform Store: healthy") {
        return Err(CliError::Failed(format!(
            "bundled Platform Store must satisfy Guardrail minimums:\n{status_ok}"
        )));
    }
    if status_ok.contains("Guardrails rejected") {
        return Err(CliError::Failed(format!(
            "bundled Platform Store settings must not be rejected by Guardrails:\n{status_ok}"
        )));
    }

    println!("Lab Scenario: status rejects absurdly low shared_buffers (inject)...");
    let (reject_ok, reject_out) = run_product_cli_allow_fail(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[(
            "MIGRALOOP_INJECT_PLATFORM_STORE_SHARED_BUFFERS_BYTES",
            "1048576",
        )],
    )
    .await?;
    if reject_ok {
        return Err(CliError::Failed(format!(
            "status must reject absurdly low shared_buffers:\n{reject_out}"
        )));
    }
    let reject_lower = reject_out.to_ascii_lowercase();
    if !(reject_lower.contains("guardrails") && reject_lower.contains("shared_buffers")) {
        return Err(CliError::Failed(format!(
            "expected Guardrails shared_buffers rejection:\n{reject_out}"
        )));
    }

    println!(
        "Lab Scenario: status with injected low free disk ({PLATFORM_STORE_GUARDRAILS_LOW_DISK_BYTES} bytes)..."
    );
    let status_warn = run_product_cli_with_env(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[(
            "MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES",
            PLATFORM_STORE_GUARDRAILS_LOW_DISK_BYTES,
        )],
    )
    .await?;
    if !status_warn.contains("Platform Store: healthy") {
        return Err(CliError::Failed(format!(
            "disk warn must leave Platform Store healthy:\n{status_warn}"
        )));
    }
    let warn_lower = status_warn.to_ascii_lowercase();
    if !(status_warn.contains("WARN:") && warn_lower.contains("disk")) {
        return Err(CliError::Failed(format!(
            "expected free-disk WARN on status:\n{status_warn}"
        )));
    }
    if !(status_warn.contains("platform_store_disk_warn")
        || status_warn.contains("\"event\":\"platform_store_disk_warn\""))
    {
        return Err(CliError::Failed(format!(
            "expected structured platform_store_disk_warn event:\n{status_warn}"
        )));
    }
    if status_warn.contains("Delivery Health: paused")
        || status_warn
            .lines()
            .any(|line| line.contains("Pipeline:") && line.contains("paused"))
    {
        return Err(CliError::Failed(format!(
            "disk threshold must not auto-pause Pipelines:\n{status_warn}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (Guardrails minimums ok; disk warn-only; Pipeline not paused)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: "LogMiner".to_string(),
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

async fn remove_platform_store_guardrails_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={PLATFORM_STORE_GUARDRAILS_TABLE}, collection={PLATFORM_STORE_GUARDRAILS_COLLECTION}, \
          deployment={PLATFORM_STORE_GUARDRAILS_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {PLATFORM_STORE_GUARDRAILS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for platform-store-guardrails:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{PLATFORM_STORE_GUARDRAILS_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for platform-store-guardrails:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, PLATFORM_STORE_GUARDRAILS_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{PLATFORM_STORE_GUARDRAILS_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_platform_store_guardrails_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_platform_store_guardrails_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {PLATFORM_STORE_GUARDRAILS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {PLATFORM_STORE_GUARDRAILS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {PLATFORM_STORE_GUARDRAILS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {PLATFORM_STORE_GUARDRAILS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare platform-store-guardrails Scenario Namespace:\n{err}"
            ))
        })
}

async fn run_backward_compatible_upgrades(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {BACKWARD_COMPATIBLE_UPGRADES_ID}");
    println!(
        "Scenario Namespace: table={BACKWARD_COMPATIBLE_UPGRADES_TABLE} \
collection={BACKWARD_COMPATIBLE_UPGRADES_COLLECTION} \
deployment={BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT}"
    );

    prepare_backward_compatible_upgrades_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, BACKWARD_COMPATIBLE_UPGRADES_ID)?;
    let older_config_path = scenario_config_path(
        lab_dir,
        BACKWARD_COMPATIBLE_UPGRADES_ID,
        BACKWARD_COMPATIBLE_UPGRADES_OLDER_CONFIG,
    )?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;
    let older_config_str = older_config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario older-config path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: status before upgrade migrate...");
    let status_before = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !status_before.contains("Platform Store: healthy")
        || !status_before.contains(&format!("Deployment: {BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT}"))
        || !status_before.contains(&format!("Base Dataset: {BACKWARD_COMPATIBLE_UPGRADES_TABLE}"))
    {
        return Err(CliError::Failed(format!(
            "pre-upgrade status missing healthy store / Deployment / Base:\n{status_before}"
        )));
    }
    let schema_line = status_before
        .lines()
        .find(|l| l.starts_with("Schema version:"))
        .unwrap_or("Schema version: (missing)")
        .to_string();

    println!("Lab Scenario: migraloop migrate (upgrade path)...");
    let migrate_out = run_product_cli(
        &bin,
        &["migrate", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !migrate_out.to_ascii_lowercase().contains("migration")
        && !migrate_out.contains("Platform Store")
    {
        return Err(CliError::Failed(format!(
            "migrate did not report Platform Store migration success:\n{migrate_out}"
        )));
    }

    println!("Lab Scenario: status after upgrade migrate...");
    let status_after_migrate = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !status_after_migrate.contains("Platform Store: healthy") {
        return Err(CliError::Failed(format!(
            "store must stay healthy after upgrade migrate:\n{status_after_migrate}"
        )));
    }
    if !status_after_migrate.contains(schema_line.trim()) {
        return Err(CliError::Failed(format!(
            "Schema version must remain at latest after upgrade migrate \
             (expected `{schema_line}`):\n{status_after_migrate}"
        )));
    }
    if !status_after_migrate
        .contains(&format!("Deployment: {BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT}"))
        || !status_after_migrate
            .contains(&format!("Base Dataset: {BACKWARD_COMPATIBLE_UPGRADES_TABLE}"))
    {
        return Err(CliError::Failed(format!(
            "Deployment/Base must survive upgrade migrate (no wipe):\n{status_after_migrate}"
        )));
    }

    println!(
        "Lab Scenario: apply older SemVer-compatible config ({BACKWARD_COMPATIBLE_UPGRADES_OLDER_CONFIG})..."
    );
    let older_apply = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            older_config_str,
        ],
    )
    .await?;
    if older_apply.contains("Initial Load complete") {
        return Err(CliError::Failed(format!(
            "older compatible config must not rebuild Base from scratch:\n{older_apply}"
        )));
    }

    let status_final = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !status_final.contains(&format!("Deployment: {BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT}"))
        || !status_final.contains(&format!("Base Dataset: {BACKWARD_COMPATIBLE_UPGRADES_TABLE}"))
    {
        return Err(CliError::Failed(format!(
            "Deployment/Base must remain after older config apply:\n{status_final}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (migrate preserved Deployment; older v1.0.0 config applies without rebuild)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: "LogMiner".to_string(),
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

async fn remove_backward_compatible_upgrades_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={BACKWARD_COMPATIBLE_UPGRADES_TABLE}, \
collection={BACKWARD_COMPATIBLE_UPGRADES_COLLECTION}, \
deployment={BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {BACKWARD_COMPATIBLE_UPGRADES_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for backward-compatible-upgrades:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{BACKWARD_COMPATIBLE_UPGRADES_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for backward-compatible-upgrades:\n{err}"
        ))
    })?;

    delete_deployment(
        LAB_PLATFORM_STORE_URL,
        BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT,
    )
    .await
    .map_err(|err| {
        CliError::Failed(format!(
            "Failed to delete Platform Store Deployment `{BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT}` \
             for Scenario Namespace cleanup:\n{err}"
        ))
    })?;

    Ok(())
}

async fn prepare_backward_compatible_upgrades_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_backward_compatible_upgrades_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {BACKWARD_COMPATIBLE_UPGRADES_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {BACKWARD_COMPATIBLE_UPGRADES_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {BACKWARD_COMPATIBLE_UPGRADES_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {BACKWARD_COMPATIBLE_UPGRADES_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare backward-compatible-upgrades Scenario Namespace:\n{err}"
            ))
        })
}

/// Start `migraloop run --metrics-addr`, scrape `/metrics`, then stop the process.
async fn scrape_run_metrics(bin: &Path, platform_store_url: &str) -> Result<String, CliError> {
    use std::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout};

    let port = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| CliError::Failed(format!("bind ephemeral metrics port: {err}")))?
        .local_addr()
        .map_err(|err| CliError::Failed(format!("read ephemeral metrics port: {err}")))?
        .port();
    let metrics_addr = format!("127.0.0.1:{port}");

    let mut child = Command::new(bin)
        .args([
            "run",
            "--platform-store-url",
            platform_store_url,
            "--metrics-addr",
            &metrics_addr,
        ])
        .env(LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT)
        .env(LAB_MONGO_PASSWORD_ENV, LAB_MONGO_PASSWORD_DEFAULT)
        .env("MIGRALOOP_PLATFORM_STORE_URL", platform_store_url)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|err| CliError::Failed(format!("failed to spawn migraloop run for metrics: {err}")))?;

    let scrape = async {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if Instant::now() > deadline {
                return Err(CliError::Failed(format!(
                    "metrics endpoint at {metrics_addr} did not become ready"
                )));
            }
            match TcpStream::connect(&metrics_addr).await {
                Ok(mut stream) => {
                    if stream
                        .write_all(
                            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .is_ok()
                    {
                        let mut buf = Vec::new();
                        if stream.read_to_end(&mut buf).await.is_ok() {
                            let body = String::from_utf8_lossy(&buf).to_string();
                            if body.contains("HTTP/1.1 200") && body.contains("migraloop_") {
                                return Ok(body);
                            }
                        }
                    }
                }
                Err(_) => {}
            }
            sleep(Duration::from_millis(50)).await;
        }
    };

    let body = match timeout(Duration::from_secs(20), scrape).await {
        Ok(result) => result,
        Err(_) => Err(CliError::Failed(
            "timed out waiting for Observability metrics scrape".to_string(),
        )),
    };

    let _ = child.kill().await;
    let _ = child.wait().await;
    body
}

fn parse_prometheus_gauge(body: &str, metric: &str) -> Option<f64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric) {
            let value_str = if rest.starts_with('{') {
                rest.split('}').nth(1)?.trim()
            } else {
                rest.trim()
            };
            if let Some(tok) = value_str.split_whitespace().next() {
                return tok.parse().ok();
            }
        }
    }
    None
}

fn parse_delivery_lag_for_pipeline(status_out: &str, pipeline: &str) -> Option<i32> {
    status_out.lines().find_map(|line| {
        if !(line.contains("Delivery Health") && line.contains(&format!("Pipeline={pipeline}"))) {
            return None;
        }
        line.split("lag=")
            .nth(1)
            .and_then(|rest| {
                rest.split(|c: char| c.is_whitespace() || c == ',')
                    .next()
                    .and_then(|n| n.parse().ok())
            })
    })
}

/// Parse `checkpoint=N` from `migraloop status` Cutover lines.
fn parse_capture_checkpoint(status_out: &str) -> Option<i64> {
    for line in status_out.lines() {
        if let Some(idx) = line.find("checkpoint=") {
            let rest = &line[idx + "checkpoint=".len()..];
            let token = rest
                .split(|c: char| c.is_whitespace() || c == ',')
                .next()
                .unwrap_or("");
            if token == "(none)" {
                continue;
            }
            if let Ok(n) = token.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

async fn run_schema_change_pause(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {SCHEMA_CHANGE_PAUSE_ID}");
    println!(
        "Scenario Namespace: table={SCHEMA_CHANGE_PAUSE_TABLE} \
collection={SCHEMA_CHANGE_PAUSE_COLLECTION} deployment={SCHEMA_CHANGE_PAUSE_DEPLOYMENT}"
    );

    prepare_schema_change_pause_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, SCHEMA_CHANGE_PAUSE_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    let status_before = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let checkpoint = parse_capture_checkpoint(&status_before).ok_or_else(|| {
        CliError::Failed(format!(
            "could not parse capture checkpoint from status after apply:\n{status_before}"
        ))
    })?;
    let inject_scn = checkpoint.saturating_add(1) as u64;

    // Real Source DDL workload on the Lab Oracle (ADR-0025 Source-driving seam).
    // LogMiner DDL contents are not yet reconstructed on the OCI path, so sync also
    // receives the matching Schema Change event via MIGRALOOP_INJECT_SCHEMA_CHANGES
    // (same class of test/Lab seam as MIGRALOOP_DELIVERY_POISON_IDENTITIES).
    println!(
        "Lab Scenario: driving Source DDL (DROP COLUMN NAME on {SCHEMA_CHANGE_PAUSE_TABLE})..."
    );
    mutate_schema_change_pause_source_ddl(lab_dir).await?;

    let inject_path = lab_dir.join(".schema-change-pause-inject.json");
    let inject_body = format!(
        r#"{{
  "changes": [
    {{
      "scn": {inject_scn},
      "table": "{SCHEMA_CHANGE_PAUSE_TABLE}",
      "schema": "SYNC_USER",
      "kind": "drop_column",
      "columns": ["NAME"],
      "summary": "ALTER TABLE {SCHEMA_CHANGE_PAUSE_TABLE} DROP COLUMN NAME"
    }}
  ]
}}"#
    );
    fs::write(&inject_path, inject_body).map_err(|err| {
        CliError::Failed(format!(
            "failed to write Schema Change inject file {}: {err}",
            inject_path.display()
        ))
    })?;
    let inject_str = inject_path.to_str().ok_or_else(|| {
        CliError::Failed("Schema Change inject path is not valid UTF-8".to_string())
    })?;

    println!(
        "Lab Scenario: sync Incremental Capture with Schema Change event for the Source DDL \
         (drop managed NAME at scn={inject_scn}; inject bridges LogMiner DDL capture gap)..."
    );
    let sync_out = run_product_cli_with_env(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        &[("MIGRALOOP_INJECT_SCHEMA_CHANGES", inject_str)],
    )
    .await?;
    let _ = fs::remove_file(&inject_path);
    let capture_note = if sync_out.to_ascii_lowercase().contains("logminer") {
        "LogMiner".to_string()
    } else {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    };
    let sync_lower = sync_out.to_ascii_lowercase();
    if !(sync_lower.contains("warn")
        && sync_lower.contains("schema change")
        && sync_lower.contains("paused"))
    {
        return Err(CliError::Failed(format!(
            "sync must WARN and pause on blocking Schema Change:\n{sync_out}"
        )));
    }
    if sync_lower.contains("alert: poison") || sync_out.contains("Quarantine:") {
        return Err(CliError::Failed(format!(
            "blocking DDL pause must be distinct from poison quarantine:\n{sync_out}"
        )));
    }
    if !sync_lower.contains("not poison quarantine") {
        return Err(CliError::Failed(format!(
            "blocking DDL pause should explicitly distinguish poison quarantine:\n{sync_out}"
        )));
    }

    let status_out = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    let status_lower = status_out.to_ascii_lowercase();
    if !(status_out.contains(SCHEMA_CHANGE_PAUSE_PIPELINE)
        && (status_out.contains("Delivery Health: paused")
            || status_lower.contains("delivery health: paused"))
        && status_lower.contains("schema change")
        && status_lower.contains("blocking"))
    {
        return Err(CliError::Failed(format!(
            "status must show Delivery Health paused + Schema Change blocking:\n{status_out}"
        )));
    }
    if status_lower.contains("quarantine")
        && !status_out.contains("Quarantine: (none)")
        && status_lower.contains("unhealthy / not aligned")
    {
        return Err(CliError::Failed(format!(
            "blocking DDL must not create poison quarantine rows:\n{status_out}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out) + count_delivery_ops(&sync_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (blocking DDL warn+pause; distinct from poison quarantine)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) complete");
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

async fn remove_schema_change_pause_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={SCHEMA_CHANGE_PAUSE_TABLE}, collection={SCHEMA_CHANGE_PAUSE_COLLECTION}, \
          deployment={SCHEMA_CHANGE_PAUSE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {SCHEMA_CHANGE_PAUSE_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for schema-change-pause:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{SCHEMA_CHANGE_PAUSE_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for schema-change-pause:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, SCHEMA_CHANGE_PAUSE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{SCHEMA_CHANGE_PAUSE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_schema_change_pause_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_schema_change_pause_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {SCHEMA_CHANGE_PAUSE_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {SCHEMA_CHANGE_PAUSE_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {SCHEMA_CHANGE_PAUSE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {SCHEMA_CHANGE_PAUSE_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare schema-change-pause Scenario Namespace:\n{err}"
            ))
        })
}

/// Drive real blocking Source DDL for the Lab Scenario Namespace table.
async fn mutate_schema_change_pause_source_ddl(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
ALTER TABLE {SCHEMA_CHANGE_PAUSE_TABLE} DROP COLUMN NAME;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source DDL for schema-change-pause:\n{err}"
            ))
        })
}

async fn run_source_alignment(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {SOURCE_ALIGNMENT_ID}");
    println!(
        "Scenario Namespace: table={SOURCE_ALIGNMENT_TABLE} \
collection={SOURCE_ALIGNMENT_COLLECTION} deployment={SOURCE_ALIGNMENT_DEPLOYMENT} \
pipeline={SOURCE_ALIGNMENT_PIPELINE}"
    );

    prepare_source_alignment_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, SOURCE_ALIGNMENT_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    println!(
        "Lab Scenario: mutating Source ID=1 → AlignedAlice (controlled Base≠Source; no sync)..."
    );
    mutate_source_alignment_name(lab_dir, 1, "AlignedAlice").await?;

    println!("Lab Scenario: running Source Alignment Check (default resource-gated budget)...");
    let align_out = run_product_cli(
        &bin,
        &[
            "align",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
        ],
    )
    .await?;
    let align_lower = align_out.to_ascii_lowercase();
    if !(align_lower.contains("source alignment")
        && (align_lower.contains("mismatched") || align_lower.contains("misaligned"))
        && align_lower.contains("repaired"))
    {
        return Err(CliError::Failed(format!(
            "align must detect mismatch and repair Base from Source:\n{align_out}"
        )));
    }
    if align_lower.contains("write source") || align_lower.contains("updating source") {
        return Err(CliError::Failed(format!(
            "align must never write Source:\n{align_out}"
        )));
    }

    let base_out = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
        ],
    )
    .await?;
    if !managed_name_present(&base_out, "AlignedAlice") || managed_name_present(&base_out, "Alice")
    {
        return Err(CliError::Failed(format!(
            "Base must be repaired to AlignedAlice from Source:\n{base_out}"
        )));
    }

    let source_name = query_source_alignment_name(lab_dir, 1).await?;
    if source_name != "AlignedAlice" {
        return Err(CliError::Failed(format!(
            "Source must remain AlignedAlice after align (never written by check); got {source_name:?}"
        )));
    }

    let status_out = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !(status_out.contains("Source Alignment:")
        && status_out.to_ascii_lowercase().contains("aligned"))
    {
        return Err(CliError::Failed(format!(
            "status must show Source Alignment after check:\n{status_out}"
        )));
    }

    println!(
        "Lab Scenario: mutating Source ID=2 → BobAligned; align --max-rows 1 (resource gate)..."
    );
    mutate_source_alignment_name(lab_dir, 2, "BobAligned").await?;
    let gated_out = run_product_cli(
        &bin,
        &[
            "align",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
            "--max-rows",
            "1",
        ],
    )
    .await?;
    let gated_lower = gated_out.to_ascii_lowercase();
    if !(gated_lower.contains("maxrows=1")
        && (gated_lower.contains("partial") || gated_lower.contains("truncated")))
    {
        return Err(CliError::Failed(format!(
            "resource-gated align must report maxRows=1 and partial/truncated:\n{gated_out}"
        )));
    }
    let base_gated = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
        ],
    )
    .await?;
    if managed_name_present(&base_gated, "BobAligned") {
        return Err(CliError::Failed(format!(
            "max-rows=1 must not full-slam repair Bob:\n{base_gated}"
        )));
    }

    println!("Lab Scenario: align with larger budget to repair remaining Base row...");
    let full_out = run_product_cli(
        &bin,
        &[
            "align",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
            "--max-rows",
            "1000",
        ],
    )
    .await?;
    let base_full = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            SOURCE_ALIGNMENT_TABLE,
        ],
    )
    .await?;
    if !managed_name_present(&base_full, "BobAligned") {
        return Err(CliError::Failed(format!(
            "larger budget must repair Bob from Source:\n{full_out}\n{base_full}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (detect + repair Base from Source; resource-gated max-rows; Source not written)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: "Source Alignment Check (OCI reads)".to_string(),
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

async fn remove_source_alignment_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={SOURCE_ALIGNMENT_TABLE}, collection={SOURCE_ALIGNMENT_COLLECTION}, \
          deployment={SOURCE_ALIGNMENT_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {SOURCE_ALIGNMENT_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for source-alignment:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{SOURCE_ALIGNMENT_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for source-alignment:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, SOURCE_ALIGNMENT_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{SOURCE_ALIGNMENT_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn run_drift_check(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {DRIFT_CHECK_ID}");
    println!(
        "Scenario Namespace: table={DRIFT_CHECK_TABLE} \
collection={DRIFT_CHECK_COLLECTION} deployment={DRIFT_CHECK_DEPLOYMENT} \
pipeline={DRIFT_CHECK_PIPELINE}"
    );

    prepare_drift_check_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, DRIFT_CHECK_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load")
        || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: align Base as trusted Drift baseline...");
    let align_out = run_product_cli(
        &bin,
        &[
            "align",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            DRIFT_CHECK_TABLE,
        ],
    )
    .await?;
    if !align_out.to_ascii_lowercase().contains("source alignment") {
        return Err(CliError::Failed(format!(
            "align must establish Drift baseline:\n{align_out}"
        )));
    }

    println!(
        "Lab Scenario: mutating Target Managed NAME→DRIFTED + planting non-Managed EXTRA..."
    );
    plant_drift_check_target_drift(lab_dir, 1, "DRIFTED", true).await?;

    println!("Lab Scenario: running Drift Check (default resource-gated budget)...");
    let drift_out = run_product_cli(
        &bin,
        &[
            "drift",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            DRIFT_CHECK_PIPELINE,
        ],
    )
    .await?;
    let drift_lower = drift_out.to_ascii_lowercase();
    if !(drift_lower.contains("drift")
        && (drift_lower.contains("mismatched") || drift_lower.contains("drifted"))
        && drift_lower.contains("repaired"))
    {
        return Err(CliError::Failed(format!(
            "drift must detect Managed mismatch and auto-repair:\n{drift_out}"
        )));
    }

    let target_out = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            DRIFT_CHECK_COLLECTION,
        ],
    )
    .await?;
    if !managed_name_present(&target_out, "Alice") || managed_name_present(&target_out, "DRIFTED")
    {
        return Err(CliError::Failed(format!(
            "Managed fields must be auto-repaired to Alice:\n{target_out}"
        )));
    }
    if !(target_out.contains(DRIFT_CHECK_EXTRA_FIELD) || target_out.contains("EXTRA")) {
        return Err(CliError::Failed(format!(
            "non-Managed EXTRA must survive Managed auto-repair:\n{target_out}"
        )));
    }

    let status_out = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if !(status_out.contains("Drift:")
        && (status_out.to_ascii_lowercase().contains("ok")
            || status_out.to_ascii_lowercase().contains("partial")))
    {
        return Err(CliError::Failed(format!(
            "status must show Drift after check:\n{status_out}"
        )));
    }

    println!(
        "Lab Scenario: mutating Target ID=2 → CORRUPT_BOB; drift --max-rows 1 (resource gate)..."
    );
    plant_drift_check_target_drift(lab_dir, 2, "CORRUPT_BOB", false).await?;
    let gated_out = run_product_cli(
        &bin,
        &[
            "drift",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            DRIFT_CHECK_PIPELINE,
            "--max-rows",
            "1",
        ],
    )
    .await?;
    let gated_lower = gated_out.to_ascii_lowercase();
    if !(gated_lower.contains("maxrows=1")
        && (gated_lower.contains("partial") || gated_lower.contains("truncated")))
    {
        return Err(CliError::Failed(format!(
            "resource-gated drift must report maxRows=1 and partial/truncated:\n{gated_out}"
        )));
    }
    let target_gated = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            DRIFT_CHECK_COLLECTION,
        ],
    )
    .await?;
    if !target_gated.contains("CORRUPT_BOB") {
        return Err(CliError::Failed(format!(
            "max-rows=1 must not full-slam repair Bob:\n{target_gated}"
        )));
    }

    println!("Lab Scenario: drift with larger budget to repair remaining Managed drift...");
    let full_out = run_product_cli(
        &bin,
        &[
            "drift",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            DRIFT_CHECK_PIPELINE,
            "--max-rows",
            "1000",
        ],
    )
    .await?;
    let target_full = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            DRIFT_CHECK_COLLECTION,
        ],
    )
    .await?;
    if !managed_name_present(&target_full, "Bob") || target_full.contains("CORRUPT_BOB") {
        return Err(CliError::Failed(format!(
            "larger budget must repair Bob Managed fields:\n{full_out}\n{target_full}"
        )));
    }
    if !(target_full.contains(DRIFT_CHECK_EXTRA_FIELD) || target_full.contains("EXTRA")) {
        return Err(CliError::Failed(format!(
            "non-Managed EXTRA must still be preserved after full drift:\n{target_full}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out);
    println!(
        "Lab Scenario: correctness checks passed \
         (detect + Managed auto-repair; non-Managed preserved; resource-gated max-rows)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
        capture_path_note: "Drift Check (Managed-field Target repair)".to_string(),
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

async fn remove_drift_check_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={DRIFT_CHECK_TABLE}, collection={DRIFT_CHECK_COLLECTION}, \
          deployment={DRIFT_CHECK_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {DRIFT_CHECK_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for drift-check:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{DRIFT_CHECK_COLLECTION}').drop();");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection for drift-check:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, DRIFT_CHECK_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{DRIFT_CHECK_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_drift_check_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_drift_check_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {DRIFT_CHECK_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {DRIFT_CHECK_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {DRIFT_CHECK_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {DRIFT_CHECK_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare drift-check Scenario Namespace:\n{err}"
            ))
        })
}

async fn plant_drift_check_target_drift(
    lab_dir: &Path,
    id: i64,
    name: &str,
    plant_extra: bool,
) -> Result<(), CliError> {
    let extra = if plant_extra {
        format!(", EXTRA: '{DRIFT_CHECK_EXTRA_FIELD}'")
    } else {
        String::new()
    };
    let js = format!(
        "const r = db.getCollection('{DRIFT_CHECK_COLLECTION}').updateOne(\n\
  {{ _id: {id} }},\n\
  {{ $set: {{ NAME: '{name}'{extra} }} }}\n\
);\n\
if (r.matchedCount !== 1) {{ throw new Error('expected to match Output Identity {id}, got ' + JSON.stringify(r)); }}\n"
    );
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to plant Target Managed drift for drift-check:\n{err}"
        ))
    })?;
    Ok(())
}

async fn prepare_source_alignment_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_source_alignment_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {SOURCE_ALIGNMENT_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {SOURCE_ALIGNMENT_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {SOURCE_ALIGNMENT_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {SOURCE_ALIGNMENT_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare source-alignment Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_source_alignment_name(
    lab_dir: &Path,
    id: i64,
    name: &str,
) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {SOURCE_ALIGNMENT_TABLE} SET NAME = '{name}' WHERE ID = {id};\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to mutate Source for source-alignment:\n{err}"
            ))
        })
}

async fn query_source_alignment_name(lab_dir: &Path, id: i64) -> Result<String, CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
SELECT NAME FROM {SOURCE_ALIGNMENT_TABLE} WHERE ID = {id};\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    let out = sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to query Source for source-alignment:\n{err}"
            ))
        })?;
    let name = out
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && *line != "NAME")
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return Err(CliError::Failed(format!(
            "Source query for ID={id} returned empty NAME:\n{out}"
        )));
    }
    Ok(name)
}

async fn run_remove_pipeline(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {REMOVE_PIPELINE_ID}");
    println!(
        "Scenario Namespace: table={REMOVE_PIPELINE_CUSTOMERS_TABLE} \
collections={REMOVE_PIPELINE_CUSTOMERS_COLLECTION},{REMOVE_PIPELINE_REPORTING_COLLECTION} \
deployment={REMOVE_PIPELINE_DEPLOYMENT}"
    );

    prepare_remove_pipeline_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir, REMOVE_PIPELINE_ID)?;
    let bin = lab_migraloop_bin();
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;

    println!("Lab Scenario: apply Deployment via real product path...");
    let apply_out = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            config_str,
        ],
    )
    .await?;
    if !(apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }

    println!("Lab Scenario: remove Pipeline {REMOVE_PIPELINE_CUSTOMERS_PIPELINE} via CLI...");
    let remove_out = run_product_cli(
        &bin,
        &[
            "remove",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            REMOVE_PIPELINE_CUSTOMERS_PIPELINE,
            "--deployment",
            REMOVE_PIPELINE_DEPLOYMENT,
        ],
    )
    .await?;
    if !remove_out.to_ascii_lowercase().contains("removed") {
        return Err(CliError::Failed(format!(
            "Lab Scenario remove did not report removed Pipeline:\n{remove_out}"
        )));
    }

    let status_after_remove = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if status_after_remove.contains(&format!("Pipeline: {REMOVE_PIPELINE_CUSTOMERS_PIPELINE} (")) {
        return Err(CliError::Failed(format!(
            "status must no longer list removed Pipeline as active:\n{status_after_remove}"
        )));
    }
    if !status_after_remove
        .contains(&format!("Pipeline: {REMOVE_PIPELINE_REPORTING_PIPELINE} ("))
    {
        return Err(CliError::Failed(format!(
            "status must still list remaining Pipeline:\n{status_after_remove}"
        )));
    }
    if !status_after_remove.contains(&format!("Base Dataset: {REMOVE_PIPELINE_CUSTOMERS_TABLE}"))
    {
        return Err(CliError::Failed(format!(
            "Shared Base must remain after remove:\n{status_after_remove}"
        )));
    }
    if !status_after_remove.contains(REMOVE_PIPELINE_DEPLOYMENT) {
        return Err(CliError::Failed(format!(
            "Deployment must remain up after Pipeline remove:\n{status_after_remove}"
        )));
    }

    println!("Lab Scenario: driving Source mutations on Shared Base table...");
    mutate_remove_pipeline_source(lab_dir).await?;

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
    if sync_out.contains(&format!(
        "Delivery complete: Pipeline {REMOVE_PIPELINE_CUSTOMERS_PIPELINE}"
    )) {
        return Err(CliError::Failed(format!(
            "removed Pipeline must not Deliver during sync:\n{sync_out}"
        )));
    }
    if !(sync_out.contains(&format!(
        "Delivery complete: Pipeline {REMOVE_PIPELINE_REPORTING_PIPELINE}"
    )) || sync_out.contains(REMOVE_PIPELINE_REPORTING_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "remaining Pipeline must still Deliver from Shared Base during sync:\n{sync_out}"
        )));
    }

    // Removed Pipeline has no Target Binding — inspect Target via Lab mongosh.
    let customers_target = mongosh_in_mongo(
        lab_dir,
        &format!(
            "JSON.stringify(db.getCollection('{REMOVE_PIPELINE_CUSTOMERS_COLLECTION}').find().toArray())"
        ),
    )
    .await
    .map_err(|err| {
        CliError::Failed(format!(
            "Failed to inspect removed Pipeline Target via mongosh:\n{err}"
        ))
    })?;
    if !(managed_name_present(&customers_target, "Alice")
        && managed_name_present(&customers_target, "Bob")
        && !managed_name_present(&customers_target, "Alicia"))
    {
        return Err(CliError::Failed(format!(
            "removed Pipeline Target must retain Initial Load (Alice/Bob, not Alicia):\n{customers_target}"
        )));
    }

    let reporting_target = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            REMOVE_PIPELINE_REPORTING_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&reporting_target, "Alicia")
        && managed_name_present(&reporting_target, "Carol")
        && !managed_name_present(&reporting_target, "Bob"))
    {
        return Err(CliError::Failed(format!(
            "remaining Pipeline must Deliver Incremental updates from Shared Base:\n{reporting_target}"
        )));
    }

    let base_out = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            REMOVE_PIPELINE_CUSTOMERS_TABLE,
        ],
    )
    .await?;
    if !(managed_name_present(&base_out, "Alicia") && managed_name_present(&base_out, "Carol")) {
        return Err(CliError::Failed(format!(
            "Shared Base must continue Incremental Capture for remaining Pipeline:\n{base_out}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_out)
        + count_delivery_ops(&sync_out)
        + count_delivery_ops(&remove_out);

    println!(
        "Lab Scenario: correctness checks passed \
         (remove ceased customers Delivery; Shared Base kept; reporting Delivered)"
    );
    if !sync_out.trim().is_empty() {
        println!("Lab Scenario: Incremental Capture ({capture_note}) complete");
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

async fn remove_remove_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={REMOVE_PIPELINE_CUSTOMERS_TABLE}, \
          collections={REMOVE_PIPELINE_CUSTOMERS_COLLECTION},{REMOVE_PIPELINE_REPORTING_COLLECTION}, \
          deployment={REMOVE_PIPELINE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {REMOVE_PIPELINE_CUSTOMERS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for remove-pipeline:\n{err}"
            ))
        })?;

    let js = format!(
        "db.getCollection('{REMOVE_PIPELINE_CUSTOMERS_COLLECTION}').drop();\n\
db.getCollection('{REMOVE_PIPELINE_REPORTING_COLLECTION}').drop();"
    );
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collections for remove-pipeline:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, REMOVE_PIPELINE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{REMOVE_PIPELINE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_remove_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_remove_pipeline_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {REMOVE_PIPELINE_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {REMOVE_PIPELINE_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {REMOVE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {REMOVE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare remove-pipeline Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_remove_pipeline_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {REMOVE_PIPELINE_CUSTOMERS_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {REMOVE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {REMOVE_PIPELINE_CUSTOMERS_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for remove-pipeline:\n{err}"
            ))
        })
}

async fn remove_idempotent_redelivery_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={IDEMPOTENT_REDELIVERY_TABLE}, collection={IDEMPOTENT_REDELIVERY_COLLECTION}, \
          deployment={IDEMPOTENT_REDELIVERY_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {IDEMPOTENT_REDELIVERY_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table {IDEMPOTENT_REDELIVERY_TABLE}:\n{err}"
            ))
        })?;

    let js = format!("db.getCollection('{IDEMPOTENT_REDELIVERY_COLLECTION}').drop()");
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collection {IDEMPOTENT_REDELIVERY_COLLECTION}:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, IDEMPOTENT_REDELIVERY_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{IDEMPOTENT_REDELIVERY_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_idempotent_redelivery_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_idempotent_redelivery_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {IDEMPOTENT_REDELIVERY_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {IDEMPOTENT_REDELIVERY_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {IDEMPOTENT_REDELIVERY_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {IDEMPOTENT_REDELIVERY_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare idempotent-redelivery Scenario Namespace:\n{err}"
            ))
        })
}

async fn mutate_idempotent_redelivery_source(lab_dir: &Path) -> Result<(), CliError> {
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
UPDATE {IDEMPOTENT_REDELIVERY_TABLE} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {IDEMPOTENT_REDELIVERY_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {IDEMPOTENT_REDELIVERY_TABLE} WHERE ID = 2;\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source insert/update/delete for idempotent-redelivery:\n{err}"
            ))
        })
}

async fn plant_idempotent_redelivery_operator_note(lab_dir: &Path) -> Result<(), CliError> {
    let js = format!(
        "const r = db.getCollection('{IDEMPOTENT_REDELIVERY_COLLECTION}').updateOne(\n\
  {{ _id: 1 }},\n\
  {{ $set: {{ operatorNote: '{IDEMPOTENT_REDELIVERY_OPERATOR_NOTE}' }} }}\n\
);\n\
if (r.matchedCount !== 1) {{ throw new Error('expected to match Output Identity 1, got ' + JSON.stringify(r)); }}\n"
    );
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to plant non-Managed Target field before re-Delivery:\n{err}"
        ))
    })?;
    Ok(())
}

async fn run_product_cli(bin: &Path, args: &[&str]) -> Result<String, CliError> {
    run_product_cli_with_env(bin, args, &[]).await
}

async fn run_product_cli_with_env(
    bin: &Path,
    args: &[&str],
    extra_env: &[(&str, &str)],
) -> Result<String, CliError> {
    let mut command = Command::new(bin);
    command
        .args(args)
        .env(LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT)
        .env(LAB_MONGO_PASSWORD_ENV, LAB_MONGO_PASSWORD_DEFAULT)
        .env("MIGRALOOP_PLATFORM_STORE_URL", LAB_PLATFORM_STORE_URL);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command.output().await.map_err(|err| {
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

/// Like [`run_product_cli_with_env`] but returns stdout/stderr even on non-zero exit
/// (used for mid-sync FAIL_AFTER backpressure observation).
async fn run_product_cli_allow_fail(
    bin: &Path,
    args: &[&str],
    extra_env: &[(&str, &str)],
) -> Result<(bool, String), CliError> {
    let mut command = Command::new(bin);
    command
        .args(args)
        .env(LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT)
        .env(LAB_MONGO_PASSWORD_ENV, LAB_MONGO_PASSWORD_DEFAULT)
        .env("MIGRALOOP_PLATFORM_STORE_URL", LAB_PLATFORM_STORE_URL);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command.output().await.map_err(|err| {
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
    print!("{text}");
    Ok((output.status.success(), text))
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

async fn run_change_pipeline(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {CHANGE_PIPELINE_ID}");
    println!(
        "Scenario Namespace: table={CHANGE_PIPELINE_CUSTOMERS_TABLE} \
collections={CHANGE_PIPELINE_ACTIVE_COLLECTION},{CHANGE_PIPELINE_REPORTING_COLLECTION} \
deployment={CHANGE_PIPELINE_DEPLOYMENT}"
    );

    prepare_change_pipeline_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let bin = lab_migraloop_bin();
    let initial_config = deployment_config_path(lab_dir, CHANGE_PIPELINE_ID)?;
    let semantic_config =
        scenario_config_path(lab_dir, CHANGE_PIPELINE_ID, CHANGE_PIPELINE_SEMANTIC_CONFIG)?;
    let metadata_config =
        scenario_config_path(lab_dir, CHANGE_PIPELINE_ID, CHANGE_PIPELINE_METADATA_CONFIG)?;

    println!("Lab Scenario: apply initial Deployment (Transform ACTIVE==1 + shared Direct)...");
    let apply_v1 = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            initial_config.to_str().ok_or_else(|| {
                CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    if !(apply_v1.contains("Derived Dataset materialized")
        || apply_v1.contains("Delivery complete: Pipeline"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario initial apply did not Deliver Pipelines:\n{apply_v1}"
        )));
    }

    let target_v1 = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            CHANGE_PIPELINE_ACTIVE_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&target_v1, "Alice")
        && managed_name_present(&target_v1, "Carol")
        && !managed_name_present(&target_v1, "Bob"))
    {
        return Err(CliError::Failed(format!(
            "Initial ACTIVE==1 Target must Deliver Alice/Carol only:\n{target_v1}"
        )));
    }

    println!(
        "Lab Scenario: apply semantic Transform revision (ACTIVE==0) via real product path..."
    );
    let apply_v2 = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            semantic_config.to_str().ok_or_else(|| {
                CliError::Failed("Scenario semantic revision path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    let apply_v2_lower = apply_v2.to_ascii_lowercase();
    if !(apply_v2_lower.contains("revision")
        && apply_v2.contains(CHANGE_PIPELINE_ACTIVE_PIPELINE)
        && (apply_v2_lower.contains("paused") || apply_v2_lower.contains("pause")))
    {
        return Err(CliError::Failed(format!(
            "Semantic revision must pause old Delivery and report Pipeline revision:\n{apply_v2}"
        )));
    }
    if !apply_v2.contains(&format!(
        "Derived Dataset materialized: Pipeline {CHANGE_PIPELINE_ACTIVE_PIPELINE}"
    )) {
        return Err(CliError::Failed(format!(
            "Semantic revision must rebuild Derived:\n{apply_v2}"
        )));
    }
    if apply_v2.contains(&format!(
        "Initial Load complete: Base Dataset {CHANGE_PIPELINE_CUSTOMERS_TABLE}"
    )) {
        return Err(CliError::Failed(format!(
            "Shared Base must not be rebuilt on Pipeline revision:\n{apply_v2}"
        )));
    }
    if apply_v2.contains(&format!(
        "Delivery complete: Pipeline {CHANGE_PIPELINE_REPORTING_PIPELINE}"
    )) {
        return Err(CliError::Failed(format!(
            "Unchanged sibling Pipeline must not be re-Delivered on revision:\n{apply_v2}"
        )));
    }

    let derived_v2 = run_product_cli(
        &bin,
        &[
            "derived",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--pipeline",
            CHANGE_PIPELINE_ACTIVE_PIPELINE,
        ],
    )
    .await?;
    if !(managed_name_present(&derived_v2, "Bob")
        && !managed_name_present(&derived_v2, "Alice")
        && !managed_name_present(&derived_v2, "Carol"))
    {
        return Err(CliError::Failed(format!(
            "Rebuilt Derived must match ACTIVE==0 filter:\n{derived_v2}"
        )));
    }

    let target_v2 = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            CHANGE_PIPELINE_ACTIVE_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&target_v2, "Bob")
        && !managed_name_present(&target_v2, "Alice")
        && !managed_name_present(&target_v2, "Carol"))
    {
        return Err(CliError::Failed(format!(
            "Re-Delivery must upsert Bob and reconcile-delete Alice/Carol:\n{target_v2}"
        )));
    }

    let base_after = run_product_cli(
        &bin,
        &[
            "base",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--table",
            CHANGE_PIPELINE_CUSTOMERS_TABLE,
        ],
    )
    .await?;
    if !(managed_name_present(&base_after, "Alice")
        && managed_name_present(&base_after, "Bob")
        && managed_name_present(&base_after, "Carol"))
    {
        return Err(CliError::Failed(format!(
            "Shared Base rows must remain after Pipeline revision:\n{base_after}"
        )));
    }

    let reporting = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            CHANGE_PIPELINE_REPORTING_COLLECTION,
        ],
    )
    .await?;
    if !(managed_name_present(&reporting, "Alice")
        && managed_name_present(&reporting, "Bob")
        && managed_name_present(&reporting, "Carol"))
    {
        return Err(CliError::Failed(format!(
            "Sibling Direct Target must remain from Shared Base:\n{reporting}"
        )));
    }

    println!("Lab Scenario: sync Incremental Capture under the new revision...");
    let sync_out = run_product_cli(
        &bin,
        &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if sync_out.to_ascii_lowercase().contains("error")
        && !sync_out.to_ascii_lowercase().contains("no changes")
    {
        return Err(CliError::Failed(format!(
            "Incremental sync after Pipeline revision must succeed:\n{sync_out}"
        )));
    }
    let status_after_sync = run_product_cli(
        &bin,
        &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
    )
    .await?;
    if status_after_sync
        .to_ascii_lowercase()
        .lines()
        .any(|line| line.contains(CHANGE_PIPELINE_ACTIVE_PIPELINE) && line.contains("paused"))
    {
        return Err(CliError::Failed(format!(
            "Pipeline must not remain paused after revision when continuing incremental:\n{status_after_sync}"
        )));
    }

    println!("Lab Scenario: apply metadata-only description change...");
    let apply_meta = run_product_cli(
        &bin,
        &[
            "apply",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--file",
            metadata_config.to_str().ok_or_else(|| {
                CliError::Failed("Scenario metadata revision path is not valid UTF-8".to_string())
            })?,
        ],
    )
    .await?;
    let apply_meta_lower = apply_meta.to_ascii_lowercase();
    if !(apply_meta_lower.contains("metadata")
        && apply_meta_lower.contains("skip")
        && apply_meta.contains(CHANGE_PIPELINE_ACTIVE_PIPELINE))
    {
        return Err(CliError::Failed(format!(
            "Metadata-only change must report rebuild skipped:\n{apply_meta}"
        )));
    }
    if apply_meta.contains(&format!(
        "Derived Dataset materialized: Pipeline {CHANGE_PIPELINE_ACTIVE_PIPELINE}"
    )) || apply_meta.contains(&format!(
        "Delivery complete: Pipeline {CHANGE_PIPELINE_ACTIVE_PIPELINE}"
    )) {
        return Err(CliError::Failed(format!(
            "Metadata-only change must not rebuild Derived or re-Deliver:\n{apply_meta}"
        )));
    }

    let target_meta = run_product_cli(
        &bin,
        &[
            "target",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
            "--collection",
            CHANGE_PIPELINE_ACTIVE_COLLECTION,
        ],
    )
    .await?;
    if target_meta != target_v2 {
        return Err(CliError::Failed(format!(
            "Metadata-only change must leave Target unchanged.\nBefore:\n{target_v2}\nAfter:\n{target_meta}"
        )));
    }

    let rows_applied = count_delivery_ops(&apply_v1)
        + count_delivery_ops(&apply_v2)
        + count_delivery_ops(&sync_out)
        + count_delivery_ops(&apply_meta);

    println!(
        "Lab Scenario: correctness checks passed \
         (semantic revision rebuilt Derived/re-Delivered; incremental continued; \
Shared Base kept; metadata-only skipped)"
    );

    Ok(ScenarioReport {
        correctness: true,
        rows_applied,
        detail: String::new(),
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
    })
}

async fn remove_change_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (table={CHANGE_PIPELINE_CUSTOMERS_TABLE}, \
          collections={CHANGE_PIPELINE_ACTIVE_COLLECTION},{CHANGE_PIPELINE_REPORTING_COLLECTION}, \
          deployment={CHANGE_PIPELINE_DEPLOYMENT})"
    );

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {CHANGE_PIPELINE_CUSTOMERS_TABLE} PURGE';\n\
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
                "Failed to drop Oracle Scenario Namespace table for change-pipeline:\n{err}"
            ))
        })?;

    let js = format!(
        "db.getCollection('{CHANGE_PIPELINE_ACTIVE_COLLECTION}').drop();\n\
db.getCollection('{CHANGE_PIPELINE_REPORTING_COLLECTION}').drop();"
    );
    mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
        CliError::Failed(format!(
            "Failed to drop Mongo Scenario Namespace collections for change-pipeline:\n{err}"
        ))
    })?;

    delete_deployment(LAB_PLATFORM_STORE_URL, CHANGE_PIPELINE_DEPLOYMENT)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{CHANGE_PIPELINE_DEPLOYMENT}` \
                 for Scenario Namespace cleanup:\n{err}"
            ))
        })?;

    Ok(())
}

async fn prepare_change_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    remove_change_pipeline_namespace(lab_dir).await?;

    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
CREATE TABLE {CHANGE_PIPELINE_CUSTOMERS_TABLE} (\n\
  ID NUMBER(10) PRIMARY KEY,\n\
  NAME VARCHAR2(100) NOT NULL,\n\
  EMAIL VARCHAR2(200),\n\
  ACTIVE NUMBER(1)\n\
);\n\
ALTER TABLE {CHANGE_PIPELINE_CUSTOMERS_TABLE} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n\
INSERT INTO {CHANGE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {CHANGE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);\n\
INSERT INTO {CHANGE_PIPELINE_CUSTOMERS_TABLE} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
COMMIT;\n\
EXIT;\n"
    );
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    sqlplus_in_oracle(lab_dir, &connect, &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare change-pipeline Scenario Namespace:\n{err}"
            ))
        })
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
        assert!(
            ids.iter().any(|id| id == RT_FIELD_OPS_ID),
            "catalog must include rt-field-ops for shipped Rich Transform addFields/rename/remove"
        );
        assert!(
            ids.iter().any(|id| id == RT_EQUILOOKUP_ID),
            "catalog must include rt-equilookup for shipped Rich Transform equiLookup"
        );
        assert!(
            ids.iter().any(|id| id == RT_DISTINCT_ADDTOSET_ID),
            "catalog must include rt-distinct-addtoset for shipped Rich Transform distinct/addToSet"
        );
        assert!(
            ids.iter().any(|id| id == IDEMPOTENT_REDELIVERY_ID),
            "catalog must include idempotent-redelivery for duplicate-safe Delivery"
        );
        assert!(
            ids.iter().any(|id| id == PAUSE_RESUME_ID),
            "catalog must include pause-resume for Pipeline pause/resume CLI verbs"
        );
        assert!(
            ids.iter().any(|id| id == REMOVE_PIPELINE_ID),
            "catalog must include remove-pipeline for Pipeline remove CLI verb"
        );
        assert!(
            ids.iter().any(|id| id == CHANGE_PIPELINE_ID),
            "catalog must include change-pipeline for Pipeline revision change"
        );
        assert!(
            ids.iter().any(|id| id == OBSERVABILITY_SURFACE_ID),
            "catalog must include observability-surface for Observability Surface"
        );
        assert!(
            ids.iter().any(|id| id == PLATFORM_STORE_GUARDRAILS_ID),
            "catalog must include platform-store-guardrails for Platform Store Guardrails"
        );
        assert!(
            ids.iter().any(|id| id == BACKWARD_COMPATIBLE_UPGRADES_ID),
            "catalog must include backward-compatible-upgrades for upgrade migrations"
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
        assert!(
            gaps.iter().any(|g| g.contains("addFields") || g.contains("rename") || g.contains("remove")),
            "missing rt-field-ops must be a visible gap; gaps={gaps:?}"
        );
        assert!(
            gaps.iter().any(|g| g.contains("equiLookup") || g.contains("equilookup")),
            "missing rt-equilookup must be a visible gap; gaps={gaps:?}"
        );
        assert!(
            gaps.iter()
                .any(|g| g.contains("distinct") || g.contains("addToSet") || g.contains("Maintenance State")),
            "missing rt-distinct-addtoset must be a visible gap; gaps={gaps:?}"
        );
        assert!(
            gaps.iter()
                .any(|g| g.contains("idempotent re-delivery") || g.contains("duplicate-safe")),
            "missing idempotent-redelivery must be a visible gap; gaps={gaps:?}"
        );
    }

    #[test]
    fn leftover_namespaces_match_present_deployments_excluding_active() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab");
        let present = vec![
            DIRECT_PIPELINE_DEPLOYMENT.to_string(),
            TRANSFORM_PIPELINE_DEPLOYMENT.to_string(),
            "unrelated-operator-deployment".to_string(),
        ];
        let leftovers = leftover_scenario_namespaces(&lab, &present, Some(DIRECT_PIPELINE_ID))
            .expect("leftover match");
        assert_eq!(
            leftovers,
            vec![TRANSFORM_PIPELINE_ID.to_string()],
            "active Scenario must not be listed as leftover; unrelated Deployments ignored"
        );

        let all = leftover_scenario_namespaces(&lab, &present, None).expect("leftover match");
        assert_eq!(
            all,
            vec![
                DIRECT_PIPELINE_ID.to_string(),
                TRANSFORM_PIPELINE_ID.to_string()
            ]
        );

        let empty = leftover_scenario_namespaces(&lab, &[], None).expect("empty");
        assert!(empty.is_empty());
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
