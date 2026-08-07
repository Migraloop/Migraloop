//! Lab Scenario catalog, recipe-driven run orchestration, and Namespace cleanup
//! (issues #60–#66, #63, #85, #86, #157, #173, #201, #205 / ADR-0025).
//!
//! Recipe-driven runner (`runner.rs` + `namespace.rs` + `correctness.rs`):
//! `recipe.yaml` workload / optional `product_path` / Namespace lifecycle /
//! executable `checks.correctness` / thresholds are the live interface.
//! Scenarios with `workload.product_path` use shared
//! prepare→apply→mutate→sync→assert steps; `namespace.lifecycle` drives wipe /
//! CREATE / supplemental logging / seed (and optional mutate SQL);
//! `checks.correctness` runs isomorphic Managed/Derived/Target inspect
//! expectations. Thin hooks keep only rare escapes (poison status, schema DDL,
//! pause timing, settle orchestration). All shipped Scenarios use shared
//! product-path steps with thin hooks.
//! Lab-specific machinery: catalog listing from on-disk recipes, Scenario
//! Namespace lifecycle (prepare / re-run wipe / manual remove / opt-in
//! auto-remove), one-at-a-time lock, refusal of non-Lab / production engine
//! bindings before apply/sync (US44), equal-weight correctness + metric
//! thresholds, and shipped-capability coverage (`lab/scenarios/COVERAGE.md`).
//! Apply / Sync / inspect use the real product CLI path. Idempotent re-delivery
//! (#86) resets Pipeline Delivery status in Platform Store so a second real
//! `apply` re-Delivers the same Output Identities (at-least-once / upsert).

mod correctness;
mod mega_mix;
mod namespace;
mod recipe;
mod runner;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::config::load_deployment_config;
use crate::lab::{
    ensure_fixture_ready_for_scenario, lab_migraloop_bin, mongosh_in_mongo,
    pause_lab_app_for_exclusive_scenario, resume_lab_app_after_scenario, sqlplus_in_oracle,
    LAB_MONGO_DATABASE, LAB_MONGO_HOST, LAB_MONGO_PASSWORD_DEFAULT, LAB_MONGO_PASSWORD_ENV,
    LAB_MONGO_PORT, LAB_MONGO_USER, LAB_ORACLE_HOST, LAB_ORACLE_PASSWORD_DEFAULT,
    LAB_ORACLE_PASSWORD_ENV, LAB_ORACLE_PORT, LAB_ORACLE_SERVICE, LAB_ORACLE_USER,
    LAB_PLATFORM_STORE_URL,
};
use crate::CliError;
use migraloop_platform_store::PlatformStore;
use migraloop_runtime::{
    capacity_estimate_from_inventory, status_inventory_from_url, ComponentPressure,
    ComponentPressureOverrides, COMPONENT_PRESSURE_NAMES,
};

use self::correctness::{
    execute_recipe_correctness, fetch_all, fetched_satisfies, inspect_mentions_amount,
    inspect_mentions_email_field, managed_field_present, managed_name_present,
    parse_inspect_row_count, parse_target_document_count,
};
use self::mega_mix::{
    e2e_qps, evaluate_mega_mix_gates, format_mega_mix_report_section, incremental_batch_sql,
    mega_mix_pipelines, store_pending_evidence, take_pending_evidence, PipelineQpsSample,
    INCREMENTAL_BATCH_ROWS, MEGA_MIX_DEPLOYMENT, MEGA_MIX_ID, MIX_ID_BASE, SOLO_ID_BASE,
};
use self::namespace::{mutate_namespace_from_recipe, prepare_namespace, wipe_namespace};
use self::recipe::{
    load_recipe, load_selectable_catalog, load_selectable_recipes, ProductPathApplyOpts,
    ProductPathStepKind, ProductPathSyncOpts, ScenarioRecipe,
};
use self::runner::{
    product_path_plan, report_from_adapter_outcome, run_recipe_driven, AdapterOutcome,
    ScenarioMetrics, ScenarioReport,
};


const LOCK_FILE_NAME: &str = ".migraloop-scenario.lock";

/// Lab Namespace cleanup via Platform Store session (not the expand-era URL free fn).
async fn lab_delete_deployment(deployment_name: &str) -> Result<(), PlatformStoreErrorMapped> {
    let store = PlatformStore::open(LAB_PLATFORM_STORE_URL)
        .await
        .map_err(|err| PlatformStoreErrorMapped(err.to_string()))?;
    store
        .delete_deployment(deployment_name)
        .await
        .map_err(|err| PlatformStoreErrorMapped(err.to_string()))
}

async fn lab_update_pipeline_delivery_status(
    deployment_name: &str,
    pipeline_name: &str,
    delivery_status: &str,
) -> Result<(), PlatformStoreErrorMapped> {
    let store = PlatformStore::open(LAB_PLATFORM_STORE_URL)
        .await
        .map_err(|err| PlatformStoreErrorMapped(err.to_string()))?;
    store
        .record_delivery_progress(
            deployment_name,
            pipeline_name,
            Some(delivery_status),
            None,
            None,
        )
        .await
        .map_err(|err| PlatformStoreErrorMapped(err.to_string()))
}

struct PlatformStoreErrorMapped(String);

impl std::fmt::Display for PlatformStoreErrorMapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

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
const CONCURRENT_SETTLE_POLL: Duration = Duration::from_secs(2);

const CHANGE_ORDERING_ID: &str = "change-ordering";
const CHANGE_ORDERING_CUSTOMERS_TABLE: &str = "LAB_CO_CUSTOMERS";
const CHANGE_ORDERING_ORDERS_TABLE: &str = "LAB_CO_ORDERS";
const CHANGE_ORDERING_CUSTOMERS_COLLECTION: &str = "lab_co_customers";
const CHANGE_ORDERING_ORDER_STATS_COLLECTION: &str = "lab_co_order_stats";
const CHANGE_ORDERING_ORDER_STATS_PIPELINE: &str = "lab-co-order-stats";

const BULK_LOAD_ID: &str = "bulk-load";
const BULK_LOAD_TABLE: &str = "LAB_BL_ITEMS";
const BULK_LOAD_COLLECTION: &str = "lab_bl_items";
const BULK_LOAD_DEPLOYMENT: &str = "lab-bulk-load";

/// Poll while waiting for mega-mix Incremental Delivery settle (#251).
const MEGA_MIX_SETTLE_POLL: Duration = Duration::from_secs(2);
/// Max wall time for one solo/mix Incremental window before correctness/QPS give up.
const MEGA_MIX_WINDOW_MAX: Duration = Duration::from_secs(300);

const RT_PROJECT_ID: &str = "rt-project";
const RT_PROJECT_COLLECTION: &str = "lab_rp_customers";
const RT_PROJECT_PIPELINE: &str = "lab-rp-customers";

const RT_FILTER_ID: &str = "rt-filter";
const RT_FILTER_COLLECTION: &str = "lab_rf_customers";
const RT_FILTER_PIPELINE: &str = "lab-rf-customers";

const RT_FIELD_OPS_ID: &str = "rt-field-ops";
const RT_FIELD_OPS_COLLECTION: &str = "lab_rfo_customers";
const RT_FIELD_OPS_PIPELINE: &str = "lab-rfo-customers";

const RT_EQUILOOKUP_ID: &str = "rt-equilookup";
const RT_EQUILOOKUP_CUSTOMERS_TABLE: &str = "LAB_REL_CUSTOMERS";
const RT_EQUILOOKUP_ORDERS_TABLE: &str = "LAB_REL_ORDERS";
const RT_EQUILOOKUP_COLLECTION: &str = "lab_rel_customers";
const RT_EQUILOOKUP_PIPELINE: &str = "lab-rel-customers";

const RT_UNION_ID: &str = "rt-union";
const RT_UNION_EAST_TABLE: &str = "LAB_RNU_EAST";
const RT_UNION_WEST_TABLE: &str = "LAB_RNU_WEST";
const RT_UNION_COLLECTION: &str = "lab_rnu_customers";
const RT_UNION_PIPELINE: &str = "lab-rnu-customers";

const RT_UNWIND_ID: &str = "rt-unwind";
const RT_UNWIND_CUSTOMERS_TABLE: &str = "LAB_RU_CUSTOMERS";
const RT_UNWIND_ORDERS_TABLE: &str = "LAB_RU_ORDERS";
const RT_UNWIND_COLLECTION: &str = "lab_ru_orders";
const RT_UNWIND_PIPELINE: &str = "lab-ru-orders";

const RT_DISTINCT_ADDTOSET_ID: &str = "rt-distinct-addtoset";
const RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION: &str = "lab_rda_distinct_customers";
const RT_DISTINCT_ADDTOSET_ADD_COLLECTION: &str = "lab_rda_amounts_by_customer";
const RT_DISTINCT_ADDTOSET_DISTINCT_PIPELINE: &str = "lab-rda-distinct-customers";
const RT_DISTINCT_ADDTOSET_ADD_PIPELINE: &str = "lab-rda-amounts-by-customer";

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
const CHANGE_PIPELINE_SEMANTIC_CONFIG: &str = "deployment-semantic.yaml";
const CHANGE_PIPELINE_METADATA_CONFIG: &str = "deployment-metadata.yaml";

const POISON_QUARANTINE_ID: &str = "poison-quarantine";
const POISON_QUARANTINE_COLLECTION: &str = "lab_pq_customers";
const POISON_QUARANTINE_PIPELINE: &str = "lab-pq-customers";
/// Lab orchestration: force Delivery failure for Output Identity 1 so quarantine runs.
const POISON_QUARANTINE_IDENTITY: &str = "1";
const POISON_QUARANTINE_MAX_ATTEMPTS: &str = "2";

const SCHEMA_CHANGE_PAUSE_ID: &str = "schema-change-pause";
const SCHEMA_CHANGE_PAUSE_TABLE: &str = "LAB_SC_CUSTOMERS";
const SCHEMA_CHANGE_PAUSE_PIPELINE: &str = "lab-sc-customers";

const SOURCE_ALIGNMENT_ID: &str = "source-alignment";
const SOURCE_ALIGNMENT_TABLE: &str = "LAB_SA_CUSTOMERS";

const DRIFT_CHECK_ID: &str = "drift-check";
const DRIFT_CHECK_TABLE: &str = "LAB_DC_CUSTOMERS";
const DRIFT_CHECK_COLLECTION: &str = "lab_dc_customers";
const DRIFT_CHECK_PIPELINE: &str = "lab-dc-customers";
const DRIFT_CHECK_EXTRA_FIELD: &str = "keep-me-non-managed";

const BOUNDED_BACKPRESSURE_ID: &str = "bounded-backpressure";
const BOUNDED_BACKPRESSURE_TABLE: &str = "LAB_BP_CUSTOMERS";
const BOUNDED_BACKPRESSURE_COLLECTION: &str = "lab_bp_customers";
const BOUNDED_BACKPRESSURE_PIPELINE: &str = "lab-bp-customers";
/// Lab orchestration: tiny Incremental window + Downstream Delivery delay.
const BOUNDED_BACKPRESSURE_CAPACITY: &str = "2";
const BOUNDED_BACKPRESSURE_DELAY_MS: &str = "80";
const BOUNDED_BACKPRESSURE_FAIL_AFTER: &str = "1";
const BOUNDED_BACKPRESSURE_BACKLOG: i64 = 20;

const OBSERVABILITY_SURFACE_ID: &str = "observability-surface";
const OBSERVABILITY_SURFACE_TABLE: &str = "LAB_OBS_CUSTOMERS";
const OBSERVABILITY_SURFACE_PIPELINE: &str = "lab-obs-customers";
const OBSERVABILITY_SURFACE_CAPACITY: &str = "2";
const OBSERVABILITY_SURFACE_DELAY_MS: &str = "80";
const OBSERVABILITY_SURFACE_FAIL_AFTER: &str = "1";
const OBSERVABILITY_SURFACE_BACKLOG: i64 = 20;

const PLATFORM_STORE_GUARDRAILS_ID: &str = "platform-store-guardrails";
/// 512 MiB — below the 1 GiB product warn threshold (ADR-0010).
const PLATFORM_STORE_GUARDRAILS_LOW_DISK_BYTES: &str = "536870912";

const BACKWARD_COMPATIBLE_UPGRADES_ID: &str = "backward-compatible-upgrades";
const BACKWARD_COMPATIBLE_UPGRADES_TABLE: &str = "LAB_UPG_CUSTOMERS";
const BACKWARD_COMPATIBLE_UPGRADES_DEPLOYMENT: &str = "lab-backward-compatible-upgrades";
const BACKWARD_COMPATIBLE_UPGRADES_OLDER_CONFIG: &str = "deployment-v1.0.0.yaml";

const INITIAL_LOAD_THROTTLED_ID: &str = "initial-load-throttled";
/// Non-trivial Source volume for chunked Initial Load (issue #124).
const INITIAL_LOAD_THROTTLED_ROW_COUNT: i64 = 500;
const INITIAL_LOAD_THROTTLED_CHUNK_SIZE: &str = "50";
const INITIAL_LOAD_THROTTLED_RATE: &str = "200";
const INITIAL_LOAD_THROTTLED_PAUSE_AFTER: &str = "2";
const INITIAL_LOAD_THROTTLED_STORE_DELAY_MS: &str = "20";

/// Default bulk volume for the Lab Scenario (US17 — on the order of 100k).
/// Thresholds (max_lag / max_duration_ms / min_rows_per_s) live in recipe.yaml — removed
/// BULK_LOAD_MAX_LAG / BULK_LOAD_MAX_DURATION_MS / BULK_LOAD_MIN_ROWS_PER_S / CONCURRENT_MAX_SETTLE_MS.
const BULK_LOAD_ROW_COUNT: u64 = 100_000;
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

/// Registered Scenario adapters (feature-time implementations in this module).
/// Selectable catalog = these ids ∩ complete on-disk recipe packages.
fn registered_scenario_ids() -> &'static [&'static str] {
    &[
        DIRECT_PIPELINE_ID,
        TRANSFORM_PIPELINE_ID,
        RT_PROJECT_ID,
        RT_FILTER_ID,
        RT_FIELD_OPS_ID,
        RT_EQUILOOKUP_ID,
        RT_UNION_ID,
        RT_UNWIND_ID,
        RT_DISTINCT_ADDTOSET_ID,
        CONCURRENT_SOURCE_WORKLOAD_ID,
        CHANGE_ORDERING_ID,
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
        MEGA_MIX_ID,
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
        (RT_UNION_ID, "Rich Transform union"),
        (RT_UNWIND_ID, "Rich Transform unwind"),
        (
            RT_DISTINCT_ADDTOSET_ID,
            "Rich Transform distinct/addToSet with Maintenance State",
        ),
        (
            CONCURRENT_SOURCE_WORKLOAD_ID,
            "intra-Scenario concurrent Source workload",
        ),
        (
            CHANGE_ORDERING_ID,
            "Change Ordering / confluence (same-key order, cross-key interleave, min Base recompute)",
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
        (
            MEGA_MIX_ID,
            "mega-mix all path families + solo/mix e2e QPS + 0.7/0.95 gates",
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
    let recipes = load_selectable_recipes(lab_dir)?;
    let recipe = recipes
        .into_iter()
        .find(|recipe| recipe.id == scenario)
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
        return emit_scenario_outcome_probe(&recipe, &probe);
    }

    // US44 / issue #85: refuse non-Lab / production engine bindings before apply/sync.
    // Runs before Fixture probes so CI can assert isolation without Docker.
    ensure_lab_fixture_engines_for_scenario(lab_dir, scenario)?;

    ensure_fixture_ready_for_scenario(lab_dir).await?;

    let lock = ScenarioLock::acquire(&lock_path, scenario)?;
    // Host Scenario apply/sync must be the sole Incremental Capture consumer.
    // Fixture `app` runs continuous `migraloop run` and otherwise races mutate→sync.
    pause_lab_app_for_exclusive_scenario(lab_dir).await?;
    let started = Instant::now();
    // Recipe-driven path: print recipe interface, run shared product-path hooks,
    // evaluate thresholds.
    let result = run_recipe_driven(&recipe, || async {
        if recipe.workload.product_path.is_some() {
            return run_product_path_scenario(lab_dir, &recipe).await;
        }
        match scenario {
            // product_path Scenarios (#173 / #178 / #179) are handled above.
            DIRECT_PIPELINE_ID
            | TRANSFORM_PIPELINE_ID
            | CONCURRENT_SOURCE_WORKLOAD_ID
            | CHANGE_ORDERING_ID
            | BULK_LOAD_ID
            | IDEMPOTENT_REDELIVERY_ID
            | PAUSE_RESUME_ID
            | REMOVE_PIPELINE_ID
            | CHANGE_PIPELINE_ID
            | SCHEMA_CHANGE_PAUSE_ID
            | SOURCE_ALIGNMENT_ID
            | DRIFT_CHECK_ID
            | BOUNDED_BACKPRESSURE_ID
            | OBSERVABILITY_SURFACE_ID
            | PLATFORM_STORE_GUARDRAILS_ID
            | BACKWARD_COMPATIBLE_UPGRADES_ID
            | INITIAL_LOAD_THROTTLED_ID
            | MEGA_MIX_ID
            | RT_PROJECT_ID
            | RT_FILTER_ID
            | RT_FIELD_OPS_ID
            | RT_EQUILOOKUP_ID
            | RT_UNION_ID
            | RT_UNWIND_ID
            | RT_DISTINCT_ADDTOSET_ID
            | POISON_QUARANTINE_ID => Err(CliError::Failed(
                format!(
                    "Lab Scenario `{scenario}` must declare workload.product_path in recipe.yaml \
                     (shared product-path runner; issue #173 / #178 / #179)"
                ),
            )),
            _ => Err(CliError::Failed(format!(
                "Lab Scenario `{scenario}` is listed but has no adapter"
            ))),
        }
    })
    .await;
    let duration = started.elapsed();
    // Always resume Fixture app so `lab status` stays ready for the next Scenario.
    if let Err(resume_err) = resume_lab_app_after_scenario(lab_dir).await {
        eprintln!("{resume_err}");
    }

    match result {
        Ok(report) => {
            let mut report = enrich_report_component_pressure(report).await?;
            attach_mega_mix_evidence(&mut report);
            let passed = report.correctness && report.thresholds_ok && !report.infra_saturated;
            let mut namespace_removed = false;
            if auto_remove && passed {
                // Opt-in cleanup after success only — failures keep Namespace for debug (US35).
                remove_scenario_namespace(scenario, lab_dir).await?;
                namespace_removed = true;
            }
            drop(lock);
            print_scenario_report(&recipe.id, true, duration, &report, namespace_removed);
            if report.infra_saturated {
                // ADR-0031 / #249: infra-saturated evidence is resize+re-run, not product FAIL.
                Ok(())
            } else if passed {
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
                component_pressure: Vec::new(),
                infra_saturated: false,
                mega_mix: None,
            };
            print_scenario_report(&recipe.id, false, duration, &report, false);
            Err(err)
        }
    }
}

/// Attach pending mega-mix gate evidence after pressure enrichment (#251).
fn attach_mega_mix_evidence(report: &mut ScenarioReport) {
    if let Some(mut evidence) = take_pending_evidence() {
        if report.infra_saturated {
            evidence.gate_0_95_pass = None;
        }
        report.mega_mix = Some(evidence);
    }
}

/// Attach component pressure / infra-saturated from Lab Platform Store (+ optional inject).
async fn enrich_report_component_pressure(
    mut report: ScenarioReport,
) -> Result<ScenarioReport, CliError> {
    let overrides = match std::env::var("MIGRALOOP_COMPONENT_PRESSURE_OVERRIDE") {
        Ok(spec) if !spec.trim().is_empty() => {
            ComponentPressureOverrides::parse(&spec).map_err(CliError::Failed)?
        }
        _ => ComponentPressureOverrides::default(),
    };
    let inventory = status_inventory_from_url(LAB_PLATFORM_STORE_URL)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let estimate = capacity_estimate_from_inventory(&inventory, &overrides);
    report.component_pressure = estimate.components;
    report.infra_saturated = estimate.infra_saturated;
    Ok(report)
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
    let recipe_path = lab_dir.join("scenarios").join(scenario).join("recipe.yaml");
    if !recipe_path.is_file() {
        return Err(CliError::Failed(format!(
            "Lab Scenario `{scenario}` is listed but has no recipe.yaml for Namespace remove"
        )));
    }
    let recipe = load_recipe(&recipe_path)?;
    wipe_namespace(lab_dir, &recipe.namespace).await
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
/// Thresholds come from the loaded bulk-load recipe (live interface).
fn emit_scenario_outcome_probe(recipe: &ScenarioRecipe, probe: &str) -> Result<(), CliError> {
    if recipe.id != BULK_LOAD_ID {
        return Err(CliError::Failed(format!(
            "MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE is only supported for `{BULK_LOAD_ID}` \
             (got `{}`)",
            recipe.id
        )));
    }
    let report = match probe {
        "threshold-fail" => {
            let max_duration_ms = recipe.thresholds.max_duration_ms.ok_or_else(|| {
                CliError::Failed(
                    "bulk-load recipe must declare thresholds.max_duration_ms for outcome probe"
                        .to_string(),
                )
            })?;
            let min_rows_per_s = recipe.thresholds.min_rows_per_s.ok_or_else(|| {
                CliError::Failed(
                    "bulk-load recipe must declare thresholds.min_rows_per_s for outcome probe"
                        .to_string(),
                )
            })?;
            let outcome = AdapterOutcome {
                correctness: true,
                detail: String::new(),
                metrics: ScenarioMetrics {
                    settle_ms: None,
                    lag: Some(0),
                    rows_per_s: Some(min_rows_per_s / 2.0),
                    duration_ms: Some(max_duration_ms + 1),
                    rows_applied: BULK_LOAD_ROW_COUNT,
                    capture_path_note: "Initial Load".to_string(),
                },
            };
            report_from_adapter_outcome(recipe, outcome)
        }
        "correctness-fail" => {
            let outcome = AdapterOutcome {
                correctness: false,
                detail: format!(
                    "correctness: expected rows={BULK_LOAD_ROW_COUNT} \
base_rows={} target_rows={}",
                    BULK_LOAD_ROW_COUNT - 1,
                    BULK_LOAD_ROW_COUNT - 1
                ),
                metrics: ScenarioMetrics {
                    settle_ms: None,
                    lag: Some(0),
                    rows_per_s: Some(800.0),
                    duration_ms: Some(120_000),
                    rows_applied: BULK_LOAD_ROW_COUNT - 1,
                    capture_path_note: "Initial Load".to_string(),
                },
            };
            report_from_adapter_outcome(recipe, outcome)
        }
        "infra-saturated" => {
            let mut report = report_from_adapter_outcome(
                recipe,
                AdapterOutcome {
                    correctness: true,
                    detail: String::new(),
                    metrics: ScenarioMetrics {
                        settle_ms: None,
                        lag: Some(0),
                        rows_per_s: Some(800.0),
                        duration_ms: Some(120_000),
                        rows_applied: BULK_LOAD_ROW_COUNT,
                        capture_path_note: "Initial Load".to_string(),
                    },
                },
            );
            report.component_pressure = COMPONENT_PRESSURE_NAMES
                .iter()
                .map(|name| {
                    let saturated = *name == "source";
                    ComponentPressure {
                        component: (*name).to_string(),
                        pressure: if saturated { 95 } else { 10 },
                        saturated,
                    }
                })
                .collect();
            report.infra_saturated = true;
            let duration = Duration::from_millis(report.measured_duration_ms.unwrap_or(0) as u64);
            print_scenario_report(&recipe.id, true, duration, &report, false);
            // infra-saturated is not a product failure — exit success with labeled report.
            return Ok(());
        }
        other => {
            return Err(CliError::Failed(format!(
                "Unknown MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE `{other}` \
                 (expected `threshold-fail`, `correctness-fail`, or `infra-saturated`)"
            )));
        }
    };

    let duration = Duration::from_millis(report.measured_duration_ms.unwrap_or(0) as u64);
    print_scenario_report(&recipe.id, true, duration, &report, false);
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
    let outcome = if report.infra_saturated {
        "INFRA-SATURATED"
    } else if overall_pass && report.correctness && report.thresholds_ok {
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
    // Always print the four stable component pressure names (ADR-0031 / #249).
    out.push_str("  component_pressure:\n");
    for name in COMPONENT_PRESSURE_NAMES {
        let comp = report
            .component_pressure
            .iter()
            .find(|c| c.component == *name);
        match comp {
            Some(c) => out.push_str(&format!(
                "    {name}: pressure={} saturated={}\n",
                c.pressure,
                if c.saturated { "yes" } else { "no" }
            )),
            None => out.push_str(&format!("    {name}: pressure=0 saturated=no\n")),
        }
    }
    if report.infra_saturated {
        out.push_str("  infra_saturated=yes\n");
        if let Some(limiting) = report
            .component_pressure
            .iter()
            .max_by_key(|c| c.pressure)
        {
            out.push_str(&format!(
                "  limiting_component={}\n",
                limiting.component
            ));
        }
        out.push_str(
            "  guidance: resize Lab Fixture Source / Platform Store / Target and re-run — \
             infra-saturated is not a product failure (ADR-0031)\n",
        );
    }
    if let Some(evidence) = &report.mega_mix {
        out.push_str(&format_mega_mix_report_section(evidence));
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

/// Successful adapter outcome with no measured threshold metrics.
fn adapter_ok(rows_applied: u64, capture_path_note: impl Into<String>) -> AdapterOutcome {
    AdapterOutcome {
        correctness: true,
        detail: String::new(),
        metrics: ScenarioMetrics {
            settle_ms: None,
            lag: None,
            rows_per_s: None,
            duration_ms: None,
            rows_applied,
            capture_path_note: capture_path_note.into(),
        },
    }
}

/// Apply Scenario deployment via the real product CLI path with recipe apply gates.
///
/// Typed ApplyOptions ride on `apply_cli_args` (#200). Optional `extra_env` remains
/// only for non-ApplyOptions Lab bridges (none today for Initial Load knobs).
async fn product_apply(
    lab_dir: &Path,
    scenario_id: &str,
    opts: &ProductPathApplyOpts,
    apply_cli_args: &[String],
    extra_env: &[(String, String)],
) -> Result<String, CliError> {
    let config_path = deployment_config_path(lab_dir, scenario_id)?;
    let bin = lab_migraloop_bin();
    println!("Lab Scenario: apply Deployment via real product path...");
    let config_str = config_path.to_str().ok_or_else(|| {
        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
    })?;
    let mut args: Vec<&str> = vec![
        "apply",
        "--platform-store-url",
        LAB_PLATFORM_STORE_URL,
        "--file",
        config_str,
    ];
    for a in apply_cli_args {
        args.push(a.as_str());
    }
    let env_refs: Vec<(&str, &str)> = extra_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let apply_out = run_product_cli_with_env(&bin, &args, &env_refs).await?;
    if opts.require_initial_load
        && !(apply_out.contains("Initial Load")
            || apply_out.to_ascii_lowercase().contains("initial_load"))
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Initial Load (real product path required):\n{apply_out}"
        )));
    }
    if opts.require_delivery && !apply_out.to_ascii_lowercase().contains("delivery") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not report Delivery (real product path required):\n{apply_out}"
        )));
    }
    if opts.require_derived && !apply_out.to_ascii_lowercase().contains("derived") {
        return Err(CliError::Failed(format!(
            "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
        )));
    }
    Ok(apply_out)
}

/// Incremental Capture + Delivery via real product path.
///
/// Typed SyncOptions ride on `sync_cli_args` (#180). Optional `extra_env` remains
/// only for non-SyncOptions Lab bridges (e.g. Schema Change inject file).
///
/// Returns `(sync_out, capture_note, sync_succeeded)`. When `opts.allow_fail` is set,
/// a non-zero sync exit still returns output so hooks can observe mid-window stops.
async fn product_sync(
    opts: &ProductPathSyncOpts,
    sync_cli_args: &[String],
    extra_env: &[(String, String)],
) -> Result<(String, String, bool), CliError> {
    let bin = lab_migraloop_bin();
    if sync_cli_args.is_empty() && extra_env.is_empty() && !opts.allow_fail {
        println!("Lab Scenario: sync Incremental Capture + Delivery via real product path...");
    }
    let mut args: Vec<&str> = vec!["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL];
    for a in sync_cli_args {
        args.push(a.as_str());
    }
    let env_refs: Vec<(&str, &str)> = extra_env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (sync_ok, sync_out) = if opts.allow_fail {
        run_product_cli_allow_fail(&bin, &args, &env_refs).await?
    } else {
        let out = run_product_cli_with_env(&bin, &args, &env_refs).await?;
        (true, out)
    };
    let has_logminer = sync_out.to_ascii_lowercase().contains("logminer");
    if opts.require_logminer && !has_logminer {
        return Err(CliError::Failed(format!(
            "Lab Scenario sync must use real LogMiner path (not contract/stub):\n{sync_out}"
        )));
    }
    let capture_note = if has_logminer {
        "LogMiner".to_string()
    } else {
        "Incremental Sync".to_string()
    };
    Ok((sync_out, capture_note, sync_ok))
}

/// Context accumulated while the shared product-path runner executes recipe steps.
struct ProductPathRunContext {
    apply_out: String,
    sync_out: String,
    /// Whether the last `product_sync` process exited successfully (false when allow_fail).
    sync_ok: bool,
    capture_path_note: String,
    /// Wall-clock start of the first `product_apply` step (bulk-load duration metrics).
    apply_started: Option<Instant>,
}

/// Thin Scenario hooks for rare escapes (#173 / #178 / #201 / #205).
/// Namespace wipe/prepare/seed (and optional mutate SQL) live in `namespace.lifecycle`.
/// Isomorphic Managed/Derived/Target correctness lives in recipe `checks.correctness`.
enum ProductPathHooks {
    DirectPipeline,
    RtProject,
    RtFilter,
    RtFieldOps,
    RtEquilookup,
    RtUnion,
    RtUnwind,
    RtDistinctAddtoset,
    PoisonQuarantine,
    TransformPipeline,
    ConcurrentSourceWorkload,
    ChangeOrdering,
    BulkLoad,
    IdempotentRedelivery,
    PauseResume,
    RemovePipeline,
    ChangePipeline,
    SchemaChangePause,
    SourceAlignment,
    DriftCheck,
    BoundedBackpressure,
    ObservabilitySurface,
    PlatformStoreGuardrails,
    BackwardCompatibleUpgrades,
    InitialLoadThrottled,
    MegaMix,
}

impl ProductPathHooks {
    fn for_recipe(recipe: &ScenarioRecipe) -> Result<Self, CliError> {
        match recipe.id.as_str() {
            DIRECT_PIPELINE_ID => Ok(Self::DirectPipeline),
            RT_PROJECT_ID => Ok(Self::RtProject),
            RT_FILTER_ID => Ok(Self::RtFilter),
            RT_FIELD_OPS_ID => Ok(Self::RtFieldOps),
            RT_EQUILOOKUP_ID => Ok(Self::RtEquilookup),
            RT_UNION_ID => Ok(Self::RtUnion),
            RT_UNWIND_ID => Ok(Self::RtUnwind),
            RT_DISTINCT_ADDTOSET_ID => Ok(Self::RtDistinctAddtoset),
            POISON_QUARANTINE_ID => Ok(Self::PoisonQuarantine),
            TRANSFORM_PIPELINE_ID => Ok(Self::TransformPipeline),
            CONCURRENT_SOURCE_WORKLOAD_ID => Ok(Self::ConcurrentSourceWorkload),
            CHANGE_ORDERING_ID => Ok(Self::ChangeOrdering),
            BULK_LOAD_ID => Ok(Self::BulkLoad),
            IDEMPOTENT_REDELIVERY_ID => Ok(Self::IdempotentRedelivery),
            PAUSE_RESUME_ID => Ok(Self::PauseResume),
            REMOVE_PIPELINE_ID => Ok(Self::RemovePipeline),
            CHANGE_PIPELINE_ID => Ok(Self::ChangePipeline),
            SCHEMA_CHANGE_PAUSE_ID => Ok(Self::SchemaChangePause),
            SOURCE_ALIGNMENT_ID => Ok(Self::SourceAlignment),
            DRIFT_CHECK_ID => Ok(Self::DriftCheck),
            BOUNDED_BACKPRESSURE_ID => Ok(Self::BoundedBackpressure),
            OBSERVABILITY_SURFACE_ID => Ok(Self::ObservabilitySurface),
            PLATFORM_STORE_GUARDRAILS_ID => Ok(Self::PlatformStoreGuardrails),
            BACKWARD_COMPATIBLE_UPGRADES_ID => Ok(Self::BackwardCompatibleUpgrades),
            INITIAL_LOAD_THROTTLED_ID => Ok(Self::InitialLoadThrottled),
            MEGA_MIX_ID => Ok(Self::MegaMix),
            other => Err(CliError::Failed(format!(
                "Lab Scenario `{other}` declares workload.product_path but has no product-path hooks \
                 (migrate the Scenario or remove product_path from recipe.yaml)"
            ))),
        }
    }

    /// Typed ApplyOptions CLI flags + optional non-options env bridges (#200).
    ///
    /// Prefer CLI flags for Initial Load knobs (same pattern as [`Self::sync_invocation`]).
    fn apply_invocation(&self) -> (Vec<String>, Vec<(String, String)>) {
        match self {
            Self::InitialLoadThrottled => (
                vec![
                    "--initial-load-chunk-size".to_string(),
                    INITIAL_LOAD_THROTTLED_CHUNK_SIZE.to_string(),
                    "--initial-load-pause-after-chunks".to_string(),
                    INITIAL_LOAD_THROTTLED_PAUSE_AFTER.to_string(),
                ],
                vec![],
            ),
            _ => (vec![], vec![]),
        }
    }

    async fn after_apply(&self, _lab_dir: &Path, apply_out: &str) -> Result<(), CliError> {
        let bin = lab_migraloop_bin();
        match self {
            Self::DirectPipeline => {
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
                Ok(())
            }
            Self::RtProject => {
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
                if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
                    && managed_field_present(&derived_after_apply, "NAME", "Bob")
                    && !inspect_mentions_email_field(&derived_after_apply))
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load project Derived check failed \
(expected Alice/Bob NAME, no EMAIL Managed field):\n{derived_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::RtFilter => {
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
                Ok(())
            }
            Self::RtFieldOps => {
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
                Ok(())
            }
            Self::RtEquilookup => {
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
                Ok(())
            }
            Self::RtUnion => {
                if !(apply_out.contains(RT_UNION_EAST_TABLE) && apply_out.contains(RT_UNION_WEST_TABLE))
                {
                    return Err(CliError::Failed(format!(
                        "Lab Scenario apply must Initial Load both union Bases:\n{apply_out}"
                    )));
                }
                if !(apply_out.to_ascii_lowercase().contains("derived")
                    || apply_out.contains(RT_UNION_PIPELINE))
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
                        RT_UNION_PIPELINE,
                    ],
                )
                .await?;
                if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
                    && managed_field_present(&derived_after_apply, "NAME", "Zoe")
                    && managed_field_present(&derived_after_apply, "NAME", "Wade")
                    && !derived_after_apply.contains("alice@example.com"))
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load union Derived check failed \
(expected Alice/Zoe/Wade without EMAIL):\n{derived_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::RtUnwind => {
                if !(apply_out.contains(RT_UNWIND_CUSTOMERS_TABLE)
                    && apply_out.contains(RT_UNWIND_ORDERS_TABLE))
                {
                    return Err(CliError::Failed(format!(
                        "Lab Scenario apply must Initial Load both equiLookup Bases for unwind:\n{apply_out}"
                    )));
                }
                if !(apply_out.to_ascii_lowercase().contains("derived")
                    || apply_out.contains(RT_UNWIND_PIPELINE))
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
                        RT_UNWIND_PIPELINE,
                    ],
                )
                .await?;
                if !(managed_field_present(&derived_after_apply, "NAME", "Alice")
                    && derived_after_apply.contains("ORDER_ID")
                    && (derived_after_apply.contains("42.50") || derived_after_apply.contains("42.5"))
                    && derived_after_apply.contains("101")
                    && managed_field_present(&derived_after_apply, "NAME", "Bob")
                    && !derived_after_apply.contains("\"orders\""))
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load unwind Derived check failed \
(expected flattened ORDER_ID rows for Alice/Bob, no orders array):\n{derived_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::RtDistinctAddtoset => {
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
                // Amount inspect may be integer JSON (10), trimmed decimal ("42.5"),
                // or fixed-scale (42.50) depending on Base NUMBER scale materialization.
                let amounts_ok = (add_after_apply.contains("42.50")
                    || add_after_apply.contains("42.5"))
                    && (add_after_apply.contains("10.00")
                        || add_after_apply.contains("10,")
                        || add_after_apply.contains("\"10\""))
                    && (add_after_apply.contains("5.00")
                        || add_after_apply.contains("[\n    5\n")
                        || add_after_apply.contains("\"5\"")
                        || add_after_apply.contains("  5\n"));
                if !(distinct_after_apply.contains("\"CUSTOMER_ID\": 1")
                    && distinct_after_apply.contains("\"CUSTOMER_ID\": 2")
                    && amounts_ok)
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load distinct/addToSet Derived check failed.\n\
distinct:\n{distinct_after_apply}\naddToSet:\n{add_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::PoisonQuarantine => Ok(()),
            Self::TransformPipeline => {
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
                if !(inspect_mentions_amount(&derived_after_apply, "30")
                    && inspect_mentions_amount(&derived_after_apply, "5"))
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load Derived check failed (expected totals 30 and 5):\n{derived_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::ConcurrentSourceWorkload => {
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
                if !(inspect_mentions_amount(&derived_after_apply, "30")
                    && inspect_mentions_amount(&derived_after_apply, "5"))
                {
                    return Err(CliError::Failed(format!(
                        "Initial Load Derived check failed (expected totals 30 and 5):\n{derived_after_apply}"
                    )));
                }
                Ok(())
            }
            Self::ChangeOrdering => {
                // Thin hook: only gate Derived materialization. Final Managed /
                // min-recompute outcomes live in recipe checks.correctness (#205).
                if !(apply_out.to_ascii_lowercase().contains("derived")
                    || apply_out.contains(CHANGE_ORDERING_ORDER_STATS_PIPELINE))
                {
                    return Err(CliError::Failed(format!(
                        "Lab Scenario apply did not materialize Transform Derived Dataset:\n{apply_out}"
                    )));
                }
                Ok(())
            }
            Self::ObservabilitySurface => {
                if !(apply_out.contains("\"event\":\"initial_load_complete\"")
                    || apply_out.contains("\"event\": \"initial_load_complete\"")
                    || apply_out.contains("\"event\":\"delivery_complete\"")
                    || apply_out.contains("\"event\": \"delivery_complete\""))
                {
                    return Err(CliError::Failed(format!(
                        "expected structured Initial Load / Delivery operator events on apply:\n{apply_out}"
                    )));
                }
                Ok(())
            }
            Self::ChangePipeline => {
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
                Ok(())
            }
            Self::InitialLoadThrottled => {
                if !(apply_out.contains("Initial Load paused")
                    || apply_out.contains("initial_load_paused")
                    || apply_out.contains("\"event\":\"initial_load_paused\""))
                {
                    return Err(CliError::Failed(format!(
                        "expected Initial Load pause after bounded chunks:\n{apply_out}"
                    )));
                }
                let progress_paused = apply_out
                    .lines()
                    .filter(|l| l.contains("initial_load_progress") || l.contains("Initial Load progress"))
                    .count();
                if progress_paused < 2 {
                    return Err(CliError::Failed(format!(
                        "expected >=2 Initial Load progress signals before pause, got {progress_paused}:\n{apply_out}"
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
                Ok(())
            }
            Self::BulkLoad
            | Self::IdempotentRedelivery
            | Self::PauseResume
            | Self::RemovePipeline
            | Self::SchemaChangePause
            | Self::SourceAlignment
            | Self::DriftCheck
            | Self::BoundedBackpressure
            | Self::PlatformStoreGuardrails
            | Self::BackwardCompatibleUpgrades
            | Self::MegaMix => Ok(()),
        }
    }

    /// Rare mutate escapes after shared `namespace.lifecycle.mutate_sql` (when set).
    /// Isomorphic SQL mutates are recipe-driven; this stays for CLI verbs, parallel
    /// sessions, generated backlog, and other non-recipe patterns.
    async fn mutate(&self, lab_dir: &Path) -> Result<(), CliError> {
        match self {
            // Recipe-driven mutate_sql already applied by the shared Namespace lifecycle.
            Self::DirectPipeline
            | Self::RtProject
            | Self::RtFilter
            | Self::RtFieldOps
            | Self::RtEquilookup
            | Self::RtUnion
            | Self::RtUnwind
            | Self::RtDistinctAddtoset
            | Self::PoisonQuarantine
            | Self::TransformPipeline
            | Self::ChangeOrdering
            | Self::IdempotentRedelivery
            | Self::SchemaChangePause
            | Self::SourceAlignment
            | Self::BulkLoad
            | Self::PlatformStoreGuardrails
            | Self::BackwardCompatibleUpgrades
            | Self::InitialLoadThrottled
            | Self::MegaMix => Ok(()),
            Self::ConcurrentSourceWorkload => {
                println!(
                    "Lab Scenario: driving concurrent Source workload \
(parallel customers + orders sessions)..."
                );
                mutate_concurrent_source_workload(lab_dir).await
            }
            Self::BoundedBackpressure => {
                println!(
                    "Lab Scenario: inserting Source backlog ({BOUNDED_BACKPRESSURE_BACKLOG} rows)..."
                );
                insert_bounded_backpressure_backlog(lab_dir).await
            }
            Self::ObservabilitySurface => {
                println!(
                    "Lab Scenario: inserting Source backlog ({OBSERVABILITY_SURFACE_BACKLOG} rows)..."
                );
                insert_observability_surface_backlog(lab_dir).await
            }
            Self::PauseResume => {
                println!("Lab Scenario: pause Pipeline {PAUSE_RESUME_CUSTOMERS_PIPELINE} via CLI...");
                let bin = lab_migraloop_bin();
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
                mutate_pause_resume_source(lab_dir).await
            }
            Self::RemovePipeline => {
                println!(
                    "Lab Scenario: remove Pipeline {REMOVE_PIPELINE_CUSTOMERS_PIPELINE} via CLI..."
                );
                let bin = lab_migraloop_bin();
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
                if status_after_remove
                    .contains(&format!("Pipeline: {REMOVE_PIPELINE_CUSTOMERS_PIPELINE} ("))
                {
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
                if !status_after_remove
                    .contains(&format!("Base Dataset: {REMOVE_PIPELINE_CUSTOMERS_TABLE}"))
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
                mutate_remove_pipeline_source(lab_dir).await
            }
            Self::ChangePipeline => {
                println!(
                    "Lab Scenario: apply semantic Transform revision (ACTIVE==0) via real product path..."
                );
                let bin = lab_migraloop_bin();
                let semantic_config = scenario_config_path(
                    lab_dir,
                    CHANGE_PIPELINE_ID,
                    CHANGE_PIPELINE_SEMANTIC_CONFIG,
                )?;
                let apply_v2 = run_product_cli(
                    &bin,
                    &[
                        "apply",
                        "--platform-store-url",
                        LAB_PLATFORM_STORE_URL,
                        "--file",
                        semantic_config.to_str().ok_or_else(|| {
                            CliError::Failed(
                                "Scenario semantic revision path is not valid UTF-8".to_string(),
                            )
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
                Ok(())
            }
            Self::DriftCheck => {
                println!("Lab Scenario: align Base as trusted Drift baseline...");
                let bin = lab_migraloop_bin();
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
                plant_drift_check_target_drift(lab_dir, 1, "DRIFTED", true).await
            }
        }
    }

    async fn before_sync(&self, lab_dir: &Path) -> Result<(), CliError> {
        match self {
            Self::PoisonQuarantine => {
                println!(
                    "Lab Scenario: sync Incremental Capture + Delivery with poison injection \
                     for Output Identity {POISON_QUARANTINE_IDENTITY}..."
                );
            }
            Self::BoundedBackpressure => {
                println!(
                    "Lab Scenario: sync under Downstream delay with queue capacity={} \
(fail after {} durable checkpoint)...",
                    BOUNDED_BACKPRESSURE_CAPACITY, BOUNDED_BACKPRESSURE_FAIL_AFTER
                );
            }
            Self::ObservabilitySurface => {
                println!(
                    "Lab Scenario: sync under Downstream delay with queue capacity={} \
(fail after {} durable checkpoint)...",
                    OBSERVABILITY_SURFACE_CAPACITY, OBSERVABILITY_SURFACE_FAIL_AFTER
                );
            }
            Self::SchemaChangePause => {
                let bin = lab_migraloop_bin();
                let status_before = run_product_cli(
                    &bin,
                    &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
                )
                .await?;
                let checkpoint = parse_capture_checkpoint(&status_before).ok_or_else(|| {
                    CliError::Failed(format!(
                        "could not parse capture checkpoint from status before sync:\n{status_before}"
                    ))
                })?;
                let inject_scn = checkpoint.saturating_add(1) as u64;
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
                println!(
                    "Lab Scenario: sync Incremental Capture with Schema Change event for the Source DDL \
         (drop managed NAME at scn={inject_scn}; inject bridges LogMiner DDL capture gap)..."
                );
            }
            _ => {}
        }
        Ok(())
    }

    /// Product `sync` invocation inputs (#180).
    ///
    /// `args` carry typed SyncOptions CLI flags. `env` is only for non-SyncOptions
    /// Lab bridges (Schema Change inject file path for the LogMiner DDL gap).
    fn sync_invocation(&self, lab_dir: &Path) -> (Vec<String>, Vec<(String, String)>) {
        match self {
            Self::PoisonQuarantine => (
                vec![
                    "--sync-poison-identity".to_string(),
                    POISON_QUARANTINE_IDENTITY.to_string(),
                    "--sync-poison-max-attempts".to_string(),
                    POISON_QUARANTINE_MAX_ATTEMPTS.to_string(),
                ],
                vec![],
            ),
            Self::BoundedBackpressure => (
                vec![
                    "--sync-queue-capacity".to_string(),
                    BOUNDED_BACKPRESSURE_CAPACITY.to_string(),
                    "--sync-delivery-delay-ms".to_string(),
                    BOUNDED_BACKPRESSURE_DELAY_MS.to_string(),
                    "--sync-fail-after-changes".to_string(),
                    BOUNDED_BACKPRESSURE_FAIL_AFTER.to_string(),
                ],
                vec![],
            ),
            Self::ObservabilitySurface => (
                vec![
                    "--sync-queue-capacity".to_string(),
                    OBSERVABILITY_SURFACE_CAPACITY.to_string(),
                    "--sync-delivery-delay-ms".to_string(),
                    OBSERVABILITY_SURFACE_DELAY_MS.to_string(),
                    "--sync-fail-after-changes".to_string(),
                    OBSERVABILITY_SURFACE_FAIL_AFTER.to_string(),
                ],
                vec![],
            ),
            Self::SchemaChangePause => {
                let inject_path = lab_dir.join(".schema-change-pause-inject.json");
                (
                    vec![],
                    vec![(
                        "MIGRALOOP_INJECT_SCHEMA_CHANGES".to_string(),
                        inject_path.to_string_lossy().into_owned(),
                    )],
                )
            }
            _ => (vec![], vec![]),
        }
    }

    async fn after_sync(
        &self,
        lab_dir: &Path,
        sync_out: &str,
        sync_ok: bool,
    ) -> Result<(), CliError> {
        let bin = lab_migraloop_bin();
        match self {
            Self::PoisonQuarantine => {
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
                Ok(())
            }
            Self::PauseResume => {
                if sync_mentions_pipeline_delivery(&sync_out, PAUSE_RESUME_CUSTOMERS_PIPELINE)
                {
                    return Err(CliError::Failed(format!(
                        "paused Pipeline must not Deliver during sync:\n{sync_out}"
                    )));
                }
                if !(sync_mentions_pipeline_delivery(&sync_out, PAUSE_RESUME_ORDERS_PIPELINE)
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
                Ok(())
            }
            Self::RemovePipeline => {
                // Match pipeline name with a token boundary so
                // `lab-rp-customers-reporting` is not mistaken for `lab-rp-customers`.
                if sync_mentions_pipeline_delivery(
                    &sync_out,
                    REMOVE_PIPELINE_CUSTOMERS_PIPELINE,
                ) {
                    return Err(CliError::Failed(format!(
                        "removed Pipeline must not Deliver during sync:\n{sync_out}"
                    )));
                }
                if !(sync_mentions_pipeline_delivery(
                    &sync_out,
                    REMOVE_PIPELINE_REPORTING_PIPELINE,
                ) || sync_out.contains(REMOVE_PIPELINE_REPORTING_PIPELINE))
                {
                    return Err(CliError::Failed(format!(
                        "remaining Pipeline must still Deliver from Shared Base during sync:\n{sync_out}"
                    )));
                }
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
                Ok(())
            }
            Self::SchemaChangePause => {
                let inject_path = lab_dir.join(".schema-change-pause-inject.json");
                let _ = fs::remove_file(&inject_path);
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
                Ok(())
            }
            Self::BoundedBackpressure => {
                if sync_ok {
                    return Err(CliError::Failed(format!(
                        "expected mid-sync stop under Downstream slowness (FAIL_AFTER), got success:\n{sync_out}"
                    )));
                }
                let slow_lower = sync_out.to_ascii_lowercase();
                if !(sync_out.contains("Backpressure:") || slow_lower.contains("backpressure")) {
                    return Err(CliError::Failed(format!(
                        "expected Backpressure signal while Downstream is slow:\n{sync_out}"
                    )));
                }
                let mut peak_depth = 0i32;
                for line in sync_out.lines() {
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
                        "queue_depth must stay within capacity=2 under backpressure, peak={peak_depth}:\n{sync_out}"
                    )));
                }
                if slow_lower
                    .lines()
                    .any(|line| line.contains("paused") && line.contains(BOUNDED_BACKPRESSURE_PIPELINE))
                {
                    return Err(CliError::Failed(format!(
                        "backpressure must not pause the Pipeline for Downstream slowness:\n{sync_out}"
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
                let delivery_lag =
                    parse_delivery_lag_for_pipeline(&status_mid, BOUNDED_BACKPRESSURE_PIPELINE)
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
                Ok(())
            }
            Self::ObservabilitySurface => {
                if sync_ok {
                    return Err(CliError::Failed(format!(
                        "expected mid-sync stop under Downstream slowness (FAIL_AFTER), got success:\n{sync_out}"
                    )));
                }
                if !(sync_out.contains("\"event\":\"backpressure\"")
                    || sync_out.contains("\"event\": \"backpressure\""))
                {
                    return Err(CliError::Failed(format!(
                        "expected structured backpressure event JSON:\n{sync_out}"
                    )));
                }
                if !(sync_out.contains("\"event\":\"incremental_capture\"")
                    || sync_out.contains("\"event\": \"incremental_capture\""))
                {
                    return Err(CliError::Failed(format!(
                        "expected structured incremental_capture event JSON:\n{sync_out}"
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
                    parse_delivery_lag_for_pipeline(&status_mid, OBSERVABILITY_SURFACE_PIPELINE)
                        .ok_or_else(|| {
                            CliError::Failed(format!(
                                "could not parse Delivery Health lag under Observability Surface probe:\n{status_mid}"
                            ))
                        })?;
                if delivery_lag < 10 {
                    return Err(CliError::Failed(format!(
                        "Delivery Health lag must reflect Downstream backlog, got {delivery_lag}:\n{status_mid}"
                    )));
                }
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
                Ok(())
            }
            Self::ChangePipeline => {
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
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Recipe-driven isomorphic assert (#205): run checks.correctness then adapter_ok.
    async fn assert_via_recipe_correctness(
        lab_dir: &Path,
        recipe: &ScenarioRecipe,
        ctx: &ProductPathRunContext,
        rows_applied: u64,
        passed_msg: &str,
    ) -> Result<AdapterOutcome, CliError> {
        execute_recipe_correctness(lab_dir, recipe).await?;
        println!("Lab Scenario: correctness checks passed ({passed_msg})");
        if !ctx.sync_out.trim().is_empty() {
            println!(
                "Lab Scenario: Incremental Capture ({}) and Delivery complete",
                ctx.capture_path_note
            );
        }
        Ok(adapter_ok(rows_applied, ctx.capture_path_note.clone()))
    }

    async fn assert_correctness(
        &self,
        lab_dir: &Path,
        recipe: &ScenarioRecipe,
        ctx: &ProductPathRunContext,
    ) -> Result<AdapterOutcome, CliError> {
        let bin = lab_migraloop_bin();
        let rows_applied =
            count_delivery_ops(&ctx.apply_out) + count_delivery_ops(&ctx.sync_out);
        match self {
            Self::DirectPipeline
            | Self::RtProject
            | Self::RtFilter
            | Self::RtFieldOps
            | Self::RtEquilookup
            | Self::RtUnion
            | Self::RtUnwind
            | Self::RtDistinctAddtoset
            | Self::PoisonQuarantine
            | Self::TransformPipeline
            | Self::ChangeOrdering => {
                let passed_msg = match self {
                    Self::DirectPipeline => "Base + Target Managed outcomes",
                    Self::RtProject => "projected Derived + Target Managed outcomes",
                    Self::RtFilter => "filtered Derived + Target Managed outcomes",
                    Self::RtFieldOps => {
                        "addFields/rename/remove Derived + Target Managed outcomes"
                    }
                    Self::RtEquilookup => {
                        "equiLookup multi-Base Derived + Target Managed outcomes"
                    }
                    Self::RtUnion => "union multi-Base Derived + Target Managed outcomes",
                    Self::RtUnwind => "unwind Output Identities insert/update/delete",
                    Self::RtDistinctAddtoset => "distinct + addToSet Derived/Target outcomes",
                    Self::PoisonQuarantine => {
                        "poison identity quarantined; Pipeline continued; status unhealthy"
                    }
                    Self::TransformPipeline => "Base + Derived + Target Managed outcomes",
                    Self::ChangeOrdering => {
                        "Change Ordering same-key / cross-key / min Base recompute outcomes"
                    }
                    _ => unreachable!("recipe-driven assert variants only"),
                };
                Self::assert_via_recipe_correctness(
                    lab_dir, recipe, ctx, rows_applied, passed_msg,
                )
                .await
            }
            Self::ConcurrentSourceWorkload => {
                    let max_settle_ms = recipe.thresholds.max_settle_ms.ok_or_else(|| {
                        CliError::Failed(
                            "Lab Scenario concurrent-source-workload recipe must declare thresholds.max_settle_ms"
                                .to_string(),
                        )
                    })?;
                    // US47: wait until Delivery catches up within recipe thresholds before final asserts.
                    println!(
                        "Lab Scenario: settling Incremental Capture + Delivery within max_settle_ms={max_settle_ms}..."
                    );
                    let settle_started = Instant::now();
                    let mut sync_out;
                    let mut capture_note = String::new();
                    let mut last_detail = String::new();

                    loop {
                        let settle_ms = settle_started.elapsed().as_millis();
                        if settle_ms > max_settle_ms {
                            // Outcomes never reached expected Managed state — correctness fails.
                            // Runner also fails recipe max_settle_ms (equal-weight threshold axis).
                            return Ok(AdapterOutcome {
                                correctness: false,
                                detail: format!(
                                    "correctness: concurrent Source Managed outcomes not settled \
                (elapsed settle_ms={settle_ms}). {last_detail}"
                                ),
                                metrics: ScenarioMetrics {
                                    settle_ms: Some(settle_ms),
                                    lag: None,
                                    rows_per_s: None,
                                    duration_ms: None,
                                    rows_applied: count_delivery_ops(&ctx.apply_out) + count_delivery_ops(&ctx.sync_out),
                                    capture_path_note: capture_note,
                                },
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

                        // Settle until recipe checks.correctness pass (#205).
                        let fetched = fetch_all(&recipe.checks.correctness).await?;
                        if fetched_satisfies(&recipe.checks.correctness, &fetched) {
                            break;
                        }

                        last_detail = format!(
                            "correctness not yet settled against recipe checks.correctness                              ({} inspect surfaces).",
                            fetched.len()
                        );
                        tokio::time::sleep(CONCURRENT_SETTLE_POLL).await;
                    }

                    let settle_ms = settle_started.elapsed().as_millis();
                    let rows_applied = count_delivery_ops(&ctx.apply_out) + count_delivery_ops(&ctx.sync_out);
                    println!(
                        "Lab Scenario: correctness checks passed after concurrent Source settle \
                (settle_ms={settle_ms}, Base + Derived + Target Managed outcomes)"
                    );
                    if !ctx.sync_out.trim().is_empty() {
                        println!("Lab Scenario: Incremental Capture ({}) and Delivery complete", ctx.capture_path_note);
                    }

                    // Runner evaluates recipe max_settle_ms against measured settle_ms (US21 / US36).
                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: Some(settle_ms),
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied,
                            capture_path_note: capture_note,
                        },
                    })
            }

            Self::BulkLoad => {
                    let max_lag = recipe.thresholds.max_lag.ok_or_else(|| {
                        CliError::Failed("Lab Scenario bulk-load recipe must declare thresholds.max_lag".to_string())
                    })?;
                    let max_duration_ms = recipe.thresholds.max_duration_ms.ok_or_else(|| {
                        CliError::Failed(
                            "Lab Scenario bulk-load recipe must declare thresholds.max_duration_ms".to_string(),
                        )
                    })?;
                    let load_started = ctx.apply_started.ok_or_else(|| {
                        CliError::Failed("internal: bulk-load missing apply_started".to_string())
                    })?;
                    // US47: wait until Delivery/Health catch up within recipe duration before final asserts.
                    println!(
                        "Lab Scenario: settling bulk Delivery / Sync Health within \
                max_duration_ms={max_duration_ms}..."
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

                        let fetched = fetch_all(&recipe.checks.correctness).await?;
                        let rows_ok = fetched_satisfies(&recipe.checks.correctness, &fetched);
                        let lag_ok = lag <= max_lag;
                        if rows_ok && lag_ok {
                            settled = true;
                            break (base_rows, target_rows, lag);
                        }

                        last_detail = format!(
                            "bulk Delivery/Health not yet caught up \
                (base_rows={base_rows} target_rows={target_rows} lag={lag}).\n\
                Base:\n{base_after}\nTarget:\n{target_after}\nStatus:\n{status_out}"
                        );
                        if measured_duration_ms > max_duration_ms {
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out).max(base_rows);
                    // Correctness is recipe checks.correctness; runner evaluates lag/duration/throughput.
                    let correctness = match fetch_all(&recipe.checks.correctness).await {
                        Ok(fetched) => fetched_satisfies(&recipe.checks.correctness, &fetched),
                        Err(_) => false,
                    };
                    let mut detail = if correctness {
                        String::new()
                    } else {
                        format!(
                            "correctness: expected rows={BULK_LOAD_ROW_COUNT} \
                base_rows={base_rows} target_rows={target_rows}"
                        )
                    };
                    if !settled && !last_detail.is_empty() {
                        let settle_note = format!(
                            "bulk Delivery/Health settle incomplete within \
                max_duration_ms={max_duration_ms}. {last_detail}"
                        );
                        if detail.is_empty() {
                            detail = settle_note;
                        } else {
                            detail = format!("{detail}; {settle_note}");
                        }
                    }

                    if correctness {
                        println!(
                            "Lab Scenario: correctness passed \
                (base/target rows={BULK_LOAD_ROW_COUNT}, lag={lag}, \
                duration_ms={measured_duration_ms}, rows_per_s={measured_rows_per_s:.2}); \
                thresholds evaluated by recipe-driven runner"
                        );
                    } else {
                        println!(
                            "Lab Scenario: correctness failed (base_rows={base_rows} target_rows={target_rows}); \
                metrics lag={lag} duration_ms={measured_duration_ms} rows_per_s={measured_rows_per_s:.2}"
                        );
                    }

                    Ok(AdapterOutcome {
                        correctness,
                        detail,
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: Some(lag),
                            rows_per_s: Some(measured_rows_per_s),
                            duration_ms: Some(measured_duration_ms),
                            rows_applied,
                            capture_path_note: "Initial Load".to_string(),
                        },
                    })
            }

            Self::IdempotentRedelivery => {
                    let config_path = deployment_config_path(lab_dir, IDEMPOTENT_REDELIVERY_ID)?;
                    let config_str = config_path.to_str().ok_or_else(|| {
                        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
                    })?;
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
                    lab_update_pipeline_delivery_status(IDEMPOTENT_REDELIVERY_DEPLOYMENT,
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

                    if docs_after != docs_before {
                        return Err(CliError::Failed(format!(
                            "document count changed across re-Delivery                              (docs_before={docs_before} docs_after={docs_after}).
                             Base:
{base_after}
Target:
{target_after}"
                        )));
                    }

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out)
                        + count_delivery_ops(&reapply_out);

                    execute_recipe_correctness(lab_dir, recipe).await?;

                    println!(
                        "Lab Scenario: correctness checks passed \
                         (Managed outcomes stable; document count={docs_after}; non-Managed field preserved)"
                    );
                    if !ctx.sync_out.trim().is_empty() {
                        println!("Lab Scenario: Incremental Capture ({}) and Delivery complete", ctx.capture_path_note);
                    }
                    println!("Lab Scenario: duplicate-safe re-Delivery complete on real product apply path");

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: ctx.capture_path_note.clone(),
                        },
                    })
            }

            Self::PauseResume => {


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

                    execute_recipe_correctness(lab_dir, recipe).await?;

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

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out)
                        + count_delivery_ops(&resume_out);

                    println!(
                        "Lab Scenario: correctness checks passed \
                         (pause stopped customers Delivery; resume catch-up; orders unaffected)"
                    );
                    if !ctx.sync_out.trim().is_empty() {
                        println!(
                            "Lab Scenario: Incremental Capture ({}) complete",
                            ctx.capture_path_note
                        );
                    }

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: ctx.capture_path_note.clone(),
                        },
                    })
            }

            Self::BoundedBackpressure => {


                    println!("Lab Scenario: catch-up sync without Downstream delay...");
                    let catch_out = run_product_cli(
                        &bin,
                        &[
                            "sync",
                            "--platform-store-url",
                            LAB_PLATFORM_STORE_URL,
                            "--sync-queue-capacity",
                            BOUNDED_BACKPRESSURE_CAPACITY,
                        ],
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out)
                        + count_delivery_ops(&catch_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (bounded backpressure; visible lag; catch-up; Pipeline not paused)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: Some(sync_lag_after),
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: capture_note,
                        },
                    })
            }

            Self::InitialLoadThrottled => {
                    let config_path = deployment_config_path(lab_dir, INITIAL_LOAD_THROTTLED_ID)?;
                    let config_str = config_path.to_str().ok_or_else(|| {
                        CliError::Failed("Scenario deployment path is not valid UTF-8".to_string())
                    })?;
                    println!(
                        "Lab Scenario: resume apply with rate_limit={INITIAL_LOAD_THROTTLED_RATE}/s \
                and store delay for backoff (typed ApplyOptions)..."
                    );
                    let resume_out = run_product_cli_with_env(
                        &bin,
                        &[
                            "apply",
                            "--platform-store-url",
                            LAB_PLATFORM_STORE_URL,
                            "--file",
                            config_str,
                            "--initial-load-chunk-size",
                            INITIAL_LOAD_THROTTLED_CHUNK_SIZE,
                            "--initial-load-rows-per-sec",
                            INITIAL_LOAD_THROTTLED_RATE,
                            "--initial-load-store-delay-ms",
                            INITIAL_LOAD_THROTTLED_STORE_DELAY_MS,
                        ],
                        &[],
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
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: {INITIAL_LOAD_THROTTLED_ID} checks passed \
                         (chunked progress; pause/resume; rate_limit; backoff; watermark retained)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: Some(0),
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: INITIAL_LOAD_THROTTLED_ROW_COUNT as u64,
                            capture_path_note: capture_note,
                        },
                    })
            }

            Self::ObservabilitySurface => {
                    println!("Lab Scenario: catch-up sync without Downstream delay...");
                    let catch_out = run_product_cli(
                        &bin,
                        &[
                            "sync",
                            "--platform-store-url",
                            LAB_PLATFORM_STORE_URL,
                            "--sync-queue-capacity",
                            OBSERVABILITY_SURFACE_CAPACITY,
                        ],
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out)
                        + count_delivery_ops(&catch_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (structured logs; Sync/Delivery Health; Prometheus lag/failures)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: Some(sync_lag_after),
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: capture_note,
                        },
                    })
            }

            Self::PlatformStoreGuardrails => {


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

                    let rows_applied = count_delivery_ops(&ctx.apply_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (Guardrails minimums ok; disk warn-only; Pipeline not paused)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: "LogMiner".to_string(),
                        },
                    })
            }

            Self::BackwardCompatibleUpgrades => {
                    let older_config_path = scenario_config_path(
                        lab_dir,
                        BACKWARD_COMPATIBLE_UPGRADES_ID,
                        BACKWARD_COMPATIBLE_UPGRADES_OLDER_CONFIG,
                    )?;
                    let older_config_str = older_config_path.to_str().ok_or_else(|| {
                        CliError::Failed("Scenario older-config path is not valid UTF-8".to_string())
                    })?;
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (migrate preserved Deployment; older v1.0.0 config applies without rebuild)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: "LogMiner".to_string(),
                        },
                    })
            }

            Self::SchemaChangePause => {


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

                    let rows_applied = count_delivery_ops(&ctx.apply_out) + count_delivery_ops(&ctx.sync_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (blocking DDL warn+pause; distinct from poison quarantine)"
                    );
                    if !ctx.sync_out.trim().is_empty() {
                        println!(
                            "Lab Scenario: Incremental Capture ({}) complete",
                            ctx.capture_path_note
                        );
                    }

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied,
                            capture_path_note: ctx.capture_path_note.clone(),
                        },
                    })
            }

            Self::SourceAlignment => {


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

                    let rows_applied = count_delivery_ops(&ctx.apply_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (detect + repair Base from Source; resource-gated max-rows; Source not written)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: "Source Alignment Check (OCI reads)".to_string(),
                        },
                    })
            }

            Self::DriftCheck => {
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out);
                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (detect + Managed auto-repair; non-Managed preserved; resource-gated max-rows)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: "Drift Check (Managed-field Target repair)".to_string(),
                        },
                    })
            }

            Self::RemovePipeline => {


                    execute_recipe_correctness(lab_dir, recipe).await?;

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out);

                    println!(
                        "Lab Scenario: correctness checks passed \
                         (remove ceased customers Delivery; Shared Base kept; reporting Delivered)"
                    );
                    if !ctx.sync_out.trim().is_empty() {
                        println!(
                            "Lab Scenario: Incremental Capture ({}) complete",
                            ctx.capture_path_note
                        );
                    }

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: ctx.capture_path_note.clone(),
                        },
                    })
            }

            Self::ChangePipeline => {
                    let metadata_config =
                        scenario_config_path(lab_dir, CHANGE_PIPELINE_ID, CHANGE_PIPELINE_METADATA_CONFIG)?;
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

                    let rows_applied = count_delivery_ops(&ctx.apply_out)
                        + count_delivery_ops(&ctx.sync_out)
                        + count_delivery_ops(&apply_meta);

                    execute_recipe_correctness(lab_dir, recipe).await?;
                    println!(
                        "Lab Scenario: correctness checks passed \
                         (semantic revision rebuilt Derived/re-Delivered; incremental continued; \
                Shared Base kept; metadata-only skipped)"
                    );

                    Ok(AdapterOutcome {
                        correctness: true,
                        detail: String::new(),
                        metrics: ScenarioMetrics {
                            settle_ms: None,
                            lag: None,
                            rows_per_s: None,
                            duration_ms: None,
                            rows_applied: rows_applied,
                            capture_path_note: String::new(),
                        },
                    })
            }

            Self::MegaMix => run_mega_mix_protocol(lab_dir, recipe, ctx).await,
        }
    }
}

/// Solo baseline → mix Incremental → correctness for mega-mix (#251 / ADR-0031).
async fn run_mega_mix_protocol(
    lab_dir: &Path,
    recipe: &ScenarioRecipe,
    ctx: &ProductPathRunContext,
) -> Result<AdapterOutcome, CliError> {
    let pipelines = mega_mix_pipelines();
    println!(
        "Lab Scenario: mega-mix solo baseline protocol \
(same Fixture sizing; pause siblings; Incremental batch={INCREMENTAL_BATCH_ROWS})..."
    );

    let mut solo_qps: Vec<f64> = Vec::with_capacity(pipelines.len());
    for (idx, pipe) in pipelines.iter().enumerate() {
        mega_mix_pause_all_except(lab_dir, pipe.name).await?;
        let started = Instant::now();
        let mut source_rows = 0u64;
        for table in pipe.workload_tables {
            let id_base = SOLO_ID_BASE + (idx as i64) * 1_000;
            let (sql, rows) = incremental_batch_sql(table, id_base, INCREMENTAL_BATCH_ROWS);
            if rows == 0 {
                return Err(CliError::Failed(format!(
                    "mega-mix solo: unsupported workload table {table}"
                )));
            }
            source_rows += rows;
            run_oracle_sql_body(lab_dir, &sql).await?;
        }
        mega_mix_sync_until_pipeline_lag_zero(lab_dir, pipe.name, pipe.workload_tables).await?;
        let qps = e2e_qps(source_rows, started.elapsed().as_secs_f64());
        println!(
            "Lab Scenario: solo baseline pipeline={} path={} qps_solo={qps:.2} rows={source_rows}",
            pipe.name,
            pipe.path.as_str()
        );
        solo_qps.push(qps);
    }
    mega_mix_resume_all(lab_dir).await?;

    println!(
        "Lab Scenario: mega-mix mix Incremental protocol \
(all Pipelines active; batch={INCREMENTAL_BATCH_ROWS})..."
    );
    let mix_started = Instant::now();
    let mut mix_source_rows: Vec<u64> = Vec::with_capacity(pipelines.len());
    // Shared Source tables (distinct + addtoset) get one insert pass.
    let mut driven: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for (idx, pipe) in pipelines.iter().enumerate() {
        let mut rows_for_pipe = 0u64;
        for table in pipe.workload_tables {
            if !driven.insert(*table) {
                // Already driven this table in the mix window; still count rows for QPS.
                rows_for_pipe += INCREMENTAL_BATCH_ROWS;
                continue;
            }
            let id_base = MIX_ID_BASE + (idx as i64) * 1_000;
            let (sql, rows) = incremental_batch_sql(table, id_base, INCREMENTAL_BATCH_ROWS);
            rows_for_pipe += rows;
            run_oracle_sql_body(lab_dir, &sql).await?;
        }
        mix_source_rows.push(rows_for_pipe);
    }
    // One shared settle for the mix window (all Pipelines).
    for pipe in pipelines {
        mega_mix_sync_until_pipeline_lag_zero(lab_dir, pipe.name, pipe.workload_tables).await?;
    }
    let mix_elapsed = mix_started.elapsed().as_secs_f64();
    let mut samples = Vec::with_capacity(pipelines.len());
    for (idx, pipe) in pipelines.iter().enumerate() {
        let qps_mix = e2e_qps(mix_source_rows[idx], mix_elapsed);
        println!(
            "Lab Scenario: mix pipeline={} path={} qps_mix={qps_mix:.2} rows={}",
            pipe.name,
            pipe.path.as_str(),
            mix_source_rows[idx]
        );
        samples.push(PipelineQpsSample {
            name: pipe.name.to_string(),
            path: pipe.path,
            qps_solo: solo_qps[idx],
            qps_mix,
        });
    }

    // Correctness mutate + sync (Managed/Derived/Target convergence — not metrics-only).
    println!("Lab Scenario: mega-mix correctness mutate + sync...");
    mutate_namespace_from_recipe(lab_dir, recipe).await?;
    let correctness = mega_mix_sync_until_correctness(lab_dir, recipe).await?;

    let evidence = evaluate_mega_mix_gates(&samples, false);
    store_pending_evidence(evidence.clone());
    println!(
        "Lab Scenario: mega-mix gates gate_0_7={} gate_0_95={} \
direct_agg={:.2} transform_agg={:.2} (floors reported; not accept on #251)",
        if evidence.gate_0_7_pass {
            "pass"
        } else {
            "fail"
        },
        match evidence.gate_0_95_pass {
            Some(true) => "pass",
            Some(false) => "fail",
            None => "n/a",
        },
        evidence.direct_aggregate_qps,
        evidence.transform_aggregate_qps
    );

    let rows_applied = count_delivery_ops(&ctx.apply_out)
        + samples.len() as u64 * INCREMENTAL_BATCH_ROWS;
    let detail = if correctness {
        String::new()
    } else {
        "correctness: mega-mix Managed/Derived/Target outcomes not settled".to_string()
    };
    Ok(AdapterOutcome {
        correctness,
        detail,
        metrics: ScenarioMetrics {
            settle_ms: None,
            lag: None,
            rows_per_s: Some(evidence.sum_mix_qps),
            duration_ms: Some((mix_elapsed * 1000.0) as u128),
            rows_applied,
            capture_path_note: "LogMiner mega-mix Incremental".to_string(),
        },
    })
}

async fn mega_mix_pause_all_except(_lab_dir: &Path, keep: &str) -> Result<(), CliError> {
    let bin = lab_migraloop_bin();
    for pipe in mega_mix_pipelines() {
        if pipe.name == keep {
            // Ensure the measured Pipeline is running.
            let _ = run_product_cli(
                &bin,
                &[
                    "resume",
                    "--platform-store-url",
                    LAB_PLATFORM_STORE_URL,
                    "--pipeline",
                    pipe.name,
                    "--deployment",
                    MEGA_MIX_DEPLOYMENT,
                ],
            )
            .await;
            continue;
        }
        let out = run_product_cli(
            &bin,
            &[
                "pause",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--pipeline",
                pipe.name,
                "--deployment",
                MEGA_MIX_DEPLOYMENT,
            ],
        )
        .await?;
        if !out.to_ascii_lowercase().contains("paused") {
            return Err(CliError::Failed(format!(
                "mega-mix solo: failed to pause sibling Pipeline {}:\n{out}",
                pipe.name
            )));
        }
    }
    Ok(())
}

async fn mega_mix_resume_all(_lab_dir: &Path) -> Result<(), CliError> {
    let bin = lab_migraloop_bin();
    for pipe in mega_mix_pipelines() {
        let out = run_product_cli(
            &bin,
            &[
                "resume",
                "--platform-store-url",
                LAB_PLATFORM_STORE_URL,
                "--pipeline",
                pipe.name,
                "--deployment",
                MEGA_MIX_DEPLOYMENT,
            ],
        )
        .await?;
        if out.to_ascii_lowercase().contains("error")
            && !out.to_ascii_lowercase().contains("not paused")
            && !out.to_ascii_lowercase().contains("already")
        {
            // Best-effort: already-running is fine.
            if !out.to_ascii_lowercase().contains("resumed")
                && !out.to_ascii_lowercase().contains("running")
            {
                return Err(CliError::Failed(format!(
                    "mega-mix: failed to resume Pipeline {}:\n{out}",
                    pipe.name
                )));
            }
        }
    }
    Ok(())
}

async fn run_oracle_sql_body(lab_dir: &Path, sql_body: &str) -> Result<(), CliError> {
    let connect = format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1");
    let script = format!(
        "SET DEFINE OFF\nWHENEVER SQLERROR EXIT SQL.SQLCODE\n{sql_body}\nCOMMIT;\nEXIT;\n"
    );
    sqlplus_in_oracle(lab_dir, &connect, &script)
        .await
        .map(|_| ())
        .map_err(|err| CliError::Failed(format!("mega-mix Source SQL failed:\n{err}")))?;
    Ok(())
}

async fn mega_mix_sync_until_pipeline_lag_zero(
    _lab_dir: &Path,
    pipeline: &str,
    tables: &[&str],
) -> Result<(), CliError> {
    let bin = lab_migraloop_bin();
    let started = Instant::now();
    let mut last_status;
    loop {
        let sync_out = run_product_cli(
            &bin,
            &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        )
        .await?;
        if !sync_out.to_ascii_lowercase().contains("logminer")
            && !sync_out.to_ascii_lowercase().contains("no changes")
            && !sync_out.to_ascii_lowercase().contains("delivery")
        {
            // Still require real path when work happens; empty windows may say no changes.
            if sync_out.to_ascii_lowercase().contains("contract")
                || sync_out.to_ascii_lowercase().contains("stub")
            {
                return Err(CliError::Failed(format!(
                    "mega-mix sync must use real LogMiner path:\n{sync_out}"
                )));
            }
        }
        let status_out = run_product_cli(
            &bin,
            &["status", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        )
        .await?;
        last_status = status_out.clone();
        let delivery_lag = parse_delivery_lag_for_pipeline(&status_out, pipeline).unwrap_or(0);
        let tables_ok = tables.iter().all(|table| {
            parse_sync_lag_for_table(&status_out, table).unwrap_or(0) <= 0
        });
        if delivery_lag <= 0 && tables_ok {
            return Ok(());
        }
        if started.elapsed() > MEGA_MIX_WINDOW_MAX {
            return Err(CliError::Failed(format!(
                "mega-mix Incremental settle timed out for Pipeline {pipeline} \
(delivery_lag={delivery_lag}). Status:\n{last_status}"
            )));
        }
        tokio::time::sleep(MEGA_MIX_SETTLE_POLL).await;
    }
}

async fn mega_mix_sync_until_correctness(
    _lab_dir: &Path,
    recipe: &ScenarioRecipe,
) -> Result<bool, CliError> {
    let bin = lab_migraloop_bin();
    let started = Instant::now();
    let mut last_detail = String::new();
    loop {
        let sync_out = run_product_cli(
            &bin,
            &["sync", "--platform-store-url", LAB_PLATFORM_STORE_URL],
        )
        .await?;
        if sync_out.to_ascii_lowercase().contains("logminer")
            || sync_out.to_ascii_lowercase().contains("delivery")
            || sync_out.to_ascii_lowercase().contains("no changes")
        {
            // ok
        } else if sync_out.to_ascii_lowercase().contains("stub") {
            return Err(CliError::Failed(format!(
                "mega-mix correctness sync must use real LogMiner path:\n{sync_out}"
            )));
        }
        match fetch_all(&recipe.checks.correctness).await {
            Ok(fetched) if fetched_satisfies(&recipe.checks.correctness, &fetched) => {
                println!(
                    "Lab Scenario: mega-mix correctness checks passed \
(Managed/Derived/Target convergence)"
                );
                return Ok(true);
            }
            Ok(_) => {
                last_detail = "recipe checks.correctness not yet satisfied".to_string();
            }
            Err(err) => {
                last_detail = err.to_string();
            }
        }
        if started.elapsed() > MEGA_MIX_WINDOW_MAX {
            println!(
                "Lab Scenario: mega-mix correctness settle timed out ({last_detail})"
            );
            return Ok(false);
        }
        tokio::time::sleep(MEGA_MIX_SETTLE_POLL).await;
    }
}

/// Execute shared product-path steps from recipe data with Scenario-specific hooks.
async fn run_product_path_scenario(
    lab_dir: &Path,
    recipe: &ScenarioRecipe,
) -> Result<AdapterOutcome, CliError> {
    let product_path = recipe.workload.product_path.as_ref().ok_or_else(|| {
        CliError::Failed(
            "internal: run_product_path_scenario called without workload.product_path".to_string(),
        )
    })?;
    let steps = product_path_plan(recipe).ok_or_else(|| {
        CliError::Failed(
            "internal: product_path_plan missing despite workload.product_path".to_string(),
        )
    })?;
    let hooks = ProductPathHooks::for_recipe(recipe)?;
    let mut ctx = ProductPathRunContext {
        apply_out: String::new(),
        sync_out: String::new(),
        sync_ok: true,
        capture_path_note: String::new(),
        apply_started: None,
    };

    for step in steps {
        match step {
            ProductPathStepKind::PrepareNamespace => {
                prepare_namespace(lab_dir, recipe).await?;
                println!(
                    "Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)"
                );
            }
            ProductPathStepKind::ProductApply => {
                let (apply_cli_args, extra_env) = hooks.apply_invocation();
                ctx.apply_started = Some(Instant::now());
                ctx.apply_out = product_apply(
                    lab_dir,
                    &recipe.id,
                    &product_path.apply,
                    &apply_cli_args,
                    &extra_env,
                )
                .await?;
                hooks.after_apply(lab_dir, &ctx.apply_out).await?;
            }
            ProductPathStepKind::Mutate => {
                if mutate_namespace_from_recipe(lab_dir, recipe).await? {
                    println!("Lab Scenario: driving Source mutations from recipe lifecycle...");
                }
                hooks.mutate(lab_dir).await?;
            }
            ProductPathStepKind::ProductSync => {
                hooks.before_sync(lab_dir).await?;
                let (sync_cli_args, extra_env) = hooks.sync_invocation(lab_dir);
                let (sync_out, capture_note, sync_ok) =
                    product_sync(&product_path.sync, &sync_cli_args, &extra_env).await?;
                hooks.after_sync(lab_dir, &sync_out, sync_ok).await?;
                ctx.sync_out = sync_out;
                ctx.sync_ok = sync_ok;
                ctx.capture_path_note = capture_note;
            }
            ProductPathStepKind::Assert => {
                return hooks.assert_correctness(lab_dir, recipe, &ctx).await;
            }
        }
    }

    Err(CliError::Failed(format!(
        "Lab Scenario `{}` product_path completed without an `assert` step",
        recipe.id
    )))
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

/// Platform Store Deployment + cascaded Bases/Pipelines). Idempotent.





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

pub(super) async fn run_product_cli(bin: &Path, args: &[&str]) -> Result<String, CliError> {
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

/// True when sync output reports Delivery for exactly `pipeline` (not a longer
/// name that shares the same prefix, e.g. `lab-rp-customers` vs
/// `lab-rp-customers-reporting`).
fn sync_mentions_pipeline_delivery(sync_out: &str, pipeline: &str) -> bool {
    let needle = format!("Delivery complete: Pipeline {pipeline}");
    let mut start = 0;
    while let Some(rel) = sync_out[start..].find(&needle) {
        let abs = start + rel;
        let after = abs + needle.len();
        let boundary_ok = match sync_out.as_bytes().get(after) {
            None => true,
            Some(b) => !b.is_ascii_alphanumeric() && *b != b'-' && *b != b'_',
        };
        if boundary_ok {
            return true;
        }
        start = abs + 1;
    }
    false
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab_scenario::recipe::{
        load_recipe, validate_product_path, ProductPathStepKind, ScenarioRecipeChecks,
        ScenarioRecipeNamespace, ScenarioRecipeProductPath, ScenarioRecipeThresholds,
        ScenarioRecipeWorkload,
    };
    use crate::lab_scenario::runner::{
        evaluate_recipe_thresholds, product_path_plan, recipe_interface_summary,
    };
    use std::time::Duration;

    #[test]
    fn sync_pipeline_delivery_match_uses_name_boundary() {
        let reporting = "Delivery complete: Pipeline lab-rp-customers-reporting upserts=1";
        assert!(
            !sync_mentions_pipeline_delivery(reporting, "lab-rp-customers"),
            "reporting pipeline must not count as removed customers pipeline"
        );
        assert!(sync_mentions_pipeline_delivery(
            reporting,
            "lab-rp-customers-reporting"
        ));
        assert!(sync_mentions_pipeline_delivery(
            "Delivery complete: Pipeline lab-rp-customers upserts=1 deletes=0",
            "lab-rp-customers"
        ));
    }

    trait ProductPathStepSink {
        fn on_step(&mut self, step: ProductPathStepKind) -> Result<(), String>;
    }

    fn dispatch_product_path_steps<S: ProductPathStepSink>(
        steps: &[ProductPathStepKind],
        sink: &mut S,
    ) -> Result<(), String> {
        for step in steps {
            sink.on_step(*step)?;
        }
        Ok(())
    }

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
            ids.iter().any(|id| id == RT_UNION_ID),
            "catalog must include rt-union for shipped Rich Transform union"
        );
        assert!(
            ids.iter().any(|id| id == RT_UNWIND_ID),
            "catalog must include rt-unwind for shipped Rich Transform unwind"
        );
        assert!(
            ids.iter().any(|id| id == RT_DISTINCT_ADDTOSET_ID),
            "catalog must include rt-distinct-addtoset for shipped Rich Transform distinct/addToSet"
        );
        assert!(
            ids.iter().any(|id| id == CHANGE_ORDERING_ID),
            "catalog must include change-ordering for Change Ordering / confluence (ADR-0029)"
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
            gaps.iter().any(|g| g.contains("union")),
            "missing rt-union must be a visible gap; gaps={gaps:?}"
        );
        assert!(
            gaps.iter().any(|g| g.contains("unwind")),
            "missing rt-unwind must be a visible gap; gaps={gaps:?}"
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
            "id: demo\nsummary: demo\nnamespace:\n  source_tables: []\n  target_collections: [c]\n  deployment: d\nworkload:\n  concurrency: serial\n  steps: [prepare]\nchecks:\n  correctness:\n    - surface: base\n      table: T\n      present:\n        - { field: NAME, value: A }\n",
        )
        .expect("write");
        let err = load_recipe(&path).expect_err("empty source_tables must fail");
        let _ = fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("source_tables"),
            "err={err}"
        );
    }

    fn bulk_load_recipe_thresholds() -> ScenarioRecipeThresholds {
        // Values matching lab/scenarios/bulk-load/recipe.yaml (live interface).
        ScenarioRecipeThresholds {
            max_settle_ms: None,
            max_lag: Some(0),
            max_duration_ms: Some(600_000),
            min_rows_per_s: Some(50.0),
        }
    }

    #[test]
    fn mega_mix_recipe_covers_path_families_and_report_prints_gates() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../lab/scenarios/mega-mix/recipe.yaml");
        let recipe = load_recipe(&path).expect("load mega-mix recipe");
        assert_eq!(recipe.id, MEGA_MIX_ID);
        assert_eq!(recipe.namespace.deployment, MEGA_MIX_DEPLOYMENT);
        for pipe in mega_mix_pipelines() {
            assert!(
                recipe.namespace.pipelines.iter().any(|p| p == pipe.name),
                "recipe namespace.pipelines missing {}",
                pipe.name
            );
        }
        assert!(
            crate::lab_scenario::mega_mix::covers_required_path_families(mega_mix_pipelines())
        );
        let samples = mega_mix_pipelines()
            .iter()
            .map(|p| PipelineQpsSample {
                name: p.name.to_string(),
                path: p.path,
                qps_solo: 100.0,
                qps_mix: 96.0,
            })
            .collect::<Vec<_>>();
        let evidence = evaluate_mega_mix_gates(&samples, false);
        let mut report = report_from_adapter_outcome(
            &recipe,
            AdapterOutcome {
                correctness: true,
                detail: String::new(),
                metrics: ScenarioMetrics {
                    settle_ms: None,
                    lag: None,
                    rows_per_s: Some(evidence.sum_mix_qps),
                    duration_ms: Some(1_000),
                    rows_applied: INCREMENTAL_BATCH_ROWS,
                    capture_path_note: "LogMiner mega-mix Incremental".to_string(),
                },
            },
        );
        report.mega_mix = Some(evidence);
        let rendered = format_scenario_report(
            MEGA_MIX_ID,
            true,
            Duration::from_millis(1_000),
            &report,
            false,
        );
        assert!(rendered.contains("mega_mix:"), "{rendered}");
        assert!(rendered.contains("gate_0_7="), "{rendered}");
        assert!(rendered.contains("gate_0_95="), "{rendered}");
        assert!(rendered.contains("path_aggregate_direct_qps="), "{rendered}");
        assert!(
            rendered.contains("path_aggregate_transform_qps="),
            "{rendered}"
        );
        assert!(rendered.contains("component_pressure:"), "{rendered}");
        assert!(rendered.contains("protocol=solo_baseline_then_mix"), "{rendered}");
    }

    #[test]
    fn infra_saturated_report_is_not_product_fail() {
        let thresholds = bulk_load_recipe_thresholds();
        let metrics = ScenarioMetrics {
            settle_ms: None,
            lag: Some(0),
            rows_per_s: Some(800.0),
            duration_ms: Some(120_000),
            rows_applied: BULK_LOAD_ROW_COUNT,
            capture_path_note: "Initial Load".to_string(),
        };
        let recipe = ScenarioRecipe {
            id: BULK_LOAD_ID.to_string(),
            summary: "bulk".to_string(),
            namespace: ScenarioRecipeNamespace {
                source_tables: vec![BULK_LOAD_TABLE.to_string()],
                target_collections: vec![BULK_LOAD_COLLECTION.to_string()],
                deployment: BULK_LOAD_DEPLOYMENT.to_string(),
                pipelines: vec![],
                lifecycle: None,
            },
            deployment_config: "deployment.yaml".to_string(),
            workload: ScenarioRecipeWorkload {
                concurrency: "serial".to_string(),
                steps: vec!["prepare".to_string()],
                product_path: None,
            },
            checks: ScenarioRecipeChecks {
                correctness: vec![crate::lab_scenario::correctness::CorrectnessCheck {
                    surface: crate::lab_scenario::correctness::CorrectnessSurface::Base,
                    table: Some(BULK_LOAD_TABLE.to_string()),
                    row_count: Some(BULK_LOAD_ROW_COUNT),
                    ..Default::default()
                }],
            },
            thresholds,
        };
        let mut report = report_from_adapter_outcome(
            &recipe,
            AdapterOutcome {
                correctness: true,
                detail: String::new(),
                metrics,
            },
        );
        report.infra_saturated = true;
        report.component_pressure = COMPONENT_PRESSURE_NAMES
            .iter()
            .map(|name| ComponentPressure {
                component: (*name).to_string(),
                pressure: if *name == "platform_store" { 90 } else { 10 },
                saturated: *name == "platform_store",
            })
            .collect();
        let rendered = format_scenario_report(
            BULK_LOAD_ID,
            true,
            Duration::from_millis(120_000),
            &report,
            false,
        );
        assert!(
            rendered.contains("Lab Scenario: INFRA-SATURATED"),
            "{rendered}"
        );
        assert!(rendered.contains("infra_saturated=yes"), "{rendered}");
        assert!(rendered.contains("platform_store: pressure=90"), "{rendered}");
        assert!(rendered.contains("not a product failure"), "{rendered}");
        assert!(!rendered.contains("Lab Scenario: FAIL"), "{rendered}");
    }

    #[test]
    fn bulk_thresholds_fail_independently_of_correctness() {
        // Metrics miss the bar while row-level correctness would pass (US21 / US36).
        let thresholds = bulk_load_recipe_thresholds();
        let metrics = ScenarioMetrics {
            settle_ms: None,
            lag: Some(0),
            rows_per_s: Some(10.0),
            duration_ms: Some(900_000),
            rows_applied: BULK_LOAD_ROW_COUNT,
            capture_path_note: "Initial Load".to_string(),
        };
        let (ok, detail) = evaluate_recipe_thresholds(&thresholds, &metrics);
        assert!(!ok, "threshold sample must fail");
        assert!(detail.contains("threshold:"), "detail={detail}");
        assert!(detail.contains("duration_ms"), "detail={detail}");
        assert!(detail.contains("rows_per_s"), "detail={detail}");

        let recipe = ScenarioRecipe {
            id: BULK_LOAD_ID.to_string(),
            summary: "bulk".to_string(),
            namespace: ScenarioRecipeNamespace {
                source_tables: vec![BULK_LOAD_TABLE.to_string()],
                target_collections: vec![BULK_LOAD_COLLECTION.to_string()],
                deployment: BULK_LOAD_DEPLOYMENT.to_string(),
                pipelines: vec![],
                lifecycle: None,
            },
            deployment_config: "deployment.yaml".to_string(),
            workload: ScenarioRecipeWorkload {
                concurrency: "serial".to_string(),
                steps: vec!["prepare".to_string()],
                product_path: None,
            },
            checks: ScenarioRecipeChecks {
                correctness: vec![crate::lab_scenario::correctness::CorrectnessCheck {
                    surface: crate::lab_scenario::correctness::CorrectnessSurface::Base,
                    table: Some(BULK_LOAD_TABLE.to_string()),
                    row_count: Some(BULK_LOAD_ROW_COUNT),
                    ..Default::default()
                }],
            },
            thresholds,
        };
        let report = report_from_adapter_outcome(
            &recipe,
            AdapterOutcome {
                correctness: true,
                detail: String::new(),
                metrics,
            },
        );
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
        let thresholds = bulk_load_recipe_thresholds();
        let metrics = ScenarioMetrics {
            settle_ms: None,
            lag: Some(0),
            rows_per_s: Some(800.0),
            duration_ms: Some(120_000),
            rows_applied: 99_000,
            capture_path_note: "Initial Load".to_string(),
        };
        let (ok, detail) = evaluate_recipe_thresholds(&thresholds, &metrics);
        assert!(ok, "metrics should pass, detail={detail}");

        let recipe = ScenarioRecipe {
            id: BULK_LOAD_ID.to_string(),
            summary: "bulk".to_string(),
            namespace: ScenarioRecipeNamespace {
                source_tables: vec![BULK_LOAD_TABLE.to_string()],
                target_collections: vec![BULK_LOAD_COLLECTION.to_string()],
                deployment: BULK_LOAD_DEPLOYMENT.to_string(),
                pipelines: vec![],
                lifecycle: None,
            },
            deployment_config: "deployment.yaml".to_string(),
            workload: ScenarioRecipeWorkload {
                concurrency: "serial".to_string(),
                steps: vec!["prepare".to_string()],
                product_path: None,
            },
            checks: ScenarioRecipeChecks {
                correctness: vec![crate::lab_scenario::correctness::CorrectnessCheck {
                    surface: crate::lab_scenario::correctness::CorrectnessSurface::Base,
                    table: Some(BULK_LOAD_TABLE.to_string()),
                    row_count: Some(BULK_LOAD_ROW_COUNT),
                    ..Default::default()
                }],
            },
            thresholds,
        };
        let report = report_from_adapter_outcome(
            &recipe,
            AdapterOutcome {
                correctness: false,
                detail: format!(
                    "correctness: expected rows={BULK_LOAD_ROW_COUNT} base_rows=99000 target_rows=99000"
                ),
                metrics,
            },
        );
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
    fn recipe_thresholds_are_live_interface_for_bulk_load() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../lab/scenarios/bulk-load/recipe.yaml");
        let recipe = load_recipe(&path).expect("load bulk-load recipe");
        assert_eq!(recipe.thresholds.max_lag, Some(0));
        assert_eq!(recipe.thresholds.max_duration_ms, Some(600_000));
        assert_eq!(recipe.thresholds.min_rows_per_s, Some(50.0));

        let metrics = ScenarioMetrics {
            settle_ms: None,
            lag: Some(1),
            rows_per_s: Some(10.0),
            duration_ms: Some(900_000),
            rows_applied: BULK_LOAD_ROW_COUNT,
            capture_path_note: "Initial Load".to_string(),
        };
        let (ok, detail) = evaluate_recipe_thresholds(&recipe.thresholds, &metrics);
        assert!(!ok, "failing metrics must fail against recipe thresholds");
        assert!(
            detail.contains("max_lag=0"),
            "detail must use recipe max_lag; got {detail}"
        );
        assert!(
            detail.contains("max_duration_ms=600000"),
            "detail must use recipe max_duration_ms; got {detail}"
        );
        assert!(
            detail.contains("min_rows_per_s=50.00"),
            "detail must use recipe min_rows_per_s; got {detail}"
        );

        let report = report_from_adapter_outcome(
            &recipe,
            AdapterOutcome {
                correctness: true,
                detail: String::new(),
                metrics,
            },
        );
        assert!(!report.thresholds_ok);
        assert!(report.correctness);
        assert!(report.detail.contains("threshold:"));
    }

    #[test]
    fn recipe_thresholds_are_live_interface_for_concurrent_settle() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../lab/scenarios/concurrent-source-workload/recipe.yaml");
        let recipe = load_recipe(&path).expect("load concurrent-source-workload recipe");
        assert_eq!(recipe.thresholds.max_settle_ms, Some(300_000));

        let metrics = ScenarioMetrics {
            settle_ms: Some(300_001),
            lag: None,
            rows_per_s: None,
            duration_ms: None,
            rows_applied: 10,
            capture_path_note: "LogMiner".to_string(),
        };
        let (ok, detail) = evaluate_recipe_thresholds(&recipe.thresholds, &metrics);
        assert!(!ok, "settle over recipe limit must fail");
        assert!(
            detail.contains("max_settle_ms=300000"),
            "detail must use recipe max_settle_ms; got {detail}"
        );
    }

    #[test]
    fn recipe_driven_runner_surfaces_workload_and_checks() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../lab/scenarios/bulk-load/recipe.yaml");
        let recipe = load_recipe(&path).expect("load bulk-load recipe");
        assert!(!recipe.workload.steps.is_empty());
        assert!(!recipe.checks.correctness.is_empty());
        let summary = recipe_interface_summary(&recipe);
        assert!(
            summary.contains("workload.concurrency=serial"),
            "summary={summary}"
        );
        assert!(
            summary.contains(&format!("workload.steps={}", recipe.workload.steps.len())),
            "summary={summary}"
        );
        assert!(
            summary.contains("product_path.steps=3"),
            "bulk-load uses product_path prepare→apply→assert; summary={summary}"
        );
        assert!(
            summary.contains(&format!(
                "checks.correctness={}",
                recipe.checks.correctness.len()
            )),
            "summary={summary}"
        );
        assert!(
            summary.contains("max_lag")
                && summary.contains("max_duration_ms")
                && summary.contains("min_rows_per_s"),
            "summary must list threshold axes from recipe; got {summary}"
        );
        // pipelines is a live namespace field (no longer dead_code).
        assert!(
            !recipe.namespace.pipelines.is_empty(),
            "bulk-load recipe declares pipelines"
        );
    }


    #[test]
    fn recipe_correctness_surfaces_match_scenario_identities() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab/scenarios");
        let direct = load_recipe(&lab.join(DIRECT_PIPELINE_ID).join("recipe.yaml")).unwrap();
        assert!(direct.checks.correctness.iter().any(|c| {
            c.table.as_deref() == Some(DIRECT_PIPELINE_TABLE)
        }));
        assert!(direct.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(DIRECT_PIPELINE_COLLECTION)
        }));
        let project = load_recipe(&lab.join(RT_PROJECT_ID).join("recipe.yaml")).unwrap();
        assert!(project
            .checks
            .correctness
            .iter()
            .any(|c| c.pipeline.as_deref() == Some(RT_PROJECT_PIPELINE)));
        assert!(project
            .checks
            .correctness
            .iter()
            .any(|c| c.collection.as_deref() == Some(RT_PROJECT_COLLECTION)));
        let poison = load_recipe(&lab.join(POISON_QUARANTINE_ID).join("recipe.yaml")).unwrap();
        assert!(poison.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(POISON_QUARANTINE_COLLECTION)
        }));
        let equi = load_recipe(&lab.join(RT_EQUILOOKUP_ID).join("recipe.yaml")).unwrap();
        assert!(equi.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(RT_EQUILOOKUP_COLLECTION)
        }));
        let union = load_recipe(&lab.join(RT_UNION_ID).join("recipe.yaml")).unwrap();
        assert!(union.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(RT_UNION_COLLECTION)
        }));
        let unwind = load_recipe(&lab.join(RT_UNWIND_ID).join("recipe.yaml")).unwrap();
        assert!(unwind.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(RT_UNWIND_COLLECTION)
        }));
        let distinct = load_recipe(&lab.join(RT_DISTINCT_ADDTOSET_ID).join("recipe.yaml")).unwrap();
        assert!(distinct.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(RT_DISTINCT_ADDTOSET_DISTINCT_COLLECTION)
        }));
        assert!(distinct.checks.correctness.iter().any(|c| {
            c.collection.as_deref() == Some(RT_DISTINCT_ADDTOSET_ADD_COLLECTION)
        }));
        for (id, collection) in [
            (RT_FILTER_ID, RT_FILTER_COLLECTION),
            (RT_FIELD_OPS_ID, RT_FIELD_OPS_COLLECTION),
            (TRANSFORM_PIPELINE_ID, TRANSFORM_CUSTOMERS_COLLECTION),
            (TRANSFORM_PIPELINE_ID, TRANSFORM_ORDER_TOTALS_COLLECTION),
            (CONCURRENT_SOURCE_WORKLOAD_ID, CONCURRENT_CUSTOMERS_COLLECTION),
            (CONCURRENT_SOURCE_WORKLOAD_ID, CONCURRENT_ORDER_TOTALS_COLLECTION),
            (CHANGE_ORDERING_ID, CHANGE_ORDERING_CUSTOMERS_COLLECTION),
            (CHANGE_ORDERING_ID, CHANGE_ORDERING_ORDER_STATS_COLLECTION),
        ] {
            let recipe = load_recipe(&lab.join(id).join("recipe.yaml")).unwrap();
            assert!(
                recipe
                    .checks
                    .correctness
                    .iter()
                    .any(|c| c.collection.as_deref() == Some(collection)),
                "{id} must declare collection {collection} in checks.correctness"
            );
        }
    }

    #[test]
    fn product_path_recipes_declare_runnable_correctness() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab/scenarios");
        for id in registered_scenario_ids() {
            let path = lab.join(id).join("recipe.yaml");
            let recipe = load_recipe(&path).unwrap_or_else(|err| panic!("load {id}: {err}"));
            assert!(
                recipe.workload.product_path.is_some(),
                "{id} must declare product_path"
            );
            assert!(
                !recipe.checks.correctness.is_empty(),
                "{id} must declare runnable checks.correctness (#205)"
            );
            assert!(
                recipe.namespace.lifecycle.is_some(),
                "{id} must declare namespace.lifecycle"
            );
        }
    }

    #[test]
    fn product_path_recipes_drive_shared_steps_for_migrated_batch() {
        let lab = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lab/scenarios");
        let five_step = [
            ProductPathStepKind::PrepareNamespace,
            ProductPathStepKind::ProductApply,
            ProductPathStepKind::Mutate,
            ProductPathStepKind::ProductSync,
            ProductPathStepKind::Assert,
        ];
        let four_step_no_sync = [
            ProductPathStepKind::PrepareNamespace,
            ProductPathStepKind::ProductApply,
            ProductPathStepKind::Mutate,
            ProductPathStepKind::Assert,
        ];
        let three_step = [
            ProductPathStepKind::PrepareNamespace,
            ProductPathStepKind::ProductApply,
            ProductPathStepKind::Assert,
        ];

        for (id, plan, require_delivery, require_derived, allow_fail) in [
            (DIRECT_PIPELINE_ID, five_step.as_slice(), false, false, false),
            (RT_PROJECT_ID, five_step.as_slice(), false, true, false),
            (RT_FILTER_ID, five_step.as_slice(), false, true, false),
            (RT_FIELD_OPS_ID, five_step.as_slice(), false, true, false),
            (RT_EQUILOOKUP_ID, five_step.as_slice(), false, true, false),
            (RT_UNION_ID, five_step.as_slice(), false, true, false),
            (RT_UNWIND_ID, five_step.as_slice(), false, true, false),
            (RT_DISTINCT_ADDTOSET_ID, five_step.as_slice(), false, true, false),
            (POISON_QUARANTINE_ID, five_step.as_slice(), true, false, false),
            (TRANSFORM_PIPELINE_ID, five_step.as_slice(), false, true, false),
            (CHANGE_ORDERING_ID, five_step.as_slice(), false, true, false),
            (IDEMPOTENT_REDELIVERY_ID, five_step.as_slice(), true, false, false),
            (PAUSE_RESUME_ID, five_step.as_slice(), true, false, false),
            (REMOVE_PIPELINE_ID, five_step.as_slice(), true, false, false),
            (CHANGE_PIPELINE_ID, five_step.as_slice(), true, true, false),
            (SCHEMA_CHANGE_PAUSE_ID, five_step.as_slice(), true, false, false),
            (BOUNDED_BACKPRESSURE_ID, five_step.as_slice(), true, false, true),
            (OBSERVABILITY_SURFACE_ID, five_step.as_slice(), true, false, true),
            (
                CONCURRENT_SOURCE_WORKLOAD_ID,
                four_step_no_sync.as_slice(),
                false,
                true,
                false,
            ),
            (SOURCE_ALIGNMENT_ID, four_step_no_sync.as_slice(), false, false, false),
            (DRIFT_CHECK_ID, four_step_no_sync.as_slice(), false, false, false),
            (BULK_LOAD_ID, three_step.as_slice(), false, false, false),
            (
                PLATFORM_STORE_GUARDRAILS_ID,
                three_step.as_slice(),
                false,
                false,
                false,
            ),
            (
                BACKWARD_COMPATIBLE_UPGRADES_ID,
                three_step.as_slice(),
                false,
                false,
                false,
            ),
            (
                INITIAL_LOAD_THROTTLED_ID,
                three_step.as_slice(),
                false,
                false,
                false,
            ),
            (MEGA_MIX_ID, three_step.as_slice(), false, true, false),
        ] {
            let recipe = load_recipe(&lab.join(id).join("recipe.yaml"))
                .unwrap_or_else(|err| panic!("load {id}: {err}"));
            let actual = product_path_plan(&recipe)
                .unwrap_or_else(|| panic!("{id} must declare workload.product_path"));
            assert_eq!(actual, plan, "{id} product_path.steps");
            let pp = recipe.workload.product_path.as_ref().expect("product_path");
            if id != INITIAL_LOAD_THROTTLED_ID {
                assert!(pp.apply.require_initial_load, "{id}");
            }
            assert_eq!(pp.apply.require_delivery, require_delivery, "{id}");
            assert_eq!(pp.apply.require_derived, require_derived, "{id}");
            assert!(pp.sync.require_logminer, "{id}");
            assert_eq!(pp.sync.allow_fail, allow_fail, "{id}");
            let summary = recipe_interface_summary(&recipe);
            assert!(
                summary.contains(&format!("product_path.steps={}", plan.len())),
                "{id} summary={summary}"
            );
            ProductPathHooks::for_recipe(&recipe).unwrap_or_else(|err| {
                panic!("{id} must have product-path hooks: {err}")
            });
            let lifecycle = recipe.namespace.lifecycle.as_ref().unwrap_or_else(|| {
                panic!("{id} must declare namespace.lifecycle for shared Namespace prepare (#201)")
            });
            let lifecycle_names: Vec<&str> =
                lifecycle.tables.iter().map(|t| t.name.as_str()).collect();
            let source_names: Vec<&str> = recipe
                .namespace
                .source_tables
                .iter()
                .map(String::as_str)
                .collect();
            assert_eq!(
                lifecycle_names, source_names,
                "{id} lifecycle.tables must match namespace.source_tables"
            );
            assert!(
                !lifecycle.seed_sql.trim().is_empty(),
                "{id} lifecycle.seed_sql must be non-empty"
            );
        }
    }

    #[test]
    fn product_path_validation_rejects_missing_namespace_lifecycle() {
        let dir = std::env::temp_dir().join(format!(
            "migraloop-recipe-lifecycle-{}-{}",
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
            "id: demo\nsummary: demo\nnamespace:\n  source_tables: [LAB_X]\n  target_collections: [lab_x]\n  deployment: lab-x\nworkload:\n  concurrency: serial\n  steps: [prepare, apply, assert]\n  product_path:\n    steps: [prepare_namespace, product_apply, assert]\n    sync:\n      require_logminer: true\nchecks:\n  correctness:\n    - surface: base\n      table: T\n      present:\n        - { field: NAME, value: A }\n",
        )
        .expect("write");
        let err = load_recipe(&path).expect_err("lifecycle required for prepare_namespace");
        let _ = fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("namespace.lifecycle"),
            "err={err}"
        );
    }

    #[test]
    fn product_path_step_dispatch_follows_recipe_order() {
        struct Recorder(Vec<ProductPathStepKind>);
        impl ProductPathStepSink for Recorder {
            fn on_step(&mut self, step: ProductPathStepKind) -> Result<(), String> {
                self.0.push(step);
                Ok(())
            }
        }
        let steps = [
            ProductPathStepKind::PrepareNamespace,
            ProductPathStepKind::ProductApply,
            ProductPathStepKind::Mutate,
            ProductPathStepKind::ProductSync,
            ProductPathStepKind::Assert,
        ];
        let mut recorder = Recorder(Vec::new());
        dispatch_product_path_steps(&steps, &mut recorder).expect("dispatch");
        assert_eq!(recorder.0, steps);
    }

    #[test]
    fn product_path_validation_rejects_incomplete_plans() {
        let incomplete = ScenarioRecipeProductPath {
            steps: vec![ProductPathStepKind::PrepareNamespace],
            apply: Default::default(),
            sync: Default::default(),
        };
        let err = validate_product_path("demo.yaml", &incomplete).expect_err("need assert");
        assert!(
            err.to_string().contains("prepare_namespace")
                && err.to_string().contains("assert"),
            "err={err}"
        );

        let no_product = ScenarioRecipeProductPath {
            steps: vec![
                ProductPathStepKind::PrepareNamespace,
                ProductPathStepKind::Mutate,
                ProductPathStepKind::Assert,
            ],
            apply: Default::default(),
            sync: Default::default(),
        };
        let err = validate_product_path("demo.yaml", &no_product).expect_err("need apply/sync");
        assert!(
            err.to_string().contains("product_apply")
                || err.to_string().contains("product_sync"),
            "err={err}"
        );

        let mock_sync = ScenarioRecipeProductPath {
            steps: vec![
                ProductPathStepKind::PrepareNamespace,
                ProductPathStepKind::ProductSync,
                ProductPathStepKind::Assert,
            ],
            apply: Default::default(),
            sync: ProductPathSyncOpts {
                require_logminer: false,
                allow_fail: false,
            },
        };
        let err = validate_product_path("demo.yaml", &mock_sync).expect_err("logminer required");
        assert!(
            err.to_string().contains("require_logminer"),
            "err={err}"
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
