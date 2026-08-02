//! Lab Scenario catalog, run orchestration, and Namespace cleanup
//! (issues #60–#62 / ADR-0025).
//!
//! Lab-specific machinery: catalog listing, Scenario Namespace lifecycle
//! (prepare / re-run wipe / manual remove / opt-in auto-remove), Source workload
//! driving, one-at-a-time lock, and result reporting.
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
const DIRECT_PIPELINE_SUMMARY: &str =
    "Direct Pipeline Initial Load + insert/update/delete (real apply/sync)";
const DIRECT_PIPELINE_TABLE: &str = "LAB_DP_CUSTOMERS";
const DIRECT_PIPELINE_COLLECTION: &str = "lab_dp_customers";
const DIRECT_PIPELINE_DEPLOYMENT: &str = "lab-direct-pipeline";

const TRANSFORM_PIPELINE_ID: &str = "transform-pipeline";
const TRANSFORM_PIPELINE_SUMMARY: &str =
    "Multi-table Transform Pipeline: customers + orders groupBy/sum → Derived → Delivery (real apply/sync)";
const TRANSFORM_CUSTOMERS_TABLE: &str = "LAB_TP_CUSTOMERS";
const TRANSFORM_ORDERS_TABLE: &str = "LAB_TP_ORDERS";
const TRANSFORM_CUSTOMERS_COLLECTION: &str = "lab_tp_customers";
const TRANSFORM_ORDER_TOTALS_COLLECTION: &str = "lab_tp_order_totals";
const TRANSFORM_ORDER_TOTALS_PIPELINE: &str = "lab-tp-order-totals";
const TRANSFORM_PIPELINE_DEPLOYMENT: &str = "lab-transform-pipeline";

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
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `transform-pipeline`)
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
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`, `transform-pipeline`)
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
            scenario_list();
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

fn catalog() -> &'static [(&'static str, &'static str)] {
    &[
        (DIRECT_PIPELINE_ID, DIRECT_PIPELINE_SUMMARY),
        (TRANSFORM_PIPELINE_ID, TRANSFORM_PIPELINE_SUMMARY),
    ]
}

fn scenario_list() {
    println!("Lab Scenarios:");
    for (id, summary) in catalog() {
        println!("  {id}  {summary}");
    }
}

async fn scenario_run(
    scenario: &str,
    lab_dir: &Path,
    auto_remove: bool,
) -> Result<(), CliError> {
    let entry = catalog()
        .iter()
        .find(|(id, _)| *id == scenario)
        .ok_or_else(|| {
            CliError::Failed(format!(
                "Unknown Lab Scenario `{scenario}`. Run `migraloop lab scenario list`."
            ))
        })?;

    // One-at-a-time check before Fixture probes so CI can assert rejection without Docker.
    let lock_path = lab_dir.join(LOCK_FILE_NAME);
    if let Some(existing) = read_active_lock(&lock_path)? {
        return Err(CliError::Failed(format!(
            "Lab Scenario run rejected: another Scenario is active \
             (`{}` since unix {})",
            existing.scenario, existing.started_at_unix
        )));
    }

    ensure_fixture_ready_for_scenario(lab_dir).await?;

    let lock = ScenarioLock::acquire(&lock_path, scenario)?;
    let started = Instant::now();
    // Catalog membership already validated; dispatch by id.
    let result = match scenario {
        DIRECT_PIPELINE_ID => run_direct_pipeline(lab_dir).await,
        TRANSFORM_PIPELINE_ID => run_transform_pipeline(lab_dir).await,
        _ => Err(CliError::Failed(format!(
            "Lab Scenario `{scenario}` is listed but has no runner"
        ))),
    };
    let duration = started.elapsed();

    match result {
        Ok(report) => {
            let mut namespace_removed = false;
            if auto_remove {
                // Opt-in cleanup after success only — failures keep Namespace for debug (US35).
                remove_scenario_namespace(scenario, lab_dir).await?;
                namespace_removed = true;
            }
            drop(lock);
            print_scenario_report(entry.0, true, duration, &report, namespace_removed);
            Ok(())
        }
        Err(err) => {
            drop(lock);
            let report = ScenarioReport {
                correctness: false,
                rows_applied: 0,
                detail: err.to_string(),
                capture_path_note: String::new(),
            };
            print_scenario_report(entry.0, false, duration, &report, false);
            Err(err)
        }
    }
}

async fn scenario_remove(scenario: &str, lab_dir: &Path) -> Result<(), CliError> {
    catalog()
        .iter()
        .find(|(id, _)| *id == scenario)
        .ok_or_else(|| {
            CliError::Failed(format!(
                "Unknown Lab Scenario `{scenario}`. Run `migraloop lab scenario list`."
            ))
        })?;

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
}

fn print_scenario_report(
    scenario: &str,
    overall_pass: bool,
    duration: Duration,
    report: &ScenarioReport,
    namespace_removed: bool,
) {
    let duration_ms = duration.as_millis();
    let rows_per_s = if duration.as_secs_f64() > 0.0 {
        report.rows_applied as f64 / duration.as_secs_f64()
    } else {
        report.rows_applied as f64
    };
    let outcome = if overall_pass && report.correctness {
        "PASS"
    } else {
        "FAIL"
    };
    println!();
    println!("Lab Scenario: {outcome}");
    println!("  scenario={scenario}");
    println!("  correctness={}", if report.correctness { "pass" } else { "fail" });
    println!("  duration_ms={duration_ms}");
    println!("  rows_applied={}", report.rows_applied);
    println!("  rows_per_s={rows_per_s:.2}");
    if !report.capture_path_note.is_empty() {
        println!("  capture={}", report.capture_path_note);
    }
    if !report.detail.is_empty() && outcome == "FAIL" {
        println!("  detail={}", report.detail);
    }
    if namespace_removed {
        println!("  namespace=removed (--auto-remove)");
    } else {
        println!(
            "  namespace=left in place (inspect with `migraloop base` / `migraloop derived` / `migraloop target`)"
        );
    }
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
