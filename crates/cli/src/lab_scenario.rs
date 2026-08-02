//! Lab Scenario catalog and run orchestration (issue #60 / ADR-0025).
//!
//! Lab-specific machinery: catalog listing, Scenario Namespace lifecycle prep,
//! Source workload driving, one-at-a-time lock, and result reporting.
//! Apply / Sync / inspect use the real product CLI path.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Subcommand;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::lab::{
    ensure_fixture_ready_for_scenario, lab_migraloop_bin, sqlplus_in_oracle, LAB_MONGO_PASSWORD_DEFAULT,
    LAB_MONGO_PASSWORD_ENV, LAB_ORACLE_PASSWORD_DEFAULT, LAB_ORACLE_PASSWORD_ENV,
    LAB_ORACLE_USER, LAB_PLATFORM_STORE_URL,
};
use crate::CliError;

const LOCK_FILE_NAME: &str = ".migraloop-scenario.lock";

const DIRECT_PIPELINE_ID: &str = "direct-pipeline";
const DIRECT_PIPELINE_SUMMARY: &str =
    "Direct Pipeline Initial Load + insert/update/delete (real apply/sync)";
const DIRECT_PIPELINE_TABLE: &str = "LAB_DP_CUSTOMERS";
const DIRECT_PIPELINE_COLLECTION: &str = "lab_dp_customers";
const DIRECT_PIPELINE_DEPLOYMENT: &str = "lab-direct-pipeline";

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
        /// Scenario id from `lab scenario list` (for example `direct-pipeline`)
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
        ScenarioCommand::Run { scenario, lab_dir } => scenario_run(&scenario, &lab_dir).await,
    }
}

fn catalog() -> &'static [(&'static str, &'static str)] {
    &[(DIRECT_PIPELINE_ID, DIRECT_PIPELINE_SUMMARY)]
}

fn scenario_list() {
    println!("Lab Scenarios:");
    for (id, summary) in catalog() {
        println!("  {id}  {summary}");
    }
}

async fn scenario_run(scenario: &str, lab_dir: &Path) -> Result<(), CliError> {
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
    let result = if scenario == DIRECT_PIPELINE_ID {
        run_direct_pipeline(lab_dir).await
    } else {
        Err(CliError::Failed(format!(
            "Lab Scenario `{scenario}` is listed but has no runner"
        )))
    };
    let duration = started.elapsed();
    drop(lock);

    match result {
        Ok(report) => {
            print_scenario_report(entry.0, true, duration, &report);
            Ok(())
        }
        Err(err) => {
            let report = ScenarioReport {
                correctness: false,
                rows_applied: 0,
                detail: err.to_string(),
                capture_path_note: String::new(),
            };
            print_scenario_report(entry.0, false, duration, &report);
            Err(err)
        }
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
    println!(
        "  namespace=left in place (inspect with `migraloop base` / `migraloop target`)"
    );
}

async fn run_direct_pipeline(lab_dir: &Path) -> Result<ScenarioReport, CliError> {
    println!("Lab Scenario: {DIRECT_PIPELINE_ID}");
    println!("Scenario Namespace: table={DIRECT_PIPELINE_TABLE} \
collection={DIRECT_PIPELINE_COLLECTION} deployment={DIRECT_PIPELINE_DEPLOYMENT}");

    prepare_direct_pipeline_namespace(lab_dir).await?;
    println!("Lab Scenario: Scenario Namespace prepared (schema + seed + supplemental logging)");

    let config_path = deployment_config_path(lab_dir)?;
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
    let patterns = [
        format!("\"NAME\": \"{name}\""),
        format!("\"NAME\":\"{name}\""),
        format!("\"name\": \"{name}\""),
        format!("\"name\":\"{name}\""),
    ];
    patterns.iter().any(|p| inspect.contains(p.as_str()))
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

fn deployment_config_path(lab_dir: &Path) -> Result<PathBuf, CliError> {
    let path = lab_dir
        .join("scenarios")
        .join("direct-pipeline")
        .join("deployment.yaml");
    if !path.is_file() {
        return Err(CliError::Failed(format!(
            "Lab Scenario deployment config not found at {} \
             (expected under the repo `lab/scenarios/direct-pipeline/` directory)",
            path.display()
        )));
    }
    Ok(path)
}

async fn prepare_direct_pipeline_namespace(lab_dir: &Path) -> Result<(), CliError> {
    // Self-contained Namespace prep. Re-run wipe-before-recreate is issue #61;
    // if leftovers exist, fail with a clear operator message.
    let sql = format!(
        "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
DECLARE\n\
  c NUMBER;\n\
BEGIN\n\
  SELECT COUNT(*) INTO c FROM user_tables WHERE table_name = '{DIRECT_PIPELINE_TABLE}';\n\
  IF c > 0 THEN\n\
    RAISE_APPLICATION_ERROR(-20001, 'Scenario Namespace table {DIRECT_PIPELINE_TABLE} already exists');\n\
  END IF;\n\
END;\n\
/\n\
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
    match sqlplus_in_oracle(lab_dir, &connect, &sql).await {
        Ok(_) => Ok(()),
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("ORA-20001") || msg.contains("already exists") {
                Err(CliError::Failed(format!(
                    "Lab Scenario Namespace for `{DIRECT_PIPELINE_ID}` already present \
                     (table {DIRECT_PIPELINE_TABLE}). Finished runs leave the Namespace for \
                     live inspection by default. Re-run wipe / manual Namespace remove are \
                     tracked separately (issue #61). Until then: `migraloop lab down` or drop \
                     the Oracle table manually before another run.\n{msg}"
                )))
            } else {
                Err(CliError::Failed(format!(
                    "Failed to prepare Direct Pipeline Scenario Namespace:\n{msg}"
                )))
            }
        }
    }
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
                "Lab Scenario run rejected: another Scenario is active \
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
                            "Lab Scenario run rejected: another Scenario is active \
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
