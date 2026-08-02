//! Local Sync Lab Fixture orchestration (issue #59 / ADR-0025).
//!
//! Lab-specific machinery only: disposable stack bring-up / status / tear-down.
//! Does not apply Deployments or Pipelines.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use clap::Subcommand;
use tokio::process::Command;
use tokio::time::sleep;

use crate::CliError;

pub const LAB_COMPOSE_PROJECT: &str = "migraloop-lab";

/// Documented Lab disposable credentials (local-dev friendly).
pub const LAB_PLATFORM_STORE_URL: &str =
    "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";
pub const LAB_ORACLE_HOST: &str = "127.0.0.1";
pub const LAB_ORACLE_PORT: u16 = 1521;
pub const LAB_ORACLE_SERVICE: &str = "FREEPDB1";
pub const LAB_ORACLE_USER: &str = "SYNC_USER";
pub const LAB_ORACLE_PASSWORD_ENV: &str = "ORACLE_PASSWORD";
pub const LAB_ORACLE_PASSWORD_DEFAULT: &str = "lab_oracle";
pub const LAB_MONGO_HOST: &str = "127.0.0.1";
pub const LAB_MONGO_PORT: u16 = 27017;
pub const LAB_MONGO_DATABASE: &str = "lab";
pub const LAB_MONGO_USER: &str = "migraloop";
pub const LAB_MONGO_PASSWORD_ENV: &str = "MONGO_PASSWORD";
pub const LAB_MONGO_PASSWORD_DEFAULT: &str = "lab_mongo";

#[derive(Debug, Subcommand)]
pub enum LabCommand {
    /// Bring up the disposable Lab Fixture (Oracle, MongoDB, Platform Store, app)
    Up {
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
    },
    /// Report Lab Fixture readiness and connection details (no default Pipeline implied)
    Status {
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
    },
    /// Tear down the disposable Lab Fixture (containers + volumes)
    Down {
        /// Directory containing Lab `compose.yaml` (default: ./lab)
        #[arg(long, default_value = "lab")]
        lab_dir: PathBuf,
    },
}

pub async fn run_lab(command: LabCommand) -> Result<(), CliError> {
    match command {
        LabCommand::Up { lab_dir } => lab_up(&lab_dir).await,
        LabCommand::Status { lab_dir } => lab_status(&lab_dir).await,
        LabCommand::Down { lab_dir } => lab_down(&lab_dir).await,
    }
}

fn compose_file(lab_dir: &Path) -> Result<PathBuf, CliError> {
    let path = lab_dir.join("compose.yaml");
    if !path.is_file() {
        return Err(CliError::Failed(format!(
            "Lab compose file not found at {} \
             (pass --lab-dir pointing at the repo `lab/` directory, or run from the repo root)",
            path.display()
        )));
    }
    Ok(path)
}

fn compose_base(lab_dir: &Path) -> Result<Command, CliError> {
    let file = compose_file(lab_dir)?;
    // Prefer `docker compose` (v2 plugin); fall back to `docker-compose`.
    let mut cmd = if docker_compose_v2_available() {
        let mut c = Command::new("docker");
        c.arg("compose");
        c
    } else if command_exists("docker-compose") {
        Command::new("docker-compose")
    } else {
        return Err(CliError::Failed(
            "Docker Compose is required for Local Sync Lab \
             (install Docker with the compose plugin, or docker-compose)"
                .to_string(),
        ));
    };
    cmd.arg("-f")
        .arg(&file)
        .arg("-p")
        .arg(LAB_COMPOSE_PROJECT)
        .current_dir(lab_dir);
    Ok(cmd)
}

fn docker_compose_v2_available() -> bool {
    std::process::Command::new("docker")
        .args(["compose", "version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn command_exists(name: &str) -> bool {
    std::process::Command::new("sh")
        .args(["-c", &format!("command -v {name} >/dev/null 2>&1")])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn run_compose(lab_dir: &Path, args: &[&str]) -> Result<(bool, String), CliError> {
    let mut cmd = compose_base(lab_dir)?;
    for arg in args {
        cmd.arg(arg);
    }
    let output = cmd
        .output()
        .await
        .map_err(|err| CliError::Failed(format!("failed to run docker compose: {err}")))?;
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok((output.status.success(), combined))
}

async fn lab_up(lab_dir: &Path) -> Result<(), CliError> {
    println!("Lab Fixture: bringing up disposable stack (Oracle, MongoDB, Platform Store, app)...");
    println!("Lab Fixture: no sample Deployment or Pipelines will be applied.");

    let (ok, out) = run_compose(lab_dir, &["up", "-d", "--build"]).await?;
    if !ok {
        return Err(CliError::Failed(format!(
            "Lab bring-up failed (image pull, port conflict, or compose error):\n{out}"
        )));
    }

    wait_for_services_healthy(lab_dir).await?;
    probe_oracle_prerequisites(lab_dir).await?;

    // Ensure Platform Store migrations have run via the app container (or host migrate).
    ensure_platform_store_migrated(lab_dir).await?;

    println!("Lab Fixture: ready");
    print_connection_details();
    print_empty_deployment_notice();
    Ok(())
}

async fn lab_status(lab_dir: &Path) -> Result<(), CliError> {
    let _ = compose_file(lab_dir)?;

    if !docker_available() {
        println!("Lab Fixture: not ready");
        println!("  Docker / Compose: unavailable");
        return Err(CliError::Failed(
            "Lab Fixture is not ready (Docker Compose unavailable)".to_string(),
        ));
    }

    let services = service_readiness(lab_dir).await?;
    let all_up = services.iter().all(|(_, ready)| *ready);

    if !all_up {
        println!("Lab Fixture: not ready");
        for (name, ready) in &services {
            println!(
                "  {name}: {}",
                if *ready { "ready" } else { "not ready" }
            );
        }
        return Err(CliError::Failed(
            "Lab Fixture is not ready".to_string(),
        ));
    }

    let oracle_ok = match probe_oracle_prerequisites(lab_dir).await {
        Ok(()) => true,
        Err(err) => {
            println!("Lab Fixture: not ready");
            println!("  Oracle Source Prerequisites: {err}");
            return Err(CliError::Failed(
                "Lab Fixture is not ready (Oracle Source Prerequisites)".to_string(),
            ));
        }
    };

    let store_ok = platform_store_reports_healthy().await;
    if !store_ok {
        println!("Lab Fixture: not ready");
        println!("  Platform Store: not healthy (run `migraloop lab up` or wait for app migrate)");
        return Err(CliError::Failed(
            "Lab Fixture is not ready (Platform Store)".to_string(),
        ));
    }

    println!("Lab Fixture: ready");
    for (name, _) in &services {
        let detail = if name == "oracle" && oracle_ok {
            "ready (ARCHIVELOG + database supplemental logging)"
        } else {
            "ready"
        };
        println!("  {name}: {detail}");
    }
    println!("  Platform Store: healthy");
    print_empty_deployment_notice();
    print_connection_details();
    Ok(())
}

async fn lab_down(lab_dir: &Path) -> Result<(), CliError> {
    let _ = compose_file(lab_dir)?;
    if !docker_available() {
        // Treat as already down when Docker is absent.
        println!("Lab Fixture: down (Docker Compose unavailable; nothing to tear down)");
        return Ok(());
    }

    let (ok, out) = run_compose(lab_dir, &["down", "-v", "--remove-orphans"]).await?;
    if !ok {
        // If the project was never created, compose may still exit non-zero; treat missing
        // project as success for idempotent destroy.
        if out.contains("No such file") || out.to_ascii_lowercase().contains("not found") {
            println!("Lab Fixture: down");
            return Ok(());
        }
        return Err(CliError::Failed(format!(
            "Lab tear-down failed:\n{out}"
        )));
    }
    println!("Lab Fixture: down");
    Ok(())
}

fn docker_available() -> bool {
    docker_compose_v2_available() || command_exists("docker-compose")
}

async fn wait_for_services_healthy(lab_dir: &Path) -> Result<(), CliError> {
    // Oracle Free first boot can take several minutes.
    let attempts = 90u32;
    for attempt in 1..=attempts {
        let services = service_readiness(lab_dir).await?;
        if services.iter().all(|(_, ready)| *ready) {
            return Ok(());
        }
        if attempt == attempts {
            let detail = services
                .iter()
                .map(|(n, r)| format!("{n}={}", if *r { "ready" } else { "not-ready" }))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CliError::Failed(format!(
                "Lab bring-up timed out waiting for healthy services ({detail})"
            )));
        }
        sleep(Duration::from_secs(4)).await;
    }
    Ok(())
}

async fn service_readiness(lab_dir: &Path) -> Result<Vec<(String, bool)>, CliError> {
    let wanted = ["platform-store", "oracle", "mongo", "app"];
    let (ok, out) = run_compose(lab_dir, &["ps", "--format", "json"]).await?;
    if !ok {
        // When the project does not exist yet, treat all as not ready.
        return Ok(wanted
            .iter()
            .map(|n| ((*n).to_string(), false))
            .collect());
    }

    // `docker compose ps --format json` may emit one JSON object per line or an array.
    let mut running: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let trimmed = out.trim();
    if trimmed.starts_with('[') {
        if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(trimmed) {
            for item in arr {
                record_service_if_running(&item, &mut running);
            }
        }
    } else {
        for line in trimmed.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(item) = serde_json::from_str::<serde_json::Value>(line) {
                record_service_if_running(&item, &mut running);
            }
        }
    }

    // Fallback: plain `compose ps` text when JSON format is unsupported.
    if running.is_empty() {
        let (ok2, text) = run_compose(lab_dir, &["ps"]).await?;
        if ok2 {
            for name in wanted {
                if text.contains(name) && (text.contains("running") || text.contains("Up")) {
                    // Heuristic only; prefer JSON path above.
                    let lower = text.to_ascii_lowercase();
                    if lower.contains(&format!("{name}")) {
                        // Mark based on presence of "healthy" near service when possible.
                        running.insert(name.to_string());
                    }
                }
            }
            // Refine: only mark services whose line mentions running/healthy.
            running.clear();
            for line in text.lines() {
                let lower = line.to_ascii_lowercase();
                for name in wanted {
                    if lower.contains(name)
                        && (lower.contains("running")
                            || lower.contains("up ")
                            || lower.contains("healthy"))
                    {
                        running.insert(name.to_string());
                    }
                }
            }
        }
    }

    Ok(wanted
        .iter()
        .map(|n| ((*n).to_string(), running.contains(*n)))
        .collect())
}

fn record_service_if_running(item: &serde_json::Value, running: &mut std::collections::BTreeSet<String>) {
    let service = item
        .get("Service")
        .or_else(|| item.get("service"))
        .or_else(|| item.get("Name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let short = service
        .rsplit('_')
        .next()
        .unwrap_or(service)
        .trim()
        .to_string();
    // Compose project prefixes: migraloop-lab-oracle-1 → prefer Service field.
    let name = item
        .get("Service")
        .or_else(|| item.get("service"))
        .and_then(|v| v.as_str())
        .unwrap_or(short.as_str())
        .to_string();

    let state = item
        .get("State")
        .or_else(|| item.get("state"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let health = item
        .get("Health")
        .or_else(|| item.get("health"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let running_state = state == "running" || state.contains("up");
    let healthy_ok = health.is_empty() || health == "healthy" || health == "running";
    if running_state && healthy_ok {
        running.insert(name);
    }
}

async fn probe_oracle_prerequisites(lab_dir: &Path) -> Result<(), CliError> {
    // Probe inside the Oracle container so host Instant Client is not required for Lab status.
    // LOG_MODE is instance-wide (CDB); supplemental logging is checked in FREEPDB1.
    let sql = "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n\
SELECT 'LOG_MODE=' || LOG_MODE FROM V$DATABASE;\n\
ALTER SESSION SET CONTAINER=FREEPDB1;\n\
SELECT 'SUPP=' || NVL(SUPPLEMENTAL_LOG_DATA_MIN, 'NO') FROM V$DATABASE;\n\
EXIT;\n";

    let mut cmd = compose_base(lab_dir)?;
    cmd.args([
        "exec",
        "-T",
        "-e",
        "ORACLE_PWD=lab_oracle_sys",
        "oracle",
        "sqlplus",
        "-s",
        "/ as sysdba",
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|err| CliError::Failed(format!("failed to exec sqlplus in Lab Oracle: {err}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin
            .write_all(sql.as_bytes())
            .await
            .map_err(|err| CliError::Failed(format!("failed to write sqlplus stdin: {err}")))?;
    }

    let output = child
        .wait_with_output()
        .await
        .map_err(|err| CliError::Failed(format!("sqlplus probe failed: {err}")))?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(CliError::Failed(format!(
            "Oracle prerequisite probe failed:\n{text}"
        )));
    }

    let upper = text.to_ascii_uppercase();
    if !upper.contains("LOG_MODE=ARCHIVELOG") {
        return Err(CliError::Failed(format!(
            "Lab Oracle is not in ARCHIVELOG mode (required for LogMiner):\n{text}"
        )));
    }
    if !(upper.contains("SUPP=YES") || upper.contains("SUPP=IMPLICIT")) {
        return Err(CliError::Failed(format!(
            "Lab Oracle database supplemental logging is not enabled:\n{text}"
        )));
    }
    Ok(())
}

async fn ensure_platform_store_migrated(lab_dir: &Path) -> Result<(), CliError> {
    // Prefer migrating via the Lab app container so host does not need network aliases.
    let (ok, out) = run_compose(
        lab_dir,
        &[
            "exec",
            "-T",
            "app",
            "migraloop",
            "migrate",
            "--platform-store-url",
            "postgres://migraloop:migraloop@platform-store:5432/migraloop",
        ],
    )
    .await?;
    if ok {
        return Ok(());
    }
    // Fallback: host-side migrate against published port (app may still be starting).
    let migrate = Command::new(std::env::current_exe().unwrap_or_else(|_| PathBuf::from("migraloop")))
        .args([
            "migrate",
            "--platform-store-url",
            LAB_PLATFORM_STORE_URL,
        ])
        .output()
        .await;
    match migrate {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(CliError::Failed(format!(
            "Lab Platform Store migrate failed (compose exec):\n{out}\n(host migrate):\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))),
        Err(err) => Err(CliError::Failed(format!(
            "Lab Platform Store migrate failed:\n{out}\nhost migrate error: {err}"
        ))),
    }
}

async fn platform_store_reports_healthy() -> bool {
    let output = Command::new(std::env::current_exe().unwrap_or_else(|_| PathBuf::from("migraloop")))
        .args(["status", "--platform-store-url", LAB_PLATFORM_STORE_URL])
        .output()
        .await;
    match output {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            out.status.success() && text.contains("Platform Store: healthy")
        }
        Err(_) => false,
    }
}

fn print_connection_details() {
    println!();
    println!("Connection details (Lab disposable defaults — local use only):");
    println!("  Platform Store: {LAB_PLATFORM_STORE_URL}");
    println!(
        "  Oracle Source:  host={LAB_ORACLE_HOST} port={LAB_ORACLE_PORT} \
         service={LAB_ORACLE_SERVICE} user={LAB_ORACLE_USER} \
         {LAB_ORACLE_PASSWORD_ENV}={LAB_ORACLE_PASSWORD_DEFAULT}"
    );
    println!(
        "  MongoDB Target: host={LAB_MONGO_HOST} port={LAB_MONGO_PORT} \
         database={LAB_MONGO_DATABASE} user={LAB_MONGO_USER} \
         {LAB_MONGO_PASSWORD_ENV}={LAB_MONGO_PASSWORD_DEFAULT}"
    );
    println!();
    println!("Next:");
    println!("  export MIGRALOOP_PLATFORM_STORE_URL={LAB_PLATFORM_STORE_URL}");
    println!("  export {LAB_ORACLE_PASSWORD_ENV}={LAB_ORACLE_PASSWORD_DEFAULT}");
    println!("  export {LAB_MONGO_PASSWORD_ENV}={LAB_MONGO_PASSWORD_DEFAULT}");
    println!("  migraloop status");
    println!("  # Apply a Deployment yourself, or run a Lab Scenario when available.");
}

fn print_empty_deployment_notice() {
    println!("Deployment: (none)");
    println!("Pipeline: (none)");
    println!("  (Lab Fixture does not apply a default Deployment or Pipelines)");
}
