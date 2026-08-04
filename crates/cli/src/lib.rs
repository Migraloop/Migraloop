//! Operator-facing CLI for the DB Sync Platform.

mod config;
mod lab;
mod lab_scenario;
mod observability;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use lab::{run_lab, LabCommand};
use migraloop_delivery::ManagedFieldAs;
use migraloop_platform_store::{
    check_store_settings, disk_warn_message, Deployment, Pipeline, PlatformStore,
    PlatformStoreHealth, SystemConnection,
};
use migraloop_runtime::{
    assemble_observability_surface, inspect_base_rows, inspect_derived_rows,
    inspect_target_documents, status_inventory_from_url,
};
use thiserror::Error;

use crate::config::{
    load_deployment_config, resolve_tls_settings, secret_ref_from_resolved, DeploymentDocument,
    PipelineSpec,
};
use crate::observability::{emit_event, EventValue};

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Failed(String),
}

impl From<migraloop_runtime::RuntimeError> for CliError {
    fn from(err: migraloop_runtime::RuntimeError) -> Self {
        CliError::Failed(err.to_string())
    }
}

#[derive(Debug, Parser)]
#[command(name = "migraloop", about = "DB Sync Platform CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Apply versioned Platform Store schema migrations
    Migrate {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
    },
    /// Apply a declarative Deployment config (YAML or JSON)
    Apply {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Path to Deployment config (YAML or JSON)
        #[arg(long, short = 'f')]
        file: PathBuf,
    },
    /// Report Platform Store reachability, health, Deployments, Pipelines, and Base Datasets
    Status {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
    },
    /// Inspect Base Dataset rows for a Source table (operator-facing Platform Store check)
    Base {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Source table name of the Base Dataset
        #[arg(long)]
        table: String,
        /// Deployment name when multiple Bases share a table name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Inspect Target documents for a Pipeline collection (operator-facing Delivery check)
    Target {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Target collection name
        #[arg(long)]
        collection: String,
        /// Deployment name when multiple Pipelines share a collection name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Inspect Derived Dataset rows for a Transform Pipeline
    Derived {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Pipeline name whose Derived Dataset to inspect
        #[arg(long)]
        pipeline: String,
        /// Deployment name when multiple Derived Datasets share a Pipeline name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// One-shot Incremental Capture into Base Datasets, then Delivery (Lab / operator catch-up)
    Sync {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
    },
    /// Run Source Alignment Check: verify Base matches Source (resource-gated); repair Base only
    Align {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Source table name of the Base Dataset (default: all Bases)
        #[arg(long)]
        table: Option<String>,
        /// Deployment name when multiple Bases share a table name
        #[arg(long)]
        deployment: Option<String>,
        /// Max Source rows to read per Base (resource gate; default 1000 — not a full slam)
        #[arg(long, default_value = "1000")]
        max_rows: u32,
    },
    /// Run Drift Check: verify Managed fields on Target match platform expected dataset; auto-repair Managed
    Drift {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Pipeline name (default: all Pipelines with a Target Binding)
        #[arg(long)]
        pipeline: Option<String>,
        /// Deployment name when multiple Pipelines share a name
        #[arg(long)]
        deployment: Option<String>,
        /// Max Output Identities to check per Pipeline (resource gate; default 1000 — not a full slam)
        #[arg(long, default_value = "1000")]
        max_rows: u32,
    },
    /// Pause a Pipeline: stop further Delivery/processing without restarting the Deployment
    Pause {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Pipeline name to pause
        #[arg(long)]
        pipeline: String,
        /// Deployment name when multiple Pipelines share a name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Resume a paused Pipeline: continue Delivery from durable Platform Store state
    Resume {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Pipeline name to resume
        #[arg(long)]
        pipeline: String,
        /// Deployment name when multiple Pipelines share a name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Remove a Pipeline: stop it and cease Delivery without restarting the Deployment
    Remove {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Pipeline name to remove
        #[arg(long)]
        pipeline: String,
        /// Deployment name when multiple Pipelines share a name
        #[arg(long)]
        deployment: Option<String>,
    },
    /// Run the app: migrate, continuous Incremental Capture + Delivery, Observability metrics
    Run {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
        /// Prometheus scrape listen address (host:port) for Observability Surface
        #[arg(long, env = "MIGRALOOP_METRICS_ADDR", default_value = "0.0.0.0:9090")]
        metrics_addr: String,
    },
    /// Local Sync Lab Fixture and Lab Scenarios (disposable Oracle, MongoDB, Platform Store, app)
    Lab {
        #[command(subcommand)]
        command: LabCommand,
    },
}

pub fn parse() -> Cli {
    Cli::parse()
}

async fn apply_migrations(platform_store_url: &str) -> Result<(), CliError> {
    // Reject absurd under-provisioning before applying schema (ADR-0010).
    let store = PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let settings = store
        .probe_settings()
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    check_store_settings(&settings).map_err(|err| CliError::Failed(err.to_string()))?;
    store
        .migrate()
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    println!("Platform Store migrations applied");
    Ok(())
}

/// Surface free-disk warn threshold (warn only — never pauses Pipelines).
async fn report_store_resource_warnings(platform_store_url: &str) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    let resources = store
        .probe_resources()
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if let (true, Some(free)) = (resources.disk_warn, resources.free_disk_bytes) {
        let msg = disk_warn_message(free);
        println!("{msg}");
        emit_event(
            "platform_store_disk_warn",
            &[
                ("free_disk_bytes", EventValue::from(free as i64)),
                (
                    "warn_threshold_bytes",
                    EventValue::from(migraloop_platform_store::DISK_FREE_WARN_BYTES as i64),
                ),
                ("auto_pause", EventValue::from(false)),
            ],
        );
    }
    Ok(())
}

fn document_to_deployment(doc: &DeploymentDocument) -> Result<Deployment, CliError> {
    // Resolve to validate references exist; never persist resolved secret values.
    let _ = doc.spec.source.password.resolve("source.password")?;
    let _ = doc.spec.target.password.resolve("target.password")?;
    let source_password_ref =
        secret_ref_from_resolved(doc.spec.source.password.resolved_ref("source.password")?);
    let target_password_ref =
        secret_ref_from_resolved(doc.spec.target.password.resolved_ref("target.password")?);
    let source_tls = resolve_tls_settings("source", doc.spec.source.tls.as_ref())?;
    let target_tls = resolve_tls_settings("target", doc.spec.target.tls.as_ref())?;

    Ok(Deployment {
        name: doc.metadata.name.clone(),
        source: SystemConnection {
            kind: doc.spec.source.kind.clone(),
            host: doc.spec.source.host.clone(),
            port: doc.spec.source.port,
            database: doc.spec.source.database.clone(),
            username: doc.spec.source.username.clone(),
            password_ref: source_password_ref,
            timezone: doc
                .spec
                .source
                .timezone
                .clone()
                .unwrap_or_default()
                .trim()
                .to_string(),
            tls: source_tls,
        },
        target: SystemConnection {
            kind: doc.spec.target.kind.clone(),
            host: doc.spec.target.host.clone(),
            port: doc.spec.target.port,
            database: doc.spec.target.database.clone(),
            username: doc.spec.target.username.clone(),
            password_ref: target_password_ref,
            timezone: String::new(),
            tls: target_tls,
        },
    })
}

fn pipelines_from_document(doc: &DeploymentDocument) -> Vec<Pipeline> {
    doc.spec
        .pipelines
        .iter()
        .map(|pipeline| pipeline_from_spec(&doc.metadata.name, pipeline))
        .collect()
}

fn pipeline_from_spec(deployment_name: &str, pipeline: &PipelineSpec) -> Pipeline {
    let target_collection = pipeline
        .target
        .as_ref()
        .map(|t| t.collection.clone())
        .unwrap_or_default();
    let delivery_status = if target_collection.is_empty() {
        "not_configured".to_string()
    } else {
        "pending".to_string()
    };
    let field_mappings = pipeline
        .fields
        .iter()
        .map(|(name, spec)| {
            let mapping = match spec.map_as {
                crate::config::FieldMappingAsSpec::String => ManagedFieldAs::String,
                crate::config::FieldMappingAsSpec::Omit => ManagedFieldAs::Omit,
            };
            (name.clone(), mapping)
        })
        .collect();
    let output_identity = pipeline.output_identity.clone().unwrap_or_default();
    let transform_json = pipeline
        .transform
        .as_ref()
        .map(|steps| serde_json::Value::Array(steps.clone()));
    Pipeline {
        deployment_name: deployment_name.to_string(),
        name: pipeline.name.clone(),
        mode: pipeline.mode.clone(),
        source_table: pipeline.source.table.clone(),
        source_schema: pipeline.source.schema.clone().unwrap_or_default(),
        target_collection,
        delivery_status,
        delivery_applied_changes: 0,
        delivery_lag: 0,
        paused: false,
        description: pipeline
            .description
            .clone()
            .unwrap_or_default()
            .trim()
            .to_string(),
        field_mappings,
        output_identity,
        transform_json,
        drift_status: "unknown".to_string(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

fn format_system_line(label: &str, system: &SystemConnection) -> String {
    let timezone = if system.timezone.is_empty() {
        "(none)".to_string()
    } else {
        system.timezone.clone()
    };
    format!(
        "  {label}: {} {}:{} database={} username={} passwordRef={} timezone={} {}",
        system.kind,
        system.host,
        system.port,
        system.database,
        system.username,
        system.password_ref.display(),
        timezone,
        system.tls.display_summary()
    )
}

/// Operator-facing Output Identity label for `status` / quarantine narrative.
fn format_output_identity(identity: &serde_json::Value) -> String {
    match identity {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Operator `status` Incremental Capture mechanism label for Oracle Sources.
///
/// Mirrors Source adapter harness selection (`host: contract|stub` → contract;
/// otherwise OCI) without constructing a full capture connect object.
fn oracle_incremental_capture_label(source: &SystemConnection) -> &'static str {
    let host = source.host.trim();
    if host.eq_ignore_ascii_case("contract") || host.eq_ignore_ascii_case("stub") {
        "LogMiner (contract)"
    } else {
        "LogMiner (OCI)"
    }
}

async fn apply_deployment(platform_store_url: &str, file: &Path) -> Result<(), CliError> {
    let store = PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let doc = load_deployment_config(file)?;
    let deployment = document_to_deployment(&doc)?;
    let pipelines = pipelines_from_document(&doc);
    migraloop_runtime::apply(&store, deployment, pipelines)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))
}

async fn sync_incremental(platform_store_url: &str) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::sync_incremental(&store).await?;
    Ok(())
}

async fn open_store(platform_store_url: &str) -> Result<PlatformStore, CliError> {
    PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))
}

async fn source_alignment_check(
    platform_store_url: &str,
    table: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::source_alignment_check(&store, table, deployment, max_rows).await?;
    Ok(())
}

async fn drift_check(
    platform_store_url: &str,
    pipeline_name: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::drift_check(&store, pipeline_name, deployment, max_rows).await?;
    Ok(())
}

async fn print_status(platform_store_url: &str) -> Result<(), CliError> {
    let inventory = status_inventory_from_url(platform_store_url).await?;
    // Typed Sync/Delivery Health (+ lag / quarantine / schema-impact / disk-warn)
    // come from one runtime assembly; CLI only formats Operator narrative (#174).
    let surface = assemble_observability_surface(&inventory);

    match &surface.store_health {
        PlatformStoreHealth::Healthy { schema_version } => {
            if let Some(err) = &surface.guardrail_error {
                println!("Platform Store: unhealthy");
                eprintln!("{err}");
                return Err(CliError::Failed(err.clone()));
            }
            println!("Platform Store: healthy");
            println!("Schema version: {schema_version}");
            // Warn-only: free-disk pressure must not flip health or pause Pipelines.
            if let (true, Some(free)) = (surface.disk_warn, surface.free_disk_bytes) {
                let msg = disk_warn_message(free);
                println!("{msg}");
                emit_event(
                    "platform_store_disk_warn",
                    &[
                        ("free_disk_bytes", EventValue::from(free as i64)),
                        (
                            "warn_threshold_bytes",
                            EventValue::from(migraloop_platform_store::DISK_FREE_WARN_BYTES as i64),
                        ),
                        ("auto_pause", EventValue::from(false)),
                    ],
                );
            }
        }
        PlatformStoreHealth::Unhealthy { reason } => {
            println!("Platform Store: unhealthy");
            eprintln!("{reason}");
            return Err(CliError::Failed(
                "Platform Store is reachable but not healthy".to_string(),
            ));
        }
        PlatformStoreHealth::Unreachable { reason } => {
            println!("Platform Store: unreachable");
            eprintln!("{reason}");
            return Err(CliError::Failed(
                "Platform Store is unreachable".to_string(),
            ));
        }
    }

    let deployments = &inventory.deployments;
    if deployments.is_empty() {
        println!("Deployment: (none)");
    } else {
        for deployment in deployments {
            println!("Deployment: {}", deployment.name);
            println!("{}", format_system_line("Source", &deployment.source));
            println!("{}", format_system_line("Target", &deployment.target));
            if deployment.source.kind.eq_ignore_ascii_case("oracle") {
                println!(
                    "  Incremental Capture: {}",
                    oracle_incremental_capture_label(&deployment.source)
                );
            }
        }
    }

    let pipelines = &inventory.pipelines;
    if pipelines.is_empty() {
        println!("Pipeline: (none)");
    } else {
        for pipeline in pipelines {
            let pause_note = if pipeline.paused { " paused" } else { "" };
            if pipeline.target_collection.is_empty() {
                println!(
                    "Pipeline: {} ({}) source={}{pause_note}",
                    pipeline.name, pipeline.mode, pipeline.source_table
                );
            } else {
                println!(
                    "Pipeline: {} ({}) source={} target={} Delivery: {}{pause_note}",
                    pipeline.name,
                    pipeline.mode,
                    pipeline.source_table,
                    pipeline.target_collection,
                    pipeline.delivery_status
                );
            }
        }
    }

    let bases = &inventory.bases;
    if bases.is_empty() {
        println!("Base Dataset: (none)");
    } else {
        for base in bases {
            let columns = base
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let omitted = if base.omitted_columns.is_empty() {
                "(none)".to_string()
            } else {
                base.omitted_columns
                    .iter()
                    .map(|c| format!("{} ({})", c.name, c.oracle_type))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            println!(
                "Base Dataset: {} status={} rows={} columns=[{}] omittedUnsupported=[{}]",
                base.source_table, base.status, base.row_count, columns, omitted
            );
            match base.status.as_str() {
                "initial_load_complete" => println!("  Initial Load complete"),
                "initial_load_in_progress" => {
                    println!(
                        "  Initial Load in progress (rows={} chunk cursor present={})",
                        base.row_count,
                        base.initial_load_cursor.is_some()
                    );
                }
                "initial_load_paused" => {
                    println!(
                        "  Initial Load paused (rows={}; re-run apply to resume)",
                        base.row_count
                    );
                }
                _ => {}
            }
            match (base.capture_low_watermark, base.capture_checkpoint) {
                (Some(wm), Some(cp)) => {
                    println!("  Cutover: low-watermark={wm} checkpoint={cp}");
                }
                (Some(wm), None) => {
                    println!("  Cutover: low-watermark={wm} checkpoint=(none)");
                }
                _ => {
                    println!("  Cutover: low-watermark=(missing)");
                }
            }
            let sync_obs = surface
                .sync
                .iter()
                .find(|s| {
                    s.deployment_name == base.deployment_name && s.source_table == base.source_table
                })
                .expect("Observability assembly must cover every Base Dataset in inventory");
            println!(
                "  Sync Health: {} appliedChanges={} lag={} checkpoint={}",
                sync_obs.health.as_str(),
                sync_obs.applied_changes,
                sync_obs.lag,
                sync_obs
                    .checkpoint
                    .map(|cp| cp.to_string())
                    .unwrap_or_else(|| "(none)".to_string())
            );
            println!(
                "  Source Alignment: {} checkedRows={} mismatchedRows={}",
                base.source_alignment,
                base.source_alignment_checked_rows,
                base.source_alignment_mismatched_rows
            );
        }
    }

    let derived = &inventory.derived;
    if derived.is_empty() {
        println!("Derived Dataset: (none)");
    } else {
        for dataset in derived {
            let columns = dataset
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let identity = dataset.output_identity.join(", ");
            println!(
                "Derived Dataset: Pipeline={} status={} rows={} outputIdentity=[{}] columns=[{}]",
                dataset.pipeline_name, dataset.status, dataset.row_count, identity, columns
            );
        }
    }

    let quarantines = &inventory.quarantines;
    let schema_impacts = &inventory.schema_impacts;

    for pipeline in pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        let delivery_obs = surface
            .delivery
            .iter()
            .find(|d| {
                d.deployment_name == pipeline.deployment_name && d.pipeline_name == pipeline.name
            })
            .expect("Observability assembly must cover every Target-bound Pipeline");
        let delivery_health = delivery_obs.health.as_str();
        let applied = delivery_obs.applied_changes;
        let lag = delivery_obs.lag;
        let quarantined = delivery_obs.quarantined;
        let schema_blocking = delivery_obs.schema_blocking;
        let status_label = if pipeline.paused {
            format!("{} (paused)", pipeline.delivery_status)
        } else {
            pipeline.delivery_status.clone()
        };
        if quarantined == 0 {
            println!(
                "  Delivery Health: {} Pipeline={} status={} appliedChanges={} lag={}",
                delivery_health, pipeline.name, status_label, applied, lag
            );
        } else {
            println!(
                "  Delivery Health: {} Pipeline={} status={} appliedChanges={} lag={} quarantined={}",
                delivery_health, pipeline.name, status_label, applied, lag, quarantined
            );
        }
        println!(
            "  Drift: {} Pipeline={} checkedRows={} mismatchedRows={}",
            pipeline.drift_status,
            pipeline.name,
            pipeline.drift_checked_rows,
            pipeline.drift_mismatched_rows
        );
        if schema_blocking > 0 {
            println!(
                "  Schema Change: Pipeline={} blocking={} paused (stream-wide DDL; not poison quarantine)",
                pipeline.name, schema_blocking
            );
        }
    }

    if quarantines.is_empty() {
        println!("Quarantine: (none)");
    } else {
        for q in quarantines {
            let identity = format_output_identity(&q.output_identity);
            println!(
                "  Quarantine: Pipeline={} identity={} change_id={} stage={} attempts={} \
                 unhealthy / not aligned error={}",
                q.pipeline_name, identity, q.change_id, q.stage, q.attempts, q.last_error
            );
        }
    }

    if schema_impacts.is_empty() {
        println!("Schema Change: (none)");
    } else {
        for s in schema_impacts {
            println!(
                "  Schema Change: Pipeline={} impact={} change_id={} table={} ddl={} status={}",
                s.pipeline_name, s.impact, s.change_id, s.source_table, s.ddl_summary, s.status
            );
        }
    }

    Ok(())
}

async fn print_base(
    platform_store_url: &str,
    table: &str,
    deployment: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    let (dataset, rows) = inspect_base_rows(&store, table, deployment).await?;

    println!(
        "Base Dataset: {} status={} rows={}",
        dataset.source_table, dataset.status, dataset.row_count
    );
    match (dataset.capture_low_watermark, dataset.capture_checkpoint) {
        (Some(wm), Some(cp)) => {
            println!("cutover: low-watermark={wm} checkpoint={cp}");
        }
        (Some(wm), None) => {
            println!("cutover: low-watermark={wm} checkpoint=(none)");
        }
        _ => {
            println!("cutover: low-watermark=(missing)");
        }
    }
    let columns = dataset
        .columns
        .iter()
        .map(|c| format!("{}:{}", c.name, c.oracle_type))
        .collect::<Vec<_>>()
        .join(", ");
    println!("columns: [{columns}]");
    if !dataset.omitted_columns.is_empty() {
        let omitted = dataset
            .omitted_columns
            .iter()
            .map(|c| format!("{} ({})", c.name, c.oracle_type))
            .collect::<Vec<_>>()
            .join(", ");
        println!("omittedUnsupported: [{omitted}]");
    }

    for row in rows {
        let value = serde_json::Value::Object(row.data);
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_default()
        );
    }

    Ok(())
}

async fn print_derived(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    let (dataset, rows) = inspect_derived_rows(&store, pipeline_name, deployment_name).await?;

    let identity = dataset.output_identity.join(", ");
    let columns = dataset
        .columns
        .iter()
        .map(|c| format!("{}:{}", c.name, c.oracle_type))
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "Derived Dataset: Pipeline={} status={} rows={} outputIdentity=[{}]",
        dataset.pipeline_name, dataset.status, dataset.row_count, identity
    );
    println!("columns: [{columns}]");
    for row in rows {
        let value = serde_json::Value::Object(row.data);
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_default()
        );
    }
    Ok(())
}

async fn print_target(
    platform_store_url: &str,
    collection: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    let (deployment, pipeline, documents) =
        inspect_target_documents(&store, collection, deployment_name).await?;

    println!(
        "Target: {}.{} Deployment={} Pipeline={} Delivery={}",
        deployment.target.database,
        collection,
        pipeline.deployment_name,
        pipeline.name,
        pipeline.delivery_status
    );
    println!("documents: {}", documents.len());
    for document in documents {
        println!(
            "{}",
            serde_json::to_string_pretty(&document).unwrap_or_default()
        );
    }
    Ok(())
}

async fn pause_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::pause_pipeline(&store, pipeline_name, deployment_name).await?;
    Ok(())
}

async fn resume_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::resume_pipeline(&store, pipeline_name, deployment_name).await?;
    Ok(())
}

async fn remove_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    let store = open_store(platform_store_url).await?;
    migraloop_runtime::remove_pipeline(&store, pipeline_name, deployment_name).await?;
    Ok(())
}

pub async fn run(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Migrate { platform_store_url } => apply_migrations(&platform_store_url).await,
        Command::Apply {
            platform_store_url,
            file,
        } => apply_deployment(&platform_store_url, &file).await,
        Command::Status { platform_store_url } => print_status(&platform_store_url).await,
        Command::Base {
            platform_store_url,
            table,
            deployment,
        } => print_base(&platform_store_url, &table, deployment.as_deref()).await,
        Command::Target {
            platform_store_url,
            collection,
            deployment,
        } => print_target(&platform_store_url, &collection, deployment.as_deref()).await,
        Command::Derived {
            platform_store_url,
            pipeline,
            deployment,
        } => print_derived(&platform_store_url, &pipeline, deployment.as_deref()).await,
        Command::Sync { platform_store_url } => sync_incremental(&platform_store_url).await,
        Command::Align {
            platform_store_url,
            table,
            deployment,
            max_rows,
        } => {
            source_alignment_check(
                &platform_store_url,
                table.as_deref(),
                deployment.as_deref(),
                max_rows,
            )
            .await
        }
        Command::Drift {
            platform_store_url,
            pipeline,
            deployment,
            max_rows,
        } => {
            drift_check(
                &platform_store_url,
                pipeline.as_deref(),
                deployment.as_deref(),
                max_rows,
            )
            .await
        }
        Command::Pause {
            platform_store_url,
            pipeline,
            deployment,
        } => pause_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await,
        Command::Resume {
            platform_store_url,
            pipeline,
            deployment,
        } => resume_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await,
        Command::Remove {
            platform_store_url,
            pipeline,
            deployment,
        } => remove_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await,
        Command::Run {
            platform_store_url,
            metrics_addr,
        } => {
            apply_migrations(&platform_store_url).await?;
            // Warn-only disk threshold at process start (never auto-pauses Pipelines).
            report_store_resource_warnings(&platform_store_url).await?;
            println!("migraloop is running");
            let addr: SocketAddr = metrics_addr.parse().map_err(|err| {
                CliError::Failed(format!(
                    "invalid --metrics-addr / MIGRALOOP_METRICS_ADDR `{metrics_addr}`: {err}"
                ))
            })?;
            // Continuous Incremental Capture + Delivery on the same single active
            // instance that serves Observability /metrics (issue #145 / ADR-0008).
            // Open the Platform Store session at the Operator edge; runtime supervise
            // prefers that session handle over URL reopen (issue #172).
            let sync_store = open_store(&platform_store_url).await?;
            tokio::spawn(async move {
                migraloop_runtime::supervise_continuous_incremental_sync(sync_store).await;
            });
            observability::serve_prometheus_metrics(addr, platform_store_url).await
        }
        Command::Lab { command } => run_lab(command).await,
    }
}
