//! Operator-facing CLI for the DB Sync Platform.

mod config;
mod lab;
mod lab_scenario;
mod observability;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use lab::{run_lab, LabCommand};
use migraloop_capture::{alignment_check_read_for_source, AlignmentCheckSample};
use migraloop_delivery::{
    list_target_documents, upsert_managed_documents, DeliveryDocument, ManagedFieldAs,
};
use migraloop_platform_store::{
    check_store_settings, clear_schema_change_impacts, delete_base_datasets_not_in, delete_pipeline,
    disk_warn_message, get_base_rows, get_derived_rows, health, list_base_datasets,
    list_deployments, list_derived_datasets, list_pipelines, list_quarantined_changes,
    list_schema_change_impacts, probe_store_resources, probe_store_settings, replace_base_dataset,
    set_pipeline_paused, update_pipeline_drift_status, BaseDataset, Deployment, Pipeline,
    PlatformStore, PlatformStoreHealth, SystemConnection,
};
use migraloop_runtime::{
    deliver_direct_pipeline_with_options, deliver_transform_pipeline_with_options,
    delivery_document_for_row, ensure_store_session_healthy, format_output_identity, identity_key,
    mongo_target_from_deployment, oracle_source_connect, pipeline_base_table_refs,
    pipeline_has_target, resolve_secret_value, source_timezone_opt, target_document_identity_key,
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

/// Reject absurdly low Platform Store settings (ADR-0010). Warn-only disk
/// thresholds are handled separately and must not fail this check.
async fn enforce_store_guardrails(platform_store_url: &str) -> Result<(), CliError> {
    let settings = probe_store_settings(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    check_store_settings(&settings).map_err(|err| CliError::Failed(err.to_string()))
}

/// Surface free-disk warn threshold (warn only — never pauses Pipelines).
async fn report_store_resource_warnings(platform_store_url: &str) -> Result<(), CliError> {
    let resources = probe_store_resources(platform_store_url)
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

async fn ensure_store_healthy(platform_store_url: &str) -> Result<(), CliError> {
    let store = PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    ensure_store_session_healthy(&store).await?;
    Ok(())
}

async fn sync_incremental(platform_store_url: &str) -> Result<(), CliError> {
    let store = PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    migraloop_runtime::sync_incremental(&store).await?;
    Ok(())
}

/// Default Source Alignment Check read budget (resource gate; not a full slam).
const DEFAULT_ALIGNMENT_MAX_ROWS: u32 = 1000;

/// Default Drift Check identity budget (resource gate; not a full slam).
const DEFAULT_DRIFT_MAX_ROWS: u32 = 1000;

fn supported_row_projection(
    row: &serde_json::Map<String, serde_json::Value>,
    supported: &BTreeSet<String>,
) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .filter(|(name, _)| supported.contains(name.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn base_identity_key(
    row: &serde_json::Map<String, serde_json::Value>,
    primary_key: &[String],
) -> Option<String> {
    if primary_key.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(primary_key.len());
    for col in primary_key {
        let value = row.get(col)?;
        parts.push(identity_key(value));
    }
    Some(parts.join("|"))
}

fn rows_equal_supported(
    left: &serde_json::Map<String, serde_json::Value>,
    right: &serde_json::Map<String, serde_json::Value>,
    supported: &BTreeSet<String>,
) -> bool {
    for name in supported {
        if left.get(name) != right.get(name) {
            return false;
        }
    }
    true
}

async fn source_alignment_check(
    platform_store_url: &str,
    table: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;
    let max_rows = if max_rows == 0 {
        DEFAULT_ALIGNMENT_MAX_ROWS
    } else {
        max_rows
    };

    let deployments = list_deployments(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if deployments.is_empty() {
        return Err(CliError::Failed(
            "no Deployments applied; run `migraloop apply` first".to_string(),
        ));
    }

    let bases = list_base_datasets(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let targets: Vec<BaseDataset> = bases
        .into_iter()
        .filter(|base| {
            table
                .map(|t| base.source_table.eq_ignore_ascii_case(t))
                .unwrap_or(true)
                && deployment
                    .map(|d| base.deployment_name == d)
                    .unwrap_or(true)
        })
        .collect();
    if targets.is_empty() {
        return Err(CliError::Failed(match (table, deployment) {
            (Some(t), Some(d)) => {
                format!("no Base Dataset found for table {t} in Deployment {d}")
            }
            (Some(t), None) => format!("no Base Dataset found for table {t}"),
            (None, Some(d)) => format!("no Base Datasets found for Deployment {d}"),
            (None, None) => "no Base Datasets found; run `migraloop apply` first".to_string(),
        }));
    }

    for base in targets {
        let deployment = deployments
            .iter()
            .find(|d| d.name == base.deployment_name)
            .ok_or_else(|| {
                CliError::Failed(format!(
                    "Deployment {} missing for Base Dataset {}",
                    base.deployment_name, base.source_table
                ))
            })?;
        align_one_base(platform_store_url, deployment, &base, max_rows).await?;
    }
    Ok(())
}

async fn align_one_base(
    platform_store_url: &str,
    deployment: &Deployment,
    base: &BaseDataset,
    max_rows: u32,
) -> Result<(), CliError> {
    if base.primary_key.is_empty() {
        return Err(CliError::Failed(format!(
            "Base Dataset {} has no primary key for Source Alignment Check",
            base.source_table
        )));
    }

    let connect = oracle_source_connect(&deployment.source)?;
    let password = resolve_secret_value(&deployment.source.password_ref, "source.password")?;
    let configured_tz = source_timezone_opt(deployment);
    let sample: AlignmentCheckSample = alignment_check_read_for_source(
        &connect,
        &password,
        &base.source_schema,
        &base.source_table,
        max_rows,
        configured_tz,
    )
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let (_, base_rows) = get_base_rows(
        platform_store_url,
        &base.source_table,
        Some(&base.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let supported: BTreeSet<String> = if base.columns.is_empty() {
        sample
            .columns
            .iter()
            .filter(|c| c.supported)
            .map(|c| c.name.clone())
            .collect()
    } else {
        base.columns.iter().map(|c| c.name.clone()).collect()
    };

    let mut base_by_id: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    for row in &base_rows {
        let Some(key) = base_identity_key(&row.data, &base.primary_key) else {
            continue;
        };
        base_by_id.insert(key, row.data.clone());
    }

    let mut repaired: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    let mut mismatched = 0i32;
    let mut repaired_count = 0i32;
    let mut checked_ids: BTreeSet<String> = BTreeSet::new();

    for source_row in &sample.rows {
        let source_as_map: serde_json::Map<String, serde_json::Value> = source_row
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let source_map = supported_row_projection(&source_as_map, &supported);
        let Some(key) = base_identity_key(&source_map, &base.primary_key) else {
            continue;
        };
        checked_ids.insert(key.clone());
        match base_by_id.get(&key) {
            Some(existing) if rows_equal_supported(existing, &source_map, &supported) => {
                repaired.insert(key, existing.clone());
            }
            Some(_) | None => {
                mismatched += 1;
                repaired_count += 1;
                repaired.insert(key, source_map);
            }
        }
    }

    // Rows outside the gated Source window: keep when truncated; drop when full read.
    for (key, row) in &base_by_id {
        if checked_ids.contains(key) {
            continue;
        }
        if sample.truncated {
            repaired.insert(key.clone(), row.clone());
        } else {
            mismatched += 1;
            repaired_count += 1;
            // Source no longer has this identity — remove from Base (never write Source).
        }
    }

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        repaired.into_values().collect();
    // Stable ordinal order by primary key for inspectability.
    rows.sort_by(|a, b| {
        let ka = base_identity_key(a, &base.primary_key).unwrap_or_default();
        let kb = base_identity_key(b, &base.primary_key).unwrap_or_default();
        ka.cmp(&kb)
    });

    let alignment_status = if sample.truncated {
        "partial"
    } else {
        "aligned"
    };
    let checked = sample.rows.len() as i32;
    let updated = BaseDataset {
        deployment_name: base.deployment_name.clone(),
        source_table: base.source_table.clone(),
        source_schema: base.source_schema.clone(),
        status: base.status.clone(),
        primary_key: base.primary_key.clone(),
        columns: base.columns.clone(),
        omitted_columns: base.omitted_columns.clone(),
        row_count: rows.len() as i32,
        sync_applied_changes: base.sync_applied_changes,
        sync_health: base.sync_health.clone(),
        capture_low_watermark: base.capture_low_watermark,
        capture_checkpoint: base.capture_checkpoint,
        sync_lag: base.sync_lag,
        source_alignment: alignment_status.to_string(),
        source_alignment_checked_rows: checked,
        source_alignment_mismatched_rows: mismatched,
        initial_load_cursor: None,
    };

    replace_base_dataset(platform_store_url, &updated, &rows)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    let truncated_note = if sample.truncated {
        " truncated=true"
    } else {
        ""
    };
    let detect_status = if mismatched > 0 {
        "misaligned"
    } else if sample.truncated {
        "partial"
    } else {
        "aligned"
    };
    println!(
        "Source Alignment Check: {} status={detect_status} checked={checked} \
         mismatched={mismatched} repaired={repaired_count} maxRows={max_rows}{truncated_note} \
         (Base repaired from Source reads; Source not written)",
        base.source_table
    );
    Ok(())
}

async fn drift_check(
    platform_store_url: &str,
    pipeline_name: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;
    let max_rows = if max_rows == 0 {
        DEFAULT_DRIFT_MAX_ROWS
    } else {
        max_rows
    };

    let deployments = list_deployments(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if deployments.is_empty() {
        return Err(CliError::Failed(
            "no Deployments applied; run `migraloop apply` first".to_string(),
        ));
    }

    let pipelines = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let targets: Vec<Pipeline> = pipelines
        .into_iter()
        .filter(|p| {
            pipeline_has_target(p)
                && pipeline_name.map(|n| p.name == n).unwrap_or(true)
                && deployment.map(|d| p.deployment_name == d).unwrap_or(true)
        })
        .collect();
    if targets.is_empty() {
        return Err(CliError::Failed(match (pipeline_name, deployment) {
            (Some(n), Some(d)) => {
                format!("no Pipeline with Target Binding named {n} in Deployment {d}")
            }
            (Some(n), None) => format!("no Pipeline with Target Binding named {n}"),
            (None, Some(d)) => {
                format!("no Pipelines with Target Binding found for Deployment {d}")
            }
            (None, None) => {
                "no Pipelines with Target Binding found; run `migraloop apply` first".to_string()
            }
        }));
    }

    for pipeline in targets {
        let deployment = deployments
            .iter()
            .find(|d| d.name == pipeline.deployment_name)
            .ok_or_else(|| {
                CliError::Failed(format!(
                    "Deployment {} missing for Pipeline {}",
                    pipeline.deployment_name, pipeline.name
                ))
            })?;
        drift_one_pipeline(platform_store_url, deployment, &pipeline, max_rows).await?;
    }
    Ok(())
}

async fn drift_one_pipeline(
    platform_store_url: &str,
    deployment: &Deployment,
    pipeline: &Pipeline,
    max_rows: u32,
) -> Result<(), CliError> {
    ensure_drift_baseline_ready(platform_store_url, pipeline).await?;

    let mongo = mongo_target_from_deployment(deployment)?;
    let (expected_docs, truncated) =
        expected_delivery_documents_for_drift(platform_store_url, pipeline, max_rows).await?;

    let target_docs = list_target_documents(&mongo, &pipeline.target_collection)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let mut target_by_id: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for doc in target_docs {
        if let Some(key) = target_document_identity_key(&doc) {
            target_by_id.insert(key, doc);
        }
    }

    let mut mismatched = 0i32;
    let mut repaired_count = 0i32;
    let mut repair_docs: Vec<DeliveryDocument> = Vec::new();

    for expected in &expected_docs {
        let key = identity_key(&expected.identity);
        let managed_keys: Vec<&str> = expected.managed_fields.keys().map(|k| k.as_str()).collect();
        let drifted = match target_by_id.get(&key) {
            Some(target_doc) => {
                !managed_fields_match_target(target_doc, &expected.managed_fields, &managed_keys)
            }
            None => true,
        };
        if drifted {
            mismatched += 1;
            repaired_count += 1;
            repair_docs.push(expected.clone());
        }
    }

    if !repair_docs.is_empty() {
        upsert_managed_documents(&mongo, &pipeline.target_collection, &repair_docs)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
    }

    let checked = expected_docs.len() as i32;
    let drift_status = if truncated { "partial" } else { "ok" };
    update_pipeline_drift_status(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        drift_status,
        checked,
        mismatched,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let truncated_note = if truncated { " truncated=true" } else { "" };
    let detect_status = if mismatched > 0 {
        "drifted"
    } else if truncated {
        "partial"
    } else {
        "ok"
    };
    println!(
        "Drift Check: Pipeline {} status={detect_status} checked={checked} \
         mismatched={mismatched} repaired={repaired_count} maxRows={max_rows}{truncated_note} \
         (Managed fields auto-repaired; non-Managed Target fields ignored)",
        pipeline.name
    );
    Ok(())
}

async fn ensure_drift_baseline_ready(
    platform_store_url: &str,
    pipeline: &Pipeline,
) -> Result<(), CliError> {
    match pipeline.mode.as_str() {
        "direct" => {
            let bases = list_base_datasets(platform_store_url)
                .await
                .map_err(|err| CliError::Failed(err.to_string()))?;
            let base = bases
                .iter()
                .find(|b| {
                    b.deployment_name == pipeline.deployment_name
                        && b.source_table.eq_ignore_ascii_case(&pipeline.source_table)
                })
                .ok_or_else(|| {
                    CliError::Failed(format!(
                        "no Base Dataset for Pipeline {} source table {}",
                        pipeline.name, pipeline.source_table
                    ))
                })?;
            if base.source_alignment == "unknown" {
                return Err(CliError::Failed(format!(
                    "Drift Check refuses Pipeline {}: Base {} Source Alignment is unknown; \
                     run `migraloop align --table {}` first so Base is a trusted Drift baseline",
                    pipeline.name, base.source_table, base.source_table
                )));
            }
            Ok(())
        }
        "transform" => {
            let derived = list_derived_datasets(platform_store_url)
                .await
                .map_err(|err| CliError::Failed(err.to_string()))?;
            let dataset = derived.iter().find(|d| {
                d.deployment_name == pipeline.deployment_name && d.pipeline_name == pipeline.name
            });
            match dataset {
                Some(d) if !d.status.is_empty() => Ok(()),
                _ => Err(CliError::Failed(format!(
                    "Drift Check refuses Pipeline {}: Derived Dataset not materialized yet",
                    pipeline.name
                ))),
            }
        }
        other => Err(CliError::Failed(format!(
            "unsupported pipeline.mode {other:?} for Drift Check"
        ))),
    }
}

async fn expected_delivery_documents_for_drift(
    platform_store_url: &str,
    pipeline: &Pipeline,
    max_rows: u32,
) -> Result<(Vec<DeliveryDocument>, bool), CliError> {
    let mut documents = match pipeline.mode.as_str() {
        "direct" => {
            let (dataset, rows) = get_base_rows(
                platform_store_url,
                &pipeline.source_table,
                Some(&pipeline.deployment_name),
            )
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
            if dataset.primary_key.is_empty() {
                return Err(CliError::Failed(format!(
                    "Base Dataset {} has no primary key for Drift Check Output Identity",
                    pipeline.source_table
                )));
            }
            // Drift reads the platform expected dataset only — no extra Source load
            // (alignment already established the Base baseline).
            let mut docs = Vec::with_capacity(rows.len());
            for row in &rows {
                docs.push(delivery_document_for_row(
                    &row.data,
                    &dataset.primary_key,
                    &dataset.columns,
                    pipeline,
                )?);
            }
            docs
        }
        "transform" => {
            if pipeline.output_identity.is_empty() {
                return Err(CliError::Failed(format!(
                    "Transform Pipeline {} requires outputIdentity for Drift Check",
                    pipeline.name
                )));
            }
            let (dataset, rows) = get_derived_rows(
                platform_store_url,
                &pipeline.name,
                Some(&pipeline.deployment_name),
            )
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
            let mut docs = Vec::with_capacity(rows.len());
            for row in &rows {
                docs.push(delivery_document_for_row(
                    &row.data,
                    &dataset.output_identity,
                    &dataset.columns,
                    pipeline,
                )?);
            }
            docs
        }
        other => {
            return Err(CliError::Failed(format!(
                "unsupported pipeline.mode {other:?} for Drift Check"
            )));
        }
    };

    documents.sort_by(|a, b| identity_key(&a.identity).cmp(&identity_key(&b.identity)));
    let truncated = documents.len() > max_rows as usize;
    if truncated {
        documents.truncate(max_rows as usize);
    }
    Ok((documents, truncated))
}

/// Compare Managed fields on a Target document to the platform expected map.
/// Non-Managed Target keys are ignored.
fn managed_fields_match_target(
    target_doc: &serde_json::Value,
    expected_managed: &serde_json::Map<String, serde_json::Value>,
    managed_keys: &[&str],
) -> bool {
    for key in managed_keys {
        let expected = expected_managed.get(*key);
        let actual = target_doc.get(*key);
        match (expected, actual) {
            (Some(exp), Some(act)) if json_values_equal_for_drift(exp, act) => {}
            (Some(_), None) | (None, Some(_)) | (Some(_), Some(_)) => return false,
            (None, None) => {}
        }
    }
    true
}

fn json_values_equal_for_drift(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    let left_n = normalize_json_for_drift(left);
    let right_n = normalize_json_for_drift(right);
    left_n == right_n
}

fn normalize_json_for_drift(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(n) = map.get("$numberLong").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<i64>() {
                    return serde_json::Value::Number(parsed.into());
                }
            }
            if let Some(n) = map.get("$numberInt").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<i64>() {
                    return serde_json::Value::Number(parsed.into());
                }
            }
            if let Some(n) = map.get("$numberDouble").and_then(|v| v.as_str()) {
                if let Ok(parsed) = n.parse::<f64>() {
                    if let Some(num) = serde_json::Number::from_f64(parsed) {
                        return serde_json::Value::Number(num);
                    }
                }
            }
            if let Some(n) = map.get("$numberDecimal").and_then(|v| v.as_str()) {
                return serde_json::Value::String(n.to_string());
            }
            if let Some(d) = map.get("$date") {
                return normalize_json_for_drift(d);
            }
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), normalize_json_for_drift(v));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(u) = n.as_u64() {
                serde_json::Value::Number(u.into())
            } else {
                value.clone()
            }
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(normalize_json_for_drift).collect())
        }
        other => other.clone(),
    }
}

async fn print_status(platform_store_url: &str) -> Result<(), CliError> {
    match health(platform_store_url).await {
        PlatformStoreHealth::Healthy { schema_version } => {
            // Reject absurd settings even when migrations are present.
            if let Err(err) = enforce_store_guardrails(platform_store_url).await {
                println!("Platform Store: unhealthy");
                eprintln!("{err}");
                return Err(err);
            }
            println!("Platform Store: healthy");
            println!("Schema version: {schema_version}");
            // Warn-only: free-disk pressure must not flip health or pause Pipelines.
            report_store_resource_warnings(platform_store_url).await?;
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

    let deployments = list_deployments(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    if deployments.is_empty() {
        println!("Deployment: (none)");
    } else {
        for deployment in &deployments {
            println!("Deployment: {}", deployment.name);
            println!("{}", format_system_line("Source", &deployment.source));
            println!("{}", format_system_line("Target", &deployment.target));
            if deployment.source.kind.eq_ignore_ascii_case("oracle") {
                let connect = oracle_source_connect(&deployment.source)?;
                let mechanism = if connect.is_contract_harness() {
                    "LogMiner (contract)"
                } else {
                    "LogMiner (OCI)"
                };
                println!("  Incremental Capture: {mechanism}");
            }
        }
    }

    let pipelines = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if pipelines.is_empty() {
        println!("Pipeline: (none)");
    } else {
        for pipeline in &pipelines {
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

    let bases = list_base_datasets(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if bases.is_empty() {
        println!("Base Dataset: (none)");
    } else {
        for base in &bases {
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
            // Sync Health stays unknown|ok; lag + checkpoint make resume state coherent after restart.
            println!(
                "  Sync Health: {} appliedChanges={} lag={} checkpoint={}",
                base.sync_health,
                base.sync_applied_changes,
                base.sync_lag,
                base.capture_checkpoint
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

    let derived = list_derived_datasets(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if derived.is_empty() {
        println!("Derived Dataset: (none)");
    } else {
        for dataset in &derived {
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

    let quarantines = list_quarantined_changes(platform_store_url, None)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let schema_impacts = list_schema_change_impacts(platform_store_url, None)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    for pipeline in &pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        let pipeline_quarantines: Vec<_> = quarantines
            .iter()
            .filter(|q| {
                q.deployment_name == pipeline.deployment_name && q.pipeline_name == pipeline.name
            })
            .collect();
        let pipeline_schema_blocks: Vec<_> = schema_impacts
            .iter()
            .filter(|s| {
                s.deployment_name == pipeline.deployment_name
                    && s.pipeline_name == pipeline.name
                    && s.impact == "blocking"
            })
            .collect();
        let delivery_health = if pipeline.paused {
            "paused"
        } else if !pipeline_quarantines.is_empty() {
            "unhealthy"
        } else {
            match pipeline.delivery_status.as_str() {
                "delivered" => "ok",
                "pending" => "pending",
                _ => "unknown",
            }
        };
        let status_label = if pipeline.paused {
            format!("{} (paused)", pipeline.delivery_status)
        } else {
            pipeline.delivery_status.clone()
        };
        if pipeline_quarantines.is_empty() {
            println!(
                "  Delivery Health: {} Pipeline={} status={} appliedChanges={} lag={}",
                delivery_health,
                pipeline.name,
                status_label,
                pipeline.delivery_applied_changes,
                pipeline.delivery_lag
            );
        } else {
            println!(
                "  Delivery Health: {} Pipeline={} status={} appliedChanges={} lag={} quarantined={}",
                delivery_health,
                pipeline.name,
                status_label,
                pipeline.delivery_applied_changes,
                pipeline.delivery_lag,
                pipeline_quarantines.len()
            );
        }
        println!(
            "  Drift: {} Pipeline={} checkedRows={} mismatchedRows={}",
            pipeline.drift_status,
            pipeline.name,
            pipeline.drift_checked_rows,
            pipeline.drift_mismatched_rows
        );
        if !pipeline_schema_blocks.is_empty() {
            println!(
                "  Schema Change: Pipeline={} blocking={} paused (stream-wide DDL; not poison quarantine)",
                pipeline.name,
                pipeline_schema_blocks.len()
            );
        }
    }

    if quarantines.is_empty() {
        println!("Quarantine: (none)");
    } else {
        for q in &quarantines {
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
        for s in &schema_impacts {
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
    ensure_store_healthy(platform_store_url).await?;

    let (dataset, rows) = get_base_rows(platform_store_url, table, deployment)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

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
    ensure_store_healthy(platform_store_url).await?;

    let (dataset, rows) = get_derived_rows(platform_store_url, pipeline_name, deployment_name)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

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
    ensure_store_healthy(platform_store_url).await?;

    let pipelines = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let matching: Vec<_> = pipelines
        .into_iter()
        .filter(|p| {
            p.target_collection == collection
                && deployment_name
                    .map(|name| p.deployment_name == name)
                    .unwrap_or(true)
        })
        .collect();

    let pipeline = match matching.as_slice() {
        [] => {
            return Err(CliError::Failed(format!(
                "no Pipeline Target Binding found for collection {collection}"
            )));
        }
        [only] => only.clone(),
        many => {
            return Err(CliError::Failed(format!(
                "multiple Pipelines bind collection {collection} across Deployments {}; \
                 pass --deployment to disambiguate",
                many.iter()
                    .map(|p| p.deployment_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };

    let deployments = list_deployments(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let deployment = deployments
        .into_iter()
        .find(|d| d.name == pipeline.deployment_name)
        .ok_or_else(|| {
            CliError::Failed(format!(
                "Deployment {} not found for Target inspect",
                pipeline.deployment_name
            ))
        })?;

    let mongo = mongo_target_from_deployment(&deployment)?;
    let documents = list_target_documents(&mongo, collection)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

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

async fn resolve_named_pipeline(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<Pipeline, CliError> {
    let pipelines = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let matching: Vec<_> = pipelines
        .into_iter()
        .filter(|p| {
            p.name == pipeline_name
                && deployment_name
                    .map(|name| p.deployment_name == name)
                    .unwrap_or(true)
        })
        .collect();
    match matching.as_slice() {
        [] => Err(CliError::Failed(format!(
            "Pipeline {pipeline_name} not found{}",
            deployment_name
                .map(|d| format!(" in Deployment {d}"))
                .unwrap_or_default()
        ))),
        [only] => Ok(only.clone()),
        many => Err(CliError::Failed(format!(
            "multiple Pipelines named {pipeline_name} across Deployments {}; \
             pass --deployment to disambiguate",
            many.iter()
                .map(|p| p.deployment_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

async fn pause_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;
    let pipeline =
        resolve_named_pipeline(platform_store_url, pipeline_name, deployment_name).await?;
    if pipeline.paused {
        println!(
            "Pipeline {} already paused (Deployment {})",
            pipeline.name, pipeline.deployment_name
        );
        return Ok(());
    }
    set_pipeline_paused(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        true,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;
    println!(
        "Pipeline {} paused (Deployment {}) — Delivery/processing stopped; \
         durable Base/checkpoint state retained",
        pipeline.name, pipeline.deployment_name
    );
    Ok(())
}

async fn resume_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;
    let pipeline =
        resolve_named_pipeline(platform_store_url, pipeline_name, deployment_name).await?;
    if !pipeline.paused {
        println!(
            "Pipeline {} is not paused (Deployment {})",
            pipeline.name, pipeline.deployment_name
        );
        return Ok(());
    }

    set_pipeline_paused(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        false,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;
    clear_schema_change_impacts(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let deployments = list_deployments(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let deployment = deployments
        .into_iter()
        .find(|d| d.name == pipeline.deployment_name)
        .ok_or_else(|| {
            CliError::Failed(format!(
                "Deployment {} not found for Pipeline resume",
                pipeline.deployment_name
            ))
        })?;

    // Catch up Delivery from durable Base/Derived state accumulated while paused.
    if pipeline_has_target(&pipeline) {
        let store = PlatformStore::open(platform_store_url)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
        let mongo = mongo_target_from_deployment(&deployment)?;
        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline_with_options(&store, &deployment, &pipeline, &mongo, true)
                    .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    &store,
                    &deployment,
                    &pipeline,
                    &mongo,
                    true,
                )
                .await?;
            }
            other => {
                return Err(CliError::Failed(format!(
                    "unsupported pipeline.mode {other:?} for resume catch-up Delivery"
                )));
            }
        }
    }

    println!(
        "Pipeline {} resumed (Deployment {}) — Delivery continues from durable state",
        pipeline.name, pipeline.deployment_name
    );
    Ok(())
}

async fn remove_pipeline_command(
    platform_store_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;
    let pipeline =
        resolve_named_pipeline(platform_store_url, pipeline_name, deployment_name).await?;

    delete_pipeline(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    // Keep Shared Bases still referenced by remaining Pipelines; prune only tables
    // no longer referenced (same capture-scope rule as apply — ADR-0019 / ADR-0007).
    let remaining = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?
        .into_iter()
        .filter(|p| p.deployment_name == pipeline.deployment_name)
        .collect::<Vec<_>>();
    let mut keep = BTreeSet::new();
    for remaining_pipeline in &remaining {
        for (schema, table) in pipeline_base_table_refs(remaining_pipeline) {
            keep.insert((schema, table));
        }
    }
    let keep_tables: Vec<(String, String)> = keep.into_iter().collect();
    delete_base_datasets_not_in(platform_store_url, &pipeline.deployment_name, &keep_tables)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    println!(
        "Pipeline {} removed (Deployment {}) — Delivery/processing stopped; \
         Shared Base Datasets kept when still referenced",
        pipeline.name, pipeline.deployment_name
    );
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
            let sync_url = platform_store_url.clone();
            tokio::spawn(async move {
                migraloop_runtime::supervise_continuous_incremental_sync(sync_url).await;
            });
            observability::serve_prometheus_metrics(addr, platform_store_url).await
        }
        Command::Lab { command } => run_lab(command).await,
    }
}
