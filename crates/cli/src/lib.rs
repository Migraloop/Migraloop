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
use migraloop_capture::{
    alignment_check_read_for_source, check_oracle_source_prerequisites, classify_number,
    classify_schema_impact, discover_source_schema, initial_load_chunk_for_source,
    is_allow_listed_oracle_type, load_injected_schema_changes, normalize_change_temporals,
    open_oracle_incremental_capture, AlignmentCheckSample, CapturePosition, ChangeEvent,
    ChangeOp, IncrementalCapture, InitialLoadChunkOptions, NumberMongoMapping,
    OracleSourceConnect, OracleTlsSettings, PipelineSchemaDeps, SchemaChangeEvent,
    SchemaImpact, SourceColumn, TypeError,
};
use migraloop_delivery::{
    delete_documents_by_identity, list_target_documents, upsert_managed_documents, DeliveryColumn,
    DeliveryDocument, ManagedFieldAs, MongoTargetConnection, MongoTlsSettings,
};
use migraloop_platform_store::{
    append_base_dataset_chunk, base_dataset_exists, check_store_settings,
    clear_schema_change_impacts, delete_base_datasets_not_in, delete_maintenance_state,
    delete_pipeline, disk_warn_message, filter_unapplied_change_ids, get_base_rows,
    get_derived_rows, get_maintenance_state_json, health, list_base_datasets, list_deployments,
    list_derived_datasets, list_pipelines, list_quarantined_changes, list_schema_change_impacts,
    migrate, probe_store_resources, probe_store_settings, record_applied_source_changes,
    replace_base_dataset, replace_derived_dataset, replace_maintenance_state, replace_pipelines,
    set_pipeline_paused, update_base_primary_key, update_pipeline_delivery_lag,
    update_pipeline_delivery_progress, update_pipeline_delivery_progress_with_lag,
    update_pipeline_drift_status, upsert_deployment, upsert_quarantined_change,
    upsert_schema_change_impact, BaseColumn, BaseDataset, Deployment, DerivedDataset,
    FieldMappingAs, OmittedColumn, Pipeline, PlatformStoreHealth, QuarantinedChange,
    SchemaChangeImpact, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};
use migraloop_transform::{
    analyze_affect_on_base_with_bases, analyze_affect_with_maintenance, build_maintenance_state,
    derived_output_field_names, evaluate_transform_with_bases,
    evaluate_transform_for_identities_with_bases, identity_matches_row, maintain_state_for_change,
    parse_transform_steps, requires_maintenance_state, secondary_base_refs, used_base_fields,
    AffectOutcome, BaseChangeKind, MaintenanceState, TransformOp,
};
use thiserror::Error;

use crate::config::{
    load_deployment_config, resolve_tls_settings, DeploymentDocument, PipelineSpec,
    ResolvedSecretRef,
};
use crate::observability::{emit_event, EventValue};

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Failed(String),
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
    /// Run Incremental Capture into Base Datasets, then Delivery for Direct Pipelines
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
    /// Run the app: migrate on startup, expose Observability metrics, keep alive
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
    enforce_store_guardrails(platform_store_url).await?;
    migrate(platform_store_url)
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
                ("warn_threshold_bytes", EventValue::from(migraloop_platform_store::DISK_FREE_WARN_BYTES as i64)),
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
                crate::config::FieldMappingAsSpec::String => FieldMappingAs::String,
                crate::config::FieldMappingAsSpec::Omit => FieldMappingAs::Omit,
            };
            (name.clone(), mapping)
        })
        .collect();
    let output_identity = pipeline.output_identity.clone().unwrap_or_default();
    let transform_json = pipeline.transform.as_ref().map(|steps| {
        serde_json::Value::Array(steps.clone())
    });
    Pipeline {
        deployment_name: deployment_name.to_string(),
        name: pipeline.name.clone(),
        mode: pipeline.mode.clone(),
        source_table: pipeline.source.table.clone(),
        source_schema: pipeline
            .source
            .schema
            .clone()
            .unwrap_or_default(),
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

fn resolve_secret_value(reference: &SecretRef, field: &str) -> Result<String, CliError> {
    match reference.kind {
        SecretRefKind::Env => std::env::var(&reference.value).map_err(|_| {
            CliError::Failed(format!(
                "unresolvable secret reference: {field} fromEnv {} is missing",
                reference.value
            ))
        }),
        SecretRefKind::File => {
            let contents = std::fs::read_to_string(&reference.value).map_err(|err| {
                CliError::Failed(format!(
                    "unresolvable secret reference: {field} {}: {err}",
                    reference.value
                ))
            })?;
            let trimmed = contents.trim_end_matches(['\n', '\r']).to_string();
            if trimmed.is_empty() {
                return Err(CliError::Failed(format!(
                    "unresolvable secret reference: {field} {} is empty",
                    reference.value
                )));
            }
            Ok(trimmed)
        }
    }
}

fn output_identity_from_row(
    row: &serde_json::Map<String, serde_json::Value>,
    identity_fields: &[String],
) -> Result<serde_json::Value, CliError> {
    if identity_fields.is_empty() {
        return Err(CliError::Failed(
            "Output Identity fields are empty".to_string(),
        ));
    }
    if identity_fields.len() == 1 {
        let key = &identity_fields[0];
        return row.get(key).cloned().ok_or_else(|| {
            CliError::Failed(format!(
                "row missing Output Identity column {key}"
            ))
        });
    }
    let mut identity = serde_json::Map::new();
    for key in identity_fields {
        let value = row.get(key).cloned().ok_or_else(|| {
            CliError::Failed(format!(
                "row missing Output Identity column {key}"
            ))
        })?;
        identity.insert(key.clone(), value);
    }
    Ok(serde_json::Value::Object(identity))
}

fn transform_ops_from_pipeline(pipeline: &Pipeline) -> Result<Vec<TransformOp>, CliError> {
    let Some(value) = &pipeline.transform_json else {
        return Err(CliError::Failed(format!(
            "Transform Pipeline {} is missing transform definition",
            pipeline.name
        )));
    };
    let steps = value.as_array().ok_or_else(|| {
        CliError::Failed(format!(
            "Transform Pipeline {} transform must be an array of operators",
            pipeline.name
        ))
    })?;
    parse_transform_steps(steps).map_err(|err| {
        CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name))
    })
}

/// Base Dataset (schema, table) pairs a Pipeline references — primary `source.table`
/// plus every `equiLookup.from` / `union.from` secondary Base.
fn pipeline_base_table_refs(pipeline: &Pipeline) -> Vec<(String, String)> {
    let mut tables = BTreeSet::new();
    if !pipeline.source_table.is_empty() {
        tables.insert((pipeline.source_schema.clone(), pipeline.source_table.clone()));
    }
    if pipeline.mode == "transform" {
        if let Ok(ops) = transform_ops_from_pipeline(pipeline) {
            for sec in secondary_base_refs(&ops) {
                let schema = sec
                    .schema
                    .unwrap_or_else(|| pipeline.source_schema.clone());
                tables.insert((schema, sec.table));
            }
        }
    }
    tables.into_iter().collect()
}

fn pipeline_references_table(pipeline: &Pipeline, table: &str) -> bool {
    pipeline_base_table_refs(pipeline)
        .iter()
        .any(|(_, t)| t.eq_ignore_ascii_case(table))
}

/// Load secondary Base rows for all `equiLookup.from` / `union.from` tables on this Pipeline.
/// Secondary Base rows plus column metadata (for unwind-flattened foreign fields).
async fn load_secondary_bases_and_columns_for_pipeline(
    platform_store_url: &str,
    pipeline: &Pipeline,
    ops: &[TransformOp],
) -> Result<
    (
        BTreeMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
        Vec<BaseColumn>,
    ),
    CliError,
> {
    let mut secondary = BTreeMap::new();
    let mut columns = Vec::new();
    let mut seen_cols = BTreeSet::new();
    for sec in secondary_base_refs(ops) {
        let (base, rows) = get_base_rows(
            platform_store_url,
            &sec.table,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Transform Pipeline {}: secondary Base Dataset `{}` (equiLookup/union.from) unavailable: {err}",
                pipeline.name, sec.table
            ))
        })?;
        for col in base.columns {
            if seen_cols.insert(col.name.clone()) {
                columns.push(col);
            }
        }
        secondary.insert(
            sec.table,
            rows.into_iter().map(|r| r.data).collect(),
        );
    }
    Ok((secondary, columns))
}

fn mongo_target_from_deployment(deployment: &Deployment) -> Result<MongoTargetConnection, CliError> {
    if deployment.target.port <= 0 || deployment.target.port > u16::MAX as i32 {
        return Err(CliError::Failed(
            "target.port must be a valid TCP port".to_string(),
        ));
    }
    let password = resolve_secret_value(&deployment.target.password_ref, "target.password")?;
    Ok(MongoTargetConnection {
        host: deployment.target.host.clone(),
        port: deployment.target.port as u16,
        database: deployment.target.database.clone(),
        username: deployment.target.username.clone(),
        password,
        tls: mongo_tls_from_settings(&deployment.target.tls),
    })
}

fn mongo_tls_from_settings(tls: &TlsSettings) -> MongoTlsSettings {
    MongoTlsSettings {
        enabled: tls.enabled,
        ca_file: tls.ca_file.clone(),
        insecure_skip_verify: tls.insecure_skip_verify,
    }
}

fn oracle_tls_from_settings(tls: &TlsSettings) -> OracleTlsSettings {
    OracleTlsSettings {
        enabled: tls.enabled,
        wallet_location: tls.wallet_location.clone(),
        insecure_skip_verify: tls.insecure_skip_verify,
    }
}

fn secret_ref_from_resolved(resolved: ResolvedSecretRef) -> SecretRef {
    match resolved {
        ResolvedSecretRef::Env(name) => SecretRef {
            kind: SecretRefKind::Env,
            value: name,
        },
        ResolvedSecretRef::File(path) => SecretRef {
            kind: SecretRefKind::File,
            value: path.display().to_string(),
        },
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

fn source_timezone_opt(deployment: &Deployment) -> Option<&str> {
    let tz = deployment.source.timezone.trim();
    if tz.is_empty() {
        None
    } else {
        Some(tz)
    }
}

fn base_columns_from_source(columns: &[&SourceColumn]) -> Vec<BaseColumn> {
    columns
        .iter()
        .map(|c| BaseColumn {
            name: c.name.clone(),
            oracle_type: c.oracle_type.clone(),
            precision: c.precision,
            scale: c.scale,
        })
        .collect()
}

fn delivery_columns_from_base(columns: &[BaseColumn]) -> Vec<DeliveryColumn> {
    columns
        .iter()
        .map(|c| DeliveryColumn {
            name: c.name.clone(),
            oracle_type: c.oracle_type.clone(),
            precision: c.precision,
            scale: c.scale,
        })
        .collect()
}

fn managed_field_as_map(
    pipeline: &Pipeline,
) -> std::collections::BTreeMap<String, ManagedFieldAs> {
    pipeline
        .field_mappings
        .iter()
        .map(|(k, v)| {
            let mapped = match v {
                FieldMappingAs::String => ManagedFieldAs::String,
                FieldMappingAs::Omit => ManagedFieldAs::Omit,
            };
            (k.clone(), mapped)
        })
        .collect()
}

fn apply_field_mappings_to_row(
    row: &serde_json::Map<String, serde_json::Value>,
    pipeline: &Pipeline,
) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for (key, value) in row {
        match pipeline.field_mappings.get(key) {
            Some(FieldMappingAs::Omit) => continue,
            Some(FieldMappingAs::String) => {
                let as_string = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    other => other.to_string(),
                };
                out.insert(key.clone(), serde_json::Value::String(as_string));
            }
            None => {
                out.insert(key.clone(), value.clone());
            }
        }
    }
    out
}

/// Apply-time validation for Managed/transform inputs (ADR-0018 / ADR-0023).
fn validate_pipeline_managed_fields(
    pipeline: &Pipeline,
    source_columns: &[SourceColumn],
    managed_column_names: &BTreeSet<String>,
) -> Result<(), CliError> {
    let by_name: std::collections::BTreeMap<&str, &SourceColumn> = source_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    for (field, mapping) in &pipeline.field_mappings {
        match by_name.get(field.as_str()) {
            None => {
                return Err(CliError::Failed(format!(
                    "Pipeline {} fields.{} references unknown Source column",
                    pipeline.name, field
                )));
            }
            Some(col) if !col.supported || !is_allow_listed_oracle_type(&col.oracle_type, col.size) => {
                if *mapping != FieldMappingAs::Omit {
                    return Err(CliError::Failed(format!(
                        "Pipeline {}: {} (column {field})",
                        pipeline.name,
                        TypeError::UnsupportedAsManaged {
                            oracle_type: col.oracle_type.clone(),
                        }
                    )));
                }
            }
            Some(_) => {}
        }
    }

    for name in managed_column_names {
        let Some(col) = by_name.get(name.as_str()) else {
            continue;
        };
        if !col.is_number() {
            continue;
        }
        if classify_number(col.precision, col.scale) != NumberMongoMapping::Unsafe {
            continue;
        }
        match pipeline.field_mappings.get(name) {
            Some(FieldMappingAs::String) | Some(FieldMappingAs::Omit) => {}
            None => {
                return Err(CliError::Failed(format!(
                    "NUMBER column {name} has unsafe declared precision/scale \
                     (precision={:?}, scale={:?}); Pipeline {} cannot apply until \
                     fields.{name}.as is string or omit (ADR-0023); never default IEEE double",
                    col.precision, col.scale, pipeline.name
                )));
            }
        }
    }
    Ok(())
}

fn delivery_document_for_row(
    row: &serde_json::Map<String, serde_json::Value>,
    identity_fields: &[String],
    columns: &[BaseColumn],
    pipeline: &Pipeline,
) -> Result<DeliveryDocument, CliError> {
    let managed = apply_field_mappings_to_row(row, pipeline);
    let identity = output_identity_from_row(&managed, identity_fields).or_else(|_| {
        // Identity may be omitted from Managed via field mapping; fall back to full row.
        output_identity_from_row(row, identity_fields)
    })?;
    Ok(DeliveryDocument {
        identity,
        managed_fields: managed,
        columns: delivery_columns_from_base(columns),
        field_as: managed_field_as_map(pipeline),
    })
}

async fn ensure_store_healthy(platform_store_url: &str) -> Result<(), CliError> {
    match health(platform_store_url).await {
        PlatformStoreHealth::Healthy { .. } => {
            // Settings guardrails reject absurd under-provisioning; disk warn is
            // intentionally not a hard failure here (ADR-0010 warn-only).
            enforce_store_guardrails(platform_store_url).await?;
            report_store_resource_warnings(platform_store_url).await?;
            Ok(())
        }
        PlatformStoreHealth::Unhealthy { reason } => Err(CliError::Failed(format!(
            "Platform Store is not healthy; run `migraloop migrate` first: {reason}"
        ))),
        PlatformStoreHealth::Unreachable { reason } => Err(CliError::Failed(format!(
            "Platform Store is unreachable: {reason}"
        ))),
    }
}

async fn sync_base_datasets_for_pipelines(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[Pipeline],
) -> Result<(), CliError> {
    let deployment_name = &deployment.name;
    let configured_tz = source_timezone_opt(deployment);
    let mut tables = BTreeSet::new();
    for pipeline in pipelines {
        for (schema, table) in pipeline_base_table_refs(pipeline) {
            tables.insert((schema, table));
        }
    }
    let keep: Vec<(String, String)> = tables.iter().cloned().collect();

    // Capture scope follows Pipeline references: drop Bases for tables no longer referenced.
    delete_base_datasets_not_in(platform_store_url, deployment_name, &keep)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    for (schema, table) in tables {
        let existing = if base_dataset_exists(platform_store_url, deployment_name, &schema, &table)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?
        {
            let (dataset, _) =
                get_base_rows(platform_store_url, &table, Some(deployment_name))
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
            Some(dataset)
        } else {
            None
        };

        if let Some(ref dataset) = existing {
            let resumable = dataset.status == "initial_load_in_progress"
                || dataset.status == "initial_load_paused";
            if !resumable {
                // Existing Bases stay; do not reload on Pipeline re-apply (ADR-0019).
                ensure_base_primary_key(
                    platform_store_url,
                    deployment,
                    &schema,
                    &table,
                    configured_tz,
                )
                .await?;
                continue;
            }
        }

        run_chunked_initial_load(
            platform_store_url,
            deployment,
            pipelines,
            &schema,
            &table,
            configured_tz,
            existing.as_ref(),
        )
        .await?;
    }

    Ok(())
}

/// Default Initial Load Source read window (issue #124). Override via
/// `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` (must be > 0).
fn initial_load_chunk_size() -> usize {
    std::env::var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(1000)
}

/// Optional Operator throttle for Initial Load (rows/sec). `0` / unset = no artificial cap.
fn initial_load_rows_per_sec() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

/// Test/Lab inject: pause Initial Load after N successful chunks.
fn initial_load_pause_after_chunks() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

/// Test/Lab inject: artificial Platform Store / Downstream pressure during Initial Load.
fn initial_load_store_delay_ms() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

async fn run_chunked_initial_load(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[Pipeline],
    schema: &str,
    table: &str,
    configured_tz: Option<&str>,
    existing: Option<&BaseDataset>,
) -> Result<(), CliError> {
    let deployment_name = &deployment.name;
    let connect = oracle_source_connect(&deployment.source)?;
    let password = resolve_secret_value(&deployment.source.password_ref, "source.password")?;
    let chunk_size = initial_load_chunk_size();
    let rate_limit = initial_load_rows_per_sec();
    let pause_after = initial_load_pause_after_chunks();
    let store_delay = initial_load_store_delay_ms();

    let mut offset = existing.map(|d| d.row_count.max(0) as usize).unwrap_or(0);
    let mut established = existing
        .and_then(|d| d.capture_low_watermark)
        .and_then(CapturePosition::from_i64);
    let mut chunks_done: u64 = 0;
    let mut primary_key = existing.map(|d| d.primary_key.clone()).unwrap_or_default();
    let mut columns = existing.map(|d| d.columns.clone()).unwrap_or_default();
    let mut omitted_columns = existing
        .map(|d| d.omitted_columns.clone())
        .unwrap_or_default();
    let mut supported_names: BTreeSet<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut low_watermark = established;

    loop {
        // Honor durable Pipeline pause between chunks (Operator `migraloop pause`).
        if initial_load_should_pause(platform_store_url, deployment_name, table, pipelines).await? {
            persist_initial_load_pause(
                platform_store_url,
                deployment_name,
                schema,
                table,
                &primary_key,
                &columns,
                &omitted_columns,
                offset,
                low_watermark,
                existing.and_then(|d| d.initial_load_cursor.clone()),
            )
            .await?;
            return Ok(());
        }

        let source_started = std::time::Instant::now();
        let chunk = initial_load_chunk_for_source(
            &connect,
            &password,
            schema,
            table,
            configured_tz,
            &InitialLoadChunkOptions {
                chunk_size,
                offset,
                established_watermark: established,
            },
        )
        .map_err(|err| CliError::Failed(err.to_string()))?;
        let source_ms = source_started.elapsed().as_millis() as u64;

        if primary_key.is_empty() {
            primary_key = chunk.primary_key.clone();
        }
        if columns.is_empty() {
            let supported = chunk
                .columns
                .iter()
                .filter(|c| c.supported)
                .collect::<Vec<_>>();
            columns = base_columns_from_source(&supported);
            omitted_columns = chunk
                .columns
                .iter()
                .filter(|c| !c.supported)
                .map(|c| OmittedColumn {
                    name: c.name.clone(),
                    oracle_type: c.oracle_type.clone(),
                })
                .collect();
            supported_names = columns.iter().map(|c| c.name.clone()).collect();
        }

        low_watermark = Some(chunk.low_watermark);
        established = Some(chunk.low_watermark);

        let rows: Vec<serde_json::Map<String, serde_json::Value>> = chunk
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .filter(|(name, _)| supported_names.contains(name))
                    .collect()
            })
            .collect();

        let start_ordinal = offset as i32;
        offset = offset.saturating_add(rows.len());
        chunks_done = chunks_done.saturating_add(1);

        let status = if chunk.exhausted {
            "initial_load_complete"
        } else {
            "initial_load_in_progress"
        };
        let cursor = if chunk.exhausted {
            None
        } else {
            chunk.cursor_pk.clone()
        };
        let wm = chunk.low_watermark;
        let dataset = BaseDataset {
            deployment_name: deployment_name.to_string(),
            source_table: table.to_string(),
            source_schema: schema.to_string(),
            status: status.to_string(),
            primary_key: primary_key.clone(),
            columns: columns.clone(),
            omitted_columns: omitted_columns.clone(),
            row_count: offset as i32,
            sync_applied_changes: 0,
            sync_health: "unknown".to_string(),
            capture_low_watermark: Some(wm.as_i64()),
            // Checkpoint starts at watermark-1 so first Incremental includes the overlap window
            // via exclusive resume (checkpoint+1 == low-watermark).
            capture_checkpoint: Some(wm.as_i64().saturating_sub(1)),
            sync_lag: 0,
            source_alignment: "unknown".to_string(),
            source_alignment_checked_rows: 0,
            source_alignment_mismatched_rows: 0,
            initial_load_cursor: cursor.clone(),
        };

        let persist_started = std::time::Instant::now();
        if let Some(ms) = store_delay {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        append_base_dataset_chunk(platform_store_url, &dataset, &rows, start_ordinal)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
        let persist_ms = persist_started.elapsed().as_millis() as u64;

        let rate_note = rate_limit
            .map(|r| format!(" rate_limit={r}/s"))
            .unwrap_or_default();
        println!(
            "Initial Load progress: {table} chunk={chunks_done} rows={offset} \
             chunk_size={chunk_size}{rate_note} low-watermark={wm}"
        );
        let mut progress_fields = vec![
            ("table", EventValue::from(table)),
            ("chunk", EventValue::from(chunks_done as i64)),
            ("rows", EventValue::from(offset as i64)),
            ("chunk_size", EventValue::from(chunk_size)),
            ("low_watermark", EventValue::from(wm.as_i64())),
            ("deployment", EventValue::from(deployment_name.as_str())),
        ];
        if let Some(rate) = rate_limit {
            progress_fields.push(("rate_limit_rows_per_sec", EventValue::from(rate as i64)));
        }
        emit_event("initial_load_progress", &progress_fields);

        // Back off when Downstream/store or Source pressure is visible (issue #124).
        let pressure_ms = store_delay.unwrap_or(0).max(persist_ms).max(source_ms);
        if store_delay.is_some() || persist_ms >= 25 || source_ms >= 25 {
            let backoff_ms = pressure_ms.max(10);
            let pressure = if store_delay.is_some() || persist_ms >= source_ms {
                "Downstream/store"
            } else {
                "Source"
            };
            println!(
                "Initial Load backoff: {table} delay_ms={backoff_ms} \
                 ({pressure} pressure; chunk window stays bounded)"
            );
            emit_event(
                "initial_load_backoff",
                &[
                    ("table", EventValue::from(table)),
                    ("delay_ms", EventValue::from(backoff_ms as i64)),
                    ("chunk_size", EventValue::from(chunk_size)),
                    ("pressure", EventValue::from(pressure)),
                    ("deployment", EventValue::from(deployment_name.as_str())),
                ],
            );
            if store_delay.is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms.min(250))).await;
            }
        }

        if let Some(rate) = rate_limit {
            if !rows.is_empty() {
                let sleep_ms = (rows.len() as u128)
                    .saturating_mul(1000)
                    .saturating_div(rate as u128) as u64;
                if sleep_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                }
            }
        }

        if chunk.exhausted {
            println!(
                "Initial Load complete: Base Dataset {table} ({} rows) low-watermark={wm}",
                offset
            );
            emit_event(
                "initial_load_complete",
                &[
                    ("table", EventValue::from(table)),
                    ("rows", EventValue::from(offset as i64)),
                    ("low_watermark", EventValue::from(wm.as_i64())),
                    ("deployment", EventValue::from(deployment_name.as_str())),
                    ("chunk_size", EventValue::from(chunk_size)),
                ],
            );
            return Ok(());
        }

        if pause_after.is_some_and(|n| chunks_done >= n) {
            persist_initial_load_pause(
                platform_store_url,
                deployment_name,
                schema,
                table,
                &primary_key,
                &columns,
                &omitted_columns,
                offset,
                Some(wm),
                cursor,
            )
            .await?;
            return Ok(());
        }
    }
}

async fn initial_load_should_pause(
    platform_store_url: &str,
    deployment_name: &str,
    table: &str,
    pipelines: &[Pipeline],
) -> Result<bool, CliError> {
    let live = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    for pipeline in pipelines {
        if !pipeline_references_table(pipeline, table)
            || (pipeline.deployment_name != deployment_name && !pipeline.deployment_name.is_empty())
        {
            // `pipelines` arg may still have deployment_name unset before persist; match by name.
            if !pipeline_references_table(pipeline, table) {
                continue;
            }
        }
        if let Some(stored) = live
            .iter()
            .find(|p| p.deployment_name == deployment_name && p.name == pipeline.name)
        {
            if stored.paused {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn persist_initial_load_pause(
    platform_store_url: &str,
    deployment_name: &str,
    schema: &str,
    table: &str,
    primary_key: &[String],
    columns: &[BaseColumn],
    omitted_columns: &[OmittedColumn],
    rows_loaded: usize,
    low_watermark: Option<CapturePosition>,
    cursor: Option<Vec<serde_json::Value>>,
) -> Result<(), CliError> {
    let wm = low_watermark.map(|w| w.as_i64());
    let dataset = BaseDataset {
        deployment_name: deployment_name.to_string(),
        source_table: table.to_string(),
        source_schema: schema.to_string(),
        status: "initial_load_paused".to_string(),
        primary_key: primary_key.to_vec(),
        columns: columns.to_vec(),
        omitted_columns: omitted_columns.to_vec(),
        row_count: rows_loaded as i32,
        sync_applied_changes: 0,
        sync_health: "unknown".to_string(),
        capture_low_watermark: wm,
        capture_checkpoint: wm.map(|w| w.saturating_sub(1)),
        sync_lag: 0,
        source_alignment: "unknown".to_string(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: cursor,
    };
    append_base_dataset_chunk(platform_store_url, &dataset, &[], rows_loaded as i32)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    println!(
        "Initial Load paused: Base Dataset {table} ({} rows) — durable progress retained; \
         re-run `migraloop apply` (or resume + apply) to continue without tearing down the Deployment",
        rows_loaded
    );
    emit_event(
        "initial_load_paused",
        &[
            ("table", EventValue::from(table)),
            ("rows", EventValue::from(rows_loaded as i64)),
            (
                "low_watermark",
                EventValue::from(wm.unwrap_or(0)),
            ),
            ("deployment", EventValue::from(deployment_name)),
        ],
    );
    Ok(())
}

async fn ensure_base_primary_key(
    platform_store_url: &str,
    deployment: &Deployment,
    source_schema: &str,
    source_table: &str,
    configured_timezone: Option<&str>,
) -> Result<(), CliError> {
    let deployment_name = &deployment.name;
    let (dataset, _) = get_base_rows(platform_store_url, source_table, Some(deployment_name))
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if !dataset.primary_key.is_empty() {
        return Ok(());
    }

    // Metadata-only: one bounded chunk for PK — never a full-table Initial Load slam.
    let connect = oracle_source_connect(&deployment.source)?;
    let password = resolve_secret_value(&deployment.source.password_ref, "source.password")?;
    let chunk = initial_load_chunk_for_source(
        &connect,
        &password,
        source_schema,
        source_table,
        configured_timezone,
        &InitialLoadChunkOptions {
            chunk_size: 1,
            offset: 0,
            established_watermark: None,
        },
    )
    .map_err(|err| CliError::Failed(err.to_string()))?;
    if chunk.primary_key.is_empty() {
        return Err(CliError::Failed(format!(
            "Source table {source_table} has no primary key for Output Identity"
        )));
    }

    update_base_primary_key(
        platform_store_url,
        deployment_name,
        source_schema,
        source_table,
        &chunk.primary_key,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;
    Ok(())
}

/// Load Source schema metadata for apply-time Managed field validation.
///
/// Real Oracle hosts use OCI discovery; contract/stub hosts use the contract catalog.
fn source_columns_for_pipeline(
    deployment: &Deployment,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, CliError> {
    let connect = oracle_source_connect(&deployment.source)?;
    let password = resolve_secret_value(&deployment.source.password_ref, "source.password")?;
    discover_source_schema(&connect, &password, schema, table)
        .map_err(|err| CliError::Failed(err.to_string()))
}

fn oracle_source_connect(source: &SystemConnection) -> Result<OracleSourceConnect, CliError> {
    if source.port <= 0 || source.port > u16::MAX as i32 {
        return Err(CliError::Failed(
            "source.port must be a valid TCP port".to_string(),
        ));
    }
    Ok(OracleSourceConnect {
        host: source.host.clone(),
        port: source.port as u16,
        database: source.database.clone(),
        username: source.username.clone(),
        tls: oracle_tls_from_settings(&source.tls),
    })
}

/// Open LogMiner-backed Incremental Capture for a Deployment Source System.
fn open_deployment_incremental_capture(
    source: &SystemConnection,
) -> Result<IncrementalCapture, CliError> {
    let connect = oracle_source_connect(source)?;
    let password = resolve_secret_value(&source.password_ref, "source.password")?;
    open_oracle_incremental_capture(&connect, &password)
        .map_err(|err| CliError::Failed(err.to_string()))
}

/// Fail-fast Oracle Source Prerequisites before capture runs (ADR-0021).
///
/// Probes via the same LogMiner capture backend the Sync path uses (contract or
/// OCI). Read-only; never auto-alters customer Oracle config.
fn ensure_oracle_source_prerequisites(
    source: &SystemConnection,
    source_tables: &[String],
) -> Result<(), CliError> {
    let capture = open_deployment_incremental_capture(source)?;
    let state = capture
        .probe_prerequisites()
        .map_err(|err| CliError::Failed(err.to_string()))?;
    check_oracle_source_prerequisites(&state, source_tables)
        .map_err(|err| CliError::Failed(err.to_string()))
}

fn pipeline_source_tables(pipelines: &[Pipeline]) -> Vec<String> {
    let mut tables = BTreeSet::new();
    for pipeline in pipelines {
        for (_, table) in pipeline_base_table_refs(pipeline) {
            if !table.is_empty() {
                tables.insert(table);
            }
        }
    }
    tables.into_iter().collect()
}

/// Whether a Pipeline has a Target Binding configured for Delivery.
fn pipeline_has_target(pipeline: &Pipeline) -> bool {
    (pipeline.mode == "direct" || pipeline.mode == "transform")
        && !pipeline.target_collection.is_empty()
}

/// Whether two Pipeline declarations are semantically the same (mode, Source table,
/// Target Binding, field mappings, transform / Output Identity) — excluding metadata
/// such as `description`. A semantic difference is a Pipeline revision/Change.
///
/// Used so runtime Pipeline add can preserve Delivery progress for unchanged Pipelines
/// (ADR-0007) without treating a declaration change as a no-op add.
fn pipeline_semantic_unchanged(previous: &Pipeline, next: &Pipeline) -> bool {
    previous.mode == next.mode
        && previous.source_table == next.source_table
        && previous.source_schema == next.source_schema
        && previous.target_collection == next.target_collection
        && previous.field_mappings == next.field_mappings
        && previous.output_identity == next.output_identity
        && previous.transform_json == next.transform_json
}

fn pipeline_metadata_only_change(previous: &Pipeline, next: &Pipeline) -> bool {
    pipeline_semantic_unchanged(previous, next) && previous.description != next.description
}

/// Preserve Delivery progress and pause for Pipelines whose semantic declaration is
/// unchanged (including metadata-only description edits).
///
/// `pipelines_from_document` always starts at pending/0; without this merge, every
/// apply would look like a Deployment restart for already-running Pipelines.
fn preserve_unchanged_pipeline_delivery(existing: &[Pipeline], pipelines: &mut [Pipeline]) {
    for pipeline in pipelines.iter_mut() {
        let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
            continue;
        };
        if pipeline_semantic_unchanged(previous, pipeline) {
            pipeline.delivery_status = previous.delivery_status.clone();
            pipeline.delivery_applied_changes = previous.delivery_applied_changes;
            pipeline.delivery_lag = previous.delivery_lag;
            pipeline.paused = previous.paused;
            pipeline.drift_status = previous.drift_status.clone();
            pipeline.drift_checked_rows = previous.drift_checked_rows;
            pipeline.drift_mismatched_rows = previous.drift_mismatched_rows;
        }
    }
}

/// Pipelines that need ordinary Delivery start: newly added, or semantically
/// unchanged but not yet delivered. Semantic revisions use the revision path.
fn pipelines_needing_delivery_start<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            if !pipeline_has_target(pipeline) || pipeline.paused {
                return false;
            }
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                // Newly added Pipeline — start Delivery after Initial Load as needed.
                return true;
            };
            if !pipeline_semantic_unchanged(previous, pipeline) {
                // Semantic revision — handled by pause → rebuild → re-Deliver.
                return false;
            }
            // Unchanged, already-delivered Pipelines keep running without re-Delivery.
            previous.delivery_status != "delivered"
        })
        .collect()
}

/// Existing Pipelines whose semantic declaration changed (revision rebuild path).
fn pipelines_needing_revision_rebuild<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            if !pipeline_has_target(pipeline) {
                return false;
            }
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                return false;
            };
            !pipeline_semantic_unchanged(previous, pipeline)
        })
        .collect()
}

fn pipelines_with_metadata_only_change<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                return false;
            };
            pipeline_metadata_only_change(previous, pipeline)
        })
        .collect()
}

async fn deliver_pipelines(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
) -> Result<(), CliError> {
    deliver_pipelines_with_options(platform_store_url, deployment, pipelines, false, false).await
}

/// Deliver Pipelines. `reconcile_deletes` removes Target identities that disappeared
/// (used for revision rebuild and resume catch-up). When `ignore_paused` is true,
/// Delivery runs even if the Pipeline is still marked paused (revision transition).
async fn deliver_pipelines_with_options(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
    reconcile_deletes: bool,
    ignore_paused: bool,
) -> Result<(), CliError> {
    let needs_delivery = pipelines.iter().any(|p| {
        pipeline_has_target(p) && (ignore_paused || !p.paused)
    });
    if !needs_delivery {
        return Ok(());
    }

    let mongo = mongo_target_from_deployment(deployment)?;

    for pipeline in pipelines {
        if !pipeline_has_target(pipeline) || (!ignore_paused && pipeline.paused) {
            continue;
        }

        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline_with_options(
                    platform_store_url,
                    deployment,
                    pipeline,
                    &mongo,
                    reconcile_deletes,
                )
                .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    platform_store_url,
                    deployment,
                    pipeline,
                    &mongo,
                    reconcile_deletes,
                )
                .await?;
            }
            other => {
                return Err(CliError::Failed(format!(
                    "unsupported pipeline.mode {other:?} for Delivery"
                )));
            }
        }
    }

    Ok(())
}

/// Direct Pipeline Delivery. When `reconcile_deletes` is true (resume / revision),
/// also remove Target documents whose Output Identity is no longer in Base.
async fn deliver_direct_pipeline_with_options(
    platform_store_url: &str,
    deployment: &Deployment,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    reconcile_deletes: bool,
) -> Result<(), CliError> {
    let (dataset, rows) = get_base_rows(
        platform_store_url,
        &pipeline.source_table,
        Some(&pipeline.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let source_columns = source_columns_for_pipeline(
        deployment,
        &pipeline.source_schema,
        &pipeline.source_table,
    )?;
    let managed_names: BTreeSet<String> = dataset
        .columns
        .iter()
        .map(|c| c.name.clone())
        .filter(|name| {
            !matches!(
                pipeline.field_mappings.get(name),
                Some(FieldMappingAs::Omit)
            )
        })
        .collect();
    validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;

    if dataset.primary_key.is_empty() {
        return Err(CliError::Failed(
            "Base Dataset has no primary key for Output Identity".to_string(),
        ));
    }

    let mut documents = Vec::with_capacity(rows.len());
    let mut live_identities = BTreeSet::new();
    for row in &rows {
        // Direct Pipeline Managed fields default to all supported Base columns,
        // minus omit mappings; unsafe NUMBER requires string/omit (ADR-0023).
        let document = delivery_document_for_row(
            &row.data,
            &dataset.primary_key,
            &dataset.columns,
            pipeline,
        )?;
        live_identities.insert(identity_key(&document.identity));
        documents.push(document);
    }

    let delivered = upsert_managed_documents(mongo, &pipeline.target_collection, &documents)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    let mut deleted = 0usize;
    if reconcile_deletes {
        deleted = reconcile_target_deletes(
            mongo,
            &pipeline.target_collection,
            &live_identities,
        )
        .await?;
    }

    update_pipeline_delivery_progress(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        "delivered",
        Some((delivered + deleted) as i32),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    if reconcile_deletes && deleted > 0 {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents, {} deletes)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered,
            deleted
        );
    } else {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered
        );
    }
    emit_event(
        "delivery_complete",
        &[
            ("pipeline", EventValue::from(pipeline.name.as_str())),
            (
                "deployment",
                EventValue::from(pipeline.deployment_name.as_str()),
            ),
            (
                "collection",
                EventValue::from(pipeline.target_collection.as_str()),
            ),
            ("documents", EventValue::from(delivered)),
            ("deletes", EventValue::from(deleted)),
        ],
    );
    Ok(())
}

fn identity_key(identity: &serde_json::Value) -> String {
    serde_json::to_string(identity).unwrap_or_else(|_| identity.to_string())
}

fn target_document_identity_key(document: &serde_json::Value) -> Option<String> {
    document.get("_id").map(identity_key)
}

async fn reconcile_target_deletes(
    mongo: &MongoTargetConnection,
    collection: &str,
    live_identities: &BTreeSet<String>,
) -> Result<usize, CliError> {
    let documents = list_target_documents(mongo, collection)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let mut stale = Vec::new();
    for document in documents {
        let Some(key) = target_document_identity_key(&document) else {
            continue;
        };
        if !live_identities.contains(&key) {
            if let Some(id) = document.get("_id") {
                stale.push(id.clone());
            }
        }
    }
    if stale.is_empty() {
        return Ok(0);
    }
    delete_documents_by_identity(mongo, collection, &stale)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))
}

async fn deliver_transform_pipeline_with_options(
    platform_store_url: &str,
    deployment: &Deployment,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    reconcile_deletes: bool,
) -> Result<(), CliError> {
    if pipeline.output_identity.is_empty() {
        return Err(CliError::Failed(format!(
            "Transform Pipeline {} requires outputIdentity before it can run",
            pipeline.name
        )));
    }

    let (base, base_rows) = get_base_rows(
        platform_store_url,
        &pipeline.source_table,
        Some(&pipeline.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let ops = transform_ops_from_pipeline(pipeline)?;
    let (secondary, secondary_columns) =
        load_secondary_bases_and_columns_for_pipeline(platform_store_url, pipeline, &ops).await?;
    let base_maps: Vec<_> = base_rows.iter().map(|r| r.data.clone()).collect();
    let derived_rows = evaluate_transform_with_bases(&ops, &base_maps, &secondary)
        .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    let derived_columns =
        derived_columns_for_ops(&base.columns, &ops, &derived_rows, &secondary_columns);
    let source_columns = source_columns_for_pipeline(
        deployment,
        &pipeline.source_schema,
        &pipeline.source_table,
    )?;
    let managed_names: BTreeSet<String> = derived_columns
        .iter()
        .map(|c| c.name.clone())
        .filter(|name| {
            !matches!(
                pipeline.field_mappings.get(name),
                Some(FieldMappingAs::Omit)
            )
        })
        .collect();
    validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;

    for field in &pipeline.output_identity {
        if !derived_columns.iter().any(|c| c.name == *field) {
            return Err(CliError::Failed(format!(
                "Transform Pipeline {} outputIdentity field {field} is not present in Derived output",
                pipeline.name
            )));
        }
    }

    let dataset = DerivedDataset {
        deployment_name: pipeline.deployment_name.clone(),
        pipeline_name: pipeline.name.clone(),
        status: "materialized".to_string(),
        output_identity: pipeline.output_identity.clone(),
        columns: derived_columns.clone(),
        row_count: derived_rows.len() as i32,
    };
    replace_derived_dataset(platform_store_url, &dataset, &derived_rows)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    persist_maintenance_state_for_pipeline(
        platform_store_url,
        pipeline,
        &ops,
        &base_maps,
    )
    .await?;

    println!(
        "Derived Dataset materialized: Pipeline {} ({} rows)",
        pipeline.name, dataset.row_count
    );

    let mut documents = Vec::with_capacity(derived_rows.len());
    let mut live_identities = BTreeSet::new();
    for row in &derived_rows {
        let document = delivery_document_for_row(
            row,
            &pipeline.output_identity,
            &derived_columns,
            pipeline,
        )?;
        live_identities.insert(identity_key(&document.identity));
        documents.push(document);
    }

    let delivered = upsert_managed_documents(mongo, &pipeline.target_collection, &documents)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    let mut deleted = 0usize;
    if reconcile_deletes {
        deleted = reconcile_target_deletes(
            mongo,
            &pipeline.target_collection,
            &live_identities,
        )
        .await?;
    }

    update_pipeline_delivery_progress(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        "delivered",
        Some((delivered + deleted) as i32),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    if reconcile_deletes && deleted > 0 {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents, {} deletes)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered,
            deleted
        );
    } else {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered
        );
    }
    Ok(())
}

/// Columns for a Derived Dataset after project/addFields/rename/remove/equiLookup/unwind/union/groupBy,
/// merged with keys observed in Derived rows. Works for empty Derived results.
/// Aggregate/`addFields`/`rename` aliases inherit the source field's Oracle type metadata.
/// `equiLookup` `as` arrays are nested documents (no Oracle scalar type). `unwind` of that
/// path flattens object elements so the path is no longer nested — foreign Base column
/// metadata in `secondary_columns` supplies types for those flattened / unioned fields.
fn derived_columns_for_ops(
    base_columns: &[BaseColumn],
    ops: &[TransformOp],
    derived_rows: &[serde_json::Map<String, serde_json::Value>],
    secondary_columns: &[BaseColumn],
) -> Vec<BaseColumn> {
    let base_names: Vec<String> = base_columns.iter().map(|c| c.name.clone()).collect();
    let mut names: BTreeSet<String> = derived_output_field_names(ops, &base_names)
        .into_iter()
        .collect();
    for row in derived_rows {
        names.extend(row.keys().cloned());
    }
    let mut by_name: BTreeMap<&str, &BaseColumn> = base_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    // Primary wins on name clashes; secondary fills unwind-flattened foreign fields.
    for col in secondary_columns {
        by_name.entry(col.name.as_str()).or_insert(col);
    }
    let mut alias_source: BTreeMap<String, String> = BTreeMap::new();
    let mut nested_document_fields: BTreeSet<String> = BTreeSet::new();
    for op in ops {
        match op {
            TransformOp::GroupBy { aggregates, .. } => {
                for agg in aggregates {
                    alias_source.insert(agg.as_name.clone(), agg.field.clone());
                }
            }
            TransformOp::AddFields { fields } => {
                for spec in fields {
                    if let migraloop_transform::AddFieldSource::Field(src) = &spec.source {
                        // Chase prior rename/addFields aliases so type metadata
                        // resolves to a Base column (e.g. displayName→customerName→NAME).
                        let resolved = alias_source
                            .get(src)
                            .cloned()
                            .unwrap_or_else(|| src.clone());
                        alias_source.insert(spec.as_name.clone(), resolved);
                    }
                }
            }
            TransformOp::Rename { fields } => {
                for spec in fields {
                    let src = alias_source
                        .get(&spec.from)
                        .cloned()
                        .unwrap_or_else(|| spec.from.clone());
                    alias_source.insert(spec.to.clone(), src);
                    if nested_document_fields.remove(&spec.from) {
                        nested_document_fields.insert(spec.to.clone());
                    }
                }
            }
            TransformOp::EquiLookup { as_name, .. } => {
                nested_document_fields.insert(as_name.clone());
            }
            TransformOp::Unwind { path } => {
                // Object-element flatten removes the array path from Derived output.
                nested_document_fields.remove(path);
            }
            TransformOp::AddToSet { as_name, .. } => {
                // Distinct values collected into a JSON array.
                nested_document_fields.insert(as_name.clone());
            }
            _ => {}
        }
    }
    names
        .into_iter()
        .map(|name| {
            if nested_document_fields.contains(&name) {
                BaseColumn {
                    name,
                    oracle_type: "JSON".to_string(),
                    precision: None,
                    scale: None,
                }
            } else if let Some(col) = by_name.get(name.as_str()) {
                (*col).clone()
            } else if let Some(source) = alias_source.get(&name) {
                if let Some(col) = by_name.get(source.as_str()) {
                    BaseColumn {
                        name,
                        oracle_type: col.oracle_type.clone(),
                        precision: col.precision,
                        scale: col.scale,
                    }
                } else {
                    BaseColumn {
                        name,
                        oracle_type: "NUMBER".to_string(),
                        precision: None,
                        scale: None,
                    }
                }
            } else {
                BaseColumn {
                    name,
                    oracle_type: "VARCHAR2".to_string(),
                    precision: None,
                    scale: None,
                }
            }
        })
        .collect()
}

async fn persist_maintenance_state_for_pipeline(
    platform_store_url: &str,
    pipeline: &Pipeline,
    ops: &[TransformOp],
    base_rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), CliError> {
    if !requires_maintenance_state(ops) {
        delete_maintenance_state(
            platform_store_url,
            &pipeline.deployment_name,
            &pipeline.name,
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
        return Ok(());
    }
    let state = build_maintenance_state(ops, base_rows).map_err(|err| {
        CliError::Failed(format!(
            "Transform Pipeline {}: failed to build Maintenance State: {err}",
            pipeline.name
        ))
    })?;
    persist_maintenance_state_json(platform_store_url, pipeline, &state).await
}

async fn persist_maintenance_state_json(
    platform_store_url: &str,
    pipeline: &Pipeline,
    state: &MaintenanceState,
) -> Result<(), CliError> {
    let state_json = serde_json::to_string(state).map_err(|err| {
        CliError::Failed(format!(
            "Transform Pipeline {}: failed to serialize Maintenance State: {err}",
            pipeline.name
        ))
    })?;
    replace_maintenance_state(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        &state_json,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))
}

async fn load_maintenance_state_for_pipeline(
    platform_store_url: &str,
    pipeline: &Pipeline,
) -> Result<MaintenanceState, CliError> {
    match get_maintenance_state_json(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?
    {
        Some(json) => serde_json::from_str(&json).map_err(|err| {
            CliError::Failed(format!(
                "Transform Pipeline {}: invalid Maintenance State JSON: {err}",
                pipeline.name
            ))
        }),
        None => Ok(MaintenanceState::default()),
    }
}

fn base_change_kind(op: ChangeOp) -> BaseChangeKind {
    match op {
        ChangeOp::Insert => BaseChangeKind::Insert,
        ChangeOp::Update => BaseChangeKind::Update,
        ChangeOp::Delete => BaseChangeKind::Delete,
    }
}

/// Incremental Transform maintenance for one Base change (Affect Analysis driven).
///
/// `changed_table` / `changed_base_rows` are the Base that just received the change
/// (primary `source.table` or an `equiLookup.from` / `union.from` secondary).
async fn maintain_transform_pipeline_for_change(
    platform_store_url: &str,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    changed_table: &str,
    changed_base_rows: &[serde_json::Map<String, serde_json::Value>],
    change: &ChangeEvent,
    pre_apply: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), CliError> {
    let ops = transform_ops_from_pipeline(pipeline)?;
    let after = match change.op {
        ChangeOp::Insert | ChangeOp::Update => {
            Some(
                changed_base_rows
                    .iter()
                    .find(|row| row_matches_identity(row, &change.identity))
                    .cloned()
                    .ok_or_else(|| {
                        CliError::Failed(format!(
                            "Base Dataset {changed_table} missing row for change identity {:?} after apply",
                            change.identity
                        ))
                    })?,
            )
        }
        ChangeOp::Delete => None,
    };

    let (primary_columns, primary_rows) = if changed_table.eq_ignore_ascii_case(&pipeline.source_table)
    {
        let (base, _) = get_base_rows(
            platform_store_url,
            &pipeline.source_table,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
        (base.columns, changed_base_rows.to_vec())
    } else {
        let (base, rows) = get_base_rows(
            platform_store_url,
            &pipeline.source_table,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
        (
            base.columns,
            rows.into_iter().map(|r| r.data).collect(),
        )
    };

    let kind = base_change_kind(change.op);
    let is_primary = changed_table.eq_ignore_ascii_case(&pipeline.source_table);
    let needs_ms = requires_maintenance_state(&ops);

    let mut maintenance = if needs_ms {
        Some(load_maintenance_state_for_pipeline(platform_store_url, pipeline).await?)
    } else {
        None
    };

    // Load secondary Bases before Affect Analysis so equiLookup/union/unwind can
    // resolve multi-Base Output Identities (including disappeared identities).
    let (mut secondary, secondary_columns) =
        load_secondary_bases_and_columns_for_pipeline(platform_store_url, pipeline, &ops).await?;
    if !is_primary {
        for sec in secondary_base_refs(&ops) {
            if sec.table.eq_ignore_ascii_case(changed_table) {
                // Incremental Delivery runs before the changed Base is persisted —
                // prefer the in-memory after-image for the table that just changed.
                secondary.insert(sec.table, changed_base_rows.to_vec());
            }
        }
    }

    let outcome = if let (true, Some(state)) = (is_primary && needs_ms, maintenance.as_ref()) {
        analyze_affect_with_maintenance(&ops, kind, pre_apply, after.as_ref(), state).map_err(
            |err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)),
        )?
    } else {
        analyze_affect_on_base_with_bases(
            &ops,
            changed_table,
            &pipeline.source_table,
            kind,
            pre_apply,
            after.as_ref(),
            &primary_rows,
            &secondary,
        )
        .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?
    };

    // Value-level distinct/addToSet skips still update Maintenance State refcounts.
    if let (true, Some(state)) = (is_primary && needs_ms, maintenance.as_mut()) {
        maintain_state_for_change(&ops, state, kind, pre_apply, after.as_ref()).map_err(|err| {
            CliError::Failed(format!(
                "Transform Pipeline {}: Maintenance State update failed: {err}",
                pipeline.name
            ))
        })?;
        persist_maintenance_state_json(platform_store_url, pipeline, state).await?;
    }

    match outcome {
        AffectOutcome::SkipUnusedFields => {
            println!(
                "Affect Analysis: Pipeline {} skipped (unused fields only)",
                pipeline.name
            );
            Ok(())
        }
        AffectOutcome::SkipValueUnchanged => {
            println!(
                "Affect Analysis: Pipeline {} skipped (value-level; no Derived change)",
                pipeline.name
            );
            Ok(())
        }
        AffectOutcome::Recompute { identities } => {
            println!(
                "Affect Analysis: Pipeline {} affected identities={}",
                pipeline.name,
                identities.len()
            );
            recompute_and_deliver_affected_identities(
                platform_store_url,
                pipeline,
                mongo,
                &primary_columns,
                &secondary_columns,
                &primary_rows,
                &secondary,
                &ops,
                &identities,
            )
            .await
        }
    }
}

async fn recompute_and_deliver_affected_identities(
    platform_store_url: &str,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    base_columns: &[BaseColumn],
    secondary_columns: &[BaseColumn],
    primary_rows: &[serde_json::Map<String, serde_json::Value>],
    secondary_bases: &BTreeMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ops: &[TransformOp],
    identities: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), CliError> {
    let recomputed = evaluate_transform_for_identities_with_bases(
        ops,
        primary_rows,
        secondary_bases,
        identities,
    )
    .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    let (mut dataset, existing_rows) = get_derived_rows(
        platform_store_url,
        &pipeline.name,
        Some(&pipeline.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    // Grouped transforms (groupBy/distinct/addToSet): match by grouping keys.
    // Row-grain transforms: match Derived rows by Pipeline Output Identity only.
    // Affect Analysis identities may include shaped Managed fields (rename/addFields)
    // whose values changed — those must not leave stale Derived duplicates behind.
    let grouped = ops.iter().any(|op| {
        matches!(
            op,
            TransformOp::GroupBy { .. }
                | TransformOp::Distinct { .. }
                | TransformOp::AddToSet { .. }
        )
    });
    let identity_targets_row =
        |identity: &serde_json::Map<String, serde_json::Value>,
         row: &serde_json::Map<String, serde_json::Value>| {
            if grouped {
                identity_matches_row(identity, row)
            } else {
                pipeline.output_identity.iter().all(|key| {
                    match (identity.get(key), row.get(key)) {
                        (Some(a), Some(b)) => migraloop_transform::json_values_eq(a, b),
                        _ => false,
                    }
                })
            }
        };

    let mut merged: Vec<serde_json::Map<String, serde_json::Value>> = existing_rows
        .into_iter()
        .map(|r| r.data)
        .filter(|row| !identities.iter().any(|id| identity_targets_row(id, row)))
        .collect();
    merged.extend(recomputed.clone());

    let derived_columns =
        derived_columns_for_ops(base_columns, ops, &merged, secondary_columns);
    dataset.status = "materialized".to_string();
    dataset.columns = derived_columns.clone();
    dataset.row_count = merged.len() as i32;
    replace_derived_dataset(platform_store_url, &dataset, &merged)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    let mut upserts = Vec::new();
    for row in &recomputed {
        upserts.push(delivery_document_for_row(
            row,
            &pipeline.output_identity,
            &derived_columns,
            pipeline,
        )?);
    }
    let mut deletes = Vec::new();
    for identity in identities {
        let still_present = recomputed
            .iter()
            .any(|row| identity_targets_row(identity, row));
        if !still_present {
            deletes.push(output_identity_from_row(identity, &pipeline.output_identity)?);
        }
    }

    let mut delivered = 0i32;
    if !upserts.is_empty() {
        delivered += upsert_managed_documents(mongo, &pipeline.target_collection, &upserts)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))? as i32;
    }
    if !deletes.is_empty() {
        delivered += delete_documents_by_identity(mongo, &pipeline.target_collection, &deletes)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))? as i32;
    }

    if delivered > 0 {
        update_pipeline_delivery_progress(
            platform_store_url,
            &pipeline.deployment_name,
            &pipeline.name,
            "delivered",
            Some(delivered),
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    }

    println!(
        "Delivery complete: Pipeline {} upserts={} deletes={} (Affect Analysis)",
        pipeline.name,
        upserts.len(),
        deletes.len()
    );
    Ok(())
}

async fn apply_deployment(platform_store_url: &str, file: &Path) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;

    let doc = load_deployment_config(file)?;
    let deployment = document_to_deployment(&doc)?;
    let mut pipelines = pipelines_from_document(&doc);

    // ADR-0021: fail-fast Source Prerequisites before discovery / Initial Load.
    // Deployment-only apply (no Pipeline tables) does not open LogMiner yet.
    let source_tables = pipeline_source_tables(&pipelines);
    if deployment.source.kind.eq_ignore_ascii_case("oracle") && !source_tables.is_empty() {
        ensure_oracle_source_prerequisites(&deployment.source, &source_tables)?;
    }

    // Apply-time Managed validation before Initial Load / Delivery so unsafe NUMBER
    // and unsupported Managed inputs fail configure-time (ADR-0018 / ADR-0023).
    // Real Oracle hosts discover schema via OCI; contract/stub use the contract catalog.
    for pipeline in &pipelines {
        let source_columns = source_columns_for_pipeline(
            &deployment,
            &pipeline.source_schema,
            &pipeline.source_table,
        )?;
        let managed_names: BTreeSet<String> = source_columns
            .iter()
            .filter(|c| c.supported)
            .map(|c| c.name.clone())
            .filter(|name| {
                !matches!(
                    pipeline.field_mappings.get(name),
                    Some(FieldMappingAs::Omit)
                )
            })
            .collect();
        validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;
    }

    let existing_pipelines = list_pipelines(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?
        .into_iter()
        .filter(|p| p.deployment_name == deployment.name)
        .collect::<Vec<_>>();
    let existing_names: BTreeSet<String> = existing_pipelines
        .iter()
        .map(|p| p.name.clone())
        .collect();
    // Owned summaries so we can mutate `pipelines` below without overlapping borrows.
    let added_pipeline_summaries: Vec<(String, String)> = pipelines
        .iter()
        .filter(|p| !existing_names.contains(&p.name))
        .map(|p| (p.name.clone(), p.source_table.clone()))
        .collect();

    // Runtime add (ADR-0007): keep already-running Pipelines' Delivery progress
    // (and Operator pause) when the semantic declaration is unchanged.
    preserve_unchanged_pipeline_delivery(&existing_pipelines, &mut pipelines);

    let revision_names: BTreeSet<String> = pipelines_needing_revision_rebuild(
        &existing_pipelines,
        &pipelines,
    )
    .into_iter()
    .map(|p| p.name.clone())
    .collect();
    let metadata_only_names: BTreeSet<String> = pipelines_with_metadata_only_change(
        &existing_pipelines,
        &pipelines,
    )
    .into_iter()
    .map(|p| p.name.clone())
    .collect();

    // Change (ADR-0007): pause old Delivery before swapping the revision so a
    // concurrent sync cannot Deliver under the previous transform/binding.
    for name in &revision_names {
        if let Some(previous) = existing_pipelines.iter().find(|p| p.name == *name) {
            if !previous.paused {
                set_pipeline_paused(platform_store_url, &deployment.name, name, true)
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
            }
            println!("Pipeline revision: {name} — paused old Delivery");
        }
        if let Some(pipeline) = pipelines.iter_mut().find(|p| p.name == *name) {
            // Hold pause through replace until rebuild/re-Deliver finishes.
            pipeline.paused = true;
            pipeline.delivery_status = if pipeline_has_target(pipeline) {
                "pending".to_string()
            } else {
                "not_configured".to_string()
            };
            pipeline.delivery_applied_changes = 0;
            pipeline.drift_status = "unknown".to_string();
            pipeline.drift_checked_rows = 0;
            pipeline.drift_mismatched_rows = 0;
        }
    }

    upsert_deployment(platform_store_url, &deployment)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    replace_pipelines(platform_store_url, &deployment.name, &pipelines)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    // Table-level Initial Load only for newly referenced tables; existing Bases stay
    // on their incremental path (ADR-0019). Shared Bases are never rebuilt for a
    // Pipeline revision (ADR-0007 Change).
    sync_base_datasets_for_pipelines(platform_store_url, &deployment, &pipelines).await?;

    if !existing_pipelines.is_empty() {
        for (name, source_table) in &added_pipeline_summaries {
            println!("Runtime Pipeline add: {name} (source={source_table})");
        }
    }

    for name in &metadata_only_names {
        println!("Pipeline revision: {name} (metadata-only; rebuild skipped)");
    }

    // Semantic revisions: rebuild Derived / re-Deliver with delete reconciliation,
    // then clear the transition pause so incremental work continues.
    let to_revise: Vec<&Pipeline> = pipelines
        .iter()
        .filter(|p| revision_names.contains(&p.name))
        .collect();
    if !to_revise.is_empty() {
        let reconcile_deletes = true;
        let ignore_paused = true;
        deliver_pipelines_with_options(
            platform_store_url,
            &deployment,
            &to_revise,
            reconcile_deletes,
            ignore_paused,
        )
        .await?;
        for pipeline in &to_revise {
            set_pipeline_paused(
                platform_store_url,
                &deployment.name,
                &pipeline.name,
                false,
            )
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
            println!(
                "Pipeline revision: {} — rebuilt and re-Delivered; incremental resumed",
                pipeline.name
            );
        }
    }

    // Start Delivery only for Pipelines that need ordinary first Delivery; do not
    // re-Deliver unchanged already-delivered Pipelines (others keep running — ADR-0007 Add).
    let to_deliver = pipelines_needing_delivery_start(&existing_pipelines, &pipelines);
    deliver_pipelines(platform_store_url, &deployment, &to_deliver).await?;

    println!("Deployment applied: {}", deployment.name);
    Ok(())
}

fn row_matches_identity(
    row: &serde_json::Map<String, serde_json::Value>,
    identity: &std::collections::BTreeMap<String, serde_json::Value>,
) -> bool {
    identity.iter().all(|(key, expected)| row.get(key) == Some(expected))
}

fn supported_row_from_change(
    change: &ChangeEvent,
    supported_names: &BTreeSet<String>,
    source_columns: &[SourceColumn],
    configured_timezone: Option<&str>,
) -> Result<serde_json::Map<String, serde_json::Value>, CliError> {
    let Some(row) = &change.row else {
        return Err(CliError::Failed(format!(
            "Incremental {:?} change for {:?} is missing row data",
            change.op, change.identity
        )));
    };
    let mut as_btree: BTreeMap<String, serde_json::Value> = row
        .iter()
        .filter(|(name, _)| supported_names.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    normalize_change_temporals(source_columns, &mut as_btree, configured_timezone)
        .map_err(|err| CliError::Failed(err.to_string()))?;
    Ok(as_btree.into_iter().collect())
}

fn apply_change_events_to_base_rows(
    rows: &mut Vec<serde_json::Map<String, serde_json::Value>>,
    changes: &[ChangeEvent],
    supported_names: &BTreeSet<String>,
    source_columns: &[SourceColumn],
    configured_timezone: Option<&str>,
) -> Result<(), CliError> {
    for change in changes {
        match change.op {
            ChangeOp::Insert | ChangeOp::Update => {
                let managed = supported_row_from_change(
                    change,
                    supported_names,
                    source_columns,
                    configured_timezone,
                )?;
                if let Some(existing) = rows
                    .iter_mut()
                    .find(|row| row_matches_identity(row, &change.identity))
                {
                    *existing = managed;
                } else {
                    rows.push(managed);
                }
            }
            ChangeOp::Delete => {
                rows.retain(|row| !row_matches_identity(row, &change.identity));
            }
        }
    }
    Ok(())
}

/// Test-only fault injection for restart-resume coverage (ADR-0011).
/// When set, sync exits after N durable checkpoints to simulate mid-incremental process kill.
fn sync_fail_after_changes() -> Option<u32> {
    std::env::var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
}

/// Bounded Delivery retries before Poison Change quarantine (ADR-0015 / issue #22).
fn poison_max_attempts() -> u32 {
    std::env::var("MIGRALOOP_POISON_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(3)
}

/// Test/Lab fault injection: comma-separated Output Identity keys that always fail Delivery.
fn delivery_poison_identity_keys() -> BTreeSet<String> {
    std::env::var("MIGRALOOP_DELIVERY_POISON_IDENTITIES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Bounded Incremental Capture / Delivery queue capacity (ADR-0020 / issue #26).
///
/// Stages never materialize more than this many pending changes at once; capture
/// slows when Downstream cannot drain the window. Override via
/// `MIGRALOOP_SYNC_QUEUE_CAPACITY` (must be > 0). Default 256.
fn sync_queue_capacity() -> usize {
    std::env::var("MIGRALOOP_SYNC_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(256)
}

/// Test/Lab fault injection: artificial Downstream Delivery slowness (milliseconds).
fn delivery_delay_ms() -> Option<u64> {
    std::env::var("MIGRALOOP_DELIVERY_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

async fn apply_delivery_delay() {
    if let Some(ms) = delivery_delay_ms() {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

async fn set_delivery_lag_for_table(
    platform_store_url: &str,
    pipelines: &[Pipeline],
    table: &str,
    delivery_lag: i32,
) -> Result<(), CliError> {
    for pipeline in pipelines {
        if pipeline.target_collection.is_empty() || !pipeline_references_table(pipeline, table) {
            continue;
        }
        update_pipeline_delivery_lag(
            platform_store_url,
            &pipeline.deployment_name,
            &pipeline.name,
            delivery_lag,
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    }
    Ok(())
}

fn format_output_identity(identity: &serde_json::Value) -> String {
    match identity {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn identity_is_poison(
    identity: &serde_json::Value,
    poison_keys: &BTreeSet<String>,
) -> bool {
    if poison_keys.is_empty() {
        return false;
    }
    poison_keys.contains(&format_output_identity(identity))
}

fn identity_value_from_change(
    change: &ChangeEvent,
    identity_fields: &[String],
) -> Result<serde_json::Value, CliError> {
    let identity_map: serde_json::Map<String, serde_json::Value> = change
        .identity
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    output_identity_from_row(&identity_map, identity_fields)
}

async fn upsert_with_bounded_retries(
    mongo: &MongoTargetConnection,
    collection: &str,
    document: &DeliveryDocument,
    max_attempts: u32,
) -> Result<usize, (u32, String)> {
    let poison = delivery_poison_identity_keys();
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay().await;
        if identity_is_poison(&document.identity, &poison) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(&document.identity)
            );
        } else {
            match upsert_managed_documents(mongo, collection, std::slice::from_ref(document)).await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

async fn delete_with_bounded_retries(
    mongo: &MongoTargetConnection,
    collection: &str,
    identity: &serde_json::Value,
    max_attempts: u32,
) -> Result<usize, (u32, String)> {
    let poison = delivery_poison_identity_keys();
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay().await;
        if identity_is_poison(identity, &poison) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(identity)
            );
        } else {
            match delete_documents_by_identity(
                mongo,
                collection,
                std::slice::from_ref(identity),
            )
            .await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

async fn quarantine_poison_change(
    platform_store_url: &str,
    pipeline: &Pipeline,
    schema: &str,
    table: &str,
    change: &ChangeEvent,
    output_identity: serde_json::Value,
    stage: &str,
    attempts: u32,
    last_error: &str,
) -> Result<(), CliError> {
    let record = QuarantinedChange {
        deployment_name: pipeline.deployment_name.clone(),
        pipeline_name: pipeline.name.clone(),
        source_schema: schema.to_string(),
        source_table: table.to_string(),
        change_id: change.change_id.clone(),
        capture_position: change.position.as_i64(),
        output_identity,
        stage: stage.to_string(),
        attempts: attempts as i32,
        last_error: last_error.to_string(),
        status: "quarantined".to_string(),
    };
    let identity_label = format_output_identity(&record.output_identity);
    upsert_quarantined_change(platform_store_url, &record)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    eprintln!(
        "ALERT: Poison Change quarantined Pipeline={} identity={} change_id={} \
         stage={stage} attempts={attempts}: {last_error}",
        pipeline.name, identity_label, change.change_id
    );
    println!(
        "Quarantine: Pipeline={} identity={} change_id={} stage={stage} \
         attempts={attempts} unhealthy / not aligned",
        pipeline.name, identity_label, change.change_id
    );
    emit_event(
        "poison_quarantine",
        &[
            ("level", EventValue::from("alert")),
            ("pipeline", EventValue::from(pipeline.name.as_str())),
            ("identity", EventValue::from(identity_label.as_str())),
            ("change_id", EventValue::from(change.change_id.as_str())),
            ("stage", EventValue::from(stage)),
            ("attempts", EventValue::from(attempts as i64)),
        ],
    );
    Ok(())
}

/// Row DML or Source Schema Change in the Incremental Capture stream (ADR-0009).
enum IncrementalItem {
    Row(ChangeEvent),
    Schema(SchemaChangeEvent),
}

impl IncrementalItem {
    fn position(&self) -> CapturePosition {
        match self {
            Self::Row(c) => c.position,
            Self::Schema(c) => c.position,
        }
    }

    fn change_id(&self) -> &str {
        match self {
            Self::Row(c) => &c.change_id,
            Self::Schema(c) => &c.change_id,
        }
    }
}

/// Dependency columns for Schema Change impact classification.
fn pipeline_schema_deps(pipeline: &Pipeline, dataset: &BaseDataset) -> PipelineSchemaDeps {
    let mut dependency_columns: BTreeSet<String> = dataset.primary_key.iter().cloned().collect();
    let is_primary = dataset
        .source_table
        .eq_ignore_ascii_case(&pipeline.source_table);
    match pipeline.mode.as_str() {
        "direct" => {
            for col in &dataset.columns {
                if pipeline.field_mappings.get(&col.name) == Some(&FieldMappingAs::Omit) {
                    continue;
                }
                dependency_columns.insert(col.name.clone());
            }
        }
        "transform" => {
            if is_primary {
                if let Some(transform) = &pipeline.transform_json {
                    if let Some(steps) = transform.as_array() {
                        if let Ok(ops) = parse_transform_steps(steps) {
                            dependency_columns.extend(used_base_fields(&ops));
                        }
                    }
                }
                for field in &pipeline.output_identity {
                    dependency_columns.insert(field.clone());
                }
            } else {
                // equiLookup embeds full foreign rows; union concatenates secondary
                // rows — any column drop/type change on a secondary Base blocks.
                for col in &dataset.columns {
                    dependency_columns.insert(col.name.clone());
                }
            }
        }
        _ => {
            for col in &dataset.columns {
                dependency_columns.insert(col.name.clone());
            }
        }
    }
    PipelineSchemaDeps {
        source_table: dataset.source_table.clone(),
        source_schema: dataset.source_schema.clone(),
        dependency_columns,
    }
}

/// Classify Schema Change impact for Pipelines on this table; warn+pause on Blocking.
async fn apply_schema_change_impacts(
    platform_store_url: &str,
    deployment_pipelines: &mut [Pipeline],
    dataset: &BaseDataset,
    schema: &str,
    table: &str,
    change: &SchemaChangeEvent,
) -> Result<(), CliError> {
    for pipeline in deployment_pipelines.iter_mut() {
        if !pipeline_references_table(pipeline, table) {
            continue;
        }
        // Schema must match the referenced Base (primary schema or equiLookup/union fromSchema).
        let refs = pipeline_base_table_refs(pipeline);
        let schema_ok = refs.iter().any(|(ref_schema, ref_table)| {
            ref_table.eq_ignore_ascii_case(table)
                && (ref_schema.is_empty()
                    || schema.is_empty()
                    || ref_schema.eq_ignore_ascii_case(schema))
        });
        if !schema_ok {
            continue;
        }
        let deps = pipeline_schema_deps(pipeline, dataset);
        let impact = classify_schema_impact(&deps, change);
        match impact {
            SchemaImpact::Blocking => {
                if !pipeline.paused {
                    set_pipeline_paused(
                        platform_store_url,
                        &pipeline.deployment_name,
                        &pipeline.name,
                        true,
                    )
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
                    pipeline.paused = true;
                }
                let record = SchemaChangeImpact {
                    deployment_name: pipeline.deployment_name.clone(),
                    pipeline_name: pipeline.name.clone(),
                    source_schema: schema.to_string(),
                    source_table: table.to_string(),
                    change_id: change.change_id.clone(),
                    capture_position: change.position.as_i64(),
                    ddl_summary: change.summary.clone(),
                    impact: impact.as_str().to_string(),
                    status: "active".to_string(),
                };
                upsert_schema_change_impact(platform_store_url, &record)
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
                eprintln!(
                    "WARN: Schema Change blocked Pipeline={} change_id={} ddl={} — \
                     pausing affected Pipeline (not poison quarantine)",
                    pipeline.name, change.change_id, change.summary
                );
                println!(
                    "Schema Change: Pipeline={} impact=blocking change_id={} ddl={} paused",
                    pipeline.name, change.change_id, change.summary
                );
                emit_event(
                    "schema_change_blocked",
                    &[
                        ("level", EventValue::from("warn")),
                        ("pipeline", EventValue::from(pipeline.name.as_str())),
                        ("change_id", EventValue::from(change.change_id.as_str())),
                        ("ddl", EventValue::from(change.summary.as_str())),
                        ("impact", EventValue::from("blocking")),
                    ],
                );
            }
            SchemaImpact::NonBlocking => {
                println!(
                    "Schema Change: Pipeline={} impact=non_blocking change_id={} ddl={} — \
                     continue (safe apply)",
                    pipeline.name, change.change_id, change.summary
                );
            }
            SchemaImpact::Unaffecting => {
                println!(
                    "Schema Change: Pipeline={} impact=unaffecting change_id={} ddl={} — \
                     continue",
                    pipeline.name, change.change_id, change.summary
                );
            }
        }
    }
    Ok(())
}

fn base_with_sync_progress(
    dataset: &BaseDataset,
    status: impl Into<String>,
    row_count: i32,
    sync_applied_changes: i32,
    capture_checkpoint: Option<i64>,
    sync_lag: i32,
) -> BaseDataset {
    BaseDataset {
        deployment_name: dataset.deployment_name.clone(),
        source_table: dataset.source_table.clone(),
        source_schema: dataset.source_schema.clone(),
        status: status.into(),
        primary_key: dataset.primary_key.clone(),
        columns: dataset.columns.clone(),
        omitted_columns: dataset.omitted_columns.clone(),
        row_count,
        sync_applied_changes,
        sync_health: "ok".to_string(),
        capture_low_watermark: dataset.capture_low_watermark,
        capture_checkpoint,
        sync_lag,
        source_alignment: dataset.source_alignment.clone(),
        source_alignment_checked_rows: dataset.source_alignment_checked_rows,
        source_alignment_mismatched_rows: dataset.source_alignment_mismatched_rows,
        initial_load_cursor: dataset.initial_load_cursor.clone(),
    }
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

    let mut repaired: BTreeMap<String, serde_json::Map<String, serde_json::Value>> = BTreeMap::new();
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
                && pipeline_name
                    .map(|n| p.name == n)
                    .unwrap_or(true)
                && deployment
                    .map(|d| p.deployment_name == d)
                    .unwrap_or(true)
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
        let managed_keys: Vec<&str> = expected
            .managed_fields
            .keys()
            .map(|k| k.as_str())
            .collect();
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
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items.iter().map(normalize_json_for_drift).collect(),
        ),
        other => other.clone(),
    }
}

async fn sync_incremental(platform_store_url: &str) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;

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

    let fail_after = sync_fail_after_changes();
    let max_poison_attempts = poison_max_attempts();
    let queue_capacity = sync_queue_capacity();
    let downstream_delay = delivery_delay_ms().is_some();
    let injected_schema_changes = load_injected_schema_changes()
        .map_err(|err| CliError::Failed(err.to_string()))?;
    let mut applied_this_run: u32 = 0;

    for deployment in &deployments {
        let mut deployment_pipelines: Vec<_> = pipelines
            .iter()
            .filter(|p| p.deployment_name == deployment.name)
            .cloned()
            .collect();
        if deployment_pipelines.is_empty() {
            continue;
        }

        let mut tables = BTreeSet::new();
        for pipeline in &deployment_pipelines {
            for (schema, table) in pipeline_base_table_refs(pipeline) {
                tables.insert((schema, table));
            }
        }

        // ADR-0021: fail-fast Source Prerequisites before Incremental Capture.
        // LogMiner-backed capture (contract or OCI) is opened once per Deployment.
        let capture = if deployment.source.kind.eq_ignore_ascii_case("oracle") {
            let source_tables: Vec<String> = tables.iter().map(|(_, t)| t.clone()).collect();
            ensure_oracle_source_prerequisites(&deployment.source, &source_tables)?;
            Some(open_deployment_incremental_capture(&deployment.source)?)
        } else {
            None
        };
        if let Some(ref capture) = capture {
            println!(
                "Incremental Capture: mechanism={}",
                capture.mechanism_label()
            );
        }

        // Resume from durable Platform Store checkpoint (exclusive). Initial Load sets
        // checkpoint = low-watermark-1 so the first Incremental still covers the ADR-0004
        // overlap window. Prefer duplicates over gaps: Deliver each change before durable
        // Base/checkpoint/change-id persistence so a Delivery failure can retry.
        let mongo = mongo_target_from_deployment(deployment)?;

        for (schema, table) in tables {
            let (dataset, base_rows) =
                get_base_rows(platform_store_url, &table, Some(&deployment.name))
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;

            let Some(low_watermark_i64) = dataset.capture_low_watermark else {
                return Err(CliError::Failed(format!(
                    "cannot start Incremental Capture for {table} without low-watermark overlap \
                     (cutover watermark missing; re-run Initial Load via `migraloop apply`)"
                )));
            };
            let low_watermark = CapturePosition::from_i64(low_watermark_i64).ok_or_else(|| {
                CliError::Failed(format!(
                    "invalid low-watermark for Base Dataset {table}: {low_watermark_i64}"
                ))
            })?;

            let mut resume_from = match dataset.capture_checkpoint {
                Some(cp) => {
                    let next = cp.saturating_add(1);
                    CapturePosition::from_i64(next).ok_or_else(|| {
                        CliError::Failed(format!(
                            "invalid capture checkpoint for Base Dataset {table}: {cp}"
                        ))
                    })?
                }
                None => low_watermark,
            };

            let Some(capture) = &capture else {
                return Err(CliError::Failed(format!(
                    "Incremental Capture requires an Oracle Source System (LogMiner); \
                     got kind={}",
                    deployment.source.kind
                )));
            };

            let supported_names: BTreeSet<String> =
                dataset.columns.iter().map(|c| c.name.clone()).collect();
            let source_columns = source_columns_for_pipeline(deployment, &schema, &table)?;
            let configured_tz = source_timezone_opt(deployment);
            let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
                base_rows.into_iter().map(|r| r.data).collect();
            let mut sync_applied = dataset.sync_applied_changes;

            for pipeline in &deployment_pipelines {
                if pipeline.paused
                    && !pipeline.target_collection.is_empty()
                    && pipeline_references_table(pipeline, &table)
                {
                    println!(
                        "Pipeline {} paused — skipping Delivery/processing for {table}",
                        pipeline.name
                    );
                }
            }

            let checkpoint_before = dataset
                .capture_checkpoint
                .unwrap_or(low_watermark_i64.saturating_sub(1));
            let mut windows_processed = 0usize;

            // ADR-0020: bounded Incremental windows. Capture only fills up to
            // queue_capacity; Downstream slowness drains slowly and backpressures
            // further fetch instead of buffering the full backlog in RAM.
            loop {
                // Count Source backlog without materializing row images so Sync/
                // Delivery Health lag can reflect delay under a bounded window.
                let source_pending = capture
                    .count_changes_in_schema(&schema, &table, resume_from)
                    .map_err(|err| CliError::Failed(err.to_string()))?;
                let candidate_changes = capture
                    .fetch_changes_in_schema_limited(
                        &schema,
                        &table,
                        resume_from,
                        Some(queue_capacity),
                    )
                    .map_err(|err| CliError::Failed(err.to_string()))?;
                let table_schema_changes: Vec<SchemaChangeEvent> = injected_schema_changes
                    .iter()
                    .filter(|c| c.table.eq_ignore_ascii_case(&table))
                    .filter(|c| c.position >= resume_from)
                    .cloned()
                    .collect();
                let mut candidate_ids: Vec<String> = candidate_changes
                    .iter()
                    .map(|c| c.change_id.clone())
                    .collect();
                candidate_ids.extend(
                    table_schema_changes
                        .iter()
                        .map(|c| c.change_id.clone()),
                );
                let unapplied_ids = filter_unapplied_change_ids(
                    platform_store_url,
                    &deployment.name,
                    &schema,
                    &table,
                    &candidate_ids,
                )
                .await
                .map_err(|err| CliError::Failed(err.to_string()))?;
                let unapplied_set: BTreeSet<_> = unapplied_ids.into_iter().collect();
                let schema_pending = table_schema_changes
                    .iter()
                    .filter(|c| unapplied_set.contains(&c.change_id))
                    .count();
                // Source count is from resume_from (exclusive of durable checkpoint).
                // Window fetch may be smaller; lag uses full Source+schema pending.
                let pending_at_window_start = source_pending.saturating_add(schema_pending);
                let mut items: Vec<IncrementalItem> = candidate_changes
                    .into_iter()
                    .filter(|c| unapplied_set.contains(&c.change_id))
                    .map(IncrementalItem::Row)
                    .collect();
                items.extend(
                    table_schema_changes
                        .into_iter()
                        .filter(|c| unapplied_set.contains(&c.change_id))
                        .map(IncrementalItem::Schema),
                );
                items.sort_by(|a, b| {
                    a.position()
                        .cmp(&b.position())
                        .then_with(|| a.change_id().cmp(b.change_id()))
                });
                if items.len() > queue_capacity {
                    items.truncate(queue_capacity);
                }

                if items.is_empty() {
                    let status = if windows_processed == 0
                        && dataset.status == "initial_load_complete"
                    {
                        dataset.status.clone()
                    } else {
                        "incremental".to_string()
                    };
                    let caught_up = base_with_sync_progress(
                        &dataset,
                        status,
                        rows.len() as i32,
                        sync_applied,
                        if windows_processed == 0 {
                            dataset.capture_checkpoint
                        } else {
                            Some(resume_from.as_i64().saturating_sub(1))
                        },
                        0,
                    );
                    replace_base_dataset(platform_store_url, &caught_up, &rows)
                        .await
                        .map_err(|err| CliError::Failed(err.to_string()))?;
                    set_delivery_lag_for_table(
                        platform_store_url,
                        &deployment_pipelines,
                        &table,
                        0,
                    )
                    .await?;
                    if windows_processed == 0 {
                        println!(
                            "Incremental Capture: Base Dataset {table} resume from checkpoint — \
                             0 new changes (already applied; lag=0)"
                        );
                    } else {
                        println!(
                            "Incremental Capture: Base Dataset {table} caught up (lag=0; \
                             bounded queue capacity={queue_capacity})"
                        );
                    }
                    break;
                }

                let queue_depth = items.len();
                let fetched_full_window = queue_depth >= queue_capacity;
                let reported_lag = pending_at_window_start as i32;
                // Backpressure is the bounded window under Downstream delay or a
                // full queue (capture cannot pull more until the window drains).
                if (downstream_delay && fetched_full_window) || fetched_full_window {
                    println!(
                        "Backpressure: queue_depth={queue_depth} capacity={queue_capacity} \
                         lag={reported_lag}"
                    );
                    emit_event(
                        "backpressure",
                        &[
                            ("table", EventValue::from(table.as_str())),
                            ("queue_depth", EventValue::from(queue_depth)),
                            ("capacity", EventValue::from(queue_capacity)),
                            ("lag", EventValue::from(reported_lag)),
                            (
                                "deployment",
                                EventValue::from(deployment.name.as_str()),
                            ),
                        ],
                    );
                }

                if windows_processed == 0 {
                    println!(
                        "Incremental Capture: resuming Base Dataset {table} from \
                         checkpoint={checkpoint_before} (exclusive next={resume_from}, \
                         queue_depth={queue_depth}, capacity={queue_capacity}, \
                         low-watermark={low_watermark})"
                    );
                } else {
                    println!(
                        "Incremental Capture: Base Dataset {table} next bounded window \
                         resume={resume_from} queue_depth={queue_depth} \
                         capacity={queue_capacity}"
                    );
                }
                emit_event(
                    "incremental_capture",
                    &[
                        ("table", EventValue::from(table.as_str())),
                        ("queue_depth", EventValue::from(queue_depth)),
                        ("capacity", EventValue::from(queue_capacity)),
                        ("lag", EventValue::from(reported_lag)),
                        (
                            "deployment",
                            EventValue::from(deployment.name.as_str()),
                        ),
                        (
                            "resume_from",
                            EventValue::from(resume_from.to_string()),
                        ),
                    ],
                );

                for (index, item) in items.iter().enumerate() {
                    // Remaining Source+schema pending after this durable apply.
                    let lag = (pending_at_window_start as i32) - (index as i32 + 1);
                    let lag = lag.max(0);
                    match item {
                        IncrementalItem::Schema(schema_change) => {
                            apply_schema_change_impacts(
                                platform_store_url,
                                &mut deployment_pipelines,
                                &dataset,
                                &schema,
                                &table,
                                schema_change,
                            )
                            .await?;

                            let current_checkpoint = schema_change.position.as_i64();
                            let updated = base_with_sync_progress(
                                &dataset,
                                "incremental",
                                rows.len() as i32,
                                sync_applied,
                                Some(current_checkpoint),
                                lag,
                            );
                            replace_base_dataset(platform_store_url, &updated, &rows)
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                            record_applied_source_changes(
                                platform_store_url,
                                &deployment.name,
                                &schema,
                                &table,
                                &[(
                                    schema_change.change_id.clone(),
                                    schema_change.position.as_i64(),
                                )],
                            )
                            .await
                            .map_err(|err| CliError::Failed(err.to_string()))?;
                            set_delivery_lag_for_table(
                                platform_store_url,
                                &deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;
                            applied_this_run += 1;
                            println!(
                                "Incremental Capture: Base Dataset {table} applied schema change_id={} \
                                 checkpoint={current_checkpoint} lag={lag}",
                                schema_change.change_id
                            );
                        }
                        IncrementalItem::Row(change) => {
                            // Capture pre-apply Base row for Affect Analysis (unused-field skip / group keys).
                            let pre_apply = rows
                                .iter()
                                .find(|row| row_matches_identity(row, &change.identity))
                                .cloned();

                            apply_change_events_to_base_rows(
                                &mut rows,
                                std::slice::from_ref(change),
                                &supported_names,
                                &source_columns,
                                configured_tz,
                            )?;

                            // Delivery before durable checkpoint so retries prefer duplicate applies.
                            for pipeline in &deployment_pipelines {
                                if pipeline.target_collection.is_empty()
                                    || !pipeline_references_table(pipeline, &table)
                                {
                                    continue;
                                }
                                if pipeline.paused {
                                    // Skip Delivery/processing; Base Capture still advances for shared Bases.
                                    continue;
                                }

                                match pipeline.mode.as_str() {
                                    "direct" => {
                                        // Direct Pipelines only Deliver their primary source.table.
                                        if !pipeline.source_table.eq_ignore_ascii_case(&table) {
                                            continue;
                                        }
                                        match change.op {
                                        ChangeOp::Insert | ChangeOp::Update => {
                                            let Some(base_row) = rows.iter().find(|row| {
                                                row_matches_identity(row, &change.identity)
                                            }) else {
                                                return Err(CliError::Failed(format!(
                                                    "Base Dataset {} missing row for Output Identity {:?}",
                                                    pipeline.source_table, change.identity
                                                )));
                                            };
                                            let document = delivery_document_for_row(
                                                base_row,
                                                &dataset.primary_key,
                                                &dataset.columns,
                                                pipeline,
                                            )?;
                                            match upsert_with_bounded_retries(
                                                &mongo,
                                                &pipeline.target_collection,
                                                &document,
                                                max_poison_attempts,
                                            )
                                            .await
                                            {
                                                Ok(upserted) => {
                                                    update_pipeline_delivery_progress_with_lag(
                                                        platform_store_url,
                                                        &pipeline.deployment_name,
                                                        &pipeline.name,
                                                        "delivered",
                                                        Some(upserted as i32),
                                                        Some(lag),
                                                    )
                                                    .await
                                                    .map_err(|err| {
                                                        CliError::Failed(err.to_string())
                                                    })?;
                                                    println!(
                                                        "Delivery complete: Pipeline {} upserts={upserted} \
                                                         deletes=0 (checkpoint-bound)",
                                                        pipeline.name
                                                    );
                                                }
                                                Err((attempts, last_error)) => {
                                                    quarantine_poison_change(
                                                        platform_store_url,
                                                        pipeline,
                                                        &schema,
                                                        &table,
                                                        change,
                                                        document.identity.clone(),
                                                        "delivery",
                                                        attempts,
                                                        &last_error,
                                                    )
                                                    .await?;
                                                    update_pipeline_delivery_lag(
                                                        platform_store_url,
                                                        &pipeline.deployment_name,
                                                        &pipeline.name,
                                                        lag,
                                                    )
                                                    .await
                                                    .map_err(|err| {
                                                        CliError::Failed(err.to_string())
                                                    })?;
                                                }
                                            }
                                        }
                                        ChangeOp::Delete => {
                                            let identity = identity_value_from_change(
                                                change,
                                                &dataset.primary_key,
                                            )?;
                                            match delete_with_bounded_retries(
                                                &mongo,
                                                &pipeline.target_collection,
                                                &identity,
                                                max_poison_attempts,
                                            )
                                            .await
                                            {
                                                Ok(deleted) => {
                                                    update_pipeline_delivery_progress_with_lag(
                                                        platform_store_url,
                                                        &pipeline.deployment_name,
                                                        &pipeline.name,
                                                        "delivered",
                                                        Some(deleted as i32),
                                                        Some(lag),
                                                    )
                                                    .await
                                                    .map_err(|err| {
                                                        CliError::Failed(err.to_string())
                                                    })?;
                                                    println!(
                                                        "Delivery complete: Pipeline {} upserts=0 \
                                                         deletes={deleted} (checkpoint-bound)",
                                                        pipeline.name
                                                    );
                                                }
                                                Err((attempts, last_error)) => {
                                                    quarantine_poison_change(
                                                        platform_store_url,
                                                        pipeline,
                                                        &schema,
                                                        &table,
                                                        change,
                                                        identity,
                                                        "delivery",
                                                        attempts,
                                                        &last_error,
                                                    )
                                                    .await?;
                                                    update_pipeline_delivery_lag(
                                                        platform_store_url,
                                                        &pipeline.deployment_name,
                                                        &pipeline.name,
                                                        lag,
                                                    )
                                                    .await
                                                    .map_err(|err| {
                                                        CliError::Failed(err.to_string())
                                                    })?;
                                                }
                                            }
                                        }
                                        }
                                    }
                                    "transform" => {
                                        let mut last_error = String::new();
                                        let mut succeeded = false;
                                        for attempt in 1..=max_poison_attempts {
                                            apply_delivery_delay().await;
                                            match maintain_transform_pipeline_for_change(
                                                platform_store_url,
                                                pipeline,
                                                &mongo,
                                                &table,
                                                &rows,
                                                change,
                                                pre_apply.as_ref(),
                                            )
                                            .await
                                            {
                                                Ok(()) => {
                                                    succeeded = true;
                                                    break;
                                                }
                                                Err(err) => {
                                                    last_error = err.to_string();
                                                    if attempt < max_poison_attempts {
                                                        eprintln!(
                                                            "Delivery retry {attempt}/{max_poison_attempts} \
                                                             failed: {last_error}"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        if !succeeded {
                                            let identity = identity_value_from_change(
                                                change,
                                                if pipeline.output_identity.is_empty() {
                                                    &dataset.primary_key
                                                } else {
                                                    &pipeline.output_identity
                                                },
                                            )
                                            .unwrap_or_else(|_| {
                                                serde_json::Value::Object(
                                                    change
                                                        .identity
                                                        .iter()
                                                        .map(|(k, v)| (k.clone(), v.clone()))
                                                        .collect(),
                                                )
                                            });
                                            quarantine_poison_change(
                                                platform_store_url,
                                                pipeline,
                                                &schema,
                                                &table,
                                                change,
                                                identity,
                                                "delivery",
                                                max_poison_attempts,
                                                &last_error,
                                            )
                                            .await?;
                                        }
                                        update_pipeline_delivery_lag(
                                            platform_store_url,
                                            &pipeline.deployment_name,
                                            &pipeline.name,
                                            lag,
                                        )
                                        .await
                                        .map_err(|err| CliError::Failed(err.to_string()))?;
                                    }
                                    other => {
                                        return Err(CliError::Failed(format!(
                                            "unsupported pipeline.mode {other:?} during Incremental Capture"
                                        )));
                                    }
                                }
                            }

                            sync_applied += 1;
                            let current_checkpoint = change.position.as_i64();
                            let updated = base_with_sync_progress(
                                &dataset,
                                "incremental",
                                rows.len() as i32,
                                sync_applied,
                                Some(current_checkpoint),
                                lag,
                            );

                            replace_base_dataset(platform_store_url, &updated, &rows)
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                            record_applied_source_changes(
                                platform_store_url,
                                &deployment.name,
                                &schema,
                                &table,
                                &[(change.change_id.clone(), change.position.as_i64())],
                            )
                            .await
                            .map_err(|err| CliError::Failed(err.to_string()))?;
                            set_delivery_lag_for_table(
                                platform_store_url,
                                &deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;

                            applied_this_run += 1;
                            println!(
                                "Incremental Capture: Base Dataset {table} applied change_id={} \
                                 checkpoint={current_checkpoint} lag={lag} rows={}",
                                change.change_id,
                                updated.row_count
                            );
                        }
                    }

                    if let Some(limit) = fail_after {
                        if applied_this_run >= limit {
                            let current_checkpoint = item.position().as_i64();
                            return Err(CliError::Failed(format!(
                                "simulated process kill after {limit} durable checkpoint(s) \
                                 (MIGRALOOP_SYNC_FAIL_AFTER_CHANGES); resume from Platform Store \
                                 checkpoint={current_checkpoint}"
                            )));
                        }
                    }
                }

                let last_pos = items
                    .last()
                    .expect("non-empty window")
                    .position()
                    .as_i64();
                resume_from = CapturePosition::from_i64(last_pos.saturating_add(1)).ok_or_else(
                    || {
                        CliError::Failed(format!(
                            "invalid capture position advance for Base Dataset {table}: {last_pos}"
                        ))
                    },
                )?;
                windows_processed += 1;
            }

        }
    }

    println!("Incremental Capture and Delivery complete");
    Ok(())
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
        println!("{}", serde_json::to_string_pretty(&value).unwrap_or_default());
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
        println!("{}", serde_json::to_string_pretty(&value).unwrap_or_default());
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
        let mongo = mongo_target_from_deployment(&deployment)?;
        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline_with_options(
                    platform_store_url,
                    &deployment,
                    &pipeline,
                    &mongo,
                    true,
                )
                .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    platform_store_url,
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
    delete_base_datasets_not_in(
        platform_store_url,
        &pipeline.deployment_name,
        &keep_tables,
    )
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
        } => {
            pause_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await
        }
        Command::Resume {
            platform_store_url,
            pipeline,
            deployment,
        } => {
            resume_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await
        }
        Command::Remove {
            platform_store_url,
            pipeline,
            deployment,
        } => {
            remove_pipeline_command(&platform_store_url, &pipeline, deployment.as_deref()).await
        }
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
            // Keep the single app instance alive and serve Prometheus /metrics (ADR-0008).
            observability::serve_prometheus_metrics(addr, platform_store_url).await
        }
        Command::Lab { command } => run_lab(command).await,
    }
}
