//! Operator-facing CLI for the DB Sync Platform.

mod config;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use migraloop_capture::{
    check_oracle_source_prerequisites, classify_number, initial_load_stub,
    is_allow_listed_oracle_type, normalize_change_temporals, open_oracle_incremental_capture,
    source_schema_stub, CapturePosition, ChangeEvent, ChangeOp, IncrementalCapture,
    NumberMongoMapping, OracleSourceConnect, SourceColumn, TypeError,
};
use migraloop_delivery::{
    delete_documents_by_identity, list_target_documents, upsert_managed_documents, DeliveryColumn,
    DeliveryDocument, ManagedFieldAs, MongoTargetConnection,
};
use migraloop_platform_store::{
    base_dataset_exists, delete_base_datasets_not_in, filter_unapplied_change_ids, get_base_rows,
    get_derived_rows, health, list_base_datasets, list_deployments, list_derived_datasets,
    list_pipelines, migrate, record_applied_source_changes, replace_base_dataset,
    replace_derived_dataset, replace_pipelines, update_base_primary_key,
    update_pipeline_delivery_progress, upsert_deployment, BaseColumn, BaseDataset, Deployment,
    DerivedDataset, FieldMappingAs, OmittedColumn, Pipeline, PlatformStoreHealth, SecretRef,
    SecretRefKind, SystemConnection,
};
use migraloop_transform::{
    analyze_affect, derived_projected_fields, evaluate_transform,
    evaluate_transform_for_identities, parse_transform_steps, AffectOutcome, BaseChangeKind,
    TransformOp,
};
use thiserror::Error;

use crate::config::{load_deployment_config, DeploymentDocument, PipelineSpec, ResolvedSecretRef};

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
    /// Run the app: migrate on startup, then keep the process alive
    Run {
        /// Platform Store connection URL (postgres://...)
        #[arg(long, env = "MIGRALOOP_PLATFORM_STORE_URL")]
        platform_store_url: String,
    },
}

pub fn parse() -> Cli {
    Cli::parse()
}

async fn apply_migrations(platform_store_url: &str) -> Result<(), CliError> {
    migrate(platform_store_url)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    println!("Platform Store migrations applied");
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
        },
        target: SystemConnection {
            kind: doc.spec.target.kind.clone(),
            host: doc.spec.target.host.clone(),
            port: doc.spec.target.port,
            database: doc.spec.target.database.clone(),
            username: doc.spec.target.username.clone(),
            password_ref: target_password_ref,
            timezone: String::new(),
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
        field_mappings,
        output_identity,
        transform_json,
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
    })
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
        "  {label}: {} {}:{} database={} username={} passwordRef={} timezone={}",
        system.kind,
        system.host,
        system.port,
        system.database,
        system.username,
        system.password_ref.display(),
        timezone
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
        PlatformStoreHealth::Healthy { .. } => Ok(()),
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
        tables.insert((
            pipeline.source_schema.clone(),
            pipeline.source_table.clone(),
        ));
    }
    let keep: Vec<(String, String)> = tables.iter().cloned().collect();

    // Capture scope follows Pipeline references: drop Bases for tables no longer referenced.
    delete_base_datasets_not_in(platform_store_url, deployment_name, &keep)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    for (schema, table) in tables {
        let already = base_dataset_exists(platform_store_url, deployment_name, &schema, &table)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;
        if already {
            // Existing Bases stay; do not reload on Pipeline re-apply (ADR-0019).
            // Backfill Output Identity PK metadata when an older Base predates Delivery.
            ensure_base_primary_key(
                platform_store_url,
                deployment_name,
                &schema,
                &table,
                configured_tz,
            )
            .await?;
            continue;
        }

        // ADR-0004: establish low-watermark first, then snapshot (stub does both).
        let snapshot = initial_load_stub(&table, configured_tz)
            .map_err(|err| CliError::Failed(err.to_string()))?;
        let low_watermark = snapshot.low_watermark;

        let supported = snapshot.supported_columns();
        let columns = base_columns_from_source(&supported);
        let omitted_columns: Vec<OmittedColumn> = snapshot
            .omitted_columns()
            .into_iter()
            .map(|c| OmittedColumn {
                name: c.name.clone(),
                oracle_type: c.oracle_type.clone(),
            })
            .collect();
        let supported_names: BTreeSet<String> =
            columns.iter().map(|c| c.name.clone()).collect();

        let rows: Vec<serde_json::Map<String, serde_json::Value>> = snapshot
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .filter(|(name, _)| supported_names.contains(name))
                    .collect()
            })
            .collect();

        let dataset = BaseDataset {
            deployment_name: deployment_name.to_string(),
            source_table: table.clone(),
            source_schema: schema,
            status: "initial_load_complete".to_string(),
            primary_key: snapshot.primary_key,
            columns,
            omitted_columns,
            row_count: rows.len() as i32,
            sync_applied_changes: 0,
            sync_health: "unknown".to_string(),
            capture_low_watermark: Some(low_watermark.as_i64()),
            // Checkpoint starts at watermark-1 so first Incremental includes the overlap window
            // via exclusive resume (checkpoint+1 == low-watermark).
            capture_checkpoint: Some(low_watermark.as_i64().saturating_sub(1)),
            sync_lag: 0,
        };

        replace_base_dataset(platform_store_url, &dataset, &rows)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;

        println!(
            "Initial Load complete: Base Dataset {table} ({} rows) low-watermark={}",
            dataset.row_count, low_watermark
        );
    }

    Ok(())
}

async fn ensure_base_primary_key(
    platform_store_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    configured_timezone: Option<&str>,
) -> Result<(), CliError> {
    let (dataset, _) = get_base_rows(platform_store_url, source_table, Some(deployment_name))
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if !dataset.primary_key.is_empty() {
        return Ok(());
    }

    let snapshot = initial_load_stub(source_table, configured_timezone)
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if snapshot.primary_key.is_empty() {
        return Err(CliError::Failed(format!(
            "stub Source table {source_table} has no primary key for Output Identity"
        )));
    }

    update_base_primary_key(
        platform_store_url,
        deployment_name,
        source_schema,
        source_table,
        &snapshot.primary_key,
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;
    Ok(())
}

/// Load stub Source schema metadata for apply-time Managed field validation.
fn stub_source_columns(table: &str) -> Result<Vec<SourceColumn>, CliError> {
    source_schema_stub(table).map_err(|err| CliError::Failed(err.to_string()))
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
        if !pipeline.source_table.is_empty() {
            tables.insert(pipeline.source_table.clone());
        }
    }
    tables.into_iter().collect()
}

/// Whether a Pipeline has a Target Binding configured for Delivery.
fn pipeline_has_target(pipeline: &Pipeline) -> bool {
    (pipeline.mode == "direct" || pipeline.mode == "transform")
        && !pipeline.target_collection.is_empty()
}

/// Whether two Pipeline declarations are the same (mode, Source table, Target Binding,
/// field mappings, transform) — not a revision/Change of that Pipeline.
///
/// Used so runtime Pipeline add can preserve Delivery progress for unchanged Pipelines
/// (ADR-0007) without treating a declaration change as a no-op add.
fn pipeline_declaration_unchanged(previous: &Pipeline, next: &Pipeline) -> bool {
    previous.mode == next.mode
        && previous.source_table == next.source_table
        && previous.source_schema == next.source_schema
        && previous.target_collection == next.target_collection
        && previous.field_mappings == next.field_mappings
        && previous.output_identity == next.output_identity
        && previous.transform_json == next.transform_json
}

/// Preserve Delivery progress for Pipelines whose declaration is unchanged.
///
/// `pipelines_from_document` always starts at pending/0; without this merge, every
/// apply would look like a Deployment restart for already-running Pipelines.
fn preserve_unchanged_pipeline_delivery(existing: &[Pipeline], pipelines: &mut [Pipeline]) {
    for pipeline in pipelines.iter_mut() {
        let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
            continue;
        };
        if pipeline_declaration_unchanged(previous, pipeline) {
            pipeline.delivery_status = previous.delivery_status.clone();
            pipeline.delivery_applied_changes = previous.delivery_applied_changes;
        }
    }
}

fn pipelines_needing_delivery_start<'a>(
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
                // Newly added Pipeline — start Delivery after Initial Load as needed.
                return true;
            };
            // Unchanged, already-delivered Pipelines keep running without re-Delivery.
            if pipeline_declaration_unchanged(previous, pipeline)
                && previous.delivery_status == "delivered"
            {
                return false;
            }
            true
        })
        .collect()
}

async fn deliver_pipelines(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
) -> Result<(), CliError> {
    let needs_delivery = pipelines.iter().any(|p| pipeline_has_target(p));
    if !needs_delivery {
        return Ok(());
    }

    let mongo = mongo_target_from_deployment(deployment)?;

    for pipeline in pipelines {
        if !pipeline_has_target(pipeline) {
            continue;
        }

        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline(platform_store_url, deployment, pipeline, &mongo).await?;
            }
            "transform" => {
                deliver_transform_pipeline(platform_store_url, deployment, pipeline, &mongo)
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

async fn deliver_direct_pipeline(
    platform_store_url: &str,
    deployment: &Deployment,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
) -> Result<(), CliError> {
    let (dataset, rows) = get_base_rows(
        platform_store_url,
        &pipeline.source_table,
        Some(&pipeline.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let source_columns = stub_source_columns(&pipeline.source_table)?;
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
    for row in &rows {
        // Direct Pipeline Managed fields default to all supported Base columns,
        // minus omit mappings; unsafe NUMBER requires string/omit (ADR-0023).
        documents.push(delivery_document_for_row(
            &row.data,
            &dataset.primary_key,
            &dataset.columns,
            pipeline,
        )?);
    }

    let delivered = upsert_managed_documents(mongo, &pipeline.target_collection, &documents)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    update_pipeline_delivery_progress(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        "delivered",
        Some(delivered as i32),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    println!(
        "Delivery complete: Pipeline {} → {}.{} ({} documents)",
        pipeline.name,
        deployment.target.database,
        pipeline.target_collection,
        delivered
    );
    Ok(())
}

async fn deliver_transform_pipeline(
    platform_store_url: &str,
    deployment: &Deployment,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
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
    let base_maps: Vec<_> = base_rows.iter().map(|r| r.data.clone()).collect();
    let derived_rows = evaluate_transform(&ops, &base_maps)
        .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    let derived_columns = derived_columns_for_ops(&base.columns, &ops, &derived_rows);
    let source_columns = stub_source_columns(&pipeline.source_table)?;
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

    println!(
        "Derived Dataset materialized: Pipeline {} ({} rows)",
        pipeline.name, dataset.row_count
    );

    let mut documents = Vec::with_capacity(derived_rows.len());
    for row in &derived_rows {
        documents.push(delivery_document_for_row(
            row,
            &pipeline.output_identity,
            &derived_columns,
            pipeline,
        )?);
    }

    let delivered = upsert_managed_documents(mongo, &pipeline.target_collection, &documents)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    update_pipeline_delivery_progress(
        platform_store_url,
        &pipeline.deployment_name,
        &pipeline.name,
        "delivered",
        Some(delivered as i32),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    println!(
        "Delivery complete: Pipeline {} → {}.{} ({} documents)",
        pipeline.name,
        deployment.target.database,
        pipeline.target_collection,
        delivered
    );
    Ok(())
}

/// Columns for a Derived Dataset: project/groupBy fields when present, else Base columns,
/// unioned with keys observed in Derived rows. Works for empty Derived results.
/// Aggregate `as` names inherit the source field's Oracle type metadata.
fn derived_columns_for_ops(
    base_columns: &[BaseColumn],
    ops: &[TransformOp],
    derived_rows: &[serde_json::Map<String, serde_json::Value>],
) -> Vec<BaseColumn> {
    let mut names = BTreeSet::new();
    if let Some(projected) = derived_projected_fields(ops) {
        names.extend(projected);
    } else {
        names.extend(base_columns.iter().map(|c| c.name.clone()));
    }
    for row in derived_rows {
        names.extend(row.keys().cloned());
    }
    let by_name: BTreeMap<&str, &BaseColumn> = base_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let mut alias_source: BTreeMap<String, String> = BTreeMap::new();
    for op in ops {
        if let TransformOp::GroupBy { aggregates, .. } = op {
            for agg in aggregates {
                alias_source.insert(agg.as_name.clone(), agg.field.clone());
            }
        }
    }
    names
        .into_iter()
        .map(|name| {
            if let Some(col) = by_name.get(name.as_str()) {
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

fn base_change_kind(op: ChangeOp) -> BaseChangeKind {
    match op {
        ChangeOp::Insert => BaseChangeKind::Insert,
        ChangeOp::Update => BaseChangeKind::Update,
        ChangeOp::Delete => BaseChangeKind::Delete,
    }
}

fn identity_map_matches_row(
    identity: &serde_json::Map<String, serde_json::Value>,
    row: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    identity.iter().all(|(key, expected)| {
        row.get(key).is_some_and(|actual| values_numerically_eq(actual, expected))
    })
}

fn values_numerically_eq(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    if left == right {
        return true;
    }
    match (json_as_f64(left), json_as_f64(right)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn json_as_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Incremental Transform maintenance for one Base change (Affect Analysis driven).
async fn maintain_transform_pipeline_for_change(
    platform_store_url: &str,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    base_columns: &[BaseColumn],
    base_rows: &[serde_json::Map<String, serde_json::Value>],
    change: &ChangeEvent,
    pre_apply: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), CliError> {
    let ops = transform_ops_from_pipeline(pipeline)?;
    let after = match change.op {
        ChangeOp::Insert | ChangeOp::Update => {
            Some(
                base_rows
                    .iter()
                    .find(|row| row_matches_identity(row, &change.identity))
                    .cloned()
                    .ok_or_else(|| {
                        CliError::Failed(format!(
                            "Base Dataset {} missing row for change identity {:?} after apply",
                            pipeline.source_table, change.identity
                        ))
                    })?,
            )
        }
        ChangeOp::Delete => None,
    };

    let outcome = analyze_affect(
        &ops,
        base_change_kind(change.op),
        pre_apply,
        after.as_ref(),
    )
    .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    match outcome {
        AffectOutcome::SkipUnusedFields => {
            println!(
                "Affect Analysis: Pipeline {} skipped (unused fields only)",
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
                base_columns,
                base_rows,
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
    base_rows: &[serde_json::Map<String, serde_json::Value>],
    ops: &[TransformOp],
    identities: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), CliError> {
    let recomputed = evaluate_transform_for_identities(ops, base_rows, identities)
        .map_err(|err| CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    let (mut dataset, existing_rows) = get_derived_rows(
        platform_store_url,
        &pipeline.name,
        Some(&pipeline.deployment_name),
    )
    .await
    .map_err(|err| CliError::Failed(err.to_string()))?;

    let mut merged: Vec<serde_json::Map<String, serde_json::Value>> = existing_rows
        .into_iter()
        .map(|r| r.data)
        .filter(|row| !identities.iter().any(|id| identity_map_matches_row(id, row)))
        .collect();
    merged.extend(recomputed.clone());

    let derived_columns = derived_columns_for_ops(base_columns, ops, &merged);
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
            .any(|row| identity_map_matches_row(identity, row));
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

    // Apply-time Managed validation before Initial Load / Delivery so unsafe NUMBER
    // and unsupported Managed inputs fail configure-time (ADR-0018 / ADR-0023).
    for pipeline in &pipelines {
        let source_columns = stub_source_columns(&pipeline.source_table)?;
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

    // ADR-0021: fail-fast Source Prerequisites before any capture (Initial Load).
    // Deployment-only apply (no Pipeline tables) does not open LogMiner yet.
    let source_tables = pipeline_source_tables(&pipelines);
    if deployment.source.kind.eq_ignore_ascii_case("oracle") && !source_tables.is_empty() {
        ensure_oracle_source_prerequisites(&deployment.source, &source_tables)?;
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

    // Runtime add (ADR-0007): keep already-running Pipelines' Delivery progress.
    preserve_unchanged_pipeline_delivery(&existing_pipelines, &mut pipelines);

    upsert_deployment(platform_store_url, &deployment)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    replace_pipelines(platform_store_url, &deployment.name, &pipelines)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    // Table-level Initial Load only for newly referenced tables; existing Bases stay
    // on their incremental path (ADR-0019).
    sync_base_datasets_for_pipelines(platform_store_url, &deployment, &pipelines).await?;

    if !existing_pipelines.is_empty() {
        for (name, source_table) in &added_pipeline_summaries {
            println!("Runtime Pipeline add: {name} (source={source_table})");
        }
    }

    // Start Delivery only for Pipelines that need it; do not re-Deliver unchanged
    // already-delivered Pipelines (others keep running — ADR-0007 Add).
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
    let mut applied_this_run: u32 = 0;

    for deployment in &deployments {
        let deployment_pipelines: Vec<_> = pipelines
            .iter()
            .filter(|p| p.deployment_name == deployment.name)
            .cloned()
            .collect();
        if deployment_pipelines.is_empty() {
            continue;
        }

        let mut tables = BTreeSet::new();
        for pipeline in &deployment_pipelines {
            tables.insert((
                pipeline.source_schema.clone(),
                pipeline.source_table.clone(),
            ));
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

            let resume_from = match dataset.capture_checkpoint {
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

            let candidate_changes = match &capture {
                Some(capture) => capture
                    .fetch_changes(&table, resume_from)
                    .map_err(|err| CliError::Failed(err.to_string()))?,
                None => {
                    return Err(CliError::Failed(format!(
                        "Incremental Capture requires an Oracle Source System (LogMiner); \
                         got kind={}",
                        deployment.source.kind
                    )));
                }
            };
            let candidate_ids: Vec<String> = candidate_changes
                .iter()
                .map(|c| c.change_id.clone())
                .collect();
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
            let changes: Vec<ChangeEvent> = candidate_changes
                .into_iter()
                .filter(|c| unapplied_set.contains(&c.change_id))
                .collect();

            if changes.is_empty() {
                let status = if dataset.status == "initial_load_complete" {
                    dataset.status.clone()
                } else {
                    "incremental".to_string()
                };
                let caught_up = base_with_sync_progress(
                    &dataset,
                    status,
                    dataset.row_count,
                    dataset.sync_applied_changes,
                    dataset.capture_checkpoint,
                    0,
                );
                let rows: Vec<serde_json::Map<String, serde_json::Value>> =
                    base_rows.into_iter().map(|r| r.data).collect();
                replace_base_dataset(platform_store_url, &caught_up, &rows)
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
                println!(
                    "Incremental Capture: Base Dataset {table} resume from checkpoint — \
                     0 new changes (already applied; lag=0)"
                );
                continue;
            }

            let checkpoint_before = dataset
                .capture_checkpoint
                .unwrap_or(low_watermark_i64.saturating_sub(1));
            println!(
                "Incremental Capture: resuming Base Dataset {table} from checkpoint={checkpoint_before} \
                 (exclusive next={}, pending={}, low-watermark={low_watermark})",
                resume_from,
                changes.len()
            );

            let supported_names: BTreeSet<String> =
                dataset.columns.iter().map(|c| c.name.clone()).collect();
            let source_columns = stub_source_columns(&table)?;
            let configured_tz = source_timezone_opt(deployment);
            let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
                base_rows.into_iter().map(|r| r.data).collect();
            let mut sync_applied = dataset.sync_applied_changes;
            let total_pending = changes.len() as i32;

            for (index, change) in changes.iter().enumerate() {
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
                    if pipeline.target_collection.is_empty() || pipeline.source_table != table {
                        continue;
                    }

                    match pipeline.mode.as_str() {
                        "direct" => match change.op {
                            ChangeOp::Insert | ChangeOp::Update => {
                                let Some(base_row) = rows
                                    .iter()
                                    .find(|row| row_matches_identity(row, &change.identity))
                                else {
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
                                let upserted = upsert_managed_documents(
                                    &mongo,
                                    &pipeline.target_collection,
                                    &[document],
                                )
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                                update_pipeline_delivery_progress(
                                    platform_store_url,
                                    &pipeline.deployment_name,
                                    &pipeline.name,
                                    "delivered",
                                    Some(upserted as i32),
                                )
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                                println!(
                                    "Delivery complete: Pipeline {} upserts={upserted} deletes=0 \
                                     (checkpoint-bound)",
                                    pipeline.name
                                );
                            }
                            ChangeOp::Delete => {
                                let identity_map: serde_json::Map<String, serde_json::Value> = change
                                    .identity
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                let identity = output_identity_from_row(
                                    &identity_map,
                                    &dataset.primary_key,
                                )?;
                                let deleted = delete_documents_by_identity(
                                    &mongo,
                                    &pipeline.target_collection,
                                    &[identity],
                                )
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                                update_pipeline_delivery_progress(
                                    platform_store_url,
                                    &pipeline.deployment_name,
                                    &pipeline.name,
                                    "delivered",
                                    Some(deleted as i32),
                                )
                                .await
                                .map_err(|err| CliError::Failed(err.to_string()))?;
                                println!(
                                    "Delivery complete: Pipeline {} upserts=0 deletes={deleted} \
                                     (checkpoint-bound)",
                                    pipeline.name
                                );
                            }
                        },
                        "transform" => {
                            maintain_transform_pipeline_for_change(
                                platform_store_url,
                                pipeline,
                                &mongo,
                                &dataset.columns,
                                &rows,
                                change,
                                pre_apply.as_ref(),
                            )
                            .await?;
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
                let remaining = total_pending - (index as i32 + 1);
                let updated = base_with_sync_progress(
                    &dataset,
                    "incremental",
                    rows.len() as i32,
                    sync_applied,
                    Some(current_checkpoint),
                    remaining,
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

                applied_this_run += 1;
                println!(
                    "Incremental Capture: Base Dataset {table} applied change_id={} \
                     checkpoint={current_checkpoint} lag={remaining} rows={}",
                    change.change_id,
                    updated.row_count
                );

                if let Some(limit) = fail_after {
                    if applied_this_run >= limit {
                        return Err(CliError::Failed(format!(
                            "simulated process kill after {limit} durable checkpoint(s) \
                             (MIGRALOOP_SYNC_FAIL_AFTER_CHANGES); resume from Platform Store \
                             checkpoint={current_checkpoint}"
                        )));
                    }
                }
            }
        }
    }

    println!("Incremental Capture and Delivery complete");
    Ok(())
}

async fn print_status(platform_store_url: &str) -> Result<(), CliError> {
    match health(platform_store_url).await {
        PlatformStoreHealth::Healthy { schema_version } => {
            println!("Platform Store: healthy");
            println!("Schema version: {schema_version}");
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
            if pipeline.target_collection.is_empty() {
                println!(
                    "Pipeline: {} ({}) source={}",
                    pipeline.name, pipeline.mode, pipeline.source_table
                );
            } else {
                println!(
                    "Pipeline: {} ({}) source={} target={} Delivery: {}",
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
            if base.status == "initial_load_complete" {
                println!("  Initial Load complete");
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

    for pipeline in &pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        let delivery_health = match pipeline.delivery_status.as_str() {
            "delivered" => "ok",
            "pending" => "pending",
            _ => "unknown",
        };
        println!(
            "  Delivery Health: {} Pipeline={} status={} appliedChanges={}",
            delivery_health,
            pipeline.name,
            pipeline.delivery_status,
            pipeline.delivery_applied_changes
        );
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
        Command::Run { platform_store_url } => {
            apply_migrations(&platform_store_url).await?;
            println!("migraloop is running");
            // Keep the single app instance alive for the compose one-install setup.
            // Future slices attach Deployment runtime work here.
            std::future::pending::<()>().await;
            Ok(())
        }
    }
}
