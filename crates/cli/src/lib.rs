//! Operator-facing CLI for the DB Sync Platform.

mod config;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use migraloop_capture::{incremental_changes_stub, initial_load_stub, ChangeEvent, ChangeOp};
use migraloop_delivery::{
    delete_documents_by_identity, list_target_documents, upsert_managed_documents, DeliveryDocument,
    MongoTargetConnection,
};
use migraloop_platform_store::{
    base_dataset_exists, delete_base_datasets_not_in, get_base_rows, health, list_base_datasets,
    list_deployments, list_pipelines, migrate, replace_base_dataset, replace_pipelines,
    update_base_primary_key, update_pipeline_delivery_progress, upsert_deployment, BaseColumn,
    BaseDataset, Deployment, OmittedColumn, Pipeline, PlatformStoreHealth, SecretRef,
    SecretRefKind, SystemConnection,
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
        },
        target: SystemConnection {
            kind: doc.spec.target.kind.clone(),
            host: doc.spec.target.host.clone(),
            port: doc.spec.target.port,
            database: doc.spec.target.database.clone(),
            username: doc.spec.target.username.clone(),
            password_ref: target_password_ref,
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
    primary_key: &[String],
) -> Result<serde_json::Value, CliError> {
    if primary_key.is_empty() {
        return Err(CliError::Failed(
            "Base Dataset has no primary key for Output Identity".to_string(),
        ));
    }
    if primary_key.len() == 1 {
        let key = &primary_key[0];
        return row.get(key).cloned().ok_or_else(|| {
            CliError::Failed(format!(
                "Base row missing Output Identity column {key}"
            ))
        });
    }
    let mut identity = serde_json::Map::new();
    for key in primary_key {
        let value = row.get(key).cloned().ok_or_else(|| {
            CliError::Failed(format!(
                "Base row missing Output Identity column {key}"
            ))
        })?;
        identity.insert(key.clone(), value);
    }
    Ok(serde_json::Value::Object(identity))
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
    format!(
        "  {label}: {} {}:{} database={} username={} passwordRef={}",
        system.kind,
        system.host,
        system.port,
        system.database,
        system.username,
        system.password_ref.display()
    )
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
    deployment_name: &str,
    pipelines: &[Pipeline],
) -> Result<(), CliError> {
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
            ensure_base_primary_key(platform_store_url, deployment_name, &schema, &table).await?;
            continue;
        }

        let snapshot = initial_load_stub(&table).map_err(|err| CliError::Failed(err.to_string()))?;

        let columns: Vec<BaseColumn> = snapshot
            .supported_columns()
            .into_iter()
            .map(|c| BaseColumn {
                name: c.name.clone(),
                oracle_type: c.oracle_type.clone(),
            })
            .collect();
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
        };

        replace_base_dataset(platform_store_url, &dataset, &rows)
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;

        println!(
            "Initial Load complete: Base Dataset {table} ({} rows)",
            dataset.row_count
        );
    }

    Ok(())
}

async fn ensure_base_primary_key(
    platform_store_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
) -> Result<(), CliError> {
    let (dataset, _) = get_base_rows(platform_store_url, source_table, Some(deployment_name))
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    if !dataset.primary_key.is_empty() {
        return Ok(());
    }

    let snapshot =
        initial_load_stub(source_table).map_err(|err| CliError::Failed(err.to_string()))?;
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

async fn deliver_direct_pipelines(
    platform_store_url: &str,
    deployment: &Deployment,
    pipelines: &[Pipeline],
) -> Result<(), CliError> {
    let needs_delivery = pipelines
        .iter()
        .any(|p| p.mode == "direct" && !p.target_collection.is_empty());
    if !needs_delivery {
        return Ok(());
    }

    let mongo = mongo_target_from_deployment(deployment)?;

    for pipeline in pipelines {
        if pipeline.mode != "direct" || pipeline.target_collection.is_empty() {
            continue;
        }

        let (dataset, rows) = get_base_rows(
            platform_store_url,
            &pipeline.source_table,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

        let mut documents = Vec::with_capacity(rows.len());
        for row in &rows {
            let identity = output_identity_from_row(&row.data, &dataset.primary_key)?;
            // Direct Pipeline Managed fields default to all supported Base columns.
            let managed_fields = row.data.clone();
            documents.push(DeliveryDocument {
                identity,
                managed_fields,
            });
        }

        let delivered = upsert_managed_documents(&mongo, &pipeline.target_collection, &documents)
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
    }

    Ok(())
}

async fn apply_deployment(platform_store_url: &str, file: &Path) -> Result<(), CliError> {
    ensure_store_healthy(platform_store_url).await?;

    let doc = load_deployment_config(file)?;
    let deployment = document_to_deployment(&doc)?;
    let pipelines = pipelines_from_document(&doc);

    upsert_deployment(platform_store_url, &deployment)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
    replace_pipelines(platform_store_url, &deployment.name, &pipelines)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;

    sync_base_datasets_for_pipelines(platform_store_url, &deployment.name, &pipelines).await?;
    deliver_direct_pipelines(platform_store_url, &deployment, &pipelines).await?;

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
) -> Result<serde_json::Map<String, serde_json::Value>, CliError> {
    let Some(row) = &change.row else {
        return Err(CliError::Failed(format!(
            "Incremental {:?} change for {:?} is missing row data",
            change.op, change.identity
        )));
    };
    Ok(row
        .iter()
        .filter(|(name, _)| supported_names.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect())
}

fn apply_change_events_to_base_rows(
    rows: &mut Vec<serde_json::Map<String, serde_json::Value>>,
    changes: &[ChangeEvent],
    supported_names: &BTreeSet<String>,
) -> Result<(), CliError> {
    for change in changes {
        match change.op {
            ChangeOp::Insert | ChangeOp::Update => {
                let managed = supported_row_from_change(change, supported_names)?;
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

        // Incremental Capture into Base first; Delivery reads Base (and delete identities).
        let mut captured_by_table: Vec<(String, Vec<ChangeEvent>)> = Vec::new();

        for (schema, table) in tables {
            let changes = incremental_changes_stub(&table)
                .map_err(|err| CliError::Failed(err.to_string()))?;
            if changes.is_empty() {
                continue;
            }

            let (dataset, base_rows) =
                get_base_rows(platform_store_url, &table, Some(&deployment.name))
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;

            let supported_names: BTreeSet<String> =
                dataset.columns.iter().map(|c| c.name.clone()).collect();
            let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
                base_rows.into_iter().map(|r| r.data).collect();

            apply_change_events_to_base_rows(&mut rows, &changes, &supported_names)?;

            let updated = BaseDataset {
                deployment_name: deployment.name.clone(),
                source_table: table.clone(),
                source_schema: schema,
                status: "incremental".to_string(),
                primary_key: dataset.primary_key.clone(),
                columns: dataset.columns.clone(),
                omitted_columns: dataset.omitted_columns.clone(),
                row_count: rows.len() as i32,
                sync_applied_changes: dataset.sync_applied_changes + changes.len() as i32,
                sync_health: "ok".to_string(),
            };

            replace_base_dataset(platform_store_url, &updated, &rows)
                .await
                .map_err(|err| CliError::Failed(err.to_string()))?;

            println!(
                "Incremental Capture: Base Dataset {table} applied {} changes (rows={})",
                changes.len(),
                updated.row_count
            );
            captured_by_table.push((table, changes));
        }

        let mongo = mongo_target_from_deployment(deployment)?;
        for pipeline in &deployment_pipelines {
            if pipeline.mode != "direct" || pipeline.target_collection.is_empty() {
                continue;
            }

            let Some((_, changes)) = captured_by_table
                .iter()
                .find(|(table, _)| table == &pipeline.source_table)
            else {
                continue;
            };

            // Direct Pipeline Delivery: upserts from current Base rows; deletes by identity.
            let (dataset, base_rows) = get_base_rows(
                platform_store_url,
                &pipeline.source_table,
                Some(&pipeline.deployment_name),
            )
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;

            let mut upserts = Vec::new();
            let mut deletes = Vec::new();
            for change in changes {
                match change.op {
                    ChangeOp::Insert | ChangeOp::Update => {
                        let Some(base_row) = base_rows
                            .iter()
                            .find(|row| row_matches_identity(&row.data, &change.identity))
                        else {
                            return Err(CliError::Failed(format!(
                                "Base Dataset {} missing row for Output Identity {:?}",
                                pipeline.source_table, change.identity
                            )));
                        };
                        let identity =
                            output_identity_from_row(&base_row.data, &dataset.primary_key)?;
                        upserts.push(DeliveryDocument {
                            identity,
                            managed_fields: base_row.data.clone(),
                        });
                    }
                    ChangeOp::Delete => {
                        let identity_map: serde_json::Map<String, serde_json::Value> = change
                            .identity
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        deletes.push(output_identity_from_row(
                            &identity_map,
                            &dataset.primary_key,
                        )?);
                    }
                }
            }

            let upserted =
                upsert_managed_documents(&mongo, &pipeline.target_collection, &upserts)
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;
            let deleted =
                delete_documents_by_identity(&mongo, &pipeline.target_collection, &deletes)
                    .await
                    .map_err(|err| CliError::Failed(err.to_string()))?;

            let applied = (upserted + deleted) as i32;
            update_pipeline_delivery_progress(
                platform_store_url,
                &pipeline.deployment_name,
                &pipeline.name,
                "delivered",
                Some(applied),
            )
            .await
            .map_err(|err| CliError::Failed(err.to_string()))?;

            println!(
                "Delivery complete: Pipeline {} upserts={} deletes={} (from Base)",
                pipeline.name, upserted, deleted
            );
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
            println!(
                "  Sync Health: {} appliedChanges={}",
                base.sync_health, base.sync_applied_changes
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
