//! Operator-facing CLI for the DB Sync Platform.

mod config;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use migraloop_capture::initial_load_stub;
use migraloop_platform_store::{
    base_dataset_exists, delete_base_datasets_not_in, get_base_rows, health, list_base_datasets,
    list_deployments, list_pipelines, migrate, replace_base_dataset, replace_pipelines,
    upsert_deployment, BaseColumn, BaseDataset, Deployment, OmittedColumn, Pipeline,
    PlatformStoreHealth, SecretRef, SecretRefKind, SystemConnection,
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
            columns,
            omitted_columns,
            row_count: rows.len() as i32,
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

    println!("Deployment applied: {}", deployment.name);
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
            println!(
                "Pipeline: {} ({}) source={}",
                pipeline.name, pipeline.mode, pipeline.source_table
            );
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
