//! Operator-facing CLI for the DB Sync Platform.

mod config;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use migraloop_platform_store::{
    health, list_deployments, migrate, upsert_deployment, Deployment, PlatformStoreHealth,
    SecretRef, SecretRefKind, SystemConnection,
};
use thiserror::Error;

use crate::config::{load_deployment_config, DeploymentDocument, ResolvedSecretRef};

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
    /// Report Platform Store reachability, health, and applied Deployments
    Status {
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

fn document_to_deployment(doc: DeploymentDocument) -> Result<Deployment, CliError> {
    // Resolve to validate references exist; never persist resolved secret values.
    let _ = doc.spec.source.password.resolve("source.password")?;
    let _ = doc.spec.target.password.resolve("target.password")?;
    let source_password_ref =
        secret_ref_from_resolved(doc.spec.source.password.resolved_ref("source.password")?);
    let target_password_ref =
        secret_ref_from_resolved(doc.spec.target.password.resolved_ref("target.password")?);

    Ok(Deployment {
        name: doc.metadata.name,
        source: SystemConnection {
            kind: doc.spec.source.kind,
            host: doc.spec.source.host,
            port: doc.spec.source.port,
            database: doc.spec.source.database,
            username: doc.spec.source.username,
            password_ref: source_password_ref,
        },
        target: SystemConnection {
            kind: doc.spec.target.kind,
            host: doc.spec.target.host,
            port: doc.spec.target.port,
            database: doc.spec.target.database,
            username: doc.spec.target.username,
            password_ref: target_password_ref,
        },
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

async fn apply_deployment(platform_store_url: &str, file: &PathBuf) -> Result<(), CliError> {
    match health(platform_store_url).await {
        PlatformStoreHealth::Healthy { .. } => {}
        PlatformStoreHealth::Unhealthy { reason } => {
            return Err(CliError::Failed(format!(
                "Platform Store is not healthy; run `migraloop migrate` first: {reason}"
            )));
        }
        PlatformStoreHealth::Unreachable { reason } => {
            return Err(CliError::Failed(format!(
                "Platform Store is unreachable: {reason}"
            )));
        }
    }

    let doc = load_deployment_config(file)?;
    let deployment = document_to_deployment(doc)?;
    upsert_deployment(platform_store_url, &deployment)
        .await
        .map_err(|err| CliError::Failed(err.to_string()))?;
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
        for deployment in deployments {
            println!("Deployment: {}", deployment.name);
            println!("{}", format_system_line("Source", &deployment.source));
            println!("{}", format_system_line("Target", &deployment.target));
        }
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
