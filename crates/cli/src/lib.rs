//! Operator-facing CLI for the DB Sync Platform.

use clap::{Parser, Subcommand};
use migraloop_platform_store::{health, migrate, PlatformStoreHealth};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Message(String),
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
    /// Report Platform Store reachability and health
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

pub async fn run(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Migrate { platform_store_url } => {
            migrate(&platform_store_url)
                .await
                .map_err(|err| CliError::Message(err.to_string()))?;
            println!("Platform Store migrations applied");
            Ok(())
        }
        Command::Status { platform_store_url } => match health(&platform_store_url).await {
            PlatformStoreHealth::Healthy { schema_version } => {
                println!("Platform Store: healthy");
                println!("Schema version: {schema_version}");
                Ok(())
            }
            PlatformStoreHealth::Unreachable { reason } => {
                println!("Platform Store: unreachable");
                eprintln!("{reason}");
                Err(CliError::Message(
                    "Platform Store is unreachable or not migrated".to_string(),
                ))
            }
        },
        Command::Run { platform_store_url } => {
            migrate(&platform_store_url)
                .await
                .map_err(|err| CliError::Message(err.to_string()))?;
            println!("Platform Store migrations applied");
            println!("migraloop is running");
            // Keep the single app instance alive for the compose one-install setup.
            // Future slices attach Deployment runtime work here.
            std::future::pending::<()>().await;
            Ok(())
        }
    }
}
