//! Minimal Operator CLI surface fixture for handbook-guard black-box tests.

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "migraloop")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Migrate {},
    Apply {},
}
