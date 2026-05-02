use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::protocol::{DEFAULT_CONTROL_PORT, DEFAULT_DISCOVERY_PORT};

#[derive(Debug, Parser)]
#[command(
    name = "lanxfer",
    version,
    about = "Fast resumable LAN file transfer CLI"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run receiver server on this machine.
    Serve {
        #[arg(long, default_value = "0.0.0.0:44818")]
        bind: String,
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long)]
        pairing_code: Option<String>,
    },
    /// Discover receivers in local network.
    Discover {
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long, default_value_t = 1500)]
        timeout_ms: u64,
    },
    /// List destination drives/paths exposed by a receiver.
    Destinations {
        target: String,
        #[arg(long, default_value_t = DEFAULT_CONTROL_PORT)]
        port: u16,
    },
    /// Send a file to a receiver.
    Send {
        target: String,
        source: PathBuf,
        destination: PathBuf,
        #[arg(long, default_value_t = DEFAULT_CONTROL_PORT)]
        port: u16,
        #[arg(long)]
        code: Option<String>,
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        no_progress: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        jobs: Option<usize>,
    },
}
