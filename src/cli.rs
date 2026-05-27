use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::protocol::{DEFAULT_CONTROL_PORT, DEFAULT_DISCOVERY_PORT};

#[derive(Debug, Parser)]
#[command(
    name = "lanxfer",
    version,
    about = "Fast resumable LAN file transfer CLI",
    long_about = "Fast resumable LAN file transfer CLI.\n\nRun with no arguments to enter peer mode: a background receiver starts, other peers on the LAN are auto-discovered, and you can pick one and send."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Sender-only interactive session (does not start a local receiver).
    Interactive {
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long, default_value_t = 1500)]
        timeout_ms: u64,
        #[arg(long, default_value_t = DEFAULT_CONTROL_PORT)]
        port: u16,
    },
    /// Run a headless receiver (no interactive UI).
    Serve {
        #[arg(long, default_value = "0.0.0.0:44818")]
        bind: String,
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long)]
        pairing_code: Option<String>,
    },
    /// Discover receivers across all network interfaces.
    Discover {
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long, default_value_t = 1500)]
        timeout_ms: u64,
    },
    /// Discover and interact with a receiver (interactive or direct).
    Connect {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
        discovery_port: u16,
        #[arg(long, default_value_t = 1500)]
        timeout_ms: u64,
        #[arg(long, default_value_t = DEFAULT_CONTROL_PORT)]
        port: u16,
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
