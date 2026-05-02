mod cli;
mod client;
mod discovery;
mod interactive;
mod protocol;
mod server;
mod storage;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use protocol::{DEFAULT_CONTROL_PORT, DEFAULT_DISCOVERY_PORT};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        None | Some(Command::Interactive { .. }) => {
            let (discovery_port, timeout_ms, port) = match cli.command {
                Some(Command::Interactive {
                    discovery_port,
                    timeout_ms,
                    port,
                }) => (discovery_port, timeout_ms, port),
                _ => (DEFAULT_DISCOVERY_PORT, 1500, DEFAULT_CONTROL_PORT),
            };
            interactive::run_interactive(discovery_port, timeout_ms, port).await?;
        }
        Some(Command::Serve {
            bind,
            discovery_port,
            pairing_code,
        }) => {
            server::run_server(bind, discovery_port, pairing_code).await?;
        }
        Some(Command::Discover {
            discovery_port,
            timeout_ms,
        }) => {
            client::discover(discovery_port, timeout_ms).await?;
        }
        Some(Command::Connect {
            target,
            discovery_port,
            timeout_ms,
            port,
        }) => {
            client::connect_interactive(target, discovery_port, timeout_ms, port).await?;
        }
        Some(Command::Destinations { target, port }) => {
            client::print_destinations(&target, port).await?;
        }
        Some(Command::Send {
            target,
            source,
            destination,
            port,
            code,
            overwrite,
            no_progress,
            dry_run,
            jobs,
        }) => {
            client::send_path(
                &target,
                port,
                &source,
                &destination,
                code.as_deref(),
                overwrite,
                dry_run,
                jobs,
                !no_progress,
            )
            .await?;
        }
    }

    Ok(())
}
