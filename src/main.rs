mod cli;
mod client;
mod discovery;
mod protocol;
mod server;
mod storage;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve {
            bind,
            discovery_port,
            pairing_code,
        } => {
            server::run_server(bind, discovery_port, pairing_code).await?;
        }
        Command::Discover {
            discovery_port,
            timeout_ms,
        } => {
            client::discover(discovery_port, timeout_ms).await?;
        }
        Command::Destinations { target, port } => {
            client::print_destinations(&target, port).await?;
        }
        Command::Send {
            target,
            source,
            destination,
            port,
            code,
            overwrite,
            no_progress,
            dry_run,
            jobs,
        } => {
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
