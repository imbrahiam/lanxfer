mod cli;
mod client;
mod discovery;
mod interactive;
mod protocol;
mod server;
mod storage;
mod ui;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use protocol::{DEFAULT_CONTROL_PORT, DEFAULT_DISCOVERY_PORT};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        None => {
            interactive::run_peer_mode(DEFAULT_DISCOVERY_PORT, 1500, DEFAULT_CONTROL_PORT).await?;
        }
        Some(Command::Interactive {
            discovery_port,
            timeout_ms,
            port,
        }) => {
            interactive::run_interactive(discovery_port, timeout_ms, port).await?;
        }
        Some(Command::Serve {
            bind,
            discovery_port,
            pairing_code,
        }) => {
            let code = server::ensure_pairing_code(pairing_code);
            let device = util::local_device_info();
            ui::banner();
            ui::section("Receiver");
            ui::kv("host", &device.host_name);
            ui::kv("platform", &format!("{} {}", device.os, device.arch));
            ui::kv("listening", &format!("tcp {bind}  ·  udp {discovery_port}"));
            ui::kv("pairing code", &ui::yellow(&code));
            println!();
            ui::info("waiting for senders…  (Ctrl-C to stop)");
            server::run_server(bind, discovery_port, code, false).await?;
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
