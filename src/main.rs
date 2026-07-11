mod cli;
mod client;
mod discovery;
mod interactive;
mod picker;
mod protocol;
mod server;
mod storage;
mod ui;
mod updater;
mod util;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use protocol::{DEFAULT_CONTROL_PORT, DEFAULT_DISCOVERY_PORT};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result = run(cli).await;
    let _ = console::Term::stdout().show_cursor();
    if let Err(err) = result {
        ui::fatal(&format!("{err:#}"));
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        None => {
            interactive::run_peer_mode(
                DEFAULT_DISCOVERY_PORT,
                1500,
                DEFAULT_CONTROL_PORT,
                cli.open,
            )
            .await?;
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
            open,
        }) => {
            let code = server::ensure_pairing_code(pairing_code);
            let device = util::local_device_info();
            let mut screen = picker::StatusScreen::new()?;
            let mut details = vec![
                ("host".into(), device.host_name),
                ("platform".into(), format!("{} {}", device.os, device.arch)),
                (
                    "listening".into(),
                    format!("tcp {bind}  ·  udp {discovery_port}"),
                ),
                (
                    "pairing code".into(),
                    if open {
                        "off (--open)".into()
                    } else {
                        code.clone()
                    },
                ),
            ];
            if open {
                details.push((
                    "security".into(),
                    "anyone on this network can send files".into(),
                ));
            }
            screen.render(
                "Receiver",
                "Waiting for senders…",
                if open {
                    picker::Tone::Warning
                } else {
                    picker::Tone::Info
                },
                &details,
                "Ctrl-C  stop",
            )?;
            server::run_server(bind, discovery_port, code, false, !open).await?;
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
        Some(Command::Update { check, yes }) => {
            updater::run(check, yes)?;
        }
    }

    Ok(())
}
