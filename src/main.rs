mod cli;
mod client;
mod discovery;
mod interactive;
mod picker;
mod progress;
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

/// File logging for debugging the TUI (stdout/stderr are unusable in raw
/// mode). `LANXFER_LOG=debug lanxfer` → /tmp/lanxfer.log, or set
/// LANXFER_LOG_FILE for a custom path.
fn init_logging() {
    let Ok(level) = std::env::var("LANXFER_LOG") else {
        return;
    };
    let level = level.parse().unwrap_or(log::LevelFilter::Debug);
    let path = std::env::var("LANXFER_LOG_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("lanxfer.log"));
    if let Ok(file) = std::fs::File::create(&path) {
        let _ = simplelog::WriteLogger::init(level, simplelog::Config::default(), file);
        log::info!("lanxfer {} logging at {level}", env!("CARGO_PKG_VERSION"));
    }
}

#[tokio::main]
async fn main() {
    init_logging();
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
            let listener = tokio::net::TcpListener::bind(&bind).await.map_err(|e| {
                anyhow::anyhow!("cannot listen on {bind}: {e} (another lanxfer running?)")
            })?;
            server::run_server(
                listener,
                discovery_port,
                code,
                false,
                !open,
                None,
                server::PullTokens::default(),
                std::sync::Arc::new(progress::Progress::default()),
            )
            .await?;
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
