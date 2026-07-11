use anyhow::{Result, bail};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::client;
use crate::discovery;
use crate::protocol::{
    ControlMessage, DestinationInfo, PROTOCOL_VERSION, read_control, send_control,
};
use crate::server;
use crate::util;

pub struct TransferRecord {
    pub peer: String,
    pub source: String,
    pub dest: String,
    pub files: usize,
    pub skipped: usize,
    pub bytes: u64,
    pub ok: bool,
}

/// Run a Select, return the chosen index; Esc returns None (go back).
/// The answered line is cleared so looping menus don't accumulate residue.
fn select(
    screen: &mut crate::picker::StatusScreen,
    prompt: &str,
    items: Vec<String>,
    cursor: usize,
    help: &str,
) -> Result<Option<usize>> {
    screen.choose(prompt, items, cursor, help)
}

fn text(
    screen: &mut crate::picker::StatusScreen,
    prompt: &str,
    help: &str,
    secret: bool,
) -> Result<Option<String>> {
    screen.input(prompt, prompt, help, secret)
}

fn confirm(screen: &mut crate::picker::StatusScreen, prompt: &str, default: bool) -> Result<bool> {
    let items = if default {
        vec!["Yes".to_string(), "No".to_string()]
    } else {
        vec!["No".to_string(), "Yes".to_string()]
    };
    let selection = screen.choose(prompt, items, 0, "↑↓ move · enter select · esc cancel")?;
    Ok(match selection {
        Some(index) => (default && index == 0) || (!default && index == 1),
        None => false,
    })
}

const NAV_HELP: &str = "↑↓ move · type to filter · enter select · esc back";

pub async fn run_peer_mode(
    discovery_port: u16,
    timeout_ms: u64,
    port: u16,
    open: bool,
) -> Result<()> {
    let mut screen = crate::picker::StatusScreen::new()?;

    let device = util::local_device_info();
    let pairing_code = util::generate_pairing_code();
    let picker_title = if open {
        format!("{}  ·  open", device.host_name)
    } else {
        format!("{}  ·  code {pairing_code}", device.host_name)
    };

    let bind = format!("0.0.0.0:{port}");
    let server_code = pairing_code.clone();
    tokio::spawn(async move {
        if let Err(err) = server::run_server(bind, discovery_port, server_code, true, !open).await {
            let _ = err;
        }
    });

    peer_loop(
        &mut screen,
        discovery_port,
        timeout_ms,
        port,
        true,
        &picker_title,
    )
    .await
}

pub async fn run_interactive(discovery_port: u16, timeout_ms: u64, port: u16) -> Result<()> {
    let mut screen = crate::picker::StatusScreen::new()?;
    peer_loop(
        &mut screen,
        discovery_port,
        timeout_ms,
        port,
        false,
        "Pick a peer",
    )
    .await
}

/// Discover peers, pick one, run a session. Repeats until the user quits.
/// Pairing codes and transfer history persist across sessions.
async fn peer_loop(
    screen: &mut crate::picker::StatusScreen,
    discovery_port: u16,
    timeout_ms: u64,
    default_port: u16,
    exclude_self: bool,
    picker_title: &str,
) -> Result<()> {
    let mut codes: HashMap<String, String> = HashMap::new();
    let mut transfers: Vec<TransferRecord> = Vec::new();
    let local_ips = local_ipv4s();
    let my_host = util::host_name();

    loop {
        let scan_ips = local_ips.clone();
        let scan_host = my_host.clone();
        let scan = async move {
            let mut hosts =
                discovery::discover_hosts_with_fallback(discovery_port, timeout_ms).await?;
            if exclude_self {
                hosts.retain(|h| !scan_ips.contains(&h.ip) && h.reply.host != scan_host);
            }
            Ok::<Vec<discovery::DiscoveredHost>, anyhow::Error>(hosts)
        };
        screen.render(
            picker_title,
            "Scanning for nearby peers…",
            crate::picker::Tone::Info,
            &[],
            "Automatic discovery is active",
        )?;
        let hosts = scan.await?;
        if hosts.is_empty() {
            screen.render(
                picker_title,
                "No peers found yet",
                crate::picker::Tone::Info,
                &[],
                "Rescanning automatically…  ·  esc quit",
            )?;
            if matches!(
                screen.poll_key(std::time::Duration::from_millis(700))?,
                Some(crossterm::event::KeyCode::Esc)
            ) {
                return Ok(());
            }
            continue;
        }
        let mut items: Vec<String> = hosts
            .iter()
            .map(|h| {
                format!(
                    "{:<20}  {}:{}  {} {}",
                    h.reply.host,
                    h.ip,
                    h.reply.control_port,
                    h.reply.device.os,
                    h.reply.device.arch
                )
            })
            .collect();
        items.push("Enter IP manually".to_string());
        items.push("↻ Rescan".to_string());
        items.push("✗ Quit".to_string());
        let selection = if hosts.len() == 1 {
            screen.render(
                picker_title,
                &format!("Connecting to {}…", hosts[0].reply.host),
                crate::picker::Tone::Info,
                &[("address".into(), hosts[0].ip.clone())],
                "Peer discovered automatically",
            )?;
            Some(0)
        } else {
            select(screen, picker_title, items, 0, NAV_HELP)?
        };
        let Some(sel) = selection else {
            return Ok(()); // esc at top level = quit
        };
        let items_len = hosts.len() + 3;

        let (ip, control_port, label) = if sel == items_len - 1 {
            return Ok(());
        } else if sel == items_len - 2 {
            continue;
        } else if sel == items_len - 3 {
            let Some(ip) = text(screen, "Receiver IP", "esc to go back", false)? else {
                continue;
            };
            let ip = ip.trim().to_string();
            (ip.clone(), default_port, ip)
        } else {
            let h = &hosts[sel];
            (h.ip.clone(), h.reply.control_port, h.reply.host.clone())
        };

        match peer_session(
            screen,
            &ip,
            control_port,
            &label,
            &mut codes,
            &mut transfers,
        )
        .await
        {
            Ok(true) => return Ok(()), // user chose quit
            Ok(false) => {}            // switch peer -> rescan
            Err(err) => {
                screen.render(
                    "Connection",
                    &format!("{err:#}"),
                    crate::picker::Tone::Error,
                    &[],
                    "Rescanning automatically…",
                )?;
                tokio::time::sleep(std::time::Duration::from_millis(900)).await;
            }
        }
    }
}

/// One connected session with a peer. Stays connected across sends —
/// no rediscovery, no re-entering the pairing code.
/// Returns Ok(true) if the user wants to quit entirely.
async fn peer_session(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    label: &str,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
) -> Result<bool> {
    let (_device, auth_required) = handshake_info(ip, port).await?;

    let mut last_dest: Option<String> = None;

    loop {
        enum Action {
            Send,
            SendAgain,
            Drives,
            History,
            SwitchPeer,
            Quit,
        }
        let mut menu: Vec<(String, Action)> = vec![("Send files".to_string(), Action::Send)];
        if let Some(dest) = &last_dest {
            menu.push((format!("Send more to {dest}"), Action::SendAgain));
        }
        menu.push(("List remote drives".to_string(), Action::Drives));
        menu.push((
            format!("Transfers this session ({})", transfers.len()),
            Action::History,
        ));
        menu.push(("Switch peer".to_string(), Action::SwitchPeer));
        menu.push(("Quit".to_string(), Action::Quit));

        let labels: Vec<String> = menu.iter().map(|(l, _)| l.clone()).collect();
        let Some(sel) = select(
            screen,
            &format!("{label} · what next?"),
            labels,
            0,
            "↑↓ move · enter select · esc switch peer",
        )?
        else {
            return Ok(false); // esc = back to peer list
        };

        let result = match &menu[sel].1 {
            Action::Send => {
                send_flow(
                    screen,
                    ip,
                    port,
                    label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_dest,
                    None,
                )
                .await
            }
            Action::SendAgain => {
                let dest = last_dest.clone();
                send_flow(
                    screen,
                    ip,
                    port,
                    label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_dest,
                    dest,
                )
                .await
            }
            Action::Drives => list_drives(screen, ip, port).await,
            Action::History => print_transfers(screen, transfers),
            Action::SwitchPeer => return Ok(false),
            Action::Quit => return Ok(true),
        };

        if let Err(err) = result {
            let msg = format!("{err:#}");
            screen.render(
                "Transfer",
                &msg,
                crate::picker::Tone::Error,
                &[],
                "enter / esc  continue",
            )?;
            screen.wait_for_close()?;
            if msg.contains("pairing code") {
                // wrong/stale code — forget it so the next attempt re-prompts
                codes.remove(ip);
            }
        }
    }
}

async fn handshake_info(ip: &str, port: u16) -> Result<(crate::protocol::DeviceInfo, bool)> {
    let addr = format!("{ip}:{port}");
    let mut s = tokio::net::TcpStream::connect(&addr).await?;
    s.set_nodelay(true)?;
    send_control(
        &mut s,
        &ControlMessage::Hello {
            version: PROTOCOL_VERSION,
            client_name: util::host_name(),
        },
    )
    .await?;
    match read_control(&mut s).await? {
        ControlMessage::HelloAck {
            version,
            server,
            auth_required,
        } if version == PROTOCOL_VERSION => Ok((server, auth_required)),
        ControlMessage::HelloAck { version, .. } => {
            bail!("protocol version mismatch: {version}")
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected handshake response: {other:?}"),
    }
}

fn ensure_code(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    auth_required: bool,
    codes: &mut HashMap<String, String>,
) -> Result<Option<Option<String>>> {
    if !auth_required {
        return Ok(Some(None));
    }
    if let Some(code) = codes.get(ip) {
        return Ok(Some(Some(code.clone())));
    }
    let Some(code) = text(
        screen,
        "Pairing code",
        "shown on the other machine · esc cancels",
        true,
    )?
    else {
        return Ok(None);
    };
    let code = code.trim().to_uppercase();
    codes.insert(ip.to_string(), code.clone());
    Ok(Some(Some(code)))
}

fn local_ipv4s() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if let if_addrs::IfAddr::V4(v4) = iface.addr {
                ips.push(v4.ip.to_string());
            }
        }
    }
    ips
}

async fn list_drives(screen: &mut crate::picker::StatusScreen, ip: &str, port: u16) -> Result<()> {
    let items = fetch_destinations(ip, port).await?;
    let details: Vec<(String, String)> = items
        .iter()
        .map(|item| {
            let mode = if item.read_only {
                "read-only"
            } else {
                "writable"
            };
            (
                item.label.clone(),
                format!(
                    "{}  ·  {} free  ·  {mode}",
                    item.path,
                    util::format_size(item.available_bytes)
                ),
            )
        })
        .collect();
    screen.render(
        "Remote drives",
        &format!("{} destination(s)", items.len()),
        crate::picker::Tone::Info,
        &details,
        "enter / esc  close",
    )?;
    screen.wait_for_close()
}

async fn fetch_destinations(ip: &str, port: u16) -> Result<Vec<DestinationInfo>> {
    let (mut stream, _) = client::connect_and_handshake(ip, port).await?;
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;

    match read_control(&mut stream).await? {
        ControlMessage::Destinations { items } => Ok(items),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_flow(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    peer_label: &str,
    auth_required: bool,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
    last_dest: &mut Option<String>,
    reuse_dest: Option<String>,
) -> Result<()> {
    let Some(auth_code) = ensure_code(screen, ip, auth_required, codes)? else {
        return Ok(());
    };
    let auth = auth_code.as_deref();

    let remote_path = match reuse_dest {
        Some(dest) => dest,
        None => {
            let destinations = fetch_destinations(ip, port).await?;
            let writable: Vec<&DestinationInfo> =
                destinations.iter().filter(|d| !d.read_only).collect();
            if writable.is_empty() {
                bail!("no writable drives found on receiver");
            }

            let drive_items: Vec<String> = writable
                .iter()
                .map(|d| {
                    format!(
                        "{:<12} {:<32} {} free",
                        d.label,
                        d.path,
                        util::format_size(d.available_bytes)
                    )
                })
                .collect();

            let Some(drive_idx) = select(screen, "Destination drive", drive_items, 0, NAV_HELP)?
            else {
                return Ok(());
            };

            match browse_remote_dir(screen, ip, port, &writable[drive_idx].path, auth).await? {
                Some(path) => path,
                None => return Ok(()),
            }
        }
    };

    let local_paths = match pick_local_paths(screen)? {
        Some(paths) if !paths.is_empty() => paths,
        _ => return Ok(()),
    };

    if !confirm(screen, "Start transfer?", true)? {
        return Ok(());
    }

    let source_label = if local_paths.len() == 1 {
        local_paths[0].display().to_string()
    } else {
        format!(
            "{} (+{} more)",
            local_paths[0].display(),
            local_paths.len() - 1
        )
    };
    screen.render(
        "Transfer",
        "Sending files…",
        crate::picker::Tone::Info,
        &[
            ("destination".into(), remote_path.clone()),
            ("source".into(), source_label.clone()),
        ],
        "Transfer is active · resumable if interrupted",
    )?;
    let result = client::send_session(
        ip,
        port,
        &local_paths,
        &remote_path,
        auth,
        client::SendOptions {
            overwrite: false,
            dry_run: false,
            jobs: None,
            show_progress: false,
        },
    )
    .await;

    let record = match &result {
        Ok(s) => TransferRecord {
            peer: peer_label.to_string(),
            source: source_label,
            dest: remote_path.clone(),
            files: s.transferred,
            skipped: s.skipped_up_to_date + s.conflicts,
            bytes: s.bytes,
            ok: s.errors.is_empty(),
        },
        Err(_) => TransferRecord {
            peer: peer_label.to_string(),
            source: source_label,
            dest: remote_path.clone(),
            files: 0,
            skipped: 0,
            bytes: 0,
            ok: false,
        },
    };
    transfers.push(record);
    *last_dest = Some(remote_path);

    let summary = result?;
    let mut details = vec![("transferred".into(), summary.transferred.to_string())];
    if summary.skipped_up_to_date > 0 {
        details.push(("up to date".into(), summary.skipped_up_to_date.to_string()));
    }
    if summary.conflicts > 0 {
        details.push((
            "conflicts".into(),
            format!("{} already exist on receiver", summary.conflicts),
        ));
    }
    if !summary.errors.is_empty() {
        for err in &summary.errors {
            details.push(("error".into(), err.clone()));
        }
        bail!("some transfers failed — send again to retry (transfers resume)");
    }
    screen.render(
        "Transfer",
        "All transfers complete",
        crate::picker::Tone::Success,
        &details,
        "enter / esc  continue",
    )?;
    screen.wait_for_close()
}

fn print_transfers(
    screen: &mut crate::picker::StatusScreen,
    transfers: &[TransferRecord],
) -> Result<()> {
    let details: Vec<(String, String)> = transfers
        .iter()
        .enumerate()
        .map(|(index, t)| {
            let status = if t.ok { "✓" } else { "✗" };
            let skipped = if t.skipped > 0 {
                format!(", {} skipped", t.skipped)
            } else {
                String::new()
            };
            (
                format!("{} {}", index + 1, status),
                format!(
                    "{}  ·  {} files{}  ·  {} → {}:{}",
                    util::format_size(t.bytes),
                    t.files,
                    skipped,
                    t.source,
                    t.peer,
                    t.dest
                ),
            )
        })
        .collect();
    screen.render(
        "Session history",
        if transfers.is_empty() {
            "No transfers yet"
        } else {
            "Transfers this session"
        },
        crate::picker::Tone::Info,
        &details,
        "enter / esc  close",
    )?;
    screen.wait_for_close()
}

/// Returns Ok(None) when the user backs out with Esc.
async fn browse_remote_dir(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    dest_root: &str,
    auth_code: Option<&str>,
) -> Result<Option<String>> {
    // One connection for the whole browse session — no reconnect per step.
    let (mut stream, _) = client::connect_and_handshake(ip, port).await?;
    let mut current_relative = String::new();
    let mut last_idx = 0;

    loop {
        send_control(
            &mut stream,
            &ControlMessage::BrowseDirectory {
                destination_path: dest_root.to_string(),
                relative_path: current_relative.clone(),
                auth_code: auth_code.map(|s| s.to_string()),
            },
        )
        .await?;

        let entries = match read_control(&mut stream).await? {
            ControlMessage::DirectoryContents { entries, .. } => entries,
            ControlMessage::Error { message } => bail!("{message}"),
            _ => bail!("unexpected response"),
        };

        let display_path = join_remote_path(dest_root, &current_relative);

        let mut items = vec!["✓ Use this folder".to_string(), ".. go up".to_string()];
        for entry in &entries {
            if entry.is_dir {
                items.push(format!("{}/", entry.name));
            } else {
                items.push(format!(
                    "{:<32}  {}",
                    entry.name,
                    util::format_size(entry.size)
                ));
            }
        }

        let Some(selection) = select(
            screen,
            &format!("remote · {display_path}"),
            items.clone(),
            last_idx.min(items.len() - 1),
            NAV_HELP,
        )?
        else {
            // esc: step up a level, or leave the browser at the root
            if current_relative.is_empty() {
                return Ok(None);
            }
            go_up(&mut current_relative);
            last_idx = 0;
            continue;
        };
        last_idx = selection;

        match selection {
            0 => break,
            1 => {
                go_up(&mut current_relative);
                last_idx = 0;
            }
            idx => {
                let entry = &entries[idx - 2];
                if entry.is_dir {
                    if current_relative.is_empty() {
                        current_relative = entry.name.clone();
                    } else {
                        current_relative = format!("{}/{}", current_relative, entry.name);
                    }
                    last_idx = 0;
                }
                // selecting a file does nothing
            }
        }
    }

    Ok(Some(join_remote_path(dest_root, &current_relative)))
}

fn go_up(relative: &mut String) {
    if let Some(pos) = relative.rfind('/') {
        relative.truncate(pos);
    } else {
        relative.clear();
    }
}

/// Join a remote root and a '/'-separated relative path WITHOUT using the
/// local OS separator — the result must be valid on the *remote* machine.
/// Both Windows and Unix accept '/' as a separator.
fn join_remote_path(root: &str, relative: &str) -> String {
    let mut base = root.trim_end_matches(['/', '\\']).to_string();
    if base.is_empty() || base.ends_with(':') {
        // unix root "/" or bare windows drive "C:" (which alone would be
        // drive-relative on Windows) — keep an explicit separator
        base.push('/');
    }
    if relative.is_empty() {
        return base;
    }
    if !base.ends_with('/') {
        base.push('/');
    }
    base.push_str(relative);
    base
}

enum StartChoice {
    Path(PathBuf),
    Drives,
    Manual,
}

fn pick_start_dir(screen: &mut crate::picker::StatusScreen) -> Result<Option<PathBuf>> {
    let cwd = std::env::current_dir()?;
    let home = dirs::home_dir().unwrap_or_else(|| cwd.clone());

    let mut options: Vec<(String, StartChoice)> = vec![(
        format!("Current directory   {}", cwd.display()),
        StartChoice::Path(cwd),
    )];
    let shortcuts = [
        ("Home", Some(home)),
        ("Desktop", dirs::desktop_dir()),
        ("Downloads", dirs::download_dir()),
        ("Documents", dirs::document_dir()),
    ];
    for (label, dir) in shortcuts {
        if let Some(dir) = dir
            && dir.is_dir()
        {
            options.push((
                format!("{:<19} {}", label, dir.display()),
                StartChoice::Path(dir),
            ));
        }
    }
    if cfg!(windows) {
        options.push((
            "Pick a drive (C:\\, D:\\, …)".to_string(),
            StartChoice::Drives,
        ));
    } else {
        options.push((
            "Root (/)".to_string(),
            StartChoice::Path(PathBuf::from("/")),
        ));
    }
    options.push(("Enter path manually".to_string(), StartChoice::Manual));

    let labels: Vec<String> = options.iter().map(|(s, _)| s.clone()).collect();
    let Some(idx) = select(screen, "Browse from", labels, 0, NAV_HELP)? else {
        return Ok(None);
    };

    Ok(match &options[idx].1 {
        StartChoice::Path(p) => Some(p.clone()),
        StartChoice::Drives => pick_windows_drive(screen)?,
        StartChoice::Manual => text(screen, "Enter path", "esc to go back", false)?
            .map(|path| PathBuf::from(path.trim())),
    })
}

/// Navigate directories and toggle files/folders for transfer:
/// - selecting a directory enters it
/// - selecting a file toggles it
/// - "Add this whole folder" toggles the current directory
/// - "Done" confirms; Esc goes up / backs out
///
/// Returns Ok(None) when the user backs out.
fn pick_local_paths(screen: &mut crate::picker::StatusScreen) -> Result<Option<Vec<PathBuf>>> {
    let Some(mut current_dir) = pick_start_dir(screen)? else {
        return Ok(None);
    };
    let mut selected: Vec<PathBuf> = Vec::new();
    let mut last_idx = 0;

    loop {
        let mut entries: Vec<(String, PathBuf, bool, u64)> = Vec::new();
        let dir_iter = match std::fs::read_dir(&current_dir) {
            Ok(it) => it,
            Err(err) => {
                screen.render(
                    "Local files",
                    &format!("cannot read {}: {err}", current_dir.display()),
                    crate::picker::Tone::Error,
                    &[],
                    "Returning to the parent folder…",
                )?;
                match current_dir.parent() {
                    Some(parent) => {
                        current_dir = parent.to_path_buf();
                        continue;
                    }
                    None => return Ok(None),
                }
            }
        };
        for entry in dir_iter.flatten() {
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            let size = if meta.is_file() { meta.len() } else { 0 };
            entries.push((name, entry.path(), meta.is_dir(), size));
        }
        entries.sort_by(|a, b| {
            b.2.cmp(&a.2)
                .then_with(|| a.0.to_lowercase().cmp(&b.0.to_lowercase()))
        });

        let done_label = if selected.is_empty() {
            "✓ Done (nothing selected — cancels)".to_string()
        } else {
            format!("✓ Done — transfer {} item(s)", selected.len())
        };
        let folder_mark = if selected.contains(&current_dir) {
            "● "
        } else {
            "  "
        };
        let mut items = vec![
            done_label,
            ".. go up".to_string(),
            format!("{folder_mark}+ Add this whole folder"),
        ];
        for (name, path, is_dir, size) in &entries {
            let mark = if selected.contains(path) {
                "● "
            } else {
                "  "
            };
            if *is_dir {
                items.push(format!("{mark}{name}/"));
            } else {
                items.push(format!("{mark}{:<32}  {}", name, util::format_size(*size)));
            }
        }

        let prompt = format!(
            "local · {} · {} selected",
            current_dir.display(),
            selected.len()
        );
        let Some(idx) = select(
            screen,
            &prompt,
            items.clone(),
            last_idx.min(items.len() - 1),
            "enter opens dir / toggles file · type to filter · esc up",
        )?
        else {
            // esc: go up, or back out at a root
            match current_dir.parent() {
                Some(parent) => {
                    current_dir = parent.to_path_buf();
                    last_idx = 0;
                    continue;
                }
                None => return Ok(None),
            }
        };
        last_idx = idx;

        match idx {
            0 => {
                if selected.is_empty() {
                    if confirm(screen, "Nothing selected. Cancel?", true)? {
                        return Ok(None);
                    }
                } else {
                    return Ok(Some(selected));
                }
            }
            1 => {
                if let Some(parent) = current_dir.parent() {
                    current_dir = parent.to_path_buf();
                } else if cfg!(windows) {
                    match pick_windows_drive(screen)? {
                        Some(d) => current_dir = d,
                        None => return Ok(None),
                    }
                }
                last_idx = 0;
            }
            2 => toggle(&mut selected, &current_dir),
            i => {
                let (_, path, is_dir, _) = &entries[i - 3];
                if *is_dir {
                    current_dir = path.clone();
                    last_idx = 0;
                } else {
                    toggle(&mut selected, path);
                }
            }
        }
    }
}

fn toggle(selected: &mut Vec<PathBuf>, path: &Path) {
    if let Some(pos) = selected.iter().position(|p| p == path) {
        selected.remove(pos);
    } else {
        selected.push(path.to_path_buf());
    }
}

fn pick_windows_drive(screen: &mut crate::picker::StatusScreen) -> Result<Option<PathBuf>> {
    let mut drives: Vec<PathBuf> = Vec::new();
    for letter in b'A'..=b'Z' {
        let p = PathBuf::from(format!("{}:\\", letter as char));
        if p.exists() {
            drives.push(p);
        }
    }
    if drives.is_empty() {
        bail!("no drives detected");
    }
    let labels: Vec<String> = drives.iter().map(|d| d.display().to_string()).collect();
    Ok(select(screen, "Select drive", labels, 0, NAV_HELP)?.map(|idx| drives[idx].clone()))
}

#[cfg(test)]
mod tests {
    use super::join_remote_path;

    #[test]
    fn remote_paths_use_forward_slash_regardless_of_local_os() {
        assert_eq!(join_remote_path("/", ""), "/");
        assert_eq!(join_remote_path("/", "a/b"), "/a/b");
        assert_eq!(join_remote_path("/Users/x", "docs"), "/Users/x/docs");
        assert_eq!(join_remote_path("C:\\", ""), "C:/");
        assert_eq!(join_remote_path("C:\\", "foo/bar"), "C:/foo/bar");
        // interior separators stay as the remote sent them (mixed is valid on
        // Windows; rewriting could corrupt a unix name containing '\')
        assert_eq!(join_remote_path("D:\\data\\", "x"), "D:\\data/x");
    }
}
