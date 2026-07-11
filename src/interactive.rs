use anyhow::{Result, bail};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client;
use crate::discovery;
use crate::picker::filtered;
use crate::protocol::{
    ControlMessage, DestinationInfo, RemoteFileSpec, PROTOCOL_VERSION, read_control, send_control,
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

/// Events sent from the server task to the interactive UI.
#[allow(dead_code)]
pub enum ServerEvent {
    /// A new client connected — stream, client name, client port, client IP.
    PeerConnected(TcpStream, String, u16, String),
    /// Progress update: file (id, bytes received, total bytes).
    TransferProgress {
        file_id: u32,
        file_name: String,
        received: u64,
        total: u64,
    },
    /// A file finished receiving (success or error).
    FileDone {
        file_id: u32,
        file_name: String,
        ok: bool,
        error: Option<String>,
    },
    /// Client disconnected.
    Disconnected,
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

    let (server_tx, server_rx) = mpsc::unbounded_channel::<ServerEvent>();
    let bind = format!("0.0.0.0:{port}");
    let server_code = pairing_code.clone();
    tokio::spawn(async move {
        if let Err(err) =
            server::run_server(bind, discovery_port, server_code, true, !open, Some(server_tx))
                .await
        {
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
        Some(server_rx),
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
        None,
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
    mut server_rx: Option<mpsc::UnboundedReceiver<ServerEvent>>,
) -> Result<()> {
    let mut codes: HashMap<String, String> = HashMap::new();
    let mut transfers: Vec<TransferRecord> = Vec::new();
    let local_ips = local_ipv4s();
    let my_host = util::host_name();

    // Track connected peers for the "Connected peers" section in the picker.
    // Each entry: (ip, port, name, label, stream).
    let mut connected_peers: Vec<(String, u16, String, String, TcpStream)> = Vec::new();

    loop {
        // Initial scan.
        screen.render(
            picker_title,
            "Scanning for nearby peers…",
            crate::picker::Tone::Info,
            &[],
            "Automatic discovery is active",
        )?;
        let mut hosts = scan_hosts(discovery_port, timeout_ms, exclude_self, &local_ips, &my_host).await;
        let mut items = hosts_to_items(&hosts);
        // Add connected peers section (separator + entries).
        for (_, _, _, label, _) in &connected_peers {
            items.push(format!("\u{25cf} {label}"));
        }
        items.push("Enter IP manually".to_string());
        items.push("\u{21bb} Rescan".to_string());
        items.push("\u{2717} Quit".to_string());

        // Custom picker loop with background rescan.
        let mut query = String::new();
        let mut selected = 0usize;
        let mut scan_task: Option<tokio::task::JoinHandle<Vec<discovery::DiscoveredHost>>> = None;

        enum PickerResult {
            Selected(usize, usize),
        }

        let result: PickerResult = loop {
            let visible = filtered(&items, &query);
            selected = selected.min(visible.len().saturating_sub(1));
            screen.draw_list(picker_title, &items, &visible, selected, &query, NAV_HELP)?;

            // Check if a background scan finished — never blocks on it.
            if let Some(task) = scan_task.take() {
                if task.is_finished() {
                    if let Ok(new_hosts) = task.await {
                        hosts = new_hosts;
                        items = hosts_to_items(&hosts);
                        for (_, _, _, label, _) in &connected_peers {
                            items.push(format!("\u{25cf} {label}"));
                        }
                        items.push("Enter IP manually".to_string());
                        items.push("\u{21bb} Rescan".to_string());
                        items.push("\u{2717} Quit".to_string());
                        selected = selected.min(items.len().saturating_sub(1));
                    }
                } else {
                    scan_task = Some(task);
                }
            }

            // Start a new scan if none is running.
            if scan_task.is_none() {
                let ips = local_ips.clone();
                let host = my_host.clone();
                scan_task = Some(tokio::spawn(async move {
                    scan_hosts(discovery_port, timeout_ms, exclude_self, &ips, &host).await
                }));
            }

            // Check for incoming server events (peer connected).
            if let Some(rx) = &mut server_rx {
                while let Ok(evt) = rx.try_recv() {
                    if let ServerEvent::PeerConnected(stream, name, port, ip) = evt {
                        let label = format!("{name} ({ip})");
                        // Queue for the picker — user selects when ready.
                        connected_peers.push((ip, port, name, label, stream));
                        // Rebuild items to include the new connected peer.
                        items = hosts_to_items(&hosts);
                        for (_, _, _, lbl, _) in &connected_peers {
                            items.push(format!("\u{25cf} {lbl}"));
                        }
                        items.push("Enter IP manually".to_string());
                        items.push("\u{21bb} Rescan".to_string());
                        items.push("\u{2717} Quit".to_string());
                        selected = selected.min(items.len().saturating_sub(1));
                    }
                }
            }

            // Non-blocking poll: only blocks the terminal event queue,
            // never the async runtime.
            if !event::poll(Duration::from_millis(50))? {
                continue;
            }

            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Esc => return Ok(()),
                KeyCode::Enter if !visible.is_empty() => {
                    break PickerResult::Selected(visible[selected], items.len());
                }
                KeyCode::Up | KeyCode::Char('k') if query.is_empty() => {
                    selected = selected.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') if query.is_empty() => {
                    selected = (selected + 1).min(visible.len().saturating_sub(1));
                }
                KeyCode::Home => selected = 0,
                KeyCode::End => selected = visible.len().saturating_sub(1),
                KeyCode::Backspace => {
                    query.pop();
                    selected = 0;
                }
                KeyCode::Char(c)
                    if !key
                        .modifiers
                        .contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    query.push(c);
                    selected = 0;
                }
                _ => {}
            }
        };

        let PickerResult::Selected(sel, _items_len) = result;

        let host_count = hosts.len();
        let connected_count = connected_peers.len();
        let manual_idx = host_count + connected_count;
        let rescan_idx = manual_idx + 1;
        let quit_idx = rescan_idx + 1;

        let (ip, control_port, label) = if sel == quit_idx {
            return Ok(());
        } else if sel == rescan_idx {
            continue; // Rescan
        } else if sel == manual_idx {
            let Some(ip) = text(screen, "Receiver IP", "esc to go back", false)? else {
                continue;
            };
            let ip = ip.trim().to_string();
            (ip.clone(), default_port, ip)
        } else if sel < host_count {
            // Discovered host — re-scan to get fresh list, pick by index.
            let fresh_hosts = scan_hosts(discovery_port, timeout_ms, exclude_self, &local_ips, &my_host).await;
            if sel >= fresh_hosts.len() {
                continue;
            }
            let h = &fresh_hosts[sel];
            (h.ip.clone(), h.reply.control_port, h.reply.host.clone())
        } else {
            // Connected peer entry.
            let idx = sel - host_count;
            if idx >= connected_peers.len() {
                continue;
            }
            let (ip, port, name, label, stream) = connected_peers.remove(idx);
            // Dispatch to connected_peer_ui with the queued stream.
            let result = connected_peer_ui(
                screen,
                stream,
                &name,
                &ip,
                port,
                default_port,
                &mut codes,
                &mut transfers,
            )
            .await;
            match result {
                Ok(()) => {}
                Err(err) => {
                    let msg = format!("{err:#}");
                    let is_disconnect = msg.contains("connection closed")
                        || msg.contains("broken pipe")
                        || msg.contains("connection aborted")
                        || msg.contains("unexpected end of file")
                        || msg.contains("Connection closed");
                    if is_disconnect {
                        screen.render(
                            "Connection",
                            &format!("Peer {label} disconnected"),
                            crate::picker::Tone::Info,
                            &[],
                            "Returning to peer list…",
                        )?;
                        tokio::time::sleep(Duration::from_millis(600)).await;
                    } else {
                        screen.render(
                            "Connection",
                            &msg,
                            crate::picker::Tone::Error,
                            &[],
                            "Rescanning automatically…",
                        )?;
                        tokio::time::sleep(Duration::from_millis(900)).await;
                    }
                }
            }
            continue;
        };

        match peer_session(
            screen,
            &ip,
            control_port,
            default_port,
            &label,
            &mut codes,
            &mut transfers,
        )
        .await
        {
            Ok(true) => return Ok(()), // user chose quit
            Ok(false) => {}            // switch peer -> rescan
            Err(err) => {
                let msg = format!("{err:#}");
                let is_disconnect = msg.contains("connection reset")
                    || msg.contains("broken pipe")
                    || msg.contains("connection aborted")
                    || msg.contains("unexpected end of file")
                    || msg.contains("Connection closed");
                if is_disconnect {
                    screen.render(
                        "Connection",
                        &format!("Peer {label} disconnected"),
                        crate::picker::Tone::Info,
                        &[],
                        "Returning to peer list…",
                    )?;
                    tokio::time::sleep(Duration::from_millis(600)).await;
                } else {
                    screen.render(
                        "Connection",
                        &msg,
                        crate::picker::Tone::Error,
                        &[],
                        "Rescanning automatically…",
                    )?;
                    tokio::time::sleep(Duration::from_millis(900)).await;
                }
            }
        }
    }
}

async fn scan_hosts(
    discovery_port: u16,
    timeout_ms: u64,
    exclude_self: bool,
    local_ips: &[String],
    my_host: &str,
) -> Vec<discovery::DiscoveredHost> {
    let mut hosts = discovery::discover_hosts_with_fallback(discovery_port, timeout_ms)
        .await
        .unwrap_or_default();
    if exclude_self {
        hosts.retain(|h| !local_ips.contains(&h.ip) && h.reply.host != my_host);
    }
    hosts
}

fn hosts_to_items(hosts: &[discovery::DiscoveredHost]) -> Vec<String> {
    hosts
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
        .collect()
}

/// Symmetric connected-peer UI for incoming connections.
/// Uses the same non-blocking try_choose loop and shared send/receive flows
/// as `peer_session`. Handles incoming transfers (BeginSession) while showing
/// the menu. Runs until the client disconnects or the user presses Esc.
#[allow(clippy::too_many_arguments)]
pub async fn connected_peer_ui(
    screen: &mut crate::picker::StatusScreen,
    stream: TcpStream,
    client_name: &str,
    client_ip: &str,
    client_port: u16,
    local_port: u16,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
) -> Result<()> {
    use tokio::io::BufReader;

    // Split stream for concurrent read/write.
    let (read_half, write_half) = stream.into_split();
    let reader = BufReader::with_capacity(4 * 1024 * 1024, read_half);

    // Channel: reader task → UI.
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ControlMessage>();
    // Channel: UI → writer task.
    let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<ControlMessage>();

    // Reader task.
    tokio::spawn(async move {
        let mut reader = reader;
        while let Ok(msg) = read_control(&mut reader).await {
            let _ = msg_tx.send(msg);
        }
        let _ = msg_tx.send(ControlMessage::Error {
            message: "connection closed".to_string(),
        });
    });

    // Writer task.
    let mut write_half = write_half;
    tokio::spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if send_control(&mut write_half, &msg).await.is_err() {
                break;
            }
        }
    });

    let mut last_dest: Option<String> = None;
    let mut last_source: Option<String> = None;
    let label = format!("{client_name} ({client_ip})");
    let auth_required = false; // incoming connections don't need pairing code

    loop {
        // Build menu (same as peer_session).
        enum Action {
            Send,
            SendAgain,
            Receive,
            ReceiveAgain,
            Drives,
            History,
            Disconnect,
        }
        let mut menu: Vec<(String, Action)> = vec![("Send files".to_string(), Action::Send)];
        if let Some(dest) = &last_dest {
            menu.push((format!("Send more to {dest}"), Action::SendAgain));
        }
        menu.push(("Receive files".to_string(), Action::Receive));
        if let Some(source) = &last_source {
            menu.push((format!("Receive more from {source}"), Action::ReceiveAgain));
        }
        menu.push(("List remote drives".to_string(), Action::Drives));
        menu.push((
            format!("Transfers this session ({})", transfers.len()),
            Action::History,
        ));
        menu.push(("Disconnect".to_string(), Action::Disconnect));

        let labels: Vec<String> = menu.iter().map(|(l, _)| l.clone()).collect();
        let nav_help = "↑↓ move · type to filter · enter select · esc back";

        // Non-blocking event loop: processes keyboard input AND incoming messages.
        let mut query = String::new();
        let mut selected = 0usize;
        let sel: Option<usize> = loop {
            let visible = filtered(&labels, &query);
            selected = selected.min(visible.len().saturating_sub(1));

            match screen.try_choose(
                &format!("{label} · what next?"),
                &labels,
                &visible,
                &mut selected,
                &mut query,
                nav_help,
            )? {
                Some(Some(idx)) => break Some(idx),
                Some(None) => return Ok(()), // esc = disconnect
                None => {}                    // no input — check messages
            }

            // Check for incoming messages (incoming transfers from this peer).
            while let Ok(msg) = msg_rx.try_recv() {
                match msg {
                    ControlMessage::BeginSession {
                        destination_path,
                        auth_code: _,
                        overwrite: _,
                        dry_run: _,
                        files,
                        dirs: _,
                    } => {
                        // Wait for SessionPlan.
                        let mut session_id = String::new();
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while Instant::now() < deadline {
                            if let Ok(ControlMessage::SessionPlan {
                                session_id: sid,
                                actions: _,
                            }) = msg_rx.try_recv()
                            {
                                session_id = sid;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }

                        let screen_title = format!("Receiving from {label}");
                        screen.render(
                            &screen_title,
                            &format!("{} files incoming…", files.len()),
                            crate::picker::Tone::Info,
                            &[("destination".into(), destination_path)],
                            "accepting transfer…",
                        )?;

                        let server_ip = client_ip.to_string();
                        let data_port = local_port;
                        let files_clone = files.clone();
                        let sid = session_id.clone();
                        tokio::spawn(async move {
                            match client::receive_session(&sid, &server_ip, data_port, &files_clone)
                                .await
                            {
                                Ok(s) => {
                                    eprintln!(
                                        "received {} files ({} bytes)",
                                        s.files_received, s.bytes
                                    );
                                }
                                Err(e) => {
                                    eprintln!("receive failed: {e:#}");
                                }
                            }
                        });
                    }
                    ControlMessage::Error { message }
                        if message == "connection closed" => {
                        screen.render(
                            "Disconnected",
                            &format!("Peer {label} disconnected"),
                            crate::picker::Tone::Info,
                            &[],
                            "returning to peer list…",
                        )?;
                        tokio::time::sleep(Duration::from_millis(600)).await;
                        return Ok(());
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        let Some(sel) = sel else {
            unreachable!()
        };
        let result = match &menu[sel].1 {
            Action::Send => {
                send_flow(
                    screen,
                    client_ip,
                    client_port,
                    &label,
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
                    client_ip,
                    client_port,
                    &label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_dest,
                    dest,
                )
                .await
            }
            Action::Receive => {
                receive_flow(
                    screen,
                    client_ip,
                    client_port,
                    &label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_source,
                    None,
                )
                .await
            }
            Action::ReceiveAgain => {
                let source = last_source.clone();
                receive_flow(
                    screen,
                    client_ip,
                    client_port,
                    &label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_source,
                    source,
                )
                .await
            }
            Action::Drives => list_drives(screen, client_ip, client_port).await,
            Action::History => print_transfers(screen, transfers),
            Action::Disconnect => return Ok(()),
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
                codes.remove(client_ip);
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
    local_port: u16,
    label: &str,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
) -> Result<bool> {
    use tokio::io::BufReader;

    // Persistent connection to the peer's server.
    let addr = format!("{ip}:{port}");
    let mut stream = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => bail!("connection to {addr} timed out after 5s"),
    };
    stream.set_nodelay(true)?;

    // Handshake.
    send_control(
        &mut stream,
        &ControlMessage::Hello {
            version: PROTOCOL_VERSION,
            client_name: util::host_name(),
            client_port: local_port,
        },
    )
    .await?;
    let (device, auth_required) = match read_control(&mut stream).await? {
        ControlMessage::HelloAck {
            version,
            server,
            auth_required,
        } if version == PROTOCOL_VERSION => (server, auth_required),
        ControlMessage::HelloAck { version, .. } => bail!("version mismatch: {version}"),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected handshake: {other:?}"),
    };
    let _ = device;

    // Split for concurrent read/write.
    let (read_half, write_half) = stream.into_split();
    let reader = BufReader::with_capacity(4 * 1024 * 1024, read_half);

    // Channel: reader task → UI.
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<ControlMessage>();
    // Channel: UI → writer task.
    let (_cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<ControlMessage>();

    // Reader task.
    tokio::spawn(async move {
        let mut reader = reader;
        while let Ok(msg) = read_control(&mut reader).await {
            let _ = msg_tx.send(msg);
        }
        let _ = msg_tx.send(ControlMessage::Error {
            message: "connection closed".to_string(),
        });
    });

    // Writer task.
    let mut write_half = write_half;
    tokio::spawn(async move {
        while let Some(msg) = cmd_rx.recv().await {
            if send_control(&mut write_half, &msg).await.is_err() {
                break;
            }
        }
    });

    let mut last_dest: Option<String> = None;
    let mut last_source: Option<String> = None;

    loop {
        enum Action {
            Send,
            SendAgain,
            Receive,
            ReceiveAgain,
            Drives,
            History,
            SwitchPeer,
            Quit,
        }
        let mut menu: Vec<(String, Action)> = vec![("Send files".to_string(), Action::Send)];
        if let Some(dest) = &last_dest {
            menu.push((format!("Send more to {dest}"), Action::SendAgain));
        }
        menu.push(("Receive files".to_string(), Action::Receive));
        if let Some(source) = &last_source {
            menu.push((format!("Receive more from {source}"), Action::ReceiveAgain));
        }
        menu.push(("List remote drives".to_string(), Action::Drives));
        menu.push((
            format!("Transfers this session ({})", transfers.len()),
            Action::History,
        ));
        menu.push(("Switch peer".to_string(), Action::SwitchPeer));
        menu.push(("Quit".to_string(), Action::Quit));

        let labels: Vec<String> = menu.iter().map(|(l, _)| l.clone()).collect();
        let nav_help = "↑↓ move · type to filter · enter select · esc back";

        // Non-blocking event loop: processes keyboard input AND incoming messages.
        let mut query = String::new();
        let mut selected = 0usize;
        let sel: Option<usize> = loop {
            let visible = filtered(&labels, &query);
            selected = selected.min(visible.len().saturating_sub(1));

            match screen.try_choose(
                &format!("{label} · what next?"),
                &labels,
                &visible,
                &mut selected,
                &mut query,
                nav_help,
            )? {
                Some(Some(idx)) => break Some(idx),
                Some(None) => return Ok(false), // esc = back to peer list
                None => {}                       // no input — check messages
            }

            // Check for incoming messages (BeginSession from server, etc.).
            while let Ok(msg) = msg_rx.try_recv() {
                match msg {
                    ControlMessage::BeginSession {
                        destination_path,
                        auth_code: _,
                        overwrite: _,
                        dry_run: _,
                        files,
                        dirs: _,
                    } => {
                        // Server is sending files to us. Wait for SessionPlan
                        // (already sent by the server, should be in the channel).
                        let mut session_id = String::new();
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while Instant::now() < deadline {
                            if let Ok(ControlMessage::SessionPlan {
                                session_id: sid,
                                actions: _,
                            }) = msg_rx.try_recv()
                            {
                                session_id = sid;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }

                        let screen_title = format!("Receiving from {label}");
                        screen.render(
                            &screen_title,
                            &format!("{} files incoming…", files.len()),
                            crate::picker::Tone::Info,
                            &[("destination".into(), destination_path)],
                            "accepting transfer…",
                        )?;

                        let server_ip = ip.to_string();
                        let data_port = port;
                        let files_clone = files.clone();
                        let sid = session_id.clone();
                        tokio::spawn(async move {
                            match client::receive_session(&sid, &server_ip, data_port, &files_clone)
                                .await
                            {
                                Ok(s) => {
                                    eprintln!(
                                        "received {} files ({} bytes)",
                                        s.files_received, s.bytes
                                    );
                                }
                                Err(e) => {
                                    eprintln!("receive failed: {e:#}");
                                }
                            }
                        });
                    }
                    ControlMessage::Error { message }
                        if message == "connection closed" => {
                            screen.render(
                                "Disconnected",
                                &format!("Peer {label} disconnected"),
                                crate::picker::Tone::Info,
                                &[],
                                "returning to peer list…",
                            )?;
                            tokio::time::sleep(Duration::from_millis(600)).await;
                            return Ok(false);
                        }
                    _ => {}
                }
            }

            // Small sleep to avoid busy-waiting.
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        let Some(sel) = sel else {
            unreachable!()
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
            Action::Receive => {
                receive_flow(
                    screen,
                    ip,
                    port,
                    label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_source,
                    None,
                )
                .await
            }
            Action::ReceiveAgain => {
                let source = last_source.clone();
                receive_flow(
                    screen,
                    ip,
                    port,
                    label,
                    auth_required,
                    codes,
                    transfers,
                    &mut last_source,
                    source,
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
                codes.remove(ip);
            }
        }
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

/// Browse a remote filesystem and toggle-select files for transfer.
/// Returns Ok(None) when the user backs out with Esc.
async fn pick_remote_files(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    auth_code: Option<&str>,
) -> Result<Option<Vec<RemoteFileSpec>>> {
    // First: pick a starting drive.
    let destinations = fetch_destinations(ip, port).await?;
    let writable: Vec<&DestinationInfo> = destinations.iter().filter(|d| !d.read_only).collect();
    if writable.is_empty() {
        bail!("no drives found on remote");
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

    let Some(drive_idx) = select(screen, "Browse remote · select drive", drive_items, 0, NAV_HELP)?
    else {
        return Ok(None);
    };

    let dest_root = writable[drive_idx].path.clone();
    let mut stream = client::connect_and_handshake(ip, port).await?.0;
    let mut current_relative = String::new();
    let mut selected_files: Vec<(String, String, u64, i64)> = Vec::new(); // (abs_path, rel_path, size, mtime)
    let mut last_idx = 0;

    loop {
        send_control(
            &mut stream,
            &ControlMessage::BrowseDirectory {
                destination_path: dest_root.clone(),
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

        let display_path = join_remote_path(&dest_root, &current_relative);
        let done_label = if selected_files.is_empty() {
            "✓ Done (nothing selected — cancels)".to_string()
        } else {
            format!("✓ Done — receive {} file(s)", selected_files.len())
        };

        let mut items = vec![done_label, ".. go up".to_string()];
        for entry in &entries {
            if entry.is_dir {
                items.push(format!("{}/", entry.name));
            } else {
                let mark = if selected_files
                    .iter()
                    .any(|(_, rel, _, _)| *rel == join_remote_path(&current_relative, &entry.name))
                {
                    "● "
                } else {
                    "  "
                };
                items.push(format!(
                    "{mark}{:<32}  {}",
                    entry.name,
                    util::format_size(entry.size)
                ));
            }
        }

        let prompt = format!(
            "remote · {} · {} selected",
            display_path,
            selected_files.len()
        );
        let Some(selection) = select(
            screen,
            &prompt,
            items.clone(),
            last_idx.min(items.len().saturating_sub(1)),
            "enter opens dir / toggles file · type to filter · esc up",
        )?
        else {
            // esc: go up, or back out at root
            if current_relative.is_empty() {
                return Ok(None);
            }
            go_up(&mut current_relative);
            last_idx = 0;
            continue;
        };
        last_idx = selection;

        match selection {
            0 => {
                if selected_files.is_empty() {
                    if confirm(screen, "Nothing selected. Cancel?", true)? {
                        return Ok(None);
                    }
                } else {
                    let result: Vec<RemoteFileSpec> = selected_files
                        .into_iter()
                        .enumerate()
                        .map(|(i, (abs, rel, size, mtime))| RemoteFileSpec {
                            id: i as u32,
                            abs_path: abs,
                            rel_path: rel,
                            size,
                            mtime_secs: mtime,
                        })
                        .collect();
                    return Ok(Some(result));
                }
            }
            1 => {
                // Go up
                if current_relative.is_empty() {
                    // At root — return to drive selection
                    return Ok(None);
                }
                go_up(&mut current_relative);
                last_idx = 0;
            }
            idx => {
                let entry = &entries[idx - 2];
                if entry.is_dir {
                    // Enter directory
                    if current_relative.is_empty() {
                        current_relative = entry.name.clone();
                    } else {
                        current_relative =
                            format!("{}/{}", current_relative, entry.name);
                    }
                    last_idx = 0;
                } else {
                    // Toggle file selection
                    let abs = join_remote_path(
                        &dest_root,
                        &join_remote_path(&current_relative, &entry.name),
                    );
                    let rel =
                        join_remote_path(&current_relative, &entry.name);
                    if let Some(pos) = selected_files
                        .iter()
                        .position(|(_, r, _, _)| *r == rel)
                    {
                        selected_files.remove(pos);
                    } else {
                        selected_files.push((
                            abs,
                            rel,
                            entry.size,
                            entry.mtime_secs,
                        ));
                    }
                }
            }
        }
    }
}

/// Full receive flow: browse remote, pick files, pick local dest, transfer.
#[allow(clippy::too_many_arguments)]
async fn receive_flow(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    peer_label: &str,
    auth_required: bool,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
    last_source: &mut Option<String>,
    reuse_source: Option<String>,
) -> Result<()> {
    let Some(auth_code) = ensure_code(screen, ip, auth_required, codes)? else {
        return Ok(());
    };
    let auth = auth_code.as_deref();

    // Pick remote files to receive.
    let remote_files = match pick_remote_files(screen, ip, port, auth).await? {
        Some(f) if !f.is_empty() => f,
        _ => return Ok(()),
    };

    // Pick local save destination.
    let local_dest = match reuse_source {
        Some(path) => path,
        None => {
            let Some(path) = pick_local_save_dir(screen)? else {
                return Ok(());
            };
            path
        }
    };

    if !confirm(
        screen,
        &format!(
            "Receive {} file(s) to {}?",
            remote_files.len(),
            local_dest
        ),
        true,
    )? {
        return Ok(());
    }

    let source_label = format!("{}:{}", peer_label, remote_files[0].rel_path);
    let total_size: u64 = remote_files.iter().map(|f| f.size).sum();
    screen.render(
        "Transfer",
        "Receiving files…",
        crate::picker::Tone::Info,
        &[
            ("source".into(), source_label.clone()),
            ("destination".into(), local_dest.clone()),
            ("size".into(), util::format_size(total_size)),
        ],
        "Transfer is active · resumable if interrupted",
    )?;

    let result = client::push_session(
        ip,
        port,
        &remote_files,
        &local_dest,
        port, // requester_port = our server port (same as the port we're connected to)
        auth,
        false, // overwrite
    )
    .await;

    let record = match &result {
        Ok(s) => TransferRecord {
            peer: peer_label.to_string(),
            source: source_label,
            dest: local_dest.clone(),
            files: s.transferred,
            skipped: 0,
            bytes: s.bytes,
            ok: s.errors.is_empty(),
        },
        Err(_) => TransferRecord {
            peer: peer_label.to_string(),
            source: source_label,
            dest: local_dest.clone(),
            files: 0,
            skipped: 0,
            bytes: 0,
            ok: false,
        },
    };
    transfers.push(record);
    *last_source = Some(local_dest);

    let summary = result?;
    let mut details = vec![("received".into(), summary.transferred.to_string())];
    if !summary.errors.is_empty() {
        for err in &summary.errors {
            details.push(("error".into(), err.clone()));
        }
        bail!("some transfers failed — try again (transfers resume)");
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

/// Pick a local directory to save received files into.
fn pick_local_save_dir(screen: &mut crate::picker::StatusScreen) -> Result<Option<String>> {
    let home = dirs::home_dir().map(|p| p.display().to_string()).unwrap_or_default();
    let desktop = dirs::desktop_dir().map(|p| p.display().to_string()).unwrap_or_default();
    let downloads = dirs::download_dir().map(|p| p.display().to_string()).unwrap_or_default();
    let documents = dirs::document_dir().map(|p| p.display().to_string()).unwrap_or_default();

    let mut options: Vec<(String, String)> = Vec::new();
    if !home.is_empty() {
        options.push((format!("Home      {home}"), home.clone()));
    }
    if !desktop.is_empty() {
        options.push((format!("Desktop   {desktop}"), desktop));
    }
    if !downloads.is_empty() {
        options.push((format!("Downloads {downloads}"), downloads));
    }
    if !documents.is_empty() {
        options.push((format!("Documents {documents}"), documents));
    }
    options.push(("Enter path manually".to_string(), String::new()));

    let labels: Vec<String> = options.iter().map(|(l, _)| l.clone()).collect();
    let Some(idx) = select(screen, "Save to (local)", labels, 0, NAV_HELP)? else {
        return Ok(None);
    };

    if options[idx].1.is_empty() {
        // Manual entry
        match text(screen, "Local path", "esc to go back", false)? {
            Some(path) => Ok(Some(path.trim().to_string())),
            None => Ok(None),
        }
    } else {
        Ok(Some(options[idx].1.clone()))
    }
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
