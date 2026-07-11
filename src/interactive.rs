use anyhow::{Result, anyhow, bail};
use crossterm::event::{self, Event};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;

use crate::client;
use crate::discovery;
use crate::picker::{KeyOutcome, MultiChoice, filtered, handle_list_key};
use crate::progress::{Progress, SpeedGauge};
use crate::protocol::{
    ControlMessage, DestinationInfo, PROTOCOL_VERSION, RemoteFileSpec, read_control,
    read_control_timeout, send_control,
};

use crate::server;
use crate::util;

/// Deadline for interactive request/response round-trips (list, browse).
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Handles shared between the peer UI and the local server.
struct PeerCtx {
    /// Our own control-server port — remotes connect back here for pulls.
    local_port: u16,
    /// One-time tokens authorizing pull write-backs.
    tokens: server::PullTokens,
    /// Live counters for everything our server is currently receiving.
    recv_progress: Arc<Progress>,
}

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
pub enum ServerEvent {
    /// An interactive peer attached — stream, client name, client port, client IP.
    PeerConnected(TcpStream, String, u16, String),
}

/// A live presence connection to a peer. Dropping it closes the link (the
/// other side sees the disconnect); keeping it in the list keeps the pairing
/// alive across menu exits.
struct PeerConn {
    ip: String,
    /// The peer's control-server port — every request (browse, list,
    /// transfer) runs over its own short-lived connection to this port.
    port: u16,
    label: String,
    auth_required: bool,
    last_dest: Option<String>,
    last_source: Option<String>,
    closed: Arc<AtomicBool>,
    _write: OwnedWriteHalf,
}

impl PeerConn {
    fn new(stream: TcpStream, ip: String, port: u16, label: String, auth_required: bool) -> Self {
        let (read_half, write_half) = stream.into_split();
        let closed = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&closed);
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(read_half);
            while read_control(&mut reader).await.is_ok() {}
            flag.store(true, Ordering::Relaxed);
        });
        Self {
            ip,
            port,
            label,
            auth_required,
            last_dest: None,
            last_source: None,
            closed,
            _write: write_half,
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

/// Connect, handshake, and attach as an interactive peer.
async fn open_peer(ip: &str, port: u16, local_port: u16, label: String) -> Result<PeerConn> {
    let addr = format!("{ip}:{port}");
    let mut stream =
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => bail!("connection to {addr} timed out after 5s"),
        };
    stream.set_nodelay(true)?;
    send_control(
        &mut stream,
        &ControlMessage::Hello {
            version: PROTOCOL_VERSION,
            client_name: util::host_name(),
            client_port: local_port,
        },
    )
    .await?;
    let auth_required = match read_control_timeout(&mut stream, Duration::from_secs(5)).await? {
        ControlMessage::HelloAck {
            version,
            auth_required,
            ..
        } if version == PROTOCOL_VERSION => auth_required,
        ControlMessage::HelloAck { version, .. } => bail!("version mismatch: {version}"),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected handshake: {other:?}"),
    };
    send_control(&mut stream, &ControlMessage::Attach).await?;
    Ok(PeerConn::new(
        stream,
        ip.to_string(),
        port,
        label,
        auth_required,
    ))
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

fn input_text(
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
    // Bind before starting the UI — a busy port must fail loudly, not leave
    // a silently unreachable peer.
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| anyhow::anyhow!("cannot listen on {bind}: {e} (another lanxfer running?)"))?;
    let server_code = pairing_code.clone();
    let ctx = PeerCtx {
        local_port: port,
        tokens: server::PullTokens::default(),
        recv_progress: Arc::new(Progress::default()),
    };
    let tokens = Arc::clone(&ctx.tokens);
    let recv_progress = Arc::clone(&ctx.recv_progress);
    tokio::spawn(async move {
        let _ = server::run_server(
            listener,
            discovery_port,
            server_code,
            true,
            !open,
            Some(server_tx),
            tokens,
            recv_progress,
        )
        .await;
    });

    peer_loop(
        &mut screen,
        discovery_port,
        timeout_ms,
        port,
        true,
        &picker_title,
        Some(server_rx),
        ctx,
    )
    .await
}

pub async fn run_interactive(discovery_port: u16, timeout_ms: u64, port: u16) -> Result<()> {
    let mut screen = crate::picker::StatusScreen::new()?;
    let ctx = PeerCtx {
        local_port: port,
        tokens: server::PullTokens::default(),
        recv_progress: Arc::new(Progress::default()),
    };
    peer_loop(
        &mut screen,
        discovery_port,
        timeout_ms,
        port,
        false,
        "Pick a peer",
        None,
        ctx,
    )
    .await
}

/// What the user picked in the peer list. Items are laid out as
/// hosts ++ connected peers ++ [manual, rescan, quit].
enum Pick {
    Host(usize),
    Peer(usize),
    Manual,
    Rescan,
    Quit,
}

fn classify_pick(sel: usize, host_count: usize, peer_count: usize) -> Pick {
    if sel < host_count {
        Pick::Host(sel)
    } else if sel < host_count + peer_count {
        Pick::Peer(sel - host_count)
    } else if sel == host_count + peer_count {
        Pick::Manual
    } else if sel == host_count + peer_count + 1 {
        Pick::Rescan
    } else {
        Pick::Quit
    }
}

fn build_items(hosts: &[discovery::DiscoveredHost], peers: &[PeerConn]) -> Vec<String> {
    let mut items = hosts_to_items(hosts);
    for peer in peers {
        items.push(format!("\u{25cf} {}", peer.label));
    }
    items.push("Enter IP manually".to_string());
    items.push("\u{21bb} Rescan".to_string());
    items.push("\u{2717} Quit".to_string());
    items
}

/// Pause between background discovery scans — keeps broadcast chatter down.
const SCAN_GAP: Duration = Duration::from_millis(1000);

/// Discover peers, pick one, run a session. Repeats until the user quits.
/// Pairing codes, connections, and transfer history persist across sessions.
#[allow(clippy::too_many_arguments)]
async fn peer_loop(
    screen: &mut crate::picker::StatusScreen,
    discovery_port: u16,
    timeout_ms: u64,
    default_port: u16,
    exclude_self: bool,
    picker_title: &str,
    mut server_rx: Option<mpsc::UnboundedReceiver<ServerEvent>>,
    ctx: PeerCtx,
) -> Result<()> {
    let mut codes: HashMap<String, String> = HashMap::new();
    let mut transfers: Vec<TransferRecord> = Vec::new();
    let local_ips = local_ipv4s();
    let my_host = util::host_name();

    let mut hosts: Vec<discovery::DiscoveredHost> = Vec::new();
    let mut peers: Vec<PeerConn> = Vec::new();

    'picker: loop {
        let mut query = String::new();
        let mut selected = 0usize;
        let mut items = build_items(&hosts, &peers);
        let mut dirty = true;
        let mut scan_task: Option<tokio::task::JoinHandle<Vec<discovery::DiscoveredHost>>> = None;
        let mut last_scan_end: Option<Instant> = None;
        let mut recv_gauge = SpeedGauge::default();
        let mut recv_line: Option<String> = None;

        let pick: Pick = loop {
            // Reap a finished background scan — never blocks on it.
            if scan_task.as_ref().is_some_and(|t| t.is_finished()) {
                if let Ok(mut new_hosts) = scan_task.take().expect("checked above").await {
                    // Already-connected peers own their row — hide the
                    // duplicate discovery entry for the same address.
                    new_hosts.retain(|h| !peers.iter().any(|p| p.ip == h.ip));
                    hosts = new_hosts;
                    let new_items = build_items(&hosts, &peers);
                    if new_items != items {
                        items = new_items;
                        dirty = true;
                    }
                }
                last_scan_end = Some(Instant::now());
            }
            if scan_task.is_none() && last_scan_end.is_none_or(|t| t.elapsed() >= SCAN_GAP) {
                let ips = local_ips.clone();
                let host = my_host.clone();
                scan_task = Some(tokio::spawn(async move {
                    scan_hosts(discovery_port, timeout_ms, exclude_self, &ips, &host).await
                }));
            }

            // Peers that attached to our server while we were here.
            if let Some(rx) = &mut server_rx {
                while let Ok(ServerEvent::PeerConnected(stream, name, port, ip)) = rx.try_recv() {
                    let label = format!("{name} ({ip})");
                    hosts.retain(|h| h.ip != ip);
                    peers.push(PeerConn::new(stream, ip, port, label, false));
                    items = build_items(&hosts, &peers);
                    dirty = true;
                }
            }

            // Drop peers whose connection died while we were in the list.
            if peers.iter().any(PeerConn::is_closed) {
                peers.retain(|p| !p.is_closed());
                items = build_items(&hosts, &peers);
                dirty = true;
            }

            // Live footer while our server is receiving a transfer.
            let status = recv_status_line(&ctx.recv_progress, &mut recv_gauge);
            if status != recv_line {
                recv_line = status;
                dirty = true;
            }

            if dirty {
                let visible = filtered(&items, &query);
                selected = selected.min(visible.len().saturating_sub(1));
                let help = recv_line.as_deref().unwrap_or(NAV_HELP);
                screen.draw_list(picker_title, &items, &visible, selected, &query, help)?;
                dirty = false;
            }

            if !event::poll(Duration::from_millis(50))? {
                continue;
            }
            // Drain every queued event before redrawing — one draw per batch,
            // so held keys and fast typing never lag behind the redraw rate.
            let mut chosen: Option<usize> = None;
            while chosen.is_none() && event::poll(Duration::ZERO)? {
                let Event::Key(key) = event::read()? else {
                    // resize etc. — repaint on the next tick
                    dirty = true;
                    continue;
                };
                match handle_list_key(&key, &items, &mut selected, &mut query) {
                    KeyOutcome::Pick(index) => chosen = Some(index),
                    KeyOutcome::Cancel => return Ok(()),
                    KeyOutcome::Handled => dirty = true,
                    KeyOutcome::Ignored => {}
                }
            }
            if let Some(sel) = chosen {
                break classify_pick(sel, hosts.len(), peers.len());
            }
        };

        let peer_idx: Option<usize> = match pick {
            Pick::Quit => return Ok(()),
            Pick::Rescan => continue 'picker,
            Pick::Peer(idx) => Some(idx),
            Pick::Host(idx) => {
                let host = &hosts[idx];
                let label = format!("{} ({})", host.reply.host, host.ip);
                connect_or_reuse(
                    screen,
                    &mut peers,
                    &host.ip.clone(),
                    host.reply.control_port,
                    default_port,
                    label,
                )
                .await?
            }
            Pick::Manual => {
                let Some(ip) = input_text(screen, "Receiver IP", "esc to go back", false)? else {
                    continue 'picker;
                };
                let ip = ip.trim().to_string();
                connect_or_reuse(
                    screen,
                    &mut peers,
                    &ip.clone(),
                    default_port,
                    default_port,
                    ip,
                )
                .await?
            }
        };
        let Some(idx) = peer_idx else {
            continue 'picker;
        };
        // The connection owns this address's row now.
        let peer_ip = peers[idx].ip.clone();
        hosts.retain(|h| h.ip != peer_ip);

        match peer_ui(screen, &mut peers[idx], &ctx, &mut codes, &mut transfers).await? {
            PeerExit::Back => {}
            PeerExit::Closed => {
                peers.remove(idx);
            }
        }
    }
}

/// Footer line while our own server is receiving files, else None.
fn recv_status_line(progress: &Progress, gauge: &mut SpeedGauge) -> Option<String> {
    if !progress.is_active() {
        return None;
    }
    let s = progress.snapshot();
    let speed = gauge.update(s.done_bytes);
    Some(format!(
        "⇣ receiving {}/{} files · {} of {} · {}/s · ETA {}",
        s.done_files,
        s.total_files,
        util::format_size(s.done_bytes),
        util::format_size(s.total_bytes),
        util::format_size(speed as u64),
        crate::progress::eta(s.total_bytes.saturating_sub(s.done_bytes), speed),
    ))
}

/// Full-card live progress for an in-flight transfer, one line per
/// connection/unit.
fn render_transfer_card(
    screen: &mut crate::picker::StatusScreen,
    title: &str,
    source: &str,
    dest: &str,
    progress: &Progress,
    gauge: &mut SpeedGauge,
) -> Result<()> {
    let s = progress.snapshot();
    let speed = gauge.update(s.done_bytes);
    let pct = (s.done_bytes * 100).checked_div(s.total_bytes).unwrap_or(0);
    let mut details = vec![
        ("from".to_string(), source.to_string()),
        ("to".to_string(), dest.to_string()),
        (
            "progress".to_string(),
            format!(
                "{}/{} files · {} of {} ({pct}%)",
                s.done_files,
                s.total_files,
                util::format_size(s.done_bytes),
                util::format_size(s.total_bytes),
            ),
        ),
        (
            "speed".to_string(),
            format!(
                "{}/s · ETA {}",
                util::format_size(speed as u64),
                crate::progress::eta(s.total_bytes.saturating_sub(s.done_bytes), speed),
            ),
        ),
    ];
    for (label, done, total) in s.units.iter().take(8) {
        let unit_pct = if *total > 0 { done * 100 / total } else { 0 };
        details.push(("⇅".to_string(), format!("{label} · {unit_pct}%")));
    }
    screen.render(
        title,
        "Transferring…",
        crate::picker::Tone::Info,
        &details,
        "ctrl+c quits · transfers resume if interrupted",
    )
}

/// Reuse a live connection to `ip` if one exists, else open a new one.
/// Returns the peer's index, or None if connecting failed (error shown).
async fn connect_or_reuse(
    screen: &mut crate::picker::StatusScreen,
    peers: &mut Vec<PeerConn>,
    ip: &str,
    port: u16,
    local_port: u16,
    label: String,
) -> Result<Option<usize>> {
    if let Some(idx) = peers.iter().position(|p| p.ip == ip && !p.is_closed()) {
        return Ok(Some(idx));
    }
    screen.render(
        "Connection",
        &format!("Connecting to {label}…"),
        crate::picker::Tone::Info,
        &[],
        "",
    )?;
    match open_peer(ip, port, local_port, label).await {
        Ok(conn) => {
            peers.push(conn);
            Ok(Some(peers.len() - 1))
        }
        Err(err) => {
            screen.render(
                "Connection",
                &format!("{err:#}"),
                crate::picker::Tone::Error,
                &[],
                "Returning to peer list…",
            )?;
            tokio::time::sleep(Duration::from_millis(900)).await;
            Ok(None)
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
                h.reply.host, h.ip, h.reply.control_port, h.reply.device.os, h.reply.device.arch
            )
        })
        .collect()
}

enum PeerExit {
    /// Keep the connection alive; return to the peer list.
    Back,
    /// Connection is gone or the user chose Disconnect — drop it.
    Closed,
}

/// Menu for one connected peer. Esc keeps the connection alive and returns
/// to the peer list; only Disconnect (or the peer vanishing) closes it.
async fn peer_ui(
    screen: &mut crate::picker::StatusScreen,
    conn: &mut PeerConn,
    ctx: &PeerCtx,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
) -> Result<PeerExit> {
    loop {
        enum Action {
            Send,
            SendAgain,
            Receive,
            ReceiveAgain,
            Drives,
            History,
            Back,
            Disconnect,
        }
        let mut menu: Vec<(String, Action)> = vec![("Send files".to_string(), Action::Send)];
        if let Some(dest) = &conn.last_dest {
            menu.push((format!("Send more to {dest}"), Action::SendAgain));
        }
        menu.push(("Receive files".to_string(), Action::Receive));
        if let Some(source) = &conn.last_source {
            menu.push((format!("Receive more from {source}"), Action::ReceiveAgain));
        }
        menu.push(("List remote drives".to_string(), Action::Drives));
        menu.push((
            format!("Transfers this session ({})", transfers.len()),
            Action::History,
        ));
        menu.push(("Back to peer list".to_string(), Action::Back));
        menu.push(("Disconnect".to_string(), Action::Disconnect));

        let labels: Vec<String> = menu.iter().map(|(l, _)| l.clone()).collect();
        let title = format!("{} · what next?", conn.label);

        let mut query = String::new();
        let mut selected = 0usize;
        let mut dirty = true;
        let mut recv_gauge = SpeedGauge::default();
        let mut recv_line: Option<String> = None;
        let sel: usize = loop {
            if conn.is_closed() {
                screen.render(
                    "Disconnected",
                    &format!("Peer {} disconnected", conn.label),
                    crate::picker::Tone::Info,
                    &[],
                    "Returning to peer list…",
                )?;
                tokio::time::sleep(Duration::from_millis(600)).await;
                return Ok(PeerExit::Closed);
            }
            // Live footer while our server is receiving a transfer.
            let status = recv_status_line(&ctx.recv_progress, &mut recv_gauge);
            if status != recv_line {
                recv_line = status;
                dirty = true;
            }
            if dirty {
                let visible = filtered(&labels, &query);
                selected = selected.min(visible.len().saturating_sub(1));
                let help = recv_line.as_deref().unwrap_or(NAV_HELP);
                screen.draw_list(&title, &labels, &visible, selected, &query, help)?;
                dirty = false;
            }
            if !event::poll(Duration::from_millis(50))? {
                continue;
            }
            let mut chosen: Option<usize> = None;
            while chosen.is_none() && event::poll(Duration::ZERO)? {
                let Event::Key(key) = event::read()? else {
                    // resize etc. — repaint on the next tick
                    dirty = true;
                    continue;
                };
                match handle_list_key(&key, &labels, &mut selected, &mut query) {
                    KeyOutcome::Pick(index) => chosen = Some(index),
                    KeyOutcome::Cancel => return Ok(PeerExit::Back),
                    KeyOutcome::Handled => dirty = true,
                    KeyOutcome::Ignored => {}
                }
            }
            if let Some(sel) = chosen {
                break sel;
            }
        };

        let result = match &menu[sel].1 {
            Action::Send => {
                send_flow(
                    screen,
                    &conn.ip,
                    conn.port,
                    &conn.label,
                    &mut conn.auth_required,
                    codes,
                    transfers,
                    &mut conn.last_dest,
                    None,
                )
                .await
            }
            Action::SendAgain => {
                let dest = conn.last_dest.clone();
                send_flow(
                    screen,
                    &conn.ip,
                    conn.port,
                    &conn.label,
                    &mut conn.auth_required,
                    codes,
                    transfers,
                    &mut conn.last_dest,
                    dest,
                )
                .await
            }
            Action::Receive => {
                receive_flow(
                    screen,
                    &conn.ip,
                    conn.port,
                    ctx,
                    &conn.label,
                    &mut conn.auth_required,
                    codes,
                    transfers,
                    &mut conn.last_source,
                    None,
                )
                .await
            }
            Action::ReceiveAgain => {
                let source = conn.last_source.clone();
                receive_flow(
                    screen,
                    &conn.ip,
                    conn.port,
                    ctx,
                    &conn.label,
                    &mut conn.auth_required,
                    codes,
                    transfers,
                    &mut conn.last_source,
                    source,
                )
                .await
            }
            Action::Drives => list_drives(screen, &conn.ip, conn.port).await,
            Action::History => print_transfers(screen, transfers),
            Action::Back => return Ok(PeerExit::Back),
            Action::Disconnect => return Ok(PeerExit::Closed),
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
                codes.remove(&conn.ip);
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
    let Some(code) = input_text(
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
    let (items, _) = fetch_destinations(ip, port).await?;
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

/// Also reports whether the peer requires a pairing code — every flow
/// learns this from its own handshake instead of guessing.
async fn fetch_destinations(ip: &str, port: u16) -> Result<(Vec<DestinationInfo>, bool)> {
    let (mut stream, _, auth_required) = client::connect_and_handshake(ip, port).await?;
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;

    match read_control_timeout(&mut stream, REPLY_TIMEOUT).await? {
        ControlMessage::Destinations { items } => Ok((items, auth_required)),
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
    auth_required: &mut bool,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
    last_dest: &mut Option<String>,
    reuse_dest: Option<String>,
) -> Result<()> {
    let (remote_path, auth_code) = match reuse_dest {
        Some(dest) => {
            let Some(code) = ensure_code(screen, ip, *auth_required, codes)? else {
                return Ok(());
            };
            (dest, code)
        }
        None => {
            // ListDestinations needs no code; its handshake tells us whether
            // browsing/writing will.
            let (destinations, needs_auth) = fetch_destinations(ip, port).await?;
            *auth_required = needs_auth;
            let Some(code) = ensure_code(screen, ip, needs_auth, codes)? else {
                return Ok(());
            };
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

            match browse_remote_dir(screen, ip, port, &writable[drive_idx].path, code.as_deref())
                .await?
            {
                Some(path) => (path, code),
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
    // Run the transfer in the background and repaint a live card
    // (files, per-connection progress, speed, ETA) until it finishes.
    let progress = Arc::new(Progress::default());
    let task = {
        let ip = ip.to_string();
        let paths = local_paths.clone();
        let dest = remote_path.clone();
        let auth = auth_code.clone();
        let progress = Arc::clone(&progress);
        tokio::spawn(async move {
            client::send_session(
                &ip,
                port,
                &paths,
                &dest,
                auth.as_deref(),
                client::SendOptions {
                    overwrite: false,
                    dry_run: false,
                    jobs: None,
                    show_progress: false,
                    progress: Some(progress),
                },
            )
            .await
        })
    };
    let title = format!("Sending to {peer_label}");
    let mut gauge = SpeedGauge::default();
    let result = loop {
        if task.is_finished() {
            break match task.await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("send task failed: {err}")),
            };
        }
        crate::picker::pump_quit_only()?;
        render_transfer_card(
            screen,
            &title,
            &source_label,
            &remote_path,
            &progress,
            &mut gauge,
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

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

/// Browse a remote filesystem and multi-select files for transfer:
/// Space toggles, Enter opens directories / confirms, Esc goes up.
/// Resolves the pairing code itself (the fetch handshake says if one is
/// needed). Returns Ok(None) when the user backs out.
async fn pick_remote_files(
    screen: &mut crate::picker::StatusScreen,
    ip: &str,
    port: u16,
    auth_required: &mut bool,
    codes: &mut HashMap<String, String>,
) -> Result<Option<(Vec<RemoteFileSpec>, Option<String>)>> {
    // First: pick a starting drive.
    let (destinations, needs_auth) = fetch_destinations(ip, port).await?;
    *auth_required = needs_auth;
    let Some(auth_code) = ensure_code(screen, ip, needs_auth, codes)? else {
        return Ok(None);
    };
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

    let Some(drive_idx) = select(
        screen,
        "Browse remote · select drive",
        drive_items,
        0,
        NAV_HELP,
    )?
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
                auth_code: auth_code.clone(),
            },
        )
        .await?;

        let entries = match read_control_timeout(&mut stream, REPLY_TIMEOUT).await? {
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
        let choice = screen.choose_multi(
            &prompt,
            items.clone(),
            last_idx.min(items.len().saturating_sub(1)),
            "space select · enter open dir / confirm · esc up",
        )?;

        let to_specs = |files: Vec<(String, String, u64, i64)>| -> Vec<RemoteFileSpec> {
            files
                .into_iter()
                .enumerate()
                .map(|(i, (abs, rel, size, mtime))| RemoteFileSpec {
                    id: i as u32,
                    abs_path: abs,
                    rel_path: rel,
                    size,
                    mtime_secs: mtime,
                })
                .collect()
        };

        match choice {
            MultiChoice::Cancel => {
                // esc: go up, or back out at root
                if current_relative.is_empty() {
                    return Ok(None);
                }
                go_up(&mut current_relative);
                last_idx = 0;
            }
            MultiChoice::Toggle(idx) => {
                last_idx = idx;
                if idx >= 2 {
                    let entry = &entries[idx - 2];
                    if !entry.is_dir {
                        let rel = join_remote_path(&current_relative, &entry.name);
                        if let Some(pos) = selected_files.iter().position(|(_, r, _, _)| *r == rel)
                        {
                            selected_files.remove(pos);
                        } else {
                            let abs = join_remote_path(&dest_root, &rel);
                            selected_files.push((abs, rel, entry.size, entry.mtime_secs));
                        }
                    }
                }
            }
            MultiChoice::Pick(0) => {
                if selected_files.is_empty() {
                    if confirm(screen, "Nothing selected. Cancel?", true)? {
                        return Ok(None);
                    }
                    last_idx = 0;
                } else {
                    return Ok(Some((to_specs(selected_files), auth_code)));
                }
            }
            MultiChoice::Pick(1) => {
                // Go up; at root return to drive selection
                if current_relative.is_empty() {
                    return Ok(None);
                }
                go_up(&mut current_relative);
                last_idx = 0;
            }
            MultiChoice::Pick(idx) => {
                let entry = &entries[idx - 2];
                if entry.is_dir {
                    if current_relative.is_empty() {
                        current_relative = entry.name.clone();
                    } else {
                        current_relative = format!("{}/{}", current_relative, entry.name);
                    }
                    last_idx = 0;
                } else {
                    // Enter on a file confirms — include it if not selected.
                    let rel = join_remote_path(&current_relative, &entry.name);
                    if !selected_files.iter().any(|(_, r, _, _)| *r == rel) {
                        let abs = join_remote_path(&dest_root, &rel);
                        selected_files.push((abs, rel, entry.size, entry.mtime_secs));
                    }
                    return Ok(Some((to_specs(selected_files), auth_code)));
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
    ctx: &PeerCtx,
    peer_label: &str,
    auth_required: &mut bool,
    codes: &mut HashMap<String, String>,
    transfers: &mut Vec<TransferRecord>,
    last_source: &mut Option<String>,
    reuse_source: Option<String>,
) -> Result<()> {
    // Pick remote files to receive (resolves the pairing code itself).
    let (remote_files, auth_code) =
        match pick_remote_files(screen, ip, port, auth_required, codes).await? {
            Some((f, auth)) if !f.is_empty() => (f, auth),
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
        &format!("Receive {} file(s) to {}?", remote_files.len(), local_dest),
        true,
    )? {
        return Ok(());
    }

    let source_label = format!("{}:{}", peer_label, remote_files[0].rel_path);

    // One-time token: our own server accepts the remote's write-back with
    // this instead of a pairing code the remote could never know.
    let token = uuid::Uuid::new_v4().simple().to_string();
    ctx.tokens.lock().unwrap().insert(token.clone());

    // The pulled bytes land on our own server — its live counters drive
    // the progress card (files, per-connection progress, speed, ETA).
    let task = {
        let ip = ip.to_string();
        let files = remote_files.clone();
        let dest = local_dest.clone();
        let auth = auth_code.clone();
        let token = token.clone();
        let requester_port = ctx.local_port;
        tokio::spawn(async move {
            client::pull_session(
                &ip,
                port,
                &files,
                &dest,
                requester_port,
                auth.as_deref(),
                Some(&token),
                false,
            )
            .await
        })
    };
    let title = format!("Receiving from {peer_label}");
    let mut gauge = SpeedGauge::default();
    let result = loop {
        if task.is_finished() {
            break match task.await {
                Ok(res) => res,
                Err(err) => Err(anyhow!("receive task failed: {err}")),
            };
        }
        crate::picker::pump_quit_only()?;
        render_transfer_card(
            screen,
            &title,
            &source_label,
            &local_dest,
            &ctx.recv_progress,
            &mut gauge,
        )?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // Consume the token if the transfer never used it.
    ctx.tokens.lock().unwrap().remove(&token);

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
    let home = dirs::home_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let desktop = dirs::desktop_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let downloads = dirs::download_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let documents = dirs::document_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default();

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
        match input_text(screen, "Local path", "esc to go back", false)? {
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
    let (mut stream, _, _) = client::connect_and_handshake(ip, port).await?;
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

        let entries = match read_control_timeout(&mut stream, REPLY_TIMEOUT).await? {
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
        StartChoice::Manual => input_text(screen, "Enter path", "esc to go back", false)?
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
        let choice = screen.choose_multi(
            &prompt,
            items.clone(),
            last_idx.min(items.len() - 1),
            "space select · enter open dir / confirm · esc up",
        )?;

        match choice {
            MultiChoice::Cancel => {
                // esc: go up, or back out at a root
                match current_dir.parent() {
                    Some(parent) => {
                        current_dir = parent.to_path_buf();
                        last_idx = 0;
                    }
                    None => return Ok(None),
                }
            }
            MultiChoice::Toggle(idx) => {
                last_idx = idx;
                match idx {
                    0 | 1 => {}
                    2 => toggle(&mut selected, &current_dir),
                    i => {
                        // files and folders both toggle with space
                        let (_, path, _, _) = &entries[i - 3];
                        toggle(&mut selected, path);
                    }
                }
            }
            MultiChoice::Pick(idx) => {
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
                            // Enter on a file confirms — include it if needed.
                            if !selected.contains(path) {
                                selected.push(path.clone());
                            }
                            return Ok(Some(selected));
                        }
                    }
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
    use super::{Pick, classify_pick};

    #[test]
    fn pick_indices_map_to_the_right_sections() {
        // 2 hosts, 1 connected peer → items: h0 h1 p0 manual rescan quit
        assert!(matches!(classify_pick(0, 2, 1), Pick::Host(0)));
        assert!(matches!(classify_pick(1, 2, 1), Pick::Host(1)));
        assert!(matches!(classify_pick(2, 2, 1), Pick::Peer(0)));
        assert!(matches!(classify_pick(3, 2, 1), Pick::Manual));
        assert!(matches!(classify_pick(4, 2, 1), Pick::Rescan));
        assert!(matches!(classify_pick(5, 2, 1), Pick::Quit));
        // empty list → items: manual rescan quit
        assert!(matches!(classify_pick(0, 0, 0), Pick::Manual));
        assert!(matches!(classify_pick(1, 0, 0), Pick::Rescan));
        assert!(matches!(classify_pick(2, 0, 0), Pick::Quit));
    }

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
