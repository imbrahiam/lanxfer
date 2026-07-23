use anyhow::{Result, anyhow, bail};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

use crate::client;
use crate::discovery;
use crate::protocol::{
    ControlMessage, DirSpec, FileAction, FileSpec, PROTOCOL_VERSION, PlanAction, RemoteFileSpec,
    read_control, send_control,
};
use crate::storage;
use crate::util;

pub fn ensure_pairing_code(opt: Option<String>) -> Result<String> {
    let code = opt.unwrap_or_else(util::generate_pairing_code);
    let code = code.trim().to_uppercase();
    if code.len() < 8 {
        bail!("custom pairing codes must be at least 8 characters");
    }
    if code.len() > 64 || !code.chars().all(|c| c.is_ascii_alphanumeric()) {
        bail!("pairing codes must contain 8-64 ASCII letters or digits");
    }
    Ok(code)
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;
const MAX_CONNECTIONS: usize = 128;
const MAX_SESSIONS: usize = 32;
const MAX_MANIFEST_FILES: usize = 100_000;
const MAX_MANIFEST_DIRS: usize = 100_000;
const MAX_RELATIVE_PATH_BYTES: usize = 4096;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DATA_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_AUTH_FAILURES: u32 = 5;
const AUTH_BLOCK_TIME: Duration = Duration::from_secs(30);

/// One-time tokens registered by our own pull requests: when we ask a peer
/// to push files back to us, the write-back BeginSession authenticates with
/// this token instead of our pairing code (which the peer must never learn).
pub type PullTokens = Arc<Mutex<HashSet<String>>>;

#[derive(Default)]
struct AuthLimiter {
    attempts: Mutex<HashMap<IpAddr, AuthAttempt>>,
}

struct AuthAttempt {
    failures: u32,
    blocked_until: Option<Instant>,
}

impl AuthLimiter {
    fn is_blocked(&self, peer: IpAddr) -> Option<Duration> {
        let mut attempts = self.attempts.lock().unwrap();
        let attempt = attempts.get_mut(&peer)?;
        let blocked_until = attempt.blocked_until?;
        let remaining = blocked_until.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            attempts.remove(&peer);
            None
        } else {
            Some(remaining)
        }
    }

    fn success(&self, peer: IpAddr) {
        self.attempts.lock().unwrap().remove(&peer);
    }

    fn failure(&self, peer: IpAddr) {
        let mut attempts = self.attempts.lock().unwrap();
        let attempt = attempts.entry(peer).or_insert(AuthAttempt {
            failures: 0,
            blocked_until: None,
        });
        attempt.failures = attempt.failures.saturating_add(1);
        if attempt.failures >= MAX_AUTH_FAILURES {
            attempt.blocked_until = Some(Instant::now() + AUTH_BLOCK_TIME);
        }
    }
}

fn consume_pull_token(tokens: &PullTokens, proof: &str, challenge: &str) -> bool {
    let mut tokens = tokens.lock().unwrap();
    let matching = tokens
        .iter()
        .find(|token| util::constant_time_eq(proof, &util::auth_proof(token, challenge)))
        .cloned();
    match matching {
        Some(token) => tokens.remove(&token),
        None => false,
    }
}

struct Session {
    dest_root: String,
    overwrite: bool,
    files: Mutex<HashMap<u32, FileState>>,
    /// FileDone / RestartAck messages routed to the control connection.
    out_tx: mpsc::UnboundedSender<ControlMessage>,
    /// Live counters shown by the receiving side's UI.
    progress: Arc<crate::progress::Progress>,
}

struct FileState {
    size: u64,
    mtime_secs: i64,
    final_path: PathBuf,
    part_path: PathBuf,
    start_offset: u64,
    received: u64,
    /// Plain (non-striped) files: streaming hasher, pre-seeded with the
    /// resume prefix. Taken while a unit is in flight.
    hasher: Option<blake3::Hasher>,
    /// Striped files: per-stripe subtree chaining values.
    stripe_cvs: HashMap<u32, util::StripeCv>,
    hash: Option<String>,
    done: bool,
    committed: bool,
    expects_data: bool,
}

/// Serve control connections on an already-bound listener. Binding happens
/// at the call site so a busy port fails fast and visibly, not inside a
/// background task.
#[allow(clippy::too_many_arguments)]
pub async fn run_server(
    listener: TcpListener,
    discovery_port: u16,
    pairing_code: String,
    quiet_errors: bool,
    require_auth: bool,
    ui_tx: Option<mpsc::UnboundedSender<super::interactive::ServerEvent>>,
    pull_tokens: PullTokens,
    recv_progress: Arc<crate::progress::Progress>,
    send_progress: Arc<crate::progress::Progress>,
) -> Result<()> {
    let local = listener.local_addr()?;
    let device = util::local_device_info();
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    let auth_limiter = Arc::new(AuthLimiter::default());
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    info!("server listening on {local} (auth: {require_auth})");

    let discovery_device = device.clone();
    tokio::spawn(async move {
        if let Err(err) =
            discovery::run_responder(discovery_port, local.port(), require_auth, discovery_device)
                .await
            && !quiet_errors
        {
            let _ = err;
        }
    });

    loop {
        let permit = Arc::clone(&connection_limit).acquire_owned().await?;
        let (socket, peer) = listener.accept().await?;
        debug!("accepted connection from {peer}");
        let server_device = device.clone();
        let server_code = pairing_code.clone();
        let sessions = Arc::clone(&sessions);
        let ui_tx = ui_tx.clone();
        let pull_tokens = Arc::clone(&pull_tokens);
        let recv_progress = Arc::clone(&recv_progress);
        let send_progress = Arc::clone(&send_progress);
        let auth_limiter = Arc::clone(&auth_limiter);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_client(
                socket,
                server_device,
                server_code,
                require_auth,
                sessions,
                ui_tx,
                pull_tokens,
                recv_progress,
                send_progress,
                auth_limiter,
            )
            .await
            {
                warn!("connection from {peer} ended with error: {err:#}");
                let _ = quiet_errors;
            }
        });
    }
}

fn tune_socket(stream: &TcpStream) {
    let sock = socket2::SockRef::from(stream);
    let _ = sock.set_send_buffer_size(4 * 1024 * 1024);
    let _ = sock.set_recv_buffer_size(4 * 1024 * 1024);
}

#[allow(clippy::too_many_arguments)]
async fn handle_client(
    mut stream: TcpStream,
    server_device: crate::protocol::DeviceInfo,
    pairing_code: String,
    require_auth: bool,
    sessions: Sessions,
    ui_tx: Option<mpsc::UnboundedSender<super::interactive::ServerEvent>>,
    pull_tokens: PullTokens,
    recv_progress: Arc<crate::progress::Progress>,
    send_progress: Arc<crate::progress::Progress>,
    auth_limiter: Arc<AuthLimiter>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    tune_socket(&stream);

    let peer_ip = stream.peer_addr()?.ip();
    let first = tokio::time::timeout(HANDSHAKE_TIMEOUT, read_control(&mut stream))
        .await
        .map_err(|_| anyhow!("handshake timed out"))??;
    let challenge = util::auth_challenge();
    let (client_name, client_port) = match first {
        ControlMessage::Hello {
            version,
            client_name,
            client_port,
        } if version == PROTOCOL_VERSION => {
            send_control(
                &mut stream,
                &ControlMessage::HelloAck {
                    version: PROTOCOL_VERSION,
                    server: server_device,
                    auth_required: require_auth,
                    auth_challenge: challenge.clone(),
                },
            )
            .await?;
            (client_name, client_port)
        }
        ControlMessage::Hello { version, .. } => {
            let _ = send_control(
                &mut stream,
                &ControlMessage::Error {
                    message: format!(
                        "protocol version mismatch: client={version}, server={PROTOCOL_VERSION}"
                    ),
                },
            )
            .await;
            return Ok(());
        }
        _ => {
            let _ = send_control(
                &mut stream,
                &ControlMessage::Error {
                    message: "expected hello".to_string(),
                },
            )
            .await;
            return Ok(());
        }
    };

    loop {
        let msg = match tokio::time::timeout(CONTROL_IDLE_TIMEOUT, read_control(&mut stream)).await
        {
            Ok(Ok(msg)) => msg,
            Ok(Err(_)) => return Ok(()),
            Err(_) => bail!("control connection idle timeout"),
        };

        match msg {
            ControlMessage::Attach => {
                // Interactive presence link — hand the whole connection to
                // the UI so this peer shows up in the picker. Headless serve
                // just keeps the link open (peer stays connected, sends
                // requests over separate connections).
                if let Some(tx) = &ui_tx {
                    let client_ip = stream
                        .peer_addr()
                        .map(|a| a.ip().to_string())
                        .unwrap_or_default();
                    let _ = tx.send(super::interactive::ServerEvent::PeerConnected(
                        stream,
                        client_name.clone(),
                        client_port,
                        client_ip,
                    ));
                    return Ok(());
                }
            }
            ControlMessage::ListDestinations { auth_code } => {
                if let Err(err) = authorize(
                    auth_code.as_deref(),
                    &pairing_code,
                    require_auth,
                    &challenge,
                    peer_ip,
                    &auth_limiter,
                    None,
                ) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let items = storage::list_destinations();
                send_control(&mut stream, &ControlMessage::Destinations { items }).await?;
            }
            ControlMessage::BrowseDirectory {
                destination_path,
                relative_path,
                auth_code,
            } => {
                let reply = browse_reply(
                    &destination_path,
                    relative_path,
                    auth_code.as_deref(),
                    &pairing_code,
                    require_auth,
                    &challenge,
                    peer_ip,
                    &auth_limiter,
                );
                send_control(&mut stream, &reply).await?;
            }
            ControlMessage::BeginSession {
                destination_path,
                auth_code,
                overwrite,
                dry_run,
                files,
                dirs,
            } => {
                info!(
                    "BeginSession from {client_name}: {} files, {} dirs -> {destination_path}",
                    files.len(),
                    dirs.len()
                );
                // A pull we initiated authenticates with its one-time token
                // instead of our pairing code.
                if let Err(err) = authorize(
                    auth_code.as_deref(),
                    &pairing_code,
                    require_auth,
                    &challenge,
                    peer_ip,
                    &auth_limiter,
                    Some(&pull_tokens),
                ) {
                    warn!("BeginSession rejected: {err}");
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                if dry_run {
                    let reply =
                        match plan_session(&destination_path, overwrite, &files, &dirs, true).await
                        {
                            Ok((actions, _)) => ControlMessage::SessionPlan {
                                session_id: String::new(),
                                actions,
                            },
                            Err(err) => ControlMessage::Error {
                                message: err.to_string(),
                            },
                        };
                    send_control(&mut stream, &reply).await?;
                    continue;
                }
                if sessions.lock().unwrap().len() >= MAX_SESSIONS {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: "receiver is busy; too many active sessions".to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                // Real session: this connection becomes the session's control
                // channel until the client disconnects.
                return run_session_control(
                    stream,
                    sessions,
                    destination_path,
                    overwrite,
                    files,
                    dirs,
                    recv_progress,
                )
                .await;
            }
            ControlMessage::PushRequest {
                files,
                dest_local_path,
                requester_port,
                auth_code,
                overwrite,
                return_auth_code,
            } => {
                info!(
                    "PushRequest from {client_name}: {} files -> {dest_local_path} (reply port {requester_port})",
                    files.len()
                );
                if let Err(err) = authorize(
                    auth_code.as_deref(),
                    &pairing_code,
                    require_auth,
                    &challenge,
                    peer_ip,
                    &auth_limiter,
                    None,
                ) {
                    warn!("PushRequest rejected: {err}");
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let requester_ip = match stream.peer_addr() {
                    Ok(addr) => addr.ip().to_string(),
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: format!("cannot determine requester address: {err}"),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                send_control(&mut stream, &ControlMessage::JoinAck).await?;
                let send_progress = Arc::clone(&send_progress);
                tokio::spawn(async move {
                    if let Err(err) = handle_push_request(
                        stream,
                        &requester_ip,
                        requester_port,
                        &files,
                        &dest_local_path,
                        return_auth_code.as_deref(),
                        overwrite,
                        send_progress,
                    )
                    .await
                    {
                        warn!("push request failed: {err:#}");
                    }
                });
                return Ok(());
            }
            ControlMessage::JoinSession { session_id } => {
                let session = sessions.lock().unwrap().get(&session_id).cloned();
                let Some(session) = session else {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: "unknown session".to_string(),
                        },
                    )
                    .await?;
                    continue;
                };
                send_control(&mut stream, &ControlMessage::JoinAck).await?;
                // This connection becomes a data channel.
                let (read_half, _write_half) = stream.into_split();
                let reader = BufReader::with_capacity(4 * 1024 * 1024, read_half);
                return run_data_conn(reader, session).await;
            }
            _ => {
                send_control(
                    &mut stream,
                    &ControlMessage::Error {
                        message: "unsupported control message in current state".to_string(),
                    },
                )
                .await?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn browse_reply(
    destination_path: &str,
    relative_path: String,
    auth_code: Option<&str>,
    pairing_code: &str,
    require_auth: bool,
    challenge: &str,
    peer_ip: IpAddr,
    auth_limiter: &AuthLimiter,
) -> ControlMessage {
    if let Err(err) = authorize(
        auth_code,
        pairing_code,
        require_auth,
        challenge,
        peer_ip,
        auth_limiter,
        None,
    ) {
        return ControlMessage::Error {
            message: err.to_string(),
        };
    }
    let result = (|| -> Result<Vec<crate::protocol::DirEntry>> {
        let root = storage::ensure_destination_root(destination_path)?;
        let target = if relative_path.is_empty() {
            root
        } else {
            storage::build_target_directory(destination_path, &relative_path)?
        };
        storage::list_directory(&target)
    })();
    match result {
        Ok(entries) => ControlMessage::DirectoryContents {
            relative_path,
            entries,
        },
        Err(err) => ControlMessage::Error {
            message: err.to_string(),
        },
    }
}

async fn run_session_control(
    stream: TcpStream,
    sessions: Sessions,
    destination_path: String,
    overwrite: bool,
    files: Vec<FileSpec>,
    dirs: Vec<DirSpec>,
    recv_progress: Arc<crate::progress::Progress>,
) -> Result<()> {
    let (actions, states) =
        match plan_session(&destination_path, overwrite, &files, &dirs, false).await {
            Ok(v) => v,
            Err(err) => {
                let mut stream = stream;
                send_control(
                    &mut stream,
                    &ControlMessage::Error {
                        message: err.to_string(),
                    },
                )
                .await?;
                return Ok(());
            }
        };

    // Live receive counters: only files that will actually stream.
    let (plan_bytes, plan_files) = states
        .values()
        .filter(|s| s.expects_data)
        .fold((0u64, 0u64), |(b, n), s| {
            (b + (s.size - s.start_offset), n + 1)
        });
    recv_progress.reset_if_idle();
    recv_progress.add_totals(plan_bytes, plan_files);
    info!("session planned: {plan_files} files, {plan_bytes} bytes to receive");

    let session_id = uuid::Uuid::new_v4().simple().to_string();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ControlMessage>();
    let session = Arc::new(Session {
        dest_root: destination_path,
        overwrite,
        files: Mutex::new(states),
        out_tx,
        progress: recv_progress,
    });
    sessions
        .lock()
        .unwrap()
        .insert(session_id.clone(), Arc::clone(&session));

    let (mut read_half, mut write_half) = stream.into_split();
    send_control(
        &mut write_half,
        &ControlMessage::SessionPlan {
            session_id: session_id.clone(),
            actions,
        },
    )
    .await?;

    // Single writer for FileDone/RestartAck — data connections feed out_tx.
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if send_control(&mut write_half, &msg).await.is_err() {
                break;
            }
        }
    });

    // Read side: RestartFile requests before data flow, then EOF when the
    // client is finished with the session.
    loop {
        match read_control(&mut read_half).await {
            Ok(ControlMessage::RestartFile { id }) => {
                let reply = match restart_file(&session, id).await {
                    Ok(()) => ControlMessage::RestartAck { id },
                    Err(err) => ControlMessage::Error {
                        message: format!("restart {id}: {err}"),
                    },
                };
                let _ = session.out_tx.send(reply);
            }
            Ok(ControlMessage::CommitFile { id, expected_hash }) => {
                let reply = match commit_file(&session, id, &expected_hash).await {
                    Ok(()) => ControlMessage::CommitAck {
                        id,
                        ok: true,
                        error: None,
                    },
                    Err(err) => ControlMessage::CommitAck {
                        id,
                        ok: false,
                        error: Some(err.to_string()),
                    },
                };
                let _ = session.out_tx.send(reply);
            }
            Ok(_) => {
                let _ = session.out_tx.send(ControlMessage::Error {
                    message: "unexpected message on session control connection".to_string(),
                });
            }
            Err(_) => break, // client disconnected — session over
        }
    }

    sessions.lock().unwrap().remove(&session_id);
    drop(session);
    writer_task.abort();
    Ok(())
}

/// Decide per-file actions and build receiver-side state.
/// `dry_run` skips all side effects (no mkdir, no part files).
async fn plan_session(
    destination_path: &str,
    overwrite: bool,
    files: &[FileSpec],
    dirs: &[DirSpec],
    dry_run: bool,
) -> Result<(Vec<FileAction>, HashMap<u32, FileState>)> {
    validate_manifest(files, dirs)?;
    storage::ensure_destination_root(destination_path)?;

    if !dry_run {
        for dir in dirs {
            let path = storage::build_target_directory(destination_path, &dir.rel_path)?;
            fs::create_dir_all(&path).await?;
            let _ = util::set_mtime(&path, dir.mtime_secs).await;
        }
    }

    let mut actions = Vec::with_capacity(files.len());
    let mut states = HashMap::new();
    let mut target_paths = HashSet::new();

    for spec in files {
        let (final_path, part_path) =
            storage::build_target_paths(destination_path, &spec.rel_path)?;
        if !target_paths.insert(final_path.clone()) || !target_paths.insert(part_path.clone()) {
            bail!("manifest contains colliding target path: {}", spec.rel_path);
        }

        let mut state = FileState {
            size: spec.size,
            mtime_secs: spec.mtime_secs,
            final_path: final_path.clone(),
            part_path: part_path.clone(),
            start_offset: 0,
            received: 0,
            hasher: None,
            stripe_cvs: HashMap::new(),
            hash: None,
            done: false,
            committed: false,
            expects_data: true,
        };

        let action = if let Ok(meta) = fs::metadata(&final_path).await {
            let existing_mtime = meta
                .modified()
                .map(util::system_time_secs)
                .unwrap_or(i64::MIN);
            if meta.len() == spec.size && (existing_mtime - spec.mtime_secs).abs() <= 2 {
                state.expects_data = false;
                PlanAction::SkipUpToDate
            } else if !overwrite {
                state.expects_data = false;
                PlanAction::Conflict
            } else {
                prepare_fresh(&mut state, dry_run).await?;
                PlanAction::Send
            }
        } else {
            match fs::metadata(&part_path).await {
                Ok(part_meta)
                    if !util::is_striped(spec.size)
                        && part_meta.len() > 0
                        && part_meta.len() <= spec.size =>
                {
                    // Resumable prefix: hash it now and keep the live hasher
                    // so verification stays single-pass across the boundary.
                    let offset = part_meta.len();
                    let hasher = util::hash_prefix_hasher(&part_path, offset).await?;
                    let partial_hash = hasher.clone().finalize().to_hex().to_string();
                    state.start_offset = offset;
                    state.hasher = Some(hasher);
                    PlanAction::Resume {
                        offset,
                        partial_hash,
                    }
                }
                _ => {
                    // ponytail: interrupted striped files restart from zero.
                    // Add a stripe bitmap in the part file if 64+ MiB re-sends
                    // ever hurt in practice.
                    prepare_fresh(&mut state, dry_run).await?;
                    PlanAction::Send
                }
            }
        };

        actions.push(FileAction {
            id: spec.id,
            action,
        });
        states.insert(spec.id, state);
    }

    Ok((actions, states))
}

fn validate_manifest(files: &[FileSpec], dirs: &[DirSpec]) -> Result<()> {
    if files.len() > MAX_MANIFEST_FILES {
        bail!(
            "manifest has too many files: {} (maximum {MAX_MANIFEST_FILES})",
            files.len()
        );
    }
    if dirs.len() > MAX_MANIFEST_DIRS {
        bail!(
            "manifest has too many directories: {} (maximum {MAX_MANIFEST_DIRS})",
            dirs.len()
        );
    }

    let mut ids = HashSet::with_capacity(files.len());
    let mut paths = HashSet::with_capacity(files.len() + dirs.len());
    let mut total_bytes = 0u64;
    for file in files {
        if !ids.insert(file.id) {
            bail!("manifest contains duplicate file id {}", file.id);
        }
        validate_manifest_path(&file.rel_path, &mut paths)?;
        total_bytes = total_bytes
            .checked_add(file.size)
            .ok_or_else(|| anyhow!("manifest byte total overflow"))?;
    }
    for dir in dirs {
        validate_manifest_path(&dir.rel_path, &mut paths)?;
    }
    let _ = total_bytes;
    Ok(())
}

fn validate_manifest_path(path: &str, seen: &mut HashSet<PathBuf>) -> Result<()> {
    if path.len() > MAX_RELATIVE_PATH_BYTES {
        bail!("manifest path exceeds {MAX_RELATIVE_PATH_BYTES} bytes");
    }
    let normalized = storage::sanitize_relative_path(path)?;
    if !seen.insert(normalized) {
        bail!("manifest contains duplicate path: {path}");
    }
    Ok(())
}

/// Prepare state for a from-scratch transfer. Plain files create their part
/// file lazily on first write (data workers parallelize those syscalls —
/// creating 10k parts serially at plan time dominates small-file transfers).
/// Striped files preallocate now so parallel stripe writers never race.
async fn prepare_fresh(state: &mut FileState, dry_run: bool) -> Result<()> {
    if dry_run {
        return Ok(());
    }
    if util::is_striped(state.size) {
        if let Some(parent) = state.part_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&state.part_path)
            .await?;
        file.set_len(state.size).await?;
    } else {
        state.hasher = Some(blake3::Hasher::new());
    }
    Ok(())
}

/// Open the part file for writing, creating parents on demand. A fresh
/// plain-file transfer (start at 0) truncates, so a stale longer part can
/// never leak trailing bytes past the rename.
async fn open_part(part_path: &std::path::Path, truncate: bool) -> Result<fs::File> {
    let mut opts = OpenOptions::new();
    opts.create(true).write(true).truncate(truncate);
    match opts.open(part_path).await {
        Ok(f) => Ok(f),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = part_path.parent() {
                fs::create_dir_all(parent).await?;
            }
            Ok(opts.open(part_path).await?)
        }
        Err(e) => Err(e.into()),
    }
}

async fn restart_file(session: &Arc<Session>, id: u32) -> Result<()> {
    let part_path = {
        let mut files = session.files.lock().unwrap();
        let state = files.get_mut(&id).ok_or_else(|| anyhow!("unknown file"))?;
        state.start_offset = 0;
        state.received = 0;
        state.hasher = Some(blake3::Hasher::new());
        state.stripe_cvs.clear();
        state.hash = None;
        state.done = false;
        state.committed = false;
        state.part_path.clone()
    };
    let file = OpenOptions::new().write(true).open(&part_path).await?;
    file.set_len(0).await?;
    Ok(())
}

async fn run_data_conn(mut reader: BufReader<OwnedReadHalf>, session: Arc<Session>) -> Result<()> {
    loop {
        let msg = match tokio::time::timeout(DATA_IDLE_TIMEOUT, read_control(&mut reader)).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(_)) => return Ok(()), // sender closed the data connection
            Err(_) => bail!("data connection idle timeout"),
        };
        match msg {
            ControlMessage::SendFile { id, offset, len } => {
                if let Err(err) = receive_unit(&mut reader, &session, id, offset, len).await {
                    let _ = session.out_tx.send(ControlMessage::FileDone {
                        id,
                        hash: String::new(),
                        ok: false,
                        error: Some(err.to_string()),
                    });
                    return Err(err); // stream position unknown — drop conn
                }
            }
            other => bail!("unexpected message on data connection: {other:?}"),
        }
    }
}

async fn receive_unit(
    reader: &mut BufReader<OwnedReadHalf>,
    session: &Arc<Session>,
    id: u32,
    offset: u64,
    len: u64,
) -> Result<()> {
    // Pull what we need out of the state, holding the lock only briefly.
    let (part_path, striped, mut hasher, stripe_index) = {
        let mut files = session.files.lock().unwrap();
        let state = files
            .get_mut(&id)
            .ok_or_else(|| anyhow!("unknown file id {id}"))?;
        if state.done || !state.expects_data {
            bail!("unexpected data for file {id}");
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| anyhow!("unit range overflow for file {id}"))?;
        if end > state.size {
            bail!("unit out of bounds for file {id}");
        }
        let striped = util::is_striped(state.size);
        if striped {
            let (stripe_start, stripe_len) =
                util::stripe_range(state.size, (offset / util::STRIPE_SIZE) as u32);
            if offset != stripe_start || len != stripe_len {
                bail!("unit is not stripe-aligned for file {id}");
            }
            if state
                .stripe_cvs
                .contains_key(&((offset / util::STRIPE_SIZE) as u32))
            {
                bail!("duplicate stripe for file {id}");
            }
            (
                state.part_path.clone(),
                true,
                util::stripe_hasher((offset / util::STRIPE_SIZE) as u32),
                (offset / util::STRIPE_SIZE) as u32,
            )
        } else {
            let hasher = state
                .hasher
                .take()
                .ok_or_else(|| anyhow!("file {id} already has a writer"))?;
            if hasher.count() != offset {
                let count = hasher.count();
                // put it back before failing so a later attempt can see state
                state.hasher = Some(hasher);
                bail!("non-sequential write for file {id}: hashed {count}, unit at {offset}");
            }
            (state.part_path.clone(), false, hasher, 0)
        }
    };

    let unit_key = crate::progress::unit_key(id, striped.then_some(stripe_index));
    {
        let name = part_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| format!("file {id}"));
        let label = if striped {
            format!("{name} [{}]", stripe_index + 1)
        } else {
            name
        };
        session.progress.begin_unit(unit_key, label, len);
    }

    let mut file = open_part(&part_path, !striped && offset == 0).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let mut buf = vec![0u8; 4 * 1024 * 1024];
    let mut remaining = len;
    let write_result: Result<()> = async {
        while remaining > 0 {
            let to_read = usize::min(remaining as usize, buf.len());
            tokio::time::timeout(DATA_IDLE_TIMEOUT, reader.read_exact(&mut buf[..to_read]))
                .await
                .map_err(|_| anyhow!("transfer read timed out"))?
                .map_err(|e| anyhow!("transfer read error: {e}"))?;
            hasher.update(&buf[..to_read]);
            file.write_all(&buf[..to_read]).await?;
            session.progress.advance(unit_key, to_read as u64);
            remaining -= to_read as u64;
        }
        file.flush().await?;
        Ok(())
    }
    .await;
    session.progress.end_unit(unit_key);
    write_result?;
    drop(file);

    // Update state; detect completion.
    let finalize = {
        let mut files = session.files.lock().unwrap();
        let state = files
            .get_mut(&id)
            .ok_or_else(|| anyhow!("file state vanished"))?;
        state.received = state
            .received
            .checked_add(len)
            .ok_or_else(|| anyhow!("received byte count overflow for file {id}"))?;
        if state.received > state.size - state.start_offset {
            bail!("received too much data for file {id}");
        }
        if striped {
            state
                .stripe_cvs
                .insert(stripe_index, util::finish_stripe(&hasher));
        } else {
            state.hasher = Some(hasher);
        }
        let complete = state.received == state.size - state.start_offset;
        if complete && !state.done {
            state.done = true;
            let hash = if striped {
                let count = util::stripe_count(state.size);
                let mut cvs = Vec::with_capacity(count as usize);
                for i in 0..count {
                    cvs.push(
                        *state
                            .stripe_cvs
                            .get(&i)
                            .ok_or_else(|| anyhow!("missing stripe {i} for file {id}"))?,
                    );
                }
                util::merge_stripes(&cvs, state.size).to_hex().to_string()
            } else {
                state
                    .hasher
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing hasher for file {id}"))?
                    .finalize()
                    .to_hex()
                    .to_string()
            };
            state.hash = Some(hash.clone());
            Some(hash)
        } else {
            None
        }
    };

    if let Some(hash) = finalize {
        debug!("file {id} staged and awaiting hash commit");
        let _ = session.out_tx.send(ControlMessage::FileDone {
            id,
            hash,
            ok: true,
            error: None,
        });
    }
    let _ = session.dest_root; // session root retained for future use in errors
    Ok(())
}

async fn commit_file(session: &Arc<Session>, id: u32, expected_hash: &str) -> Result<()> {
    if expected_hash.len() != 64 || !expected_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid expected hash for file {id}");
    }
    let (part_path, final_path, mtime_secs) = {
        let files = session.files.lock().unwrap();
        let state = files
            .get(&id)
            .ok_or_else(|| anyhow!("unknown file id {id}"))?;
        if !state.done {
            bail!("file {id} is not fully staged");
        }
        if state.committed {
            bail!("file {id} was already committed");
        }
        let actual = state
            .hash
            .as_deref()
            .ok_or_else(|| anyhow!("file {id} has no staged hash"))?;
        if !util::constant_time_eq(actual, expected_hash) {
            bail!("hash mismatch for file {id}; staged data was preserved");
        }
        (
            state.part_path.clone(),
            state.final_path.clone(),
            state.mtime_secs,
        )
    };

    let staged = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&part_path)
        .await?;
    staged.sync_all().await?;
    drop(staged);

    if session.overwrite {
        if let Err(first_err) = fs::rename(&part_path, &final_path).await {
            if fs::metadata(&final_path).await.is_ok() {
                fs::remove_file(&final_path).await?;
                fs::rename(&part_path, &final_path).await?;
            } else {
                return Err(first_err.into());
            }
        }
    } else {
        fs::hard_link(&part_path, &final_path)
            .await
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    anyhow!(
                        "destination appeared during transfer: {}",
                        final_path.display()
                    )
                } else {
                    anyhow!(
                        "could not atomically commit {}: {err}",
                        final_path.display()
                    )
                }
            })?;
        if let Err(err) = fs::remove_file(&part_path).await {
            warn!(
                "committed {} but could not remove staged link {}: {err}",
                final_path.display(),
                part_path.display()
            );
        }
    }
    if let Err(err) = util::set_mtime(&final_path, mtime_secs).await {
        warn!(
            "could not preserve mtime for {}: {err}",
            final_path.display()
        );
    }

    let mut files = session.files.lock().unwrap();
    let state = files
        .get_mut(&id)
        .ok_or_else(|| anyhow!("file state vanished"))?;
    state.committed = true;
    session.progress.file_done();
    debug!("file {id} committed -> {}", final_path.display());
    Ok(())
}

fn authorize(
    provided: Option<&str>,
    expected_secret: &str,
    require_auth: bool,
    challenge: &str,
    peer_ip: IpAddr,
    limiter: &AuthLimiter,
    pull_tokens: Option<&PullTokens>,
) -> Result<()> {
    if !require_auth {
        return Ok(());
    }
    if let Some(remaining) = limiter.is_blocked(peer_ip) {
        bail!(
            "too many invalid pairing attempts; retry in {} seconds",
            remaining.as_secs().max(1)
        );
    }
    let proof = provided.unwrap_or_default().trim();
    if proof.is_empty() {
        limiter.failure(peer_ip);
        bail!("pairing code is required for write operations");
    }
    let expected = util::auth_proof(expected_secret, challenge);
    let pairing_matches = util::constant_time_eq(proof, &expected);
    let token_matches = pull_tokens
        .map(|tokens| consume_pull_token(tokens, proof, challenge))
        .unwrap_or(false);
    if pairing_matches || token_matches {
        limiter.success(peer_ip);
        return Ok(());
    }
    limiter.failure(peer_ip);
    bail!("invalid pairing code")
}

/// Handle an incoming PushRequest: verify requested files exist locally,
/// then act as sender — connect back to the requester's server and stream
/// the files using the existing v5 protocol. `return_auth_code` is the
/// requester's one-time token, presented back to its server.
#[allow(clippy::too_many_arguments)]
async fn handle_push_request(
    mut stream: TcpStream,
    requester_ip: &str,
    requester_port: u16,
    requested_files: &[RemoteFileSpec],
    dest_local_path: &str,
    return_auth_code: Option<&str>,
    overwrite: bool,
    send_progress: Arc<crate::progress::Progress>,
) -> Result<()> {
    // Build the local source paths from the remote file specs.
    let sources: Vec<PathBuf> = requested_files
        .iter()
        .map(|f| PathBuf::from(&f.abs_path))
        .collect();

    // Verify all selected paths exist before starting the transfer. Sources
    // may be files or directories; send_session recursively scans directories
    // into the normal resumable manifest.
    for path in &sources {
        if !path.exists() {
            let _ = send_control(
                &mut stream,
                &ControlMessage::PushComplete {
                    files_sent: 0,
                    bytes: 0,
                    errors: vec![format!("file not found: {}", path.display())],
                },
            )
            .await;
            return Ok(());
        }
        if !path.is_file() && !path.is_dir() {
            let _ = send_control(
                &mut stream,
                &ControlMessage::PushComplete {
                    files_sent: 0,
                    bytes: 0,
                    errors: vec![format!("not a file or directory: {}", path.display())],
                },
            )
            .await;
            return Ok(());
        }
    }

    info!(
        "pushing {} files back to {requester_ip}:{requester_port} -> {dest_local_path}",
        sources.len()
    );
    let summary = client::send_session(
        requester_ip,
        requester_port,
        &sources,
        dest_local_path,
        return_auth_code,
        client::SendOptions {
            overwrite,
            dry_run: false,
            jobs: None,
            show_progress: false,
            progress: Some(send_progress),
        },
    )
    .await;

    let (files_sent, bytes, errors) = match summary {
        Ok(s) => (s.transferred, s.bytes, s.errors),
        Err(err) => (0, 0, vec![err.to_string()]),
    };

    let _ = send_control(
        &mut stream,
        &ControlMessage::PushComplete {
            files_sent,
            bytes,
            errors,
        },
    )
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
    use crate::protocol::{FileSpec, PlanAction};
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::io::AsyncWriteExt;

    /// Full pull round-trip over localhost: requester B (auth on) registers a
    /// one-time token, remote A pushes the file back authenticating with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pull_directory_round_trip_uses_one_time_token() -> Result<()> {
        let base =
            std::env::temp_dir().join(format!("lanxfer-pull-{}", uuid::Uuid::new_v4().simple()));
        let src = base.join("src");
        let dst = base.join("dst");
        std::fs::create_dir_all(&src)?;
        std::fs::create_dir_all(&dst)?;
        let file = src.join("hello.txt");
        std::fs::write(&file, b"pull me")?;
        std::fs::create_dir_all(src.join("nested"))?;
        std::fs::write(src.join("nested/world.txt"), b"recursively")?;
        std::fs::create_dir_all(src.join("empty"))?;

        // Peer A: owns the file, open (no auth).
        let a = TcpListener::bind("127.0.0.1:0").await?;
        let a_port = a.local_addr()?.port();
        tokio::spawn(run_server(
            a,
            0,
            "AAAA".into(),
            true,
            false,
            None,
            PullTokens::default(),
            Arc::default(),
            Arc::default(),
        ));

        // Peer B (the requester): auth required, one-time token registered.
        let b = TcpListener::bind("127.0.0.1:0").await?;
        let b_port = b.local_addr()?.port();
        let tokens = PullTokens::default();
        tokens.lock().unwrap().insert("TOK".into());
        tokio::spawn(run_server(
            b,
            0,
            "BBBB".into(),
            true,
            true,
            None,
            Arc::clone(&tokens),
            Arc::default(),
            Arc::default(),
        ));

        let spec = RemoteFileSpec {
            id: 0,
            abs_path: src.to_string_lossy().into_owned(),
            rel_path: "src".into(),
            size: 0,
            mtime_secs: 0,
        };
        let summary = client::pull_session(
            "127.0.0.1",
            a_port,
            std::slice::from_ref(&spec),
            dst.to_str().unwrap(),
            b_port,
            None,
            Some("TOK"),
            false,
        )
        .await?;
        assert!(summary.errors.is_empty(), "{:?}", summary.errors);
        assert_eq!(summary.transferred, 2);
        assert_eq!(std::fs::read(dst.join("src/hello.txt"))?, b"pull me");
        assert_eq!(
            std::fs::read(dst.join("src/nested/world.txt"))?,
            b"recursively"
        );
        assert!(dst.join("src/empty").is_dir());
        assert!(tokens.lock().unwrap().is_empty(), "token not consumed");

        // Without a token the write-back must be rejected by B's server.
        let summary = client::pull_session(
            "127.0.0.1",
            a_port,
            &[spec],
            dst.to_str().unwrap(),
            b_port,
            None,
            None,
            false,
        )
        .await?;
        assert!(!summary.errors.is_empty(), "pull without token must fail");

        let _ = std::fs::remove_dir_all(&base);
        Ok(())
    }

    #[test]
    fn rejects_weak_custom_codes_and_abusive_manifests() {
        assert!(ensure_pairing_code(Some("ABC123".into())).is_err());
        assert_eq!(
            ensure_pairing_code(Some(" secure42 ".into())).unwrap(),
            "SECURE42"
        );

        let duplicate_ids = vec![
            FileSpec {
                id: 1,
                rel_path: "a".into(),
                size: 1,
                mtime_secs: 0,
            },
            FileSpec {
                id: 1,
                rel_path: "b".into(),
                size: 1,
                mtime_secs: 0,
            },
        ];
        assert!(validate_manifest(&duplicate_ids, &[]).is_err());

        let duplicate_paths = vec![
            FileSpec {
                id: 1,
                rel_path: "folder/file".into(),
                size: 1,
                mtime_secs: 0,
            },
            FileSpec {
                id: 2,
                rel_path: "folder/./file".into(),
                size: 1,
                mtime_secs: 0,
            },
        ];
        assert!(validate_manifest(&duplicate_paths, &[]).is_err());
    }

    #[test]
    fn repeated_bad_pairing_proofs_are_throttled() {
        let limiter = AuthLimiter::default();
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);
        for _ in 0..MAX_AUTH_FAILURES {
            assert!(
                authorize(
                    Some("bad-proof"),
                    "SECURE42",
                    true,
                    "challenge",
                    peer,
                    &limiter,
                    None,
                )
                .is_err()
            );
        }
        let correct = util::auth_proof("SECURE42", "challenge");
        let error = authorize(
            Some(&correct),
            "SECURE42",
            true,
            "challenge",
            peer,
            &limiter,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("too many invalid"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hash_mismatch_never_installs_staged_file() -> Result<()> {
        let base =
            std::env::temp_dir().join(format!("lanxfer-commit-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&base)?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        tokio::spawn(run_server(
            listener,
            0,
            "SECURE42".into(),
            true,
            false,
            None,
            PullTokens::default(),
            Arc::default(),
            Arc::default(),
        ));

        let (mut control, _, _, _) = client::connect_and_handshake("127.0.0.1", port).await?;
        send_control(
            &mut control,
            &ControlMessage::BeginSession {
                destination_path: base.to_string_lossy().into_owned(),
                auth_code: None,
                overwrite: false,
                dry_run: false,
                files: vec![FileSpec {
                    id: 0,
                    rel_path: "staged.txt".into(),
                    size: 3,
                    mtime_secs: 0,
                }],
                dirs: vec![],
            },
        )
        .await?;
        let session_id = match read_control(&mut control).await? {
            ControlMessage::SessionPlan {
                session_id,
                actions,
            } => {
                assert!(matches!(actions[0].action, PlanAction::Send));
                session_id
            }
            other => bail!("unexpected plan: {other:?}"),
        };

        let (mut data, _, _, _) = client::connect_and_handshake("127.0.0.1", port).await?;
        send_control(&mut data, &ControlMessage::JoinSession { session_id }).await?;
        assert!(matches!(
            read_control(&mut data).await?,
            ControlMessage::JoinAck
        ));
        send_control(
            &mut data,
            &ControlMessage::SendFile {
                id: 0,
                offset: 0,
                len: 3,
            },
        )
        .await?;
        data.write_all(b"bad").await?;
        data.flush().await?;

        assert!(matches!(
            read_control(&mut control).await?,
            ControlMessage::FileDone {
                id: 0,
                ok: true,
                ..
            }
        ));
        send_control(
            &mut control,
            &ControlMessage::CommitFile {
                id: 0,
                expected_hash: blake3::hash(b"not the bytes").to_hex().to_string(),
            },
        )
        .await?;
        assert!(matches!(
            read_control(&mut control).await?,
            ControlMessage::CommitAck {
                id: 0,
                ok: false,
                ..
            }
        ));
        assert!(!base.join("staged.txt").exists());
        assert!(base.join("staged.txt.lanxfer.part").exists());

        drop(data);
        drop(control);
        tokio::time::sleep(Duration::from_millis(20)).await;
        std::fs::remove_dir_all(base)?;
        Ok(())
    }
}
