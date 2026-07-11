use anyhow::{Result, anyhow, bail};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::client;
use crate::discovery;
use crate::protocol::{
    ControlMessage, DirSpec, FileAction, FileSpec, PROTOCOL_VERSION, PlanAction, RemoteFileSpec,
    read_control, send_control,
};
use crate::storage;
use crate::util;

pub fn ensure_pairing_code(opt: Option<String>) -> String {
    opt.unwrap_or_else(util::generate_pairing_code)
}

type Sessions = Arc<Mutex<HashMap<String, Arc<Session>>>>;

/// One-time tokens registered by our own pull requests: when we ask a peer
/// to push files back to us, the write-back BeginSession authenticates with
/// this token instead of our pairing code (which the peer must never learn).
pub type PullTokens = Arc<Mutex<HashSet<String>>>;

fn consume_pull_token(tokens: &PullTokens, code: Option<&str>) -> bool {
    match code {
        Some(code) => tokens.lock().unwrap().remove(code),
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
    done: bool,
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
) -> Result<()> {
    let local = listener.local_addr()?;
    let device = util::local_device_info();
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
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
        let (socket, peer) = listener.accept().await?;
        debug!("accepted connection from {peer}");
        let server_device = device.clone();
        let server_code = pairing_code.clone();
        let sessions = Arc::clone(&sessions);
        let ui_tx = ui_tx.clone();
        let pull_tokens = Arc::clone(&pull_tokens);
        let recv_progress = Arc::clone(&recv_progress);
        tokio::spawn(async move {
            if let Err(err) = handle_client(
                socket,
                server_device,
                server_code,
                require_auth,
                sessions,
                ui_tx,
                pull_tokens,
                recv_progress,
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
) -> Result<()> {
    stream.set_nodelay(true)?;
    tune_socket(&stream);

    let first = read_control(&mut stream).await?;
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
        let msg = match read_control(&mut stream).await {
            Ok(msg) => msg,
            Err(_) => return Ok(()),
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
            ControlMessage::ListDestinations => {
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
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code, require_auth)
                    && !consume_pull_token(&pull_tokens, auth_code.as_deref())
                {
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
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code, require_auth) {
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
                tokio::spawn(async move {
                    if let Err(err) = handle_push_request(
                        stream,
                        &requester_ip,
                        requester_port,
                        &files,
                        &dest_local_path,
                        return_auth_code.as_deref(),
                        overwrite,
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

fn browse_reply(
    destination_path: &str,
    relative_path: String,
    auth_code: Option<&str>,
    pairing_code: &str,
    require_auth: bool,
) -> ControlMessage {
    if let Err(err) = ensure_auth(auth_code, pairing_code, require_auth) {
        return ControlMessage::Error {
            message: err.to_string(),
        };
    }
    let result = (|| -> Result<Vec<crate::protocol::DirEntry>> {
        let root = storage::ensure_destination_root(destination_path)?;
        let target = if relative_path.is_empty() {
            root
        } else {
            root.join(storage::sanitize_relative_path(&relative_path)?)
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
    storage::ensure_destination_root(destination_path)?;

    if !dry_run {
        for dir in dirs {
            let rel = storage::sanitize_relative_path(&dir.rel_path)?;
            let path = PathBuf::from(destination_path).join(rel);
            fs::create_dir_all(&path).await?;
            let _ = util::set_mtime(&path, dir.mtime_secs).await;
        }
    }

    let mut actions = Vec::with_capacity(files.len());
    let mut states = HashMap::new();

    for spec in files {
        let (final_path, part_path) =
            storage::build_target_paths(destination_path, &spec.rel_path)?;

        let mut state = FileState {
            size: spec.size,
            mtime_secs: spec.mtime_secs,
            final_path: final_path.clone(),
            part_path: part_path.clone(),
            start_offset: 0,
            received: 0,
            hasher: None,
            stripe_cvs: HashMap::new(),
            done: false,
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
        state.part_path.clone()
    };
    let file = OpenOptions::new().write(true).open(&part_path).await?;
    file.set_len(0).await?;
    Ok(())
}

async fn run_data_conn(mut reader: BufReader<OwnedReadHalf>, session: Arc<Session>) -> Result<()> {
    loop {
        let msg = match read_control(&mut reader).await {
            Ok(msg) => msg,
            Err(_) => return Ok(()), // sender closed the data connection
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
        if offset + len > state.size {
            bail!("unit out of bounds for file {id}");
        }
        let striped = util::is_striped(state.size);
        if striped {
            let (stripe_start, stripe_len) =
                util::stripe_range(state.size, (offset / util::STRIPE_SIZE) as u32);
            if offset != stripe_start || len != stripe_len {
                bail!("unit is not stripe-aligned for file {id}");
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
            reader
                .read_exact(&mut buf[..to_read])
                .await
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
        state.received += len;
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
            Some((
                hash,
                state.part_path.clone(),
                state.final_path.clone(),
                state.mtime_secs,
            ))
        } else {
            None
        }
    };

    if let Some((hash, part_path, final_path, mtime_secs)) = finalize {
        session.progress.file_done();
        debug!("file {id} complete -> {}", final_path.display());
        if session.overwrite && fs::metadata(&final_path).await.is_ok() {
            let _ = fs::remove_file(&final_path).await;
        }
        fs::rename(&part_path, &final_path).await?;
        // fire-and-forget: mtime is cosmetic metadata, not worth serializing
        // 10k blocking waits into the completion path
        tokio::spawn(async move {
            let _ = util::set_mtime(&final_path, mtime_secs).await;
        });
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

fn ensure_auth(provided: Option<&str>, expected: &str, require_auth: bool) -> Result<()> {
    if !require_auth {
        return Ok(());
    }
    let value = provided.unwrap_or_default().trim();
    if value.is_empty() {
        bail!("pairing code is required for write operations");
    }
    if value != expected {
        bail!("invalid pairing code");
    }
    Ok(())
}

/// Handle an incoming PushRequest: verify requested files exist locally,
/// then act as sender — connect back to the requester's server and stream
/// the files using the existing v3 protocol. `return_auth_code` is the
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
            progress: None,
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
}
