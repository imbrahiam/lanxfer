use anyhow::{Result, anyhow, bail};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::discovery;
use crate::protocol::{
    ControlMessage, DirSpec, FileSpec, PROTOCOL_VERSION, PlanAction, RemoteFileSpec, read_control,
    read_control_timeout, send_control,
};
use crate::ui;
use crate::util;

const MAX_PARALLEL_JOBS: usize = 32;

#[derive(Debug, Clone)]
pub(crate) struct DirectoryEntry {
    pub relative_path: String,
    pub mtime_secs: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct FileEntry {
    pub abs_path: PathBuf,
    pub relative_path: String,
    pub size: u64,
    pub mtime_secs: i64,
}

pub(crate) struct ScanResult {
    pub directories: Vec<DirectoryEntry>,
    pub files: Vec<FileEntry>,
}

pub struct SendOptions {
    pub overwrite: bool,
    pub dry_run: bool,
    pub jobs: Option<usize>,
    /// CLI progress bars (indicatif). The TUI uses `progress` instead.
    pub show_progress: bool,
    /// Live counters for the interactive UI.
    pub progress: Option<Arc<crate::progress::Progress>>,
}

#[derive(Default)]
pub struct SendSummary {
    pub transferred: usize,
    pub skipped_up_to_date: usize,
    pub conflicts: usize,
    pub bytes: u64,
    pub errors: Vec<String>,
}

/// One transfer unit pulled by data workers: a whole plain file, or one
/// 64 MiB stripe of a large file.
#[derive(Clone)]
struct Unit {
    id: u32,
    offset: u64,
    len: u64,
    stripe: Option<u32>,
}

/// v5 session engine: one manifest round-trip, then persistent data
/// connections streaming units back-to-back. Large files are striped across
/// connections; BLAKE3 subtree CVs are merged for whole-file verification.
pub async fn send_session(
    target: &str,
    port: u16,
    sources: &[PathBuf],
    destination: &str,
    auth_code: Option<&str>,
    opts: SendOptions,
) -> Result<SendSummary> {
    // Scan all sources into one manifest.
    let mut files: Vec<FileEntry> = Vec::new();
    let mut dirs: Vec<DirectoryEntry> = Vec::new();
    for source in sources {
        let source = fs::canonicalize(source).await?;
        let scan = scan_source(&source).await?;
        files.extend(scan.files);
        dirs.extend(scan.directories);
    }
    if files.is_empty() && dirs.is_empty() {
        bail!("sources have no transferable entries");
    }

    let specs: Vec<FileSpec> = files
        .iter()
        .enumerate()
        .map(|(i, f)| FileSpec {
            id: i as u32,
            rel_path: f.relative_path.clone(),
            size: f.size,
            mtime_secs: f.mtime_secs,
        })
        .collect();
    let dir_specs: Vec<DirSpec> = dirs
        .iter()
        .map(|d| DirSpec {
            rel_path: d.relative_path.clone(),
            mtime_secs: d.mtime_secs,
        })
        .collect();

    // Control connection: manifest -> plan.
    let (mut control, _device, _, auth_challenge) = connect_and_handshake(target, port).await?;
    let auth_proof = auth_code.map(|code| util::auth_proof(code, &auth_challenge));
    send_control(
        &mut control,
        &ControlMessage::BeginSession {
            destination_path: destination.to_string(),
            auth_code: auth_proof,
            overwrite: opts.overwrite,
            dry_run: opts.dry_run,
            files: specs,
            dirs: dir_specs,
        },
    )
    .await?;
    let (session_id, plan) = match read_control(&mut control).await? {
        ControlMessage::SessionPlan {
            session_id,
            actions,
        } => (session_id, actions),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected plan response: {other:?}"),
    };
    let mut actions: HashMap<u32, PlanAction> =
        plan.into_iter().map(|a| (a.id, a.action)).collect();

    if opts.dry_run {
        return Ok(print_dry_run(&files, &actions));
    }

    // Resume verification: hash local prefixes; on mismatch tell the server
    // to restart that file from zero.
    let mut hashers: HashMap<u32, blake3::Hasher> = HashMap::new();
    for (id, file) in files.iter().enumerate() {
        let id = id as u32;
        let Some(PlanAction::Resume {
            offset,
            partial_hash,
        }) = actions.get(&id).cloned()
        else {
            continue;
        };
        let matches = if offset > file.size {
            false
        } else {
            match util::hash_prefix_hasher(&file.abs_path, offset).await {
                Ok(h) if h.clone().finalize().to_hex().to_string() == partial_hash => {
                    hashers.insert(id, h);
                    true
                }
                _ => false,
            }
        };
        if !matches {
            send_control(&mut control, &ControlMessage::RestartFile { id }).await?;
            loop {
                match read_control(&mut control).await? {
                    ControlMessage::RestartAck { id: acked } if acked == id => break,
                    ControlMessage::Error { message } => bail!("restart failed: {message}"),
                    _ => continue,
                }
            }
            actions.insert(id, PlanAction::Send);
        }
    }

    // Build the unit queue.
    let mut queue: VecDeque<Unit> = VecDeque::new();
    let mut expected_dones = 0usize;
    let mut total_bytes_to_send = 0u64;
    let mut summary = SendSummary::default();
    for (id, file) in files.iter().enumerate() {
        let id = id as u32;
        match actions.get(&id) {
            Some(PlanAction::SkipUpToDate) => summary.skipped_up_to_date += 1,
            Some(PlanAction::Conflict) => summary.conflicts += 1,
            Some(PlanAction::Send) => {
                expected_dones += 1;
                if util::is_striped(file.size) {
                    for i in 0..util::stripe_count(file.size) {
                        let (offset, len) = util::stripe_range(file.size, i);
                        total_bytes_to_send += len;
                        queue.push_back(Unit {
                            id,
                            offset,
                            len,
                            stripe: Some(i),
                        });
                    }
                } else {
                    total_bytes_to_send += file.size;
                    queue.push_back(Unit {
                        id,
                        offset: 0,
                        len: file.size,
                        stripe: None,
                    });
                }
            }
            Some(PlanAction::Resume { offset, .. }) => {
                expected_dones += 1;
                let len = file.size - offset;
                total_bytes_to_send += len;
                queue.push_back(Unit {
                    id,
                    offset: *offset,
                    len,
                    stripe: None,
                });
            }
            None => summary.errors.push(format!(
                "{}: server plan missing this file",
                file.relative_path
            )),
        }
    }

    let file_count = expected_dones;
    let worker_count = opts
        .jobs
        .unwrap_or_else(|| default_jobs(queue.len()))
        .clamp(1, MAX_PARALLEL_JOBS)
        .min(queue.len().max(1));
    log::info!(
        "send session to {target}: {file_count} files, {} bytes, {worker_count} connections",
        total_bytes_to_send
    );
    if let Some(p) = &opts.progress {
        p.reset_if_idle();
        p.add_totals(total_bytes_to_send, expected_dones as u64);
    }

    let multi = if opts.show_progress {
        Some(MultiProgress::new())
    } else {
        None
    };
    let overall = multi.as_ref().map(|m| {
        let bar = m.add(ProgressBar::new(total_bytes_to_send));
        bar.set_style(
            ProgressStyle::default_bar()
                .template(ui::overall_bar_template())
                .unwrap()
                .progress_chars(ui::progress_chars()),
        );
        bar.set_message(ui::dim(&format!("0/{file_count} files")));
        bar
    });

    // Shared state between workers and the control reader.
    let files = Arc::new(files);
    let queue = Arc::new(Mutex::new(queue));
    let hashers = Arc::new(Mutex::new(hashers));
    let client_hashes: Arc<Mutex<HashMap<u32, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let stripe_cvs: Arc<Mutex<HashMap<u32, HashMap<u32, util::StripeCv>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let sent_bytes = Arc::new(AtomicU64::new(0));
    let expected_ids: HashSet<u32> = actions
        .iter()
        .filter_map(|(id, action)| {
            matches!(action, PlanAction::Send | PlanAction::Resume { .. }).then_some(*id)
        })
        .collect();

    // Control reader collects FileDone results while workers stream.
    let done_overall = overall.clone();
    let done_progress = opts.progress.clone();
    let control_task = tokio::spawn(async move {
        let mut dones: HashMap<u32, (String, bool, Option<String>)> = HashMap::new();
        while dones.len() < expected_dones {
            match read_control(&mut control).await {
                Ok(ControlMessage::FileDone {
                    id,
                    hash,
                    ok,
                    error,
                }) => {
                    if !expected_ids.contains(&id) {
                        return (
                            control,
                            dones,
                            Some(format!("receiver reported unexpected file id {id}")),
                        );
                    }
                    dones.insert(id, (hash, ok, error));
                    if let Some(p) = &done_progress {
                        p.file_done();
                    }
                    if let Some(bar) = &done_overall {
                        bar.set_message(ui::dim(&format!("{}/{file_count} files", dones.len())));
                    }
                }
                Ok(ControlMessage::Error { message }) => {
                    return (control, dones, Some(message));
                }
                Ok(_) => continue,
                Err(err) => return (control, dones, Some(err.to_string())),
            }
        }
        (control, dones, None)
    });

    let mut workers = JoinSet::new();
    for _ in 0..worker_count {
        let target = target.to_string();
        let session_id = session_id.clone();
        let files = Arc::clone(&files);
        let queue = Arc::clone(&queue);
        let hashers = Arc::clone(&hashers);
        let client_hashes = Arc::clone(&client_hashes);
        let stripe_cvs = Arc::clone(&stripe_cvs);
        let sent_bytes = Arc::clone(&sent_bytes);
        let multi = multi.clone();
        let overall = overall.clone();
        let progress = opts.progress.clone();
        workers.spawn(async move {
            run_worker(
                &target,
                port,
                &session_id,
                files,
                queue,
                hashers,
                client_hashes,
                stripe_cvs,
                sent_bytes,
                multi,
                overall,
                progress,
            )
            .await
        });
    }

    let mut worker_errors = Vec::new();
    while let Some(res) = workers.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => worker_errors.push(err.to_string()),
            Err(err) => worker_errors.push(format!("worker panicked: {err}")),
        }
    }

    // Wait for the server's completion reports. If a worker died, some
    // FileDones will never come — bounded wait, then report.
    let mut control_task = control_task;
    let (mut control, dones, control_err) = if worker_errors.is_empty() {
        let (control, dones, err) = control_task
            .await
            .map_err(|e| anyhow!("control reader: {e}"))?;
        (Some(control), dones, err)
    } else {
        match tokio::time::timeout(std::time::Duration::from_secs(5), &mut control_task).await {
            Ok(joined) => {
                let (control, dones, err) = joined.map_err(|e| anyhow!("control reader: {e}"))?;
                (Some(control), dones, err)
            }
            Err(_) => {
                control_task.abort();
                (
                    None,
                    HashMap::new(),
                    Some("timed out waiting for results".into()),
                )
            }
        }
    };

    if let Some(bar) = &overall {
        bar.finish_with_message(ui::ok("done"));
    }

    summary.errors.extend(worker_errors);
    if let Some(err) = control_err {
        summary.errors.push(err);
    }

    // Reconcile: server hash must equal our locally computed hash.
    let client_hashes = client_hashes.lock().unwrap().clone();
    let stripe_cvs = stripe_cvs.lock().unwrap().clone();
    for (id, file) in files.iter().enumerate() {
        let id = id as u32;
        let needs_done = matches!(
            actions.get(&id),
            Some(PlanAction::Send) | Some(PlanAction::Resume { .. })
        );
        if !needs_done {
            continue;
        }
        let local_hash = if util::is_striped(file.size) {
            stripe_cvs.get(&id).and_then(|cvs| {
                let count = util::stripe_count(file.size);
                let ordered: Option<Vec<util::StripeCv>> =
                    (0..count).map(|i| cvs.get(&i).copied()).collect();
                ordered.map(|cvs| util::merge_stripes(&cvs, file.size).to_hex().to_string())
            })
        } else {
            client_hashes.get(&id).cloned()
        };
        match (dones.get(&id), local_hash) {
            (Some((server_hash, true, _)), Some(local)) if *server_hash == local => {
                let Some(control) = control.as_mut() else {
                    summary.errors.push(format!(
                        "{}: control connection unavailable",
                        file.relative_path
                    ));
                    continue;
                };
                send_control(
                    control,
                    &ControlMessage::CommitFile {
                        id,
                        expected_hash: local,
                    },
                )
                .await?;
                match read_control_timeout(control, std::time::Duration::from_secs(30)).await? {
                    ControlMessage::CommitAck {
                        id: acked,
                        ok: true,
                        ..
                    } if acked == id => summary.transferred += 1,
                    ControlMessage::CommitAck {
                        id: acked, error, ..
                    } if acked == id => summary.errors.push(format!(
                        "{}: commit failed: {}",
                        file.relative_path,
                        error.unwrap_or_else(|| "receiver rejected commit".into())
                    )),
                    ControlMessage::Error { message } => summary
                        .errors
                        .push(format!("{}: commit failed: {message}", file.relative_path)),
                    other => summary.errors.push(format!(
                        "{}: unexpected commit response: {other:?}",
                        file.relative_path
                    )),
                }
            }
            (Some((server_hash, true, _)), Some(local)) => summary.errors.push(format!(
                "{}: hash mismatch (local {local}, remote {server_hash})",
                file.relative_path
            )),
            (Some((_, false, error)), _) => summary.errors.push(format!(
                "{}: {}",
                file.relative_path,
                error.clone().unwrap_or_else(|| "receive failed".into())
            )),
            (None, _) => summary.errors.push(format!(
                "{}: no completion report from receiver",
                file.relative_path
            )),
            (_, None) => summary
                .errors
                .push(format!("{}: transfer incomplete", file.relative_path)),
        }
    }
    summary.bytes = sent_bytes.load(Ordering::Relaxed);

    Ok(summary)
}

/// Pull files from a remote peer. Sends a PushRequest telling the remote to
/// read its local files and push them to us. Waits for the transfer to
/// complete on the remote side.
#[allow(clippy::too_many_arguments)]
pub async fn pull_session(
    target: &str,
    port: u16,
    remote_files: &[RemoteFileSpec],
    dest_local_path: &str,
    requester_port: u16,
    auth_code: Option<&str>,
    return_auth_code: Option<&str>,
    overwrite: bool,
) -> Result<SendSummary> {
    if remote_files.is_empty() {
        bail!("no files selected for pull");
    }
    log::info!(
        "pull from {target}:{port}: {} files -> {dest_local_path} (reply port {requester_port})",
        remote_files.len()
    );

    let (mut control, _device, _, auth_challenge) = connect_and_handshake(target, port).await?;
    let auth_proof = auth_code.map(|code| util::auth_proof(code, &auth_challenge));
    send_control(
        &mut control,
        &ControlMessage::PushRequest {
            files: remote_files.to_vec(),
            dest_local_path: dest_local_path.to_string(),
            requester_port,
            auth_code: auth_proof,
            overwrite,
            return_auth_code: return_auth_code.map(ToString::to_string),
        },
    )
    .await?;

    match read_control_timeout(&mut control, std::time::Duration::from_secs(10)).await? {
        ControlMessage::JoinAck => {}
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected push response: {other:?}"),
    }

    // Wait for the remote to finish pushing files to us.
    let mut summary = SendSummary::default();
    loop {
        match read_control(&mut control).await {
            Ok(ControlMessage::PushComplete {
                files_sent,
                bytes,
                errors,
            }) => {
                summary.transferred = files_sent;
                summary.bytes = bytes;
                summary.errors = errors;
                break;
            }
            Ok(ControlMessage::Error { message }) => {
                summary.errors.push(message);
                break;
            }
            Ok(_) => continue,
            Err(err) => {
                summary.errors.push(format!("remote disconnected: {err}"));
                break;
            }
        }
    }
    log::info!(
        "pull finished: {} files, {} bytes, errors: {:?}",
        summary.transferred,
        summary.bytes,
        summary.errors
    );

    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
async fn run_worker(
    target: &str,
    port: u16,
    session_id: &str,
    files: Arc<Vec<FileEntry>>,
    queue: Arc<Mutex<VecDeque<Unit>>>,
    hashers: Arc<Mutex<HashMap<u32, blake3::Hasher>>>,
    client_hashes: Arc<Mutex<HashMap<u32, String>>>,
    stripe_cvs: Arc<Mutex<HashMap<u32, HashMap<u32, util::StripeCv>>>>,
    sent_bytes: Arc<AtomicU64>,
    multi: Option<MultiProgress>,
    overall: Option<ProgressBar>,
    progress: Option<Arc<crate::progress::Progress>>,
) -> Result<()> {
    // Don't open a connection if there's no work left.
    if queue.lock().unwrap().is_empty() {
        return Ok(());
    }

    let (mut stream, _, _, _) = connect_and_handshake(target, port).await?;
    send_control(
        &mut stream,
        &ControlMessage::JoinSession {
            session_id: session_id.to_string(),
        },
    )
    .await?;
    match read_control(&mut stream).await? {
        ControlMessage::JoinAck => {}
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected join response: {other:?}"),
    }
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, stream);
    let mut buf = vec![0u8; 4 * 1024 * 1024];

    loop {
        let unit = match queue.lock().unwrap().pop_front() {
            Some(u) => u,
            None => break,
        };
        let entry = &files[unit.id as usize];

        let bar = multi.as_ref().map(|m| {
            let bar = m.add(ProgressBar::new(unit.len));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template(ui::unit_bar_template())
                    .unwrap()
                    .progress_chars(ui::progress_chars()),
            );
            let label = match unit.stripe {
                Some(i) => format!("{} [{}]", entry.relative_path, i),
                None => entry.relative_path.clone(),
            };
            bar.set_prefix(label);
            bar
        });

        let unit_key = crate::progress::unit_key(unit.id, unit.stripe);
        if let Some(p) = &progress {
            let label = match unit.stripe {
                Some(i) => format!("{} [{}]", entry.relative_path, i + 1),
                None => entry.relative_path.clone(),
            };
            p.begin_unit(unit_key, label, unit.len);
        }

        send_control(
            &mut writer,
            &ControlMessage::SendFile {
                id: unit.id,
                offset: unit.offset,
                len: unit.len,
            },
        )
        .await?;

        // Hasher: stripes hash independently as BLAKE3 subtrees; plain files
        // use one streaming hasher (pre-seeded with the prefix on resume).
        let mut hasher = match unit.stripe {
            Some(i) => util::stripe_hasher(i),
            None => hashers.lock().unwrap().remove(&unit.id).unwrap_or_default(),
        };

        let mut file = fs::File::open(&entry.abs_path).await?;
        file.seek(std::io::SeekFrom::Start(unit.offset)).await?;
        let mut remaining = unit.len;
        while remaining > 0 {
            let cap = usize::min(remaining as usize, buf.len());
            let read = file.read(&mut buf[..cap]).await?;
            if read == 0 {
                bail!(
                    "{}: file became shorter during transfer",
                    entry.relative_path
                );
            }
            hasher.update(&buf[..read]);
            writer.write_all(&buf[..read]).await?;
            remaining -= read as u64;
            sent_bytes.fetch_add(read as u64, Ordering::Relaxed);
            if let Some(p) = &progress {
                p.advance(unit_key, read as u64);
            }
            if let Some(b) = &bar {
                b.inc(read as u64);
            }
            if let Some(b) = &overall {
                b.inc(read as u64);
            }
        }
        writer.flush().await?;
        if let Some(p) = &progress {
            p.end_unit(unit_key);
        }

        match unit.stripe {
            Some(i) => {
                stripe_cvs
                    .lock()
                    .unwrap()
                    .entry(unit.id)
                    .or_default()
                    .insert(i, util::finish_stripe(&hasher));
            }
            None => {
                client_hashes
                    .lock()
                    .unwrap()
                    .insert(unit.id, hasher.finalize().to_hex().to_string());
            }
        }
        if let Some(b) = bar {
            b.finish_and_clear();
        }
    }

    writer.flush().await?;
    Ok(())
}

fn print_dry_run(files: &[FileEntry], actions: &HashMap<u32, PlanAction>) -> SendSummary {
    let mut summary = SendSummary::default();
    let mut send_bytes = 0u64;
    ui::section("Plan (dry run)");
    for (id, file) in files.iter().enumerate() {
        match actions.get(&(id as u32)) {
            Some(PlanAction::Send) => {
                summary.transferred += 1;
                send_bytes += file.size;
            }
            Some(PlanAction::Resume { offset, .. }) => {
                summary.transferred += 1;
                send_bytes += file.size - offset;
                ui::info(&format!(
                    "resume {} from {}",
                    file.relative_path,
                    util::format_size(*offset)
                ));
            }
            Some(PlanAction::SkipUpToDate) => summary.skipped_up_to_date += 1,
            Some(PlanAction::Conflict) => {
                summary.conflicts += 1;
                ui::warn(&format!("conflict: {} exists", file.relative_path));
            }
            None => {}
        }
    }
    ui::kv(
        "would send",
        &format!(
            "{} files ({})",
            summary.transferred,
            util::format_size(send_bytes)
        ),
    );
    ui::kv("up to date", &summary.skipped_up_to_date.to_string());
    ui::kv("conflicts", &summary.conflicts.to_string());
    summary
}

pub async fn discover(discovery_port: u16, timeout_ms: u64) -> Result<()> {
    ui::section("Interfaces");
    for iface in &discovery::get_interface_summary() {
        ui::info(iface.trim_start());
    }

    ui::section("Receivers");
    let hosts = discovery::discover_hosts(discovery_port, timeout_ms).await?;
    if hosts.is_empty() {
        ui::warn("no receivers found");
        return Ok(());
    }

    println!(
        "  {}  {}  {}  {}",
        ui::dim(&format!("{:<20}", "HOST")),
        ui::dim(&format!("{:<22}", "ENDPOINT")),
        ui::dim(&format!("{:<14}", "PLATFORM")),
        ui::dim("AUTH"),
    );
    for host in hosts {
        let host_pad = format!("{:<20}", host.reply.host);
        let endpoint_pad = format!("{:<22}", format!("{}:{}", host.ip, host.reply.control_port));
        let platform_pad = format!(
            "{:<14}",
            format!("{} {}", host.reply.device.os, host.reply.device.arch)
        );
        let auth = if host.reply.auth_required {
            ui::yellow("required")
        } else {
            ui::dim("off")
        };
        println!(
            "  {}  {}  {}  {}",
            ui::bold(&host_pad),
            endpoint_pad,
            platform_pad,
            auth
        );
    }
    Ok(())
}

pub async fn connect_interactive(
    direct_target: Option<String>,
    auth_code: Option<String>,
    discovery_port: u16,
    timeout_ms: u64,
    port: u16,
) -> Result<()> {
    if let Some(target) = direct_target {
        connect_and_list_destinations(&target, port, auth_code.as_deref()).await?;
        return Ok(());
    }

    ui::section("Discover");
    ui::info("scanning all network interfaces…");
    let hosts = discovery::discover_hosts(discovery_port, timeout_ms).await?;
    if hosts.is_empty() {
        ui::warn("no receivers found");
        return Ok(());
    }

    if hosts.len() == 1 {
        let host = &hosts[0];
        ui::success(&format!(
            "found single receiver {}",
            ui::bold(&host.reply.host)
        ));
        connect_and_list_destinations(&host.ip, host.reply.control_port, auth_code.as_deref())
            .await?;
        return Ok(());
    }

    ui::section("Receivers");
    for (i, host) in hosts.iter().enumerate() {
        println!(
            "  {} {}  {}",
            ui::yellow(&format!("{:>2}.", i + 1)),
            ui::bold(&format!("{:<20}", host.reply.host)),
            ui::dim(&format!(
                "{}:{}  {} {}  auth:{}",
                host.ip,
                host.reply.control_port,
                host.reply.device.os,
                host.reply.device.arch,
                if host.reply.auth_required {
                    "required"
                } else {
                    "off"
                },
            )),
        );
    }

    let (stream, device, _, _) = loop {
        print!(
            "\n  {} select receiver (1-{}) or 0 to quit: ",
            console::style("▶").yellow().bold(),
            hosts.len()
        );
        std::io::Write::flush(&mut std::io::stdout())?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let choice: usize = match input.trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        if choice == 0 {
            ui::info("cancelled");
            return Ok(());
        }
        if choice >= 1 && choice <= hosts.len() {
            let host = &hosts[choice - 1];
            break connect_and_handshake(&host.ip, host.reply.control_port).await?;
        }
    };

    ui::success(&format!(
        "connected to {} ({} {}, protocol {})",
        ui::bold(&device.host_name),
        device.os,
        device.arch,
        device.protocol_version
    ));
    drop(stream);
    Ok(())
}

async fn connect_and_list_destinations(
    target: &str,
    port: u16,
    auth_code: Option<&str>,
) -> Result<()> {
    let (mut stream, device, auth_required, auth_challenge) =
        connect_and_handshake(target, port).await?;
    ui::success(&format!(
        "connected to {} ({} {}, protocol {})",
        ui::bold(&device.host_name),
        device.os,
        device.arch,
        device.protocol_version
    ));
    if auth_required && auth_code.is_none() {
        bail!("pairing code required; pass --code <CODE>");
    }
    send_control(
        &mut stream,
        &ControlMessage::ListDestinations {
            auth_code: auth_code.map(|code| util::auth_proof(code, &auth_challenge)),
        },
    )
    .await?;
    let reply = read_control(&mut stream).await?;

    match reply {
        ControlMessage::Destinations { items } => print_destination_table(&items),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected server response: {other:?}"),
    }
    Ok(())
}

pub async fn print_destinations(target: &str, port: u16, auth_code: Option<&str>) -> Result<()> {
    connect_and_list_destinations(target, port, auth_code).await
}

fn print_destination_table(items: &[crate::protocol::DestinationInfo]) {
    ui::section("Drives");
    println!(
        "  {}  {}  {}",
        ui::dim(&format!("{:<28}", "PATH")),
        ui::dim(&format!("{:>12}", "FREE")),
        ui::dim("MODE"),
    );
    for item in items {
        let mode = if item.read_only {
            ui::dim("read-only")
        } else {
            ui::ok("writable")
        };
        let path_pad = format!("{:<28}", item.path);
        let free_pad = format!("{:>12}", util::format_size(item.available_bytes));
        println!("  {}  {}  {}", ui::bold(&path_pad), free_pad, mode);
    }
}

/// Direct `lanxfer send` entry point — wraps the session engine.
#[allow(clippy::too_many_arguments)]
pub async fn send_path(
    target: &str,
    port: u16,
    source: &Path,
    destination: &Path,
    auth_code: Option<&str>,
    overwrite: bool,
    dry_run: bool,
    jobs: Option<usize>,
    show_progress: bool,
) -> Result<()> {
    let summary = send_session(
        target,
        port,
        &[source.to_path_buf()],
        &destination.to_string_lossy(),
        auth_code,
        SendOptions {
            overwrite,
            dry_run,
            jobs,
            show_progress,
            progress: None,
        },
    )
    .await?;

    if dry_run {
        return Ok(());
    }
    println!();
    ui::kv("transferred", &summary.transferred.to_string());
    if summary.skipped_up_to_date > 0 {
        ui::kv("up to date", &summary.skipped_up_to_date.to_string());
    }
    if summary.conflicts > 0 {
        ui::kv("conflicts", &summary.conflicts.to_string());
    }
    if !summary.errors.is_empty() {
        ui::kv("errors", &summary.errors.len().to_string());
        for err in &summary.errors {
            ui::error(err);
        }
        bail!("one or more file transfers failed");
    }
    Ok(())
}

async fn connect_with_timeout(addr: &str) -> Result<TcpStream> {
    match tokio::time::timeout(std::time::Duration::from_secs(5), TcpStream::connect(addr)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(err)) => Err(err.into()),
        Err(_) => bail!("connection to {addr} timed out after 5s"),
    }
}

/// Returns the stream, the server's device info, and whether the server
/// requires a pairing code for write operations.
pub(crate) async fn connect_and_handshake(
    target: &str,
    port: u16,
) -> Result<(TcpStream, crate::protocol::DeviceInfo, bool, String)> {
    let addr = format!("{target}:{port}");
    let mut stream = connect_with_timeout(&addr).await?;
    stream.set_nodelay(true)?;
    tune_socket(&stream);

    send_control(
        &mut stream,
        &ControlMessage::Hello {
            version: PROTOCOL_VERSION,
            client_name: util::host_name(),
            client_port: port,
        },
    )
    .await?;

    match read_control_timeout(&mut stream, std::time::Duration::from_secs(5)).await? {
        ControlMessage::HelloAck {
            version,
            server,
            auth_required,
            auth_challenge,
        } if version == PROTOCOL_VERSION => Ok((stream, server, auth_required, auth_challenge)),
        ControlMessage::HelloAck { version, .. } => {
            bail!("server protocol version mismatch: {version}")
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected handshake response: {other:?}"),
    }
}

pub(crate) async fn scan_source(source: &Path) -> Result<ScanResult> {
    let source = source.to_path_buf();
    tokio::task::spawn_blocking(move || scan_source_sync(&source)).await?
}

fn scan_source_sync(source: &Path) -> Result<ScanResult> {
    let meta = std::fs::metadata(source)?;
    let mut directories = Vec::new();
    let mut files = Vec::new();

    if meta.is_file() {
        let name = source
            .file_name()
            .ok_or_else(|| anyhow!("source has no file name"))?
            .to_string_lossy()
            .to_string();
        files.push(FileEntry {
            abs_path: source.to_path_buf(),
            relative_path: name,
            size: meta.len(),
            mtime_secs: meta.modified().map(util::system_time_secs).unwrap_or(0),
        });
        return Ok(ScanResult { directories, files });
    }

    if !meta.is_dir() {
        bail!("source is neither file nor directory: {}", source.display());
    }

    let base = source
        .parent()
        .ok_or_else(|| anyhow!("source directory parent not found"))?;

    for entry in WalkDir::new(source).follow_links(false) {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(base)?;
        let relative_path = path_to_slash_string(rel)?;
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            let mtime_secs = metadata.modified().map(util::system_time_secs).unwrap_or(0);
            directories.push(DirectoryEntry {
                relative_path,
                mtime_secs,
            });
            continue;
        }

        if metadata.is_file() {
            let size = metadata.len();
            let mtime_secs = metadata.modified().map(util::system_time_secs).unwrap_or(0);
            files.push(FileEntry {
                abs_path: path.to_path_buf(),
                relative_path,
                size,
                mtime_secs,
            });
        }
    }

    directories.sort_by_key(|d| d.relative_path.matches('/').count());
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));

    Ok(ScanResult { directories, files })
}

fn path_to_slash_string(path: &Path) -> Result<String> {
    let mut out = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(v) => out.push(v.to_string_lossy().to_string()),
            std::path::Component::CurDir => {}
            _ => bail!("unsupported path component in {}", path.display()),
        }
    }
    if out.is_empty() {
        bail!("empty relative path");
    }
    Ok(out.join("/"))
}

fn tune_socket(stream: &TcpStream) {
    let sock = socket2::SockRef::from(stream);
    let _ = sock.set_send_buffer_size(4 * 1024 * 1024);
    let _ = sock.set_recv_buffer_size(4 * 1024 * 1024);
}

fn default_jobs(unit_count: usize) -> usize {
    if unit_count <= 1 {
        return 1;
    }
    let cpu = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(4);
    usize::min(unit_count, usize::min(8, usize::max(2, cpu)))
}
