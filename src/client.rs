use anyhow::{Result, anyhow, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::discovery;
use crate::protocol::{
    ControlMessage, PROTOCOL_VERSION, PrepareStatus, read_control, send_control,
};
use crate::util;

#[derive(Debug, Clone)]
struct DirectoryEntry {
    relative_path: String,
    mtime_secs: i64,
}

#[derive(Debug, Clone)]
struct FileEntry {
    abs_path: PathBuf,
    relative_path: String,
    size: u64,
    mtime_secs: i64,
}

#[derive(Debug)]
enum FileTransferStatus {
    Transferred,
    AlreadyExists,
    Conflict,
}

pub async fn discover(discovery_port: u16, timeout_ms: u64) -> Result<()> {
    let ifaces = discovery::get_interface_summary();
    println!("scanning interfaces:");
    for iface in &ifaces {
        println!("  {iface}");
    }
    println!();

    let hosts = discovery::discover_hosts(discovery_port, timeout_ms).await?;
    if hosts.is_empty() {
        println!("no receivers found");
        return Ok(());
    }

    for host in hosts {
        println!(
            "{} {}:{} | {} {} | auth:{} | proto:{}",
            host.reply.host,
            host.ip,
            host.reply.control_port,
            host.reply.device.os,
            host.reply.device.arch,
            if host.reply.auth_required {
                "required"
            } else {
                "off"
            },
            host.reply.device.protocol_version
        );
    }
    Ok(())
}

pub async fn connect_interactive(
    direct_target: Option<String>,
    discovery_port: u16,
    timeout_ms: u64,
    port: u16,
) -> Result<()> {
    if let Some(target) = direct_target {
        connect_and_list_destinations(&target, port).await?;
        return Ok(());
    }

    println!("scanning all network interfaces for receivers...");
    let hosts = discovery::discover_hosts(discovery_port, timeout_ms).await?;
    if hosts.is_empty() {
        println!("no receivers found");
        return Ok(());
    }

    if hosts.len() == 1 {
        let host = &hosts[0];
        println!("found single receiver, connecting...");
        connect_and_list_destinations(&host.ip, host.reply.control_port).await?;
        return Ok(());
    }

    println!("\nfound {} receivers:\n", hosts.len());
    for (i, host) in hosts.iter().enumerate() {
        println!(
            "  {:>2}. {} {}:{} | {} {} | auth:{}",
            i + 1,
            host.reply.host,
            host.ip,
            host.reply.control_port,
            host.reply.device.os,
            host.reply.device.arch,
            if host.reply.auth_required {
                "required"
            } else {
                "off"
            },
        );
    }

    let (stream, device) = loop {
        print!("\nselect receiver (1-{}) or 0 to quit: ", hosts.len());
        std::io::Write::flush(&mut std::io::stdout())?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let choice: usize = match input.trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        if choice == 0 {
            println!("cancelled");
            return Ok(());
        }
        if choice >= 1 && choice <= hosts.len() {
            let host = &hosts[choice - 1];
            break connect_and_handshake(&host.ip, host.reply.control_port).await?;
        }
    };

    println!(
        "\nconnected to {} | {} {} | protocol {}",
        device.host_name, device.os, device.arch, device.protocol_version
    );
    drop(stream);
    Ok(())
}

async fn connect_and_list_destinations(target: &str, port: u16) -> Result<()> {
    let (mut stream, device) = connect_and_handshake(target, port).await?;
    println!(
        "connected to {} | {} {} | protocol {}",
        device.host_name, device.os, device.arch, device.protocol_version
    );
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;
    let reply = read_control(&mut stream).await?;

    match reply {
        ControlMessage::Destinations { items } => {
            for item in items {
                let writable = if item.read_only { "ro" } else { "rw" };
                println!(
                    "{} | free {} bytes | {}",
                    item.path, item.available_bytes, writable
                );
            }
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected server response: {other:?}"),
    }
    Ok(())
}

pub async fn print_destinations(target: &str, port: u16) -> Result<()> {
    let (mut stream, device) = connect_and_handshake(target, port).await?;
    println!(
        "connected to {} | {} {} | protocol {}",
        device.host_name, device.os, device.arch, device.protocol_version
    );
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;
    let reply = read_control(&mut stream).await?;

    match reply {
        ControlMessage::Destinations { items } => {
            for item in items {
                let writable = if item.read_only { "ro" } else { "rw" };
                println!(
                    "{} | free {} bytes | {}",
                    item.path, item.available_bytes, writable
                );
            }
            Ok(())
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected server response: {other:?}"),
    }
}

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
    let source = fs::canonicalize(source).await?;
    let scan = scan_source(&source).await?;
    if scan.files.is_empty() && scan.directories.is_empty() {
        bail!("source has no transferable entries");
    }

    let worker_count = jobs.unwrap_or_else(|| default_jobs(scan.files.len()));
    println!(
        "prepared {} files, {} directories, {} bytes, workers {}{}",
        scan.files.len(),
        scan.directories.len(),
        scan.total_bytes,
        worker_count,
        if dry_run { " (dry-run)" } else { "" }
    );

    let (stream, device) = connect_and_handshake(target, port).await?;
    drop(stream);
    println!(
        "target device: {} | {} {} | protocol {}",
        device.host_name, device.os, device.arch, device.protocol_version
    );

    create_directories(
        target,
        port,
        destination,
        auth_code,
        dry_run,
        &scan.directories,
    )
    .await?;

    let total_bytes = scan.total_bytes;
    let transferred_bytes = Arc::new(AtomicU64::new(0));
    let done_files = Arc::new(AtomicUsize::new(0));
    let active_files = Arc::new(AtomicUsize::new(0));
    let stop_reporter = Arc::new(AtomicBool::new(false));

    let reporter = if show_progress && !dry_run {
        let transferred = Arc::clone(&transferred_bytes);
        let done = Arc::clone(&done_files);
        let active = Arc::clone(&active_files);
        let stop = Arc::clone(&stop_reporter);
        let file_count = scan.files.len();
        Some(tokio::spawn(async move {
            let mut last = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = transferred.load(Ordering::Relaxed);
                let delta = now.saturating_sub(last);
                last = now;
                let speed_mbps = (delta as f64 * 8.0) / 1_000_000.0;
                let pct = if total_bytes == 0 {
                    100.0
                } else {
                    (now as f64 / total_bytes as f64) * 100.0
                };
                println!(
                    "progress {:.1}% | {}/{} bytes | {:.2} Mbps | files done {}/{} | active {}",
                    pct,
                    now,
                    total_bytes,
                    speed_mbps,
                    done.load(Ordering::Relaxed),
                    file_count,
                    active.load(Ordering::Relaxed)
                );
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        }))
    } else {
        None
    };

    let semaphore = Arc::new(Semaphore::new(worker_count.max(1)));
    let mut set = JoinSet::new();
    for entry in scan.files {
        let permit = semaphore.clone().acquire_owned().await?;
        let target = target.to_string();
        let destination = destination.to_string_lossy().to_string();
        let auth_code = auth_code.map(ToString::to_string);
        let transferred = Arc::clone(&transferred_bytes);
        let done = Arc::clone(&done_files);
        let active = Arc::clone(&active_files);

        set.spawn(async move {
            let _permit = permit;
            active.fetch_add(1, Ordering::Relaxed);
            let result = transfer_one_file(
                &target,
                port,
                &destination,
                auth_code.as_deref(),
                overwrite,
                dry_run,
                &entry,
                &transferred,
            )
            .await;
            active.fetch_sub(1, Ordering::Relaxed);
            done.fetch_add(1, Ordering::Relaxed);
            result
        });
    }

    let mut transferred_files = 0usize;
    let mut skipped_existing = 0usize;
    let mut conflicts = 0usize;
    let mut errors = Vec::new();
    while let Some(task) = set.join_next().await {
        match task {
            Ok(Ok(FileTransferStatus::Transferred)) => transferred_files += 1,
            Ok(Ok(FileTransferStatus::AlreadyExists)) => skipped_existing += 1,
            Ok(Ok(FileTransferStatus::Conflict)) => conflicts += 1,
            Ok(Err(err)) => errors.push(err.to_string()),
            Err(err) => errors.push(format!("worker join error: {err}")),
        }
    }

    stop_reporter.store(true, Ordering::Relaxed);
    if let Some(task) = reporter {
        let _ = task.await;
    }

    println!(
        "summary: transferred={}, skipped_existing={}, conflicts={}, errors={}",
        transferred_files,
        skipped_existing,
        conflicts,
        errors.len()
    );

    if !errors.is_empty() {
        for err in errors {
            eprintln!("error: {err}");
        }
        bail!("one or more file transfers failed");
    }
    Ok(())
}

async fn create_directories(
    target: &str,
    port: u16,
    destination: &Path,
    auth_code: Option<&str>,
    dry_run: bool,
    directories: &[DirectoryEntry],
) -> Result<()> {
    if directories.is_empty() {
        return Ok(());
    }

    let mut stream = connect_and_handshake(target, port).await?.0;
    for dir in directories {
        send_control(
            &mut stream,
            &ControlMessage::CreateDirectory {
                destination_path: destination.to_string_lossy().to_string(),
                relative_path: dir.relative_path.clone(),
                mtime_secs: dir.mtime_secs,
                auth_code: auth_code.map(ToString::to_string),
                dry_run,
            },
        )
        .await?;
        match read_control(&mut stream).await? {
            ControlMessage::DirectoryCreated { .. } => {}
            ControlMessage::Error { message } => bail!("{message}"),
            other => bail!("unexpected create directory response: {other:?}"),
        }
    }
    Ok(())
}

async fn transfer_one_file(
    target: &str,
    port: u16,
    destination_path: &str,
    auth_code: Option<&str>,
    overwrite: bool,
    dry_run: bool,
    file: &FileEntry,
    transferred_total: &Arc<AtomicU64>,
) -> Result<FileTransferStatus> {
    let source_hash = util::hash_file(&file.abs_path).await?;
    let mut stream = connect_and_handshake(target, port).await?.0;

    send_control(
        &mut stream,
        &ControlMessage::PrepareUpload {
            destination_path: destination_path.to_string(),
            relative_path: file.relative_path.clone(),
            file_size: file.size,
            file_hash: source_hash.clone(),
            mtime_secs: file.mtime_secs,
            overwrite,
            auth_code: auth_code.map(ToString::to_string),
            dry_run,
        },
    )
    .await?;

    let mut ready = match read_control(&mut stream).await? {
        ControlMessage::UploadReady {
            status,
            offset,
            partial_hash,
            message: _,
        } => UploadReady {
            status,
            offset,
            partial_hash,
        },
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected upload response: {other:?}"),
    };

    match ready.status {
        PrepareStatus::AlreadyExists => return Ok(FileTransferStatus::AlreadyExists),
        PrepareStatus::Conflict => return Ok(FileTransferStatus::Conflict),
        PrepareStatus::Ready => {}
    }

    if dry_run {
        return Ok(FileTransferStatus::Transferred);
    }

    if ready.offset > 0 {
        let local_prefix = util::hash_file_prefix_exact(&file.abs_path, ready.offset).await?;
        if ready.partial_hash.as_deref() != Some(local_prefix.as_str()) {
            send_control(
                &mut stream,
                &ControlMessage::RestartUpload {
                    destination_path: destination_path.to_string(),
                    relative_path: file.relative_path.clone(),
                    auth_code: auth_code.map(ToString::to_string),
                },
            )
            .await?;
            ready = match read_control(&mut stream).await? {
                ControlMessage::UploadReady {
                    status,
                    offset,
                    partial_hash,
                    message: _,
                } => UploadReady {
                    status,
                    offset,
                    partial_hash,
                },
                ControlMessage::Error { message } => bail!("{message}"),
                other => bail!("unexpected restart response: {other:?}"),
            };
            if !matches!(ready.status, PrepareStatus::Ready) {
                bail!("restart did not return ready state");
            }
        }
    }

    send_control(
        &mut stream,
        &ControlMessage::BeginUpload {
            destination_path: destination_path.to_string(),
            relative_path: file.relative_path.clone(),
            offset: ready.offset,
            file_size: file.size,
            file_hash: source_hash,
            mtime_secs: file.mtime_secs,
            overwrite,
            auth_code: auth_code.map(ToString::to_string),
            dry_run: false,
        },
    )
    .await?;

    match read_control(&mut stream).await? {
        ControlMessage::BeginAck { offset } if offset == ready.offset => {}
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected begin response: {other:?}"),
    }

    transmit_payload(
        &mut stream,
        &file.abs_path,
        file.size,
        ready.offset,
        transferred_total,
    )
    .await?;

    match read_control(&mut stream).await? {
        ControlMessage::TransferResult {
            verified,
            error,
            bytes_received: _,
            ..
        } => {
            if verified {
                Ok(FileTransferStatus::Transferred)
            } else {
                bail!(error.unwrap_or_else(|| "hash verification failed".to_string()))
            }
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected transfer result: {other:?}"),
    }
}

struct UploadReady {
    status: PrepareStatus,
    offset: u64,
    partial_hash: Option<String>,
}

async fn connect_and_handshake(
    target: &str,
    port: u16,
) -> Result<(TcpStream, crate::protocol::DeviceInfo)> {
    let addr = format!("{target}:{port}");
    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;

    send_control(
        &mut stream,
        &ControlMessage::Hello {
            version: PROTOCOL_VERSION,
            client_name: util::host_name(),
        },
    )
    .await?;

    match read_control(&mut stream).await? {
        ControlMessage::HelloAck {
            version,
            server,
            auth_required: _,
        } if version == PROTOCOL_VERSION => Ok((stream, server)),
        ControlMessage::HelloAck { version, .. } => {
            bail!("server protocol version mismatch: {version}")
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected handshake response: {other:?}"),
    }
}

async fn transmit_payload(
    stream: &mut TcpStream,
    source: &Path,
    file_size: u64,
    offset: u64,
    transferred_total: &Arc<AtomicU64>,
) -> Result<()> {
    let mut file = fs::File::open(source).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let to_send = file_size.saturating_sub(offset);
    let mut sent = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    while sent < to_send {
        let remaining = (to_send - sent) as usize;
        let cap = usize::min(remaining, buf.len());
        let read = file.read(&mut buf[..cap]).await?;
        if read == 0 {
            bail!("source file became shorter during transfer");
        }
        stream.write_all(&buf[..read]).await?;
        sent += read as u64;
        transferred_total.fetch_add(read as u64, Ordering::Relaxed);
    }
    stream.flush().await?;
    Ok(())
}

struct ScanResult {
    directories: Vec<DirectoryEntry>,
    files: Vec<FileEntry>,
    total_bytes: u64,
}

async fn scan_source(source: &Path) -> Result<ScanResult> {
    let meta = fs::metadata(source).await?;
    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut total_bytes = 0u64;

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
        total_bytes = meta.len();
        return Ok(ScanResult {
            directories,
            files,
            total_bytes,
        });
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
            total_bytes += size;
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

    Ok(ScanResult {
        directories,
        files,
        total_bytes,
    })
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

fn default_jobs(file_count: usize) -> usize {
    if file_count <= 1 {
        return 1;
    }
    let cpu = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(4);
    usize::min(file_count, usize::min(8, usize::max(2, cpu)))
}
