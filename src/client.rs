use anyhow::{Result, anyhow, bail};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use walkdir::WalkDir;

use crate::discovery;
use crate::protocol::{
    ControlMessage, PROTOCOL_VERSION, PrepareStatus, read_control, send_control,
};
use crate::ui;
use crate::util;

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

#[derive(Debug)]
enum FileTransferStatus {
    Transferred,
    AlreadyExists,
    Conflict,
}

pub(crate) struct ScanResult {
    pub directories: Vec<DirectoryEntry>,
    pub files: Vec<FileEntry>,
    pub total_bytes: u64,
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
        println!("  {}  {}  {}  {}", ui::bold(&host_pad), endpoint_pad, platform_pad, auth);
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

    ui::section("Discover");
    ui::info("scanning all network interfaces…");
    let hosts = discovery::discover_hosts(discovery_port, timeout_ms).await?;
    if hosts.is_empty() {
        ui::warn("no receivers found");
        return Ok(());
    }

    if hosts.len() == 1 {
        let host = &hosts[0];
        ui::success(&format!("found single receiver {}", ui::bold(&host.reply.host)));
        connect_and_list_destinations(&host.ip, host.reply.control_port).await?;
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
                if host.reply.auth_required { "required" } else { "off" },
            )),
        );
    }

    let (stream, device) = loop {
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

async fn connect_and_list_destinations(target: &str, port: u16) -> Result<()> {
    let (mut stream, device) = connect_and_handshake(target, port).await?;
    ui::success(&format!(
        "connected to {} ({} {}, protocol {})",
        ui::bold(&device.host_name),
        device.os,
        device.arch,
        device.protocol_version
    ));
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;
    let reply = read_control(&mut stream).await?;

    match reply {
        ControlMessage::Destinations { items } => print_destination_table(&items),
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected server response: {other:?}"),
    }
    Ok(())
}

pub async fn print_destinations(target: &str, port: u16) -> Result<()> {
    let (mut stream, device) = connect_and_handshake(target, port).await?;
    ui::success(&format!(
        "connected to {} ({} {}, protocol {})",
        ui::bold(&device.host_name),
        device.os,
        device.arch,
        device.protocol_version
    ));
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;
    let reply = read_control(&mut stream).await?;

    match reply {
        ControlMessage::Destinations { items } => {
            print_destination_table(&items);
            Ok(())
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected server response: {other:?}"),
    }
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
    ui::section("Plan");
    ui::kv("files", &scan.files.len().to_string());
    ui::kv("directories", &scan.directories.len().to_string());
    ui::kv("size", &util::format_size(scan.total_bytes));
    ui::kv("workers", &worker_count.to_string());
    if dry_run {
        ui::warn("dry-run mode — no files will be written");
    }

    let (stream, device) = connect_and_handshake(target, port).await?;
    drop(stream);
    ui::success(&format!(
        "target {} ({} {}, protocol {})",
        ui::bold(&device.host_name),
        device.os,
        device.arch,
        device.protocol_version
    ));

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

    let multi = if show_progress && !dry_run {
        Some(MultiProgress::new())
    } else {
        None
    };
    let overall_bar = multi.as_ref().map(|m| {
        let bar = m.add(ProgressBar::new(total_bytes));
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {spinner:.cyan} [{bar:36.cyan/blue}] {bytes:>10}/{total_bytes:<10} {binary_bytes_per_sec:>11} eta {eta:<5} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏ "),
        );
        bar.set_message(ui::dim(&format!("0/{} files", scan.files.len())));
        bar
    });

    let semaphore = Arc::new(Semaphore::new(worker_count.max(1)));
    let mut set = JoinSet::new();
    let file_count = scan.files.len();
    for entry in scan.files {
        let permit = semaphore.clone().acquire_owned().await?;
        let target = target.to_string();
        let destination = destination.to_string_lossy().to_string();
        let auth_code = auth_code.map(ToString::to_string);
        let transferred = Arc::clone(&transferred_bytes);
        let done = Arc::clone(&done_files);
        let file_bar = multi.as_ref().map(|m| {
            let bar = m.add(ProgressBar::new(entry.size));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("    {prefix:.dim} [{bar:28.green/black}] {bytes}/{total_bytes}")
                    .unwrap()
                    .progress_chars("█▉▊▋▌▍▎▏ "),
            );
            bar.set_prefix(entry.relative_path.clone());
            bar
        });
        let overall = overall_bar.clone();
        let fc = file_count;

        set.spawn(async move {
            let _permit = permit;
            let result = transfer_one_file(
                &target,
                port,
                &destination,
                auth_code.as_deref(),
                overwrite,
                dry_run,
                &entry,
                &transferred,
                file_bar.as_ref(),
                overall.as_ref(),
            )
            .await;
            done.fetch_add(1, Ordering::Relaxed);
            if let Some(fb) = &file_bar {
                fb.finish_and_clear();
            }
            if let Some(ob) = &overall {
                ob.set_message(ui::dim(&format!(
                    "{}/{} files",
                    done.load(Ordering::Relaxed),
                    fc
                )));
            }
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

    if let Some(ob) = &overall_bar {
        ob.finish_with_message(ui::ok("done"));
    }

    println!();
    ui::kv("transferred", &transferred_files.to_string());
    if skipped_existing > 0 {
        ui::kv("skipped", &skipped_existing.to_string());
    }
    if conflicts > 0 {
        ui::kv("conflicts", &conflicts.to_string());
    }
    if !errors.is_empty() {
        ui::kv("errors", &errors.len().to_string());
        for err in errors {
            ui::error(&err);
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
    file_bar: Option<&ProgressBar>,
    overall_bar: Option<&ProgressBar>,
) -> Result<FileTransferStatus> {
    // Stream-hash mode: no pre-hash, hash while sending (single disk read)
    let mut stream = connect_and_handshake(target, port).await?.0;

    send_control(
        &mut stream,
        &ControlMessage::PrepareUpload {
            destination_path: destination_path.to_string(),
            relative_path: file.relative_path.clone(),
            file_size: file.size,
            file_hash: String::new(), // empty = stream-hash mode
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

    // For resume: verify prefix hash if server has partial data
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
            file_hash: String::new(), // computed after sending
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

    // Hash while sending — single disk read
    let client_hash = transmit_payload(
        &mut stream,
        &file.abs_path,
        file.size,
        ready.offset,
        transferred_total,
        file_bar,
        overall_bar,
    )
    .await?;

    match read_control(&mut stream).await? {
        ControlMessage::TransferResult {
            verified,
            final_hash,
            error,
            ..
        } => {
            // Server computed its own hash — verify against ours
            if verified && final_hash == client_hash {
                Ok(FileTransferStatus::Transferred)
            } else if !final_hash.is_empty() && final_hash == client_hash {
                // Server didn't have pre-hash to compare but hashes match
                Ok(FileTransferStatus::Transferred)
            } else {
                bail!(
                    "{}",
                    error.unwrap_or_else(|| format!(
                        "hash mismatch: client={} server={}",
                        client_hash, final_hash
                    ))
                )
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

pub(crate) async fn connect_and_handshake(
    target: &str,
    port: u16,
) -> Result<(TcpStream, crate::protocol::DeviceInfo)> {
    let addr = format!("{target}:{port}");
    let mut stream = TcpStream::connect(&addr).await?;
    stream.set_nodelay(true)?;
    tune_socket(&stream);

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

/// Transmit file data while computing BLAKE3 hash. Returns the full-file hash.
/// Hashes the entire file (including any prefix before offset) for complete verification.
async fn transmit_payload(
    stream: &mut TcpStream,
    source: &Path,
    file_size: u64,
    offset: u64,
    transferred_total: &Arc<AtomicU64>,
    file_bar: Option<&ProgressBar>,
    overall_bar: Option<&ProgressBar>,
) -> Result<String> {
    let source = source.to_path_buf();
    let file = fs::File::open(&source).await?;
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, stream);
    let mut hasher = blake3::Hasher::new();

    if let Some(fb) = file_bar {
        fb.set_position(offset);
    }

    // Read-and-hash prefix once to avoid a second full-file disk pass on resume.
    let mut hashed_prefix = 0u64;
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    while hashed_prefix < offset {
        let remaining = (offset - hashed_prefix) as usize;
        let cap = usize::min(remaining, buf.len());
        let read = reader.read(&mut buf[..cap]).await?;
        if read == 0 {
            bail!("source file became shorter before resume offset");
        }
        hasher.update(&buf[..read]);
        hashed_prefix += read as u64;
    }

    let to_send = file_size.saturating_sub(offset);
    let mut sent = 0u64;
    while sent < to_send {
        let remaining = (to_send - sent) as usize;
        let cap = usize::min(remaining, buf.len());
        let read = reader.read(&mut buf[..cap]).await?;
        if read == 0 {
            bail!("source file became shorter during transfer");
        }
        hasher.update(&buf[..read]);
        writer.write_all(&buf[..read]).await?;
        sent += read as u64;
        transferred_total.fetch_add(read as u64, Ordering::Relaxed);
        if let Some(fb) = file_bar {
            fb.inc(read as u64);
        }
        if let Some(ob) = overall_bar {
            ob.inc(read as u64);
        }
    }
    writer.flush().await?;

    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) async fn scan_source(source: &Path) -> Result<ScanResult> {
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

fn tune_socket(stream: &TcpStream) {
    let sock = socket2::SockRef::from(stream);
    let _ = sock.set_send_buffer_size(4 * 1024 * 1024);
    let _ = sock.set_recv_buffer_size(4 * 1024 * 1024);
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
