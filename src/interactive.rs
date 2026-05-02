use anyhow::{Result, bail};
use dialoguer::{Confirm, Input, MultiSelect, Select, theme::ColorfulTheme};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::client;
use crate::discovery;
use crate::protocol::{
    ControlMessage, DestinationInfo, PROTOCOL_VERSION, read_control, send_control,
};
use crate::util;

pub async fn run_interactive(discovery_port: u16, timeout_ms: u64, port: u16) -> Result<()> {
    let theme = ColorfulTheme::default();

    // Phase 1: Discovery + host selection
    let (ip, control_port) = select_host(&theme, discovery_port, timeout_ms, port).await?;

    // Phase 2: Connect + auth
    let (device, auth_code) = connect_with_auth(&theme, &ip, control_port).await?;
    println!(
        "\nconnected to {} | {} {} | protocol {}\n",
        device.host_name, device.os, device.arch, device.protocol_version
    );

    // Phase 3: Main menu loop
    main_menu_loop(&theme, &ip, control_port, auth_code.as_deref()).await
}

async fn select_host(
    theme: &ColorfulTheme,
    discovery_port: u16,
    timeout_ms: u64,
    default_port: u16,
) -> Result<(String, u16)> {
    println!("scanning for receivers...");
    let hosts = discovery::discover_hosts_with_fallback(discovery_port, timeout_ms).await?;

    if hosts.is_empty() {
        println!("no receivers found on network");
        let ip: String = Input::with_theme(theme)
            .with_prompt("Enter receiver IP")
            .interact_text()?;
        let ip = ip.trim().to_string();
        return Ok((ip, default_port));
    }

    if hosts.len() == 1 {
        let host = &hosts[0];
        println!(
            "found: {} ({}:{}) | {} {}",
            host.reply.host,
            host.ip,
            host.reply.control_port,
            host.reply.device.os,
            host.reply.device.arch,
        );
        return Ok((host.ip.clone(), host.reply.control_port));
    }

    let items: Vec<String> = hosts
        .iter()
        .map(|h| {
            format!(
                "{} ({}:{}) | {} {} | auth:{}",
                h.reply.host,
                h.ip,
                h.reply.control_port,
                h.reply.device.os,
                h.reply.device.arch,
                if h.reply.auth_required { "yes" } else { "no" },
            )
        })
        .collect();

    let mut items_with_manual = items.clone();
    items_with_manual.push("Enter IP manually".to_string());

    let selection = Select::with_theme(theme)
        .with_prompt("Select receiver")
        .items(&items_with_manual)
        .default(0)
        .interact()?;

    if selection == hosts.len() {
        let ip: String = Input::with_theme(theme)
            .with_prompt("Enter receiver IP")
            .interact_text()?;
        return Ok((ip.trim().to_string(), default_port));
    }

    let host = &hosts[selection];
    Ok((host.ip.clone(), host.reply.control_port))
}

async fn connect_with_auth(
    theme: &ColorfulTheme,
    ip: &str,
    port: u16,
) -> Result<(crate::protocol::DeviceInfo, Option<String>)> {
    let (stream, device, auth_required) = {
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
            } if version == PROTOCOL_VERSION => (s, server, auth_required),
            ControlMessage::HelloAck { version, .. } => {
                bail!("protocol version mismatch: {version}")
            }
            ControlMessage::Error { message } => bail!("{message}"),
            other => bail!("unexpected handshake response: {other:?}"),
        }
    };
    drop(stream);

    let auth_code = if auth_required {
        let code: String = Input::with_theme(theme)
            .with_prompt("Enter pairing code")
            .interact_text()?;
        Some(code.trim().to_uppercase())
    } else {
        None
    };

    Ok((device, auth_code))
}

async fn main_menu_loop(
    theme: &ColorfulTheme,
    ip: &str,
    port: u16,
    auth_code: Option<&str>,
) -> Result<()> {
    loop {
        let choices = vec!["Send files", "List remote drives", "Disconnect & exit"];
        let selection = Select::with_theme(theme)
            .with_prompt("Main menu")
            .items(&choices)
            .default(0)
            .interact()?;

        match selection {
            0 => {
                if let Err(err) = send_files_flow(theme, ip, port, auth_code).await {
                    eprintln!("error: {err:#}");
                }
            }
            1 => {
                if let Err(err) = list_drives(ip, port).await {
                    eprintln!("error: {err:#}");
                }
            }
            2 => {
                println!("disconnected");
                return Ok(());
            }
            _ => unreachable!(),
        }
        println!();
    }
}

async fn list_drives(ip: &str, port: u16) -> Result<()> {
    let (mut stream, _) = client::connect_and_handshake(ip, port).await?;
    send_control(&mut stream, &ControlMessage::ListDestinations).await?;

    match read_control(&mut stream).await? {
        ControlMessage::Destinations { items } => {
            println!();
            for item in &items {
                let writable = if item.read_only { "ro" } else { "rw" };
                println!(
                    "  {} | {} free | {}",
                    item.path,
                    util::format_size(item.available_bytes),
                    writable
                );
            }
            Ok(())
        }
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected response: {other:?}"),
    }
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

async fn send_files_flow(
    theme: &ColorfulTheme,
    ip: &str,
    port: u16,
    auth_code: Option<&str>,
) -> Result<()> {
    // Step 1: Select destination drive
    let destinations = fetch_destinations(ip, port).await?;
    let writable: Vec<&DestinationInfo> = destinations.iter().filter(|d| !d.read_only).collect();

    if writable.is_empty() {
        println!("no writable drives found on receiver");
        return Ok(());
    }

    let drive_items: Vec<String> = writable
        .iter()
        .map(|d| format!("{} | {} free", d.path, util::format_size(d.available_bytes)))
        .collect();

    let drive_idx = Select::with_theme(theme)
        .with_prompt("Select destination drive")
        .items(&drive_items)
        .default(0)
        .interact()?;

    let dest_root = &writable[drive_idx].path;

    // Step 2: Browse remote directory
    let remote_path = browse_remote_dir(theme, ip, port, dest_root, auth_code).await?;

    // Step 3: Pick local files/folders
    let local_paths = pick_local_paths(theme)?;
    if local_paths.is_empty() {
        println!("no files selected");
        return Ok(());
    }

    // Step 4: Confirm
    println!("\nTransfer summary:");
    for path in &local_paths {
        println!("  {} -> {}", path.display(), remote_path);
    }
    let confirmed = Confirm::with_theme(theme)
        .with_prompt("Start transfer?")
        .default(true)
        .interact()?;

    if !confirmed {
        println!("cancelled");
        return Ok(());
    }

    // Step 5: Transfer each selected path
    for source in &local_paths {
        println!("\ntransferring: {}", source.display());
        transfer_with_progress(ip, port, source, &remote_path, auth_code).await?;
    }

    println!("\nall transfers complete");
    Ok(())
}

async fn browse_remote_dir(
    theme: &ColorfulTheme,
    ip: &str,
    port: u16,
    dest_root: &str,
    auth_code: Option<&str>,
) -> Result<String> {
    let mut current_relative = String::new();

    loop {
        let (mut stream, _) = client::connect_and_handshake(ip, port).await?;
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
        drop(stream);

        // Build menu items
        let display_path = if current_relative.is_empty() {
            dest_root.to_string()
        } else {
            format!("{}/{}", dest_root, current_relative)
        };

        let mut items = vec![">> SELECT THIS FOLDER <<".to_string()];
        if !current_relative.is_empty() {
            items.push(".. (go back)".to_string());
        }
        for entry in &entries {
            if entry.is_dir {
                items.push(format!("{}/", entry.name));
            } else {
                items.push(format!(
                    "  {} ({})",
                    entry.name,
                    util::format_size(entry.size)
                ));
            }
        }

        let selection = Select::with_theme(theme)
            .with_prompt(&display_path)
            .items(&items)
            .default(0)
            .interact()?;

        match selection {
            0 => break, // select this folder
            1 if !current_relative.is_empty() => {
                // go back
                if let Some(pos) = current_relative.rfind('/') {
                    current_relative.truncate(pos);
                } else {
                    current_relative.clear();
                }
            }
            idx => {
                let offset = if current_relative.is_empty() { 1 } else { 2 };
                if idx >= offset {
                    let entry = &entries[idx - offset];
                    if entry.is_dir {
                        if current_relative.is_empty() {
                            current_relative = entry.name.clone();
                        } else {
                            current_relative = format!("{}/{}", current_relative, entry.name);
                        }
                    }
                    // clicking a file does nothing
                }
            }
        }
    }

    if current_relative.is_empty() {
        Ok(dest_root.to_string())
    } else {
        Ok(format!("{}/{}", dest_root, current_relative))
    }
}

fn pick_local_paths(theme: &ColorfulTheme) -> Result<Vec<PathBuf>> {
    // Choose starting location
    let cwd = std::env::current_dir()?;
    let home = dirs::home_dir().unwrap_or_else(|| cwd.clone());
    let desktop = home.join("Desktop");

    let mut start_options = vec![
        format!("Current directory ({})", cwd.display()),
        format!("Home ({})", home.display()),
    ];
    if desktop.is_dir() {
        start_options.push(format!("Desktop ({})", desktop.display()));
    }
    start_options.push("Root (/)".to_string());
    start_options.push("Enter path manually".to_string());

    let start_idx = Select::with_theme(theme)
        .with_prompt("Browse from")
        .items(&start_options)
        .default(0)
        .interact()?;

    let mut current_dir = match start_idx {
        0 => cwd,
        1 => home.clone(),
        idx if desktop.is_dir() && idx == 2 => desktop,
        idx if desktop.is_dir() && idx == 3 => PathBuf::from("/"),
        idx if !desktop.is_dir() && idx == 2 => PathBuf::from("/"),
        _ => {
            let path: String = Input::with_theme(theme)
                .with_prompt("Enter path")
                .interact_text()?;
            PathBuf::from(path.trim())
        }
    };

    loop {
        let mut entries: Vec<(String, PathBuf, bool, u64)> = Vec::new();
        for entry in std::fs::read_dir(&current_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') {
                continue;
            }
            let size = if meta.is_file() { meta.len() } else { 0 };
            entries.push((name, entry.path(), meta.is_dir(), size));
        }
        entries.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));

        let items: Vec<String> = std::iter::once(".. (go back)".to_string())
            .chain(entries.iter().map(|(name, _, is_dir, size)| {
                if *is_dir {
                    format!("{}/", name)
                } else {
                    format!("{} ({})", name, util::format_size(*size))
                }
            }))
            .collect();

        println!("\nLocal: {}", current_dir.display());
        let selections = MultiSelect::with_theme(theme)
            .with_prompt("Select files/folders (space=toggle, enter=confirm)")
            .items(&items)
            .interact()?;

        if selections.is_empty() {
            if Confirm::with_theme(theme)
                .with_prompt("Nothing selected. Cancel?")
                .default(false)
                .interact()?
            {
                return Ok(Vec::new());
            }
            continue;
        }

        // If only ".." selected, navigate up
        if selections == vec![0] {
            if let Some(parent) = current_dir.parent() {
                current_dir = parent.to_path_buf();
            }
            continue;
        }

        let mut selected = Vec::new();
        for idx in selections {
            if idx == 0 {
                continue; // skip ".."
            }
            selected.push(entries[idx - 1].1.clone());
        }

        if !selected.is_empty() {
            return Ok(selected);
        }
    }
}

async fn transfer_with_progress(
    ip: &str,
    port: u16,
    source: &Path,
    remote_destination: &str,
    auth_code: Option<&str>,
) -> Result<()> {
    let source = tokio::fs::canonicalize(source).await?;
    let scan = client::scan_source(&source).await?;
    if scan.files.is_empty() && scan.directories.is_empty() {
        bail!("source has no transferable entries");
    }

    let file_count = scan.files.len();
    let worker_count = default_jobs(file_count);
    println!(
        "  {} files, {} dirs, {}, {} workers",
        file_count,
        scan.directories.len(),
        util::format_size(scan.total_bytes),
        worker_count,
    );

    // Create directories first
    if !scan.directories.is_empty() {
        let mut stream = client::connect_and_handshake(ip, port).await?.0;
        for dir in &scan.directories {
            send_control(
                &mut stream,
                &ControlMessage::CreateDirectory {
                    destination_path: remote_destination.to_string(),
                    relative_path: dir.relative_path.clone(),
                    mtime_secs: dir.mtime_secs,
                    auth_code: auth_code.map(|s| s.to_string()),
                    dry_run: false,
                },
            )
            .await?;
            match read_control(&mut stream).await? {
                ControlMessage::DirectoryCreated { .. } => {}
                ControlMessage::Error { message } => bail!("{message}"),
                other => bail!("unexpected create directory response: {other:?}"),
            }
        }
    }

    // Transfer files with progress
    let multi = MultiProgress::new();
    let overall_bar = multi.add(ProgressBar::new(scan.total_bytes));
    overall_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );
    overall_bar.set_message(format!("0/{file_count} files"));

    let transferred_bytes = Arc::new(AtomicU64::new(0));
    let done_files = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(worker_count.max(1)));
    let mut set = JoinSet::new();

    for entry in scan.files {
        let permit = semaphore.clone().acquire_owned().await?;
        let target = ip.to_string();
        let destination = remote_destination.to_string();
        let auth = auth_code.map(|s| s.to_string());
        let transferred = Arc::clone(&transferred_bytes);
        let done = Arc::clone(&done_files);
        let file_bar = multi.add(ProgressBar::new(entry.size));
        file_bar.set_style(
            ProgressStyle::default_bar()
                .template("  {prefix:.dim} [{bar:30}] {bytes}/{total_bytes}")
                .unwrap()
                .progress_chars("#>-"),
        );
        file_bar.set_prefix(entry.relative_path.clone());
        let overall = overall_bar.clone();
        let fc = file_count;

        set.spawn(async move {
            let _permit = permit;
            let result = transfer_single_file(
                &target,
                port,
                &destination,
                auth.as_deref(),
                &entry,
                &transferred,
                &file_bar,
                &overall,
            )
            .await;
            done.fetch_add(1, Ordering::Relaxed);
            file_bar.finish_and_clear();
            overall.set_message(format!("{}/{fc} files", done.load(Ordering::Relaxed)));
            (entry.relative_path.clone(), result)
        });
    }

    let mut errors = Vec::new();
    let mut transferred = 0usize;
    let mut skipped = 0usize;

    while let Some(task) = set.join_next().await {
        match task {
            Ok((_, Ok(TransferOutcome::Transferred))) => transferred += 1,
            Ok((_, Ok(TransferOutcome::AlreadyExists))) => skipped += 1,
            Ok((_, Ok(TransferOutcome::Conflict))) => skipped += 1,
            Ok((path, Err(err))) => errors.push(format!("{path}: {err}")),
            Err(err) => errors.push(format!("worker: {err}")),
        }
    }

    overall_bar.finish_with_message("done");

    println!(
        "  transferred: {}, skipped: {}, errors: {}",
        transferred,
        skipped,
        errors.len()
    );
    for err in &errors {
        eprintln!("  error: {err}");
    }
    if !errors.is_empty() {
        bail!("some transfers failed");
    }
    Ok(())
}

enum TransferOutcome {
    Transferred,
    AlreadyExists,
    Conflict,
}

async fn transfer_single_file(
    target: &str,
    port: u16,
    destination_path: &str,
    auth_code: Option<&str>,
    file: &client::FileEntry,
    transferred_total: &Arc<AtomicU64>,
    file_bar: &ProgressBar,
    overall_bar: &ProgressBar,
) -> Result<TransferOutcome> {
    // Stream-hash mode: no pre-hash, hash in parallel with send
    let (mut stream, _) = client::connect_and_handshake(target, port).await?;

    send_control(
        &mut stream,
        &ControlMessage::PrepareUpload {
            destination_path: destination_path.to_string(),
            relative_path: file.relative_path.clone(),
            file_size: file.size,
            file_hash: String::new(), // stream-hash mode
            mtime_secs: file.mtime_secs,
            overwrite: false,
            auth_code: auth_code.map(|s| s.to_string()),
            dry_run: false,
        },
    )
    .await?;

    let mut ready = match read_control(&mut stream).await? {
        ControlMessage::UploadReady {
            status,
            offset,
            partial_hash,
            ..
        } => ReadyState {
            status,
            offset,
            partial_hash,
        },
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected: {other:?}"),
    };

    match ready.status {
        crate::protocol::PrepareStatus::AlreadyExists => return Ok(TransferOutcome::AlreadyExists),
        crate::protocol::PrepareStatus::Conflict => return Ok(TransferOutcome::Conflict),
        crate::protocol::PrepareStatus::Ready => {}
    }

    // Handle resume
    if ready.offset > 0 {
        let local_prefix =
            crate::util::hash_file_prefix_exact(&file.abs_path, ready.offset).await?;
        if ready.partial_hash.as_deref() != Some(local_prefix.as_str()) {
            send_control(
                &mut stream,
                &ControlMessage::RestartUpload {
                    destination_path: destination_path.to_string(),
                    relative_path: file.relative_path.clone(),
                    auth_code: auth_code.map(|s| s.to_string()),
                },
            )
            .await?;
            ready = match read_control(&mut stream).await? {
                ControlMessage::UploadReady {
                    status,
                    offset,
                    partial_hash,
                    ..
                } => ReadyState {
                    status,
                    offset,
                    partial_hash,
                },
                ControlMessage::Error { message } => bail!("{message}"),
                other => bail!("unexpected: {other:?}"),
            };
        }
    }

    send_control(
        &mut stream,
        &ControlMessage::BeginUpload {
            destination_path: destination_path.to_string(),
            relative_path: file.relative_path.clone(),
            offset: ready.offset,
            file_size: file.size,
            file_hash: String::new(), // stream-hash mode
            mtime_secs: file.mtime_secs,
            overwrite: false,
            auth_code: auth_code.map(|s| s.to_string()),
            dry_run: false,
        },
    )
    .await?;

    match read_control(&mut stream).await? {
        ControlMessage::BeginAck { offset } if offset == ready.offset => {}
        ControlMessage::Error { message } => bail!("{message}"),
        other => bail!("unexpected: {other:?}"),
    }

    file_bar.set_position(ready.offset);
    let file_handle = tokio::fs::File::open(&file.abs_path).await?;
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, file_handle);
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, &mut stream);
    let mut hasher = blake3::Hasher::new();

    let mut buf = vec![0u8; 4 * 1024 * 1024];
    let mut hashed_prefix = 0u64;
    while hashed_prefix < ready.offset {
        let remaining = (ready.offset - hashed_prefix) as usize;
        let cap = usize::min(remaining, buf.len());
        let read = reader.read(&mut buf[..cap]).await?;
        if read == 0 {
            bail!("source file became shorter before resume offset");
        }
        hasher.update(&buf[..read]);
        hashed_prefix += read as u64;
    }

    let to_send = file.size.saturating_sub(ready.offset);
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
        file_bar.inc(read as u64);
        overall_bar.inc(read as u64);
    }
    writer.flush().await?;
    let client_hash = hasher.finalize().to_hex().to_string();

    match read_control(&mut stream).await? {
        ControlMessage::TransferResult {
            verified,
            final_hash,
            error,
            ..
        } => {
            if verified && final_hash == client_hash {
                Ok(TransferOutcome::Transferred)
            } else if !final_hash.is_empty() && final_hash == client_hash {
                Ok(TransferOutcome::Transferred)
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
        other => bail!("unexpected: {other:?}"),
    }
}

struct ReadyState {
    status: crate::protocol::PrepareStatus,
    offset: u64,
    partial_hash: Option<String>,
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
