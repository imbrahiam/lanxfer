use anyhow::{Result, anyhow, bail};
use std::cmp;
use std::path::Path;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::discovery;
use crate::protocol::{
    ControlMessage, PROTOCOL_VERSION, PrepareStatus, read_control, send_control,
};
use crate::storage;
use crate::util;

pub async fn run_server(
    bind: String,
    discovery_port: u16,
    pairing_code: Option<String>,
) -> Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    let local = listener.local_addr()?;
    let device = util::local_device_info();
    let pairing_code = pairing_code.unwrap_or_else(util::generate_pairing_code);

    let discovery_device = device.clone();
    tokio::spawn(async move {
        if let Err(err) =
            discovery::run_responder(discovery_port, local.port(), true, discovery_device).await
        {
            eprintln!("discovery responder stopped: {err:#}");
        }
    });

    println!(
        "lanxfer receiver listening on {} (discovery udp {})",
        local, discovery_port
    );
    println!("device: {} {} {}", device.host_name, device.os, device.arch);
    println!("pairing code: {pairing_code}");

    loop {
        let (socket, peer) = listener.accept().await?;
        let server_device = device.clone();
        let server_code = pairing_code.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(socket, server_device, server_code).await {
                eprintln!("client {peer} error: {err:#}");
            }
        });
    }
}

fn tune_socket(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let buf_size: libc::c_int = 4 * 1024 * 1024;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

async fn handle_client(
    mut stream: TcpStream,
    server_device: crate::protocol::DeviceInfo,
    pairing_code: String,
) -> Result<()> {
    stream.set_nodelay(true)?;
    tune_socket(&stream);

    let first = read_control(&mut stream).await?;
    match first {
        ControlMessage::Hello { version, .. } if version == PROTOCOL_VERSION => {
            send_control(
                &mut stream,
                &ControlMessage::HelloAck {
                    version: PROTOCOL_VERSION,
                    server: server_device,
                    auth_required: true,
                },
            )
            .await?;
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
    }

    loop {
        let msg = match read_control(&mut stream).await {
            Ok(msg) => msg,
            Err(_) => return Ok(()),
        };

        match msg {
            ControlMessage::ListDestinations => {
                let items = storage::list_destinations();
                send_control(&mut stream, &ControlMessage::Destinations { items }).await?;
            }
            ControlMessage::BrowseDirectory {
                destination_path,
                relative_path,
                auth_code,
            } => {
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let root = match storage::ensure_destination_root(&destination_path) {
                    Ok(v) => v,
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                let target = if relative_path.is_empty() {
                    root
                } else {
                    let rel = match storage::sanitize_relative_path(&relative_path) {
                        Ok(v) => v,
                        Err(err) => {
                            send_control(
                                &mut stream,
                                &ControlMessage::Error {
                                    message: err.to_string(),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };
                    root.join(rel)
                };
                let entries = match storage::list_directory(&target) {
                    Ok(v) => v,
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                send_control(
                    &mut stream,
                    &ControlMessage::DirectoryContents {
                        relative_path,
                        entries,
                    },
                )
                .await?;
            }
            ControlMessage::CreateDirectory {
                destination_path,
                relative_path,
                mtime_secs,
                auth_code,
                dry_run,
            } => {
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let root = match storage::ensure_destination_root(&destination_path) {
                    Ok(v) => v,
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                let rel = match storage::sanitize_relative_path(&relative_path) {
                    Ok(v) => v,
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: err.to_string(),
                            },
                        )
                        .await?;
                        continue;
                    }
                };
                let dir = root.join(rel);
                if !dry_run {
                    fs::create_dir_all(&dir).await?;
                    let _ = util::set_mtime(&dir, mtime_secs).await;
                }
                send_control(
                    &mut stream,
                    &ControlMessage::DirectoryCreated { relative_path },
                )
                .await?;
            }
            ControlMessage::PrepareUpload {
                destination_path,
                relative_path,
                file_size,
                file_hash,
                overwrite,
                auth_code,
                dry_run,
                ..
            } => {
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let reply = match prepare_upload(
                    &destination_path,
                    &relative_path,
                    file_size,
                    &file_hash,
                    overwrite,
                    dry_run,
                )
                .await
                {
                    Ok(v) => v,
                    Err(err) => ControlMessage::Error {
                        message: err.to_string(),
                    },
                };
                send_control(&mut stream, &reply).await?;
            }
            ControlMessage::RestartUpload {
                destination_path,
                relative_path,
                auth_code,
            } => {
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }
                let (_, part_path) =
                    match storage::build_target_paths(&destination_path, &relative_path) {
                        Ok(v) => v,
                        Err(err) => {
                            send_control(
                                &mut stream,
                                &ControlMessage::Error {
                                    message: err.to_string(),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&part_path)
                    .await?;
                file.set_len(0).await?;
                file.flush().await?;
                send_control(
                    &mut stream,
                    &ControlMessage::UploadReady {
                        status: PrepareStatus::Ready,
                        offset: 0,
                        partial_hash: None,
                        message: None,
                    },
                )
                .await?;
            }
            ControlMessage::BeginUpload {
                destination_path,
                relative_path,
                offset,
                file_size,
                file_hash,
                mtime_secs,
                overwrite,
                auth_code,
                dry_run,
            } => {
                if let Err(err) = ensure_auth(auth_code.as_deref(), &pairing_code) {
                    send_control(
                        &mut stream,
                        &ControlMessage::Error {
                            message: err.to_string(),
                        },
                    )
                    .await?;
                    continue;
                }

                let (final_path, part_path) =
                    match storage::build_target_paths(&destination_path, &relative_path) {
                        Ok(v) => v,
                        Err(err) => {
                            send_control(
                                &mut stream,
                                &ControlMessage::Error {
                                    message: err.to_string(),
                                },
                            )
                            .await?;
                            continue;
                        }
                    };

                if dry_run {
                    send_control(&mut stream, &ControlMessage::BeginAck { offset }).await?;
                    send_control(
                        &mut stream,
                        &ControlMessage::TransferResult {
                            verified: true,
                            final_hash: String::new(),
                            bytes_received: file_size,
                            error: None,
                        },
                    )
                    .await?;
                    continue;
                }

                match fs::metadata(&part_path).await {
                    Ok(meta) if meta.len() != offset => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: format!(
                                    "offset mismatch. expected {}, got {}",
                                    meta.len(),
                                    offset
                                ),
                            },
                        )
                        .await?;
                        continue;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::Error {
                                message: format!("missing part file: {err}"),
                            },
                        )
                        .await?;
                        continue;
                    }
                }

                send_control(&mut stream, &ControlMessage::BeginAck { offset }).await?;

                match receive_transfer(
                    &mut stream,
                    &part_path,
                    &final_path,
                    file_size,
                    offset,
                    &file_hash,
                    overwrite,
                    mtime_secs,
                )
                .await
                {
                    Ok((bytes_received, final_hash, verified)) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::TransferResult {
                                verified,
                                final_hash,
                                bytes_received,
                                error: if verified {
                                    None
                                } else {
                                    Some("hash mismatch".to_string())
                                },
                            },
                        )
                        .await?;
                    }
                    Err(err) => {
                        send_control(
                            &mut stream,
                            &ControlMessage::TransferResult {
                                verified: false,
                                final_hash: String::new(),
                                bytes_received: 0,
                                error: Some(err.to_string()),
                            },
                        )
                        .await?;
                    }
                }
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

fn ensure_auth(provided: Option<&str>, expected: &str) -> Result<()> {
    let value = provided.unwrap_or_default().trim();
    if value.is_empty() {
        bail!("pairing code is required for write operations");
    }
    if value != expected {
        bail!("invalid pairing code");
    }
    Ok(())
}

async fn prepare_upload(
    destination_path: &str,
    relative_path: &str,
    file_size: u64,
    file_hash: &str,
    overwrite: bool,
    dry_run: bool,
) -> Result<ControlMessage> {
    let (final_path, part_path) = storage::build_target_paths(destination_path, relative_path)?;

    if let Some(parent) = final_path.parent() {
        if !dry_run {
            fs::create_dir_all(parent).await?;
        }
    }

    if final_path.exists() {
        if !overwrite {
            return Ok(ControlMessage::UploadReady {
                status: PrepareStatus::Conflict,
                offset: 0,
                partial_hash: None,
                message: Some(format!(
                    "target file exists and overwrite disabled: {}",
                    final_path.display()
                )),
            });
        }
        // AlreadyExists check only if client provided a hash (non-streaming mode)
        if !file_hash.is_empty() && fs::metadata(&final_path).await?.len() == file_size {
            let existing_hash = util::hash_file(&final_path).await?;
            if existing_hash == file_hash {
                return Ok(ControlMessage::UploadReady {
                    status: PrepareStatus::AlreadyExists,
                    offset: file_size,
                    partial_hash: None,
                    message: Some("matching target file already exists".to_string()),
                });
            }
        }
    }

    if dry_run {
        return Ok(ControlMessage::UploadReady {
            status: PrepareStatus::Ready,
            offset: 0,
            partial_hash: None,
            message: Some("dry-run mode".to_string()),
        });
    }

    if !part_path.exists() {
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&part_path)
            .await?;
    }

    let mut offset = fs::metadata(&part_path).await?.len();
    if offset > file_size {
        let file = OpenOptions::new().write(true).open(&part_path).await?;
        file.set_len(0).await?;
        offset = 0;
    }

    let partial_hash = if offset > 0 {
        Some(util::hash_file_prefix_exact(&part_path, offset).await?)
    } else {
        None
    };

    Ok(ControlMessage::UploadReady {
        status: PrepareStatus::Ready,
        offset,
        partial_hash,
        message: None,
    })
}

async fn receive_transfer(
    stream: &mut TcpStream,
    part_path: &Path,
    final_path: &Path,
    file_size: u64,
    offset: u64,
    file_hash: &str,
    overwrite: bool,
    mtime_secs: i64,
) -> Result<(u64, String, bool)> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(part_path)
        .await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut file = BufWriter::with_capacity(4 * 1024 * 1024, file);

    let mut remaining = file_size.saturating_sub(offset);
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    while remaining > 0 {
        let to_read = cmp::min(remaining as usize, buf.len());
        stream
            .read_exact(&mut buf[..to_read])
            .await
            .map_err(|e| anyhow!("transfer read error: {e}"))?;
        file.write_all(&buf[..to_read]).await?;
        remaining -= to_read as u64;
    }
    file.flush().await?;

    let final_hash = util::hash_file(part_path).await?;
    // If file_hash is empty (stream-hash mode), client will verify.
    // Always finalize the file — client does the comparison.
    let verified = if file_hash.is_empty() {
        true // stream mode: server trusts transfer, client verifies hash
    } else {
        final_hash == file_hash
    };
    if verified {
        if overwrite && final_path.exists() {
            let _ = fs::remove_file(final_path).await;
        }
        fs::rename(part_path, final_path).await?;
        let _ = util::set_mtime(final_path, mtime_secs).await;
    }

    Ok((file_size, final_hash, verified))
}
