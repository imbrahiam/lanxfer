use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task;

use crate::protocol::{DeviceInfo, PROTOCOL_VERSION};

pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub fn host_name() -> String {
    hostname::get()
        .ok()
        .and_then(|v| v.into_string().ok())
        .unwrap_or_else(|| "unknown-host".to_string())
}

pub fn local_device_info() -> DeviceInfo {
    DeviceInfo {
        host_name: host_name(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        protocol_version: PROTOCOL_VERSION,
    }
}

pub fn generate_pairing_code() -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    raw[..6].to_uppercase()
}

pub fn system_time_secs(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub async fn set_mtime(path: &Path, mtime_secs: i64) -> Result<()> {
    let target = path.to_path_buf();
    task::spawn_blocking(move || {
        let ft = filetime::FileTime::from_unix_time(mtime_secs, 0);
        filetime::set_file_mtime(target, ft)?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|e| anyhow!("mtime worker task failed: {e}"))??;
    Ok(())
}

pub async fn hash_file(path: &Path) -> Result<String> {
    hash_file_prefix(path, None).await
}

pub async fn hash_file_prefix_exact(path: &Path, size: u64) -> Result<String> {
    hash_file_prefix(path, Some(size)).await
}

async fn hash_file_prefix(path: &Path, limit: Option<u64>) -> Result<String> {
    let path = path.to_path_buf();
    let digest = task::spawn_blocking(move || hash_file_blocking(&path, limit))
        .await
        .map_err(|e| anyhow!("hash worker task failed: {e}"))??;
    Ok(digest)
}

fn hash_file_blocking(path: &PathBuf, limit: Option<u64>) -> Result<String> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    let mut remaining = limit.unwrap_or(u64::MAX);

    while remaining > 0 {
        let to_read = usize::min(remaining as usize, buf.len());
        let read = file.read(&mut buf[..to_read])?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        remaining = remaining.saturating_sub(read as u64);
    }

    Ok(hasher.finalize().to_hex().to_string())
}
