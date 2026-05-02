use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 2;
pub const DEFAULT_CONTROL_PORT: u16 = 44818;
pub const DEFAULT_DISCOVERY_PORT: u16 = 44819;
const MAX_CONTROL_FRAME: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub host_name: String,
    pub os: String,
    pub arch: String,
    pub protocol_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationInfo {
    pub label: String,
    pub path: String,
    pub available_bytes: u64,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrepareStatus {
    Ready,
    AlreadyExists,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlMessage {
    Hello {
        version: u32,
        client_name: String,
    },
    HelloAck {
        version: u32,
        server: DeviceInfo,
        auth_required: bool,
    },
    ListDestinations,
    Destinations {
        items: Vec<DestinationInfo>,
    },
    CreateDirectory {
        destination_path: String,
        relative_path: String,
        mtime_secs: i64,
        auth_code: Option<String>,
        dry_run: bool,
    },
    DirectoryCreated {
        relative_path: String,
    },
    PrepareUpload {
        destination_path: String,
        relative_path: String,
        file_size: u64,
        file_hash: String,
        mtime_secs: i64,
        overwrite: bool,
        auth_code: Option<String>,
        dry_run: bool,
    },
    UploadReady {
        status: PrepareStatus,
        offset: u64,
        partial_hash: Option<String>,
        message: Option<String>,
    },
    RestartUpload {
        destination_path: String,
        relative_path: String,
        auth_code: Option<String>,
    },
    BeginUpload {
        destination_path: String,
        relative_path: String,
        offset: u64,
        file_size: u64,
        file_hash: String,
        mtime_secs: i64,
        overwrite: bool,
        auth_code: Option<String>,
        dry_run: bool,
    },
    BeginAck {
        offset: u64,
    },
    TransferResult {
        verified: bool,
        final_hash: String,
        bytes_received: u64,
        error: Option<String>,
    },
    Error {
        message: String,
    },
}

pub async fn send_control<W>(writer: &mut W, msg: &ControlMessage) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let body = serde_json::to_vec(msg)?;
    if body.len() > MAX_CONTROL_FRAME {
        bail!("control frame too large: {} bytes", body.len());
    }
    writer
        .write_u32(body.len() as u32)
        .await
        .map_err(|e| anyhow!("failed writing control frame len: {e}"))?;
    writer
        .write_all(&body)
        .await
        .map_err(|e| anyhow!("failed writing control frame: {e}"))?;
    writer
        .flush()
        .await
        .map_err(|e| anyhow!("failed flushing control frame: {e}"))?;
    Ok(())
}

pub async fn read_control<R>(reader: &mut R) -> Result<ControlMessage>
where
    R: AsyncRead + Unpin,
{
    read_framed_json::<R, ControlMessage>(reader).await
}

pub async fn read_framed_json<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = reader
        .read_u32()
        .await
        .map_err(|e| anyhow!("failed reading control frame len: {e}"))? as usize;
    if len == 0 || len > MAX_CONTROL_FRAME {
        bail!("invalid control frame len: {len}");
    }
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| anyhow!("failed reading control frame body: {e}"))?;
    let msg = serde_json::from_slice::<T>(&buf)?;
    Ok(msg)
}
