use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 4;
pub const DEFAULT_CONTROL_PORT: u16 = 44818;
pub const DEFAULT_DISCOVERY_PORT: u16 = 44819;
// Manifests carry the whole file tree in one frame.
const MAX_CONTROL_FRAME: usize = 64 * 1024 * 1024;

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
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSpec {
    pub id: u32,
    pub rel_path: String,
    pub size: u64,
    pub mtime_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirSpec {
    pub rel_path: String,
    pub mtime_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFileSpec {
    pub id: u32,
    pub abs_path: String,
    pub rel_path: String,
    pub size: u64,
    pub mtime_secs: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanAction {
    /// Send the whole file from byte 0.
    Send,
    /// A matching `.part` file exists — continue from `offset` if the
    /// sender's prefix hash matches `partial_hash`.
    Resume { offset: u64, partial_hash: String },
    /// Identical size + mtime already at destination.
    SkipUpToDate,
    /// Exists with different content and overwrite is off.
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileAction {
    pub id: u32,
    pub action: PlanAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlMessage {
    Hello {
        version: u32,
        client_name: String,
        /// Client's listening port for reverse-initiated sessions.
        /// Missing in older clients — defaults to 0.
        #[serde(default)]
        client_port: u16,
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
    BrowseDirectory {
        destination_path: String,
        relative_path: String,
        auth_code: Option<String>,
    },
    DirectoryContents {
        relative_path: String,
        entries: Vec<DirEntry>,
    },

    // --- v3 session protocol ---
    /// Whole transfer manifest in one message. Receiver creates directories,
    /// plans each file, and replies with SessionPlan.
    BeginSession {
        destination_path: String,
        auth_code: Option<String>,
        overwrite: bool,
        dry_run: bool,
        files: Vec<FileSpec>,
        dirs: Vec<DirSpec>,
    },
    SessionPlan {
        session_id: String,
        actions: Vec<FileAction>,
    },
    /// Sender's local prefix didn't match a Resume action — receiver must
    /// truncate the part file and expect the whole file from 0.
    RestartFile {
        id: u32,
    },
    RestartAck {
        id: u32,
    },
    /// Sent on a data connection to bind it to a session.
    JoinSession {
        session_id: String,
    },
    JoinAck,
    /// Data frame header: exactly `len` raw bytes follow on this connection,
    /// then the next control frame — back-to-back, no acks in between.
    SendFile {
        id: u32,
        offset: u64,
        len: u64,
    },
    /// Emitted on the control connection when a file is fully received,
    /// verified-by-hash and renamed into place.
    FileDone {
        id: u32,
        hash: String,
        ok: bool,
        error: Option<String>,
    },

    /// Ask the remote to push these selected paths to us. Paths may be files
    /// or directories; directories are scanned recursively by the remote.
    /// Remote reads paths from its own disk, connects back to
    /// requester's server, and acts as the sender.
    PushRequest {
        files: Vec<RemoteFileSpec>,
        dest_local_path: String,
        requester_port: u16,
        auth_code: Option<String>,
        overwrite: bool,
        /// One-time token the requester registered with its own server —
        /// authorizes the remote's write-back without leaking pairing codes.
        #[serde(default)]
        return_auth_code: Option<String>,
    },
    /// Sent by the remote after finishing a PushRequest transfer.
    PushComplete {
        files_sent: usize,
        bytes: u64,
        errors: Vec<String>,
    },

    /// Sent by an interactive client right after the handshake to mark this
    /// connection as a persistent peer-presence link (it appears in the
    /// remote picker). Connections that don't attach are ordinary request
    /// connections and are served normally.
    Attach,

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

/// `read_control` with a deadline — for interactive request/response
/// round-trips that must never freeze the UI. Not for transfer waits,
/// which can legitimately take minutes.
pub async fn read_control_timeout<R>(
    reader: &mut R,
    wait: std::time::Duration,
) -> Result<ControlMessage>
where
    R: AsyncRead + Unpin,
{
    match tokio::time::timeout(wait, read_control(reader)).await {
        Ok(result) => result,
        Err(_) => bail!("peer did not respond within {}s", wait.as_secs()),
    }
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
