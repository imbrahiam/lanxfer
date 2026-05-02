use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time;

use crate::protocol::DeviceInfo;

const DISCOVER_MAGIC: &str = "LANXFER_DISCOVER_V2";
const DISCOVER_REPLY_MAGIC: &str = "LANXFER_HERE_V2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryReply {
    pub host: String,
    pub control_port: u16,
    pub auth_required: bool,
    pub device: DeviceInfo,
}

#[derive(Debug, Clone)]
pub struct DiscoveredHost {
    pub ip: String,
    pub reply: DiscoveryReply,
}

pub async fn run_responder(
    discovery_port: u16,
    control_port: u16,
    auth_required: bool,
    device: DeviceInfo,
) -> Result<()> {
    let bind = format!("0.0.0.0:{discovery_port}");
    let socket = UdpSocket::bind(bind).await?;
    let mut buf = [0u8; 1024];

    loop {
        let (len, addr) = socket.recv_from(&mut buf).await?;
        if &buf[..len] != DISCOVER_MAGIC.as_bytes() {
            continue;
        }
        let payload = DiscoveryReply {
            host: device.host_name.clone(),
            control_port,
            auth_required,
            device: device.clone(),
        };
        let encoded = serde_json::to_string(&payload)?;
        let packet = format!("{DISCOVER_REPLY_MAGIC}|{encoded}");
        let _ = socket.send_to(packet.as_bytes(), addr).await;
    }
}

pub async fn discover_hosts(discovery_port: u16, timeout_ms: u64) -> Result<Vec<DiscoveredHost>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.set_broadcast(true)?;

    let broadcast_addr: SocketAddr = format!("255.255.255.255:{discovery_port}").parse()?;
    let localhost_addr: SocketAddr = format!("127.0.0.1:{discovery_port}").parse()?;

    let _ = socket
        .send_to(DISCOVER_MAGIC.as_bytes(), broadcast_addr)
        .await?;
    let _ = socket
        .send_to(DISCOVER_MAGIC.as_bytes(), localhost_addr)
        .await?;

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut buf = [0u8; 8192];
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let wait = deadline.saturating_duration_since(now);
        let recv = time::timeout(wait, socket.recv_from(&mut buf)).await;
        let Ok(Ok((len, addr))) = recv else {
            break;
        };

        if let Some(host) = parse_reply(&buf[..len], addr) {
            let key = format!("{}:{}", host.ip, host.reply.control_port);
            if seen.insert(key) {
                out.push(host);
            }
        }
    }

    out.sort_by(|a, b| a.ip.cmp(&b.ip));
    Ok(out)
}

fn parse_reply(payload: &[u8], addr: SocketAddr) -> Option<DiscoveredHost> {
    let text = std::str::from_utf8(payload).ok()?;
    let (magic, json) = text.split_once('|')?;
    if magic != DISCOVER_REPLY_MAGIC {
        return None;
    }
    let reply = serde_json::from_str::<DiscoveryReply>(json).ok()?;
    Some(DiscoveredHost {
        ip: addr.ip().to_string(),
        reply,
    })
}
