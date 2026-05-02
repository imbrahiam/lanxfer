use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
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

fn get_interface_broadcast_addrs() -> Vec<Ipv4Addr> {
    let mut addrs = Vec::new();
    match if_addrs::get_if_addrs() {
        Ok(interfaces) => {
            for iface in interfaces {
                if iface.is_loopback() {
                    continue;
                }
                if let if_addrs::IfAddr::V4(ref v4) = iface.addr {
                    let ip = v4.ip;
                    // Compute broadcast from common subnet masks
                    for cidr in [8, 16, 24, 12, 20] {
                        let broadcast = cidr_to_broadcast(ip, cidr);
                        if !addrs.contains(&broadcast) {
                            addrs.push(broadcast);
                        }
                    }
                }
            }
        }
        Err(err) => eprintln!("warning: failed to enumerate interfaces: {err}"),
    }
    addrs.sort();
    addrs.dedup();
    addrs
}

pub fn get_interface_summary() -> Vec<String> {
    let mut lines = Vec::new();
    match if_addrs::get_if_addrs() {
        Ok(interfaces) => {
            for iface in interfaces {
                if let if_addrs::IfAddr::V4(ref v4) = iface.addr {
                    let ip = v4.ip;
                    let mask = u32::from(v4.netmask);
                    let cidr = mask.leading_ones() as u8;
                    let bcast = cidr_to_broadcast(ip, cidr);
                    if iface.is_loopback() {
                        lines.push(format!("  {} ({}/{} loopback)", iface.name, ip, cidr));
                    } else {
                        lines.push(format!("  {} ({}/{}, broadcast {})", iface.name, ip, cidr, bcast));
                    }
                }
            }
        }
        Err(err) => {
            lines.push(format!("  warning: {err}"));
        }
    }
    lines
}

fn cidr_to_broadcast(ip: Ipv4Addr, cidr: u8) -> Ipv4Addr {
    let ip_bits = u32::from(ip);
    let mask = if cidr == 0 {
        0u32
    } else {
        !0u32 << (32 - cidr)
    };
    let broadcast_bits = ip_bits | !mask;
    Ipv4Addr::from(broadcast_bits)
}

pub async fn discover_hosts(discovery_port: u16, timeout_ms: u64) -> Result<Vec<DiscoveredHost>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.set_broadcast(true)?;

    let mut targets: Vec<std::net::SocketAddr> = Vec::new();

    // Try limited broadcast (always included)
    let global_broadcast: std::net::SocketAddr =
        format!("255.255.255.255:{discovery_port}").parse()?;
    targets.push(global_broadcast);

    // Try localhost
    let localhost_addr: std::net::SocketAddr = format!("127.0.0.1:{discovery_port}").parse()?;
    targets.push(localhost_addr);

    // Add per-interface broadcast addresses
    let iface_broadcasts = get_interface_broadcast_addrs();
    for bcast in &iface_broadcasts {
        let addr = std::net::SocketAddr::V4(SocketAddrV4::new(*bcast, discovery_port));
        if !targets.contains(&addr) {
            targets.push(addr);
        }
    }

    // Send discovery packet to all targets
    let magic_bytes = DISCOVER_MAGIC.as_bytes();
    for target in &targets {
        let _ = socket.send_to(magic_bytes, target).await;
    }

    // Retry after short delay to catch interfaces that come up late
    tokio::time::sleep(Duration::from_millis(200)).await;
    for target in &targets {
        let _ = socket.send_to(magic_bytes, target).await;
    }

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
    match addr {
        SocketAddr::V4(v4) => Some(DiscoveredHost {
            ip: v4.ip().to_string(),
            reply,
        }),
        SocketAddr::V6(v6) => Some(DiscoveredHost {
            ip: v6.ip().to_string(),
            reply,
        }),
    }
}
