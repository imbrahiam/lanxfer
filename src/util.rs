use anyhow::{Result, anyhow};
use std::path::Path;
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

pub fn local_ipv4s() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if let if_addrs::IfAddr::V4(v4) = iface.addr {
                ips.push(v4.ip.to_string());
            }
        }
    }
    ips
}

pub fn generate_pairing_code() -> String {
    const CHARS: &[u8; 32] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut value = u128::from_le_bytes(*uuid::Uuid::new_v4().as_bytes());
    (0..8)
        .map(|_| {
            let c = CHARS[(value & 31) as usize] as char;
            value >>= 5;
            c
        })
        .collect()
}

pub fn auth_challenge() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Challenge-response proof used in place of sending pairing secrets over
/// the network. Each connection receives a fresh server challenge.
pub fn auth_proof(secret: &str, challenge: &str) -> String {
    let normalized = secret.trim().to_uppercase();
    let key = blake3::derive_key("lanxfer pairing proof v1", normalized.as_bytes());
    let proof = blake3::keyed_hash(&key, challenge.as_bytes());
    format!("v1:{}", proof.to_hex())
}

pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut different = left.len() ^ right.len();
    let max = left.len().max(right.len());
    for i in 0..max {
        different |=
            (left.get(i).copied().unwrap_or(0) ^ right.get(i).copied().unwrap_or(0)) as usize;
    }
    different == 0
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

/// Hash the first `size` bytes of a file, returning the live hasher so the
/// caller can keep hashing subsequent data in the same stream (single-pass
/// verification across a resume boundary).
pub async fn hash_prefix_hasher(path: &Path, size: u64) -> Result<blake3::Hasher> {
    let path = path.to_path_buf();
    task::spawn_blocking(move || {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut remaining = size;

        while remaining > 0 {
            let to_read = usize::min(remaining as usize, buf.len());
            let read = file.read(&mut buf[..to_read])?;
            if read == 0 {
                anyhow::bail!("file shorter than expected prefix");
            }
            hasher.update(&buf[..read]);
            remaining -= read as u64;
        }
        Ok(hasher)
    })
    .await
    .map_err(|e| anyhow!("hash worker task failed: {e}"))?
}

// ---------------------------------------------------------------------------
// Merkle striping
//
// Large files are transferred as independent stripes over parallel
// connections. Stripe boundaries are aligned to BLAKE3's internal Merkle
// tree (1 KiB chunks, power-of-two subtrees), so each side hashes stripes
// independently — in any order, on any connection — and merges the subtree
// chaining values into the exact whole-file BLAKE3 hash. No extra disk pass.
// ---------------------------------------------------------------------------

/// 64 MiB = 2^16 BLAKE3 chunks — a valid subtree boundary.
pub const STRIPE_SIZE: u64 = 64 * 1024 * 1024;
/// Files at or above this are striped across connections.
pub const STRIPE_THRESHOLD: u64 = 256 * 1024 * 1024;

pub type StripeCv = blake3::hazmat::ChainingValue;

pub fn is_striped(size: u64) -> bool {
    size >= STRIPE_THRESHOLD
}

pub fn stripe_count(size: u64) -> u32 {
    size.div_ceil(STRIPE_SIZE) as u32
}

pub fn stripe_range(size: u64, index: u32) -> (u64, u64) {
    let start = index as u64 * STRIPE_SIZE;
    (start, u64::min(STRIPE_SIZE, size - start))
}

/// Hasher positioned to hash the stripe at `index` as a BLAKE3 subtree.
pub fn stripe_hasher(index: u32) -> blake3::Hasher {
    use blake3::hazmat::HasherExt;
    let mut h = blake3::Hasher::new();
    h.set_input_offset(index as u64 * STRIPE_SIZE);
    h
}

pub fn finish_stripe(hasher: &blake3::Hasher) -> StripeCv {
    use blake3::hazmat::HasherExt;
    hasher.finalize_non_root()
}

/// Merge per-stripe chaining values into the whole-file BLAKE3 hash.
/// `cvs` must be ordered by stripe index and cover exactly `total_len` bytes.
pub fn merge_stripes(cvs: &[StripeCv], total_len: u64) -> blake3::Hash {
    merge_stripes_with(cvs, total_len, STRIPE_SIZE)
}

fn merge_stripes_with(cvs: &[StripeCv], total_len: u64, stripe_size: u64) -> blake3::Hash {
    use blake3::hazmat::{Mode, merge_subtrees_non_root, merge_subtrees_root};

    fn non_root(cvs: &[StripeCv], len: u64, stripe_size: u64) -> StripeCv {
        if cvs.len() == 1 {
            return cvs[0];
        }
        let left_len = blake3::hazmat::left_subtree_len(len);
        let left_count = (left_len / stripe_size) as usize;
        let left = non_root(&cvs[..left_count], left_len, stripe_size);
        let right = non_root(&cvs[left_count..], len - left_len, stripe_size);
        merge_subtrees_non_root(&left, &right, Mode::Hash)
    }

    assert!(cvs.len() >= 2, "merge_stripes needs at least two stripes");
    let left_len = blake3::hazmat::left_subtree_len(total_len);
    let left_count = (left_len / stripe_size) as usize;
    let left = non_root(&cvs[..left_count], left_len, stripe_size);
    let right = non_root(&cvs[left_count..], total_len - left_len, stripe_size);
    merge_subtrees_root(&left, &right, Mode::Hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blake3::hazmat::HasherExt;

    // The correctness keystone: hashing stripes independently (out of order
    // conceptually) and merging must equal hashing the whole input at once.
    #[test]
    fn merged_stripe_cvs_equal_whole_file_hash() {
        // 4 KiB test stripes = 4 chunks (power of two), same math as 64 MiB.
        const S: u64 = 4096;
        for total in [
            2 * S,        // exact two stripes
            4 * S,        // exact power-of-two stripe count
            3 * S + 1,    // partial tail of 1 byte
            5 * S + 1500, // odd count + odd tail
            7 * S,        // non-power-of-two exact
        ] {
            let data: Vec<u8> = (0..total).map(|i| (i * 31 % 251) as u8).collect();
            let expected = blake3::hash(&data);

            let mut cvs = Vec::new();
            let mut off = 0u64;
            while off < total {
                let len = u64::min(S, total - off);
                let mut h = blake3::Hasher::new();
                h.set_input_offset(off);
                h.update(&data[off as usize..(off + len) as usize]);
                cvs.push(h.finalize_non_root());
                off += len;
            }
            let merged = merge_stripes_with(&cvs, total, S);
            assert_eq!(merged, expected, "total_len={total}");
        }
    }

    #[test]
    fn stripe_geometry() {
        assert!(!is_striped(STRIPE_THRESHOLD - 1));
        assert!(is_striped(STRIPE_THRESHOLD));
        assert_eq!(stripe_count(STRIPE_THRESHOLD), 4);
        assert_eq!(stripe_range(STRIPE_THRESHOLD + 5, 4), (4 * STRIPE_SIZE, 5));
    }

    #[test]
    fn pairing_codes_and_proofs_are_strong_and_challenge_bound() {
        let code = generate_pairing_code();
        assert_eq!(code.len(), 8);
        assert!(code.chars().all(|c| c.is_ascii_alphanumeric()));

        let first = auth_proof(&code, "challenge-a");
        assert!(constant_time_eq(
            &first,
            &auth_proof(&code.to_lowercase(), "challenge-a")
        ));
        assert!(!constant_time_eq(&first, &auth_proof(&code, "challenge-b")));
    }
}
