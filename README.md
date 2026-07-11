# lanxfer

Fast, resumable LAN file transfer CLI with zero-config peer mode. Built for moving large files between machines on the same network at maximum speed.

## Features

- **Peer mode** - Just run `lanxfer` on each machine. Each peer becomes both sender and receiver. No `serve` step required.
- **Interactive mode** - Browse remote directories, pick local files, select destination drives — all from one session
- **Smart discovery** - UDP broadcast with automatic subnet scan fallback when broadcast is blocked
- **Resumable transfers** - Interrupted transfers resume from where they left off via `.lanxfer.part` files
- **BLAKE3 verification** - Every file verified with BLAKE3 hash after transfer
- **Parallel workers** - Multiple files transfer simultaneously for maximum throughput
- **Progress bars** - Real-time per-file and overall progress with speed and ETA
- **Pairing code auth** - Simple 6-character code protects against unauthorized writes
- **Cross-platform** - macOS, Linux, Windows

## Install

### From source (all platforms)

```bash
cargo install --git https://github.com/imbrahiam/lanxfer
```

### Updates

Release binaries can update themselves from GitHub Releases:

```bash
lanxfer update --check
lanxfer update
```

Use `lanxfer update --yes` for non-interactive installs. Builds installed
through a package manager should continue using that package manager.

### Build locally

```bash
git clone https://github.com/imbrahiam/lanxfer
cd lanxfer
cargo build --release
# Binary at target/release/lanxfer
```

### Add to PATH

```bash
# macOS/Linux - symlink to a directory in PATH
sudo ln -sf $(pwd)/target/release/lanxfer /usr/local/bin/lanxfer

# Or copy
sudo cp target/release/lanxfer /usr/local/bin/
```

```powershell
# Windows (PowerShell) - copy to a folder on PATH, or add the target folder to PATH:
Copy-Item .\target\release\lanxfer.exe "$env:USERPROFILE\bin\lanxfer.exe"
# (one-time) put $env:USERPROFILE\bin on PATH via System Properties -> Environment Variables
```

## Quick Start

### Peer mode (the easy way)

Run `lanxfer` on every machine. That's it.

```bash
lanxfer
```

Each peer starts a background receiver, shows its own pairing code, and lists the other peers it discovers on the LAN. Pick a peer, enter its code (shown on its screen), browse, send. Done.

Once connected you stay in a session with that peer: send more files, reuse the last destination, view the list of transfers so far — no rescanning or re-entering the code between sends.

On a trusted network you can skip pairing codes entirely:

```bash
lanxfer --open        # peer mode, no code needed to receive
lanxfer serve --open  # headless receiver, no code needed
```

### Headless receiver

If you want a machine to only receive (e.g., a server with no interactive shell):

```bash
lanxfer serve
```

### Direct commands

```bash
# Discover receivers
lanxfer discover

# Send files directly
lanxfer send 10.0.0.69 ./myfile.txt /home/user/dest --code A1B2C3

# Send a folder with overwrite
lanxfer send 10.0.0.69 ./myfolder /home/user/dest --code A1B2C3 --overwrite --jobs 6
```

## Commands

| Command | Description |
|---------|-------------|
| `lanxfer` | Peer mode (default) — auto-serve + auto-discover |
| `lanxfer --open` | Peer mode without pairing codes (trusted networks) |
| `lanxfer interactive` | Sender-only interactive session |
| `lanxfer serve` | Headless receiver |
| `lanxfer discover` | Find receivers on network |
| `lanxfer connect` | Connect to a receiver (discovery or `--target IP`) |
| `lanxfer destinations <ip>` | List drives on a receiver |
| `lanxfer send <ip> <src> <dest>` | Direct file transfer |

## Performance

Protocol v3 is built to hit the hardware limit, not the protocol limit:

- **Manifest sessions** — the whole file tree is negotiated in one round-trip.
  Files then stream back-to-back over persistent connections with zero
  per-file handshakes. 10k small files cost ~2 round-trips total, not ~40k.
- **Merkle striping** — files ≥ 256 MiB are split into 64 MiB stripes sent
  over parallel TCP connections. Stripe boundaries align with BLAKE3's
  internal Merkle tree (2¹⁶ × 1 KiB chunks), so each side hashes stripes
  independently — in any order, on any connection — and merges the subtree
  chaining values into the exact whole-file BLAKE3 hash. Parallel transfer
  *and* parallel verification, no extra disk pass.
- **Single-pass receiving** — the receiver hashes while writing. (v2 re-read
  every completed file from disk to verify it: 2× disk I/O, now gone.)
- **Skip-unchanged** — re-sending a tree skips files whose size+mtime already
  match; a repeat send of 10k files finishes in ~2 s.
- **4 MB I/O + socket buffers**, TCP_NODELAY, raw TCP streaming.

Loopback benchmark (Windows 11, NVMe): 1.5 GiB file in 1.14 s (~10.5 Gbps
effective); 10k × 4 KiB files in 9.2 s (v2: 15.1 s — and v2's per-file
round-trips cost far more on a real network than on loopback).

On a gigabit LAN expect wire speed; on 2.5/10 GbE and WiFi the striped
parallel streams keep the link full where a single TCP flow stalls.

### Tips to maximize speed

- Use **wired ethernet** if possible (WiFi adds latency and halves throughput)
- For many small files, increase workers: `--jobs 8`
- Both machines should be on the **same subnet** (no router hops)

## Network Requirements

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| 44818 | TCP | Sender -> Receiver | Control + file data |
| 44819 | UDP | Broadcast/Unicast | Discovery |

### Firewall

On the **receiver**, open these ports:

```bash
# Linux (ufw)
sudo ufw allow 44818/tcp
sudo ufw allow 44819/udp

# macOS - usually works out of the box (accept the firewall prompt)
```

```powershell
# Windows (Run PowerShell as Administrator)
New-NetFirewallRule -DisplayName "lanxfer TCP" -Direction Inbound -Protocol TCP -LocalPort 44818 -Action Allow
New-NetFirewallRule -DisplayName "lanxfer UDP" -Direction Inbound -Protocol UDP -LocalPort 44819 -Action Allow
```

## Architecture (protocol v3)

```
Sender                              Receiver
──────                              ────────
  ├─ UDP discovery ────────────────►├─ UDP responder (44819)
  │                                 │
  ├─ control conn ─────────────────►├─ TCP listener (44818)
  │   Hello/HelloAck                │
  │   BeginSession {manifest} ─────►├─ mkdirs, plans every file
  │  ◄── SessionPlan ───────────────┤   (send / resume / skip / conflict)
  │                                 │
  ├─ N data conns: JoinSession ────►│
  │   SendFile{id,offset,len} ─────►├─ write at offset, hash while writing
  │   [raw bytes] SendFile […] ────►│   (stripes: BLAKE3 subtree CVs,
  │   back-to-back, no acks         │    merged into whole-file hash)
  │                                 │
  │  ◄── FileDone{id,hash} ─────────┤  on the control conn, async
```

Files ≥ 256 MiB travel as 64 MiB stripes spread across the data
connections; everything else streams whole, back-to-back.

## Web version

`web/` contains a Next.js app (RetroUI) for browser↔browser transfers over
WebRTC — end-to-end encrypted (DTLS), works across the internet, no server
storage; the public PeerJS broker is used for connection setup only. Deploy
with `vercel` from `web/`, or run locally with `bun run dev`.

## License

MIT
