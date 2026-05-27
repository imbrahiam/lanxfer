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
| `lanxfer interactive` | Sender-only interactive session |
| `lanxfer serve` | Headless receiver |
| `lanxfer discover` | Find receivers on network |
| `lanxfer connect` | Connect to a receiver (discovery or `--target IP`) |
| `lanxfer destinations <ip>` | List drives on a receiver |
| `lanxfer send <ip> <src> <dest>` | Direct file transfer |

## Performance

lanxfer is optimized for maximum LAN throughput:

- **4 MB I/O buffers** on both sender and receiver
- **4 MB socket buffers** (SO_SNDBUF/SO_RCVBUF) to reduce kernel copies
- **TCP_NODELAY** for low-latency control messages
- **Parallel workers** saturate the link with multiple file streams
- **Raw TCP streaming** — file data goes directly on the wire, no framing overhead

On a gigabit LAN, expect 800-950 Mbps. On 300 Mbps WiFi, you'll hit the link limit.

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

## Architecture

```
Sender                          Receiver
──────                          ────────
lanxfer (interactive)           lanxfer serve
  │                               │
  ├─ UDP discovery ──────────────►├─ UDP responder (44819)
  │  (broadcast + subnet scan)    │
  │                               │
  ├─ TCP connect ────────────────►├─ TCP listener (44818)
  │  Hello/HelloAck handshake     │
  │                               │
  ├─ ListDestinations ──────────►├─ Returns drives/mounts
  ├─ BrowseDirectory ───────────►├─ Returns dir contents
  │                               │
  ├─ CreateDirectory ───────────►├─ mkdir -p
  ├─ PrepareUpload ─────────────►├─ Check resume state
  ├─ BeginUpload ───────────────►├─ Ready to receive
  ├─ [raw file bytes] ──────────►├─ Write to .lanxfer.part
  │                               ├─ Verify BLAKE3 hash
  │                               ├─ Rename to final
  ◄── TransferResult ────────────┤
```

## License

MIT
