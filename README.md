# lanxfer

Fast, resumable LAN file transfer CLI with interactive mode. Built for moving large files between machines on the same network at maximum speed.

## Features

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

## Quick Start

### 1. Start the receiver

On the machine that will **receive** files:

```bash
lanxfer serve
```

Output:
```
lanxfer receiver listening on 0.0.0.0:44818 (discovery udp 44819)
device: mypc linux x86_64
pairing code: A1B2C3
```

### 2. Send files (interactive)

On the machine that will **send** files, just run:

```bash
lanxfer
```

This launches interactive mode:

1. **Discovers receivers** on the network (broadcast + subnet scan fallback)
2. **Prompts for pairing code** shown on the receiver
3. **Main menu**: Send files, List drives, Exit
4. **Select destination drive** on the remote machine
5. **Browse remote directories** to pick where files land
6. **Browse local filesystem** — start from current dir, home, desktop, root, or any path
7. **Select files/folders** to send (multi-select with space bar)
8. **Transfers** with progress bars and BLAKE3 verification

### 3. Or use direct commands

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
| `lanxfer` | Interactive mode (default) |
| `lanxfer interactive` | Same as above, explicit |
| `lanxfer serve` | Start receiver server |
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
