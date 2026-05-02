# lanxfer

High-throughput resumable LAN transfer CLI for macOS, Linux, and Windows.

## Features

- File and folder transfer (recursive).
- Resume via `.lanxfer.part` files.
- BLAKE3 integrity verification before finalize.
- Pairing-code auth for write operations.
- Multi-interface UDP broadcast discovery (works across macOS, Linux, Windows).
- Interactive receiver selection via `connect` command.
- Drive/destination discovery on receiver.
- Parallel file workers for better throughput on large trees.
- Live transfer speed/overall progress output.

## Commands

```bash
lanxfer serve --bind 0.0.0.0:44818 --discovery-port 44819
lanxfer discover --discovery-port 44819 --timeout-ms 1500
lanxfer connect                     # interactive discovery + selection
lanxfer connect --target 10.0.0.35  # direct connect by IP
lanxfer destinations <target-ip-or-host> --port 44818
lanxfer send <target-ip-or-host> <source-file-or-folder> <destination-dir> --port 44818 --code <PAIRCODE>
```

### `connect` command

Scans all network interfaces and discovers receivers across each subnet.

- If a single receiver is found, connects automatically and lists destinations.
- If multiple receivers are found, presents a numbered menu to pick from.
- Use `--target <ip>` to skip discovery and connect directly.

### Useful `send` options

```bash
--overwrite      # overwrite existing target files
--jobs N         # parallel workers (default adaptive)
--dry-run        # validate and plan without writing files
--no-progress    # disable periodic progress logs
```

## Typical flow (Linux Mint receiver + Mac sender)

On Linux Mint:

```bash
lanxfer serve
```

Receiver prints pairing code, e.g. `A1B2C3`.

On macOS sender:

```bash
# Quick way: auto-discover and connect
lanxfer connect

# Or the manual way:
lanxfer discover
lanxfer destinations <linux-ip>
lanxfer send <linux-ip> "/Users/brahiam/Library/Application Support/Spectrasonics" /path/on/linux --code A1B2C3 --jobs 6
```

## Build

```bash
cargo build --release
```

Binary:

```text
target/release/lanxfer
```
