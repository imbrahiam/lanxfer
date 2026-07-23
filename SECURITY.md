# Security policy

## Supported versions

Security fixes are provided for the latest release. Protocol v5 is not
compatible with older protocol versions; upgrade both peers together.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository. Do
not open a public issue for an unpatched vulnerability. Include the affected
version, platform, reproduction steps, impact, and any suggested mitigation.

You should receive an acknowledgement within 72 hours. Please allow time for a
fix and coordinated release before publishing details.

## Deployment guidance

- Use native transfers only on a trusted LAN or trusted VPN. Pairing
  authenticates requests but native TCP file contents are not encrypted.
- Do not use `--open` on shared, guest, public, or otherwise untrusted
  networks.
- Run the receiver as an unprivileged user.
- Treat the `lanxfer web` private URL like a temporary password. Stop the
  process when sharing is complete.
- Keep both peers on the same current protocol version.

The built-in browser share confines access beneath the preopened share
directory, rejects traversal through symlinks, limits concurrent connections
and upload size, and times out idle requests. Received native files are not
installed until both peers agree on their BLAKE3 hash.
