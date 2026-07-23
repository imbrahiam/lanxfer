# LANXFER Web

The hosted browser-to-browser interface for
[LANXFER](https://github.com/imbrahiam/lanxfer). It transfers files over an
encrypted WebRTC data channel without accounts or application file storage.

## Development

Requires Bun and a current Node.js runtime:

```bash
bun install --frozen-lockfile
bun run dev
```

Open <http://localhost:3000>. Before submitting a change, run:

```bash
bun run test
bun run lint
bun run build
```

## Transfer behavior

- A host creates an eight-character room code and shares it with one peer.
- PeerJS provides signaling. WebRTC attempts a direct route and may fall back
  to an encrypted TURN relay.
- Files are sent sequentially with bounded data-channel backpressure.
- The receiver validates paths and sizes, avoids overwriting existing files,
  and acknowledges each completed file before the sender reports success.
- Chromium browsers can stream into a selected folder. Other browsers use
  bounded in-memory downloads and expose a manual save link.

The production project is `helix-0639ccaf/lanxfer` on Vercel, served at
<https://lanxfer.vercel.app>. Link the local directory explicitly before a
manual deployment:

```bash
vercel link --yes --project lanxfer --scope helix-0639ccaf
vercel --prod
```

Do not create or deploy a second `lanxfer` project in another Vercel scope.
