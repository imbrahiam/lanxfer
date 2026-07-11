"use client";

import * as React from "react";
import type Peer from "peerjs";
import type { DataConnection } from "peerjs";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";

const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CHUNK_SIZE = 256 * 1024;
const BUFFER_HIGH = 8 * 1024 * 1024;

type Screen = "start" | "waiting" | "connected";

type Tx = {
  key: string;
  name: string;
  size: number;
  done: number;
  dir: "up" | "down";
  status: "active" | "done" | "failed";
  url?: string; // blob download URL for received files
};

type ControlMsg =
  | { t: "start"; id: string; name: string; size: number }
  | { t: "end"; id: string };

function genCode() {
  const buf = new Uint32Array(6);
  crypto.getRandomValues(buf);
  return Array.from(buf, (v) => CODE_CHARS[v % CODE_CHARS.length]).join("");
}

function fmtSize(n: number) {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(1)} GB`;
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KB`;
  return `${n} B`;
}

async function waitForDrain(conn: DataConnection) {
  const channel = (conn as unknown as { dataChannel?: RTCDataChannel }).dataChannel;
  if (!channel) return;
  while (channel.bufferedAmount > BUFFER_HIGH) {
    await new Promise((r) => setTimeout(r, 30));
  }
}

// Incoming file being assembled: either streamed straight to disk
// (File System Access API) or buffered for a blob download.
type Incoming = {
  id: string;
  name: string;
  size: number;
  received: number;
  chunks: BlobPart[];
  writable?: FileSystemWritableFileStream;
  lastUpdate: number;
};

export function Transfer() {
  const [screen, setScreen] = React.useState<Screen>("start");
  const [roomCode, setRoomCode] = React.useState("");
  const [joinCode, setJoinCode] = React.useState("");
  const [status, setStatus] = React.useState("");
  const [peerLabel, setPeerLabel] = React.useState("");
  const [txs, setTxs] = React.useState<Tx[]>([]);
  const [dragging, setDragging] = React.useState(false);
  const [saveDirName, setSaveDirName] = React.useState("");
  const fsAccess = React.useSyncExternalStore(
    () => () => {},
    () => "showDirectoryPicker" in window,
    () => false,
  );

  const peerRef = React.useRef<Peer | null>(null);
  const connRef = React.useRef<DataConnection | null>(null);
  const incomingRef = React.useRef<Incoming | null>(null);
  const saveDirRef = React.useRef<FileSystemDirectoryHandle | null>(null);
  const sendQueueRef = React.useRef<Promise<void>>(Promise.resolve());
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const folderInputRef = React.useRef<HTMLInputElement>(null);

  const updateTx = React.useCallback((key: string, patch: Partial<Tx>) => {
    setTxs((list) => list.map((t) => (t.key === key ? { ...t, ...patch } : t)));
  }, []);

  const reset = React.useCallback((message: string) => {
    connRef.current?.close();
    peerRef.current?.destroy();
    connRef.current = null;
    peerRef.current = null;
    incomingRef.current = null;
    setScreen("start");
    setStatus(message);
  }, []);

  const bindConnection = React.useCallback(
    (conn: DataConnection) => {
      connRef.current = conn;
      conn.on("open", () => {
        setPeerLabel(conn.peer.replace(/^lanxfer-/, ""));
        setScreen("connected");
        setStatus("");
      });
      conn.on("close", () => reset("peer disconnected"));
      conn.on("error", (err) => reset(`connection error: ${err.message ?? err}`));
      conn.on("data", async (data) => {
        // Binary chunk of the file currently in flight.
        if (data instanceof ArrayBuffer || data instanceof Uint8Array) {
          const inc = incomingRef.current;
          if (!inc) return;
          const buf = new Uint8Array(
            data instanceof Uint8Array
              ? data.slice().buffer
              : (data as ArrayBuffer),
          ) as Uint8Array<ArrayBuffer>;
          inc.received += buf.byteLength;
          if (inc.writable) {
            await inc.writable.write(buf);
          } else {
            inc.chunks.push(buf);
          }
          // throttle re-renders — per-chunk updates make the bar flash
          const now = performance.now();
          if (now - inc.lastUpdate > 100) {
            inc.lastUpdate = now;
            updateTx(`down-${inc.id}`, { done: inc.received });
          }
          return;
        }
        const msg = data as ControlMsg;
        if (msg.t === "start") {
          const inc: Incoming = {
            id: msg.id,
            name: msg.name,
            size: msg.size,
            received: 0,
            chunks: [],
            lastUpdate: 0,
          };
          // Stream to the chosen folder when available (creates subfolders
          // for relative paths); otherwise buffer for a blob download.
          const dir = saveDirRef.current;
          if (dir) {
            try {
              let d = dir;
              const parts = msg.name.split("/");
              for (const part of parts.slice(0, -1)) {
                d = await d.getDirectoryHandle(part, { create: true });
              }
              const fh = await d.getFileHandle(parts[parts.length - 1], {
                create: true,
              });
              inc.writable = await fh.createWritable();
            } catch {
              inc.writable = undefined;
            }
          }
          incomingRef.current = inc;
          setTxs((list) => [
            {
              key: `down-${msg.id}`,
              name: msg.name,
              size: msg.size,
              done: 0,
              dir: "down",
              status: "active",
            },
            ...list,
          ]);
        } else if (msg.t === "end") {
          const inc = incomingRef.current;
          if (!inc || inc.id !== msg.id) return;
          incomingRef.current = null;
          if (inc.writable) {
            await inc.writable.close();
            updateTx(`down-${inc.id}`, { status: "done", done: inc.size });
          } else {
            const url = URL.createObjectURL(new Blob(inc.chunks));
            updateTx(`down-${inc.id}`, { status: "done", done: inc.size, url });
            // auto-download
            const a = document.createElement("a");
            a.href = url;
            a.download = inc.name.replaceAll("/", "_");
            a.hidden = true;
            document.body.appendChild(a);
            a.click();
            a.remove();
          }
        }
      });
    },
    [reset, updateTx],
  );

  const host = React.useCallback(async () => {
    const { default: PeerCtor } = await import("peerjs");
    const code = genCode();
    setRoomCode(code);
    setScreen("waiting");
    setStatus("waiting for peer…");
    const peer = new PeerCtor(`lanxfer-${code}`);
    peerRef.current = peer;
    peer.on("connection", (conn) => bindConnection(conn));
    peer.on("error", (err) => reset(`signaling error: ${err.message ?? err}`));
  }, [bindConnection, reset]);

  const join = React.useCallback(
    async (code: string) => {
      code = code.trim().toUpperCase();
      if (code.length !== 6) {
        setStatus("code is 6 characters");
        return;
      }
      const { default: PeerCtor } = await import("peerjs");
      setScreen("waiting");
      setRoomCode(code);
      setStatus("connecting…");
      const peer = new PeerCtor();
      peerRef.current = peer;
      peer.on("open", () => {
        bindConnection(peer.connect(`lanxfer-${code}`, { reliable: true }));
      });
      peer.on("error", (err) => reset(`could not join: ${err.message ?? err}`));
    },
    [bindConnection, reset],
  );

  const sendFiles = React.useCallback(
    (files: { file: File; relPath: string }[]) => {
      const conn = connRef.current;
      if (!conn || files.length === 0) return;
      for (const { file, relPath } of files) {
        const id = crypto.randomUUID();
        setTxs((list) => [
          {
            key: `up-${id}`,
            name: relPath,
            size: file.size,
            done: 0,
            dir: "up",
            status: "active",
          },
          ...list,
        ]);
        // Sequential queue: the receiver assembles one incoming file at a time.
        sendQueueRef.current = sendQueueRef.current.then(async () => {
          try {
            conn.send({ t: "start", id, name: relPath, size: file.size });
            let offset = 0;
            let lastUpdate = 0;
            while (offset < file.size) {
              const chunk = await file
                .slice(offset, offset + CHUNK_SIZE)
                .arrayBuffer();
              await waitForDrain(conn);
              conn.send(chunk);
              offset += chunk.byteLength;
              const now = performance.now();
              if (now - lastUpdate > 100) {
                lastUpdate = now;
                updateTx(`up-${id}`, { done: offset });
              }
            }
            conn.send({ t: "end", id });
            updateTx(`up-${id}`, { status: "done", done: file.size });
          } catch {
            updateTx(`up-${id}`, { status: "failed" });
          }
        });
      }
    },
    [updateTx],
  );

  const pickSaveDir = React.useCallback(async () => {
    const picker = (
      window as unknown as {
        showDirectoryPicker?: (o?: object) => Promise<FileSystemDirectoryHandle>;
      }
    ).showDirectoryPicker;
    if (!picker) {
      setStatus("folder streaming needs Chrome/Edge — files download instead");
      return;
    }
    try {
      const dir = await picker({ mode: "readwrite" });
      saveDirRef.current = dir;
      setSaveDirName(dir.name);
    } catch {
      /* user cancelled */
    }
  }, []);

  const onInputFiles = React.useCallback(
    (list: FileList | null) => {
      if (!list) return;
      sendFiles(
        Array.from(list).map((f) => ({
          file: f,
          relPath:
            (f as File & { webkitRelativePath?: string }).webkitRelativePath ||
            f.name,
        })),
      );
    },
    [sendFiles],
  );

  const onDrop = React.useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      sendFiles(
        Array.from(e.dataTransfer.files).map((f) => ({
          file: f,
          relPath: f.name,
        })),
      );
    },
    [sendFiles],
  );

  return (
    <main className="mx-auto flex w-full max-w-3xl flex-col gap-5 px-4 py-6 pb-16">
      <Card className="border-black bg-[#ffdb33] shadow-[6px_6px_0_#111]">
        <CardHeader>
          <CardTitle className="font-head text-4xl tracking-wider sm:text-5xl">
            LANXFER
            <span className="animate-pulse">_</span>
          </CardTitle>
          <p className="font-medium">
            browser ⇄ browser file transfer · end-to-end encrypted · nothing
            stored anywhere
          </p>
        </CardHeader>
      </Card>

      {status && screen === "start" && (
        <Badge variant="outline" className="self-start border-2 border-black bg-white">
          {status}
        </Badge>
      )}

      {screen === "start" && (
        <div className="grid gap-5 sm:grid-cols-2">
          <Card className="border-black shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle className="font-head tracking-wide">
                SEND / RECEIVE
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <p>Get a room code, share it with the other machine.</p>
              <Button onClick={host} className="self-start">
                CREATE ROOM
              </Button>
            </CardContent>
          </Card>
          <Card className="border-black shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle className="font-head tracking-wide">
                JOIN A ROOM
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <p>Type the code shown on the other machine.</p>
              <form
                className="flex gap-3"
                onSubmit={(e) => {
                  e.preventDefault();
                  join(joinCode);
                }}
              >
                <Input
                  value={joinCode}
                  onChange={(e) => setJoinCode(e.target.value.toUpperCase())}
                  maxLength={6}
                  placeholder="ABC123"
                  autoCapitalize="characters"
                  spellCheck={false}
                  className="border-2 border-black font-mono text-lg tracking-[0.4em] uppercase"
                />
                <Button type="submit" variant="secondary">
                  JOIN
                </Button>
              </form>
            </CardContent>
          </Card>
        </div>
      )}

      {screen === "waiting" && (
        <Card className="border-black text-center shadow-[6px_6px_0_#111]">
          <CardHeader>
            <CardTitle className="font-head tracking-wide">ROOM CODE</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col items-center gap-4">
            <div className="select-all border-2 border-black bg-[#ff90e8] px-6 py-4 font-mono text-4xl tracking-[0.35em] shadow-[4px_4px_0_#111] sm:text-5xl">
              {roomCode}
            </div>
            <p className="font-bold">{status}</p>
            <p>On the other machine, open this page and join with the code.</p>
            <Button variant="outline" onClick={() => reset("")}>
              CANCEL
            </Button>
          </CardContent>
        </Card>
      )}

      {screen === "connected" && (
        <>
          <Card className="border-black shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle className="flex items-center justify-between font-head tracking-wide">
                <span>
                  CONNECTED{" "}
                  <Badge className="ml-2 border-2 border-black bg-[#3ddc84] text-black">
                    {peerLabel || "peer"}
                  </Badge>
                </span>
                <Button size="sm" variant="outline" onClick={() => reset("")}>
                  DISCONNECT
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              <div
                onDragOver={(e) => {
                  e.preventDefault();
                  setDragging(true);
                }}
                onDragLeave={() => setDragging(false)}
                onDrop={onDrop}
                className={`flex flex-col items-center gap-3 border-2 border-dashed border-black p-8 text-center transition-colors ${
                  dragging ? "bg-[#23d3ee]" : "bg-[#fdf6e3]"
                }`}
              >
                <p className="font-head tracking-wide">DROP FILES HERE</p>
                <p>or</p>
                <div className="flex flex-wrap justify-center gap-3">
                  <Button onClick={() => fileInputRef.current?.click()}>
                    PICK FILES
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => folderInputRef.current?.click()}
                  >
                    PICK FOLDER
                  </Button>
                </div>
                <input
                  ref={fileInputRef}
                  type="file"
                  multiple
                  hidden
                  onChange={(e) => {
                    onInputFiles(e.target.files);
                    e.target.value = "";
                  }}
                />
                <input
                  ref={folderInputRef}
                  type="file"
                  hidden
                  {...{ webkitdirectory: "" }}
                  onChange={(e) => {
                    onInputFiles(e.target.files);
                    e.target.value = "";
                  }}
                />
              </div>
              <div className="flex flex-wrap items-center gap-3">
                {fsAccess && (
                  <Button size="sm" variant="outline" onClick={pickSaveDir}>
                    SAVE TO FOLDER…
                  </Button>
                )}
                <span className="text-sm text-muted-foreground">
                  {saveDirName
                    ? `incoming files stream into “${saveDirName}”`
                    : fsAccess
                      ? "incoming files download automatically"
                      : "incoming files download automatically (folder streaming needs Chrome/Edge)"}
                </span>
              </div>
            </CardContent>
          </Card>

          <Card className="border-black shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle className="font-head tracking-wide">
                TRANSFERS
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              {txs.length === 0 && (
                <p className="text-muted-foreground">none yet</p>
              )}
              {txs.map((t) => (
                <div
                  key={t.key}
                  className="border-2 border-black bg-white p-3 shadow-[4px_4px_0_#111]"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-bold break-all">
                      {t.dir === "up" ? "↑" : "↓"} {t.name}
                    </span>
                    <span className="font-mono text-sm">
                      {t.status === "active"
                        ? `${fmtSize(t.done)} / ${fmtSize(t.size)}`
                        : t.status === "done"
                          ? `${fmtSize(t.size)} ✓`
                          : "failed ✗"}
                    </span>
                  </div>
                  {t.status === "active" && (
                    <Progress
                      className="mt-2 border-2 border-black"
                      value={t.size > 0 ? (t.done / t.size) * 100 : 0}
                    />
                  )}
                  {t.url && (
                    <a
                      className="text-sm underline"
                      href={t.url}
                      download={t.name.replaceAll("/", "_")}
                    >
                      download again
                    </a>
                  )}
                </div>
              ))}
            </CardContent>
          </Card>
        </>
      )}

      <footer className="flex flex-col gap-2 text-sm text-muted-foreground">
        <p>
          Files travel directly between the two browsers over WebRTC
          (DTLS-encrypted). The room code only brokers the connection via the
          public PeerJS server — file bytes never touch it. Codes are single-use;
          don&apos;t share them publicly.
        </p>
        <p>
          CLI version for LAN transfers at max speed:{" "}
          <code className="border border-black bg-white px-1">
            cargo install --git https://github.com/imbrahiam/lanxfer
          </code>
        </p>
      </footer>
    </main>
  );
}
