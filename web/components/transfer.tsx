"use client";

import * as React from "react";
import type Peer from "peerjs";
import type { DataConnection } from "peerjs";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  CODE_LENGTH,
  MAX_BUFFERED_FILE_SIZE,
  MAX_FILE_SIZE,
  cleanRoomCode,
  fmtSize,
  genCode,
  isValidRoomCode,
  numberedFileName,
  safeRelativeName,
} from "@/lib/transfer-protocol";

const CHUNK_SIZE = 256 * 1024;
const BUFFER_HIGH = 8 * 1024 * 1024;
const BUFFER_DRAIN_TIMEOUT_MS = 30_000;
const CONNECTION_TIMEOUT_MS = 20_000;
const READY_TIMEOUT_MS = 30_000;
const RECEIPT_TIMEOUT_MS = 120_000;
const MAX_FILES_PER_SELECTION = 1_000;

type Screen = "start" | "waiting" | "connected";

type Tx = {
  key: string;
  name: string;
  size: number;
  done: number;
  dir: "up" | "down";
  status: "active" | "done" | "failed";
  url?: string; // blob download URL for received files
  error?: string;
};

type ControlMsg =
  | { t: "start"; id: string; name: string; size: number }
  | { t: "ready"; id: string }
  | { t: "end"; id: string }
  | { t: "complete"; id: string }
  | { t: "cancel"; id: string }
  | { t: "reject"; id: string; reason: string };

async function waitForDrain(conn: DataConnection) {
  const channel = (conn as unknown as { dataChannel?: RTCDataChannel }).dataChannel;
  if (!channel) return;
  const deadline = performance.now() + BUFFER_DRAIN_TIMEOUT_MS;
  while (channel.bufferedAmount > BUFFER_HIGH) {
    if (!conn.open) throw new Error("connection closed while sending");
    if (performance.now() >= deadline) {
      throw new Error("peer stopped accepting file data");
    }
    await new Promise((r) => setTimeout(r, 30));
  }
}

async function getUniqueFileHandle(
  dir: FileSystemDirectoryHandle,
  requestedName: string,
) {
  for (let index = 0; index < 10_000; index += 1) {
    const candidate = numberedFileName(requestedName, index);
    try {
      await dir.getFileHandle(candidate);
    } catch (error) {
      if (error instanceof DOMException && error.name === "NotFoundError") {
        return {
          handle: await dir.getFileHandle(candidate, { create: true }),
          name: candidate,
        };
      }
      throw error;
    }
  }
  throw new Error("too many files have the same name");
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

type ReceiptWaiter = {
  expected: "ready" | "complete";
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
};

export function Transfer() {
  const [screen, setScreen] = React.useState<Screen>("start");
  const [mode, setMode] = React.useState<"host" | "join" | null>(null);
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
  const receiveQueueRef = React.useRef<Promise<void>>(Promise.resolve());
  const receiptWaitersRef = React.useRef(new Map<string, ReceiptWaiter>());
  const downloadUrlsRef = React.useRef(new Set<string>());
  const connectionTimeoutRef = React.useRef<ReturnType<typeof setTimeout> | null>(
    null,
  );
  const resettingRef = React.useRef(false);
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const folderInputRef = React.useRef<HTMLInputElement>(null);

  const updateTx = React.useCallback((key: string, patch: Partial<Tx>) => {
    setTxs((list) => list.map((t) => (t.key === key ? { ...t, ...patch } : t)));
  }, []);

  const clearConnectionTimeout = React.useCallback(() => {
    if (connectionTimeoutRef.current) {
      clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
  }, []);

  const reset = React.useCallback((message: string) => {
    if (resettingRef.current) return;
    resettingRef.current = true;
    clearConnectionTimeout();
    const incoming = incomingRef.current;
    if (incoming?.writable) {
      void incoming.writable.abort().catch(() => {});
    }
    const connection = connRef.current;
    const peer = peerRef.current;
    connRef.current = null;
    peerRef.current = null;
    incomingRef.current = null;
    connection?.close();
    peer?.destroy();
    for (const waiter of receiptWaitersRef.current.values()) {
      clearTimeout(waiter.timeout);
      waiter.reject(new Error(message || "connection closed"));
    }
    receiptWaitersRef.current.clear();
    for (const url of downloadUrlsRef.current) URL.revokeObjectURL(url);
    downloadUrlsRef.current.clear();
    sendQueueRef.current = Promise.resolve();
    receiveQueueRef.current = Promise.resolve();
    setTxs([]);
    setScreen("start");
    setMode(null);
    setRoomCode("");
    setPeerLabel("");
    setStatus(message);
    queueMicrotask(() => {
      resettingRef.current = false;
    });
  }, [clearConnectionTimeout]);

  const waitForPeerSignal = React.useCallback(
    (
      id: string,
      expected: ReceiptWaiter["expected"],
      timeoutMs: number,
    ) =>
      new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          receiptWaitersRef.current.delete(id);
          reject(
            new Error(
              expected === "ready"
                ? "the receiver did not accept the file"
                : "the receiver did not confirm the file",
            ),
          );
        }, timeoutMs);
        receiptWaitersRef.current.set(id, {
          expected,
          resolve,
          reject,
          timeout,
        });
      }),
    [],
  );

  const hasActiveTransfers = txs.some((transfer) => transfer.status === "active");
  React.useEffect(() => {
    if (!hasActiveTransfers) return;
    const preventAccidentalClose = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", preventAccidentalClose);
    return () =>
      window.removeEventListener("beforeunload", preventAccidentalClose);
  }, [hasActiveTransfers]);

  React.useEffect(() => {
    let cancelled = false;
    const receiptWaiters = receiptWaitersRef.current;
    const downloadUrls = downloadUrlsRef.current;
    const invitedCode = cleanRoomCode(
      new URLSearchParams(window.location.search).get("room") ?? "",
    );
    if (isValidRoomCode(invitedCode)) {
      queueMicrotask(() => {
        if (cancelled) return;
        setJoinCode(invitedCode);
        setStatus("Room code added from invite. Tap JOIN when ready.");
      });
    }

    return () => {
      cancelled = true;
      clearConnectionTimeout();
      const connection = connRef.current;
      const peer = peerRef.current;
      connRef.current = null;
      peerRef.current = null;
      connection?.close();
      peer?.destroy();
      const incoming = incomingRef.current;
      if (incoming?.writable) void incoming.writable.abort().catch(() => {});
      for (const waiter of receiptWaiters.values()) {
        clearTimeout(waiter.timeout);
        waiter.reject(new Error("page closed"));
      }
      receiptWaiters.clear();
      for (const url of downloadUrls) URL.revokeObjectURL(url);
      downloadUrls.clear();
    };
  }, [clearConnectionTimeout]);

  const bindConnection = React.useCallback(
    (conn: DataConnection) => {
      if (connRef.current && connRef.current !== conn) {
        conn.close();
        return;
      }
      connRef.current = conn;
      conn.on("open", () => {
        if (connRef.current !== conn) return;
        clearConnectionTimeout();
        setPeerLabel(conn.peer.replace(/^lanxfer-/, ""));
        setScreen("connected");
        setStatus("");
      });
      conn.on("close", () => {
        if (connRef.current === conn) reset("Peer disconnected.");
      });
      conn.on("error", (err) => {
        if (connRef.current === conn) {
          reset(`Connection error: ${err.message ?? err}`);
        }
      });
      conn.on("data", (data) => {
        if (connRef.current !== conn) return;
        receiveQueueRef.current = receiveQueueRef.current
          .then(async () => {
            // Serialize event handling: EventEmitter does not await async
            // listeners, and concurrent writes can otherwise reorder chunks.
            if (
              data instanceof ArrayBuffer ||
              data instanceof Blob ||
              ArrayBuffer.isView(data)
            ) {
              const inc = incomingRef.current;
              if (!inc) throw new Error("received file bytes before a start message");
              let buf: Uint8Array<ArrayBuffer>;
              if (data instanceof Blob) {
                buf = new Uint8Array(await data.arrayBuffer());
              } else if (data instanceof ArrayBuffer) {
                buf = new Uint8Array(data);
              } else {
                buf = new Uint8Array(
                  data.buffer,
                  data.byteOffset,
                  data.byteLength,
                ).slice();
              }
              if (inc.received + buf.byteLength > inc.size) {
                if (inc.writable) await inc.writable.abort();
                incomingRef.current = null;
                updateTx(`down-${inc.id}`, {
                  status: "failed",
                  error: "Peer sent more data than declared.",
                });
                throw new Error("peer sent more bytes than declared");
              }
              if (inc.writable) {
                await inc.writable.write(buf);
              } else {
                inc.chunks.push(buf);
              }
              inc.received += buf.byteLength;
              const now = performance.now();
              if (now - inc.lastUpdate > 100) {
                inc.lastUpdate = now;
                updateTx(`down-${inc.id}`, { done: inc.received });
              }
              return;
            }

            if (!data || typeof data !== "object" || !("t" in data)) {
              throw new Error("peer sent an invalid control message");
            }
            const msg = data as ControlMsg;
            if (
              msg.t === "ready" ||
              msg.t === "complete" ||
              msg.t === "reject"
            ) {
              if (typeof msg.id !== "string" || msg.id.length > 128) {
                throw new Error("peer sent an invalid transfer response");
              }
              const waiter = receiptWaitersRef.current.get(msg.id);
              if (!waiter) return;
              clearTimeout(waiter.timeout);
              receiptWaitersRef.current.delete(msg.id);
              if (msg.t === "reject") {
                const reason =
                  typeof msg.reason === "string" && msg.reason.length <= 256
                    ? msg.reason
                    : "receiver rejected the file";
                waiter.reject(new Error(reason));
              } else if (waiter.expected !== msg.t) {
                waiter.reject(new Error(`unexpected ${msg.t} response`));
              } else {
                waiter.resolve();
              }
            } else if (msg.t === "cancel") {
              if (typeof msg.id !== "string" || msg.id.length > 128) {
                throw new Error("peer sent an invalid cancellation");
              }
              const inc = incomingRef.current;
              if (!inc || inc.id !== msg.id) return;
              if (inc.writable) await inc.writable.abort();
              incomingRef.current = null;
              updateTx(`down-${inc.id}`, {
                status: "failed",
                error: "Sender cancelled the transfer.",
              });
            } else if (msg.t === "start") {
              if (incomingRef.current) {
                throw new Error("peer started a second file before finishing the first");
              }
              if (typeof msg.id !== "string" || msg.id.length > 128) {
                throw new Error("peer sent an invalid file identifier");
              }
              const name = safeRelativeName(msg.name);
              if (
                !name ||
                !Number.isSafeInteger(msg.size) ||
                msg.size < 0 ||
                msg.size > MAX_FILE_SIZE
              ) {
                conn.send({
                  t: "reject",
                  id: msg.id,
                  reason: "File name or size is not supported.",
                } satisfies ControlMsg);
                return;
              }
              if (!saveDirRef.current && msg.size > MAX_BUFFERED_FILE_SIZE) {
                setStatus(
                  `Incoming ${fmtSize(msg.size)} file rejected. This browser needs a save folder for files over ${fmtSize(MAX_BUFFERED_FILE_SIZE)}.`,
                );
                conn.send({
                  t: "reject",
                  id: msg.id,
                  reason:
                    "File is too large for browser memory. Choose a save folder first.",
                } satisfies ControlMsg);
                return;
              }
              const inc: Incoming = {
                id: msg.id,
                name,
                size: msg.size,
                received: 0,
                chunks: [],
                lastUpdate: 0,
              };
              const dir = saveDirRef.current;
              if (dir) {
                try {
                  let d = dir;
                  const parts = name.split("/");
                  for (const part of parts.slice(0, -1)) {
                    d = await d.getDirectoryHandle(part, { create: true });
                  }
                  const uniqueFile = await getUniqueFileHandle(
                    d,
                    parts[parts.length - 1],
                  );
                  inc.name = [...parts.slice(0, -1), uniqueFile.name].join("/");
                  inc.writable = await uniqueFile.handle.createWritable();
                } catch {
                  inc.writable = undefined;
                }
              }
              if (!inc.writable && inc.size > MAX_BUFFERED_FILE_SIZE) {
                setStatus(
                  "Incoming file rejected because the selected folder could not be opened.",
                );
                conn.send({
                  t: "reject",
                  id: msg.id,
                  reason: "Could not open the selected save folder.",
                } satisfies ControlMsg);
                return;
              }
              incomingRef.current = inc;
              setTxs((list) => [
                {
                  key: `down-${msg.id}`,
                  name: inc.name,
                  size: msg.size,
                  done: 0,
                  dir: "down",
                  status: "active",
                },
                ...list,
              ]);
              conn.send({ t: "ready", id: msg.id } satisfies ControlMsg);
            } else if (msg.t === "end") {
              const inc = incomingRef.current;
              if (!inc || inc.id !== msg.id || inc.received !== inc.size) {
                if (inc?.writable) await inc.writable.abort();
                incomingRef.current = null;
                if (inc) {
                  updateTx(`down-${inc.id}`, {
                    status: "failed",
                    error: "File ended before all bytes arrived.",
                  });
                }
                throw new Error("file ended before the declared size was received");
              }
              incomingRef.current = null;
              if (inc.writable) {
                await inc.writable.close();
                updateTx(`down-${inc.id}`, { status: "done", done: inc.size });
              } else {
                const url = URL.createObjectURL(new Blob(inc.chunks));
                downloadUrlsRef.current.add(url);
                updateTx(`down-${inc.id}`, { status: "done", done: inc.size, url });
                const a = document.createElement("a");
                a.href = url;
                a.download = inc.name.replaceAll("/", "_");
                a.hidden = true;
                document.body.appendChild(a);
                a.click();
                a.remove();
              }
              conn.send({ t: "complete", id: inc.id } satisfies ControlMsg);
            } else {
              throw new Error("peer sent an unsupported control message");
            }
          })
          .catch((error: unknown) => {
            reset(
              `Transfer rejected: ${error instanceof Error ? error.message : String(error)}`,
            );
          });
      });
    },
    [clearConnectionTimeout, reset, updateTx],
  );

  const host = React.useCallback(async () => {
    try {
      const { default: PeerCtor } = await import("peerjs");
      const code = genCode();
      setMode("host");
      setRoomCode(code);
      setScreen("waiting");
      setStatus("Creating room…");
      const peer = new PeerCtor(`lanxfer-${code}`);
      peerRef.current = peer;
      peer.on("open", () => {
        if (peerRef.current === peer) setStatus("Waiting for peer…");
      });
      peer.on("connection", (conn) => {
        if (peerRef.current === peer) bindConnection(conn);
        else conn.close();
      });
      peer.on("error", (err) => {
        if (peerRef.current === peer) {
          reset(`Signaling error: ${err.message ?? err}`);
        }
      });
    } catch (error) {
      reset(
        `Could not create room: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }, [bindConnection, reset]);

  const join = React.useCallback(
    async (code: string) => {
      code = cleanRoomCode(code);
      setJoinCode(code);
      if (!isValidRoomCode(code)) {
        setStatus(`Enter the complete ${CODE_LENGTH}-character room code.`);
        return;
      }
      try {
        const { default: PeerCtor } = await import("peerjs");
        setMode("join");
        setScreen("waiting");
        setRoomCode(code);
        setStatus("Connecting…");
        const peer = new PeerCtor();
        peerRef.current = peer;
        clearConnectionTimeout();
        connectionTimeoutRef.current = setTimeout(() => {
          if (peerRef.current === peer) {
            reset(
              "Connection timed out. Check the code and try another network if WebRTC is blocked.",
            );
          }
        }, CONNECTION_TIMEOUT_MS);
        peer.on("open", () => {
          if (peerRef.current !== peer) return;
          bindConnection(
            peer.connect(`lanxfer-${code}`, {
              reliable: true,
              serialization: "binary",
            }),
          );
        });
        peer.on("error", (err) => {
          if (peerRef.current === peer) {
            reset(`Could not join: ${err.message ?? err}`);
          }
        });
      } catch (error) {
        reset(
          `Could not join: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    },
    [bindConnection, clearConnectionTimeout, reset],
  );

  const sendFiles = React.useCallback(
    (files: { file: File; relPath: string }[]) => {
      const conn = connRef.current;
      if (!conn?.open || files.length === 0) {
        if (files.length > 0) setStatus("Connect to a peer before sending files.");
        return;
      }
      for (const { file, relPath } of files) {
        const id = crypto.randomUUID();
        const safeName = safeRelativeName(relPath);
        if (!safeName || file.size > MAX_FILE_SIZE) {
          setTxs((list) => [
            {
              key: `up-${id}`,
              name: relPath,
              size: file.size,
              done: 0,
              dir: "up",
              status: "failed",
              error:
                file.size > MAX_FILE_SIZE
                  ? "File exceeds the 64 GB safety limit."
                  : "File name is not supported.",
            },
            ...list,
          ]);
          continue;
        }
        setTxs((list) => [
          {
            key: `up-${id}`,
            name: safeName,
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
            if (connRef.current !== conn || !conn.open) {
              throw new Error("connection is no longer open");
            }
            conn.send({
              t: "start",
              id,
              name: safeName,
              size: file.size,
            } satisfies ControlMsg);
            await waitForPeerSignal(id, "ready", READY_TIMEOUT_MS);
            let offset = 0;
            let lastUpdate = 0;
            while (offset < file.size) {
              if (connRef.current !== conn || !conn.open) {
                throw new Error("connection closed while sending");
              }
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
            conn.send({ t: "end", id } satisfies ControlMsg);
            updateTx(`up-${id}`, { done: file.size });
            await waitForPeerSignal(id, "complete", RECEIPT_TIMEOUT_MS);
            updateTx(`up-${id}`, { status: "done", done: file.size });
          } catch (error) {
            const waiter = receiptWaitersRef.current.get(id);
            if (waiter) {
              clearTimeout(waiter.timeout);
              receiptWaitersRef.current.delete(id);
            }
            if (connRef.current === conn && conn.open) {
              conn.send({ t: "cancel", id } satisfies ControlMsg);
            }
            updateTx(`up-${id}`, {
              status: "failed",
              error: error instanceof Error ? error.message : "Transfer failed.",
            });
          }
        });
      }
    },
    [updateTx, waitForPeerSignal],
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
      const files = Array.from(list);
      if (files.length > MAX_FILES_PER_SELECTION) {
        setStatus(
          `Only the first ${MAX_FILES_PER_SELECTION.toLocaleString()} files were queued.`,
        );
      }
      sendFiles(
        files.slice(0, MAX_FILES_PER_SELECTION).map((f) => ({
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
      const files = Array.from(e.dataTransfer.files);
      if (files.length > MAX_FILES_PER_SELECTION) {
        setStatus(
          `Only the first ${MAX_FILES_PER_SELECTION.toLocaleString()} files were queued.`,
        );
      }
      sendFiles(
        files.slice(0, MAX_FILES_PER_SELECTION).map((f) => ({
          file: f,
          relPath: f.name,
        })),
      );
    },
    [sendFiles],
  );

  const copyRoomCode = React.useCallback(async () => {
    try {
      await navigator.clipboard.writeText(roomCode);
      setStatus("Room code copied.");
    } catch {
      setStatus("Could not copy automatically. Select and copy the code.");
    }
  }, [roomCode]);

  const shareRoom = React.useCallback(async () => {
    const inviteUrl = new URL(window.location.href);
    inviteUrl.search = "";
    inviteUrl.hash = "";
    inviteUrl.searchParams.set("room", roomCode);
    const shareData = {
      title: "Join my LANXFER room",
      text: `Join my LANXFER room with code ${roomCode}`,
      url: inviteUrl.toString(),
    };
    try {
      if (navigator.share) {
        await navigator.share(shareData);
        setStatus("Room invite shared.");
      } else {
        await navigator.clipboard.writeText(inviteUrl.toString());
        setStatus("Invite link copied.");
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      setStatus("Could not share automatically. Copy the room code instead.");
    }
  }, [roomCode]);

  return (
    <main className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-3 py-4 pb-[calc(env(safe-area-inset-bottom)+2rem)] sm:gap-5 sm:px-4 sm:py-6">
      <Card className="border-black bg-[#ffdb33] shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
        <CardHeader>
          <CardTitle
            role="heading"
            aria-level={1}
            className="font-head text-3xl tracking-wider sm:text-5xl"
          >
            LANXFER
            <span aria-hidden="true" className="animate-pulse">
              _
            </span>
          </CardTitle>
          <p className="text-sm font-medium sm:text-base">
            browser ⇄ browser file transfer · WebRTC encrypted in transit ·
            no server storage
          </p>
        </CardHeader>
      </Card>

      {status && screen === "start" && (
        <Badge
          role="status"
          aria-live="polite"
          variant="outline"
          className="max-w-full self-start whitespace-normal border-2 border-black bg-white px-3 py-2 text-left"
        >
          {status}
        </Badge>
      )}

      {screen === "start" && (
        <div className="grid gap-5 sm:grid-cols-2">
          <Card className="border-black shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle
                role="heading"
                aria-level={2}
                className="font-head tracking-wide"
              >
                SEND / RECEIVE
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <p>Get a room code, share it with the other machine.</p>
              <Button type="button" onClick={host} className="w-full sm:w-fit">
                CREATE ROOM
              </Button>
            </CardContent>
          </Card>
          <Card className="border-black shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle
                role="heading"
                aria-level={2}
                className="font-head tracking-wide"
              >
                JOIN A ROOM
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <p>Type the code shown on the other machine.</p>
              <form
                className="grid gap-3 min-[420px]:grid-cols-[1fr_auto]"
                onSubmit={(e) => {
                  e.preventDefault();
                  join(joinCode);
                }}
              >
                <label htmlFor="room-code" className="sr-only">
                  Eight-character room code
                </label>
                <Input
                  id="room-code"
                  value={joinCode}
                  onChange={(e) => setJoinCode(cleanRoomCode(e.target.value))}
                  maxLength={CODE_LENGTH}
                  placeholder="ABCD2345"
                  autoCapitalize="characters"
                  autoComplete="off"
                  autoCorrect="off"
                  inputMode="text"
                  spellCheck={false}
                  aria-describedby="room-code-hint"
                  className="h-11 border-2 border-black font-mono text-lg tracking-[0.25em] uppercase sm:tracking-[0.35em]"
                />
                <Button type="submit" variant="secondary" className="w-full">
                  JOIN
                </Button>
              </form>
              <p id="room-code-hint" className="text-xs text-muted-foreground">
                Letters and numbers only; ambiguous characters are omitted.
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {screen === "waiting" && (
        <Card className="border-black text-center shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
          <CardHeader>
            <CardTitle
              role="heading"
              aria-level={2}
              className="font-head tracking-wide"
            >
              {mode === "host" ? "ROOM READY" : "JOINING ROOM"}
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col items-center gap-4">
            <output
              aria-label={`Room code ${roomCode}`}
              className="w-full max-w-full select-all overflow-hidden border-2 border-black bg-[#ff90e8] px-2 py-4 font-mono text-2xl tracking-[0.16em] shadow-[4px_4px_0_#111] min-[390px]:text-3xl min-[390px]:tracking-[0.22em] sm:w-auto sm:px-6 sm:text-5xl sm:tracking-[0.35em]"
            >
              {roomCode}
            </output>
            <p role="status" aria-live="polite" className="font-bold">
              {status}
            </p>
            {mode === "host" ? (
              <>
                <p>
                  On the other device, open this page and join with the code.
                </p>
                <div className="grid w-full gap-3 min-[420px]:grid-cols-2 sm:w-auto">
                  <Button type="button" variant="secondary" onClick={copyRoomCode}>
                    COPY CODE
                  </Button>
                  <Button type="button" variant="outline" onClick={shareRoom}>
                    SHARE INVITE
                  </Button>
                </div>
              </>
            ) : (
              <p>Keep this page open while the peer connection is negotiated.</p>
            )}
            <Button
              type="button"
              variant="outline"
              className="w-full min-[420px]:w-auto"
              onClick={() => reset("")}
            >
              CANCEL
            </Button>
          </CardContent>
        </Card>
      )}

      {screen === "connected" && (
        <>
          <Card className="border-black shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle
                role="heading"
                aria-level={2}
                className="flex flex-col items-start gap-3 font-head tracking-wide min-[480px]:flex-row min-[480px]:items-center min-[480px]:justify-between"
              >
                <span className="max-w-full">
                  CONNECTED{" "}
                  <Badge className="max-w-full border-2 border-black bg-[#3ddc84] text-black min-[480px]:ml-2">
                    {peerLabel || "peer"}
                  </Badge>
                </span>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="w-full min-[480px]:w-auto"
                  onClick={() => reset("")}
                >
                  DISCONNECT
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-4">
              {status && (
                <Badge
                  role="status"
                  aria-live="polite"
                  variant="outline"
                  className="max-w-full self-start whitespace-normal border-2 border-black bg-white px-3 py-2 text-left"
                >
                  {status}
                </Badge>
              )}
              <div
                role="region"
                aria-label="File drop and selection area"
                onDragOver={(e) => {
                  e.preventDefault();
                  setDragging(true);
                }}
                onDragLeave={() => setDragging(false)}
                onDrop={onDrop}
                className={`flex flex-col items-center gap-3 border-2 border-dashed border-black p-5 text-center transition-colors sm:p-8 ${
                  dragging ? "bg-[#23d3ee]" : "bg-[#fdf6e3]"
                }`}
              >
                <p className="font-head tracking-wide">
                  DROP FILES HERE OR CHOOSE
                </p>
                <div className="flex flex-wrap justify-center gap-3">
                  <Button
                    type="button"
                    className="grow"
                    onClick={() => fileInputRef.current?.click()}
                  >
                    PICK FILES
                  </Button>
                  <Button
                    type="button"
                    variant="secondary"
                    className="grow"
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
                  multiple
                  {...{ webkitdirectory: "" }}
                  onChange={(e) => {
                    onInputFiles(e.target.files);
                    e.target.value = "";
                  }}
                />
              </div>
              <div className="flex flex-wrap items-center gap-3">
                {fsAccess && (
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="w-full min-[480px]:w-auto"
                    onClick={pickSaveDir}
                  >
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

          <Card className="border-black shadow-[4px_4px_0_#111] sm:shadow-[6px_6px_0_#111]">
            <CardHeader>
              <CardTitle
                role="heading"
                aria-level={2}
                className="font-head tracking-wide"
              >
                TRANSFERS
              </CardTitle>
            </CardHeader>
            <CardContent
              aria-live="polite"
              aria-relevant="additions"
              className="flex flex-col gap-3"
            >
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
                      aria-label={`${t.dir === "up" ? "Sending" : "Receiving"} ${t.name}`}
                      className="mt-2 border-2 border-black"
                      value={t.size > 0 ? (t.done / t.size) * 100 : 0}
                    />
                  )}
                  {t.error && (
                    <p role="alert" className="mt-2 text-sm font-medium text-red-700">
                      {t.error}
                    </p>
                  )}
                  {t.url && (
                    <a
                      className="mt-2 inline-flex min-h-11 items-center text-sm font-bold underline underline-offset-4"
                      href={t.url}
                      download={t.name.replaceAll("/", "_")}
                    >
                      SAVE / DOWNLOAD AGAIN
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
          Connections use WebRTC transport encryption. PeerJS brokers signaling;
          when a direct route is unavailable, WebRTC may use an encrypted TURN
          relay. LANXFER does not store file contents. Keep room codes private.
        </p>
        <p>
          CLI version for LAN transfers at max speed:
          <code className="mt-1 block overflow-x-auto whitespace-nowrap border border-black bg-white p-2 text-foreground">
            cargo install --git https://github.com/imbrahiam/lanxfer
          </code>
        </p>
        <a
          href="https://github.com/imbrahiam/lanxfer"
          className="min-h-11 self-start py-3 font-bold text-foreground underline underline-offset-4"
        >
          View source and report issues on GitHub
        </a>
      </footer>
    </main>
  );
}
