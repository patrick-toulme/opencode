import { createConnection, createServer, type Server, type Socket } from "node:net"
import { createHash } from "node:crypto"
import fs from "fs/promises"
import os from "os"
import path from "path"
import type { InstanceContext } from "@/project/instance"
import { Effect } from "effect"
import { SwarmMailbox } from "./mailbox"

export type PeerMessage =
  | {
      version: 1
      type: "event-log"
      originID: string
      eventID?: string
      createdAt: number
    }
  | {
      version: 1
      type: "inbox"
      originID: string
      workerID: string
      inputID?: string
      createdAt: number
    }

export type PeerMessageInput =
  | {
      type: "event-log"
      eventID?: string
    }
  | {
      type: "inbox"
      workerID: string
      inputID?: string
    }

export type PeerRecord = {
  version: 1
  ownerID: string
  pid: number
  hostname: string
  socketPath: string
  updatedAt: number
}

export type PeerHandle = {
  ownerID: string
  key: string
  socketPath?: string
}

const PEER_TTL_MS = 15_000

export const start = Effect.fn("SwarmPeer.start")(function* (
  ctx: InstanceContext,
  onMessage: (message: PeerMessage) => void,
) {
  return yield* Effect.tryPromise({
    try: async (): Promise<PeerHandle> => {
      const ownerID = SwarmMailbox.currentOwnerID()
      const key = peerKey(ctx, ownerID)
      const socketPath = socketPathForOwner(ctx, ownerID)
      if (process.platform !== "win32") await fs.rm(socketPath, { force: true }).catch(() => undefined)
      await fs.mkdir(path.dirname(socketPath), { recursive: true }).catch(() => undefined)

      const server = createServer((socket) => consumeSocket(socket, onMessage))
      server.on("error", () => {})
      await listen(server, socketPath)

      const record = currentRecord(ownerID, socketPath)
      await writePeerRecord(ctx, record)
      const interval = setInterval(() => {
        void writePeerRecord(ctx, { ...record, updatedAt: Date.now() }).catch(() => undefined)
      }, 5_000)
      interval.unref()

      const stop = async () => {
        clearInterval(interval)
        await removePeerRecord(ctx, ownerID).catch(() => undefined)
        await closeServer(server).catch(() => undefined)
        if (process.platform !== "win32") await fs.rm(socketPath, { force: true }).catch(() => undefined)
      }
      activeStops.set(key, stop)
      return { ownerID, key, socketPath }
    },
    catch: () => new Error("failed to start swarm peer transport"),
  }).pipe(
    Effect.catch(() =>
      Effect.succeed({
        ownerID: SwarmMailbox.currentOwnerID(),
        key: peerKey(ctx, SwarmMailbox.currentOwnerID()),
      } satisfies PeerHandle),
    ),
  )
})

export const stop = Effect.fn("SwarmPeer.stop")(function* (handle: PeerHandle) {
  const cleanup = activeStops.get(handle.key)
  if (!cleanup) return
  activeStops.delete(handle.key)
  yield* Effect.promise(() => cleanup()).pipe(Effect.ignore)
})

export const notifyAll = Effect.fn("SwarmPeer.notifyAll")(function* (
  ctx: InstanceContext,
  message: PeerMessageInput,
) {
  const full = buildMessage(message)
  const peers = yield* readPeers(ctx)
  yield* Effect.forEach(
    peers.filter((peer) => peer.ownerID !== SwarmMailbox.currentOwnerID()),
    (peer) => send(peer.socketPath, full),
    { concurrency: "unbounded", discard: true },
  )
})

export const notifyOwner = Effect.fn("SwarmPeer.notifyOwner")(function* (
  ctx: InstanceContext,
  ownerID: string,
  message: PeerMessageInput,
) {
  const peer = yield* readPeer(ctx, ownerID)
  if (!peer) return
  yield* send(peer.socketPath, buildMessage(message)).pipe(Effect.ignore)
})

export const registerPeer = Effect.fn("SwarmPeer.registerPeer")(function* (
  ctx: InstanceContext,
  record: Omit<PeerRecord, "version" | "updatedAt"> & { updatedAt?: number },
) {
  yield* Effect.promise(() =>
    writePeerRecord(ctx, {
      version: 1,
      ownerID: record.ownerID,
      pid: record.pid,
      hostname: record.hostname,
      socketPath: record.socketPath,
      updatedAt: record.updatedAt ?? Date.now(),
    }),
  )
})

export const socketPathForOwner = (ctx: InstanceContext, ownerID: string) => {
  const hash = createHash("sha1").update(`${ctx.project.id}:${ownerID}`).digest("hex").slice(0, 32)
  if (process.platform === "win32") return `\\\\.\\pipe\\opencode-swarm-${hash}`
  return path.join(os.tmpdir(), "opencode-swarm", `${hash}.sock`)
}

export const peerDirectory = (ctx: InstanceContext) => path.join(SwarmMailbox.pathsForContext(ctx).swarm, "peers")

export const peerRecordPath = (ctx: InstanceContext, ownerID: string) =>
  path.join(peerDirectory(ctx), `${safe(ownerID)}.json`)

const activeStops = new Map<string, () => Promise<void>>()

const peerKey = (ctx: InstanceContext, ownerID: string) => `${ctx.project.id}:${ownerID}`

const buildMessage = (message: PeerMessageInput): PeerMessage =>
  ({
    version: 1,
    originID: SwarmMailbox.currentOwnerID(),
    createdAt: Date.now(),
    ...message,
  }) as PeerMessage

const readPeers = Effect.fn("SwarmPeer.readPeers")(function* (ctx: InstanceContext) {
  const dir = peerDirectory(ctx)
  return yield* Effect.tryPromise({
    try: async () => {
      let entries: string[]
      try {
        entries = await fs.readdir(dir)
      } catch (error) {
        if (code(error) === "ENOENT") return []
        throw error
      }
      const records = await Promise.all(
        entries
          .filter((entry) => entry.endsWith(".json"))
          .map((entry) => readPeerFile(path.join(dir, entry)).catch(() => undefined)),
      )
      return records.filter((record): record is PeerRecord => Boolean(record)).filter(isPeerAlive)
    },
    catch: () => [] as PeerRecord[],
  })
})

const readPeer = Effect.fn("SwarmPeer.readPeer")(function* (ctx: InstanceContext, ownerID: string) {
  const record = yield* Effect.tryPromise({
    try: () => readPeerFile(peerRecordPath(ctx, ownerID)),
    catch: () => undefined,
  })
  return record && isPeerAlive(record) ? record : undefined
})

async function writePeerRecord(ctx: InstanceContext, record: PeerRecord) {
  const file = peerRecordPath(ctx, record.ownerID)
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify(record, null, 2), "utf-8")
}

async function removePeerRecord(ctx: InstanceContext, ownerID: string) {
  await fs.rm(peerRecordPath(ctx, ownerID), { force: true })
}

async function readPeerFile(file: string) {
  const raw = await fs.readFile(file, "utf-8")
  const parsed = JSON.parse(raw)
  return isPeerRecord(parsed) ? parsed : undefined
}

const send = (socketPath: string, message: PeerMessage) =>
  Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        const socket = createConnection(socketPath)
        const done = once((error?: Error) => {
          socket.destroy()
          error ? reject(error) : resolve()
        })
        socket.setTimeout(250, () => done(new Error("swarm peer notification timed out")))
        socket.once("error", done)
        socket.once("connect", () => {
          socket.write(`${JSON.stringify(message)}\n`, (error) => {
            if (error) return done(error)
            socket.end(() => done())
          })
        })
      }),
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  }).pipe(Effect.ignore)

function consumeSocket(socket: Socket, onMessage: (message: PeerMessage) => void) {
  let buffer = ""
  socket.setEncoding("utf-8")
  socket.on("data", (chunk) => {
    buffer += chunk
    while (true) {
      const index = buffer.indexOf("\n")
      if (index === -1) break
      const line = buffer.slice(0, index).trim()
      buffer = buffer.slice(index + 1)
      if (!line) continue
      const message = parseMessage(line)
      if (message) onMessage(message)
    }
  })
  socket.on("error", () => {})
}

function parseMessage(line: string) {
  try {
    const parsed = JSON.parse(line)
    return isPeerMessage(parsed) ? parsed : undefined
  } catch {
    return undefined
  }
}

const listen = (server: Server, socketPath: string) =>
  new Promise<void>((resolve, reject) => {
    const done = once((error?: Error) => (error ? reject(error) : resolve()))
    server.once("error", done)
    server.listen(socketPath, () => done())
  })

const closeServer = (server: Server) =>
  new Promise<void>((resolve) => {
    server.close(() => resolve())
  })

const currentRecord = (ownerID: string, socketPath: string): PeerRecord => ({
  version: 1,
  ownerID,
  pid: process.pid,
  hostname: os.hostname(),
  socketPath,
  updatedAt: Date.now(),
})

function isPeerAlive(record: PeerRecord) {
  if (Date.now() - record.updatedAt > PEER_TTL_MS) return false
  if (record.hostname !== os.hostname()) return true
  try {
    process.kill(record.pid, 0)
    return true
  } catch {
    return false
  }
}

function isPeerRecord(value: unknown): value is PeerRecord {
  if (!value || typeof value !== "object") return false
  const item = value as Partial<PeerRecord>
  return (
    item.version === 1 &&
    typeof item.ownerID === "string" &&
    typeof item.pid === "number" &&
    typeof item.hostname === "string" &&
    typeof item.socketPath === "string" &&
    typeof item.updatedAt === "number"
  )
}

function isPeerMessage(value: unknown): value is PeerMessage {
  if (!value || typeof value !== "object") return false
  const item = value as Partial<PeerMessage>
  if (item.version !== 1 || typeof item.originID !== "string" || typeof item.createdAt !== "number") return false
  if (item.type === "event-log") return item.eventID === undefined || typeof item.eventID === "string"
  if (item.type === "inbox") {
    return typeof item.workerID === "string" && (item.inputID === undefined || typeof item.inputID === "string")
  }
  return false
}

function once<T extends unknown[]>(fn: (...args: T) => void) {
  let called = false
  return (...args: T) => {
    if (called) return
    called = true
    fn(...args)
  }
}

const safe = (value: string) => {
  const cleaned = value.replaceAll(/[^a-zA-Z0-9._-]/g, "_").slice(0, 160)
  return cleaned || "default"
}

const code = (error: unknown) => {
  if (!error || typeof error !== "object" || !("code" in error)) return undefined
  const value = error.code
  return typeof value === "string" ? value : undefined
}

export * as SwarmPeer from "./peer"
