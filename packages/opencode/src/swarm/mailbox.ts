import { InstanceState } from "@/effect/instance-state"
import { Identifier } from "@/id/id"
import type { InstanceContext } from "@/project/instance"
import type { WorkerInput, WorkerSnapshot } from "@/swarm/state"
import { Flock } from "@opencode-ai/core/util/flock"
import { Effect } from "effect"
import { mkdirSync, watch } from "fs"
import fs from "fs/promises"
import os from "os"
import path from "path"

type InboxItem = WorkerInput & {
  read: boolean
  readAt?: number
}

type WorkerHeartbeat = {
  version: 1
  workerID: string
  ownerID: string
  pid: number
  hostname: string
  parentSessionID: string
  sessionID: string
  name?: string
  team?: string
  status: WorkerSnapshot["status"]
  updatedAt: number
}

export type EventLogEntry = {
  version: 1
  id: string
  type: string
  properties: unknown
  originID: string
  createdAt: number
}

export type EventLogRead = {
  offset: number
  events: EventLogEntry[]
}

const OWNER_ID = `${os.hostname()}:${process.pid}:${Identifier.create("swo", "ascending")}`
const HEARTBEAT_TTL_MS = 15_000
const LOCK_OPTIONS = {
  staleMs: 30_000,
  timeoutMs: 10_000,
  baseDelayMs: 10,
  maxDelayMs: 250,
} as const

export type WorkerLiveness = {
  alive: boolean
  ownedByCurrent: boolean
  heartbeat?: WorkerHeartbeat
}

export const currentOwnerID = () => OWNER_ID

export const paths = Effect.fn("SwarmMailbox.paths")(function* () {
  const ctx = yield* InstanceState.context
  return pathsForContext(ctx)
})

export const pathsForContext = (ctx: InstanceContext) => {
  const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
  const swarm = path.join(root, ".opencode", "swarm")
  return {
    root,
    swarm,
    teams: path.join(swarm, "teams"),
    workers: path.join(swarm, "workers"),
  }
}

export const inboxPath = (ctx: InstanceContext, team: string, recipient: string) =>
  path.join(pathsForContext(ctx).teams, safe(team), "inboxes", `${safe(recipient)}.json`)

export const inboxPathForWorker = (ctx: InstanceContext, worker: WorkerSnapshot) => {
  const address = mailboxAddress(worker)
  return inboxPath(ctx, address.team, address.recipient)
}

export const workerHeartbeatPath = (ctx: InstanceContext, workerID: string) =>
  path.join(pathsForContext(ctx).workers, `${safe(workerID)}.json`)

export const eventLogPath = (ctx: InstanceContext) => path.join(pathsForContext(ctx).swarm, "events.jsonl")

export const watchEventLog = Effect.fn("SwarmMailbox.watchEventLog")(function* (
  onChange: () => void,
  ctx?: InstanceContext,
) {
  const resolved = ctx ?? (yield* InstanceState.context)
  return yield* watchPath(eventLogPath(resolved), onChange)
})

export const watchInbox = Effect.fn("SwarmMailbox.watchInbox")(function* (
  worker: WorkerSnapshot,
  onChange: () => void,
  ctx?: InstanceContext,
) {
  const resolved = ctx ?? (yield* InstanceState.context)
  return yield* watchPath(inboxPathForWorker(resolved, worker), onChange)
})

export const registerWorker = Effect.fn("SwarmMailbox.registerWorker")(function* (worker: WorkerSnapshot) {
  const ctx = yield* InstanceState.context
  const address = mailboxAddress(worker)
  yield* ensureInbox(inboxPath(ctx, address.team, address.recipient))
  yield* touchWorkerInContext(ctx, worker)
})

export const touchWorker = Effect.fn("SwarmMailbox.touchWorker")(function* (worker: WorkerSnapshot) {
  const ctx = yield* InstanceState.context
  yield* touchWorkerInContext(ctx, worker)
})

export const clearWorker = Effect.fn("SwarmMailbox.clearWorker")(function* (workerID: string, ctx?: InstanceContext) {
  const resolved = ctx ?? (yield* InstanceState.context)
  const file = workerHeartbeatPath(resolved, workerID)
  yield* withLock(`swarm-worker:${file}`, async () => {
    const existing = await readHeartbeatFile(file)
    if (!existing || existing.ownerID !== OWNER_ID) return
    await fs.rm(file, { force: true })
  }).pipe(Effect.ignore)
})

export const inspectWorker = Effect.fn("SwarmMailbox.inspectWorker")(function* (
  workerID: string,
  ctx?: InstanceContext,
) {
  const resolved = ctx ?? (yield* InstanceState.context)
  const file = workerHeartbeatPath(resolved, workerID)
  return yield* withLock(`swarm-worker:${file}`, async (): Promise<WorkerLiveness> => {
    const heartbeat = await readHeartbeatFile(file)
    if (!heartbeat) return { alive: false, ownedByCurrent: false }
    const alive = Date.now() - heartbeat.updatedAt <= HEARTBEAT_TTL_MS
    return {
      alive,
      ownedByCurrent: heartbeat.ownerID === OWNER_ID,
      heartbeat,
    }
  }).pipe(Effect.catch(() => Effect.succeed({ alive: false, ownedByCurrent: false })))
})

export const writeInput = Effect.fn("SwarmMailbox.writeInput")(function* (
  worker: WorkerSnapshot,
  input: Pick<WorkerInput, "message" | "summary" | "from">,
) {
  const ctx = yield* InstanceState.context
  const address = mailboxAddress(worker)
  const file = inboxPath(ctx, address.team, address.recipient)
  const queued: WorkerInput = {
    id: Identifier.create("swi", "ascending"),
    message: input.message,
    createdAt: Date.now(),
    ...(input.summary ? { summary: input.summary } : {}),
    ...(input.from ? { from: input.from } : {}),
  }

  yield* withLock(`swarm-inbox:${file}`, async () => {
    await ensureInboxFile(file)
    const items = await readInboxFile(file)
    items.push({ ...queued, read: false })
    await writeInboxFile(file, items)
  })

  return queued
})

export const takeInput = Effect.fn("SwarmMailbox.takeInput")(function* (worker: WorkerSnapshot) {
  const ctx = yield* InstanceState.context
  const address = mailboxAddress(worker)
  const file = inboxPath(ctx, address.team, address.recipient)

  return yield* withLock(`swarm-inbox:${file}`, async (): Promise<WorkerInput | undefined> => {
    const items = await readInboxFile(file)
    const index = selectInboxIndex(items)
    if (index === -1) return undefined
    const item = items[index]!
    items[index] = { ...item, read: true, readAt: Date.now() }
    await writeInboxFile(file, items)
    const { read: _read, readAt: _readAt, ...input } = item
    return input
  }).pipe(Effect.catch(() => Effect.succeed(undefined)))
})

export const eventLogOffset = Effect.fn("SwarmMailbox.eventLogOffset")(function* (ctx?: InstanceContext) {
  const resolved = ctx ?? (yield* InstanceState.context)
  const file = eventLogPath(resolved)
  return yield* Effect.tryPromise({
    try: async () => {
      try {
        return (await fs.stat(file)).size
      } catch (error) {
        if (code(error) === "ENOENT") return 0
        throw error
      }
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  }).pipe(Effect.catch(() => Effect.succeed(0)))
})

export const appendEvent = Effect.fn("SwarmMailbox.appendEvent")(function* (
  input: Pick<EventLogEntry, "id" | "type" | "properties"> & {
    originID?: string
    createdAt?: number
  },
  ctx?: InstanceContext,
) {
  const resolved = ctx ?? (yield* InstanceState.context)
  const file = eventLogPath(resolved)
  const entry: EventLogEntry = {
    version: 1,
    id: input.id,
    type: input.type,
    properties: input.properties,
    originID: input.originID ?? OWNER_ID,
    createdAt: input.createdAt ?? Date.now(),
  }
  yield* withLock(`swarm-events:${file}`, async () => {
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.appendFile(file, `${JSON.stringify(entry)}\n`, "utf-8")
  })
  return entry
})

export const readEvents = Effect.fn("SwarmMailbox.readEvents")(function* (offset: number, ctx?: InstanceContext) {
  const resolved = ctx ?? (yield* InstanceState.context)
  const file = eventLogPath(resolved)
  return yield* withLock(`swarm-events:${file}`, async (): Promise<EventLogRead> => {
    let buffer: Buffer
    try {
      buffer = await fs.readFile(file)
    } catch (error) {
      if (code(error) === "ENOENT") return { offset: 0, events: [] }
      throw error
    }
    const start = offset <= buffer.length ? Math.max(0, offset) : 0
    const raw = buffer.subarray(start).toString("utf-8")
    const events = raw
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .map((line) => parseEventLogEntry(line))
      .filter((entry): entry is EventLogEntry => Boolean(entry))
    return { offset: buffer.length, events }
  }).pipe(Effect.catch(() => Effect.succeed({ offset, events: [] })))
})

const touchWorkerInContext = (ctx: InstanceContext, worker: WorkerSnapshot) => {
  const file = workerHeartbeatPath(ctx, worker.spec.workerID)
  const heartbeat: WorkerHeartbeat = {
    version: 1,
    workerID: worker.spec.workerID,
    ownerID: OWNER_ID,
    pid: process.pid,
    hostname: os.hostname(),
    parentSessionID: worker.spec.parentSessionID,
    sessionID: worker.spec.sessionID,
    ...(worker.spec.name ? { name: worker.spec.name } : {}),
    ...(worker.spec.team ? { team: worker.spec.team } : {}),
    status: worker.status,
    updatedAt: Date.now(),
  }

  return withLock(`swarm-worker:${file}`, async () => {
    await fs.mkdir(path.dirname(file), { recursive: true })
    await fs.writeFile(file, JSON.stringify(heartbeat, null, 2), "utf-8")
  })
}

const ensureInbox = (file: string) =>
  withLock(`swarm-inbox:${file}`, async () => {
    await ensureInboxFile(file)
  }).pipe(Effect.ignore)

const ensureInboxFile = async (file: string) => {
  await fs.mkdir(path.dirname(file), { recursive: true })
  try {
    await fs.writeFile(file, "[]", { encoding: "utf-8", flag: "wx" })
  } catch (error) {
    if (code(error) !== "EEXIST") throw error
  }
}

const readInboxFile = async (file: string): Promise<InboxItem[]> => {
  try {
    const raw = await fs.readFile(file, "utf-8")
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(isInboxItem)
  } catch (error) {
    if (code(error) === "ENOENT") return []
    throw error
  }
}

const writeInboxFile = async (file: string, items: InboxItem[]) => {
  await fs.mkdir(path.dirname(file), { recursive: true })
  await fs.writeFile(file, JSON.stringify(items, null, 2), "utf-8")
}

const readHeartbeatFile = async (file: string): Promise<WorkerHeartbeat | undefined> => {
  try {
    const raw = await fs.readFile(file, "utf-8")
    const parsed = JSON.parse(raw)
    return isHeartbeat(parsed) ? parsed : undefined
  } catch (error) {
    if (code(error) === "ENOENT") return undefined
    throw error
  }
}

const withLock = <A>(key: string, body: () => Promise<A>) =>
  Effect.tryPromise({
    try: () => Flock.withLock(key, body, LOCK_OPTIONS),
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  })

const watchPath = (file: string, onChange: () => void) =>
  Effect.sync(() => {
    const dir = path.dirname(file)
    const target = path.basename(file)
    mkdirSync(dir, { recursive: true })
    const watcher = watch(dir, { persistent: false }, (_event, filename) => {
      if (filename && String(filename) !== target) return
      onChange()
    })
    watcher.on("error", () => {})
    return () => watcher.close()
  }).pipe(Effect.catch(() => Effect.succeed(() => {})))

const mailboxAddress = (worker: WorkerSnapshot) => ({
  team: worker.spec.team ?? worker.spec.parentSessionID,
  recipient: worker.spec.name ?? worker.spec.workerID,
})

const selectInboxIndex = (items: InboxItem[]) => {
  const unread = items
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => !item.read)
  if (unread.length === 0) return -1

  const shutdown = unread.find(({ item }) => isShutdownRequest(item))
  if (shutdown) return shutdown.index

  const lead = unread.find(({ item }) => item.from === "team-lead")
  if (lead) return lead.index

  return unread[0]!.index
}

const isShutdownRequest = (input: WorkerInput) =>
  input.message.includes("<type>shutdown_request</type>") || input.message.includes('"type":"shutdown_request"')

const isInboxItem = (value: unknown): value is InboxItem => {
  if (!value || typeof value !== "object") return false
  const item = value as Partial<InboxItem>
  return (
    typeof item.id === "string" &&
    typeof item.message === "string" &&
    typeof item.createdAt === "number" &&
    typeof item.read === "boolean"
  )
}

const isHeartbeat = (value: unknown): value is WorkerHeartbeat => {
  if (!value || typeof value !== "object") return false
  const heartbeat = value as Partial<WorkerHeartbeat>
  return (
    heartbeat.version === 1 &&
    typeof heartbeat.workerID === "string" &&
    typeof heartbeat.ownerID === "string" &&
    typeof heartbeat.pid === "number" &&
    typeof heartbeat.hostname === "string" &&
    typeof heartbeat.parentSessionID === "string" &&
    typeof heartbeat.sessionID === "string" &&
    typeof heartbeat.status === "string" &&
    typeof heartbeat.updatedAt === "number"
  )
}

const parseEventLogEntry = (line: string) => {
  try {
    const parsed = JSON.parse(line)
    return isEventLogEntry(parsed) ? parsed : undefined
  } catch {
    return undefined
  }
}

const isEventLogEntry = (value: unknown): value is EventLogEntry => {
  if (!value || typeof value !== "object") return false
  const entry = value as Partial<EventLogEntry>
  return (
    entry.version === 1 &&
    typeof entry.id === "string" &&
    typeof entry.type === "string" &&
    typeof entry.originID === "string" &&
    typeof entry.createdAt === "number" &&
    "properties" in entry
  )
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

export * as SwarmMailbox from "./mailbox"
