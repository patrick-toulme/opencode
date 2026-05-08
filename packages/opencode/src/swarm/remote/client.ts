import fs from "fs/promises"
import path from "path"
import { Effect } from "effect"
import type { WorkerInput, WorkerSpec } from "../state"

export type RemoteWorkerLaunchRequest = {
  version: 1
  worker: WorkerSpec
  cwd: string
  prompt: string
  contextMessages?: unknown[]
}

export type RemoteWorkerLaunchResponse = {
  remoteID: string
  sessionURL?: string
  outputPath?: string
  cursor?: string
}

export type RemoteWorkerEvent =
  | { type: "progress"; message: string }
  | { type: "output"; text: string }
  | { type: "completed"; text: string }
  | { type: "failed"; error: string }
  | { type: "cancelled"; text?: string }

export type RemoteWorkerPollResponse = {
  events: RemoteWorkerEvent[]
  cursor?: string
}

export type RemoteWorkerClientOptions = {
  endpoint: string
  token?: string
}

export const launchRemoteWorker = Effect.fn("SwarmRemote.launch")(function* (
  options: RemoteWorkerClientOptions,
  request: RemoteWorkerLaunchRequest,
) {
  const body = yield* requestJSON(options, "/workers", {
    method: "POST",
    body: JSON.stringify(request),
  })
  const remoteID = pickString(body, "remoteID", "remote_id", "id")
  if (!remoteID) return yield* Effect.fail(new Error("Remote subagent launch response did not include remoteID"))
  const sessionURL = pickString(body, "sessionURL", "session_url", "url")
  const outputPath = pickString(body, "outputPath", "output_path")
  const cursor = pickString(body, "cursor", "nextCursor", "next_cursor")
  return {
    remoteID,
    ...(sessionURL ? { sessionURL } : {}),
    ...(outputPath ? { outputPath } : {}),
    ...(cursor ? { cursor } : {}),
  } satisfies RemoteWorkerLaunchResponse
})

export const pollRemoteWorker = Effect.fn("SwarmRemote.poll")(function* (
  options: RemoteWorkerClientOptions,
  remoteID: string,
  cursor?: string,
) {
  const query = cursor === undefined ? "" : `?cursor=${encodeURIComponent(cursor)}`
  const body = yield* requestJSON(options, `/workers/${encodeURIComponent(remoteID)}/events${query}`, {
    method: "GET",
  })
  const rawEvents = Array.isArray(body) ? body : isRecord(body) && Array.isArray(body.events) ? body.events : []
  const nextCursor = pickString(body, "cursor", "nextCursor", "next_cursor")
  return {
    events: rawEvents
      .map((event: unknown) => normalizeEvent(event))
      .filter((event): event is RemoteWorkerEvent => Boolean(event)),
    ...(nextCursor ? { cursor: nextCursor } : {}),
  } satisfies RemoteWorkerPollResponse
})

export const sendRemoteWorkerInput = Effect.fn("SwarmRemote.sendInput")(function* (
  options: RemoteWorkerClientOptions,
  remoteID: string,
  input: WorkerInput,
) {
  yield* requestJSON(options, `/workers/${encodeURIComponent(remoteID)}/message`, {
    method: "POST",
    body: JSON.stringify({ version: 1, input }),
  })
})

export const cancelRemoteWorker = Effect.fn("SwarmRemote.cancel")(function* (
  options: RemoteWorkerClientOptions,
  remoteID: string,
) {
  yield* requestJSON(options, `/workers/${encodeURIComponent(remoteID)}/cancel`, {
    method: "POST",
    body: JSON.stringify({ version: 1 }),
  })
})

export const appendRemoteWorkerOutput = Effect.fn("SwarmRemote.appendOutput")(function* (
  outputPath: string,
  workerID: string,
  remoteID: string,
  event: RemoteWorkerEvent | { type: "launch"; sessionURL?: string; outputPath?: string },
) {
  const line = JSON.stringify({
    type: "remote-subagent-event",
    workerID,
    remoteID,
    event,
    time: Date.now(),
  })
  yield* Effect.promise(async () => {
    await fs.mkdir(path.dirname(outputPath), { recursive: true })
    await fs.appendFile(outputPath, `${line}\n`, "utf8")
  })
})

function normalizeEvent(input: unknown): RemoteWorkerEvent | undefined {
  if (!isRecord(input)) return undefined
  const type = typeof input.type === "string" ? input.type : undefined
  switch (type) {
    case "progress": {
      const message = pickString(input, "message", "summary", "text")
      return message ? { type, message } : undefined
    }
    case "output": {
      const text = pickString(input, "text", "message")
      return text ? { type, text } : undefined
    }
    case "completed": {
      const text = pickString(input, "text", "result", "message")
      return { type, text: text ?? "" }
    }
    case "failed": {
      const error = pickString(input, "error", "message")
      return { type, error: error ?? "Remote subagent failed" }
    }
    case "cancelled": {
      const text = pickString(input, "text", "message", "reason")
      return { type, ...(text ? { text } : {}) }
    }
    default:
      return undefined
  }
}

function requestJSON(options: RemoteWorkerClientOptions, route: string, init: RequestInit) {
  return Effect.tryPromise({
    try: async () => {
      const response = await fetch(url(options.endpoint, route), {
        ...init,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
          ...(init.headers ?? {}),
        },
      })
      if (!response.ok) {
        const text = await response.text().catch(() => "")
        throw new Error(`Remote subagent request failed (${response.status}): ${text || response.statusText}`)
      }
      if (response.status === 204) return {}
      return (await response.json().catch(() => ({}))) as unknown
    },
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  })
}

function url(endpoint: string, route: string) {
  const base = endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint
  return `${base}${route}`
}

function pickString(input: unknown, ...keys: string[]) {
  if (!isRecord(input)) return undefined
  for (const key of keys) {
    const value = input[key]
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

function isRecord(input: unknown): input is Record<string, unknown> {
  return typeof input === "object" && input !== null
}
