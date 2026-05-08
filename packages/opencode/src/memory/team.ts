import path from "path"
import { createHash } from "crypto"
import fs, { lstat, realpath } from "fs/promises"
import { Global } from "@opencode-ai/core/global"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Config } from "@/config/config"
import { InstanceState } from "@/effect/instance-state"
import type { InstanceContext } from "@/project/instance"
import { Context, Effect, Layer, Schema } from "effect"

const ENTRYPOINT = "MEMORY.md"
const MAX_ENTRYPOINT_LINES = 200
const MAX_ENTRYPOINT_BYTES = 25_000
const MAX_SYNC_FILE_BYTES = 250_000
const MAX_SYNC_PUT_BODY_BYTES = 200_000

export class PathTraversalError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "PathTraversalError"
  }
}

export class SecretDetectedError extends Schema.TaggedErrorClass<SecretDetectedError>()("TeamMemorySecretDetected", {
  filepath: Schema.optional(Schema.String),
  matches: Schema.Array(Schema.String),
}) {
  override get message() {
    const target = this.filepath ? ` in ${this.filepath}` : ""
    return `Refusing to write likely secret material${target}: ${this.matches.join(", ")}`
  }
}

export type TeamMemoryPaths = {
  directory: string
  entrypoint: string
  projectID: string
}

export type SecretScanResult = {
  safe: boolean
  matches: string[]
}

export type TeamMemorySyncResult = {
  enabled: boolean
  pulled: number
  pushed: number
  skipped: Array<{ key: string; reason: string }>
}

type TeamMemoryRemoteData = {
  entries: Record<string, string>
  entryChecksums: Record<string, string>
}

export interface Interface {
  readonly paths: () => Effect.Effect<TeamMemoryPaths>
  readonly prompt: () => Effect.Effect<string | undefined>
  readonly resolveKey: (key: string) => Effect.Effect<string, PathTraversalError>
  readonly sync: () => Effect.Effect<TeamMemorySyncResult>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/TeamMemory") {}

export const layer: Layer.Layer<Service, never, AppFileSystem.Service | Config.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service
    const config = yield* Config.Service

    const paths: Interface["paths"] = Effect.fn("TeamMemory.paths")(function* () {
      const ctx = yield* InstanceState.context
      return pathsForContext(ctx)
    })

    const resolveKey: Interface["resolveKey"] = Effect.fn("TeamMemory.resolveKey")(function* (key) {
      const ctx = yield* InstanceState.context
      return yield* validateKeyInContext(ctx, key)
    })

    const sync: Interface["sync"] = Effect.fn("TeamMemory.sync")(function* () {
      const cfg = yield* config.get()
      const endpoint = syncEndpoint(cfg)
      if (!endpoint) return { enabled: false, pulled: 0, pushed: 0, skipped: [] } satisfies TeamMemorySyncResult
      const ctx = yield* InstanceState.context
      return yield* syncForContext(ctx, endpoint)
    })

    const prompt: Interface["prompt"] = Effect.fn("TeamMemory.prompt")(function* () {
      const p = yield* paths()
      const cfg = yield* config.get()
      const endpoint = syncEndpoint(cfg)
      if (endpoint) yield* syncForContext(yield* InstanceState.context, endpoint).pipe(Effect.ignore)
      const raw = yield* fs.readFileStringSafe(p.entrypoint).pipe(Effect.catch(() => Effect.succeed(undefined)))
      const enabled =
        cfg.experimental?.team_memory === true || process.env.OPENCODE_TEAM_MEMORY === "1" || Boolean(endpoint)
      if (!enabled && !raw?.trim()) return undefined

      yield* fs.ensureDir(p.directory).pipe(Effect.ignore)
      const entrypoint = raw?.trim() ? truncateEntrypoint(raw) : undefined
      return buildPrompt(p, entrypoint)
    })

    return Service.of({ paths, prompt, resolveKey, sync })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(AppFileSystem.defaultLayer), Layer.provide(Config.defaultLayer))

export function pathsForContext(ctx: InstanceContext): TeamMemoryPaths {
  const projectID = sanitizeSegment(ctx.project.id)
  const directory = path.join(Global.Path.data, "team-memory", projectID)
  return {
    directory,
    entrypoint: path.join(directory, ENTRYPOINT),
    projectID,
  }
}

export function isTeamMemoryPathForContext(ctx: InstanceContext, filepath: string) {
  const directory = pathsForContext(ctx).directory
  const resolved = path.resolve(filepath)
  return resolved === directory || resolved.startsWith(directory + path.sep)
}

export const assertSafeContentForPath = Effect.fn("TeamMemory.assertSafeContentForPath")(function* (
  filepath: string,
  content: string,
) {
  const ctx = yield* InstanceState.context
  if (!isTeamMemoryPathForContext(ctx, filepath)) return
  const scan = scanForSecrets(content)
  if (!scan.safe) return yield* Effect.fail(new SecretDetectedError({ filepath, matches: scan.matches }))
})

export function scanForSecrets(content: string): SecretScanResult {
  const patterns: Array<[string, RegExp]> = [
    ["private key", /-----BEGIN [A-Z ]*PRIVATE KEY-----/i],
    ["AWS access key", /\bAKIA[0-9A-Z]{16}\b/g],
    ["GitHub token", /\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b/g],
    ["GitHub fine-grained token", /\bgithub_pat_[A-Za-z0-9_]{20,}\b/g],
    ["Slack token", /\bxox[baprs]-[A-Za-z0-9-]{20,}\b/g],
    ["OpenAI API key", /\bsk-[A-Za-z0-9_-]{32,}\b/g],
    ["Anthropic API key", /\bsk-ant-[A-Za-z0-9_-]{20,}\b/g],
    ["named API key", /\b(?:OPENAI|ANTHROPIC|AWS|GITHUB|SLACK|API)_?(?:API_)?KEY\s*=\s*['"]?[^'"\s]{12,}/gi],
  ]
  const matches = patterns.filter(([, pattern]) => pattern.test(content)).map(([name]) => name)
  return {
    safe: matches.length === 0,
    matches,
  }
}

export const syncForContext = Effect.fn("TeamMemory.syncForContext")(function* (
  ctx: InstanceContext,
  endpoint: string,
) {
  const skipped: TeamMemorySyncResult["skipped"] = []
  const paths = pathsForContext(ctx)
  const remote = yield* fetchRemoteTeamMemory(endpoint, paths.projectID)
  let pulled = 0

  yield* Effect.promise(() => fs.mkdir(paths.directory, { recursive: true }))
  for (const [key, content] of Object.entries(remote.entries)) {
    const target = yield* validateKeyInContext(ctx, key).pipe(
      Effect.catch((error) => {
        skipped.push({ key, reason: error.message })
        return Effect.succeed(undefined)
      }),
    )
    if (!target) continue
    if (byteLength(content) > MAX_SYNC_FILE_BYTES) {
      skipped.push({ key, reason: "remote entry is too large" })
      continue
    }
    const scan = scanForSecrets(content)
    if (!scan.safe) {
      skipped.push({ key, reason: `remote entry contains likely secret: ${scan.matches.join(", ")}` })
      continue
    }
    yield* Effect.promise(async () => {
      await fs.mkdir(path.dirname(target), { recursive: true })
      await fs.writeFile(target, content, "utf-8")
    })
    pulled++
  }

  const local = yield* readLocalEntries(ctx, skipped)
  const changed = Object.fromEntries(
    Object.entries(local.entries).filter(([key, content]) => local.entryChecksums[key] !== remote.entryChecksums[key]),
  )
  const pushed = yield* pushRemoteTeamMemory(endpoint, paths.projectID, changed, skipped)
  return {
    enabled: true,
    pulled,
    pushed,
    skipped,
  } satisfies TeamMemorySyncResult
})

export function hashContent(content: string) {
  return `sha256:${createHash("sha256").update(content, "utf8").digest("hex")}`
}

const readLocalEntries = Effect.fn("TeamMemory.readLocalEntries")(function* (
  ctx: InstanceContext,
  skipped?: TeamMemorySyncResult["skipped"],
) {
  const paths = pathsForContext(ctx)
  const entries: Record<string, string> = {}
  const entryChecksums: Record<string, string> = {}

  const files = yield* collectMarkdownFiles(paths.directory).pipe(Effect.catch(() => Effect.succeed([])))
  for (const file of files) {
    const key = path.relative(paths.directory, file).split(path.sep).join("/")
    yield* validateKeyInContext(ctx, key).pipe(
      Effect.catch(() => Effect.succeed(undefined)),
      Effect.flatMap((target) =>
        target
          ? Effect.tryPromise({
              try: async () => {
                const stat = await fs.stat(target)
                if (stat.size > MAX_SYNC_FILE_BYTES) {
                  skipped?.push({ key, reason: "local entry is too large" })
                  return
                }
                const content = await fs.readFile(target, "utf-8")
                const scan = scanForSecrets(content)
                if (!scan.safe) {
                  skipped?.push({ key, reason: `local entry contains likely secret: ${scan.matches.join(", ")}` })
                  return
                }
                entries[key] = content
                entryChecksums[key] = hashContent(content)
              },
              catch: () => new Error("failed to read local team memory entry"),
            }).pipe(Effect.ignore)
          : Effect.void,
      ),
    )
  }
  return { entries, entryChecksums } satisfies TeamMemoryRemoteData
})

const fetchRemoteTeamMemory = Effect.fn("TeamMemory.fetchRemoteTeamMemory")(function* (
  endpoint: string,
  projectID: string,
) {
  return yield* Effect.tryPromise({
    try: async () => {
      const response = await fetch(syncURL(endpoint, projectID), {
        method: "GET",
        headers: { accept: "application/json" },
      })
      if (response.status === 404) return emptyRemoteData()
      if (!response.ok) throw new Error(`team memory sync fetch failed: HTTP ${response.status}`)
      return normalizeRemoteData(await response.json())
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  }).pipe(Effect.catch(() => Effect.succeed(emptyRemoteData())))
})

const pushRemoteTeamMemory = Effect.fn("TeamMemory.pushRemoteTeamMemory")(function* (
  endpoint: string,
  projectID: string,
  entries: Record<string, string>,
  skipped: TeamMemorySyncResult["skipped"],
) {
  const pending = Object.entries(entries).toSorted(([a], [b]) => a.localeCompare(b))
  let pushed = 0
  let batch: Record<string, string> = {}

  const flush = Effect.fn("TeamMemory.pushRemoteTeamMemory.flush")(function* () {
    const keys = Object.keys(batch)
    if (keys.length === 0) return
    const body = JSON.stringify({
      entries: batch,
      entryChecksums: Object.fromEntries(Object.entries(batch).map(([key, content]) => [key, hashContent(content)])),
    })
    yield* Effect.tryPromise({
      try: async () => {
        const response = await fetch(syncURL(endpoint, projectID), {
          method: "PUT",
          headers: {
            accept: "application/json",
            "content-type": "application/json",
          },
          body,
        })
        if (!response.ok) throw new Error(`team memory sync push failed: HTTP ${response.status}`)
      },
      catch: () => new Error("team memory sync push failed"),
    }).pipe(Effect.ignore)
    pushed += keys.length
    batch = {}
  })

  for (const [key, content] of pending) {
    if (byteLength(content) > MAX_SYNC_FILE_BYTES) {
      skipped.push({ key, reason: "local entry is too large" })
      continue
    }
    const scan = scanForSecrets(content)
    if (!scan.safe) {
      skipped.push({ key, reason: `local entry contains likely secret: ${scan.matches.join(", ")}` })
      continue
    }

    const candidate = { ...batch, [key]: content }
    if (byteLength(JSON.stringify({ entries: candidate })) > MAX_SYNC_PUT_BODY_BYTES) {
      yield* flush()
    }
    const single = JSON.stringify({ entries: { [key]: content } })
    if (byteLength(single) > MAX_SYNC_PUT_BODY_BYTES) {
      skipped.push({ key, reason: "local entry exceeds sync request size" })
      continue
    }
    batch[key] = content
  }
  yield* flush()
  return pushed
})

const collectMarkdownFiles = Effect.fn("TeamMemory.collectMarkdownFiles")((directory: string) =>
  Effect.tryPromise({
    try: async () => {
      const result: string[] = []
      async function walk(current: string) {
        let entries: Array<{ name: string; isDirectory: () => boolean; isFile: () => boolean; isSymbolicLink: () => boolean }>
        try {
          entries = await fs.readdir(current, { withFileTypes: true })
        } catch (error) {
          if (errno(error) === "ENOENT") return
          throw error
        }
        for (const entry of entries) {
          if (entry.isSymbolicLink()) continue
          const next = path.join(current, entry.name)
          if (entry.isDirectory()) {
            await walk(next)
            continue
          }
          if (entry.isFile() && entry.name.endsWith(".md")) result.push(next)
        }
      }
      await walk(directory)
      return result
    },
    catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
  }),
)

function syncEndpoint(cfg: { experimental?: { team_memory_sync_url?: string } }) {
  const env = process.env.OPENCODE_TEAM_MEMORY_SYNC_URL?.trim()
  if (env) return env
  const configured = cfg.experimental?.team_memory_sync_url?.trim()
  return configured || undefined
}

function syncURL(endpoint: string, projectID: string) {
  const url = new URL(endpoint)
  url.searchParams.set("project", projectID)
  return url
}

function normalizeRemoteData(value: unknown): TeamMemoryRemoteData {
  const source = isRecord(value) ? value : {}
  const rawEntries = isRecord(source.entries) ? source.entries : source
  const entries: Record<string, string> = {}
  for (const [key, content] of Object.entries(rawEntries)) {
    if (typeof key === "string" && typeof content === "string") entries[key] = content
  }

  const entryChecksums: Record<string, string> = {}
  const rawChecksums = isRecord(source.entryChecksums) ? source.entryChecksums : {}
  for (const [key, checksum] of Object.entries(rawChecksums)) {
    if (typeof checksum === "string") entryChecksums[key] = checksum
  }
  for (const [key, content] of Object.entries(entries)) {
    entryChecksums[key] = entryChecksums[key] ?? hashContent(content)
  }
  return { entries, entryChecksums }
}

function emptyRemoteData(): TeamMemoryRemoteData {
  return { entries: {}, entryChecksums: {} }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value)
}

function byteLength(content: string) {
  return Buffer.byteLength(content, "utf-8")
}

export const validateKeyInContext = Effect.fn("TeamMemory.validateKeyInContext")(function* (
  ctx: InstanceContext,
  key: string,
) {
  const safeKey = yield* sanitizePathKey(key)
  if (!safeKey.endsWith(".md")) {
    return yield* Effect.fail(new PathTraversalError(`Team memory key must be a markdown file: "${key}"`))
  }

  const base = pathsForContext(ctx).directory
  const candidate = path.resolve(base, safeKey)
  if (!(candidate === base || candidate.startsWith(base + path.sep))) {
    return yield* Effect.fail(new PathTraversalError(`Path escapes team memory directory: "${key}"`))
  }

  const realCandidate = yield* realpathDeepestExisting(candidate)
  const inside = yield* isRealPathWithinDirectory(base, realCandidate)
  if (!inside) {
    return yield* Effect.fail(new PathTraversalError(`Path escapes team memory directory through symlinks: "${key}"`))
  }
  return candidate
})

const sanitizePathKey = Effect.fn("TeamMemory.sanitizePathKey")(function* (key: string) {
  if (!key.trim()) return yield* Effect.fail(new PathTraversalError("Empty team memory key"))
  if (key.includes("\0")) {
    return yield* Effect.fail(new PathTraversalError(`Null byte in team memory key: "${key}"`))
  }
  if (key.includes("\\")) {
    return yield* Effect.fail(new PathTraversalError(`Backslash in team memory key: "${key}"`))
  }
  if (key.startsWith("/")) return yield* Effect.fail(new PathTraversalError(`Absolute team memory key: "${key}"`))

  let decoded = key
  try {
    decoded = decodeURIComponent(key)
  } catch {
    decoded = key
  }
  if (decoded !== key && (decoded.includes("..") || decoded.includes("/") || decoded.includes("\\"))) {
    return yield* Effect.fail(new PathTraversalError(`URL-encoded traversal in team memory key: "${key}"`))
  }

  const normalized = key.normalize("NFKC")
  if (
    normalized !== key &&
    (normalized.includes("..") || normalized.includes("/") || normalized.includes("\\") || normalized.includes("\0"))
  ) {
    return yield* Effect.fail(new PathTraversalError(`Unicode-normalized traversal in team memory key: "${key}"`))
  }

  const parts = key.split("/")
  if (parts.some((part) => part === "" || part === "." || part === "..")) {
    return yield* Effect.fail(new PathTraversalError(`Unsafe team memory key: "${key}"`))
  }
  return key
})

const realpathDeepestExisting = Effect.fn("TeamMemory.realpathDeepestExisting")((absolutePath: string) =>
  Effect.tryPromise({
    try: async () => {
      const tail: string[] = []
      let current = absolutePath
      while (true) {
        try {
          const realCurrent = await realpath(current)
          return tail.length === 0 ? realCurrent : path.join(realCurrent, ...tail.reverse())
        } catch (error) {
          const code = errno(error)
          if (code === "ENOENT") {
            const isDanglingSymlink = await lstat(current)
              .then((stat) => stat.isSymbolicLink())
              .catch(() => false)
            if (isDanglingSymlink) throw new PathTraversalError(`Dangling symlink detected: "${current}"`)
          } else if (code === "ELOOP") {
            throw new PathTraversalError(`Symlink loop detected: "${current}"`)
          } else if (code !== "ENOTDIR" && code !== "ENAMETOOLONG") {
            throw new PathTraversalError(`Cannot verify path containment (${code}): "${current}"`)
          }

          const parent = path.dirname(current)
          if (parent === current) return absolutePath
          tail.push(path.basename(current))
          current = parent
        }
      }
    },
    catch: (cause) =>
      cause instanceof PathTraversalError
        ? cause
        : new PathTraversalError(`Cannot verify path containment (${errno(cause)}): "${absolutePath}"`),
  }),
)

const isRealPathWithinDirectory = Effect.fn("TeamMemory.isRealPathWithinDirectory")(
  (directory: string, candidate: string) =>
    Effect.tryPromise({
      try: async () => {
        try {
          const realDirectory = await realpath(directory)
          return candidate === realDirectory || candidate.startsWith(realDirectory + path.sep)
        } catch (error) {
          const code = errno(error)
          if (code === "ENOENT" || code === "ENOTDIR") return true
          throw new PathTraversalError(`Cannot verify team memory directory (${code}): "${directory}"`)
        }
      },
      catch: (cause) =>
        cause instanceof PathTraversalError
          ? cause
          : new PathTraversalError(`Cannot verify team memory directory (${errno(cause)}): "${directory}"`),
    }),
)

function truncateEntrypoint(raw: string) {
  const trimmed = raw.trim()
  const lines = trimmed.split("\n")
  const lineTruncated = lines.length > MAX_ENTRYPOINT_LINES
  const byteTruncated = trimmed.length > MAX_ENTRYPOINT_BYTES
  if (!lineTruncated && !byteTruncated) return trimmed

  let content = lineTruncated ? lines.slice(0, MAX_ENTRYPOINT_LINES).join("\n") : trimmed
  if (content.length > MAX_ENTRYPOINT_BYTES) {
    const cutAt = content.lastIndexOf("\n", MAX_ENTRYPOINT_BYTES)
    content = content.slice(0, cutAt > 0 ? cutAt : MAX_ENTRYPOINT_BYTES)
  }
  return `${content}\n\n> WARNING: ${ENTRYPOINT} was truncated before loading. Keep the index concise and move detail into topic files.`
}

function buildPrompt(paths: TeamMemoryPaths, entrypoint: string | undefined) {
  return [
    "# Shared Team Memory",
    "",
    "This project has persistent file-based team memory shared by the lead agent and all subagents for this repository.",
    `Directory: ${paths.directory}`,
    `Entrypoint: ${paths.entrypoint}`,
    "",
    "Use team memory for durable project knowledge that should improve future agentic work in this repository.",
    "Do not use team memory for transient task state, private user facts, credentials, tokens, secrets, or raw transcripts.",
    "",
    "When saving team memory:",
    "- Save one durable fact or convention per markdown file.",
    "- Keep MEMORY.md as a concise index of links or short reminders.",
    "- Update or remove stale memories instead of duplicating them.",
    "- Never save secrets, credentials, API keys, private keys, or access tokens.",
    "",
    `<team-memory path="${xmlEscape(paths.entrypoint)}">`,
    entrypoint ?? "No team MEMORY.md entries have been saved yet.",
    "</team-memory>",
  ].join("\n")
}

function sanitizeSegment(value: string) {
  return value.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "project"
}

function errno(error: unknown) {
  return error instanceof Error && "code" in error && typeof error.code === "string" ? error.code : "UNKNOWN"
}

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export * as TeamMemory from "./team"
