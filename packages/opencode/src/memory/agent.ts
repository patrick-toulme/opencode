import path from "path"
import { Global } from "@opencode-ai/core/global"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { InstanceState } from "@/effect/instance-state"
import type { Agent } from "@/agent/agent"
import type { InstanceContext } from "@/project/instance"
import { Context, Effect, Layer, Schema } from "effect"

export const AgentMemoryScope = Schema.Literals(["user", "project", "local"])
export type AgentMemoryScope = Schema.Schema.Type<typeof AgentMemoryScope>

const ENTRYPOINT = "MEMORY.md"
const SNAPSHOT_META = "snapshot.json"
const SYNCED_META = ".snapshot-synced.json"
const MAX_ENTRYPOINT_LINES = 200
const MAX_ENTRYPOINT_BYTES = 25_000

export interface Interface {
  readonly prompt: (agent: Agent.Info) => Effect.Effect<string | undefined>
  readonly paths: (agent: Agent.Info) => Effect.Effect<AgentMemoryPaths | undefined>
}

export type AgentMemoryPaths = {
  scope: AgentMemoryScope
  directory: string
  entrypoint: string
  snapshotDirectory: string
}

export class Service extends Context.Service<Service, Interface>()("@opencode/AgentMemory") {}

export const layer: Layer.Layer<Service, never, AppFileSystem.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service

    const paths: Interface["paths"] = Effect.fn("AgentMemory.paths")(function* (agent) {
      const scope = memoryScope(agent)
      if (!scope) return undefined
      const ctx = yield* InstanceState.context
      return pathsForContext(ctx, agent.name, scope)
    })

    const prompt: Interface["prompt"] = Effect.fn("AgentMemory.prompt")(function* (agent) {
      const p = yield* paths(agent)
      if (!p) return undefined
      const snapshotNote = yield* syncSnapshotIfNeeded(fs, p, agent.name)
      yield* fs.ensureDir(p.directory).pipe(Effect.ignore)
      const raw = yield* fs.readFileStringSafe(p.entrypoint).pipe(Effect.catch(() => Effect.succeed(undefined)))
      const entrypoint = raw?.trim() ? truncateEntrypoint(raw) : undefined
      return buildPrompt(agent.name, p, entrypoint, snapshotNote)
    })

    return Service.of({ prompt, paths })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(AppFileSystem.defaultLayer))

export function memoryScope(agent: Pick<Agent.Info, "memory" | "options">): AgentMemoryScope | undefined {
  if (agent.memory) return agent.memory
  const value = agent.options?.memory
  return value === "user" || value === "project" || value === "local" ? value : undefined
}

export function pathsForContext(ctx: InstanceContext, agentName: string, scope: AgentMemoryScope): AgentMemoryPaths {
  const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
  const dirName = sanitizeAgentName(agentName)
  const directory =
    scope === "user"
      ? path.join(Global.Path.data, "agent-memory", dirName)
      : scope === "project"
        ? path.join(root, ".opencode", "agent-memory", dirName)
        : path.join(root, ".opencode", "agent-memory-local", dirName)
  return {
    scope,
    directory,
    entrypoint: path.join(directory, ENTRYPOINT),
    snapshotDirectory: path.join(root, ".opencode", "agent-memory-snapshots", dirName),
  }
}

function sanitizeAgentName(agentName: string) {
  return agentName.replace(/[^a-zA-Z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "agent"
}

const syncSnapshotIfNeeded = Effect.fn("AgentMemory.syncSnapshotIfNeeded")(function* (
  fs: AppFileSystem.Interface,
  p: AgentMemoryPaths,
  agentName: string,
) {
  if (p.scope !== "user") return undefined
  const snapshot = yield* readJson<{ updatedAt?: string }>(fs, path.join(p.snapshotDirectory, SNAPSHOT_META))
  if (!snapshot?.updatedAt) return undefined

  const localFiles = yield* fs
    .readDirectoryEntries(p.directory)
    .pipe(Effect.map((entries) => entries.filter((entry) => entry.type === "file" && entry.name.endsWith(".md"))))
    .pipe(Effect.catch(() => Effect.succeed([])))

  if (localFiles.length === 0) {
    yield* copySnapshot(fs, p)
    yield* writeJson(fs, path.join(p.directory, SYNCED_META), { syncedFrom: snapshot.updatedAt })
    return `Initialized ${agentName} user memory from project snapshot ${snapshot.updatedAt}.`
  }

  const synced = yield* readJson<{ syncedFrom?: string }>(fs, path.join(p.directory, SYNCED_META))
  if (!synced?.syncedFrom || new Date(snapshot.updatedAt) > new Date(synced.syncedFrom)) {
    return [
      `A newer project snapshot exists for ${agentName} user memory: ${snapshot.updatedAt}.`,
      `Review files under ${p.snapshotDirectory} before replacing or merging local memory.`,
    ].join("\n")
  }
  return undefined
})

const copySnapshot = Effect.fn("AgentMemory.copySnapshot")(function* (fs: AppFileSystem.Interface, p: AgentMemoryPaths) {
  yield* fs.ensureDir(p.directory).pipe(Effect.ignore)
  const files = yield* fs.readDirectoryEntries(p.snapshotDirectory).pipe(Effect.catch(() => Effect.succeed([])))
  yield* Effect.forEach(
    files.filter((entry) => entry.type === "file" && entry.name !== SNAPSHOT_META),
    (entry) =>
      fs
        .readFileString(path.join(p.snapshotDirectory, entry.name))
        .pipe(Effect.flatMap((content) => fs.writeWithDirs(path.join(p.directory, entry.name), content))),
    { discard: true },
  ).pipe(Effect.ignore)
})

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

function buildPrompt(agentName: string, paths: AgentMemoryPaths, entrypoint: string | undefined, snapshotNote?: string) {
  const scopeGuidance =
    paths.scope === "user"
      ? "This is user-scoped memory. Keep learnings general because they apply across projects."
      : paths.scope === "project"
        ? "This is project-scoped memory. Tailor memories to this repository and keep them appropriate for sharing with the project."
        : "This is local project memory. Tailor memories to this repository and machine; it is not intended for version control."

  return [
    "# Persistent Agent Memory",
    "",
    `Agent "${agentName}" has persistent file-based memory enabled.`,
    `Scope: ${paths.scope}`,
    `Directory: ${paths.directory}`,
    `Entrypoint: ${paths.entrypoint}`,
    "",
    scopeGuidance,
    "The directory already exists or will be created for you; write to it directly with write/edit tools when saving durable memories.",
    "",
    "When saving memory:",
    "- Save only durable facts, user preferences, recurring feedback, or project-specific conventions useful in future sessions.",
    "- Do not save secrets, credentials, or transient task state.",
    "- Prefer one topic per markdown file and keep MEMORY.md as a concise index of links or short reminders.",
    "- Update or remove stale memories instead of duplicating them.",
    snapshotNote ? ["", "<agent-memory-snapshot>", snapshotNote, "</agent-memory-snapshot>"].join("\n") : "",
    "",
    `<agent-memory scope="${paths.scope}" path="${xmlEscape(paths.entrypoint)}">`,
    entrypoint ?? "No MEMORY.md entries have been saved yet.",
    "</agent-memory>",
  ]
    .filter((line) => line !== "")
    .join("\n")
}

const readJson = <T>(fs: AppFileSystem.Interface, filepath: string) =>
  fs.readJson(filepath).pipe(
    Effect.map((value) => value as T),
    Effect.catch(() => Effect.succeed(undefined as T | undefined)),
  )

const writeJson = (fs: AppFileSystem.Interface, filepath: string, value: unknown) =>
  fs.writeWithDirs(filepath, JSON.stringify(value, null, 2) + "\n").pipe(Effect.ignore)

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export * as AgentMemory from "./agent"
