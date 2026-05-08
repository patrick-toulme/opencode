import { describe, expect } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import fs from "fs/promises"
import { createServer, type Server } from "node:http"
import path from "path"
import { InstanceState } from "@/effect/instance-state"
import { TeamMemory } from "@/memory/team"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.empty)
const memoryIt = testEffect(TeamMemory.defaultLayer)

function startMemoryServer(entries: Record<string, string>) {
  const puts: Array<Record<string, string>> = []
  return new Promise<{ url: string; close: () => Promise<void>; puts: Array<Record<string, string>> }>(
    (resolve, reject) => {
      const server = createServer((req, res) => {
        if (req.method === "GET") {
          res.setHeader("content-type", "application/json")
          res.end(
            JSON.stringify({
              entries,
              entryChecksums: Object.fromEntries(
                Object.entries(entries).map(([key, content]) => [key, TeamMemory.hashContent(content)]),
              ),
            }),
          )
          return
        }
        if (req.method === "PUT") {
          let body = ""
          req.setEncoding("utf-8")
          req.on("data", (chunk) => {
            body += chunk
          })
          req.on("end", () => {
            const parsed = JSON.parse(body) as { entries?: Record<string, string> }
            const next = parsed.entries ?? {}
            puts.push(next)
            Object.assign(entries, next)
            res.setHeader("content-type", "application/json")
            res.end(JSON.stringify({ ok: true }))
          })
          return
        }
        res.statusCode = 405
        res.end()
      })
      server.once("error", reject)
      server.listen(0, "127.0.0.1", () => {
        const address = server.address()
        if (!address || typeof address === "string") return reject(new Error("failed to bind test memory server"))
        resolve({
          url: `http://127.0.0.1:${address.port}/team-memory`,
          puts,
          close: () => closeServer(server),
        })
      })
    },
  )
}

function closeServer(server: Server) {
  return new Promise<void>((resolve) => server.close(() => resolve()))
}

describe("memory.team", () => {
  it.instance("resolves safe markdown keys and rejects traversal keys", () =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      const paths = TeamMemory.pathsForContext(ctx)

      const resolved = yield* TeamMemory.validateKeyInContext(ctx, "guides/style.md")
      expect(resolved).toBe(path.join(paths.directory, "guides/style.md"))

      for (const key of ["../outside.md", "nested/../../outside.md", "%2e%2e%2fsecret.md", "/tmp/secret.md"]) {
        const exit = yield* TeamMemory.validateKeyInContext(ctx, key).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
      }
    }),
  )

  it.instance("rejects team memory keys that escape through symlinks", () =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      const paths = TeamMemory.pathsForContext(ctx)
      yield* Effect.promise(() => fs.mkdir(paths.directory, { recursive: true }))
      yield* Effect.promise(() => fs.symlink(ctx.directory, path.join(paths.directory, "escape")))

      const exit = yield* TeamMemory.validateKeyInContext(ctx, "escape/secret.md").pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  it.instance("blocks likely secrets when writing team memory", () =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      const paths = TeamMemory.pathsForContext(ctx)

      const safe = yield* TeamMemory.assertSafeContentForPath(
        paths.entrypoint,
        "- Prefer plan approval before coordinated edits.\n",
      ).pipe(Effect.exit)
      expect(Exit.isSuccess(safe)).toBe(true)

      const blocked = yield* TeamMemory.assertSafeContentForPath(
        paths.entrypoint,
        "OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN",
      ).pipe(Effect.exit)
      expect(Exit.isFailure(blocked)).toBe(true)

      const outside = yield* TeamMemory.assertSafeContentForPath(
        path.join(ctx.directory, "notes.md"),
        "OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN",
      ).pipe(Effect.exit)
      expect(Exit.isSuccess(outside)).toBe(true)
    }),
  )

  it.instance("syncs shared team memory with server-wins pull and safe local delta push", () =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      const paths = TeamMemory.pathsForContext(ctx)
      const remote: Record<string, string> = {
        "MEMORY.md": "- Prefer remote team conventions.\n",
        "remote/topic.md": "Remote topic detail.\n",
      }
      const server = yield* Effect.promise(() => startMemoryServer(remote))

      try {
        yield* Effect.promise(async () => {
          await fs.mkdir(paths.directory, { recursive: true })
          await fs.writeFile(path.join(paths.directory, "local.md"), "Local learning.\n", "utf-8")
          await fs.writeFile(
            path.join(paths.directory, "secret.md"),
            "OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN",
            "utf-8",
          )
        })

        const result = yield* TeamMemory.syncForContext(ctx, server.url)
        expect(result.enabled).toBe(true)
        expect(result.pulled).toBe(2)
        expect(result.pushed).toBe(1)
        expect(result.skipped.map((item) => item.key)).toContain("secret.md")
        expect(yield* Effect.promise(() => fs.readFile(paths.entrypoint, "utf-8"))).toBe(
          "- Prefer remote team conventions.\n",
        )
        expect(yield* Effect.promise(() => fs.readFile(path.join(paths.directory, "remote/topic.md"), "utf-8"))).toBe(
          "Remote topic detail.\n",
        )
        expect(remote["local.md"]).toBe("Local learning.\n")
        expect(remote["secret.md"]).toBeUndefined()
        expect(server.puts).toHaveLength(1)
      } finally {
        yield* Effect.promise(() => server.close())
      }
    }),
  )

  memoryIt.instance("prompt syncs configured remote team memory before rendering", () =>
    Effect.gen(function* () {
      const server = yield* Effect.promise(() =>
        startMemoryServer({
          "MEMORY.md": "- Synced remote team convention.\n",
        }),
      )
      const previous = process.env.OPENCODE_TEAM_MEMORY_SYNC_URL
      process.env.OPENCODE_TEAM_MEMORY_SYNC_URL = server.url

      try {
        const memory = yield* TeamMemory.Service
        const prompt = yield* memory.prompt()
        expect(prompt).toContain("# Shared Team Memory")
        expect(prompt).toContain("Synced remote team convention")
      } finally {
        if (previous === undefined) delete process.env.OPENCODE_TEAM_MEMORY_SYNC_URL
        else process.env.OPENCODE_TEAM_MEMORY_SYNC_URL = previous
        yield* Effect.promise(() => server.close())
      }
    }),
  )
})
