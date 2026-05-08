import { afterEach, describe, expect } from "bun:test"
import { Effect } from "effect"
import fs from "fs/promises"
import path from "path"
import { Flag } from "@opencode-ai/core/flag/flag"
import { AppRuntime } from "../../src/effect/app-runtime"
import { InstanceStore } from "../../src/project/instance-store"
import { SessionID } from "../../src/session/schema"
import { Server } from "../../src/server/server"
import { SwarmPaths } from "../../src/server/routes/instance/httpapi/groups/swarm"
import { SwarmRuntime } from "../../src/swarm/runtime"
import { disposeAllInstances, tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { it } from "../lib/effect"

const original = Flag.OPENCODE_EXPERIMENTAL_HTTPAPI

function app(experimental = true) {
  Flag.OPENCODE_EXPERIMENTAL_HTTPAPI = experimental
  return experimental ? Server.Default().app : Server.Legacy().app
}

function pathFor(path: string, params: Record<string, string>) {
  return Object.entries(params).reduce((result, [key, value]) => result.replace(`:${key}`, value), path)
}

function request(path: string, init?: RequestInit) {
  return Effect.promise(async () => app().request(path, init))
}

function requestWithBackend(experimental: boolean, path: string, init?: RequestInit) {
  return Effect.promise(async () => app(experimental).request(path, init))
}

function seedRunningWorker(input: {
  directory: string
  parentSessionID: SessionID
  name: string
  pane?: {
    paneID: string
    paneExternalSession?: boolean
    paneWindowTarget?: string
  }
}) {
  return Effect.promise(() =>
    AppRuntime.runPromise(
      InstanceStore.Service.use((store) =>
        store.provide(
          { directory: input.directory },
          Effect.gen(function* () {
            const swarm = yield* SwarmRuntime.Service
            const worker = yield* swarm.spawn({
              parentSessionID: input.parentSessionID,
              sessionID: SessionID.descending(),
              agent: "general",
              name: input.name,
              prompt: "wait for instructions",
              description: input.name,
              ...(input.pane
                ? {
                    backend: "tmux" as const,
                    paneID: input.pane.paneID,
                    paneExternalSession: input.pane.paneExternalSession,
                    paneWindowTarget: input.pane.paneWindowTarget,
                  }
                : {}),
              wait: false,
              run: Effect.never,
            })
            return worker.workerID
          }),
        ),
      ),
    ),
  )
}

function json<T>(response: Response) {
  return Effect.promise(async () => {
    if (response.status !== 200) throw new Error(await response.text())
    return (await response.json()) as T
  })
}

function withTmp<A, E, R>(
  options: Parameters<typeof tmpdir>[0],
  fn: (tmp: Awaited<ReturnType<typeof tmpdir>>) => Effect.Effect<A, E, R>,
) {
  return Effect.acquireRelease(
    Effect.promise(() => tmpdir(options)),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  ).pipe(Effect.flatMap(fn))
}

afterEach(async () => {
  Flag.OPENCODE_EXPERIMENTAL_HTTPAPI = original
  await disposeAllInstances()
  await resetDatabase()
})

describe("swarm HttpApi", () => {
  it.live(
    "creates, lists, and deletes teams through the HttpApi bridge",
    withTmp({ git: true, config: { formatter: false, lsp: false } }, (tmp) =>
      Effect.gen(function* () {
        const sessionID = SessionID.descending()
        const headers = {
          "x-opencode-directory": tmp.path,
          "content-type": "application/json",
        }

        const created = yield* request(
          pathFor(SwarmPaths.teamCreate, { sessionID }),
          {
            headers,
            method: "POST",
            body: JSON.stringify({
              team_name: "red",
              description: "parallel review",
              agent_type: "lead",
            }),
          },
        ).pipe(Effect.flatMap(json<any>))
        expect(created.name).toBe("red")
        expect(created.description).toBe("parallel review")
        expect(created.workerIDs).toEqual([])

        const teams = yield* request(pathFor(SwarmPaths.teams, { sessionID }), { headers }).pipe(Effect.flatMap(json<any[]>))
        expect(teams.map((team) => team.name)).toEqual(["red"])

        const workers = yield* request(pathFor(SwarmPaths.workers, { sessionID }), { headers }).pipe(
          Effect.flatMap(json<any[]>),
        )
        expect(workers).toEqual([])

        const deleted = yield* request(pathFor(SwarmPaths.teamDelete, { sessionID, teamName: "red" }), {
          headers,
          method: "DELETE",
        }).pipe(Effect.flatMap(json<any>))
        expect(deleted.name).toBe("red")

        const empty = yield* request(pathFor(SwarmPaths.teams, { sessionID }), { headers }).pipe(Effect.flatMap(json<any[]>))
        expect(empty).toEqual([])
      }),
    ),
  )

  it.live(
    "stops workers gracefully and cancels workers forcefully through the HttpApi bridge",
    withTmp({ git: true, config: { formatter: false, lsp: false } }, (tmp) =>
      Effect.gen(function* () {
        const sessionID = SessionID.descending()
        const headers = {
          "x-opencode-directory": tmp.path,
          "content-type": "application/json",
        }

        const gracefulID = yield* seedRunningWorker({
          directory: tmp.path,
          parentSessionID: sessionID,
          name: "graceful",
        })
        const stopped = yield* request(pathFor(SwarmPaths.workerStop, { sessionID, target: "graceful" }), {
          headers,
          method: "POST",
        }).pipe(Effect.flatMap(json<any>))
        expect(stopped.spec.workerID).toBe(gracefulID)
        expect(stopped.status).toBe("cancelled")
        expect(stopped.result?.text).toBe("stopped: stopped through API")

        const forceID = yield* seedRunningWorker({
          directory: tmp.path,
          parentSessionID: sessionID,
          name: "force",
        })
        const cancelled = yield* request(pathFor(SwarmPaths.workerCancel, { sessionID, target: "force" }), {
          headers,
          method: "POST",
        }).pipe(Effect.flatMap(json<any>))
        expect(cancelled.spec.workerID).toBe(forceID)
        expect(cancelled.status).toBe("cancelled")
        expect(cancelled.result?.text).toBe("cancelled")
      }),
    ),
  )

  it.live(
    "hides and shows pane-backed workers through the HttpApi bridge",
    withTmp({ git: true, config: { formatter: false, lsp: false } }, (tmp) =>
      Effect.gen(function* () {
        const sessionID = SessionID.descending()
        const headers = {
          "x-opencode-directory": tmp.path,
          "content-type": "application/json",
        }
        const bin = path.join(tmp.path, "bin")
        const log = path.join(tmp.path, "tmux.log")
        const originalPath = process.env.PATH

        try {
          yield* Effect.promise(() => fs.mkdir(bin))
          yield* Effect.promise(() =>
            fs.writeFile(path.join(bin, "tmux"), `#!/bin/sh\nprintf '%s\\n' "$*" >> ${JSON.stringify(log)}\nexit 0\n`, {
              mode: 0o755,
            }),
          )
          process.env.PATH = `${bin}:${originalPath ?? ""}`

          const workerID = yield* seedRunningWorker({
            directory: tmp.path,
            parentSessionID: sessionID,
            name: "pane-worker",
            pane: {
              paneID: "%7",
              paneExternalSession: true,
              paneWindowTarget: "opencode-swarm:agents",
            },
          })

          const hidden = yield* request(pathFor(SwarmPaths.workerPane, { sessionID, target: "pane-worker", action: "hide" }), {
            headers,
            method: "POST",
          }).pipe(Effect.flatMap(json<any>))
          expect(hidden.spec.workerID).toBe(workerID)
          expect(hidden.paneHidden).toBe(true)

          const shown = yield* request(pathFor(SwarmPaths.workerPane, { sessionID, target: "pane-worker", action: "show" }), {
            headers,
            method: "POST",
          }).pipe(Effect.flatMap(json<any>))
          expect(shown.spec.workerID).toBe(workerID)
          expect(shown.paneHidden).toBe(false)

          const commands = yield* Effect.promise(() => fs.readFile(log, "utf8"))
          expect(commands).toContain("-L opencode-swarm break-pane -d -s %7 -t opencode-swarm-hidden:")
          expect(commands).toContain("-L opencode-swarm join-pane -h -s %7 -t opencode-swarm:agents")
        } finally {
          process.env.PATH = originalPath
        }
      }),
    ),
  )

  it.live(
    "creates, lists, gets, and updates shared tasks through the HttpApi bridge",
    withTmp({ git: true, config: { formatter: false, lsp: false } }, (tmp) =>
      Effect.gen(function* () {
        const sessionID = SessionID.descending()
        const headers = {
          "x-opencode-directory": tmp.path,
          "content-type": "application/json",
        }

        yield* request(pathFor(SwarmPaths.teamCreate, { sessionID }), {
          headers,
          method: "POST",
          body: JSON.stringify({ team_name: "red" }),
        }).pipe(Effect.flatMap(json<any>))

        const created = yield* request(pathFor(SwarmPaths.taskCreate, { sessionID }), {
          headers,
          method: "POST",
          body: JSON.stringify({
            team: "red",
            subject: "Run tests",
            description: "Run the suite",
            owner: "tester",
          }),
        }).pipe(Effect.flatMap(json<any>))
        expect(created.id).toBe("1")
        expect(created.owner).toBe("tester")

        const listed = yield* request(`${pathFor(SwarmPaths.taskList, { sessionID })}?team=red`, { headers }).pipe(
          Effect.flatMap(json<any[]>),
        )
        expect(listed.map((task) => task.subject)).toEqual(["Run tests"])

        const detail = yield* request(`${pathFor(SwarmPaths.taskGet, { sessionID, taskID: "1" })}?team=red`, {
          headers,
        }).pipe(Effect.flatMap(json<any>))
        expect(detail.description).toBe("Run the suite")

        const updated = yield* request(pathFor(SwarmPaths.taskUpdate, { sessionID, taskID: "1" }), {
          headers,
          method: "PATCH",
          body: JSON.stringify({
            team: "red",
            status: "completed",
          }),
        }).pipe(Effect.flatMap(json<any>))
        expect(updated.success).toBe(true)
        expect(updated.statusChange).toEqual({ from: "pending", to: "completed" })
      }),
    ),
  )

  it.live(
    "serves team lifecycle routes through the legacy backend",
    withTmp({ git: true, config: { formatter: false, lsp: false } }, (tmp) =>
      Effect.gen(function* () {
        const sessionID = SessionID.descending()
        const headers = {
          "x-opencode-directory": tmp.path,
          "content-type": "application/json",
        }

        const created = yield* requestWithBackend(false, pathFor(SwarmPaths.teamCreate, { sessionID }), {
          headers,
          method: "POST",
          body: JSON.stringify({ team_name: "legacy" }),
        }).pipe(Effect.flatMap(json<any>))
        expect(created.name).toBe("legacy")

        const teams = yield* requestWithBackend(false, pathFor(SwarmPaths.teams, { sessionID }), { headers }).pipe(
          Effect.flatMap(json<any[]>),
        )
        expect(teams.map((team) => team.name)).toEqual(["legacy"])
      }),
    ),
  )
})
