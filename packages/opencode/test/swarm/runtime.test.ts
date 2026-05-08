import { afterEach, describe, expect } from "bun:test"
import { Effect, Exit, Fiber, Layer } from "effect"
import fs from "fs/promises"
import { createServer, type Server, type Socket } from "node:net"
import os from "os"
import path from "path"
import { Bus } from "@/bus"
import { InstanceState } from "@/effect/instance-state"
import { InstanceRef } from "@/effect/instance-ref"
import { SessionID } from "@/session/schema"
import { SwarmMailbox } from "@/swarm/mailbox"
import { SwarmPeer, type PeerMessage } from "@/swarm/peer"
import { Storage } from "@/storage/storage"
import { SwarmRuntime } from "@/swarm/runtime"
import { WorkerID, type WorkerCompletion } from "@/swarm/state"
import { disposeAllInstances } from "../fixture/fixture"
import { TestConfig } from "../fixture/config"
import { testEffect } from "../lib/effect"

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(SwarmRuntime.layer.pipe(Layer.provideMerge(Bus.layer), Layer.provideMerge(Storage.defaultLayer)))
const itWithConfig = testEffect(
  SwarmRuntime.layer.pipe(
    Layer.provideMerge(Bus.layer),
    Layer.provideMerge(Storage.defaultLayer),
    Layer.provideMerge(
      TestConfig.layer({
        get: () =>
          Effect.succeed({
            experimental: {
              swarm_remote_token: "cfg-token",
            },
          }),
      }),
    ),
  ),
)

function startPeerCapture(socketPath: string) {
  return new Promise<{ server: Server; messages: PeerMessage[] }>((resolve, reject) => {
    const messages: PeerMessage[] = []
    const server = createServer((socket: Socket) => {
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
          messages.push(JSON.parse(line) as PeerMessage)
        }
      })
    })
    server.once("error", reject)
    server.listen(socketPath, () => resolve({ server, messages }))
  })
}

function closePeerCapture(server: Server) {
  return new Promise<void>((resolve) => server.close(() => resolve()))
}

function withRemoteServer<A, E, R>(
  fetch: (request: Request) => Response | Promise<Response>,
  run: (endpoint: string) => Effect.Effect<A, E, R>,
) {
  return Effect.acquireRelease(
    Effect.sync(() => Bun.serve({ port: 0, fetch })),
    (server) => Effect.sync(() => server.stop(true)),
  ).pipe(Effect.flatMap((server) => run(server.url.origin)))
}

describe("swarm.runtime", () => {
  it.instance("spawns, indexes, completes, and lists workers", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()

      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID,
        agent: "general",
        prompt: "inspect the bug",
        description: "inspect bug",
        wait: true,
        run: Effect.succeed({ status: "completed", text: "done" } satisfies WorkerCompletion),
      })

      expect(result.completion).toEqual({ status: "completed", text: "done" })
      expect(result.state.status).toBe("completed")

      const byID = yield* swarm.get(result.workerID)
      const bySession = yield* swarm.getBySession(sessionID)
      const byParent = yield* swarm.list(parentSessionID)

      expect(byID?.spec.workerID).toBe(result.workerID)
      expect(bySession?.spec.workerID).toBe(result.workerID)
      expect(byParent.map((worker) => worker.spec.workerID)).toEqual([result.workerID])
      expect(byID?.result?.text).toBe("done")
    }),
  )

  it.instance("registers externally launched workers and lets a worker process adopt them", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()
      const workerID = WorkerID.ascending()
      let launched = false

      const spawned = yield* swarm.spawn({
        workerID,
        parentSessionID,
        sessionID,
        agent: "general",
        name: "pane-worker",
        team: "blue",
        prompt: "wait for external process",
        description: "external pane worker",
        wait: false,
        executionStrategy: "persistent",
        backend: "in-process",
        launch: Effect.sync(() => {
          launched = true
        }),
      })

      expect(launched).toBe(true)
      expect(spawned.workerID).toBe(workerID)
      expect(spawned.state.status).toBe("running")
      expect((yield* swarm.get(workerID))?.spec.name).toBe("pane-worker")

      const adopted = yield* swarm
        .adopt({
          workerID,
          wait: false,
          run: Effect.gen(function* () {
            const input = yield* swarm.awaitInput(workerID)
            return { status: "completed", text: `adopted:${input.message}` } satisfies WorkerCompletion
          }),
        })
        .pipe(Effect.timeout("1 second"))
      expect(adopted.workerID).toBe(workerID)

      yield* swarm.sendInput({
        parentSessionID,
        to: "pane-worker",
        message: "continue externally",
        summary: "external input",
        from: "team-lead",
      })

      const completed = yield* Effect.gen(function* () {
        for (let i = 0; i < 100; i++) {
          const current = yield* swarm.get(workerID)
          if (current?.status === "completed") return current
          yield* Effect.sleep("10 millis")
        }
        return yield* swarm.get(workerID)
      })
      if (!completed) throw new Error("missing completed worker")
      expect(completed.status).toBe("completed")
      expect(completed.result?.text).toBe("adopted:continue externally")
    }),
  )

  it.instance("runs external cleanup when pane launch fails", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()
      const workerID = WorkerID.ascending()
      let cleaned = false

      const result = yield* swarm.spawn({
        workerID,
        parentSessionID,
        sessionID,
        agent: "general",
        prompt: "launch in pane",
        description: "pane launch failure",
        wait: false,
        executionStrategy: "persistent",
        backend: "tmux",
        paneID: "%9",
        launch: Effect.sync(() => {
          throw new Error("tmux send failed")
        }),
        cancel: Effect.sync(() => {
          cleaned = true
        }),
      })

      expect(cleaned).toBe(true)
      expect(result.completion).toEqual({ status: "failed", error: "tmux send failed" })
      const worker = yield* swarm.get(workerID)
      expect(worker?.status).toBe("failed")
      expect(worker?.result?.error).toBe("tmux send failed")
    }),
  )

  it.instance("hides and shows external tmux panes through runtime control", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const dir = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-swarm-pane-")))
      const originalPath = process.env.PATH

      try {
        const log = path.join(dir, "tmux.log")
        const tmux = path.join(dir, "tmux")
        yield* Effect.promise(() =>
          fs.writeFile(tmux, `#!/bin/sh\nprintf '%s\\n' "$*" >> ${JSON.stringify(log)}\nexit 0\n`, {
            mode: 0o755,
          }),
        )
        process.env.PATH = `${dir}:${originalPath ?? ""}`

        const parentSessionID = SessionID.descending()
        const result = yield* swarm.spawn({
          parentSessionID,
          sessionID: SessionID.descending(),
          agent: "general",
          prompt: "launch in pane",
          description: "pane visibility",
          wait: false,
          executionStrategy: "persistent",
          backend: "tmux",
          paneID: "%9",
          paneExternalSession: true,
          paneWindowTarget: "opencode-swarm:agents",
          launch: Effect.void,
        })

        const hidden = yield* swarm.controlPane(result.workerID, "hide")
        expect(hidden.paneHidden).toBe(true)
        expect(hidden.lastProgress).toBe("pane hidden")

        const shown = yield* swarm.controlPane(result.workerID, "show")
        expect(shown.paneHidden).toBe(false)
        expect(shown.lastProgress).toBe("pane shown")

        const commands = yield* Effect.promise(() => fs.readFile(log, "utf8"))
        expect(commands).toContain("-L opencode-swarm new-session -d -s opencode-swarm-hidden")
        expect(commands).toContain("-L opencode-swarm break-pane -d -s %9 -t opencode-swarm-hidden:")
        expect(commands).toContain("-L opencode-swarm join-pane -h -s %9 -t opencode-swarm:agents")
        expect(commands).toContain("-L opencode-swarm select-layout -t opencode-swarm:agents main-vertical")
      } finally {
        process.env.PATH = originalPath
        yield* Effect.promise(() => fs.rm(dir, { recursive: true, force: true }))
      }
    }),
  )

  it.instance("removes clean persisted worktrees when external workers reach a terminal state", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const dir = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-swarm-worktree-")))
      const originalPath = process.env.PATH

      try {
        const bin = path.join(dir, "bin")
        const root = path.join(dir, "repo")
        const worktree = path.join(dir, "worker")
        const log = path.join(dir, "git.log")
        yield* Effect.promise(() => fs.mkdir(bin))
        yield* Effect.promise(() => fs.mkdir(root))
        yield* Effect.promise(() => fs.mkdir(worktree))
        yield* Effect.promise(() =>
          fs.writeFile(
            path.join(bin, "git"),
            `#!/bin/sh\nprintf '%s|%s\\n' "$PWD" "$*" >> ${JSON.stringify(log)}\nexit 0\n`,
            { mode: 0o755 },
          ),
        )
        process.env.PATH = `${bin}:${originalPath ?? ""}`

        const result = yield* swarm.spawn({
          parentSessionID: SessionID.descending(),
          sessionID: SessionID.descending(),
          agent: "general",
          prompt: "work in isolated tree",
          description: "external worktree cleanup",
          wait: false,
          executionStrategy: "persistent",
          backend: "tmux",
          paneID: "%10",
          worktreeRoot: root,
          worktreePath: worktree,
          worktreeBranch: "opencode-agent-test",
          launch: Effect.void,
          cancel: Effect.void,
        })

        yield* swarm.cancel(result.workerID)

        const commands = yield* Effect.promise(() => fs.readFile(log, "utf8"))
        expect(commands).toContain(`${worktree}|status --porcelain`)
        expect(commands).toContain(`${root}|worktree remove --force ${worktree}`)
        expect(commands).toContain(`${root}|branch -D opencode-agent-test`)
      } finally {
        process.env.PATH = originalPath
        yield* Effect.promise(() => fs.rm(dir, { recursive: true, force: true }))
      }
    }),
  )

  it.instance("routes messages and cancellation to remote workers through persisted remote metadata", () =>
    Effect.gen(function* () {
      const requests: string[] = []
      yield* withRemoteServer(
        async (request) => {
          const url = new URL(request.url)
          requests.push(`${request.method} ${url.pathname}`)
          if (request.method === "POST" && url.pathname === "/workers/remote-1/message") {
            const body = await request.json()
            return Response.json({ ok: true, inputID: body.input?.id })
          }
          if (request.method === "POST" && url.pathname === "/workers/remote-1/cancel") {
            return Response.json({ ok: true })
          }
          return new Response("not found", { status: 404 })
        },
        (endpoint) =>
          Effect.gen(function* () {
          const swarm = yield* SwarmRuntime.Service
          const parentSessionID = SessionID.descending()
          const workerID = WorkerID.ascending()
          const spawned = yield* swarm.spawn({
            workerID,
            parentSessionID,
            sessionID: SessionID.descending(),
            agent: "general",
            prompt: "remote work",
            description: "remote worker",
            wait: false,
            executionStrategy: "persistent",
            backend: "remote",
            remoteEndpoint: endpoint,
            remoteID: "remote-1",
            remoteSessionURL: "https://remote.example/session/remote-1",
            run: Effect.never,
          })

          expect(spawned.state.spec.backend).toBe("remote")
          expect(spawned.state.spec.remoteID).toBe("remote-1")

          const input = yield* swarm.sendInput({
            parentSessionID,
            to: workerID,
            message: "continue remotely",
            summary: "remote follow-up",
            from: "test",
          })
          expect(input.message).toBe("continue remotely")

          yield* swarm.cancel(workerID)
          const cancelled = yield* swarm.get(workerID)
          expect(cancelled?.status).toBe("cancelled")
          }),
      )
      expect(requests).toContain("POST /workers/remote-1/message")
      expect(requests).toContain("POST /workers/remote-1/cancel")
    }),
  )

  itWithConfig.instance("uses configured remote token for remote messages and cancellation", () =>
    Effect.gen(function* () {
      const previousToken = process.env.OPENCODE_SWARM_REMOTE_TOKEN
      delete process.env.OPENCODE_SWARM_REMOTE_TOKEN
      const authorizations: string[] = []
      try {
        yield* withRemoteServer(
          async (request) => {
            const url = new URL(request.url)
            authorizations.push(request.headers.get("authorization") ?? "")
            if (request.method === "POST" && url.pathname === "/workers/remote-auth/message") {
              return Response.json({ ok: true })
            }
            if (request.method === "POST" && url.pathname === "/workers/remote-auth/cancel") {
              return Response.json({ ok: true })
            }
            return new Response("not found", { status: 404 })
          },
          (endpoint) =>
            Effect.gen(function* () {
              const swarm = yield* SwarmRuntime.Service
              const parentSessionID = SessionID.descending()
              const workerID = WorkerID.ascending()
              yield* swarm.spawn({
                workerID,
                parentSessionID,
                sessionID: SessionID.descending(),
                agent: "general",
                prompt: "remote work",
                description: "remote worker",
                wait: false,
                executionStrategy: "persistent",
                backend: "remote",
                remoteEndpoint: endpoint,
                remoteID: "remote-auth",
                run: Effect.never,
              })

              yield* swarm.sendInput({
                parentSessionID,
                to: workerID,
                message: "continue remotely",
                from: "test",
              })
              yield* swarm.cancel(workerID)
            }),
        )
      } finally {
        if (previousToken === undefined) delete process.env.OPENCODE_SWARM_REMOTE_TOKEN
        else process.env.OPENCODE_SWARM_REMOTE_TOKEN = previousToken
      }

      expect(authorizations).toEqual(["Bearer cfg-token", "Bearer cfg-token"])
    }),
  )

  it.instance("reload resumes polling persisted remote workers that lost their local fiber", () =>
    Effect.gen(function* () {
      const dir = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-swarm-remote-")))
      const outputPath = path.join(dir, "remote-worker.jsonl")
      const requests: string[] = []

      try {
        yield* withRemoteServer(
          async (request) => {
            const url = new URL(request.url)
            requests.push(`${request.method} ${url.pathname} ${url.searchParams.get("cursor") ?? ""}`.trim())
            if (request.method === "GET" && url.pathname === "/workers/remote-recover/events") {
              return Response.json({
                cursor: "1",
                events: [
                  { type: "progress", message: "recovered poller running" },
                  { type: "completed", text: "recovered result" },
                ],
              })
            }
            return new Response("not found", { status: 404 })
          },
          (endpoint) =>
            Effect.gen(function* () {
              const swarm = yield* SwarmRuntime.Service
              const workerID = WorkerID.ascending()

              const spawned = yield* swarm.spawn({
                workerID,
                parentSessionID: SessionID.descending(),
                sessionID: SessionID.descending(),
                agent: "general",
                prompt: "recover remote worker",
                description: "recover remote worker",
                outputPath,
                wait: false,
                executionStrategy: "persistent",
                backend: "remote",
                remoteEndpoint: endpoint,
                remoteID: "remote-recover",
                remoteSessionURL: "https://remote.example/session/remote-recover",
                launch: Effect.void,
              })
              expect(spawned.state.status).toBe("running")

              yield* swarm.reload()

              let worker = yield* swarm.get(workerID)
              for (let i = 0; i < 100 && worker?.status !== "completed"; i++) {
                yield* Effect.sleep("10 millis")
                worker = yield* swarm.get(workerID)
              }

              expect(worker?.status).toBe("completed")
              expect(worker?.remoteCursor).toBe("1")
              expect(worker?.result?.text).toBe("recovered result")
            }),
        )

        expect(requests).toContain("GET /workers/remote-recover/events")
        const output = yield* Effect.promise(() => fs.readFile(outputPath, "utf8"))
        expect(output).toContain("recovered poller running")
        expect(output).toContain("recovered result")
      } finally {
        yield* Effect.promise(() => fs.rm(dir, { recursive: true, force: true }))
      }
    }),
  )

  it.instance("tracks current tool and pending permission for running workers", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()

      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID,
        agent: "general",
        prompt: "read a file",
        description: "read file",
        wait: false,
        run: Effect.never,
      })

      yield* swarm.updateCurrentTool(result.workerID, { name: "read", title: "src/index.ts" })
      let worker = yield* swarm.get(result.workerID)
      expect(worker?.currentTool).toEqual({ name: "read", title: "src/index.ts" })

      yield* swarm.markPermissionPending(result.workerID, "per_test")
      worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("waiting_permission")
      expect(worker?.pendingPermissionID).toBe("per_test")

      yield* swarm.clearPermissionPending(result.workerID, "per_test")
      yield* swarm.updateCurrentTool(result.workerID, undefined)
      worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("running")
      expect(worker?.pendingPermissionID).toBeUndefined()
      expect(worker?.currentTool).toBeUndefined()

      yield* swarm.cancel(result.workerID)
      worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("cancelled")
    }),
  )

  it.instance("tracks pending plan approval requests and responses", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()

      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID,
        agent: "general",
        prompt: "plan before editing",
        description: "plan worker",
        wait: false,
        executionStrategy: "persistent",
        planModeRequired: true,
        run: Effect.never,
      })

      yield* swarm.requestPlanApproval(result.workerID, "par_test")
      let worker = yield* swarm.get(result.workerID)
      expect(worker?.pendingPlanApprovalID).toBe("par_test")
      expect(worker?.lastProgress).toBe("plan approval requested")

      yield* swarm.approvePlan(result.workerID, "par_test")
      worker = yield* swarm.get(result.workerID)
      expect(worker?.pendingPlanApprovalID).toBeUndefined()
      expect(worker?.lastProgress).toBe("plan approved: par_test")

      yield* swarm.requestPlanApproval(result.workerID, "par_again")
      yield* swarm.rejectPlan(result.workerID, "par_again", "Need more detail")
      worker = yield* swarm.get(result.workerID)
      expect(worker?.pendingPlanApprovalID).toBeUndefined()
      expect(worker?.lastProgress).toBe("plan rejected: Need more detail")

      yield* swarm.cancel(result.workerID)
    }),
  )

  it.instance("queues messages by scoped name and wakes idle workers", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()

      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID,
        agent: "general",
        name: "worker-a",
        prompt: "initial prompt",
        description: "background worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const waiter = yield* swarm.awaitInput(result.workerID).pipe(Effect.forkScoped)
      yield* Effect.sleep("10 millis")

      let worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("idle")

      const sent = yield* swarm.sendInput({
        parentSessionID,
        to: "worker-a",
        message: "continue with the second step",
        summary: "second step",
        from: "build",
      })
      const received = yield* Fiber.join(waiter)

      expect(received).toEqual(sent)
      expect(received.message).toBe("continue with the second step")
      expect(received.summary).toBe("second step")

      worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("running")
      expect(worker?.mailboxSize).toBe(0)

      yield* swarm.cancel(result.workerID)
    }),
  )

  it.instance("wakes idle workers from the swarm file mailbox", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "worker-file",
        team: "review",
        prompt: "initial prompt",
        description: "background worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const waiter = yield* swarm.awaitInput(result.workerID).pipe(Effect.forkScoped)
      yield* Effect.sleep("10 millis")
      const worker = yield* swarm.get(result.workerID)
      expect(worker?.status).toBe("idle")
      if (!worker) throw new Error("missing worker")

      const sent = yield* SwarmMailbox.writeInput(worker, {
        message: "continue from another process",
        summary: "remote follow-up",
        from: "remote-lead",
      })
      const received = yield* Fiber.join(waiter).pipe(Effect.timeout("1 second"))

      expect(received).toEqual(sent)
      expect(received.message).toBe("continue from another process")
      expect(received.summary).toBe("remote follow-up")
      expect(received.from).toBe("remote-lead")

      yield* swarm.cancel(result.workerID)
    }),
  )

  it.instance("prioritizes shutdown requests and team-lead messages for idle workers", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const result = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "worker-a",
        team: "review",
        prompt: "initial prompt",
        description: "background worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      yield* swarm.sendInput({
        parentSessionID,
        to: "worker-a",
        message: "peer message",
        from: "peer",
      })
      yield* swarm.sendInput({
        parentSessionID,
        to: "worker-a",
        message: "lead message",
        from: "team-lead",
      })
      yield* swarm.sendInput({
        parentSessionID,
        to: "worker-a",
        message: ["<structured-message>", "<type>shutdown_request</type>", "</structured-message>"].join("\n"),
        from: "team-lead",
      })

      const waiter = yield* swarm.awaitInput(result.workerID).pipe(Effect.forkScoped)
      const first = yield* Fiber.join(waiter)
      expect(first.message).toContain("<type>shutdown_request</type>")

      const secondWaiter = yield* swarm.awaitInput(result.workerID).pipe(Effect.forkScoped)
      const second = yield* Fiber.join(secondWaiter)
      expect(second.message).toBe("lead message")

      const thirdWaiter = yield* swarm.awaitInput(result.workerID).pipe(Effect.forkScoped)
      const third = yield* Fiber.join(thirdWaiter)
      expect(third.message).toBe("peer message")

      yield* swarm.cancel(result.workerID)
    }),
  )

  it.instance("broadcasts messages to every accepting worker in a scoped team", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const first = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "worker-a",
        team: "review",
        prompt: "initial prompt",
        description: "background worker a",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const second = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "worker-b",
        team: "review",
        prompt: "initial prompt",
        description: "background worker b",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const other = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "worker-c",
        team: "other",
        prompt: "initial prompt",
        description: "background worker c",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const firstWaiter = yield* swarm.awaitInput(first.workerID).pipe(Effect.forkScoped)
      const secondWaiter = yield* swarm.awaitInput(second.workerID).pipe(Effect.forkScoped)
      yield* swarm.awaitInput(other.workerID).pipe(Effect.forkScoped)
      yield* Effect.gen(function* () {
        while ((yield* swarm.get(other.workerID))?.status !== "idle") {
          yield* Effect.sleep("5 millis")
        }
      }).pipe(Effect.timeout("1 second"))

      const sent = yield* swarm.broadcast({
        parentSessionID,
        team: "review",
        message: "report status",
        summary: "status request",
        from: "build",
      })
      const firstReceived = yield* Fiber.join(firstWaiter)
      const secondReceived = yield* Fiber.join(secondWaiter)

      expect(sent).toHaveLength(2)
      expect([firstReceived.id, secondReceived.id].sort()).toEqual(sent.map((input) => input.id).sort())
      expect(firstReceived.message).toBe("report status")
      expect(secondReceived.summary).toBe("status request")

      const otherWorker = yield* swarm.get(other.workerID)
      expect(otherWorker?.status).toBe("idle")
      expect(otherWorker?.mailboxSize).toBeUndefined()

      yield* swarm.cancel(first.workerID)
      yield* swarm.cancel(second.workerID)
      yield* swarm.cancel(other.workerID)
    }),
  )

  it.instance("creates, lists, and deletes explicit teams", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()

      const created = yield* swarm.createTeam({
        parentSessionID,
        name: "red",
        description: "parallel review",
        leadSessionID: parentSessionID,
        agentType: "lead",
      })
      expect(created.name).toBe("red")
      expect(created.description).toBe("parallel review")
      expect(created.workerIDs).toEqual([])

      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "reviewer",
        team: "red",
        prompt: "review",
        description: "review worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const teams = yield* swarm.listTeams(parentSessionID)
      expect(teams).toHaveLength(1)
      expect(teams[0]?.workerIDs).toEqual([worker.workerID])

      const refused = yield* swarm.deleteTeam({ parentSessionID, name: "red" }).pipe(Effect.exit)
      expect(Exit.isFailure(refused)).toBe(true)
      expect((yield* swarm.listTeams(parentSessionID))).toHaveLength(1)
      expect((yield* swarm.get(worker.workerID))?.status).toBe("running")

      const deleted = yield* swarm.deleteTeam({ parentSessionID, name: "red", cancelWorkers: true })
      expect(deleted?.name).toBe("red")
      expect((yield* swarm.listTeams(parentSessionID))).toEqual([])
      expect((yield* swarm.get(worker.workerID))?.status).toBe("cancelled")
    }),
  )

  it.instance("publishes worker snapshots and team lifecycle events", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const bus = yield* Bus.Service
      const events: any[] = []
      const unsubscribe = yield* bus.subscribeAllCallback((event) => {
        events.push(event)
      })
      const parentSessionID = SessionID.descending()

      try {
        yield* swarm.createTeam({
          parentSessionID,
          name: "blue",
          description: "parallel implementation",
        })
        const worker = yield* swarm.spawn({
          parentSessionID,
          sessionID: SessionID.descending(),
          agent: "general",
          name: "builder",
          team: "blue",
          prompt: "build",
          description: "build worker",
          wait: false,
          executionStrategy: "persistent",
          run: Effect.never,
        })
        yield* swarm.updateProgress(worker.workerID, "scanning")
        yield* swarm.updateCurrentTool(worker.workerID, { name: "grep", title: "runtime" })
        yield* swarm.markPermissionPending(worker.workerID, "per_test")
        yield* swarm.cancel(worker.workerID)
        yield* swarm.deleteTeam({ parentSessionID, name: "blue" })
        yield* Effect.sleep("10 millis")

        expect(events.map((event) => event.type)).toEqual(
          expect.arrayContaining([
            "swarm.team.created",
            "swarm.team.updated",
            "swarm.worker.spawned",
            "swarm.worker.status",
            "swarm.worker.progress",
            "swarm.worker.tool",
            "swarm.worker.permission",
            "swarm.worker.stopped",
            "swarm.team.deleted",
          ]),
        )

        const spawned = events.find((event) => event.type === "swarm.worker.spawned")
        expect(spawned.properties.worker.spec.workerID).toBe(worker.workerID)
        expect(spawned.properties.worker.spec.team).toBe("blue")

        const updated = events.find((event) => event.type === "swarm.team.updated")
        expect(updated.properties.team.workerIDs).toEqual([worker.workerID])

        const permission = events.find((event) => event.type === "swarm.worker.permission")
        expect(permission.properties.worker.status).toBe("waiting_permission")
        expect(permission.properties.worker.pendingPermissionID).toBe("per_test")

        const stopped = events.find((event) => event.type === "swarm.worker.stopped")
        expect(stopped.properties.workerID).toBe(worker.workerID)
        expect(stopped.properties.status).toBe("cancelled")
      } finally {
        unsubscribe()
      }
    }),
  )

  it.instance("publishes idle and task-completed lifecycle events", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const bus = yield* Bus.Service
      const events: any[] = []
      const unsubscribe = yield* bus.subscribeAllCallback((event) => {
        events.push(event)
      })
      const parentSessionID = SessionID.descending()

      try {
        yield* swarm.createTeam({ parentSessionID, name: "blue" })
        const worker = yield* swarm.spawn({
          parentSessionID,
          sessionID: SessionID.descending(),
          agent: "general",
          name: "tester",
          team: "blue",
          prompt: "wait",
          description: "tester",
          wait: false,
          executionStrategy: "persistent",
          run: Effect.never,
        })

        yield* swarm.recordResult(worker.workerID, { status: "completed", text: "last turn done" })
        const task = yield* swarm.createTask({
          parentSessionID,
          team: "blue",
          subject: "Run tests",
          description: "Run the suite",
          owner: "tester",
        })
        yield* swarm.updateTeamTask({
          parentSessionID,
          team: "blue",
          taskID: task.id,
          status: "completed",
        })
        yield* Effect.sleep("10 millis")

        const idle = events.find((event) => event.type === "swarm.worker.idle")
        expect(idle.properties.workerID).toBe(worker.workerID)
        expect(idle.properties.team).toBe("blue")
        expect(idle.properties.idleReason).toBe("available")
        expect(idle.properties.completedStatus).toBe("resolved")

        const completed = events.find((event) => event.type === "swarm.task.completed")
        expect(completed.properties.task.id).toBe(task.id)
        expect(completed.properties.completedBy).toBe("tester")

        yield* swarm.cancel(worker.workerID)
      } finally {
        unsubscribe()
      }
    }),
  )

  it.instance("fans out durable swarm events written by another process", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const bus = yield* Bus.Service
      const events: any[] = []
      const unsubscribe = yield* bus.subscribeAllCallback((event) => {
        events.push(event)
      })
      const parentSessionID = SessionID.descending()

      try {
        yield* swarm.list(parentSessionID)
        const team = {
          parentSessionID,
          name: "remote-blue",
          description: "created remotely",
          leadSessionID: parentSessionID,
          agentType: "general",
          createdAt: Date.now(),
          updatedAt: Date.now(),
          workerIDs: [],
        }
        yield* SwarmMailbox.appendEvent({
          id: "evt_remote_swarm_team_created",
          type: "swarm.team.created",
          originID: "remote-host:444:remote-runtime",
          properties: {
            parentSessionID,
            name: "remote-blue",
            team,
          },
        })

        yield* Effect.gen(function* () {
          while (!events.some((event) => event.id === "evt_remote_swarm_team_created")) {
            yield* Effect.sleep("10 millis")
          }
        }).pipe(Effect.timeout("2 seconds"))

        const remote = events.find((event) => event.id === "evt_remote_swarm_team_created")
        expect(remote?.type).toBe("swarm.team.created")
        expect(remote?.properties.team.name).toBe("remote-blue")
      } finally {
        unsubscribe()
      }
    }),
  )

  it.instance("wakes durable event fanout through the swarm peer socket", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const ctx = yield* InstanceState.context
      const bus = yield* Bus.Service
      const events: any[] = []
      const unsubscribe = yield* bus.subscribeAllCallback((event) => {
        events.push(event)
      })
      const parentSessionID = SessionID.descending()

      try {
        yield* swarm.list(parentSessionID)
        const team = {
          parentSessionID,
          name: "peer-blue",
          description: "created through peer wake",
          leadSessionID: parentSessionID,
          agentType: "general",
          createdAt: Date.now(),
          updatedAt: Date.now(),
          workerIDs: [],
        }
        const eventID = "evt_peer_swarm_team_created"
        yield* SwarmMailbox.appendEvent({
          id: eventID,
          type: "swarm.team.created",
          originID: "remote-host:445:remote-runtime",
          properties: {
            parentSessionID,
            name: "peer-blue",
            team,
          },
        })
        yield* SwarmPeer.notifyOwner(ctx, SwarmMailbox.currentOwnerID(), {
          type: "event-log",
          eventID,
        })

        yield* Effect.gen(function* () {
          while (!events.some((event) => event.id === eventID)) {
            yield* Effect.sleep("10 millis")
          }
        }).pipe(Effect.timeout("1 second"))

        const remote = events.find((event) => event.id === eventID)
        expect(remote?.type).toBe("swarm.team.created")
        expect(remote?.properties.team.name).toBe("peer-blue")
      } finally {
        unsubscribe()
      }
    }),
  )

  it.instance("manages shared team tasks, dependencies, owner assignment, and deletion", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      yield* swarm.createTeam({ parentSessionID, name: "blue" })
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "tester",
        team: "blue",
        prompt: "wait for assignments",
        description: "tester",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const setup = yield* swarm.createTask({
        parentSessionID,
        team: "blue",
        subject: "Prepare fixture",
        description: "Create the test fixture",
      })
      const test = yield* swarm.createTask({
        parentSessionID,
        team: "blue",
        subject: "Run tests",
        description: "Run the suite",
        owner: "tester",
      })
      const waiter = yield* swarm.awaitInput(worker.workerID).pipe(Effect.forkScoped)
      const assignment = yield* Fiber.join(waiter).pipe(Effect.timeout("1 second"))
      expect(assignment.summary).toBe("assigned task #2")
      expect(assignment.message).toContain('"type": "task_assignment"')
      expect(assignment.message).toContain('"taskId": "2"')
      expect(assignment.message).toContain("Run tests")

      const updated = yield* swarm.updateTeamTask({
        parentSessionID,
        team: "blue",
        taskID: test.id,
        status: "in_progress",
        addBlockedBy: [setup.id],
      })
      expect(updated.success).toBe(true)
      expect(updated.updatedFields).toContain("status")
      expect(updated.updatedFields).toContain("blockedBy")

      let tasks = yield* swarm.listTeamTasks({ parentSessionID, team: "blue" })
      expect(tasks.map((task) => [task.id, task.status, task.owner, task.blockedBy])).toEqual([
        [setup.id, "pending", undefined, []],
        [test.id, "in_progress", "tester", [setup.id]],
      ])

      yield* swarm.updateTeamTask({
        parentSessionID,
        team: "blue",
        taskID: setup.id,
        status: "completed",
      })
      tasks = yield* swarm.listTeamTasks({ parentSessionID, team: "blue" })
      expect(tasks.find((task) => task.id === test.id)?.blockedBy).toEqual([])

      const deleted = yield* swarm.updateTeamTask({
        parentSessionID,
        team: "blue",
        taskID: test.id,
        status: "deleted",
      })
      expect(deleted.success).toBe(true)
      expect((yield* swarm.getTeamTask({ parentSessionID, team: "blue", taskID: test.id }))).toBeUndefined()

      yield* swarm.cancel(worker.workerID)
    }),
  )

  it.instance("lets idle team workers auto-claim unowned unblocked tasks", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      yield* swarm.createTeam({ parentSessionID, name: "blue" })
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "tester",
        team: "blue",
        prompt: "wait for work",
        description: "tester",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const waiter = yield* swarm.awaitInput(worker.workerID).pipe(Effect.forkScoped)
      yield* Effect.sleep("10 millis")
      expect((yield* swarm.get(worker.workerID))?.status).toBe("idle")

      const task = yield* swarm.createTask({
        parentSessionID,
        team: "blue",
        subject: "Run tests",
        description: "Run the suite",
      })
      const assignment = yield* Fiber.join(waiter).pipe(Effect.timeout("1 second"))

      expect(assignment.from).toBe("task_board")
      expect(assignment.summary).toBe(`claimed task #${task.id}`)
      expect(assignment.message).toContain('"type": "task_assignment"')
      expect(assignment.message).toContain(`"taskId": "${task.id}"`)
      expect(assignment.message).toContain(`task #${task.id}`)
      expect(assignment.message).toContain("Run tests")

      const claimed = yield* swarm.getTeamTask({ parentSessionID, team: "blue", taskID: task.id })
      expect(claimed?.owner).toBe("tester")
      expect(claimed?.status).toBe("in_progress")
      expect((yield* swarm.get(worker.workerID))?.status).toBe("running")

      yield* swarm.cancel(worker.workerID)
      const released = yield* swarm.getTeamTask({ parentSessionID, team: "blue", taskID: task.id })
      expect(released?.owner).toBeUndefined()
      expect(released?.status).toBe("pending")
    }),
  )

  it.instance("persists worker, mailbox, team, and task-board snapshots", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const storage = yield* Storage.Service
      const ctx = yield* InstanceState.context
      const parentSessionID = SessionID.descending()
      const teamName = `persist-${parentSessionID}`
      yield* swarm.createTeam({ parentSessionID, name: teamName })
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "tester",
        team: teamName,
        prompt: "wait",
        description: "tester",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      yield* swarm.createTask({
        parentSessionID,
        team: teamName,
        subject: "Run tests",
        description: "Run the suite",
      })
      yield* swarm.sendInput({
        parentSessionID,
        to: "tester",
        message: "start task #1",
        summary: "assignment",
        from: "team-lead",
      })

      const persisted = yield* storage.read<any>(["swarm", encodeURIComponent(ctx.project.id)])
      expect(persisted.version).toBe(1)
      expect(
        persisted.teams
          .filter((team: any) => team.parentSessionID === parentSessionID)
          .map((team: any) => team.name),
      ).toEqual([teamName])
      expect(persisted.tasks[`${parentSessionID}:${teamName}`].map((task: any) => task.subject)).toEqual([
        "Run tests",
      ])
      const persistedWorker = persisted.workers.find((item: any) => item.state.spec.workerID === worker.workerID)
      expect(persistedWorker.state.spec.name).toBe("tester")
      expect(persistedWorker.mailbox.map((input: any) => input.message)).toEqual(["start task #1"])

      yield* swarm.reload()

      const reloadedTeams = yield* swarm.listTeams(parentSessionID)
      expect(reloadedTeams.map((team) => team.name)).toEqual([teamName])
      const reloadedTasks = yield* swarm.listTeamTasks({ parentSessionID, team: teamName })
      expect(reloadedTasks.map((task) => task.subject)).toEqual(["Run tests"])
      const reloadedWorker = yield* swarm.get(worker.workerID)
      expect(reloadedWorker?.status).toBe("interrupted")
      expect(reloadedWorker?.mailboxSize).toBe(1)
      expect(reloadedWorker?.lastProgress).toBe("runtime restarted before worker completed")

      yield* swarm.cancel(worker.workerID)
    }),
  )

  it.instance("reload marks idle workers interrupted instead of leaving ghost consumers", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "idle-worker",
        team: "blue",
        prompt: "wait",
        description: "idle worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      yield* swarm.recordResult(worker.workerID, { status: "completed", text: "last turn done" })
      expect((yield* swarm.get(worker.workerID))?.status).toBe("idle")
      const task = yield* swarm.createTask({
        parentSessionID,
        team: "blue",
        subject: "Finish work",
        description: "Finish interrupted work",
        owner: "idle-worker",
      })
      yield* swarm.updateTeamTask({
        parentSessionID,
        team: "blue",
        taskID: task.id,
        status: "in_progress",
      })

      yield* swarm.reload()

      const reloaded = yield* swarm.get(worker.workerID)
      expect(reloaded?.status).toBe("interrupted")
      expect(reloaded?.lastProgress).toBe("runtime restarted before worker completed")
      const released = yield* swarm.getTeamTask({ parentSessionID, team: "blue", taskID: task.id })
      expect(released?.owner).toBeUndefined()
      expect(released?.status).toBe("pending")
      const sendExit = yield* swarm
        .sendInput({
          parentSessionID,
          to: "idle-worker",
          message: "continue",
        })
        .pipe(Effect.exit)
      expect(Exit.isFailure(sendExit)).toBe(true)
    }),
  )

  it.instance(
    "does not let another same-process instance mark live workers interrupted",
    () =>
      Effect.gen(function* () {
        const swarm = yield* SwarmRuntime.Service
        const ctx = yield* InstanceState.context
        const siblingDir = path.join(ctx.directory, "packages", "nested")
        yield* Effect.promise(() => fs.mkdir(siblingDir, { recursive: true }))
        const siblingCtx = { ...ctx, directory: siblingDir }

        const parentSessionID = SessionID.descending()
        const worker = yield* swarm.spawn({
          parentSessionID,
          sessionID: SessionID.descending(),
          agent: "general",
          name: "same-process-worker",
          prompt: "wait",
          description: "same process worker",
          wait: false,
          executionStrategy: "persistent",
          run: Effect.never,
        })

        const seenFromSibling = yield* swarm.get(worker.workerID).pipe(Effect.provideService(InstanceRef, siblingCtx))

        expect(seenFromSibling?.status).toBe("running")
        expect(seenFromSibling?.lastProgress).not.toBe("runtime restarted before worker completed")

        const seenFromOwner = yield* swarm.get(worker.workerID)
        expect(seenFromOwner?.status).toBe("running")
        expect(seenFromOwner?.lastProgress).not.toBe("runtime restarted before worker completed")

        yield* swarm.cancel(worker.workerID)
      }),
    { git: true },
  )

  it.instance("keeps worker indexes stable during parallel waits and refreshes", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const workers = yield* Effect.forEach(
        ["arch", "runtime", "flow"],
        (name) =>
          swarm.spawn({
            parentSessionID,
            sessionID: SessionID.descending(),
            agent: "general",
            name,
            team: "research",
            prompt: `inspect ${name}`,
            description: `inspect ${name}`,
            wait: false,
            executionStrategy: "persistent",
            run: Effect.never,
          }),
        { concurrency: "unbounded" },
      )

      const waited = yield* Effect.forEach(
        ["arch", "runtime", "flow"],
        (name) => swarm.wait({ parentSessionID, to: name, timeoutMS: 5 }),
        { concurrency: "unbounded" },
      )
      const listed = yield* swarm.list(parentSessionID)

      expect(waited.map((worker) => worker.spec.name).sort()).toEqual(["arch", "flow", "runtime"])
      expect(listed.map((worker) => worker.spec.name).sort()).toEqual(["arch", "flow", "runtime"])
      expect(listed.every((worker) => worker.lastProgress !== "runtime restarted before worker completed")).toBe(true)

      yield* Effect.forEach(workers, (worker) => swarm.cancel(worker.workerID), {
        concurrency: "unbounded",
        discard: true,
      })
    }),
  )

  it.instance("reload preserves live workers owned by another process and routes messages through their inbox", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const ctx = yield* InstanceState.context
      const parentSessionID = SessionID.descending()
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "remote-worker",
        team: "blue",
        prompt: "wait",
        description: "remote worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const snapshot = yield* swarm.get(worker.workerID)
      if (!snapshot) throw new Error("missing worker")

      const heartbeatPath = SwarmMailbox.workerHeartbeatPath(ctx, worker.workerID)
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(heartbeatPath), { recursive: true })
        await fs.writeFile(
          heartbeatPath,
          JSON.stringify(
            {
              version: 1,
              workerID: worker.workerID,
              ownerID: "remote-host:999:remote-owner",
              pid: 999,
              hostname: "remote-host",
              parentSessionID,
              sessionID: worker.sessionID,
              name: "remote-worker",
              team: "blue",
              status: "running",
              updatedAt: Date.now(),
            },
            null,
            2,
          ),
          "utf-8",
        )
      })

      yield* swarm.reload()

      const reloaded = yield* swarm.get(worker.workerID)
      expect(reloaded?.status).toBe("running")
      expect(reloaded?.lastProgress).not.toBe("runtime restarted before worker completed")
      if (!reloaded) throw new Error("missing reloaded worker")

      const sent = yield* swarm.sendInput({
        parentSessionID,
        to: "remote-worker",
        message: "remote process should receive this",
        summary: "remote delivery",
        from: "team-lead",
      })
      const delivered = yield* SwarmMailbox.takeInput(reloaded)

      expect(delivered?.id).toBe(sent.id)
      expect(delivered?.message).toBe("remote process should receive this")
      expect(delivered?.summary).toBe("remote delivery")
    }),
  )

  it.instance("notifies a remote worker owner through the swarm peer socket after queueing inbox input", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const ctx = yield* InstanceState.context
      const parentSessionID = SessionID.descending()
      const remoteOwnerID = "remote-host:998:peer-owner"
      const socketPath = SwarmPeer.socketPathForOwner(ctx, remoteOwnerID)
      const capture = yield* Effect.promise(async () => {
        if (process.platform !== "win32") {
          await fs.rm(socketPath, { force: true })
          await fs.mkdir(path.dirname(socketPath), { recursive: true })
        }
        return startPeerCapture(socketPath)
      })

      try {
        const worker = yield* swarm.spawn({
          parentSessionID,
          sessionID: SessionID.descending(),
          agent: "general",
          name: "remote-peer",
          team: "blue",
          prompt: "wait",
          description: "remote peer worker",
          wait: false,
          executionStrategy: "persistent",
          run: Effect.never,
        })
        const heartbeatPath = SwarmMailbox.workerHeartbeatPath(ctx, worker.workerID)
        yield* Effect.promise(async () => {
          await fs.mkdir(path.dirname(heartbeatPath), { recursive: true })
          await fs.writeFile(
            heartbeatPath,
            JSON.stringify(
              {
                version: 1,
                workerID: worker.workerID,
                ownerID: remoteOwnerID,
                pid: 998,
                hostname: "remote-host",
                parentSessionID,
                sessionID: worker.sessionID,
                name: "remote-peer",
                team: "blue",
                status: "running",
                updatedAt: Date.now(),
              },
              null,
              2,
            ),
            "utf-8",
          )
        })
        yield* SwarmPeer.registerPeer(ctx, {
          ownerID: remoteOwnerID,
          pid: 998,
          hostname: "remote-host",
          socketPath,
        })

        yield* swarm.reload()
        const sent = yield* swarm.sendInput({
          parentSessionID,
          to: "remote-peer",
          message: "peer socket should wake you",
          summary: "peer wake",
          from: "team-lead",
        })

        yield* Effect.gen(function* () {
          while (!capture.messages.some((message) => message.type === "inbox" && message.inputID === sent.id)) {
            yield* Effect.sleep("10 millis")
          }
        }).pipe(Effect.timeout("1 second"))

        const notification = capture.messages.find((message) => message.type === "inbox" && message.inputID === sent.id)
        if (notification?.type !== "inbox") throw new Error("missing inbox peer notification")
        expect(notification.workerID).toBe(worker.workerID)
        expect(notification?.originID).toBe(SwarmMailbox.currentOwnerID())
      } finally {
        yield* Effect.promise(() => closePeerCapture(capture.server))
        if (process.platform !== "win32") yield* Effect.promise(() => fs.rm(socketPath, { force: true }))
      }
    }),
  )

  it.instance("refreshes workers spawned by another process without an explicit reload", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const storage = yield* Storage.Service
      const ctx = yield* InstanceState.context
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()
      const workerID = WorkerID.ascending()
      const now = Date.now()

      expect(yield* swarm.list(parentSessionID)).toEqual([])

      yield* storage.write(["swarm", encodeURIComponent(ctx.project.id)], {
        version: 1,
        workers: [
          {
            state: {
              spec: {
                workerID,
                parentSessionID,
                sessionID,
                agent: "general",
                name: "late-remote",
                team: "blue",
                prompt: "inspect late work",
                description: "late remote worker",
                contextStrategy: "auto",
                permissionStrategy: "bubble",
                executionStrategy: "persistent",
                backend: "in-process",
              },
              status: "running",
              startedAt: now,
              updatedAt: now,
            },
            mailbox: [],
          },
        ],
        teams: [
          {
            parentSessionID,
            name: "blue",
            description: "remote team",
            leadSessionID: parentSessionID,
            agentType: "general",
            createdAt: now,
            updatedAt: now,
          },
        ],
        tasks: {},
        taskSeq: {},
      })

      const heartbeatPath = SwarmMailbox.workerHeartbeatPath(ctx, workerID)
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(heartbeatPath), { recursive: true })
        await fs.writeFile(
          heartbeatPath,
          JSON.stringify(
            {
              version: 1,
              workerID,
              ownerID: "remote-host:222:late-worker",
              pid: 222,
              hostname: "remote-host",
              parentSessionID,
              sessionID,
              name: "late-remote",
              team: "blue",
              status: "running",
              updatedAt: Date.now(),
            },
            null,
            2,
          ),
          "utf-8",
        )
      })

      const workers = yield* swarm.list(parentSessionID)
      expect(workers.map((worker) => worker.spec.name)).toEqual(["late-remote"])
      expect((yield* swarm.listTeams(parentSessionID)).map((team) => team.name)).toEqual(["blue"])

      const sent = yield* swarm.sendInput({
        parentSessionID,
        to: "late-remote",
        message: "visible without reload",
        summary: "late follow-up",
        from: "team-lead",
      })
      const delivered = yield* SwarmMailbox.takeInput(workers[0]!)

      expect(delivered?.id).toBe(sent.id)
      expect(delivered?.message).toBe("visible without reload")
      expect(delivered?.summary).toBe("late follow-up")
    }),
  )

  it.instance("preserves remote workers when local writes persist refreshed state", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const storage = yield* Storage.Service
      const ctx = yield* InstanceState.context
      const parentSessionID = SessionID.descending()
      const sessionID = SessionID.descending()
      const workerID = WorkerID.ascending()
      const now = Date.now()

      expect(yield* swarm.list(parentSessionID)).toEqual([])

      yield* storage.write(["swarm", encodeURIComponent(ctx.project.id)], {
        version: 1,
        workers: [
          {
            state: {
              spec: {
                workerID,
                parentSessionID,
                sessionID,
                agent: "general",
                name: "remote-preserved",
                team: "blue",
                prompt: "stay visible",
                description: "remote preserved worker",
                contextStrategy: "auto",
                permissionStrategy: "bubble",
                executionStrategy: "persistent",
                backend: "in-process",
              },
              status: "running",
              startedAt: now,
              updatedAt: now,
            },
            mailbox: [],
          },
        ],
        teams: [
          {
            parentSessionID,
            name: "blue",
            description: "remote team",
            leadSessionID: parentSessionID,
            agentType: "general",
            createdAt: now,
            updatedAt: now,
          },
        ],
        tasks: {},
        taskSeq: {},
      })

      const heartbeatPath = SwarmMailbox.workerHeartbeatPath(ctx, workerID)
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(heartbeatPath), { recursive: true })
        await fs.writeFile(
          heartbeatPath,
          JSON.stringify(
            {
              version: 1,
              workerID,
              ownerID: "remote-host:333:preserved-worker",
              pid: 333,
              hostname: "remote-host",
              parentSessionID,
              sessionID,
              name: "remote-preserved",
              team: "blue",
              status: "running",
              updatedAt: Date.now(),
            },
            null,
            2,
          ),
          "utf-8",
        )
      })

      const task = yield* swarm.createTask({
        parentSessionID,
        team: "blue",
        subject: "Local follow-up",
        description: "A local write after remote worker appears",
      })
      const persisted = yield* storage.read<any>(["swarm", encodeURIComponent(ctx.project.id)])

      expect(persisted.workers.map((item: any) => item.state.spec.name)).toContain("remote-preserved")
      expect(persisted.tasks[`${parentSessionID}:blue`].map((item: any) => item.subject)).toEqual([
        "Local follow-up",
      ])
      expect((yield* swarm.getTeamTask({ parentSessionID, team: "blue", taskID: task.id }))?.subject).toBe(
        "Local follow-up",
      )
    }),
  )
})
