import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Agent } from "@/agent/agent"
import { MessageID, SessionID } from "@/session/schema"
import { RemoteTriggerTool } from "@/tool/remote_trigger"
import { Tool } from "@/tool/tool"
import { Truncate } from "@/tool/truncate"
import { disposeAllInstances, provideTmpdirInstance } from "../fixture/fixture"
import { TestConfig } from "../fixture/config"
import { testEffect } from "../lib/effect"

let configuredEndpoint = ""

const it = testEffect(
  Layer.mergeAll(
    TestConfig.layer({
      get: () =>
        Effect.succeed({
          experimental: {
            swarm_remote_trigger_endpoint: configuredEndpoint,
            swarm_remote_trigger_token: "cfg-token",
          },
        }),
    }),
    Truncate.defaultLayer,
    Agent.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
  ),
)

const ctx: Tool.Context = {
  sessionID: SessionID.make("ses_test"),
  messageID: MessageID.make("msg_test"),
  callID: "",
  agent: "build",
  abort: new AbortController().signal,
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
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

afterEach(async () => {
  await disposeAllInstances()
})

describe("tool.remote_trigger", () => {
  it.live("routes trigger actions through configured endpoint and token", () =>
    provideTmpdirInstance(() =>
      Effect.gen(function* () {
        const calls: Array<{ method: string; path: string; auth: string | null; body: unknown }> = []
        const info = yield* RemoteTriggerTool
        const tool = yield* info.init()

        yield* withRemoteServer(
          async (request) => {
            calls.push({
              method: request.method,
              path: new URL(request.url).pathname,
              auth: request.headers.get("authorization"),
              body: request.method === "GET" ? undefined : await request.json(),
            })
            return Response.json({ ok: true, path: new URL(request.url).pathname }, { status: 202 })
          },
          (endpoint) =>
            Effect.gen(function* () {
              configuredEndpoint = endpoint
              yield* tool.execute({ action: "create", body: { prompt: "run checks" } }, ctx)
              const run = yield* tool.execute({ action: "run", trigger_id: "trigger-1" }, ctx)

              expect(run.output).toBe('HTTP 202\n{"ok":true,"path":"/triggers/trigger-1/run"}')
            }),
        )

        expect(calls).toEqual([
          {
            method: "POST",
            path: "/triggers",
            auth: "Bearer cfg-token",
            body: { prompt: "run checks" },
          },
          {
            method: "POST",
            path: "/triggers/trigger-1/run",
            auth: "Bearer cfg-token",
            body: {},
          },
        ])
      }),
    ),
  )
})
