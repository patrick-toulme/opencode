import { Config } from "@/config/config"
import { Effect, Schema } from "effect"
import * as Tool from "./tool"

export const RemoteTriggerParameters = Schema.Struct({
  action: Schema.Literals(["list", "get", "create", "update", "run"]).annotate({
    description: "Remote trigger action to perform.",
  }),
  trigger_id: Schema.optional(
    Schema.String.check(Schema.isPattern(/^[\w-]+$/)).annotate({
      description: "Remote trigger ID. Required for get, update, and run.",
    }),
  ),
  body: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)).annotate({
    description: "JSON body for create and update.",
  }),
})

type RemoteTriggerMetadata = {
  action: string
  status: number
  triggerID?: string
}

export const RemoteTriggerTool = Tool.define(
  "remote_trigger",
  Effect.gen(function* () {
    const config = yield* Config.Service

    return {
      description:
        "Manage remote scheduled agent triggers. Uses a provider-neutral HTTP trigger endpoint configured with experimental.swarm_remote_trigger_endpoint, OPENCODE_SWARM_REMOTE_TRIGGER_ENDPOINT, or the remote swarm endpoint.",
      parameters: RemoteTriggerParameters,
      execute: (args: Schema.Schema.Type<typeof RemoteTriggerParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const cfg = yield* config.get()
          const endpoint = pickString(
            ctx.extra?.swarmRemoteTriggerEndpoint,
            process.env.OPENCODE_SWARM_REMOTE_TRIGGER_ENDPOINT,
            cfg.experimental?.swarm_remote_trigger_endpoint,
            ctx.extra?.swarmRemoteEndpoint,
            process.env.OPENCODE_SWARM_REMOTE_ENDPOINT,
            cfg.experimental?.swarm_remote_endpoint,
          )
          if (!endpoint) {
            return yield* Effect.fail(
              new Error(
                "remote_trigger requires experimental.swarm_remote_trigger_endpoint, OPENCODE_SWARM_REMOTE_TRIGGER_ENDPOINT, experimental.swarm_remote_endpoint, or OPENCODE_SWARM_REMOTE_ENDPOINT",
              ),
            )
          }
          const token = pickString(
            ctx.extra?.swarmRemoteTriggerToken,
            process.env.OPENCODE_SWARM_REMOTE_TRIGGER_TOKEN,
            cfg.experimental?.swarm_remote_trigger_token,
            ctx.extra?.swarmRemoteToken,
            process.env.OPENCODE_SWARM_REMOTE_TOKEN,
            cfg.experimental?.swarm_remote_token,
          )

          const request = buildRequest(args)
          const response = yield* requestJSON({
            endpoint,
            token,
            route: request.route,
            method: request.method,
            body: request.body,
            abort: ctx.abort,
          })
          const output = `HTTP ${response.status}\n${response.text}`
          return {
            title: "Remote trigger",
            metadata: {
              action: args.action,
              status: response.status,
              ...(args.trigger_id ? { triggerID: args.trigger_id } : {}),
            } satisfies RemoteTriggerMetadata,
            output,
          }
        }).pipe(Effect.orDie),
    }
  }),
)

function buildRequest(args: Schema.Schema.Type<typeof RemoteTriggerParameters>): {
  method: "GET" | "POST"
  route: string
  body?: unknown
} {
  switch (args.action) {
    case "list":
      return { method: "GET", route: "/triggers" }
    case "get":
      if (!args.trigger_id) throw new Error("get requires trigger_id")
      return { method: "GET", route: `/triggers/${encodeURIComponent(args.trigger_id)}` }
    case "create":
      if (!args.body) throw new Error("create requires body")
      return { method: "POST", route: "/triggers", body: args.body }
    case "update":
      if (!args.trigger_id) throw new Error("update requires trigger_id")
      if (!args.body) throw new Error("update requires body")
      return { method: "POST", route: `/triggers/${encodeURIComponent(args.trigger_id)}`, body: args.body }
    case "run":
      if (!args.trigger_id) throw new Error("run requires trigger_id")
      return { method: "POST", route: `/triggers/${encodeURIComponent(args.trigger_id)}/run`, body: {} }
  }
}

function requestJSON(input: {
  endpoint: string
  token?: string
  route: string
  method: "GET" | "POST"
  body?: unknown
  abort: AbortSignal
}) {
  return Effect.tryPromise({
    try: async () => {
      const response = await fetch(url(input.endpoint, input.route), {
        method: input.method,
        signal: input.abort,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          ...(input.token ? { authorization: `Bearer ${input.token}` } : {}),
        },
        ...(input.body === undefined ? {} : { body: JSON.stringify(input.body) }),
      })
      const text = await response.text().catch(() => "")
      const formatted = formatResponseText(text)
      if (!response.ok) {
        throw new Error(`Remote trigger request failed (${response.status}): ${formatted || response.statusText}`)
      }
      return {
        status: response.status,
        text: formatted,
      }
    },
    catch: (error) => (error instanceof Error ? error : new Error(String(error))),
  })
}

function formatResponseText(text: string) {
  if (!text) return "{}"
  try {
    return JSON.stringify(JSON.parse(text))
  } catch {
    return text
  }
}

function url(endpoint: string, route: string) {
  const base = endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint
  return `${base}${route}`
}

function pickString(...values: unknown[]) {
  for (const value of values) {
    if (typeof value === "string" && value.length > 0) return value
  }
  return undefined
}

export * as RemoteTrigger from "./remote_trigger"
