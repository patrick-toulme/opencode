import { afterEach, describe, expect } from "bun:test"
import fs from "fs/promises"
import os from "os"
import path from "path"
import { Effect, Exit, Fiber, Layer } from "effect"
import { Agent } from "../../src/agent/agent"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { PermissionID } from "@/permission/schema"
import { Plugin } from "@/plugin"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Session } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"
import type { SessionPrompt } from "../../src/session/prompt"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { ModelID, ProviderID } from "../../src/provider/schema"
import { TaskTool, type TaskPromptOps } from "../../src/tool/task"
import { ListPeersTool, SendMessageTool } from "../../src/tool/send_message"
import {
  BroadcastTool,
  CancelTaskTool,
  CreateTaskTool,
  CreateTeamTool,
  DeleteTeamTool,
  GetTaskTool,
  ListTasksTool,
  ListTeamTasksTool,
  ListTeamsTool,
  ReadTaskOutputTool,
  StopTaskTool,
  UpdateTaskTool,
  WaitTaskTool,
} from "../../src/tool/task_control"
import { Truncate } from "@/tool/truncate"
import { ToolRegistry } from "@/tool/registry"
import { SwarmRuntime } from "@/swarm/runtime"
import { WorkerID } from "@/swarm/state"
import { InstanceState } from "@/effect/instance-state"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderID.make("test"),
  modelID: ModelID.make("test-model"),
}

const it = testEffect(
  Layer.mergeAll(
    Agent.defaultLayer,
    Config.defaultLayer,
    CrossSpawnSpawner.defaultLayer,
    Session.defaultLayer,
    Permission.defaultLayer,
    Plugin.defaultLayer,
    SwarmRuntime.defaultLayer,
    Truncate.defaultLayer,
    ToolRegistry.defaultLayer,
  ),
)

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function pluginHook(handler: (name: string, input: unknown, output: unknown) => void) {
  return Layer.mock(Plugin.Service)({
    trigger: <Name extends string, Input, Output>(name: Name, input: Input, output: Output) =>
      Effect.sync(() => {
        handler(name, input, output)
        return output
      }),
    list: () => Effect.succeed([]),
    init: () => Effect.void,
  })
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

const seed = Effect.fn("TaskToolTest.seed")(function* (title = "Pinned") {
  const session = yield* Session.Service
  const chat = yield* session.create({ title })
  const user = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const assistant: MessageV2.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    parentID: user.id,
    sessionID: chat.id,
    mode: "build",
    agent: "build",
    cost: 0,
    path: { cwd: "/tmp", root: "/tmp" },
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    time: { created: Date.now() },
  }
  yield* session.updateMessage(assistant)
  return { chat, assistant }
})

function stubOps(opts?: { onPrompt?: (input: SessionPrompt.PromptInput) => void; text?: string }): TaskPromptOps {
  return {
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        opts?.onPrompt?.(input)
        return reply(input, opts?.text ?? "done")
      }),
  }
}

function reply(input: SessionPrompt.PromptInput, text: string): MessageV2.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: input.messageID ?? MessageID.ascending(),
      sessionID: input.sessionID,
      mode: input.agent ?? "general",
      agent: input.agent ?? "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: input.model?.modelID ?? ref.modelID,
      providerID: input.model?.providerID ?? ref.providerID,
      time: { created: Date.now() },
      finish: "stop",
    },
    parts: [
      {
        id: PartID.ascending(),
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text,
      },
    ],
  }
}

describe("tool.task", () => {
  it.instance(
    "description sorts subagents by name and is stable across calls",
    () =>
      Effect.gen(function* () {
        const agent = yield* Agent.Service
        const build = yield* agent.get("build")
        const registry = yield* ToolRegistry.Service
        const get = Effect.fnUntraced(function* () {
          const tools = yield* registry.tools({ ...ref, agent: build })
          return tools.find((tool) => tool.id === TaskTool.id)?.description ?? ""
        })
        const first = yield* get()
        const second = yield* get()

        expect(first).toBe(second)

        const alpha = first.indexOf("- alpha: Alpha agent")
        const explore = first.indexOf("- explore:")
        const general = first.indexOf("- general:")
        const zebra = first.indexOf("- zebra: Zebra agent")

        expect(alpha).toBeGreaterThan(-1)
        expect(explore).toBeGreaterThan(alpha)
        expect(general).toBeGreaterThan(explore)
        expect(zebra).toBeGreaterThan(general)
      }),
    {
      config: {
        agent: {
          zebra: {
            description: "Zebra agent",
            mode: "subagent",
          },
          alpha: {
            description: "Alpha agent",
            mode: "subagent",
          },
        },
      },
    },
  )

  it.instance(
    "description hides denied subagents for the caller",
    () =>
      Effect.gen(function* () {
        const agent = yield* Agent.Service
        const build = yield* agent.get("build")
        const registry = yield* ToolRegistry.Service
        const description =
          (yield* registry.tools({ ...ref, agent: build })).find((tool) => tool.id === TaskTool.id)?.description ?? ""

        expect(description).toContain("- alpha: Alpha agent")
        expect(description).not.toContain("- zebra: Zebra agent")
      }),
    {
      config: {
        permission: {
          task: {
            "*": "allow",
            zebra: "deny",
          },
        },
        agent: {
          zebra: {
            description: "Zebra agent",
            mode: "subagent",
          },
          alpha: {
            description: "Alpha agent",
            mode: "subagent",
          },
        },
      },
    },
  )

  it.instance("execute resumes an existing task session from task_id", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "Existing child" })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let seen: SessionPrompt.PromptInput | undefined
      const promptOps = stubOps({ text: "resumed", onPrompt: (input) => (seen = input) })

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          task_id: child.id,
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const kids = yield* sessions.children(chat.id)
      expect(kids).toHaveLength(1)
      expect(kids[0]?.id).toBe(child.id)
      expect(result.metadata.sessionId).toBe(child.id)
      expect(result.metadata.workerId).toMatch(/^swa_/)
      expect(result.metadata.status).toBe("completed")
      expect(result.output).toContain(`task_id: ${child.id}`)
      expect(seen?.sessionID).toBe(child.id)

      const swarm = yield* SwarmRuntime.Service
      const worker = yield* swarm.getBySession(child.id)
      expect(worker?.spec.workerID).toBe(result.metadata.workerId)
      expect(worker?.spec.parentSessionID).toBe(chat.id)
      expect(worker?.spec.agent).toBe("general")
      expect(worker?.status).toBe("completed")
      expect(worker?.result?.text).toBe("resumed")
    }),
  )

  it.instance("omitted subagent_type resumes an existing explicit worker instead of forking", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const swarm = yield* SwarmRuntime.Service
      const { chat, assistant } = yield* seed()
      const child = yield* sessions.create({ parentID: chat.id, title: "Existing explicit child" })
      const existing = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: child.id,
        agent: "general",
        name: "explicit-worker",
        prompt: "original",
        description: "explicit worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const finish = defer<void>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(async () => {
            ready.resolve(input)
            await finish.promise
            return reply(input, "resumed-explicit")
          }),
      }

      const result = yield* def.execute(
        {
          description: "resume worker",
          prompt: "continue explicit worker",
          task_id: child.id,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.metadata.fork).toBeUndefined()
      expect(result.output).toContain("Background subagent launched.")
      const input = yield* Effect.promise(() => ready.promise)
      expect(input.agent).toBe("general")
      const text = input.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
      expect(text).toContain("You are a long-lived background subagent")
      expect(text).not.toContain("<fork-boilerplate>")

      const worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.spec.agent).toBe("general")
      expect(worker?.spec.fork).toBeUndefined()

      finish.resolve()
      yield* swarm.cancel(result.metadata.workerId)
      yield* swarm.cancel(existing.workerID)
    }),
  )

  it.instance("ignores run_in_background=false unless the caller is internal", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps = stubOps({ text: "background result" })

      const result = yield* def.execute(
        {
          description: "model foreground request",
          prompt: "try to run synchronously",
          subagent_type: "general",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.output).toContain("Background subagent launched.")
      expect(result.metadata.status).toBe("running")
      const worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.spec.executionStrategy).toBe("persistent")
      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance(
    "honors default background mode from subagent configuration",
    () =>
      Effect.gen(function* () {
        const swarm = yield* SwarmRuntime.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()

        const result = yield* def.execute(
          {
            description: "configured background",
            prompt: "stay addressable",
            subagent_type: "background-reviewer",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps(), allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.output).toContain("Background subagent launched.")
        expect(result.metadata.status).toBe("running")
        const worker = yield* swarm.get(result.metadata.workerId)
        expect(worker?.spec.executionStrategy).toBe("persistent")
        yield* swarm.cancel(result.metadata.workerId)
      }),
    {
      config: {
        agent: {
          "background-reviewer": {
            mode: "subagent",
            background: true,
          },
        },
      },
    },
  )

  it.instance("execute asks by default and skips checks when bypassed", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const calls: unknown[] = []
      const promptOps = stubOps()

      const exec = (extra?: Record<string, any>) =>
        def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true, ...extra },
            messages: [],
            metadata: () => Effect.void,
            ask: (input) =>
              Effect.sync(() => {
                calls.push(input)
              }),
          },
        )

      yield* exec()
      yield* exec({ bypassAgentCheck: true })

      expect(calls).toHaveLength(1)
      expect(calls[0]).toEqual({
        permission: "task",
        patterns: ["general"],
        always: ["*"],
        metadata: {
          description: "inspect bug",
          subagent_type: "general",
        },
      })
    }),
  )

  it.instance("execute cancels child session when abort signal fires", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const cancelled = defer<SessionID>()
      const abort = new AbortController()
      const promptOps: TaskPromptOps = {
        cancel: (sessionID) =>
          Effect.sync(() => {
            cancelled.resolve(sessionID)
          }),
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(() => {
            ready.resolve(input)
            return cancelled.promise
          }).pipe(Effect.as(reply(input, "cancelled"))),
      }

      const fiber = yield* def
        .execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: abort.signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.forkChild)

      const input = yield* Effect.promise(() => ready.promise)
      abort.abort()
      expect(yield* Effect.promise(() => cancelled.promise)).toBe(input.sessionID)

      const exit = yield* Fiber.await(fiber)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value.metadata.status).toBe("cancelled")

      const swarm = yield* SwarmRuntime.Service
      const worker = yield* swarm.getBySession(input.sessionID)
      expect(worker?.status).toBe("cancelled")
    }),
  )

  it.instance("execute creates a child when task_id does not exist", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let seen: SessionPrompt.PromptInput | undefined
      const promptOps = stubOps({ text: "created", onPrompt: (input) => (seen = input) })

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          task_id: "ses_missing",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const kids = yield* sessions.children(chat.id)
      expect(kids).toHaveLength(1)
      expect(kids[0]?.id).toBe(result.metadata.sessionId)
      expect(result.metadata.sessionId).not.toBe("ses_missing")
      expect(result.output).toContain(`task_id: ${result.metadata.sessionId}`)
      expect(seen?.sessionID).toBe(result.metadata.sessionId)
    }),
  )

  it.instance("forks parent context into new child sessions by default", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      if (!assistant.parentID) throw new Error("seed assistant must have a parent user")
      yield* sessions.updatePart({
        id: PartID.ascending(),
        messageID: assistant.parentID,
        sessionID: chat.id,
        type: "text",
        text: "parent context that the child should inherit",
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps = stubOps({ text: "created" })

      const result = yield* def.execute(
        {
          description: "inspect bug",
          prompt: "look into the cache key path",
          subagent_type: "general",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const childMessages = yield* MessageV2.filterCompactedEffect(result.metadata.sessionId)
      const childText = childMessages.flatMap((message) => message.parts).filter((part) => part.type === "text")
      expect(childText.some((part) => part.text === "parent context that the child should inherit")).toBe(true)
      expect(
        childText.some(
          (part) =>
            part.synthetic === true &&
            part.metadata?.kind === "fork-context-notice" &&
            part.text.includes("inherited the parent agent's conversation context"),
        ),
      ).toBe(true)

      const worker = yield* (yield* SwarmRuntime.Service).get(result.metadata.workerId)
      expect(worker?.spec.contextStrategy).toBe("auto")
    }),
  )

  it.instance("omitted subagent_type forks the current agent in the background", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      yield* sessions.updatePart({
        id: PartID.ascending(),
        messageID: assistant.id,
        sessionID: chat.id,
        type: "tool",
        callID: "call_fork_prefix",
        tool: "task",
        state: {
          status: "running",
          input: { description: "audit fork", prompt: "inspect cache path" },
          title: "audit fork",
          time: { start: Date.now() },
        },
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const finish = defer<void>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(async () => {
            ready.resolve(input)
            await finish.promise
            return reply(input, "fork-result")
          }),
      }

      const result = yield* def.execute(
        {
          description: "audit fork",
          prompt: "inspect cache path",
          name: "audit",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.output).toContain("Background fork launched.")
      expect(result.metadata.fork).toBe(true)
      expect(result.metadata.status).toBe("running")

      const input = yield* Effect.promise(() => ready.promise)
      const text = input.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
      expect(text).toContain("<fork-boilerplate>")
      expect(text).toContain("Your directive: inspect cache path")
      expect(text).toContain("Do NOT spawn sub-agents")
      expect(text).toContain('Your response MUST begin with "Scope:"')
      expect(text).toContain("commit your changes before reporting")
      expect(text).toContain("You are a long-lived background fork")

      const childMessages = yield* MessageV2.filterCompactedEffect(result.metadata.sessionId)
      const currentAssistantClone = childMessages.find((message) => message.info.role === "assistant")
      const taskPart = currentAssistantClone?.parts.find(
        (part): part is MessageV2.ToolPart => part.type === "tool" && part.tool === "task",
      )
      expect(taskPart?.state.status).toBe("completed")
      if (taskPart?.state.status === "completed") {
        expect(taskPart.state.output).toBe("Fork started - processing in background")
      }

      const swarm = yield* SwarmRuntime.Service
      const worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.spec.agent).toBe("build")
      expect(worker?.spec.executionStrategy).toBe("persistent")
      expect(worker?.spec.contextStrategy).toBe("fork")
      expect(worker?.spec.fork).toBe(true)
      expect(worker?.spec.name).toBe("audit")

      finish.resolve()
      for (let i = 0; i < 50; i++) {
        const current = yield* swarm.get(result.metadata.workerId)
        if (current?.status === "idle") break
        yield* Effect.sleep("10 millis")
      }
      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance("rejects implicit fork attempts from inside a fork child", () =>
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const { chat, assistant } = yield* seed()
      if (!assistant.parentID) throw new Error("seed assistant must have a parent user")
      yield* sessions.updatePart({
        id: PartID.ascending(),
        messageID: assistant.parentID,
        sessionID: chat.id,
        type: "text",
        text: "<fork-boilerplate>existing fork directive</fork-boilerplate>",
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps = stubOps()

      const exit = yield* def
        .execute(
          {
            description: "nested fork",
            prompt: "try to fork again",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
    }),
  )

  it.instance("rejects implicit fork attempts using the persisted fork worker marker", () =>
    Effect.gen(function* () {
      const swarm = yield* SwarmRuntime.Service
      const { chat, assistant } = yield* seed()
      const current = yield* swarm.spawn({
        parentSessionID: SessionID.descending(),
        sessionID: chat.id,
        agent: "build",
        prompt: "fork child",
        description: "fork child",
        wait: false,
        executionStrategy: "persistent",
        fork: true,
        run: Effect.never,
      })
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps = stubOps()

      const exit = yield* def
        .execute(
          {
            description: "nested fork",
            prompt: "try to fork again",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      yield* swarm.cancel(current.workerID)
    }),
  )

  it.instance("injects SubagentStart hook context before the subagent prompt", () => {
    let hookInput: { agentType?: string; description?: string; background?: boolean } | undefined
    let seenBeforePrompt = ""

    return Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.gen(function* () {
            const messages = yield* MessageV2.filterCompactedEffect(input.sessionID)
            seenBeforePrompt = messages
              .flatMap((message) => message.parts)
              .map((part) => (part.type === "text" ? part.text : ""))
              .join("\n")
            return reply(input, "started")
          }),
      }

      const result = yield* def.execute(
        {
          description: "hooked worker",
          prompt: "start with extra context",
          subagent_type: "general",
          context: "fresh",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.metadata.status).toBe("completed")
      expect(hookInput).toEqual({
        agentType: "general",
        description: "hooked worker",
        background: false,
      })
      expect(seenBeforePrompt).toContain('<hook-additional-context hook="SubagentStart">')
      expect(seenBeforePrompt).toContain("Use the fixture database before editing")

      const childMessages = yield* MessageV2.filterCompactedEffect(result.metadata.sessionId)
      const hookText = childMessages
        .flatMap((message) => message.parts)
        .map((part) => (part.type === "text" && part.metadata?.kind === "hook-additional-context" ? part.text : ""))
        .join("\n")
      expect(hookText).toContain("Use the fixture database before editing")
    }).pipe(
      Effect.provide(
        pluginHook((name, input, output) => {
          if (name !== "swarm.subagent.start") return
          const typedInput = input as { agentType: string; description: string; background: boolean }
          hookInput = {
            agentType: typedInput.agentType,
            description: typedInput.description,
            background: typedInput.background,
          }
          ;(output as { additionalContexts: string[] }).additionalContexts.push(
            "Use the fixture database before editing",
          )
        }),
      ),
    )
  })

  it.instance("lets SubagentStop hooks block foreground subagent completion", () => {
    let hookCount = 0
    let promptCount = 0
    let secondPromptText = ""
    let transcriptPath = ""
    const stopHookActiveValues: boolean[] = []
    const lastAssistantMessages: string[] = []

    return Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            promptCount++
            if (promptCount === 2) {
              secondPromptText = input.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
            }
            return reply(input, `turn-${promptCount}`)
          }),
      }

      const result = yield* def.execute(
        {
          description: "stop hooked worker",
          prompt: "finish once",
          subagent_type: "general",
          context: "fresh",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.output).toContain("turn-2")
      expect(promptCount).toBe(2)
      expect(hookCount).toBe(2)
      expect(secondPromptText).toContain("SubagentStop hook feedback")
      expect(secondPromptText).toContain("Include verification details before stopping")
      expect(stopHookActiveValues).toEqual([false, true])
      expect(lastAssistantMessages).toEqual(["turn-1", "turn-2"])

      const transcript = yield* Effect.promise(() => fs.stat(transcriptPath))
      expect(transcript.isFile()).toBe(true)
    }).pipe(
      Effect.provide(
        pluginHook((name, input, output) => {
          if (name !== "swarm.subagent.stop") return
          hookCount++
          const typedInput = input as {
            stopHookActive: boolean
            transcriptPath: string
            lastAssistantMessage?: string
          }
          transcriptPath = typedInput.transcriptPath
          stopHookActiveValues.push(typedInput.stopHookActive)
          lastAssistantMessages.push(typedInput.lastAssistantMessage ?? "")
          if (hookCount !== 1) return
          ;(output as { continue: boolean; message?: string }).continue = false
          ;(output as { continue: boolean; message?: string }).message = "Include verification details before stopping"
        }),
      ),
    )
  })

  it.instance(
    "runs worktree-isolated tasks in a git worktree context",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const parentInstance = yield* InstanceState.context
        const tool = yield* TaskTool
        const def = yield* tool.init()
        let seenDirectory: string | undefined
        let seenWorktree: string | undefined
        let existsDuringPrompt = false
        const promptOps: TaskPromptOps = {
          cancel: () => Effect.void,
          resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
          prompt: (input) =>
            Effect.gen(function* () {
              const instance = yield* InstanceState.context
              seenDirectory = instance.directory
              seenWorktree = instance.worktree
              existsDuringPrompt = yield* Effect.promise(() =>
                fs
                  .stat(instance.directory)
                  .then((stat) => stat.isDirectory())
                  .catch(() => false),
              )
              return reply(input, "isolated")
            }),
        }

        const result = yield* def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "general",
            isolation: "worktree",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(seenDirectory).toContain(".opencode/worktrees")
        expect(seenWorktree).toBe(seenDirectory)
        expect(existsDuringPrompt).toBe(true)
        expect(result.metadata.worktreePath).toBe(seenDirectory)
        expect(result.metadata.worktreeBranch).toMatch(/^opencode-agent-/)

        const existsAfterCompletion = yield* Effect.promise(() =>
          fs
            .stat(seenDirectory!)
            .then((stat) => stat.isDirectory())
            .catch(() => false),
        )
        expect(existsAfterCompletion).toBe(false)

        const swarm = yield* SwarmRuntime.Service
        const worker = yield* swarm.get(result.metadata.workerId)
        expect(worker?.spec.backend).toBe("worktree")
        expect(worker?.spec.worktreeRoot).toBe(parentInstance.worktree)
        expect(worker?.spec.worktreePath).toBe(seenDirectory)
        expect(worker?.spec.worktreeBranch).toBe(result.metadata.worktreeBranch)
      }),
    { git: true },
  )

  it.instance("uses cwd instead of failing when cwd and isolation are both provided", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const parentInstance = yield* InstanceState.context
      const tool = yield* TaskTool
      const def = yield* tool.init()
      let seenDirectory: string | undefined
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.gen(function* () {
            seenDirectory = (yield* InstanceState.context).directory
            return reply(input, "done")
          }),
      }

      const result = yield* def.execute(
        {
          description: "cwd scoped",
          prompt: "inspect this package",
          subagent_type: "general",
          cwd: ".",
          isolation: "worktree",
          run_in_background: false,
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(seenDirectory).toBe(parentInstance.directory)
      expect(result.metadata.worktreePath).toBeUndefined()

      const swarm = yield* SwarmRuntime.Service
      const worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.spec.backend).toBe("in-process")
    }),
  )

  it.instance("falls back to local execution when remote isolation has no endpoint", () =>
    Effect.gen(function* () {
      const oldBackend = process.env.OPENCODE_SWARM_BACKEND
      const oldEndpoint = process.env.OPENCODE_SWARM_REMOTE_ENDPOINT
      const oldToken = process.env.OPENCODE_SWARM_REMOTE_TOKEN
      try {
        process.env.OPENCODE_SWARM_BACKEND = "in-process"
        delete process.env.OPENCODE_SWARM_REMOTE_ENDPOINT
        delete process.env.OPENCODE_SWARM_REMOTE_TOKEN

        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()
        const result = yield* def.execute(
          {
            description: "local fallback",
            prompt: "research locally",
            subagent_type: "general",
            isolation: "remote",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps(), allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        const swarm = yield* SwarmRuntime.Service
        const worker = yield* swarm.get(result.metadata.workerId)
        expect(worker?.spec.backend).toBe("in-process")
        expect(result.metadata.remoteId).toBeUndefined()
      } finally {
        if (oldBackend === undefined) delete process.env.OPENCODE_SWARM_BACKEND
        else process.env.OPENCODE_SWARM_BACKEND = oldBackend
        if (oldEndpoint === undefined) delete process.env.OPENCODE_SWARM_REMOTE_ENDPOINT
        else process.env.OPENCODE_SWARM_REMOTE_ENDPOINT = oldEndpoint
        if (oldToken === undefined) delete process.env.OPENCODE_SWARM_REMOTE_TOKEN
        else process.env.OPENCODE_SWARM_REMOTE_TOKEN = oldToken
      }
    }),
  )

  it.instance(
    "honors default isolation from subagent configuration",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()
        let seenDirectory: string | undefined
        const promptOps: TaskPromptOps = {
          cancel: () => Effect.void,
          resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
          prompt: (input) =>
            Effect.gen(function* () {
              const instance = yield* InstanceState.context
              seenDirectory = instance.directory
              return reply(input, "done")
            }),
        }

        const result = yield* def.execute(
          {
            description: "configured isolation",
            prompt: "use the agent default worktree",
            subagent_type: "isolated-reviewer",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(seenDirectory).toContain(".opencode/worktrees")
        expect(result.metadata.worktreePath).toBe(seenDirectory)

        const swarm = yield* SwarmRuntime.Service
        const worker = yield* swarm.get(result.metadata.workerId)
        expect(worker?.spec.backend).toBe("worktree")
      }),
    {
      git: true,
      config: {
        agent: {
          "isolated-reviewer": {
            mode: "subagent",
            isolation: "worktree",
          },
        },
      },
    },
  )

  it.instance(
    "prepends configured initial prompt to the first subagent turn",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()
        let seenPrompt = ""
        const promptOps: TaskPromptOps = {
          cancel: () => Effect.void,
          resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
          prompt: (input) =>
            Effect.sync(() => {
              seenPrompt = input.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
              return reply(input, "done")
            }),
        }

        yield* def.execute(
          {
            description: "initial prompt",
            prompt: "Inspect the cache layer.",
            subagent_type: "prefaced-reviewer",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(seenPrompt).toContain("Always produce a risk-ranked review first.")
        expect(seenPrompt.indexOf("Always produce")).toBeLessThan(seenPrompt.indexOf("Inspect the cache layer."))
      }),
    {
      config: {
        agent: {
          "prefaced-reviewer": {
            mode: "subagent",
            initial_prompt: "Always produce a risk-ranked review first.",
          },
        },
      },
    },
  )

  it.instance("honors explicit model overrides and team metadata", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const tool = yield* TaskTool
      const def = yield* tool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const finish = defer<void>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(async () => {
            ready.resolve(input)
            await finish.promise
            return reply(input, "modeled")
          }),
      }

      const result = yield* def.execute(
        {
          description: "modeled worker",
          prompt: "use the requested model",
          subagent_type: "general",
          team: "analysis",
          model: "test/override-model",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const seen = yield* Effect.promise(() => ready.promise)
      expect(String(seen?.model?.providerID)).toBe("test")
      expect(String(seen?.model?.modelID)).toBe("override-model")
      expect(result.metadata.team).toBe("analysis")
      expect(result.metadata.status).toBe("running")
      expect(result.output).toContain("Background subagent launched.")

      const worker = yield* (yield* SwarmRuntime.Service).get(result.metadata.workerId)
      expect(worker?.spec.team).toBe("analysis")
      expect(worker?.spec.executionStrategy).toBe("persistent")
      expect(String(worker?.spec.model?.providerID)).toBe("test")
      expect(String(worker?.spec.model?.modelID)).toBe("override-model")
      finish.resolve()
      yield* (yield* SwarmRuntime.Service).cancel(result.metadata.workerId)
    }),
  )

  it.instance("deduplicates background worker names within a parent session", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const promptOps = stubOps()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps, allowForegroundTask: true },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const first = yield* task.execute(
        {
          description: "first worker",
          prompt: "first",
          subagent_type: "general",
          run_in_background: true,
          name: "tester",
          team: "red",
        },
        ctx,
      )
      const second = yield* task.execute(
        {
          description: "second worker",
          prompt: "second",
          subagent_type: "general",
          run_in_background: true,
          name: "tester",
          team: "red",
        },
        ctx,
      )

      expect(first.metadata.name).toBe("tester")
      expect(second.metadata.name).toBe("tester-2")
      expect(second.output).toContain("name: tester-2")

      const swarm = yield* SwarmRuntime.Service
      const workers = yield* swarm.list(chat.id)
      expect(workers.map((worker) => worker.spec.name).sort()).toEqual(["tester", "tester-2"])

      yield* swarm.cancel(first.metadata.workerId)
      yield* swarm.cancel(second.metadata.workerId)
    }),
  )

  it.instance("launches a background worker and resumes it via send_message", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const firstPrompt = defer<SessionPrompt.PromptInput>()
      const secondPrompt = defer<SessionPrompt.PromptInput>()
      const thirdPrompt = defer<SessionPrompt.PromptInput>()
      const fourthPrompt = defer<SessionPrompt.PromptInput>()
      const finishThirdPrompt = defer<void>()
      const seen: SessionPrompt.PromptInput[] = []
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(async () => {
            const count = seen.length + 1
            seen.push(input)
            if (count === 1) firstPrompt.resolve(input)
            if (count === 2) secondPrompt.resolve(input)
            if (count === 3) thirdPrompt.resolve(input)
            if (count === 4) fourthPrompt.resolve(input)
            if (count === 3) await finishThirdPrompt.promise
            return reply(input, `result-${count}`)
          }),
      }

      const result = yield* task.execute(
        {
          description: "background worker",
          prompt: "initial work",
          subagent_type: "general",
          run_in_background: true,
          name: "worker-a",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(result.metadata.status).toBe("running")
      expect(result.metadata.name).toBe("worker-a")
      expect(result.metadata.outputPath).toMatch(/subagent-transcripts\/swa_.*\.jsonl$/)
      expect(result.output).toContain("running_in_background")
      expect(result.output).toContain("worker_id:")
      expect(result.output).toContain(`output_file: ${result.metadata.outputPath}`)
      expect(result.output).toContain("prefer read_task_output")
      const initialTranscript = yield* Effect.promise(() => fs.stat(result.metadata.outputPath))
      expect(initialTranscript.isFile()).toBe(true)

      const first = yield* Effect.promise(() => firstPrompt.promise)
      expect(first.sessionID).toBe(result.metadata.sessionId)
      expect(first.parts).toHaveLength(1)
      expect(first.parts[0]).toMatchObject({ type: "text" })
      expect(first.parts[0]?.type === "text" ? first.parts[0].text : "").toContain(
        "You are a long-lived background subagent",
      )
      expect(first.parts[0]?.type === "text" ? first.parts[0].text : "").toContain("initial work")

      const swarm = yield* SwarmRuntime.Service
      let worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.status !== "idle"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      expect(worker?.result?.text).toBe("result-1")
      expect(worker?.spec.outputPath).toBe(result.metadata.outputPath)

      const transcript = yield* Effect.promise(() => fs.readFile(result.metadata.outputPath, "utf8"))
      expect(transcript).toContain("result-1")

      let notification: MessageV2.TextPart | undefined
      for (let i = 0; i < 50 && !notification; i++) {
        const messages = yield* MessageV2.filterCompactedEffect(chat.id)
        notification = messages
          .flatMap((message) => message.parts)
          .find(
            (part): part is MessageV2.TextPart =>
              part.type === "text" &&
              part.synthetic === true &&
              part.metadata?.kind === "task-notification" &&
              part.text.includes("<task-notification>"),
          )
        if (!notification) yield* Effect.sleep("10 millis")
      }
      expect(notification?.text).toContain(`<task-id>${result.metadata.sessionId}</task-id>`)
      expect(notification?.text).toContain(`<worker-id>${result.metadata.workerId}</worker-id>`)
      expect(notification?.text).toContain("<name>worker-a</name>")
      expect(notification?.text).toContain("<status>idle</status>")
      expect(notification?.text).not.toContain("<result>result-1</result>")

      const leadMessage = yield* sendMessage.execute(
        {
          to: "team-lead",
          message: "progress update for the lead",
          summary: "progress update",
        },
        {
          sessionID: result.metadata.sessionId,
          messageID: assistant.id,
          agent: "general",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )
      expect(leadMessage.title).toBe("Message sent to team lead")
      const parentMessages = yield* MessageV2.filterCompactedEffect(chat.id)
      const delivered = parentMessages
        .flatMap((message) => message.parts)
        .find(
          (part): part is MessageV2.TextPart =>
            part.type === "text" && part.synthetic === true && part.metadata?.kind === "swarm-message",
        )
      expect(delivered?.text).toContain("<from>worker-a</from>")
      expect(delivered?.text).toContain("<message>progress update for the lead</message>")

      const sent = yield* sendMessage.execute(
        {
          to: "worker-a",
          message: "second work",
          summary: "second step",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )
      expect(sent.metadata.to).toBe("worker-a")
      expect(sent.output).toContain("input_id:")

      const second = yield* Effect.promise(() => secondPrompt.promise)
      expect(second.sessionID).toBe(result.metadata.sessionId)
      expect(second.parts).toEqual([{ type: "text", text: "second work" }])
      expect(second.messageID).not.toBe(first.messageID)

      worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.result?.text !== "result-2"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      expect(worker?.result?.text).toBe("result-2")

      const shutdown = yield* sendMessage.execute(
        {
          to: "worker-a",
          message: {
            type: "shutdown_request",
            reason: "task complete",
          },
          summary: "shutdown request",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )
      expect(shutdown.metadata.requestId).toMatch(/^shr_/)
      const shutdownRequestID = shutdown.metadata.requestId!
      worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.pendingShutdownID).toBe(shutdownRequestID)
      const third = yield* Effect.promise(() => thirdPrompt.promise)
      expect(third.parts).toEqual([
        {
          type: "text",
          text: [
            "<structured-message>",
            "<type>shutdown_request</type>",
            `<request-id>${shutdownRequestID}</request-id>`,
            "<reason>task complete</reason>",
            "</structured-message>",
          ].join("\n"),
        },
      ])

      const approved = yield* sendMessage.execute(
        {
          to: "team-lead",
          message: {
            type: "shutdown_response",
            request_id: shutdownRequestID,
            approve: true,
          },
        },
        {
          sessionID: result.metadata.sessionId,
          messageID: assistant.id,
          agent: "general",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )
      expect(approved.metadata.approved).toBe(true)
      finishThirdPrompt.resolve()

      for (let i = 0; i < 50 && worker?.status !== "cancelled"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("cancelled")
      expect(worker?.result?.text).toContain("shutdown approved")

      const resumed = yield* sendMessage.execute(
        {
          to: "worker-a",
          message: "resume after shutdown",
          summary: "resume worker",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )
      expect(resumed.title).toBe("Subagent resumed")
      expect(resumed.metadata.resumed).toBe(true)
      expect(resumed.metadata.previousStatus).toBe("cancelled")
      expect(resumed.metadata.sessionId).toBe(result.metadata.sessionId)
      expect(resumed.metadata.workerId).not.toBe(result.metadata.workerId)

      const fourth = yield* Effect.promise(() => fourthPrompt.promise)
      expect(fourth.sessionID).toBe(result.metadata.sessionId)
      expect(fourth.parts[0]?.type === "text" ? fourth.parts[0].text : "").toContain("resume after shutdown")
      expect(fourth.parts[0]?.type === "text" ? fourth.parts[0].text : "").toContain(
        "You are a long-lived background subagent",
      )

      const resumedWorkerID = WorkerID.ascending(resumed.metadata.workerId!)
      let resumedWorker = yield* swarm.get(resumedWorkerID)
      for (let i = 0; i < 50 && resumedWorker?.result?.text !== "result-4"; i++) {
        yield* Effect.sleep("10 millis")
        resumedWorker = yield* swarm.get(resumedWorkerID)
      }
      expect(resumedWorker?.status).toBe("idle")
      expect(resumedWorker?.spec.sessionID).toBe(result.metadata.sessionId)
      expect(resumedWorker?.spec.name).toBe("worker-a")
      expect(resumedWorker?.result?.text).toBe("result-4")
    }),
  )

  it.instance("launches and polls a remote background worker through the remote subagent backend", () =>
    Effect.gen(function* () {
      const requests: string[] = []
      let launchBody: any
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()

      yield* withRemoteServer(
        async (request) => {
          const url = new URL(request.url)
          requests.push(`${request.method} ${url.pathname}`)
          if (request.method === "POST" && url.pathname === "/workers") {
            launchBody = await request.json()
            return Response.json({
              remote_id: "remote-task-1",
              session_url: "https://remote.example/session/remote-task-1",
              cursor: "0",
            })
          }
          if (request.method === "GET" && url.pathname === "/workers/remote-task-1/events") {
            return Response.json({
              cursor: "1",
              events: [
                { type: "progress", message: "remote started" },
                { type: "output", text: "remote chunk" },
                { type: "completed", text: "remote-result" },
              ],
            })
          }
          return new Response("not found", { status: 404 })
        },
        (endpoint) =>
          Effect.gen(function* () {
              const result = yield* task.execute(
                {
                  description: "remote worker",
                  prompt: "do remote work",
                  subagent_type: "general",
                  run_in_background: true,
                  isolation: "remote",
                  name: "remote-a",
                },
                {
                  sessionID: chat.id,
                  messageID: assistant.id,
                  agent: "build",
                  abort: new AbortController().signal,
                  extra: { promptOps: stubOps(), allowForegroundTask: true, swarmRemoteEndpoint: endpoint },
                  messages: [],
                  metadata: () => Effect.void,
                  ask: () => Effect.void,
                },
              )

              expect(result.metadata.status).toBe("running")
              expect(result.metadata.backend).toBe("remote")
              expect(result.metadata.remoteId).toBe("remote-task-1")
              expect(result.output).toContain("backend: remote remote_id: remote-task-1")
              expect(result.output).toContain("remote_session_url: https://remote.example/session/remote-task-1")

              const swarm = yield* SwarmRuntime.Service
              let worker = yield* swarm.get(result.metadata.workerId)
              for (let i = 0; i < 50 && worker?.status !== "completed"; i++) {
                yield* Effect.sleep("10 millis")
                worker = yield* swarm.get(result.metadata.workerId)
              }
              expect(worker?.status).toBe("completed")
              expect(worker?.spec.backend).toBe("remote")
              expect(worker?.spec.remoteEndpoint).toBe(endpoint)
              expect(worker?.spec.remoteID).toBe("remote-task-1")
              expect(worker?.spec.remoteSessionURL).toBe("https://remote.example/session/remote-task-1")
              expect(worker?.remoteCursor).toBe("1")
              expect(worker?.result?.text).toBe("remote-result")

              const transcript = yield* Effect.promise(() => fs.readFile(result.metadata.outputPath, "utf8"))
              expect(transcript).toContain("remote started")
              expect(transcript).toContain("remote chunk")
              expect(transcript).toContain("remote-result")

              let notification: MessageV2.TextPart | undefined
              for (let i = 0; i < 50 && !notification; i++) {
                const messages = yield* MessageV2.filterCompactedEffect(chat.id)
                notification = messages
                  .flatMap((message) => message.parts)
                  .find(
                    (part): part is MessageV2.TextPart =>
                      part.type === "text" &&
                      part.synthetic === true &&
                      part.metadata?.kind === "task-notification" &&
                      part.text.includes("<remote-id>remote-task-1</remote-id>"),
                  )
                if (!notification) yield* Effect.sleep("10 millis")
              }
              expect(notification?.text).toContain("<status>completed</status>")
              expect(notification?.text).toContain(`<output-file>${result.metadata.outputPath}</output-file>`)
              expect(notification?.text).toContain(
                "<remote-session-url>https://remote.example/session/remote-task-1</remote-session-url>",
              )
            }),
      )

      expect(requests).toContain("POST /workers")
      expect(requests).toContain("GET /workers/remote-task-1/events")
      expect(launchBody.worker.backend).toBe("remote")
      expect(launchBody.worker.name).toBe("remote-a")
      expect(launchBody.prompt).toContain("do remote work")
    }),
  )

  it.instance("writes parent task notifications for recovered remote workers", () =>
    Effect.gen(function* () {
      const { chat } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const outputPath = path.join(os.tmpdir(), `opencode-remote-recovered-${Date.now()}.jsonl`)

      try {
        yield* withRemoteServer(
          async (request) => {
            const url = new URL(request.url)
            if (request.method === "GET" && url.pathname === "/workers/remote-recovered/events") {
              return Response.json({
                cursor: "done",
                events: [{ type: "completed", text: "remote recovered result" }],
              })
            }
            return new Response("not found", { status: 404 })
          },
          (endpoint) =>
            Effect.gen(function* () {
              const worker = yield* swarm.spawn({
                parentSessionID: chat.id,
                sessionID: SessionID.descending(),
                agent: "general",
                name: "remote-recovered",
                prompt: "recover remote worker",
                description: "remote recovered",
                outputPath,
                model: ref,
                wait: false,
                executionStrategy: "persistent",
                backend: "remote",
                remoteEndpoint: endpoint,
                remoteID: "remote-recovered",
                remoteSessionURL: "https://remote.example/session/remote-recovered",
                launch: Effect.void,
              })

              yield* swarm.reload()

              let latest = yield* swarm.get(worker.workerID)
              for (let i = 0; i < 100 && latest?.status !== "completed"; i++) {
                yield* Effect.sleep("10 millis")
                latest = yield* swarm.get(worker.workerID)
              }
              expect(latest?.status).toBe("completed")

              let notification: MessageV2.TextPart | undefined
              for (let i = 0; i < 50 && !notification; i++) {
                const messages = yield* MessageV2.filterCompactedEffect(chat.id)
                notification = messages
                  .flatMap((message) => message.parts)
                  .find(
                    (part): part is MessageV2.TextPart =>
                      part.type === "text" &&
                      part.synthetic === true &&
                      part.metadata?.kind === "task-notification" &&
                      part.text.includes("<remote-id>remote-recovered</remote-id>"),
                  )
                if (!notification) yield* Effect.sleep("10 millis")
              }

              expect(notification?.text).toContain("<status>completed</status>")
              expect(notification?.text).toContain("<result>remote recovered result</result>")
              expect(notification?.text).toContain(
                "<remote-session-url>https://remote.example/session/remote-recovered</remote-session-url>",
              )
              expect(notification?.metadata?.remoteID).toBe("remote-recovered")
            }),
        )
      } finally {
        yield* Effect.promise(() => fs.rm(outputPath, { force: true }))
      }
    }),
  )

  it.instance("launches background workers through the tmux pane backend when configured", () =>
    Effect.gen(function* () {
      const oldBackend = process.env.OPENCODE_SWARM_BACKEND
      const oldForce = process.env.OPENCODE_SWARM_FORCE_PANE
      const oldWorkerCommand = process.env.OPENCODE_SWARM_WORKER_COMMAND
      const oldPath = process.env.PATH
      const tmp = yield* Effect.promise(() => fs.mkdtemp(path.join(os.tmpdir(), "opencode-fake-tmux-")))
      const logPath = path.join(tmp, "tmux.log")
      const tmux = path.join(tmp, "tmux")
      yield* Effect.promise(async () => {
        await fs.writeFile(
          tmux,
          [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "printf '%s\\n' \"$*\" >> \"$TMUX_LOG\"",
            "case \"$*\" in",
            "  '-V') echo 'tmux 3.5'; exit 0 ;;",
            "  *' has-session '*) exit 1 ;;",
            "  *' new-session '*) echo '%42'; exit 0 ;;",
            "  *' list-panes '*) echo '%42'; exit 0 ;;",
            "esac",
            "exit 0",
            "",
          ].join("\n"),
          "utf8",
        )
        await fs.chmod(tmux, 0o755)
      })

      try {
        process.env.OPENCODE_SWARM_BACKEND = "tmux"
        process.env.OPENCODE_SWARM_FORCE_PANE = "1"
        process.env.OPENCODE_SWARM_WORKER_COMMAND = "opencode-test"
        process.env.TMUX_LOG = logPath
        process.env.PATH = `${tmp}:${oldPath ?? ""}`

        const { chat, assistant } = yield* seed()
        const taskTool = yield* TaskTool
        const task = yield* taskTool.init()
        const result = yield* task.execute(
          {
            description: "pane worker",
            prompt: "use a pane backend",
            subagent_type: "general",
            run_in_background: true,
            name: "pane-worker",
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps: stubOps() },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        expect(result.metadata.status).toBe("running")
        expect(result.metadata.backend).toBe("tmux")
        expect(result.metadata.paneId).toBe("%42")
        expect(result.output).toContain("backend: tmux pane_id: %42")

        const swarm = yield* SwarmRuntime.Service
        const worker = yield* swarm.get(result.metadata.workerId)
        expect(worker?.spec.backend).toBe("tmux")
        expect(worker?.spec.paneID).toBe("%42")
        expect(worker?.spec.paneExternalSession).toBe(true)
        expect(worker?.spec.paneWindowTarget).toBe("opencode-swarm:agents")

        const log = yield* Effect.promise(() => fs.readFile(logPath, "utf8"))
        expect(log).toContain("-L opencode-swarm new-session -d -s opencode-swarm -n agents")
        expect(log).toContain("-L opencode-swarm send-keys -t %42")
        expect(log).toContain("opencode-test 'swarm' 'worker'")

        yield* swarm.cancel(result.metadata.workerId)
        const afterCancel = yield* Effect.promise(() => fs.readFile(logPath, "utf8"))
        expect(afterCancel).toContain("-L opencode-swarm kill-pane -t %42")
      } finally {
        if (oldBackend === undefined) delete process.env.OPENCODE_SWARM_BACKEND
        else process.env.OPENCODE_SWARM_BACKEND = oldBackend
        if (oldForce === undefined) delete process.env.OPENCODE_SWARM_FORCE_PANE
        else process.env.OPENCODE_SWARM_FORCE_PANE = oldForce
        if (oldWorkerCommand === undefined) delete process.env.OPENCODE_SWARM_WORKER_COMMAND
        else process.env.OPENCODE_SWARM_WORKER_COMMAND = oldWorkerCommand
        if (oldPath === undefined) delete process.env.PATH
        else process.env.PATH = oldPath
        delete process.env.TMUX_LOG
        yield* Effect.promise(() => fs.rm(tmp, { recursive: true, force: true }))
      }
    }),
  )

  it.instance("does not plan-gate read-only explore workers when mode=plan is accidental", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const firstPrompt = defer<SessionPrompt.PromptInput>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            firstPrompt.resolve(input)
            return reply(input, "read-only done")
          }),
      }

      const result = yield* task.execute(
        {
          description: "read only research",
          prompt: "Research the codebase. Do not modify code.",
          subagent_type: "explore",
          run_in_background: true,
          mode: "plan",
          name: "readonly-explorer",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const first = yield* Effect.promise(() => firstPrompt.promise)
      expect(first.tools?.bash).not.toBe(false)
      expect(first.tools?.edit).not.toBe(false)
      expect(first.tools?.write).not.toBe(false)
      expect(first.tools?.apply_patch).not.toBe(false)

      const swarm = yield* SwarmRuntime.Service
      let worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.status !== "idle"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      expect(worker?.spec.planModeRequired).not.toBe(true)
      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance("respects explicit plan_mode_required=false even when mode=plan is supplied", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const firstPrompt = defer<SessionPrompt.PromptInput>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            firstPrompt.resolve(input)
            return reply(input, "not gated")
          }),
      }

      const result = yield* task.execute(
        {
          description: "explicit read only",
          prompt: "Research only. Do not modify code.",
          subagent_type: "general",
          run_in_background: true,
          mode: "plan",
          plan_mode_required: false,
          name: "explicit-readonly",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const first = yield* Effect.promise(() => firstPrompt.promise)
      expect(first.tools?.bash).not.toBe(false)
      expect(first.tools?.edit).not.toBe(false)
      expect(first.tools?.write).not.toBe(false)
      expect(first.tools?.apply_patch).not.toBe(false)

      const swarm = yield* SwarmRuntime.Service
      let worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.status !== "idle"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      expect(worker?.spec.planModeRequired).not.toBe(true)
      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance("disables lead-only orchestration tools inside spawned workers", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const firstPrompt = defer<SessionPrompt.PromptInput>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            firstPrompt.resolve(input)
            return reply(input, "generic work done")
          }),
      }

      const result = yield* task.execute(
        {
          description: "generic worker",
          prompt: "Implement the assigned change directly, run relevant tests, and report back.",
          subagent_type: "general",
          run_in_background: true,
          name: "generic-worker",
          context: "fork",
        },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      const first = yield* Effect.promise(() => firstPrompt.promise)
      expect(first.tools?.task).toBe(false)
      expect(first.tools?.goal_create).toBe(false)
      expect(first.tools?.goal_update).toBe(false)
      expect(first.tools?.create_team).toBe(false)
      expect(first.tools?.delete_team).toBe(false)
      expect(first.tools?.wait_task).toBe(false)
      expect(first.tools?.read_task_output).toBe(false)
      expect(first.tools?.cancel_task).toBe(false)
      expect(first.tools?.remote_trigger).toBe(false)
      expect(first.tools?.bash).not.toBe(false)
      expect(first.tools?.edit).not.toBe(false)
      expect(first.tools?.write).not.toBe(false)
      expect(first.tools?.apply_patch).not.toBe(false)

      const text = first.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
      expect(text).toContain("Do the assigned generic work directly")
      expect(text).toContain("Do not create or update session goals")
      expect(text).toContain("Do not re-run it")

      const swarm = yield* SwarmRuntime.Service
      let worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.status !== "idle"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance("requires plan approval before a background worker can use mutating tools", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const firstPrompt = defer<SessionPrompt.PromptInput>()
      const secondPrompt = defer<SessionPrompt.PromptInput>()
      const seen: SessionPrompt.PromptInput[] = []
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            seen.push(input)
            if (seen.length === 1) firstPrompt.resolve(input)
            if (seen.length === 2) secondPrompt.resolve(input)
            return reply(input, `plan-turn-${seen.length}`)
          }),
      }
      const parentCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps, allowForegroundTask: true },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const result = yield* task.execute(
        {
          description: "planned worker",
          prompt: "inspect first, then implement",
          subagent_type: "general",
          run_in_background: true,
          mode: "plan",
          name: "planner",
          team_name: "red",
        },
        parentCtx,
      )

      const first = yield* Effect.promise(() => firstPrompt.promise)
      expect(first.tools?.bash).toBe(false)
      expect(first.tools?.edit).toBe(false)
      expect(first.tools?.write).toBe(false)
      expect(first.tools?.apply_patch).toBe(false)
      expect(first.persistToolPermissions).toBe(false)
      const firstText = first.parts[0]?.type === "text" ? first.parts[0].text : ""
      expect(firstText).toContain("Plan approval gates mutating work only")
      expect(firstText).toContain("plan_approval_request")

      const swarm = yield* SwarmRuntime.Service
      let worker = yield* swarm.get(result.metadata.workerId)
      for (let i = 0; i < 50 && worker?.status !== "idle"; i++) {
        yield* Effect.sleep("10 millis")
        worker = yield* swarm.get(result.metadata.workerId)
      }
      expect(worker?.status).toBe("idle")
      expect(worker?.spec.planModeRequired).toBe(true)

      const workerCtx = {
        ...parentCtx,
        sessionID: result.metadata.sessionId,
        agent: "general",
        extra: {},
      }
      const requested = yield* sendMessage.execute(
        {
          to: "team-lead",
          message: {
            type: "plan_approval_request",
            plan: "1. Inspect affected files\n2. Apply the implementation patch\n3. Run focused tests",
            plan_file_path: "/tmp/opencode-plan.md",
          },
          summary: "approval plan",
        },
        workerCtx,
      )
      expect(requested.title).toBe("Plan approval requested")
      expect(requested.metadata.requestId).toMatch(/^par_/)
      const requestID = requested.metadata.requestId!
      worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.pendingPlanApprovalID).toBe(requestID)

      const parentMessages = yield* MessageV2.filterCompactedEffect(chat.id)
      const delivered = parentMessages
        .flatMap((message) => message.parts)
        .find(
          (part): part is MessageV2.TextPart =>
            part.type === "text" &&
            part.synthetic === true &&
            part.metadata?.kind === "swarm-message" &&
            part.text.includes(requestID),
        )
      expect(delivered?.text).toContain("plan_approval_request")
      expect(delivered?.text).toContain("Inspect affected files")

      const approved = yield* sendMessage.execute(
        {
          to: "planner",
          message: {
            type: "plan_approval_response",
            request_id: requestID,
            approve: true,
          },
        },
        parentCtx,
      )
      expect(approved.title).toBe("Plan approved")
      expect(approved.metadata.approved).toBe(true)
      worker = yield* swarm.get(result.metadata.workerId)
      expect(worker?.pendingPlanApprovalID).toBeUndefined()

      const second = yield* Effect.promise(() => secondPrompt.promise)
      expect(second.parts[0]).toMatchObject({ type: "text" })
      expect(second.parts[0]?.type === "text" ? second.parts[0].text : "").toContain(
        "<type>plan_approval_response</type>",
      )
      expect(second.tools?.bash).not.toBe(false)
      expect(second.tools?.edit).not.toBe(false)
      expect(second.tools?.write).not.toBe(false)
      expect(second.tools?.apply_patch).not.toBe(false)
      expect(second.persistToolPermissions).toBe(false)

      yield* swarm.cancel(result.metadata.workerId)
    }),
  )

  it.instance(
    "accepts plan approvals requested from worktree-isolated background workers",
    () =>
      Effect.gen(function* () {
        const { chat, assistant } = yield* seed()
        const taskTool = yield* TaskTool
        const task = yield* taskTool.init()
        const sendMessageTool = yield* SendMessageTool
        const sendMessage = yield* sendMessageTool.init()
        const firstPrompt = defer<SessionPrompt.PromptInput>()
        const secondPrompt = defer<SessionPrompt.PromptInput>()
        const requestID = "worktree-plan-1"
        const seen: SessionPrompt.PromptInput[] = []
        const promptOps: TaskPromptOps = {
          cancel: () => Effect.void,
          resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
          prompt: (input) =>
            Effect.gen(function* () {
              seen.push(input)
              if (seen.length === 1) {
                const childSendMessageTool = yield* SendMessageTool
                const childSendMessage = yield* childSendMessageTool.init()
                yield* childSendMessage.execute(
                  {
                    to: "team-lead",
                    message: {
                      type: "plan_approval_request",
                      request_id: requestID,
                      plan: "1. Inspect in the worktree\n2. Apply the patch after approval",
                    },
                    summary: "approval plan",
                  },
                  {
                    sessionID: input.sessionID,
                    messageID: input.messageID ?? MessageID.ascending(),
                    agent: input.agent ?? "general",
                    abort: new AbortController().signal,
                    extra: {},
                    messages: [],
                    metadata: () => Effect.void,
                    ask: () => Effect.void,
                  },
                )
                firstPrompt.resolve(input)
                return reply(input, "approval requested")
              }
              secondPrompt.resolve(input)
              return reply(input, "approval accepted")
            }) as Effect.Effect<MessageV2.WithParts>,
        }
        const parentCtx = {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: { promptOps, allowForegroundTask: true },
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        }

        const result = yield* task.execute(
          {
            description: "planned worktree worker",
            prompt: "inspect in isolation, then request approval before changing files",
            subagent_type: "general",
            isolation: "worktree",
            run_in_background: true,
            mode: "plan",
            name: "worktree-planner",
            team_name: "red",
          },
          parentCtx,
        )

        const first = yield* Effect.promise(() => firstPrompt.promise)
        expect(first.tools?.bash).toBe(false)
        expect(first.tools?.edit).toBe(false)
        expect(first.persistToolPermissions).toBe(false)

        const swarm = yield* SwarmRuntime.Service
        let worker = yield* swarm.get(result.metadata.workerId)
        for (let i = 0; i < 50 && worker?.pendingPlanApprovalID !== requestID; i++) {
          yield* Effect.sleep("10 millis")
          worker = yield* swarm.get(result.metadata.workerId)
        }
        expect(worker?.pendingPlanApprovalID).toBe(requestID)
        expect(worker?.spec.backend).toBe("worktree")

        const approved = yield* sendMessage.execute(
          {
            to: "worktree-planner",
            message: {
              type: "plan_approval_response",
              request_id: requestID,
              approve: true,
            },
          },
          parentCtx,
        )
        expect(approved.title).toBe("Plan approved")
        expect(approved.metadata.approved).toBe(true)

        const second = yield* Effect.promise(() => secondPrompt.promise)
        expect(second.parts[0]?.type === "text" ? second.parts[0].text : "").toContain(
          "<type>plan_approval_response</type>",
        )
        expect(second.tools?.bash).not.toBe(false)
        expect(second.tools?.edit).not.toBe(false)
        expect(second.persistToolPermissions).toBe(false)

        yield* swarm.cancel(result.metadata.workerId)
      }),
    { git: true },
  )

  it.instance("lets the team lead answer a worker permission request via send_message", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const permissions = yield* Permission.Service
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const requestID = PermissionID.ascending()
      const worker = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "needs-permission",
        team: "red",
        prompt: "run a command",
        description: "permission worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const parentCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* swarm.markPermissionPending(worker.workerID, requestID as unknown as string)
      const waiter = yield* permissions
        .ask({
          id: requestID,
          sessionID: chat.id,
          permission: "bash",
          patterns: ["git status"],
          always: ["git status"],
          metadata: { command: "git status" },
          ruleset: [],
        })
        .pipe(
          Effect.ensuring(swarm.clearPermissionPending(worker.workerID, requestID as unknown as string)),
          Effect.exit,
          Effect.forkScoped,
        )

      for (let i = 0; i < 50; i++) {
        if ((yield* permissions.list()).some((request) => request.id === requestID)) break
        yield* Effect.sleep("10 millis")
      }

      const approved = yield* sendMessage.execute(
        {
          to: "needs-permission",
          message: {
            type: "permission_response",
            request_id: requestID as unknown as string,
            approve: true,
          },
        },
        parentCtx,
      )

      expect(approved.title).toBe("Permission approved")
      expect(approved.metadata.approved).toBe(true)
      const outcome = yield* Fiber.join(waiter)
      expect(Exit.isSuccess(outcome)).toBe(true)

      let snapshot = yield* swarm.get(worker.workerID)
      for (let i = 0; i < 50 && snapshot?.pendingPermissionID; i++) {
        yield* Effect.sleep("10 millis")
        snapshot = yield* swarm.get(worker.workerID)
      }
      expect(snapshot?.pendingPermissionID).toBeUndefined()

      yield* swarm.cancel(worker.workerID)
    }),
  )

  it.instance("applies team permission updates and mode set requests to worker sessions", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const sessions = yield* Session.Service
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const parentCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }
      const firstSession = yield* sessions.create({ parentID: chat.id, title: "perm one" })
      const secondSession = yield* sessions.create({ parentID: chat.id, title: "perm two" })
      const first = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: firstSession.id,
        agent: "general",
        name: "perm-one",
        team: "red",
        prompt: "wait",
        description: "perm one",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const second = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: secondSession.id,
        agent: "general",
        name: "perm-two",
        team: "red",
        prompt: "wait",
        description: "perm two",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const direct = yield* sendMessage.execute(
        {
          to: "perm-one",
          message: {
            type: "team_permission_update",
            rules: [{ permission: "bash", pattern: "git status", action: "allow" }],
            tool_name: "bash",
          },
        },
        parentCtx,
      )
      expect(direct.title).toBe("Permission rules updated")
      expect((yield* sessions.get(first.sessionID)).permission).toContainEqual({
        permission: "bash",
        pattern: "git status",
        action: "allow",
      })

      const mode = yield* sendMessage.execute(
        {
          to: "perm-one",
          message: {
            type: "mode_set_request",
            mode: "accept_edits",
          },
        },
        parentCtx,
      )
      expect(mode.title).toBe("Mode updated")
      expect((yield* sessions.get(first.sessionID)).permission).toEqual([
        { permission: "edit", pattern: "*", action: "allow" },
      ])

      const broadcast = yield* sendMessage.execute(
        {
          to: "*",
          team: "red",
          message: {
            type: "mode_set_request",
            mode: "readonly",
          },
        },
        parentCtx,
      )
      expect(broadcast.title).toBe("Team mode updated")
      expect(broadcast.metadata.count).toBe(2)
      for (const worker of [first, second]) {
        expect((yield* sessions.get(worker.sessionID)).permission).toEqual([
          { permission: "edit", pattern: "*", action: "deny" },
          { permission: "bash", pattern: "*", action: "deny" },
        ])
      }

      yield* swarm.cancel(first.workerID)
      yield* swarm.cancel(second.workerID)
    }),
  )

  it.instance("routes peer-to-peer messages between background workers", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const sessions = yield* Session.Service
      const listPeersTool = yield* ListPeersTool
      const listPeers = yield* listPeersTool.init()
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const firstSession = yield* sessions.create({ parentID: chat.id, title: "peer one" })
      const secondSession = yield* sessions.create({ parentID: chat.id, title: "peer two" })
      const first = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: firstSession.id,
        agent: "general",
        name: "peer-one",
        team: "red",
        prompt: "wait",
        description: "peer one",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const second = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: secondSession.id,
        agent: "general",
        name: "peer-two",
        team: "red",
        prompt: "wait",
        description: "peer two",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const waiter = yield* swarm.awaitInput(second.workerID).pipe(Effect.forkScoped)
      const workerCtx = {
        sessionID: first.sessionID,
        messageID: assistant.id,
        agent: "general",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const peers = yield* listPeers.execute({ scope: "current" }, workerCtx)
      expect(peers.output).toContain("<name>peer-one</name>")
      expect(peers.output).toContain("<name>peer-two</name>")

      const sent = yield* sendMessage.execute(
        {
          to: "peer-two",
          message: "please review task #2",
          summary: "peer review request",
        },
        workerCtx,
      )
      expect(sent.title).toBe("Message sent")
      const received = yield* Fiber.join(waiter)
      expect(received.from).toBe("peer-one")
      expect(received.message).toBe("please review task #2")
      expect(received.summary).toBe("peer review request")

      yield* swarm.cancel(first.workerID)
      yield* swarm.cancel(second.workerID)
    }),
  )

  it.instance("lists peers and delivers cross-session messages", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const sessions = yield* Session.Service
      const target = yield* sessions.create({ title: "Peer target" })
      yield* sessions.updateMessage({
        id: MessageID.ascending(),
        role: "user",
        sessionID: target.id,
        agent: "build",
        model: ref,
        time: { created: Date.now() },
      })
      const listPeersTool = yield* ListPeersTool
      const listPeers = yield* listPeersTool.init()
      const sendMessageTool = yield* SendMessageTool
      const sendMessage = yield* sendMessageTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const peers = yield* listPeers.execute({ scope: "all" }, ctx)
      expect(peers.output).toContain(`session:${chat.id}`)
      expect(peers.output).toContain(`session:${target.id}`)

      const sent = yield* sendMessage.execute(
        {
          to: `session:${target.id}`,
          message: "Please review the current branch.",
          summary: "branch review",
        },
        ctx,
      )
      expect(sent.title).toBe("Peer message sent")
      expect(sent.output).toContain(`Message sent to peer session: ${target.id}`)

      const targetMessages = yield* MessageV2.filterCompactedEffect(target.id)
      const peerMessage = targetMessages
        .flatMap((message) => message.parts)
        .find(
          (part): part is MessageV2.TextPart =>
            part.type === "text" &&
            part.synthetic === true &&
            part.metadata?.kind === "cross-session-message",
        )
      expect(peerMessage?.text).toContain(`<cross-session-message from="session:${chat.id}">`)
      expect(peerMessage?.text).toContain("<summary>branch review</summary>")
      expect(peerMessage?.text).toContain("<message>Please review the current branch.</message>")
    }),
  )

  it.instance("manages background workers through list, wait, read, and cancel tools", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const listTool = yield* ListTasksTool
      const listTasks = yield* listTool.init()
      const waitTool = yield* WaitTaskTool
      const waitTask = yield* waitTool.init()
      const readTool = yield* ReadTaskOutputTool
      const readTaskOutput = yield* readTool.init()
      const cancelTool = yield* CancelTaskTool
      const cancelTask = yield* cancelTool.init()
      const stopTool = yield* StopTaskTool
      const stopTask = yield* stopTool.init()
      const ready = defer<SessionPrompt.PromptInput>()
      const finish = defer<void>()
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.promise(async () => {
            ready.resolve(input)
            await finish.promise
            return reply(input, "managed-result")
          }),
      }
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps, allowForegroundTask: true },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const launched = yield* task.execute(
        {
          description: "managed worker",
          prompt: "do managed work",
          subagent_type: "general",
          run_in_background: true,
          name: "managed",
        },
        ctx,
      )

      yield* Effect.promise(() => ready.promise)
      const listedRunning = yield* listTasks.execute({ scope: "current" }, ctx)
      expect(listedRunning.metadata.count).toBe(1)
      expect(listedRunning.output).toContain("<name>managed</name>")
      expect(listedRunning.output).toContain("<status>running</status>")

      const timed = yield* waitTask.execute({ task_id: "managed", timeout_ms: 1 }, ctx)
      expect(timed.output).toContain("<status>running</status>")

      const blockingReadFiber = yield* readTaskOutput
        .execute({ task_id: "managed", block: true, timeout_ms: 1000 }, ctx)
        .pipe(Effect.forkScoped)
      finish.resolve()
      const blockingRead = yield* Fiber.join(blockingReadFiber)
      expect(blockingRead.output).toContain("<status>idle</status>")
      expect(blockingRead.output).toContain("<result>managed-result</result>")

      const waited = yield* waitTask.execute({ task_id: launched.metadata.workerId, timeout_ms: 1000 }, ctx)
      expect(waited.output).toContain("<status>idle</status>")
      expect(waited.output).toContain("<result>managed-result</result>")

      const read = yield* readTaskOutput.execute(
        {
          task_id: launched.metadata.sessionId,
          include_transcript: true,
        },
        ctx,
      )
      expect(read.output).toContain("<result>managed-result</result>")
      expect(read.output).toContain("<transcript")

      const cancelled = yield* cancelTask.execute({ task_id: "managed" }, ctx)
      expect(cancelled.output).toContain("<status>cancelled</status>")

      const stoppedAgain = yield* stopTask.execute({ task_id: launched.metadata.workerId }, ctx)
      expect(stoppedAgain.output).toContain("<status>cancelled</status>")
    }),
  )

  it.instance("stop_task refreshes the advertised transcript from persisted messages", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const session = yield* Session.Service
      const swarm = yield* SwarmRuntime.Service
      const ctx = yield* InstanceState.context
      const stopTool = yield* StopTaskTool
      const stopTask = yield* stopTool.init()
      const child = yield* session.create({ parentID: chat.id, title: "Partial transcript worker" })
      const childUser = yield* session.updateMessage({
        id: MessageID.ascending(),
        role: "user",
        sessionID: child.id,
        agent: "general",
        model: ref,
        time: { created: Date.now() },
      })
      const childAssistant: MessageV2.Assistant = {
        id: MessageID.ascending(),
        role: "assistant",
        parentID: childUser.id,
        sessionID: child.id,
        mode: "build",
        agent: "general",
        cost: 0,
        path: { cwd: ctx.directory, root: ctx.directory },
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        modelID: ref.modelID,
        providerID: ref.providerID,
        time: { created: Date.now() },
      }
      yield* session.updateMessage(childAssistant)
      yield* session.updatePart({
        id: PartID.ascending(),
        messageID: childAssistant.id,
        sessionID: child.id,
        type: "text",
        text: "partial durable transcript result",
      } satisfies MessageV2.TextPart)
      const outputPath = path.join(ctx.directory, "partial-worker-transcript.jsonl")
      yield* Effect.promise(() => fs.writeFile(outputPath, ""))
      const workerID = WorkerID.ascending()
      yield* swarm.spawn({
        workerID,
        parentSessionID: chat.id,
        sessionID: child.id,
        agent: "general",
        name: "partial-worker",
        prompt: "wait",
        description: "partial transcript worker",
        outputPath,
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const stopped = yield* stopTask.execute(
        { task_id: "partial-worker" },
        {
          sessionID: chat.id,
          messageID: assistant.id,
          agent: "build",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(stopped.output).toContain("<status>cancelled</status>")
      const transcript = yield* Effect.promise(() => fs.readFile(outputPath, "utf8"))
      expect(transcript).toContain("partial durable transcript result")
      expect(transcript.trim().length).toBeGreaterThan(0)
    }),
  )

  it.instance("lets a background worker stop itself with stop_task", () =>
    Effect.gen(function* () {
      const { chat } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const worker = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: SessionID.descending(),
        agent: "general",
        name: "self-stop",
        prompt: "stop when done",
        description: "self stopping worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const stopTool = yield* StopTaskTool
      const stopTask = yield* stopTool.init()

      const stopped = yield* stopTask.execute(
        { reason: "task complete" },
        {
          sessionID: worker.sessionID,
          messageID: MessageID.ascending(),
          agent: "general",
          abort: new AbortController().signal,
          extra: {},
          messages: [],
          metadata: () => Effect.void,
          ask: () => Effect.void,
        },
      )

      expect(stopped.metadata.self).toBe(true)
      expect(stopped.output).toContain("Subagent will stop after the current turn.")
      expect(stopped.output).toContain("<status>cancelled</status>")
      expect((yield* swarm.get(worker.workerID))?.result?.text).toBe("stopped: task complete")
    }),
  )

  it.instance("feeds TeammateIdle hook feedback back to a background worker", () => {
    let hookCount = 0
    return Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const waitTool = yield* WaitTaskTool
      const waitTask = yield* waitTool.init()
      const secondPrompt = defer<SessionPrompt.PromptInput>()
      let promptCount = 0
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            promptCount++
            if (promptCount === 2) secondPrompt.resolve(input)
            return reply(input, `turn-${promptCount}`)
          }),
      }
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps, allowForegroundTask: true },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const launched = yield* task.execute(
        {
          description: "idle hook worker",
          prompt: "finish and idle",
          subagent_type: "general",
          run_in_background: true,
          name: "idle-hook",
        },
        ctx,
      )

      const second = yield* Effect.promise(() => secondPrompt.promise)
      const secondText = second.parts.map((part) => (part.type === "text" ? part.text : "")).join("\n")
      expect(secondText).toContain("TeammateIdle hook feedback")
      expect(secondText).toContain("Need verification before idling")

      const waited = yield* waitTask.execute({ task_id: launched.metadata.workerId, timeout_ms: 1000 }, ctx)
      expect(waited.output).toContain("<status>idle</status>")
      expect(waited.output).toContain("<result>turn-2</result>")
      expect(hookCount).toBe(2)
    }).pipe(
      Effect.provide(
        pluginHook((name, _input, output) => {
          if (name !== "swarm.teammate.idle") return
          hookCount++
          if (hookCount !== 1) return
          ;(output as { continue: boolean; message?: string }).continue = false
          ;(output as { continue: boolean; message?: string }).message = "Need verification before idling"
        }),
      ),
    )
  })

  it.instance("creates, lists, and deletes subagent teams through tools", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const createTool = yield* CreateTeamTool
      const createTeam = yield* createTool.init()
      const listTool = yield* ListTeamsTool
      const listTeams = yield* listTool.init()
      const deleteTool = yield* DeleteTeamTool
      const deleteTeam = yield* deleteTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const created = yield* createTeam.execute(
        {
          team_name: "red",
          description: "parallel review",
          agent_type: "lead",
        },
        ctx,
      )
      expect(created.output).toContain("<name>red</name>")
      expect(created.output).toContain("<description>parallel review</description>")

      const listed = yield* listTeams.execute({}, ctx)
      expect(listed.metadata.count).toBe(1)
      expect(listed.output).toContain("<name>red</name>")

      const duplicate = yield* createTeam.execute({ team_name: "blue" }, ctx)
      expect(duplicate.title).toBe("Team not created")
      expect(duplicate.output).toContain('Already leading team "red"')
      expect((yield* listTeams.execute({}, ctx)).metadata.count).toBe(1)

      const deleted = yield* deleteTeam.execute({ team_name: "red" }, ctx)
      expect(deleted.output).toContain("<name>red</name>")

      const empty = yield* listTeams.execute({}, ctx)
      expect(empty.metadata.count).toBe(0)
      expect(empty.output).toBe("No subagent teams found.")
    }),
  )

  it.instance("refuses to delete teams with active workers unless force cleanup is explicit", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const swarm = yield* SwarmRuntime.Service
      const createTool = yield* CreateTeamTool
      const createTeam = yield* createTool.init()
      const deleteTool = yield* DeleteTeamTool
      const deleteTeam = yield* deleteTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* createTeam.execute({ team_name: "red" }, ctx)
      const worker = yield* swarm.spawn({
        parentSessionID: chat.id,
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

      const refused = yield* deleteTeam.execute({ team_name: "red" }, ctx)
      expect(refused.title).toBe("Team not deleted")
      expect(refused.output).toContain("Cannot delete team with 1 active member(s): reviewer")
      expect(refused.output).toContain("<active-workers count=\"1\">")
      expect((yield* swarm.listTeams(chat.id))).toHaveLength(1)
      expect((yield* swarm.get(worker.workerID))?.status).toBe("running")

      const deleted = yield* deleteTeam.execute({ team_name: "red", cancel_workers: true }, ctx)
      expect(deleted.title).toBe("Team deleted")
      expect((yield* swarm.listTeams(chat.id))).toEqual([])
      expect((yield* swarm.get(worker.workerID))?.status).toBe("cancelled")
    }),
  )

  it.instance("coordinates shared team tasks through task board tools", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const createTeamTool = yield* CreateTeamTool
      const createTeam = yield* createTeamTool.init()
      const createTaskTool = yield* CreateTaskTool
      const createTask = yield* createTaskTool.init()
      const updateTaskTool = yield* UpdateTaskTool
      const updateTask = yield* updateTaskTool.init()
      const getTaskTool = yield* GetTaskTool
      const getTask = yield* getTaskTool.init()
      const listTeamTasksTool = yield* ListTeamTasksTool
      const listTeamTasks = yield* listTeamTasksTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* createTeam.execute({ team_name: "red" }, ctx)
      const first = yield* createTask.execute(
        {
          subject: "Prepare fixture",
          description: "Create the test fixture",
        },
        ctx,
      )
      const second = yield* createTask.execute(
        {
          subject: "Run tests",
          description: "Run the test suite",
          owner: "tester",
        },
        ctx,
      )

      expect(first.output).toContain("Task #1 created successfully")
      expect(second.metadata.task.owner).toBe("tester")

      const updated = yield* updateTask.execute(
        {
          task_id: "2",
          status: "in_progress",
          add_blocked_by: ["1"],
        },
        ctx,
      )
      expect(updated.output).toContain("Updated task #2")
      expect(updated.metadata.result.updatedFields).toContain("blockedBy")

      const listed = yield* listTeamTasks.execute({}, ctx)
      expect(listed.metadata.count).toBe(2)
      expect(listed.output).toContain("<subject>Run tests</subject>")
      expect(listed.output).toContain("<blocked-by>#1</blocked-by>")

      const detail = yield* getTask.execute({ task_id: "2" }, ctx)
      expect(detail.output).toContain("<description>Run the test suite</description>")

      const completed = yield* updateTask.execute({ task_id: "1", status: "completed" }, ctx)
      expect(completed.output).toContain("Call list_team_tasks now")

      const deleted = yield* updateTask.execute({ task_id: "2", status: "deleted" }, ctx)
      expect(deleted.output).toBe("Deleted task #2")
      const missing = yield* getTask.execute({ task_id: "2" }, ctx)
      expect(missing.output).toBe("Task not found")
    }),
  )

  it.instance("lets TaskCreated hooks reject shared task creation", () => {
    let seenTaskSubject: string | undefined
    return Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const createTeamTool = yield* CreateTeamTool
      const createTeam = yield* createTeamTool.init()
      const createTaskTool = yield* CreateTaskTool
      const createTask = yield* createTaskTool.init()
      const listTeamTasksTool = yield* ListTeamTasksTool
      const listTeamTasks = yield* listTeamTasksTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* createTeam.execute({ team_name: "red" }, ctx)
      const blocked = yield* createTask.execute(
        {
          subject: "Vague task",
          description: "Do stuff",
        },
        ctx,
      )

      expect(blocked.title).toBe("Task creation blocked")
      expect(blocked.output).toBe("Task needs a concrete verification step")
      expect(blocked.metadata.result.success).toBe(false)
      expect(seenTaskSubject).toBe("Vague task")

      const listed = yield* listTeamTasks.execute({}, ctx)
      expect(listed.metadata.count).toBe(0)
      expect(listed.output).toBe("No team tasks found.")
    }).pipe(
      Effect.provide(
        pluginHook((name, input, output) => {
          if (name !== "swarm.task.created") return
          seenTaskSubject = (input as { taskSubject: string }).taskSubject
          ;(output as { allow: boolean; message?: string }).allow = false
          ;(output as { allow: boolean; message?: string }).message = "Task needs a concrete verification step"
        }),
      ),
    )
  })

  it.instance("lets TaskCompleted hooks block shared task completion", () => {
    let seenTaskSubject: string | undefined
    return Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const createTeamTool = yield* CreateTeamTool
      const createTeam = yield* createTeamTool.init()
      const createTaskTool = yield* CreateTaskTool
      const createTask = yield* createTaskTool.init()
      const updateTaskTool = yield* UpdateTaskTool
      const updateTask = yield* updateTaskTool.init()
      const getTaskTool = yield* GetTaskTool
      const getTask = yield* getTaskTool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* createTeam.execute({ team_name: "red" }, ctx)
      yield* createTask.execute(
        {
          subject: "Run tests",
          description: "Run the test suite",
          owner: "tester",
        },
        ctx,
      )

      const blocked = yield* updateTask.execute({ task_id: "1", status: "completed" }, ctx)
      expect(blocked.title).toBe("Task completion blocked")
      expect(blocked.output).toBe("Tests must pass before completion")
      expect(blocked.metadata.result.success).toBe(false)
      expect(seenTaskSubject).toBe("Run tests")

      const unchanged = yield* getTask.execute({ task_id: "1" }, ctx)
      expect(unchanged.output).toContain("<status>pending</status>")
    }).pipe(
      Effect.provide(
        pluginHook((name, input, output) => {
          if (name !== "swarm.task.completed") return
          seenTaskSubject = (input as { taskSubject: string }).taskSubject
          ;(output as { allow: boolean; message?: string }).allow = false
          ;(output as { allow: boolean; message?: string }).message = "Tests must pass before completion"
        }),
      ),
    )
  })

  it.instance("auto-assigns a worker when it marks an unowned team task in progress", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const createTeamTool = yield* CreateTeamTool
      const createTeam = yield* createTeamTool.init()
      const createTaskTool = yield* CreateTaskTool
      const createTask = yield* createTaskTool.init()
      const updateTaskTool = yield* UpdateTaskTool
      const updateTask = yield* updateTaskTool.init()
      const getTaskTool = yield* GetTaskTool
      const getTask = yield* getTaskTool.init()
      const swarm = yield* SwarmRuntime.Service
      const parentCtx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* createTeam.execute({ team_name: "red" }, parentCtx)
      yield* createTask.execute(
        {
          subject: "Run tests",
          description: "Run the test suite",
        },
        parentCtx,
      )

      const childSessionID = SessionID.descending()
      const worker = yield* swarm.spawn({
        parentSessionID: chat.id,
        sessionID: childSessionID,
        agent: "general",
        name: "tester",
        team: "red",
        prompt: "wait",
        description: "tester",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })
      const workerCtx = {
        ...parentCtx,
        sessionID: childSessionID,
        agent: "general",
      }

      const updated = yield* updateTask.execute({ task_id: "1", status: "in_progress" }, workerCtx)
      expect(updated.metadata.result.updatedFields).toContain("owner")
      expect(updated.metadata.result.updatedFields).toContain("status")

      const detail = yield* getTask.execute({ task_id: "1" }, parentCtx)
      expect(detail.output).toContain("<owner>tester</owner>")
      expect(detail.output).toContain("<status>in_progress</status>")

      yield* swarm.cancel(worker.workerID)
    }),
  )

  it.instance("broadcasts follow-up messages to a background subagent team", () =>
    Effect.gen(function* () {
      const { chat, assistant } = yield* seed()
      const taskTool = yield* TaskTool
      const task = yield* taskTool.init()
      const broadcastTool = yield* BroadcastTool
      const broadcast = yield* broadcastTool.init()
      const seenBySession = new Map<SessionID, number>()
      const followups: SessionPrompt.PromptInput[] = []
      const promptOps: TaskPromptOps = {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
        prompt: (input) =>
          Effect.sync(() => {
            const count = (seenBySession.get(input.sessionID) ?? 0) + 1
            seenBySession.set(input.sessionID, count)
            if (count === 2 && input.parts.some((part) => part.type === "text" && part.text === "team update")) {
              followups.push(input)
            }
            return reply(input, `${input.sessionID}-turn-${count}`)
          }),
      }
      const ctx = {
        sessionID: chat.id,
        messageID: assistant.id,
        agent: "build",
        abort: new AbortController().signal,
        extra: { promptOps, allowForegroundTask: true },
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      const first = yield* task.execute(
        {
          description: "team worker one",
          prompt: "initial one",
          subagent_type: "general",
          run_in_background: true,
          name: "team-one",
          team: "red",
        },
        ctx,
      )
      const second = yield* task.execute(
        {
          description: "team worker two",
          prompt: "initial two",
          subagent_type: "general",
          run_in_background: true,
          name: "team-two",
          team: "red",
        },
        ctx,
      )

      const swarm = yield* SwarmRuntime.Service
      for (let i = 0; i < 50; i++) {
        const workers = yield* swarm.list(chat.id)
        if (workers.filter((worker) => worker.spec.team === "red" && worker.status === "idle").length === 2) break
        yield* Effect.sleep("10 millis")
      }

      const sent = yield* broadcast.execute(
        {
          team: "red",
          message: "team update",
          summary: "sync team",
        },
        ctx,
      )

      expect(sent.metadata.count).toBe(2)
      for (let i = 0; i < 50 && followups.length < 2; i++) {
        yield* Effect.sleep("10 millis")
      }
      expect(followups.map((input) => input.sessionID).sort()).toEqual(
        [first.metadata.sessionId, second.metadata.sessionId].sort(),
      )
      expect(followups.every((input) => input.parts.some((part) => part.type === "text" && part.text === "team update")))
        .toBe(true)

      yield* swarm.cancel(first.metadata.workerId)
      yield* swarm.cancel(second.metadata.workerId)
    }),
  )

  it.instance(
    "execute shapes child permissions for worker, todowrite, and primary tools",
    () =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const { chat, assistant } = yield* seed()
        const tool = yield* TaskTool
        const def = yield* tool.init()
        let seen: SessionPrompt.PromptInput | undefined
        const promptOps = stubOps({ onPrompt: (input) => (seen = input) })

        const result = yield* def.execute(
          {
            description: "inspect bug",
            prompt: "look into the cache key path",
            subagent_type: "reviewer",
            run_in_background: false,
          },
          {
            sessionID: chat.id,
            messageID: assistant.id,
            agent: "build",
            abort: new AbortController().signal,
            extra: { promptOps, allowForegroundTask: true },
            messages: [],
            metadata: () => Effect.void,
            ask: () => Effect.void,
          },
        )

        const child = yield* sessions.get(result.metadata.sessionId)
        expect(child.parentID).toBe(chat.id)
        expect(child.permission).toEqual(expect.arrayContaining([
          {
            permission: "todowrite",
            pattern: "*",
            action: "deny",
          },
          {
            permission: "bash",
            pattern: "*",
            action: "allow",
          },
          {
            permission: "read",
            pattern: "*",
            action: "allow",
          },
          {
            permission: "task",
            pattern: "*",
            action: "deny",
          },
          {
            permission: "create_team",
            pattern: "*",
            action: "deny",
          },
          {
            permission: "goal_create",
            pattern: "*",
            action: "deny",
          },
        ]))
        expect(seen?.tools).toMatchObject({
          todowrite: false,
          task: false,
          bash: false,
          read: false,
          create_team: false,
          delete_team: false,
          wait_task: false,
          cancel_task: false,
          control_task_pane: false,
          read_task_output: false,
          remote_trigger: false,
          goal_create: false,
          goal_update: false,
        })
      }),
    {
      config: {
        agent: {
          reviewer: {
            mode: "subagent",
            permission: {
              task: "allow",
            },
          },
        },
        experimental: {
          primary_tools: ["bash", "read"],
        },
      },
    },
  )
})
