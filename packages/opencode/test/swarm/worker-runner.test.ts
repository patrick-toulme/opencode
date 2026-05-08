import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { Bus } from "@/bus"
import { Plugin } from "@/plugin"
import { Session } from "@/session/session"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { ModelID, ProviderID } from "@/provider/schema"
import { SessionPrompt } from "@/session/prompt"
import { Storage } from "@/storage/storage"
import { SwarmRuntime } from "@/swarm/runtime"
import { runExternalWorker } from "@/swarm/worker-runner"
import { WorkerID, type WorkerCompletion } from "@/swarm/state"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

afterEach(async () => {
  await disposeAllInstances()
})

const ref = {
  providerID: ProviderID.make("test"),
  modelID: ModelID.make("test-model"),
}

function promptLayer(prompts: string[], inputs: SessionPrompt.PromptInput[] = []) {
  return Layer.mock(SessionPrompt.Service)({
    cancel: () => Effect.void,
    resolvePromptParts: (template) => Effect.succeed([{ type: "text" as const, text: template }]),
    prompt: (input) =>
      Effect.sync(() => {
        inputs.push(input)
        prompts.push(input.parts.find((part) => part.type === "text")?.text ?? "")
        return reply(input, `reply-${prompts.length}`)
      }),
    loop: () => Effect.die("unused"),
    shell: () => Effect.die("unused"),
    command: () => Effect.die("unused"),
  })
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

const pluginLayer = Layer.mock(Plugin.Service)({
  trigger: <Name extends string, Input, Output>(_name: Name, _input: Input, output: Output) => Effect.succeed(output),
  list: () => Effect.succeed([]),
  init: () => Effect.void,
})

const waitForResult = (swarm: SwarmRuntime.Interface, workerID: WorkerID, text: string) =>
  Effect.gen(function* () {
    for (let i = 0; i < 80; i++) {
      const worker = yield* swarm.get(workerID)
      if (worker?.status === "idle" && worker.result?.text === text) return worker
      yield* Effect.sleep("25 millis")
    }
    throw new Error(`worker did not reach idle result ${text}`)
  })

describe("swarm worker runner", () => {
  const prompts: string[] = []
  const inputs: SessionPrompt.PromptInput[] = []
  const it = testEffect(
    Layer.mergeAll(
      Bus.layer,
      Storage.defaultLayer,
      Session.defaultLayer,
      SwarmRuntime.defaultLayer,
      promptLayer(prompts, inputs),
      pluginLayer,
    ),
  )

  it.instance("adopts an externally launched worker and keeps it idle for follow-up input", () =>
    Effect.gen(function* () {
      prompts.length = 0
      inputs.length = 0
      const sessions = yield* Session.Service
      const swarm = yield* SwarmRuntime.Service
      const parent = yield* sessions.create({ title: "Parent" })
      const child = yield* sessions.create({ parentID: parent.id, title: "Child" })
      const workerID = WorkerID.ascending()

      yield* swarm.spawn({
        workerID,
        parentSessionID: parent.id,
        sessionID: child.id,
        agent: "general",
        name: "pane-worker",
        prompt: "initial assignment",
        description: "pane worker",
        model: ref,
        backend: "tmux",
        executionStrategy: "persistent",
        wait: false,
        launch: Effect.void,
      })

      yield* swarm.adopt({
        workerID,
        wait: false,
        run: runExternalWorker(workerID).pipe(Effect.orDie) as Effect.Effect<WorkerCompletion>,
      })

      const idle = yield* swarm.wait({ parentSessionID: parent.id, to: workerID, timeoutMS: 1_000 })
      expect(idle.status).toBe("idle")
      expect(idle.result?.text).toBe("reply-1")
      expect(prompts[0]).toContain("initial assignment")
      expect(inputs[0]?.persistToolPermissions).toBe(false)

      yield* swarm.sendInput({
        parentSessionID: parent.id,
        to: workerID,
        message: "follow up",
        from: "test",
      })

      const secondIdle = yield* waitForResult(swarm, workerID, "reply-2")
      expect(secondIdle.status).toBe("idle")
      expect(secondIdle.result?.text).toBe("reply-2")
      expect(prompts[1]).toContain("follow up")

      yield* swarm.cancel(workerID)
    }),
  )
})
