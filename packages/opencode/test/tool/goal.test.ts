import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer } from "effect"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { Session } from "@/session/session"
import { MessageID } from "@/session/schema"
import { GoalCreateTool } from "@/tool/goal"
import { Truncate } from "@/tool/truncate"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

afterEach(async () => {
  await disposeAllInstances()
})

const it = testEffect(Layer.mergeAll(Agent.defaultLayer, Config.defaultLayer, Session.defaultLayer, Truncate.defaultLayer))

describe("tool.goal", () => {
  it.instance("ignores tokenBudget unless it was explicitly requested", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const chat = yield* session.create({ title: "Goal" })
      const tool = yield* GoalCreateTool
      const def = yield* tool.init()
      const ctx = {
        sessionID: chat.id,
        messageID: MessageID.ascending(),
        agent: "build",
        abort: new AbortController().signal,
        extra: {},
        messages: [],
        metadata: () => Effect.void,
        ask: () => Effect.void,
      }

      yield* def.execute(
        {
          objective: "Research opencode architecture",
          tokenBudget: 12000,
        },
        ctx,
      )

      const withoutExplicitBudget = yield* session.get(chat.id)
      expect(withoutExplicitBudget.goal?.tokenBudget).toBeUndefined()

      yield* def.execute(
        {
          objective: "Research opencode architecture",
          tokenBudget: 12000,
          budgetExplicitlyRequested: true,
        },
        ctx,
      )

      const withExplicitBudget = yield* session.get(chat.id)
      expect(withExplicitBudget.goal?.tokenBudget).toBe(12000)
    }),
  )
})
