import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import path from "path"
import { WithInstance } from "../../src/project/with-instance"
import { Session as SessionNs } from "@/session/session"
import * as Goal from "@/session/goal"
import type { SessionID } from "@/session/schema"
import * as Log from "@opencode-ai/core/util/log"

const root = path.join(__dirname, "../..")
void Log.init({ print: false })

function run<A, E>(fx: Effect.Effect<A, E, SessionNs.Service>) {
  return Effect.runPromise(fx.pipe(Effect.provide(SessionNs.defaultLayer)))
}

const svc = {
  create(input?: SessionNs.CreateInput) {
    return run(SessionNs.Service.use((s) => s.create(input)))
  },
  remove(id: SessionID) {
    return run(SessionNs.Service.use((s) => s.remove(id)))
  },
  get(id: SessionID) {
    return run(SessionNs.Service.use((s) => s.get(id)))
  },
  setGoal(input: { sessionID: SessionID; objective: string; tokenBudget?: number }) {
    return run(SessionNs.Service.use((s) => s.setGoal(input)))
  },
  updateGoalStatus(input: { sessionID: SessionID; status: SessionNs.GoalStatus }) {
    return run(SessionNs.Service.use((s) => s.updateGoalStatus(input)))
  },
  clearGoal(id: SessionID) {
    return run(SessionNs.Service.use((s) => s.clearGoal(id)))
  },
  trackGoalUsage(input: { sessionID: SessionID; tokens: number; timeMs: number }) {
    return run(SessionNs.Service.use((s) => s.trackGoalUsage(input)))
  },
}

describe("Session goal lifecycle", () => {
  test("setGoal seeds usage counters and survives reload", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const session = await svc.create({})
        const goal = await svc.setGoal({
          sessionID: session.id,
          objective: "ship the auth refactor",
          tokenBudget: 50_000,
        })
        expect(goal.objective).toBe("ship the auth refactor")
        expect(goal.status).toBe("active")
        expect(goal.tokenBudget).toBe(50_000)
        expect(goal.tokensUsed).toBe(0)
        expect(goal.timeUsedMs).toBe(0)
        expect(goal.timeCreated).toBeGreaterThan(0)

        const reloaded = await svc.get(session.id)
        expect(reloaded.goal?.objective).toBe("ship the auth refactor")
        expect(reloaded.goal?.tokenBudget).toBe(50_000)

        await svc.remove(session.id)
      },
    })
  })

  test("trackGoalUsage flips active → budget_limited when budget exhausted", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const session = await svc.create({})
        await svc.setGoal({ sessionID: session.id, objective: "test goal", tokenBudget: 1000 })

        let goal = await svc.trackGoalUsage({ sessionID: session.id, tokens: 600, timeMs: 1500 })
        expect(goal?.status).toBe("active")
        expect(goal?.tokensUsed).toBe(600)
        expect(goal?.timeUsedMs).toBe(1500)

        goal = await svc.trackGoalUsage({ sessionID: session.id, tokens: 500, timeMs: 800 })
        expect(goal?.status).toBe("budget_limited")
        expect(goal?.tokensUsed).toBe(1100)
        expect(goal?.timeUsedMs).toBe(2300)

        // budget_limited is sticky — further usage doesn't flip it back
        goal = await svc.trackGoalUsage({ sessionID: session.id, tokens: 100, timeMs: 50 })
        expect(goal?.status).toBe("budget_limited")

        await svc.remove(session.id)
      },
    })
  })

  test("trackGoalUsage is a no-op when no goal is set", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const session = await svc.create({})
        const result = await svc.trackGoalUsage({ sessionID: session.id, tokens: 100, timeMs: 200 })
        expect(result).toBeUndefined()
        const reloaded = await svc.get(session.id)
        expect(reloaded.goal).toBeUndefined()
        await svc.remove(session.id)
      },
    })
  })

  test("updateGoalStatus → complete persists and clearGoal removes the goal", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const session = await svc.create({})
        await svc.setGoal({ sessionID: session.id, objective: "test goal" })

        const completed = await svc.updateGoalStatus({ sessionID: session.id, status: "complete" })
        expect(completed.status).toBe("complete")

        const persisted = await svc.get(session.id)
        expect(persisted.goal?.status).toBe("complete")

        await svc.clearGoal(session.id)
        const cleared = await svc.get(session.id)
        expect(cleared.goal).toBeUndefined()

        await svc.remove(session.id)
      },
    })
  })

  test("re-setting an objective preserves usage counters but resets status to active", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const session = await svc.create({})
        await svc.setGoal({ sessionID: session.id, objective: "first" })
        await svc.trackGoalUsage({ sessionID: session.id, tokens: 250, timeMs: 100 })
        await svc.updateGoalStatus({ sessionID: session.id, status: "paused" })

        const updated = await svc.setGoal({ sessionID: session.id, objective: "second" })
        expect(updated.objective).toBe("second")
        expect(updated.status).toBe("active")
        expect(updated.tokensUsed).toBe(250)
        expect(updated.timeUsedMs).toBe(100)

        await svc.remove(session.id)
      },
    })
  })
})

describe("Goal.renderSystem template", () => {
  test("renders objective, status, and token meter for active goals", () => {
    const out = Goal.renderSystem(
      {
        objective: "build the thing",
        status: "active",
        tokenBudget: 10_000,
        tokensUsed: 2500,
        timeUsedMs: 65_000,
        timeCreated: 0,
        timeUpdated: 0,
      },
      { isContinuationTurn: false },
    )
    expect(out).toContain("build the thing")
    expect(out).toContain("Status: active")
    expect(out).toContain("2.5K")
    expect(out).toContain("10.0K budget")
    expect(out).toContain("25%")
    expect(out).toContain("1m 5s")
    expect(out).not.toContain("autonomous continuation turn")
  })

  test("hides noisy token totals when no token budget is set", () => {
    const out = Goal.renderSystem(
      {
        objective: "research the codebase",
        status: "active",
        tokensUsed: 1_024_835,
        timeUsedMs: 276_000,
        timeCreated: 0,
        timeUpdated: 0,
      },
      { isContinuationTurn: false },
    )

    expect(out).toContain("Token budget: none set")
    expect(out).not.toContain("1.02M")
  })

  test("flags continuation turns when invoked autonomously", () => {
    const out = Goal.renderSystem(
      {
        objective: "x",
        status: "active",
        tokensUsed: 0,
        timeUsedMs: 0,
        timeCreated: 0,
        timeUpdated: 0,
      },
      { isContinuationTurn: true },
    )
    expect(out).toContain("autonomous continuation turn")
  })

  test("uses budget-limit template when status is budget_limited", () => {
    const out = Goal.renderSystem(
      {
        objective: "x",
        status: "budget_limited",
        tokenBudget: 1000,
        tokensUsed: 1100,
        timeUsedMs: 30_000,
        timeCreated: 0,
        timeUpdated: 0,
      },
      { isContinuationTurn: true },
    )
    expect(out).toContain("budget for this goal has been exhausted")
    expect(out).toContain("wrap-up")
    expect(out).not.toContain("Make tangible progress")
  })
})

describe("Goal.shouldAutoContinue", () => {
  test("only continues when status is active", () => {
    const base = {
      objective: "x",
      tokensUsed: 0,
      timeUsedMs: 0,
      timeCreated: 0,
      timeUpdated: 0,
    }
    expect(Goal.shouldAutoContinue(undefined)).toBe(false)
    expect(Goal.shouldAutoContinue({ ...base, status: "paused" })).toBe(false)
    expect(Goal.shouldAutoContinue({ ...base, status: "complete" })).toBe(false)
    expect(Goal.shouldAutoContinue({ ...base, status: "budget_limited" })).toBe(false)
    expect(Goal.shouldAutoContinue({ ...base, status: "active" })).toBe(true)
  })
})
