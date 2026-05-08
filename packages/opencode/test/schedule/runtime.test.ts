import { afterEach, describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect, Layer } from "effect"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Bus } from "@/bus"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "@/session/schema"
import { ScheduledTask } from "@/schedule/runtime"
import { SwarmRuntime } from "@/swarm/runtime"
import { Storage } from "@/storage/storage"
import { disposeAllInstances } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

afterEach(async () => {
  await disposeAllInstances()
})

const swarm = SwarmRuntime.layer.pipe(Layer.provideMerge(Bus.layer), Layer.provideMerge(Storage.defaultLayer))
const scheduled = ScheduledTask.layer.pipe(
  Layer.provideMerge(AppFileSystem.defaultLayer),
  Layer.provideMerge(Bus.layer),
  Layer.provideMerge(swarm),
)

const it = testEffect(Layer.mergeAll(swarm, scheduled))

describe("schedule.runtime", () => {
  it.instance("creates, lists, fires, and deletes one-shot tasks", () =>
    Effect.gen(function* () {
      const scheduled = yield* ScheduledTask.Service
      const fired: string[] = []
      yield* scheduled.configureRunner((task) =>
        Effect.sync(() => {
          fired.push(task.prompt)
        }),
      )

      const due = nextMinuteSchedule()
      const task = yield* scheduled.create({
        sessionID: SessionID.descending(),
        parentSessionID: SessionID.descending(),
        agent: "build",
        cron: due.cron,
        prompt: "check status",
        recurring: false,
      })

      expect((yield* scheduled.list({ includeAll: true })).map((item) => item.id)).toEqual([task.id])
      expect(yield* scheduled.fireDue(Date.now())).toEqual([])
      expect((yield* scheduled.fireDue(due.at)).map((item) => item.id)).toEqual([task.id])
      expect(fired).toEqual(["check status"])
      expect(yield* scheduled.list({ includeAll: true })).toEqual([])
    }),
  )

  it.instance("routes subagent schedules through the swarm mailbox", () =>
    Effect.gen(function* () {
      const scheduled = yield* ScheduledTask.Service
      const swarm = yield* SwarmRuntime.Service
      const parentSessionID = SessionID.descending()
      const worker = yield* swarm.spawn({
        parentSessionID,
        sessionID: SessionID.descending(),
        agent: "build",
        name: "worker-a",
        prompt: "initial",
        description: "worker",
        wait: false,
        executionStrategy: "persistent",
        run: Effect.never,
      })

      const due = nextMinuteSchedule()
      const task = yield* scheduled.create({
        sessionID: worker.sessionID,
        parentSessionID,
        agent: "build",
        cron: due.cron,
        prompt: "scheduled worker prompt",
        recurring: false,
        targetWorkerID: worker.workerID,
        targetName: "worker-a",
      })
      const fired = yield* scheduled.fireDue(due.at)
      expect(fired.map((item) => item.id)).toEqual([task.id])

      const input = yield* swarm.awaitInput(worker.workerID)
      expect(input.from).toBe("scheduled_task")
      expect(input.summary).toBe(`scheduled task ${task.id}`)
      expect(input.message).toContain("scheduled worker prompt")

      yield* swarm.cancel(worker.workerID)
    }),
  )

  it.instance("persists durable parent schedules to .opencode/scheduled_tasks.json", () =>
    Effect.gen(function* () {
      const scheduled = yield* ScheduledTask.Service
      const task = yield* scheduled.create({
        sessionID: SessionID.descending(),
        parentSessionID: SessionID.descending(),
        agent: "build",
        cron: "*/5 * * * *",
        prompt: "durable check",
        durable: true,
      })

      const ctx = yield* InstanceState.context
      const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
      const file = Bun.file(path.join(root, ".opencode", "scheduled_tasks.json"))
      const persisted = (yield* Effect.promise(() => file.json())) as { tasks: Array<{ id: string; prompt: string }> }
      expect(persisted.tasks).toEqual([expect.objectContaining({ id: task.id, prompt: "durable check" })])

      yield* scheduled.delete({ id: task.id })
      const afterDelete = (yield* Effect.promise(() => file.json())) as { tasks: Array<{ id: string }> }
      expect(afterDelete.tasks).toEqual([])
    }),
  )

  it.instance("refreshes and preserves durable schedules written by another process", () =>
    Effect.gen(function* () {
      const scheduled = yield* ScheduledTask.Service
      const ctx = yield* InstanceState.context
      const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
      const filePath = path.join(root, ".opencode", "scheduled_tasks.json")
      const parentSessionID = SessionID.descending()
      const remoteTask = {
        id: "sch_remote",
        sessionID: parentSessionID,
        parentSessionID,
        agent: "build",
        cron: "*/10 * * * *",
        prompt: "remote durable check",
        recurring: true,
        durable: true,
        createdAt: Date.now(),
      }

      expect(yield* scheduled.list({ includeAll: true })).toEqual([])
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(filePath), { recursive: true })
        await fs.writeFile(
          filePath,
          JSON.stringify(
            {
              version: 1,
              tasks: [remoteTask],
            },
            null,
            2,
          ),
          "utf-8",
        )
      })

      expect((yield* scheduled.list({ includeAll: true })).map((task) => task.prompt)).toEqual([
        "remote durable check",
      ])

      const local = yield* scheduled.create({
        sessionID: parentSessionID,
        parentSessionID,
        agent: "build",
        cron: "*/5 * * * *",
        prompt: "local durable check",
        durable: true,
      })
      const afterCreate = (yield* Effect.promise(() => Bun.file(filePath).json())) as {
        tasks: Array<{ id: string; prompt: string }>
      }
      expect(afterCreate.tasks.map((task) => task.prompt).sort()).toEqual([
        "local durable check",
        "remote durable check",
      ])

      yield* scheduled.delete({ id: remoteTask.id })
      const afterDelete = (yield* Effect.promise(() => Bun.file(filePath).json())) as {
        tasks: Array<{ id: string; prompt: string }>
      }
      expect(afterDelete.tasks).toEqual([expect.objectContaining({ id: local.id, prompt: "local durable check" })])
    }),
  )

  it.instance("surfaces missed durable one-shot tasks for confirmation instead of executing them", () =>
    Effect.gen(function* () {
      const ctx = yield* InstanceState.context
      const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
      const filePath = path.join(root, ".opencode", "scheduled_tasks.json")
      const sessionID = SessionID.descending()
      yield* Effect.promise(() =>
        fs
          .mkdir(path.dirname(filePath), { recursive: true })
          .then(() =>
            Bun.write(
              filePath,
              JSON.stringify(
                {
                  version: 1,
                  tasks: [
                    {
                      id: "sch_missed",
                      sessionID,
                      parentSessionID: sessionID,
                      agent: "build",
                      cron: "1 9 1 1 *",
                      prompt: "deploy the missed thing",
                      recurring: false,
                      durable: true,
                      createdAt: new Date(2020, 0, 1, 8, 0, 0).getTime(),
                    },
                  ],
                },
                null,
                2,
              ),
            ),
          ),
      )

      const scheduled = yield* ScheduledTask.Service
      const fired: string[] = []
      yield* scheduled.configureRunner((task) =>
        Effect.sync(() => {
          fired.push(task.prompt)
        }),
      )
      yield* scheduled.ensureStarted()

      expect(fired).toHaveLength(1)
      expect(fired[0]).toContain("<missed-scheduled-tasks")
      expect(fired[0]).toContain("Do NOT execute")
      expect(fired[0]).toContain("deploy the missed thing")

      const afterStart = (yield* Effect.promise(() => Bun.file(filePath).json())) as { tasks: Array<{ id: string }> }
      expect(afterStart.tasks).toEqual([])
    }),
  )
})

function nextMinuteSchedule() {
  const due = new Date(Date.now() + 2 * 60 * 1000)
  due.setSeconds(0, 0)
  if (due.getMinutes() % 30 === 0) due.setMinutes(due.getMinutes() + 1)
  return {
    at: due.getTime(),
    cron: `${due.getMinutes()} ${due.getHours()} ${due.getDate()} ${due.getMonth() + 1} *`,
  }
}
