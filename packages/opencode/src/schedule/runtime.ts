import path from "path"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Flock } from "@opencode-ai/core/util/flock"
import { Bus } from "@/bus"
import { EffectBridge } from "@/effect/bridge"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "@/session/schema"
import { SwarmRuntime } from "@/swarm/runtime"
import { type WorkerStatus, WorkerID } from "@/swarm/state"
import { Context, Effect, Fiber, Layer } from "effect"
import {
  cronToHuman,
  isRecurringTaskAged,
  jitteredNextCronRunMs,
  nextCronRunMs,
  oneShotJitteredNextCronRunMs,
  parseCronExpression,
} from "./cron"
import { Event } from "./events"
import { ScheduledTaskID, type ScheduledTaskSnapshot, type ScheduledTaskState } from "./state"

const MAX_TASKS = 50
const CHECK_INTERVAL = "1 second"
const terminalStatuses = new Set<WorkerStatus>(["completed", "cancelled", "failed", "interrupted"])

export type CreateInput = {
  sessionID: SessionID
  parentSessionID: SessionID
  agent: string
  cron: string
  prompt: string
  recurring?: boolean
  durable?: boolean
  targetWorkerID?: WorkerID
  targetName?: string
  targetTeam?: string
}

export type ListInput = {
  sessionID?: SessionID
  parentSessionID?: SessionID
  ownerWorkerID?: WorkerID
  includeAll?: boolean
}

export type DeleteInput = {
  id: string
  ownerWorkerID?: WorkerID
}

type Runner = (task: ScheduledTaskSnapshot) => Effect.Effect<void>

type State = {
  path: string
  tasks: Map<ScheduledTaskID, ScheduledTaskState>
  nextFireAt: Map<ScheduledTaskID, number>
  runner?: Runner
  missedSurfaced: Set<ScheduledTaskID>
  started: boolean
  fiber?: Fiber.Fiber<void, never>
}

type DurableFile = {
  version: 1
  tasks: ScheduledTaskState[]
}

type MissedOneShot = {
  original: ScheduledTaskState
  notification: ScheduledTaskState
}

export interface Interface {
  readonly create: (input: CreateInput) => Effect.Effect<ScheduledTaskSnapshot, Error>
  readonly list: (input?: ListInput) => Effect.Effect<ScheduledTaskSnapshot[]>
  readonly delete: (input: DeleteInput) => Effect.Effect<ScheduledTaskSnapshot | undefined, Error>
  readonly configureRunner: (runner: Runner) => Effect.Effect<void>
  readonly ensureStarted: () => Effect.Effect<void, Error>
  readonly fireDue: (now?: number) => Effect.Effect<ScheduledTaskSnapshot[], Error>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ScheduledTask") {}

export const layer: Layer.Layer<Service, never, AppFileSystem.Service | Bus.Service | SwarmRuntime.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service
    const bus = yield* Bus.Service
    const swarm = yield* SwarmRuntime.Service

    const state = yield* InstanceState.make<State>(
      Effect.fn("ScheduledTask.state")(function* (ctx) {
        const root = ctx.worktree === "/" ? ctx.directory : ctx.worktree
        const filepath = path.join(root, ".opencode", "scheduled_tasks.json")
        const tasks = yield* readDurable(fs, filepath)
        const s: State = {
          path: filepath,
          tasks: new Map(tasks.map((task) => [task.id, task])),
          nextFireAt: new Map(),
          missedSurfaced: new Set(),
          started: false,
        }
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (s.fiber) yield* Fiber.interrupt(s.fiber).pipe(Effect.ignore)
            s.tasks.clear()
            s.nextFireAt.clear()
            s.missedSurfaced.clear()
          }),
        )
        return s
      }),
    )

    const snapshot = (task: ScheduledTaskState): ScheduledTaskSnapshot => ({ ...task })
    const withScheduleLock = <A, E, R>(s: State, body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.scoped(Flock.effect(`schedule:${s.path}`).pipe(Effect.orDie, Effect.flatMap(() => body)))
    const refreshDurable = Effect.fn("ScheduledTask.refreshDurable")(function* (s: State) {
      const durable = yield* readDurable(fs, s.path)
      const durableIDs = new Set(durable.map((task) => task.id))
      for (const [id, task] of s.tasks) {
        if (!task.durable || durableIDs.has(id)) continue
        s.tasks.delete(id)
        s.nextFireAt.delete(id)
        s.missedSurfaced.delete(id)
      }
      for (const task of durable) {
        s.tasks.set(task.id, task)
      }
    })
    const writeDurable = (s: State) =>
      fs
        .writeWithDirs(
          s.path,
          JSON.stringify(
            {
              version: 1,
              tasks: Array.from(s.tasks.values()).filter((task) => task.durable),
            } satisfies DurableFile,
            null,
            2,
          ) + "\n",
        )
        .pipe(Effect.mapError((error) => new Error(`Failed to write scheduled tasks: ${String(error)}`)))

    const create: Interface["create"] = Effect.fn("ScheduledTask.create")(function* (input) {
      if (!parseCronExpression(input.cron)) {
        return yield* Effect.fail(new Error(`Invalid cron expression '${input.cron}'. Expected 5 fields: M H DoM Mon DoW.`))
      }
      if (nextCronRunMs(input.cron, Date.now()) === null) {
        return yield* Effect.fail(new Error(`Cron expression '${input.cron}' does not match any calendar date in the next year.`))
      }

      const s = yield* InstanceState.get(state)
      const task = yield* withScheduleLock(
        s,
        Effect.gen(function* () {
          yield* refreshDurable(s)
          if (s.tasks.size >= MAX_TASKS) {
            return yield* Effect.fail(new Error(`Too many scheduled tasks (max ${MAX_TASKS}). Delete one first.`))
          }

          const task: ScheduledTaskState = {
            id: ScheduledTaskID.ascending(),
            sessionID: input.sessionID,
            parentSessionID: input.parentSessionID,
            agent: input.agent,
            cron: input.cron,
            prompt: input.prompt,
            recurring: input.recurring ?? true,
            durable: input.durable ?? false,
            createdAt: Date.now(),
            ...(input.targetWorkerID ? { targetWorkerID: input.targetWorkerID } : {}),
            ...(input.targetName ? { targetName: input.targetName } : {}),
            ...(input.targetTeam ? { targetTeam: input.targetTeam } : {}),
          }
          s.tasks.set(task.id, task)
          s.nextFireAt.delete(task.id)
          if (task.durable) yield* writeDurable(s)
          return task
        }),
      )
      yield* bus.publish(Event.Created, { taskID: task.id, task: snapshot(task) })
      return snapshot(task)
    })

    const list: Interface["list"] = Effect.fn("ScheduledTask.list")(function* (input = {}) {
      const s = yield* InstanceState.get(state)
      yield* withScheduleLock(s, refreshDurable(s))
      const tasks = Array.from(s.tasks.values()).filter((task) => {
        if (input.includeAll) return true
        if (input.ownerWorkerID) return task.targetWorkerID === input.ownerWorkerID
        if (input.parentSessionID) return task.parentSessionID === input.parentSessionID
        if (input.sessionID) return task.sessionID === input.sessionID || task.parentSessionID === input.sessionID
        return true
      })
      return tasks.toSorted((a, b) => a.createdAt - b.createdAt || a.id.localeCompare(b.id)).map(snapshot)
    })

    const deleteTask: Interface["delete"] = Effect.fn("ScheduledTask.delete")(function* (input) {
      const s = yield* InstanceState.get(state)
      const task = yield* withScheduleLock(
        s,
        Effect.gen(function* () {
          yield* refreshDurable(s)
          const id = ScheduledTaskID.ascending(input.id)
          const task = s.tasks.get(id)
          if (!task) return undefined
          if (input.ownerWorkerID && task.targetWorkerID !== input.ownerWorkerID) {
            return yield* Effect.fail(new Error(`Cannot delete scheduled task '${input.id}': owned by another subagent.`))
          }
          s.tasks.delete(id)
          s.nextFireAt.delete(id)
          s.missedSurfaced.delete(id)
          if (task.durable) yield* writeDurable(s)
          return task
        }),
      )
      if (!task) return undefined
      yield* bus.publish(Event.Deleted, { taskID: task.id, task: snapshot(task) })
      return snapshot(task)
    })

    const configureRunner: Interface["configureRunner"] = Effect.fn("ScheduledTask.configureRunner")(function* (runner) {
      const s = yield* InstanceState.get(state)
      s.runner = runner
    })

    const fireDue: Interface["fireDue"] = Effect.fn("ScheduledTask.fireDue")(function* (now = Date.now()) {
      const s = yield* InstanceState.get(state)
      const due = yield* withScheduleLock(
        s,
        Effect.gen(function* () {
          yield* refreshDurable(s)
          const fired: { task: ScheduledTaskState; deleted: boolean }[] = []
          const durableTouched = new Set<ScheduledTaskID>()

          for (const task of Array.from(s.tasks.values())) {
            const next = ensureNextFire(s, task)
            if (now < next) continue

            const aged = task.recurring && isRecurringTaskAged(task.createdAt, now)
            fired.push({ task, deleted: !task.recurring || aged })
            if (task.recurring && !aged) {
              const updated = { ...task, lastFiredAt: now }
              s.tasks.set(task.id, updated)
              s.nextFireAt.set(task.id, jitteredNextCronRunMs(task.cron, now, task.id) ?? Infinity)
              if (task.durable) durableTouched.add(task.id)
              continue
            }

            s.tasks.delete(task.id)
            s.nextFireAt.delete(task.id)
            s.missedSurfaced.delete(task.id)
            if (task.durable) durableTouched.add(task.id)
          }

          if (durableTouched.size > 0) yield* writeDurable(s)
          return fired
        }),
      )

      const fired: ScheduledTaskSnapshot[] = []
      for (const item of due) {
        const routedToWorker = yield* fireTask(s, item.task)
        fired.push(snapshot(item.task))
        yield* bus.publish(Event.Fired, { taskID: item.task.id, task: snapshot(item.task), routedToWorker })
        if (item.deleted) yield* bus.publish(Event.Deleted, { taskID: item.task.id, task: snapshot(item.task) })
      }
      return fired
    })

    const ensureStarted: Interface["ensureStarted"] = Effect.fn("ScheduledTask.ensureStarted")(function* () {
      const s = yield* InstanceState.get(state)
      if (s.started) return
      s.started = true
      const missed = yield* withScheduleLock(
        s,
        Effect.gen(function* () {
          yield* refreshDurable(s)
          return yield* surfaceMissedOneShots(s, Date.now())
        }),
      )
      for (const task of missed) {
        if (s.runner) yield* s.runner(snapshot(task.notification)).pipe(Effect.ignore)
        yield* bus.publish(Event.Fired, { taskID: task.original.id, task: snapshot(task.original), routedToWorker: false })
        yield* bus.publish(Event.Deleted, { taskID: task.original.id, task: snapshot(task.original) })
      }
      const bridge = yield* EffectBridge.make()
      s.fiber = bridge.fork(
        Effect.forever(
          Effect.gen(function* () {
            yield* Effect.sleep(CHECK_INTERVAL)
            yield* fireDue().pipe(Effect.catchCause(() => Effect.void))
          }),
        ),
      )
    })

    const surfaceMissedOneShots = Effect.fn("ScheduledTask.surfaceMissedOneShots")(function* (s: State, now: number) {
      const missed = Array.from(s.tasks.values()).filter((task) => {
        if (!task.durable || task.recurring || s.missedSurfaced.has(task.id)) return false
        const next = nextCronRunMs(task.cron, task.createdAt)
        return next !== null && next < now
      })
      const result: MissedOneShot[] = []
      for (const task of missed) {
        s.missedSurfaced.add(task.id)
        const notification = {
          ...task,
          durable: false,
          prompt: buildMissedTaskNotification([task]),
        }
        result.push({ original: task, notification })
        s.tasks.delete(task.id)
        s.nextFireAt.delete(task.id)
      }
      if (missed.length > 0) yield* writeDurable(s)
      return result
    })

    const fireTask = Effect.fn("ScheduledTask.fireTask")(function* (s: State, task: ScheduledTaskState) {
      const message = renderFirePrompt(task)
      if (task.targetWorkerID) {
        const worker = yield* swarm.get(task.targetWorkerID)
        if (worker && !terminalStatuses.has(worker.status)) {
          const sent = yield* swarm
            .sendInput({
              parentSessionID: worker.spec.parentSessionID,
              to: worker.spec.workerID,
              message,
              summary: `scheduled task ${task.id}`,
              from: "scheduled_task",
            })
            .pipe(
              Effect.as(true),
              Effect.catchCause(() => Effect.succeed(false)),
            )
          if (sent) return true
        }
      }

      if (s.runner) {
        yield* s.runner(snapshot(task)).pipe(Effect.ignore)
      }
      return false
    })

    return Service.of({ create, list, delete: deleteTask, configureRunner, ensureStarted, fireDue })
  }),
)

export const defaultLayer = layer.pipe(
  Layer.provide(AppFileSystem.defaultLayer),
  Layer.provide(Bus.layer),
  Layer.provide(SwarmRuntime.defaultLayer),
)

function ensureNextFire(s: State, task: ScheduledTaskState) {
  const existing = s.nextFireAt.get(task.id)
  if (existing !== undefined) return existing
  const from = task.recurring ? (task.lastFiredAt ?? task.createdAt) : task.createdAt
  const next = task.recurring
    ? (jitteredNextCronRunMs(task.cron, from, task.id) ?? Infinity)
    : (oneShotJitteredNextCronRunMs(task.cron, from, task.id) ?? Infinity)
  s.nextFireAt.set(task.id, next)
  return next
}

const readDurable = Effect.fn("ScheduledTask.readDurable")(function* (
  fs: AppFileSystem.Interface,
  filepath: string,
) {
  const raw = yield* fs.readJson(filepath).pipe(Effect.catch(() => Effect.succeed(undefined as unknown)))
  if (!raw || typeof raw !== "object") return [] as ScheduledTaskState[]
  const tasks = Array.isArray((raw as Partial<DurableFile>).tasks) ? (raw as Partial<DurableFile>).tasks! : []
  return tasks
    .filter((task): task is ScheduledTaskState => Boolean(task && typeof task.id === "string"))
    .filter((task) => Boolean(parseCronExpression(task.cron)))
    .map((task) => ({ ...task, durable: true }))
})

export function renderFirePrompt(task: ScheduledTaskSnapshot): string {
  const missed = task.prompt.startsWith("<missed-scheduled-tasks>")
  return [
    `<scheduled-task id="${xmlEscape(task.id)}" recurring="${task.recurring ? "true" : "false"}">`,
    `<cron>${xmlEscape(task.cron)}</cron>`,
    `<human-schedule>${xmlEscape(cronToHuman(task.cron))}</human-schedule>`,
    task.targetWorkerID ? `<target-worker-id>${xmlEscape(task.targetWorkerID)}</target-worker-id>` : "",
    task.targetName ? `<target-name>${xmlEscape(task.targetName)}</target-name>` : "",
    missed
      ? "The following is a missed scheduled task notification. Follow its safety instructions; do not execute the original missed prompt unless the user confirms."
      : "The following prompt was scheduled earlier. Execute it now unless it is obsolete or unsafe.",
    `<prompt>${xmlEscape(task.prompt)}</prompt>`,
    "</scheduled-task>",
  ]
    .filter((line) => line !== "")
    .join("\n")
}

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export function buildMissedTaskNotification(tasks: ScheduledTaskSnapshot[]): string {
  const plural = tasks.length > 1
  const blocks = tasks.map((task) => {
    const longestRun = (task.prompt.match(/`+/g) ?? []).reduce((max, run) => Math.max(max, run.length), 0)
    const fence = "`".repeat(Math.max(3, longestRun + 1))
    return [
      `[${cronToHuman(task.cron)}, created ${new Date(task.createdAt).toLocaleString()}]`,
      fence,
      task.prompt,
      fence,
    ].join("\n")
  })
  return [
    `<missed-scheduled-tasks count="${tasks.length}">`,
    `The following one-shot scheduled task${plural ? "s were" : " was"} missed while opencode was not running. ${plural ? "They have" : "It has"} already been removed from .opencode/scheduled_tasks.json.`,
    "",
    `Do NOT execute ${plural ? "these prompts" : "this prompt"} yet. First ask the user whether to run ${plural ? "each missed task" : "the missed task"} now. Only execute a missed prompt if the user confirms.`,
    "",
    blocks.join("\n\n"),
    "</missed-scheduled-tasks>",
  ].join("\n")
}

export * as ScheduledTask from "./runtime"
