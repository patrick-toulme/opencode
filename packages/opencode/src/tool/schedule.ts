import { Effect, Schema } from "effect"
import * as Tool from "./tool"
import { ScheduledTask } from "@/schedule/runtime"
import { cronToHuman, nextCronRunMs } from "@/schedule/cron"
import { SwarmRuntime } from "@/swarm/runtime"

export const ScheduleTaskParameters = Schema.Struct({
  cron: Schema.String.annotate({
    description:
      'Standard 5-field cron expression in local time: "M H DoM Mon DoW" (for example "*/5 * * * *" or "30 14 28 2 *").',
  }),
  prompt: Schema.String.annotate({
    description: "The prompt to enqueue when the schedule fires.",
  }),
  recurring: Schema.optional(Schema.Boolean).annotate({
    description:
      "true (default) fires on every cron match until deleted or auto-expired. false fires once at the next match, then deletes itself.",
  }),
  durable: Schema.optional(Schema.Boolean).annotate({
    description:
      "true persists to .opencode/scheduled_tasks.json and survives restarts. false (default) is in-memory for this opencode process only.",
  }),
  to: Schema.optional(Schema.String).annotate({
    description:
      "Optional target subagent worker_id, session_id, or launch name. Omit to target the current session; from inside a subagent, omit to target that subagent.",
  }),
})

export const ListScheduledTasksParameters = Schema.Struct({})

export const DeleteScheduledTaskParameters = Schema.Struct({
  id: Schema.String.annotate({ description: "Scheduled task ID returned by schedule_task." }),
})

type ScheduleMetadata = {
  id?: string
  count?: number
  recurring?: boolean
  durable?: boolean
  humanSchedule?: string
  nextRunAt?: string
  targetWorkerId?: string
  sessionId?: string
}

export const ScheduleTaskTool = Tool.define(
  "schedule_task",
  Effect.gen(function* () {
    const scheduled = yield* ScheduledTask.Service
    const swarm = yield* SwarmRuntime.Service

    return {
      description:
        "Schedule a prompt to run later. Supports one-shot or recurring local-time cron schedules, session-only or durable parent-session jobs, and live subagent targets.",
      parameters: ScheduleTaskParameters,
      execute: (args: Schema.Schema.Type<typeof ScheduleTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const parentSessionID = currentWorker?.spec.parentSessionID ?? ctx.sessionID
          const explicitTarget = args.to
            ? yield* swarm.resolve({ parentSessionID, to: args.to })
            : undefined
          if (args.to && !explicitTarget) {
            return yield* Effect.fail(new Error(`No subagent found for schedule target: ${args.to}`))
          }
          const target = explicitTarget ?? currentWorker
          if (args.durable && target) {
            return yield* Effect.fail(
              new Error("durable scheduled tasks are not supported for subagents because subagents do not persist across restarts"),
            )
          }

          const task = yield* scheduled.create({
            sessionID: target?.spec.sessionID ?? ctx.sessionID,
            parentSessionID: target?.spec.parentSessionID ?? ctx.sessionID,
            agent: target?.spec.agent ?? ctx.agent,
            cron: args.cron,
            prompt: args.prompt,
            recurring: args.recurring ?? true,
            durable: args.durable ?? false,
            ...(target ? { targetWorkerID: target.spec.workerID } : {}),
            ...(target?.spec.name ? { targetName: target.spec.name } : {}),
            ...(target?.spec.team ? { targetTeam: target.spec.team } : {}),
          })
          yield* scheduled.ensureStarted()

          const nextRun = nextCronRunMs(task.cron, Date.now())
          return {
            title: "Scheduled task created",
            metadata: {
              id: task.id,
              recurring: task.recurring,
              durable: task.durable,
              humanSchedule: cronToHuman(task.cron),
              nextRunAt: nextRun ? new Date(nextRun).toISOString() : undefined,
              targetWorkerId: task.targetWorkerID,
              sessionId: task.sessionID,
            } as ScheduleMetadata,
            output: [
              `${task.recurring ? "Scheduled recurring task" : "Scheduled one-shot task"} ${task.id}.`,
              `schedule: ${cronToHuman(task.cron)}`,
              `durability: ${task.durable ? "durable (.opencode/scheduled_tasks.json)" : "session-only"}`,
              nextRun ? `next_run_at: ${new Date(nextRun).toISOString()}` : undefined,
              task.targetWorkerID ? `target_worker_id: ${task.targetWorkerID}` : `session_id: ${task.sessionID}`,
              task.recurring ? "Recurring tasks auto-expire after 7 days unless deleted sooner." : "The task will auto-delete after it fires.",
            ]
              .filter(Boolean)
              .join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ListScheduledTasksTool = Tool.define(
  "list_scheduled_tasks",
  Effect.gen(function* () {
    const scheduled = yield* ScheduledTask.Service
    const swarm = yield* SwarmRuntime.Service

    return {
      description:
        "List scheduled prompts. Team leads see schedules for the current swarm; subagents see their own schedules.",
      parameters: ListScheduledTasksParameters,
      execute: (_args: Schema.Schema.Type<typeof ListScheduledTasksParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const tasks = yield* scheduled.list(
            currentWorker
              ? { ownerWorkerID: currentWorker.spec.workerID }
              : { parentSessionID: ctx.sessionID },
          )
          return {
            title: "Scheduled tasks",
            metadata: {
              count: tasks.length,
            } as ScheduleMetadata,
            output: tasks.length
              ? [
                  `<scheduled-tasks count="${tasks.length}">`,
                  ...tasks.map((task) =>
                    [
                      "<task>",
                      `<id>${xmlEscape(task.id)}</id>`,
                      `<cron>${xmlEscape(task.cron)}</cron>`,
                      `<human-schedule>${xmlEscape(cronToHuman(task.cron))}</human-schedule>`,
                      `<recurring>${task.recurring ? "true" : "false"}</recurring>`,
                      `<durable>${task.durable ? "true" : "false"}</durable>`,
                      task.targetWorkerID ? `<target-worker-id>${xmlEscape(task.targetWorkerID)}</target-worker-id>` : "",
                      task.targetName ? `<target-name>${xmlEscape(task.targetName)}</target-name>` : "",
                      `<prompt>${xmlEscape(task.prompt)}</prompt>`,
                      "</task>",
                    ]
                      .filter((line) => line !== "")
                      .join("\n"),
                  ),
                  "</scheduled-tasks>",
                ].join("\n")
              : "No scheduled tasks.",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const DeleteScheduledTaskTool = Tool.define(
  "delete_scheduled_task",
  Effect.gen(function* () {
    const scheduled = yield* ScheduledTask.Service
    const swarm = yield* SwarmRuntime.Service

    return {
      description: "Cancel a scheduled prompt by ID.",
      parameters: DeleteScheduledTaskParameters,
      execute: (args: Schema.Schema.Type<typeof DeleteScheduledTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const deleted = yield* scheduled.delete({
            id: args.id,
            ...(currentWorker ? { ownerWorkerID: currentWorker.spec.workerID } : {}),
          })
          if (!deleted) return yield* Effect.fail(new Error(`No scheduled task with id '${args.id}'`))
          return {
            title: "Scheduled task deleted",
            metadata: {
              id: deleted.id,
            } as ScheduleMetadata,
            output: `Cancelled scheduled task ${deleted.id}.`,
          }
        }).pipe(Effect.orDie),
    }
  }),
)

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export * as ScheduleTools from "./schedule"
