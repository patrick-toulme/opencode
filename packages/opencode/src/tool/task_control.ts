import { Effect, Schema } from "effect"
import * as Tool from "./tool"
import { Session } from "@/session/session"
import { MessageV2 } from "@/session/message-v2"
import { SwarmRuntime } from "@/swarm/runtime"
import {
  TeamTaskStatus,
  WorkerStatus,
  type TeamSnapshot,
  type TeamTaskState,
  type WorkerSnapshot,
} from "@/swarm/state"
import { NonNegativeInt } from "@/util/schema"
import { Plugin } from "@/plugin"
import { writeWorkerTranscriptWithSession } from "@/swarm/transcript"

const targetDescription =
  "The target subagent worker_id, task_id/session_id, or launch name. Names are resolved within the current parent session first."

export const ListTasksParameters = Schema.Struct({
  scope: Schema.optional(Schema.Literals(["current", "all"])).annotate({
    description: 'Use "current" to list subagents spawned by this session, or "all" to list every known subagent.',
  }),
  status: Schema.optional(WorkerStatus).annotate({
    description: "Optional status filter.",
  }),
  team: Schema.optional(Schema.String).annotate({
    description: "Optional team filter.",
  }),
})

export const WaitTaskParameters = Schema.Struct({
  task_id: Schema.optional(Schema.String).annotate({
    description: targetDescription,
  }),
  task_ids: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Multiple target subagents to wait for. Each item accepts worker_id, task_id/session_id, or name.",
  }),
  timeout_ms: Schema.optional(NonNegativeInt).annotate({
    description: "Maximum time to wait in milliseconds. If omitted, waits until each task completes, idles, fails, or blocks.",
  }),
})

export const CancelTaskParameters = Schema.Struct({
  task_id: Schema.String.annotate({
    description: targetDescription,
  }),
})

export const ControlTaskPaneParameters = Schema.Struct({
  task_id: Schema.String.annotate({
    description: targetDescription,
  }),
  action: Schema.Literals(["hide", "show"]).annotate({
    description: 'Pane visibility action. Use "hide" to detach the visible pane and "show" to rejoin it.',
  }),
})

export const StopTaskParameters = Schema.Struct({
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "Optional target subagent worker_id, task_id/session_id, or launch name. When omitted from inside a background subagent, stops the current subagent after this turn.",
  }),
  reason: Schema.optional(Schema.String).annotate({
    description: "Optional reason for stopping the subagent.",
  }),
})

export const ReadTaskOutputParameters = Schema.Struct({
  task_id: Schema.String.annotate({
    description: targetDescription,
  }),
  block: Schema.optional(Schema.Boolean).annotate({
    description:
      'Task output compatibility mode. When true, wait until the subagent is idle, completed, failed, cancelled, or blocked before reading. Defaults to false for read_task_output.',
  }),
  timeout_ms: Schema.optional(NonNegativeInt).annotate({
    description: "Maximum wait time in milliseconds when block is true.",
  }),
  include_transcript: Schema.optional(Schema.Boolean).annotate({
    description: "Include the subagent session transcript in addition to the latest task result.",
  }),
  limit: Schema.optional(NonNegativeInt).annotate({
    description: "Maximum number of transcript messages to include when include_transcript is true.",
  }),
})

export const BroadcastParameters = Schema.Struct({
  team: Schema.String.annotate({
    description: "The target subagent team name.",
  }),
  message: Schema.String.annotate({
    description: "The message to send to every active subagent in the team.",
  }),
  summary: Schema.optional(Schema.String).annotate({
    description: "A short 5-10 word summary of the broadcast for status displays.",
  }),
})

export const CreateTeamParameters = Schema.Struct({
  team_name: Schema.String.annotate({
    description: "Name for the new team to create.",
  }),
  description: Schema.optional(Schema.String).annotate({
    description: "Team description or purpose.",
  }),
  agent_type: Schema.optional(Schema.String).annotate({
    description: "Type or role of the team lead for coordination metadata.",
  }),
})

export const ListTeamsParameters = Schema.Struct({})

export const DeleteTeamParameters = Schema.Struct({
  team_name: Schema.String.annotate({
    description: "Name of the team to delete.",
  }),
  cancel_workers: Schema.optional(Schema.Boolean).annotate({
    description:
      "Force-cancel active workers in the team before deleting it. Defaults to false; without this, active teams must be gracefully shut down first.",
  }),
})

const Metadata = Schema.Record(Schema.String, Schema.Unknown)

const refreshTranscript = (sessions: Session.Interface, worker: WorkerSnapshot) =>
  writeWorkerTranscriptWithSession(sessions, {
    workerID: worker.spec.workerID,
    sessionID: worker.spec.sessionID,
    outputPath: worker.spec.outputPath,
  }).pipe(Effect.ignore)

type DeleteTeamMetadata = {
  teamName: string
  error?: string
  activeWorkers: WorkerSnapshot[]
  team?: TeamSnapshot
}

type CreateTeamMetadata = {
  requestedTeamName: string
  existingTeam?: TeamSnapshot
  team?: TeamSnapshot
}

export const CreateTaskParameters = Schema.Struct({
  team: Schema.optional(Schema.String).annotate({
    description: "Optional team task list. Defaults to the current worker's team, the only active team, or this session.",
  }),
  subject: Schema.String.annotate({
    description: "Brief actionable task title.",
  }),
  description: Schema.String.annotate({
    description: "Detailed requirements and context for the task.",
  }),
  active_form: Schema.optional(Schema.String).annotate({
    description: 'Present continuous form shown while in progress, e.g. "Running tests".',
  }),
  owner: Schema.optional(Schema.String).annotate({
    description: "Optional worker/team member name to assign this task to.",
  }),
  metadata: Schema.optional(Metadata).annotate({
    description: "Optional arbitrary metadata to attach to the task.",
  }),
})

export const UpdateTaskParameters = Schema.Struct({
  team: Schema.optional(Schema.String).annotate({
    description: "Optional team task list. Defaults to the current worker's team, the only active team, or this session.",
  }),
  task_id: Schema.String.annotate({
    description: "Task id to update.",
  }),
  subject: Schema.optional(Schema.String).annotate({
    description: "New task title.",
  }),
  description: Schema.optional(Schema.String).annotate({
    description: "New task description.",
  }),
  active_form: Schema.optional(Schema.String).annotate({
    description: "New active-form text.",
  }),
  status: Schema.optional(Schema.Union([TeamTaskStatus, Schema.Literal("deleted")])).annotate({
    description: 'New status: "pending", "in_progress", "completed", or "deleted".',
  }),
  owner: Schema.optional(Schema.String).annotate({
    description: "New task owner/worker name.",
  }),
  add_blocks: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Task ids that this task blocks.",
  }),
  add_blocked_by: Schema.optional(Schema.Array(Schema.String)).annotate({
    description: "Task ids that must complete before this task can start.",
  }),
  metadata: Schema.optional(Metadata).annotate({
    description: "Metadata keys to merge. Set a key to null to delete it.",
  }),
})

export const GetTaskParameters = Schema.Struct({
  team: Schema.optional(Schema.String).annotate({
    description: "Optional team task list. Defaults to the current worker's team, the only active team, or this session.",
  }),
  task_id: Schema.String.annotate({
    description: "Task id to retrieve.",
  }),
})

export const ListTeamTasksParameters = Schema.Struct({
  team: Schema.optional(Schema.String).annotate({
    description: "Optional team task list. Defaults to the current worker's team, the only active team, or this session.",
  }),
  status: Schema.optional(TeamTaskStatus).annotate({
    description: "Optional task status filter.",
  }),
  owner: Schema.optional(Schema.String).annotate({
    description: "Optional owner filter.",
  }),
})

const LIST_DESCRIPTION = `List subagents spawned by this session or all known subagents.

Use this to inspect background task status, current tool activity, pending permission requests, mailbox size, and final result metadata before deciding whether to wait, cancel, read output, or send a follow-up message.`

const WAIT_DESCRIPTION = `Wait for one or more subagents to reach an actionable state.

For background agents this returns when the subagent becomes idle after a turn, completes, fails, is cancelled, or blocks on permission. For foreground/oneshot agents this returns when the task reaches a terminal state.`

const CANCEL_DESCRIPTION = `Cancel a running or idle subagent.

This interrupts the subagent runtime and its child prompt session, then records the worker as cancelled.`

const CONTROL_PANE_DESCRIPTION = `Hide or show a visible terminal pane for an external subagent.

This is only available for pane-capable backends such as tmux. It does not stop the subagent; use cancel_task or stop_task for lifecycle control.`

const STOP_DESCRIPTION = `Stop a running background subagent.

When called by a background subagent without task_id, it marks itself stopped after the current turn. When task_id is provided, it stops that target subagent by worker_id, task_id/session_id, or name.`

const READ_DESCRIPTION = `Read a subagent's latest result and optionally its transcript.

Use this when a background subagent has completed a turn, failed, or produced a result that needs to be inspected without sending it another message.`

const BROADCAST_DESCRIPTION = `Send the same message to every active subagent in a team.

Use this to coordinate swarms of background agents, distribute updated instructions, or tell a group of teammate agents to stop, converge, or report status.`

const CREATE_TEAM_DESCRIPTION = `Create a named team for coordinating multiple background subagents.

Use this before spawning related teammate-style agents. Pass the same team name to task(..., run_in_background=true, team=...) so the agents can be listed, broadcast to, and cleaned up together. A lead session can manage one active team at a time; delete the current team before creating another.`

const LIST_TEAMS_DESCRIPTION = `List active subagent teams for the current parent session.`

const DELETE_TEAM_DESCRIPTION = `Delete a subagent team after its active workers have shut down.

By default this refuses teams with active workers for graceful lifecycle management. Pass cancel_workers=true only when you intentionally want to force-cancel active workers.`

const CREATE_TASK_DESCRIPTION = `Create a task in the shared team task list.

Use this for swarm coordination: break complex work into explicit tasks, then assign them to teammates with update_task(owner=...). Tasks default to pending and are visible to every worker in the same team.`

const UPDATE_TASK_DESCRIPTION = `Update a shared team task.

Use this to claim work, assign work to another teammate, mark tasks in_progress/completed, delete obsolete tasks, and add dependency edges between tasks. Assigning owner to an active worker queues an assignment message for that worker.`

const GET_TASK_DESCRIPTION = `Read the full details for a shared team task before starting or updating it.`

const LIST_TEAM_TASKS_DESCRIPTION = `List shared team tasks.

Use this to find pending unowned work, inspect blocked tasks, and coordinate what teammates should do next.`

const resolveTaskBoard = Effect.fn("TaskControl.resolveTaskBoard")(function* (
  swarm: SwarmRuntime.Interface,
  ctx: Tool.Context,
  team?: string,
) {
  const worker = yield* swarm.getBySession(ctx.sessionID)
  if (worker) {
    return {
      parentSessionID: worker.spec.parentSessionID,
      ...(team ?? worker.spec.team ? { team: team ?? worker.spec.team } : {}),
    }
  }

  if (team) return { parentSessionID: ctx.sessionID, team }

  const teams = yield* swarm.listTeams(ctx.sessionID)
  if (teams.length === 1 && teams[0]) {
    return { parentSessionID: ctx.sessionID, team: teams[0].name }
  }

  return { parentSessionID: ctx.sessionID }
})

export const ListTasksTool = Tool.define(
  "list_tasks",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: LIST_DESCRIPTION,
      parameters: ListTasksParameters,
      execute: (args: Schema.Schema.Type<typeof ListTasksParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const tasks = (yield* swarm.list(args.scope === "all" ? undefined : ctx.sessionID)).filter(
            (task) => (!args.status || task.status === args.status) && (!args.team || task.spec.team === args.team),
          )

          return {
            title: "Subagents",
            metadata: {
              count: tasks.length,
              tasks,
            },
            output: tasks.length
              ? [`<tasks count="${tasks.length}">`, ...tasks.map(formatWorker), "</tasks>"].join("\n")
              : "No subagents found.",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const CreateTeamTool = Tool.define(
  "create_team",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: CREATE_TEAM_DESCRIPTION,
      parameters: CreateTeamParameters,
      execute: (args: Schema.Schema.Type<typeof CreateTeamParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const existing = yield* swarm.listTeams(ctx.sessionID)
          if (existing.length > 0) {
            const metadata: CreateTeamMetadata = {
              requestedTeamName: args.team_name,
              existingTeam: existing[0],
            }
            return {
              title: "Team not created",
              metadata,
              output: [
                `Already leading team "${existing[0]!.name}".`,
                "A lead session can only manage one active team at a time. Use delete_team to end the current team before creating another.",
                formatTeam(existing[0]!),
              ].join("\n"),
            }
          }
          const team = yield* swarm.createTeam({
            parentSessionID: ctx.sessionID,
            name: args.team_name,
            description: args.description,
            leadSessionID: ctx.sessionID,
            agentType: args.agent_type,
          })
          const metadata: CreateTeamMetadata = {
            requestedTeamName: args.team_name,
            team,
          }
          return {
            title: "Team created",
            metadata,
            output: ["Team created.", formatTeam(team)].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ListTeamsTool = Tool.define(
  "list_teams",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: LIST_TEAMS_DESCRIPTION,
      parameters: ListTeamsParameters,
      execute: (_args: Schema.Schema.Type<typeof ListTeamsParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const teams = yield* swarm.listTeams(ctx.sessionID)
          return {
            title: "Teams",
            metadata: {
              count: teams.length,
              teams,
            },
            output: teams.length
              ? [`<teams count="${teams.length}">`, ...teams.map(formatTeam), "</teams>"].join("\n")
              : "No subagent teams found.",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const DeleteTeamTool = Tool.define(
  "delete_team",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: DELETE_TEAM_DESCRIPTION,
      parameters: DeleteTeamParameters,
      execute: (args: Schema.Schema.Type<typeof DeleteTeamParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const result = yield* swarm
            .deleteTeam({
              parentSessionID: ctx.sessionID,
              name: args.team_name,
              cancelWorkers: args.cancel_workers,
            })
            .pipe(
              Effect.match({
                onFailure: (error) => ({ ok: false as const, error }),
                onSuccess: (team) => ({ ok: true as const, team }),
              }),
            )

          if (!result.ok) {
            const activeWorkers = (yield* swarm.list(ctx.sessionID)).filter(
              (worker) => worker.spec.team === args.team_name && isActiveWorker(worker),
            )
            const metadata: DeleteTeamMetadata = {
              teamName: args.team_name,
              error: result.error.message,
              activeWorkers,
            }
            return {
              title: "Team not deleted",
              metadata,
              output: [
                result.error.message,
                activeWorkers.length
                  ? [
                      "",
                      `<active-workers count="${activeWorkers.length}">`,
                      ...activeWorkers.map(formatWorker),
                      "</active-workers>",
                    ].join("\n")
                  : "",
              ]
                .filter((line) => line !== "")
                .join("\n"),
            }
          }

          const team = result.team
          if (!team) {
            const error = `No subagent team found for: ${args.team_name}`
            const metadata: DeleteTeamMetadata = {
              teamName: args.team_name,
              error,
              activeWorkers: [],
            }
            return {
              title: "Team not found",
              metadata,
              output: error,
            }
          }
          const metadata: DeleteTeamMetadata = {
            teamName: args.team_name,
            activeWorkers: [],
            team,
          }
          return {
            title: "Team deleted",
            metadata,
            output: ["Team deleted.", formatTeam(team)].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const CreateTaskTool = Tool.define(
  "create_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const plugin = yield* Plugin.Service

    return {
      description: CREATE_TASK_DESCRIPTION,
      parameters: CreateTaskParameters,
      execute: (args: Schema.Schema.Type<typeof CreateTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const worker = yield* swarm.getBySession(ctx.sessionID)
          const board = yield* resolveTaskBoard(swarm, ctx, args.team)
          const task = yield* swarm.createTask({
            ...board,
            subject: args.subject,
            description: args.description,
            activeForm: args.active_form,
            owner: args.owner,
            metadata: args.metadata,
          })
          const output: { allow: boolean; message?: string } = { allow: true }
          yield* plugin.trigger(
            "swarm.task.created",
            {
              taskID: task.id,
              taskSubject: task.subject,
              taskDescription: task.description,
              ...(worker ? { teammateName: worker.spec.name ?? worker.spec.workerID } : {}),
              ...(board.team ? { teamName: board.team } : {}),
            },
            output,
          )
          if (output.allow === false) {
            yield* swarm.updateTeamTask({
              ...board,
              taskID: task.id,
              status: "deleted",
            })
            const message = output.message?.trim() || "TaskCreated hook blocked task creation."
            return {
              title: "Task creation blocked",
              metadata: {
                task,
                result: {
                  success: false,
                  taskID: task.id,
                  updatedFields: [] as string[],
                  error: message,
                },
              },
              output: message,
            }
          }
          return {
            title: "Task created",
            metadata: {
              task,
              result: {
                success: true,
                taskID: task.id,
                updatedFields: ["created"],
                error: "",
              },
            },
            output: `Task #${task.id} created successfully: ${task.subject}`,
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const UpdateTaskTool = Tool.define(
  "update_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const plugin = yield* Plugin.Service

    return {
      description: UPDATE_TASK_DESCRIPTION,
      parameters: UpdateTaskParameters,
      execute: (args: Schema.Schema.Type<typeof UpdateTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const worker = yield* swarm.getBySession(ctx.sessionID)
          const board = yield* resolveTaskBoard(swarm, ctx, args.team)
          const existing = yield* swarm.getTeamTask({ ...board, taskID: args.task_id })
          if (args.status === "completed" && existing && existing.status !== "completed") {
            const output: { allow: boolean; message?: string } = { allow: true }
            yield* plugin.trigger(
              "swarm.task.completed",
              {
                taskID: existing.id,
                taskSubject: existing.subject,
                taskDescription: existing.description,
                ...(worker ? { teammateName: worker.spec.name ?? worker.spec.workerID } : {}),
                ...(board.team ? { teamName: board.team } : {}),
              },
              output,
            )
            if (output.allow === false) {
              const message = output.message?.trim() || "TaskCompleted hook blocked completion."
              return {
                title: "Task completion blocked",
                metadata: {
                  result: {
                    success: false,
                    taskID: args.task_id,
                    updatedFields: [],
                    error: message,
                  },
                },
                output: message,
              }
            }
          }
          const owner =
            args.owner ??
            (worker && args.status === "in_progress" && existing && !existing.owner
              ? (worker.spec.name ?? worker.spec.workerID)
              : undefined)
          const result = yield* swarm.updateTeamTask({
            ...board,
            taskID: args.task_id,
            subject: args.subject,
            description: args.description,
            activeForm: args.active_form,
            status: args.status,
            owner,
            addBlocks: args.add_blocks,
            addBlockedBy: args.add_blocked_by,
            metadata: args.metadata,
          })
          return {
            title: result.success ? "Task updated" : "Task not found",
            metadata: { result },
            output: result.success
              ? result.deleted
                ? `Deleted task #${result.taskID}`
                : [
                    `Updated task #${result.taskID} ${result.updatedFields.join(", ") || "(no changes)"}`,
                    result.statusChange?.to === "completed"
                      ? "Task completed. Call list_team_tasks now to find newly unblocked work or confirm the team is done."
                      : "",
                  ]
                    .filter((line) => line !== "")
                    .join("\n\n")
              : (result.error ?? `Task #${result.taskID} not found`),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const GetTaskTool = Tool.define(
  "get_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: GET_TASK_DESCRIPTION,
      parameters: GetTaskParameters,
      execute: (args: Schema.Schema.Type<typeof GetTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const board = yield* resolveTaskBoard(swarm, ctx, args.team)
          const task = yield* swarm.getTeamTask({ ...board, taskID: args.task_id })
          return {
            title: task ? task.subject : "Task not found",
            metadata: { task },
            output: task ? formatTeamTask(task, { detail: true }) : "Task not found",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ListTeamTasksTool = Tool.define(
  "list_team_tasks",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: LIST_TEAM_TASKS_DESCRIPTION,
      parameters: ListTeamTasksParameters,
      execute: (args: Schema.Schema.Type<typeof ListTeamTasksParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const board = yield* resolveTaskBoard(swarm, ctx, args.team)
          const tasks = (yield* swarm.listTeamTasks(board)).filter(
            (task) => (!args.status || task.status === args.status) && (!args.owner || task.owner === args.owner),
          )
          return {
            title: "Team tasks",
            metadata: { count: tasks.length, tasks },
            output: tasks.length
              ? [`<team_tasks count="${tasks.length}">`, ...tasks.map((task) => formatTeamTask(task)), "</team_tasks>"]
                  .join("\n")
              : "No team tasks found.",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const BroadcastTool = Tool.define(
  "broadcast",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: BROADCAST_DESCRIPTION,
      parameters: BroadcastParameters,
      execute: (args: Schema.Schema.Type<typeof BroadcastParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const inputs = yield* swarm.broadcast({
            parentSessionID: ctx.sessionID,
            team: args.team,
            message: args.message,
            summary: args.summary,
            from: currentWorker ? (currentWorker.spec.name ?? ctx.agent) : "team-lead",
          })

          return {
            title: "Broadcast sent",
            metadata: {
              team: args.team,
              count: inputs.length,
              inputIds: inputs.map((input) => input.id),
            },
            output: [
              `Broadcast sent to team: ${args.team}`,
              `recipients: ${inputs.length}`,
              ...inputs.map((input) => `input_id: ${input.id}`),
              args.summary ? `summary: ${args.summary}` : "",
            ]
              .filter((line) => line !== "")
              .join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const WaitTaskTool = Tool.define(
  "wait_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: WAIT_DESCRIPTION,
      parameters: WaitTaskParameters,
      execute: (args: Schema.Schema.Type<typeof WaitTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const targets = [...(args.task_id ? [args.task_id] : []), ...(args.task_ids ?? [])]
          if (targets.length === 0) return yield* Effect.fail(new Error("wait_task requires task_id or task_ids"))

          const tasks = yield* Effect.forEach(
            targets,
            (target) =>
              swarm.wait({
                parentSessionID: ctx.sessionID,
                to: target,
                timeoutMS: args.timeout_ms,
              }),
            { concurrency: "unbounded" },
          )

          return {
            title: "Subagent wait",
            metadata: {
              count: tasks.length,
              tasks,
            },
            output: [`<tasks count="${tasks.length}">`, ...tasks.map(formatWorker), "</tasks>"].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const CancelTaskTool = Tool.define(
  "cancel_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const sessions = yield* Session.Service

    return {
      description: CANCEL_DESCRIPTION,
      parameters: CancelTaskParameters,
      execute: (args: Schema.Schema.Type<typeof CancelTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const task = yield* swarm.resolve({ parentSessionID: ctx.sessionID, to: args.task_id })
          if (!task) return yield* Effect.fail(new Error(`No subagent found for: ${args.task_id}`))
          yield* swarm.cancel(task.spec.workerID)
          const cancelled = (yield* swarm.get(task.spec.workerID)) ?? task
          yield* refreshTranscript(sessions, cancelled)

          return {
            title: "Subagent cancelled",
            metadata: {
              task: cancelled,
            },
            output: ["Subagent cancelled.", formatWorker(cancelled)].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ControlTaskPaneTool = Tool.define(
  "control_task_pane",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    return {
      description: CONTROL_PANE_DESCRIPTION,
      parameters: ControlTaskPaneParameters,
      execute: (args: Schema.Schema.Type<typeof ControlTaskPaneParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const task = yield* swarm.resolve({ parentSessionID: ctx.sessionID, to: args.task_id })
          if (!task) return yield* Effect.fail(new Error(`No subagent found for: ${args.task_id}`))
          const updated = yield* swarm.controlPane(task.spec.workerID, args.action)

          return {
            title: args.action === "hide" ? "Subagent pane hidden" : "Subagent pane shown",
            metadata: {
              task: updated,
              action: args.action,
            },
            output: [`Subagent pane ${args.action === "hide" ? "hidden" : "shown"}.`, formatWorker(updated)].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const StopTaskTool = Tool.define(
  "stop_task",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const sessions = yield* Session.Service

    return {
      description: STOP_DESCRIPTION,
      parameters: StopTaskParameters,
      execute: (args: Schema.Schema.Type<typeof StopTaskParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const parentSessionID = currentWorker?.spec.parentSessionID ?? ctx.sessionID

          if (!args.task_id && currentWorker) {
            yield* swarm.stopAfterCurrentTurn(currentWorker.spec.workerID, args.reason)
            const stopped = (yield* swarm.get(currentWorker.spec.workerID)) ?? currentWorker
            return {
              title: "Subagent stopped",
              metadata: {
                task: stopped,
                self: true,
              },
              output: ["Subagent will stop after the current turn.", formatWorker(stopped)].join("\n"),
            }
          }

          if (!args.task_id) return yield* Effect.fail(new Error("stop_task requires task_id outside a background subagent"))

          const task = yield* swarm.resolve({ parentSessionID, to: args.task_id })
          if (!task) return yield* Effect.fail(new Error(`No subagent found for: ${args.task_id}`))

          if (currentWorker?.spec.workerID === task.spec.workerID) {
            yield* swarm.stopAfterCurrentTurn(task.spec.workerID, args.reason)
          } else {
            yield* swarm.cancel(task.spec.workerID)
          }
          const stopped = (yield* swarm.get(task.spec.workerID)) ?? task
          yield* refreshTranscript(sessions, stopped)

          return {
            title: "Subagent stopped",
            metadata: {
              task: stopped,
              self: currentWorker?.spec.workerID === task.spec.workerID,
            },
            output: ["Subagent stopped.", formatWorker(stopped)].join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ReadTaskOutputTool = Tool.define(
  "read_task_output",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const sessions = yield* Session.Service

    return {
      description: READ_DESCRIPTION,
      parameters: ReadTaskOutputParameters,
      execute: (args: Schema.Schema.Type<typeof ReadTaskOutputParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const task =
            args.block === true
              ? yield* swarm.wait({
                  parentSessionID: ctx.sessionID,
                  to: args.task_id,
                  timeoutMS: args.timeout_ms,
                })
              : yield* swarm.resolve({ parentSessionID: ctx.sessionID, to: args.task_id })
          if (!task) return yield* Effect.fail(new Error(`No subagent found for: ${args.task_id}`))

          const transcript = args.include_transcript
            ? yield* sessions.messages({ sessionID: task.spec.sessionID, limit: args.limit })
            : []
          const renderedTranscript = transcript.map(renderMessage).filter(Boolean)

          return {
            title: task.spec.description,
            metadata: {
              task,
              ...(args.include_transcript ? { transcriptMessages: renderedTranscript.length } : {}),
            },
            output: [
              formatWorker(task),
              args.include_transcript
                ? [
                    "",
                    `<transcript message_count="${renderedTranscript.length}">`,
                    ...renderedTranscript,
                    "</transcript>",
                  ].join("\n")
                : "",
            ]
              .filter((line) => line !== "")
              .join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

const formatWorker = (worker: WorkerSnapshot) => {
  const spec = worker.spec
  return [
    "<task>",
    `<task-id>${xmlEscape(spec.sessionID)}</task-id>`,
    `<worker-id>${xmlEscape(spec.workerID)}</worker-id>`,
    spec.name ? `<name>${xmlEscape(spec.name)}</name>` : "",
    spec.team ? `<team>${xmlEscape(spec.team)}</team>` : "",
    `<description>${xmlEscape(spec.description)}</description>`,
    `<agent>${xmlEscape(spec.agent)}</agent>`,
    spec.outputPath ? `<output-path>${xmlEscape(spec.outputPath)}</output-path>` : "",
    `<status>${xmlEscape(worker.status)}</status>`,
    `<execution>${xmlEscape(spec.executionStrategy)}</execution>`,
    `<backend>${xmlEscape(spec.backend)}</backend>`,
    spec.paneID ? `<pane-id>${xmlEscape(spec.paneID)}</pane-id>` : "",
    spec.paneWindowTarget ? `<pane-window-target>${xmlEscape(spec.paneWindowTarget)}</pane-window-target>` : "",
    worker.paneHidden !== undefined ? `<pane-hidden>${worker.paneHidden}</pane-hidden>` : "",
    spec.worktreePath ? `<worktree-path>${xmlEscape(spec.worktreePath)}</worktree-path>` : "",
    spec.worktreeBranch ? `<worktree-branch>${xmlEscape(spec.worktreeBranch)}</worktree-branch>` : "",
    spec.remoteEndpoint ? `<remote-endpoint>${xmlEscape(spec.remoteEndpoint)}</remote-endpoint>` : "",
    spec.remoteID ? `<remote-id>${xmlEscape(spec.remoteID)}</remote-id>` : "",
    spec.remoteSessionURL ? `<remote-session-url>${xmlEscape(spec.remoteSessionURL)}</remote-session-url>` : "",
    spec.remoteOutputPath ? `<remote-output-path>${xmlEscape(spec.remoteOutputPath)}</remote-output-path>` : "",
    worker.remoteCursor ? `<remote-cursor>${xmlEscape(worker.remoteCursor)}</remote-cursor>` : "",
    worker.currentTool
      ? `<current-tool>${xmlEscape(worker.currentTool.title ?? worker.currentTool.name)}</current-tool>`
      : "",
    worker.pendingPermissionID ? `<pending-permission>${xmlEscape(worker.pendingPermissionID)}</pending-permission>` : "",
    worker.pendingShutdownID ? `<pending-shutdown>${xmlEscape(worker.pendingShutdownID)}</pending-shutdown>` : "",
    worker.pendingPlanApprovalID
      ? `<pending-plan-approval>${xmlEscape(worker.pendingPlanApprovalID)}</pending-plan-approval>`
      : "",
    worker.mailboxSize ? `<mailbox-size>${worker.mailboxSize}</mailbox-size>` : "",
    worker.lastProgress ? `<progress>${xmlEscape(worker.lastProgress)}</progress>` : "",
    worker.result?.text ? `<result>${xmlEscape(worker.result.text)}</result>` : "",
    worker.result?.error ? `<error>${xmlEscape(worker.result.error)}</error>` : "",
    "</task>",
  ]
    .filter((line) => line !== "")
    .join("\n")
}

const terminalWorkerStatus = new Set<WorkerStatus>(["completed", "cancelled", "failed", "interrupted"])
const isActiveWorker = (worker: WorkerSnapshot) => !terminalWorkerStatus.has(worker.status)

const formatTeam = (team: TeamSnapshot) =>
  [
    "<team>",
    `<name>${xmlEscape(team.name)}</name>`,
    team.description ? `<description>${xmlEscape(team.description)}</description>` : "",
    team.agentType ? `<agent-type>${xmlEscape(team.agentType)}</agent-type>` : "",
    team.leadSessionID ? `<lead-session-id>${xmlEscape(team.leadSessionID)}</lead-session-id>` : "",
    `<worker-count>${team.workerIDs.length}</worker-count>`,
    ...team.workerIDs.map((workerID) => `<worker-id>${xmlEscape(workerID)}</worker-id>`),
    "</team>",
  ]
    .filter((line) => line !== "")
    .join("\n")

const formatTeamTask = (task: TeamTaskState, options: { detail?: boolean } = {}) =>
  [
    "<team-task>",
    `<id>${xmlEscape(task.id)}</id>`,
    task.team ? `<team>${xmlEscape(task.team)}</team>` : "",
    `<subject>${xmlEscape(task.subject)}</subject>`,
    `<status>${xmlEscape(task.status)}</status>`,
    task.owner ? `<owner>${xmlEscape(task.owner)}</owner>` : "",
    task.blockedBy.length
      ? `<blocked-by>${task.blockedBy.map((id) => `#${xmlEscape(id)}`).join(", ")}</blocked-by>`
      : "",
    task.blocks.length ? `<blocks>${task.blocks.map((id) => `#${xmlEscape(id)}`).join(", ")}</blocks>` : "",
    options.detail ? `<description>${xmlEscape(task.description)}</description>` : "",
    "</team-task>",
  ]
    .filter((line) => line !== "")
    .join("\n")

const renderMessage = (message: MessageV2.WithParts) => {
  const body = message.parts.map(renderPart).filter(Boolean).join("\n")
  if (!body.trim()) return ""
  return [
    `<message role="${xmlEscape(message.info.role)}" id="${xmlEscape(message.info.id)}">`,
    xmlEscape(body),
    "</message>",
  ].join("\n")
}

const renderPart = (part: MessageV2.Part) => {
  switch (part.type) {
    case "text":
    case "reasoning":
      return part.text
    case "tool":
      if (part.state.status === "completed") return `[tool:${part.tool}] ${part.state.output}`
      if (part.state.status === "error") return `[tool:${part.tool} error] ${part.state.error}`
      return `[tool:${part.tool} ${part.state.status}]`
    case "file":
      return `[file] ${part.filename ?? part.url}`
    case "agent":
      return `[agent] ${part.name}`
    case "patch":
      return `[patch] ${part.files.join(", ")}`
    case "subtask":
      return `[subtask:${part.agent}] ${part.description}`
    default:
      return ""
  }
}

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export * as TaskControl from "./task_control"
