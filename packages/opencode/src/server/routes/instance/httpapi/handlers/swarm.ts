import { SessionID } from "@/session/schema"
import { SwarmRuntime } from "@/swarm/runtime"
import { Effect } from "effect"
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi"
import { InstanceHttpApi } from "../api"
import {
  BroadcastPayload,
  CreateTaskPayload,
  CreateTeamPayload,
  DeleteTeamQuery,
  MessagePayload,
  TaskQuery,
  TaskTarget,
  TeamTarget,
  UpdateTaskPayload,
  WorkerPaneTarget,
  WorkerTarget,
} from "../groups/swarm"
import * as ApiError from "../errors"

export const swarmHandlers = HttpApiBuilder.group(InstanceHttpApi, "swarm", (handlers) =>
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service

    const workers = Effect.fn("SwarmHttpApi.workers")(function* (ctx: { params: { sessionID: SessionID } }) {
      return yield* swarm.list(ctx.params.sessionID)
    })

    const teams = Effect.fn("SwarmHttpApi.teams")(function* (ctx: { params: { sessionID: SessionID } }) {
      return yield* swarm.listTeams(ctx.params.sessionID)
    })

    const workerMessage = Effect.fn("SwarmHttpApi.workerMessage")(function* (ctx: {
      params: typeof WorkerTarget.Type
      payload: typeof MessagePayload.Type
    }) {
      const input = yield* swarm
        .sendInput({
          parentSessionID: ctx.params.sessionID,
          to: ctx.params.target,
          message: ctx.payload.message,
          summary: ctx.payload.summary,
          from: "api",
        })
        .pipe(Effect.mapError((error) => ApiError.notFound(error.message)))
      return { inputID: input.id }
    })

    const workerCancel = Effect.fn("SwarmHttpApi.workerCancel")(function* (ctx: { params: typeof WorkerTarget.Type }) {
      const worker = yield* swarm.resolve({ parentSessionID: ctx.params.sessionID, to: ctx.params.target })
      if (!worker) return yield* Effect.fail(ApiError.notFound(`No subagent found for: ${ctx.params.target}`))
      yield* swarm.cancel(worker.spec.workerID)
      return (yield* swarm.get(worker.spec.workerID)) ?? worker
    })

    const workerStop = Effect.fn("SwarmHttpApi.workerStop")(function* (ctx: { params: typeof WorkerTarget.Type }) {
      const worker = yield* swarm.resolve({ parentSessionID: ctx.params.sessionID, to: ctx.params.target })
      if (!worker) return yield* Effect.fail(ApiError.notFound(`No subagent found for: ${ctx.params.target}`))
      yield* swarm.stopAfterCurrentTurn(worker.spec.workerID, "stopped through API")
      return (yield* swarm.get(worker.spec.workerID)) ?? worker
    })

    const workerPane = Effect.fn("SwarmHttpApi.workerPane")(function* (ctx: { params: typeof WorkerPaneTarget.Type }) {
      const worker = yield* swarm.resolve({ parentSessionID: ctx.params.sessionID, to: ctx.params.target })
      if (!worker) return yield* Effect.fail(ApiError.notFound(`No subagent found for: ${ctx.params.target}`))
      return yield* swarm
        .controlPane(worker.spec.workerID, ctx.params.action)
        .pipe(Effect.mapError((error) => ApiError.notFound(error.message)))
    })

    const teamCreate = Effect.fn("SwarmHttpApi.teamCreate")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof CreateTeamPayload.Type
    }) {
      return yield* swarm.createTeam({
        parentSessionID: ctx.params.sessionID,
        name: ctx.payload.team_name,
        description: ctx.payload.description,
        leadSessionID: ctx.params.sessionID,
        agentType: ctx.payload.agent_type,
      })
    })

    const teamDelete = Effect.fn("SwarmHttpApi.teamDelete")(function* (ctx: {
      params: typeof TeamTarget.Type
      query: typeof DeleteTeamQuery.Type
    }) {
      const team = yield* swarm
        .deleteTeam({
          parentSessionID: ctx.params.sessionID,
          name: ctx.params.teamName,
          cancelWorkers: ctx.query.cancel_workers,
        })
        .pipe(Effect.mapError(() => new HttpApiError.BadRequest({})))
      if (!team) return yield* Effect.fail(ApiError.notFound(`No subagent team found for: ${ctx.params.teamName}`))
      return team
    })

    const teamBroadcast = Effect.fn("SwarmHttpApi.teamBroadcast")(function* (ctx: {
      params: typeof TeamTarget.Type
      payload: typeof BroadcastPayload.Type
    }) {
      const inputs = yield* swarm
        .broadcast({
          parentSessionID: ctx.params.sessionID,
          team: ctx.params.teamName,
          message: ctx.payload.message,
          summary: ctx.payload.summary,
          from: "api",
        })
        .pipe(Effect.mapError((error) => ApiError.notFound(error.message)))
      return { inputIDs: inputs.map((input) => input.id) }
    })

    const taskList = Effect.fn("SwarmHttpApi.taskList")(function* (ctx: {
      params: { sessionID: SessionID }
      query: typeof TaskQuery.Type
    }) {
      const tasks = yield* swarm.listTeamTasks({
        parentSessionID: ctx.params.sessionID,
        team: ctx.query.team,
      })
      return tasks.filter(
        (task) => (!ctx.query.status || task.status === ctx.query.status) && (!ctx.query.owner || task.owner === ctx.query.owner),
      )
    })

    const taskCreate = Effect.fn("SwarmHttpApi.taskCreate")(function* (ctx: {
      params: { sessionID: SessionID }
      payload: typeof CreateTaskPayload.Type
    }) {
      return yield* swarm.createTask({
        parentSessionID: ctx.params.sessionID,
        team: ctx.payload.team,
        subject: ctx.payload.subject,
        description: ctx.payload.description,
        activeForm: ctx.payload.active_form,
        owner: ctx.payload.owner,
        metadata: ctx.payload.metadata,
      })
    })

    const taskGet = Effect.fn("SwarmHttpApi.taskGet")(function* (ctx: {
      params: typeof TaskTarget.Type
      query: typeof TaskQuery.Type
    }) {
      const task = yield* swarm.getTeamTask({
        parentSessionID: ctx.params.sessionID,
        team: ctx.query.team,
        taskID: ctx.params.taskID,
      })
      if (!task) return yield* Effect.fail(ApiError.notFound(`No shared subagent task found for: ${ctx.params.taskID}`))
      return task
    })

    const taskUpdate = Effect.fn("SwarmHttpApi.taskUpdate")(function* (ctx: {
      params: typeof TaskTarget.Type
      payload: typeof UpdateTaskPayload.Type
    }) {
      return yield* swarm.updateTeamTask({
        parentSessionID: ctx.params.sessionID,
        team: ctx.payload.team,
        taskID: ctx.params.taskID,
        subject: ctx.payload.subject,
        description: ctx.payload.description,
        activeForm: ctx.payload.active_form,
        status: ctx.payload.status,
        owner: ctx.payload.owner,
        addBlocks: ctx.payload.add_blocks,
        addBlockedBy: ctx.payload.add_blocked_by,
        metadata: ctx.payload.metadata,
      })
    })

    return handlers
      .handle("workers", workers)
      .handle("teams", teams)
      .handle("workerMessage", workerMessage)
      .handle("workerCancel", workerCancel)
      .handle("workerStop", workerStop)
      .handle("workerPane", workerPane)
      .handle("teamCreate", teamCreate)
      .handle("teamDelete", teamDelete)
      .handle("teamBroadcast", teamBroadcast)
      .handle("taskList", taskList)
      .handle("taskCreate", taskCreate)
      .handle("taskGet", taskGet)
      .handle("taskUpdate", taskUpdate)
  }),
)
