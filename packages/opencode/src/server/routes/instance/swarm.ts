import { Hono } from "hono"
import { validator } from "hono-openapi"
import { SessionID } from "@/session/schema"
import { SwarmRuntime } from "@/swarm/runtime"
import z from "zod"
import { Effect } from "effect"
import { jsonRequest } from "./trace"
import { lazy } from "@/util/lazy"

const taskStatus = z.enum(["pending", "in_progress", "completed"])
const taskQuery = z.object({
  team: z.string().optional(),
  status: taskStatus.optional(),
  owner: z.string().optional(),
})
const taskPayload = z.object({
  team: z.string().optional(),
  subject: z.string(),
  description: z.string(),
  active_form: z.string().optional(),
  owner: z.string().optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
})
const updateTaskPayload = z.object({
  team: z.string().optional(),
  subject: z.string().optional(),
  description: z.string().optional(),
  active_form: z.string().optional(),
  status: z.union([taskStatus, z.literal("deleted")]).optional(),
  owner: z.string().optional(),
  add_blocks: z.array(z.string()).optional(),
  add_blocked_by: z.array(z.string()).optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
})

export const SwarmRoutes = lazy(() =>
  new Hono()
    .get(
      "/:sessionID/worker",
      validator("param", z.object({ sessionID: SessionID.zod })),
      async (c) =>
        jsonRequest("SwarmRoutes.workers", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          return yield* swarm.list(c.req.valid("param").sessionID)
        }),
    )
    .get(
      "/:sessionID/team",
      validator("param", z.object({ sessionID: SessionID.zod })),
      async (c) =>
        jsonRequest("SwarmRoutes.teams", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          return yield* swarm.listTeams(c.req.valid("param").sessionID)
        }),
    )
    .post(
      "/:sessionID/worker/:target/message",
      validator("param", z.object({ sessionID: SessionID.zod, target: z.string() })),
      validator("json", z.object({ message: z.string(), summary: z.string().optional() })),
      async (c) =>
        jsonRequest("SwarmRoutes.workerMessage", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const body = c.req.valid("json")
          const input = yield* swarm.sendInput({
            parentSessionID: params.sessionID,
            to: params.target,
            message: body.message,
            summary: body.summary,
            from: "api",
          })
          return { inputID: input.id }
        }),
    )
    .post(
      "/:sessionID/worker/:target/cancel",
      validator("param", z.object({ sessionID: SessionID.zod, target: z.string() })),
      async (c) =>
        jsonRequest("SwarmRoutes.workerCancel", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const worker = yield* swarm.resolve({ parentSessionID: params.sessionID, to: params.target })
          if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${params.target}`))
          yield* swarm.cancel(worker.spec.workerID)
          return (yield* swarm.get(worker.spec.workerID)) ?? worker
        }),
    )
    .post(
      "/:sessionID/worker/:target/stop",
      validator("param", z.object({ sessionID: SessionID.zod, target: z.string() })),
      async (c) =>
        jsonRequest("SwarmRoutes.workerStop", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const worker = yield* swarm.resolve({ parentSessionID: params.sessionID, to: params.target })
          if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${params.target}`))
          yield* swarm.stopAfterCurrentTurn(worker.spec.workerID, "stopped through API")
          return (yield* swarm.get(worker.spec.workerID)) ?? worker
        }),
    )
    .post(
      "/:sessionID/team",
      validator("param", z.object({ sessionID: SessionID.zod })),
      validator(
        "json",
        z.object({
          team_name: z.string(),
          description: z.string().optional(),
          agent_type: z.string().optional(),
        }),
      ),
      async (c) =>
        jsonRequest("SwarmRoutes.teamCreate", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const sessionID = c.req.valid("param").sessionID
          const body = c.req.valid("json")
          return yield* swarm.createTeam({
            parentSessionID: sessionID,
            name: body.team_name,
            description: body.description,
            leadSessionID: sessionID,
            agentType: body.agent_type,
          })
        }),
    )
    .delete(
      "/:sessionID/team/:teamName",
      validator("param", z.object({ sessionID: SessionID.zod, teamName: z.string() })),
      validator("query", z.object({ cancel_workers: z.enum(["true", "false"]).optional() })),
      async (c) =>
        jsonRequest("SwarmRoutes.teamDelete", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const query = c.req.valid("query")
          const team = yield* swarm.deleteTeam({
            parentSessionID: params.sessionID,
            name: params.teamName,
            cancelWorkers: query.cancel_workers === undefined ? undefined : query.cancel_workers === "true",
          })
          if (!team) return yield* Effect.fail(new Error(`No subagent team found for: ${params.teamName}`))
          return team
        }),
    )
    .post(
      "/:sessionID/team/:teamName/broadcast",
      validator("param", z.object({ sessionID: SessionID.zod, teamName: z.string() })),
      validator("json", z.object({ message: z.string(), summary: z.string().optional() })),
      async (c) =>
        jsonRequest("SwarmRoutes.teamBroadcast", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const body = c.req.valid("json")
          const inputs = yield* swarm.broadcast({
            parentSessionID: params.sessionID,
            team: params.teamName,
            message: body.message,
            summary: body.summary,
            from: "api",
          })
          return { inputIDs: inputs.map((input) => input.id) }
        }),
    )
    .get(
      "/:sessionID/task",
      validator("param", z.object({ sessionID: SessionID.zod })),
      validator("query", taskQuery),
      async (c) =>
        jsonRequest("SwarmRoutes.taskList", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const sessionID = c.req.valid("param").sessionID
          const query = c.req.valid("query")
          const tasks = yield* swarm.listTeamTasks({ parentSessionID: sessionID, team: query.team })
          return tasks.filter(
            (task) => (!query.status || task.status === query.status) && (!query.owner || task.owner === query.owner),
          )
        }),
    )
    .post(
      "/:sessionID/task",
      validator("param", z.object({ sessionID: SessionID.zod })),
      validator("json", taskPayload),
      async (c) =>
        jsonRequest("SwarmRoutes.taskCreate", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const sessionID = c.req.valid("param").sessionID
          const body = c.req.valid("json")
          return yield* swarm.createTask({
            parentSessionID: sessionID,
            team: body.team,
            subject: body.subject,
            description: body.description,
            activeForm: body.active_form,
            owner: body.owner,
            metadata: body.metadata,
          })
        }),
    )
    .get(
      "/:sessionID/task/:taskID",
      validator("param", z.object({ sessionID: SessionID.zod, taskID: z.string() })),
      validator("query", taskQuery),
      async (c) =>
        jsonRequest("SwarmRoutes.taskGet", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const query = c.req.valid("query")
          const task = yield* swarm.getTeamTask({
            parentSessionID: params.sessionID,
            team: query.team,
            taskID: params.taskID,
          })
          if (!task) return yield* Effect.fail(new Error(`No shared subagent task found for: ${params.taskID}`))
          return task
        }),
    )
    .patch(
      "/:sessionID/task/:taskID",
      validator("param", z.object({ sessionID: SessionID.zod, taskID: z.string() })),
      validator("json", updateTaskPayload),
      async (c) =>
        jsonRequest("SwarmRoutes.taskUpdate", c, function* () {
          const swarm = yield* SwarmRuntime.Service
          const params = c.req.valid("param")
          const body = c.req.valid("json")
          return yield* swarm.updateTeamTask({
            parentSessionID: params.sessionID,
            team: body.team,
            taskID: params.taskID,
            subject: body.subject,
            description: body.description,
            activeForm: body.active_form,
            status: body.status,
            owner: body.owner,
            addBlocks: body.add_blocks,
            addBlockedBy: body.add_blocked_by,
            metadata: body.metadata,
          })
        }),
    ),
)
