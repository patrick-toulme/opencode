import { SessionID } from "@/session/schema"
import { TeamSnapshot, TeamTaskState, TeamTaskStatus, WorkerState } from "@/swarm/state"
import { Schema, SchemaGetter } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiError, HttpApiGroup, OpenApi } from "effect/unstable/httpapi"
import { Authorization } from "../middleware/authorization"
import { ApiNotFoundError } from "../errors"
import { InstanceContextMiddleware } from "../middleware/instance-context"
import { WorkspaceRoutingMiddleware } from "../middleware/workspace-routing"
import { described } from "./metadata"

const root = "/swarm"

export const WorkerTarget = Schema.Struct({
  sessionID: SessionID,
  target: Schema.String,
})

export const WorkerPaneTarget = Schema.Struct({
  sessionID: SessionID,
  target: Schema.String,
  action: Schema.Literals(["hide", "show"]),
})

export const TeamTarget = Schema.Struct({
  sessionID: SessionID,
  teamName: Schema.String,
})

export const MessagePayload = Schema.Struct({
  message: Schema.String,
  summary: Schema.optional(Schema.String),
})

export const BroadcastPayload = Schema.Struct({
  message: Schema.String,
  summary: Schema.optional(Schema.String),
})

export const CreateTeamPayload = Schema.Struct({
  team_name: Schema.String,
  description: Schema.optional(Schema.String),
  agent_type: Schema.optional(Schema.String),
})

export const TaskQuery = Schema.Struct({
  team: Schema.optional(Schema.String),
  status: Schema.optional(TeamTaskStatus),
  owner: Schema.optional(Schema.String),
})

export const TaskTarget = Schema.Struct({
  sessionID: SessionID,
  taskID: Schema.String,
})

const Metadata = Schema.Record(Schema.String, Schema.Unknown)

export const CreateTaskPayload = Schema.Struct({
  team: Schema.optional(Schema.String),
  subject: Schema.String,
  description: Schema.String,
  active_form: Schema.optional(Schema.String),
  owner: Schema.optional(Schema.String),
  metadata: Schema.optional(Metadata),
})

export const UpdateTaskPayload = Schema.Struct({
  team: Schema.optional(Schema.String),
  subject: Schema.optional(Schema.String),
  description: Schema.optional(Schema.String),
  active_form: Schema.optional(Schema.String),
  status: Schema.optional(Schema.Union([TeamTaskStatus, Schema.Literal("deleted")])),
  owner: Schema.optional(Schema.String),
  add_blocks: Schema.optional(Schema.Array(Schema.String)),
  add_blocked_by: Schema.optional(Schema.Array(Schema.String)),
  metadata: Schema.optional(Metadata),
})

export const UpdateTaskResponse = Schema.Struct({
  success: Schema.Boolean,
  taskID: Schema.String,
  updatedFields: Schema.Array(Schema.String),
  task: Schema.optional(TeamTaskState),
  deleted: Schema.optional(TeamTaskState),
  error: Schema.optional(Schema.String),
  statusChange: Schema.optional(
    Schema.Struct({
      from: TeamTaskStatus,
      to: Schema.Union([TeamTaskStatus, Schema.Literal("deleted")]),
    }),
  ),
})

export const DeleteTeamQuery = Schema.Struct({
  cancel_workers: Schema.optional(
    Schema.Literals(["true", "false"]).pipe(
      Schema.decodeTo(Schema.Boolean, {
        decode: SchemaGetter.transform((value) => value === "true"),
        encode: SchemaGetter.transform((value) => (value ? "true" : "false")),
      }),
    ),
  ),
})

export const SwarmPaths = {
  workers: `${root}/:sessionID/worker`,
  teams: `${root}/:sessionID/team`,
  workerMessage: `${root}/:sessionID/worker/:target/message`,
  workerCancel: `${root}/:sessionID/worker/:target/cancel`,
  workerStop: `${root}/:sessionID/worker/:target/stop`,
  workerPane: `${root}/:sessionID/worker/:target/pane/:action`,
  teamCreate: `${root}/:sessionID/team`,
  teamDelete: `${root}/:sessionID/team/:teamName`,
  teamBroadcast: `${root}/:sessionID/team/:teamName/broadcast`,
  taskList: `${root}/:sessionID/task`,
  taskCreate: `${root}/:sessionID/task`,
  taskGet: `${root}/:sessionID/task/:taskID`,
  taskUpdate: `${root}/:sessionID/task/:taskID`,
} as const

export const SwarmApi = HttpApi.make("swarm").add(
  HttpApiGroup.make("swarm")
    .add(
      HttpApiEndpoint.get("workers", SwarmPaths.workers, {
        params: { sessionID: SessionID },
        success: described(Schema.Array(WorkerState), "Subagent workers"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.workers",
          summary: "List subagent workers",
          description: "List subagent workers spawned by a parent session.",
        }),
      ),
      HttpApiEndpoint.get("teams", SwarmPaths.teams, {
        params: { sessionID: SessionID },
        success: described(Schema.Array(TeamSnapshot), "Subagent teams"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.teams",
          summary: "List subagent teams",
          description: "List subagent teams for a parent session.",
        }),
      ),
      HttpApiEndpoint.post("workerMessage", SwarmPaths.workerMessage, {
        params: WorkerTarget,
        payload: MessagePayload,
        success: described(Schema.Struct({ inputID: Schema.String }), "Queued subagent message"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.worker.message",
          summary: "Message subagent worker",
          description: "Queue a follow-up message for a running or idle subagent worker.",
        }),
      ),
      HttpApiEndpoint.post("workerCancel", SwarmPaths.workerCancel, {
        params: WorkerTarget,
        success: described(WorkerState, "Cancelled subagent worker"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.worker.cancel",
          summary: "Cancel subagent worker",
          description: "Cancel a subagent worker by worker id, child session id, or name.",
        }),
      ),
      HttpApiEndpoint.post("workerStop", SwarmPaths.workerStop, {
        params: WorkerTarget,
        success: described(WorkerState, "Stopped subagent worker"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.worker.stop",
          summary: "Stop subagent worker",
          description: "Stop a subagent worker by worker id, child session id, or name.",
        }),
      ),
      HttpApiEndpoint.post("workerPane", SwarmPaths.workerPane, {
        params: WorkerPaneTarget,
        success: described(WorkerState, "Updated subagent worker pane"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.worker.pane",
          summary: "Hide or show subagent worker pane",
          description: "Hide or show a pane-backed subagent worker without stopping it.",
        }),
      ),
      HttpApiEndpoint.post("teamCreate", SwarmPaths.teamCreate, {
        params: { sessionID: SessionID },
        payload: CreateTeamPayload,
        success: described(TeamSnapshot, "Created subagent team"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.team.create",
          summary: "Create subagent team",
          description: "Create a named team for coordinating background subagents.",
        }),
      ),
      HttpApiEndpoint.delete("teamDelete", SwarmPaths.teamDelete, {
        params: TeamTarget,
        query: DeleteTeamQuery,
        success: described(TeamSnapshot, "Deleted subagent team"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.team.delete",
          summary: "Delete subagent team",
          description:
            "Delete a subagent team. Refuses active workers by default; pass cancel_workers=true to force-cancel them.",
        }),
      ),
      HttpApiEndpoint.post("teamBroadcast", SwarmPaths.teamBroadcast, {
        params: TeamTarget,
        payload: BroadcastPayload,
        success: described(Schema.Struct({ inputIDs: Schema.Array(Schema.String) }), "Queued broadcast messages"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.team.broadcast",
          summary: "Broadcast to subagent team",
          description: "Send the same message to every active worker in a subagent team.",
        }),
      ),
      HttpApiEndpoint.get("taskList", SwarmPaths.taskList, {
        params: { sessionID: SessionID },
        query: TaskQuery,
        success: described(Schema.Array(TeamTaskState), "Shared team tasks"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.tasks",
          summary: "List shared subagent tasks",
          description: "List shared task-board entries for a parent session or team.",
        }),
      ),
      HttpApiEndpoint.post("taskCreate", SwarmPaths.taskCreate, {
        params: { sessionID: SessionID },
        payload: CreateTaskPayload,
        success: described(TeamTaskState, "Created shared subagent task"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.task.create",
          summary: "Create shared subagent task",
          description: "Create a shared task-board entry for a parent session or team.",
        }),
      ),
      HttpApiEndpoint.get("taskGet", SwarmPaths.taskGet, {
        params: TaskTarget,
        query: TaskQuery,
        success: described(TeamTaskState, "Shared subagent task"),
        error: [HttpApiError.BadRequest, ApiNotFoundError],
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.task.get",
          summary: "Get shared subagent task",
          description: "Get one shared task-board entry by id.",
        }),
      ),
      HttpApiEndpoint.patch("taskUpdate", SwarmPaths.taskUpdate, {
        params: TaskTarget,
        payload: UpdateTaskPayload,
        success: described(UpdateTaskResponse, "Updated shared subagent task"),
        error: HttpApiError.BadRequest,
      }).annotateMerge(
        OpenApi.annotations({
          identifier: "swarm.task.update",
          summary: "Update shared subagent task",
          description: "Update, assign, complete, delete, or add dependencies to a shared task-board entry.",
        }),
      ),
    )
    .middleware(InstanceContextMiddleware)
    .middleware(WorkspaceRoutingMiddleware)
    .middleware(Authorization),
)
