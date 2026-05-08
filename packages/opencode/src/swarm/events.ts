import { BusEvent } from "@/bus/bus-event"
import { SessionID } from "@/session/schema"
import { Schema } from "effect"
import { TeamSnapshot, TeamTaskState, WorkerID, WorkerState, WorkerStatus } from "./state"

export const Event = {
  Spawned: BusEvent.define(
    "swarm.worker.spawned",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      agent: Schema.String,
      worker: WorkerState,
    }),
  ),
  StatusChanged: BusEvent.define(
    "swarm.worker.status",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      status: WorkerStatus,
      message: Schema.optional(Schema.String),
      worker: WorkerState,
    }),
  ),
  Progress: BusEvent.define(
    "swarm.worker.progress",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      message: Schema.String,
      worker: WorkerState,
    }),
  ),
  ToolChanged: BusEvent.define(
    "swarm.worker.tool",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      tool: Schema.optional(
        Schema.Struct({
          name: Schema.String,
          title: Schema.optional(Schema.String),
        }),
      ),
      worker: WorkerState,
    }),
  ),
  PermissionChanged: BusEvent.define(
    "swarm.worker.permission",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      permissionID: Schema.optional(Schema.String),
      worker: WorkerState,
    }),
  ),
  InputQueued: BusEvent.define(
    "swarm.worker.input.queued",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      inputID: Schema.String,
      from: Schema.optional(Schema.String),
      summary: Schema.optional(Schema.String),
      mailboxSize: Schema.Number,
      worker: WorkerState,
    }),
  ),
  WorkerIdle: BusEvent.define(
    "swarm.worker.idle",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      team: Schema.optional(Schema.String),
      idleReason: Schema.optional(Schema.Literals(["available", "interrupted", "failed"])),
      summary: Schema.optional(Schema.String),
      completedTaskID: Schema.optional(Schema.String),
      completedStatus: Schema.optional(Schema.Literals(["resolved", "blocked", "failed"])),
      failureReason: Schema.optional(Schema.String),
      worker: WorkerState,
    }),
  ),
  WorkerStopped: BusEvent.define(
    "swarm.worker.stopped",
    Schema.Struct({
      workerID: WorkerID,
      parentSessionID: SessionID,
      sessionID: SessionID,
      status: WorkerStatus,
      reason: Schema.optional(Schema.String),
      worker: WorkerState,
    }),
  ),
  TeamCreated: BusEvent.define(
    "swarm.team.created",
    Schema.Struct({
      parentSessionID: SessionID,
      name: Schema.String,
      team: TeamSnapshot,
    }),
  ),
  TeamUpdated: BusEvent.define(
    "swarm.team.updated",
    Schema.Struct({
      parentSessionID: SessionID,
      name: Schema.String,
      team: TeamSnapshot,
    }),
  ),
  TeamDeleted: BusEvent.define(
    "swarm.team.deleted",
    Schema.Struct({
      parentSessionID: SessionID,
      name: Schema.String,
      team: TeamSnapshot,
    }),
  ),
  TaskCreated: BusEvent.define(
    "swarm.task.created",
    Schema.Struct({
      parentSessionID: SessionID,
      team: Schema.optional(Schema.String),
      task: TeamTaskState,
    }),
  ),
  TaskUpdated: BusEvent.define(
    "swarm.task.updated",
    Schema.Struct({
      parentSessionID: SessionID,
      team: Schema.optional(Schema.String),
      task: TeamTaskState,
    }),
  ),
  TaskDeleted: BusEvent.define(
    "swarm.task.deleted",
    Schema.Struct({
      parentSessionID: SessionID,
      team: Schema.optional(Schema.String),
      task: TeamTaskState,
    }),
  ),
  TaskCompleted: BusEvent.define(
    "swarm.task.completed",
    Schema.Struct({
      parentSessionID: SessionID,
      team: Schema.optional(Schema.String),
      task: TeamTaskState,
      completedBy: Schema.optional(Schema.String),
    }),
  ),
}

export * as SwarmEvents from "./events"
