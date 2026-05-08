import { Schema } from "effect"
import { Identifier } from "@/id/id"
import { ModelID, ProviderID } from "@/provider/schema"
import { MessageID, SessionID } from "@/session/schema"
import { withStatics } from "@/util/schema"
import { zod } from "@/util/effect-zod"

export const WorkerID = Schema.String.pipe(
  Schema.brand("SwarmWorkerID"),
  withStatics((s) => ({
    ascending: (id?: string) => s.make(id ?? Identifier.create("swa", "ascending")),
    zod: zod(s),
  })),
)
export type WorkerID = Schema.Schema.Type<typeof WorkerID>

export const TeamTaskID = Schema.String.pipe(
  Schema.brand("SwarmTeamTaskID"),
  withStatics((s) => ({
    from: (id: string) => s.make(id),
    zod: zod(s),
  })),
)
export type TeamTaskID = Schema.Schema.Type<typeof TeamTaskID>

export const WorkerStatus = Schema.Literals([
  "queued",
  "booting",
  "running",
  "waiting_permission",
  "waiting_input",
  "idle",
  "completed",
  "cancelled",
  "failed",
  "interrupted",
])
export type WorkerStatus = Schema.Schema.Type<typeof WorkerStatus>

export const TeamTaskStatus = Schema.Literals(["pending", "in_progress", "completed"])
export type TeamTaskStatus = Schema.Schema.Type<typeof TeamTaskStatus>

export const ContextStrategy = Schema.Literals(["fresh", "fork", "auto"])
export type ContextStrategy = Schema.Schema.Type<typeof ContextStrategy>

export const PermissionStrategy = Schema.Literals(["bubble", "local", "deny"])
export type PermissionStrategy = Schema.Schema.Type<typeof PermissionStrategy>

export const ExecutionStrategy = Schema.Literals(["oneshot", "persistent"])
export type ExecutionStrategy = Schema.Schema.Type<typeof ExecutionStrategy>

export const WorkerBackend = Schema.Literals(["in-process", "worktree", "tmux", "iterm2", "remote"])
export type WorkerBackend = Schema.Schema.Type<typeof WorkerBackend>

export const WorkerModel = Schema.Struct({
  providerID: ProviderID,
  modelID: ModelID,
  variant: Schema.optional(Schema.String),
})
export type WorkerModel = Schema.Schema.Type<typeof WorkerModel>

export const WorkerSpec = Schema.Struct({
  workerID: WorkerID,
  parentSessionID: SessionID,
  sessionID: SessionID,
  agent: Schema.String,
  name: Schema.optional(Schema.String),
  team: Schema.optional(Schema.String),
  prompt: Schema.String,
  description: Schema.String,
  outputPath: Schema.optional(Schema.String),
  contextStrategy: ContextStrategy,
  permissionStrategy: PermissionStrategy,
  executionStrategy: ExecutionStrategy,
  backend: WorkerBackend,
  paneID: Schema.optional(Schema.String),
  paneExternalSession: Schema.optional(Schema.Boolean),
  paneWindowTarget: Schema.optional(Schema.String),
  worktreeRoot: Schema.optional(Schema.String),
  worktreePath: Schema.optional(Schema.String),
  worktreeBranch: Schema.optional(Schema.String),
  remoteEndpoint: Schema.optional(Schema.String),
  remoteID: Schema.optional(Schema.String),
  remoteSessionURL: Schema.optional(Schema.String),
  remoteOutputPath: Schema.optional(Schema.String),
  model: Schema.optional(WorkerModel),
  fork: Schema.optional(Schema.Boolean),
  planModeRequired: Schema.optional(Schema.Boolean),
  sourceToolCallID: Schema.optional(Schema.String),
  sourceMessageID: Schema.optional(MessageID),
})
export type WorkerSpec = Schema.Schema.Type<typeof WorkerSpec>

export const WorkerState = Schema.Struct({
  spec: WorkerSpec,
  status: WorkerStatus,
  startedAt: Schema.Number,
  updatedAt: Schema.Number,
  currentTool: Schema.optional(
    Schema.Struct({
      name: Schema.String,
      title: Schema.optional(Schema.String),
    }),
  ),
  lastProgress: Schema.optional(Schema.String),
  pendingPermissionID: Schema.optional(Schema.String),
  pendingShutdownID: Schema.optional(Schema.String),
  pendingPlanApprovalID: Schema.optional(Schema.String),
  paneHidden: Schema.optional(Schema.Boolean),
  remoteCursor: Schema.optional(Schema.String),
  mailboxSize: Schema.optional(Schema.Number),
  result: Schema.optional(
    Schema.Struct({
      text: Schema.optional(Schema.String),
      error: Schema.optional(Schema.String),
    }),
  ),
})
  .annotate({ identifier: "SwarmWorkerState" })
  .pipe(withStatics((s) => ({ zod: zod(s) })))
export type WorkerState = Schema.Schema.Type<typeof WorkerState>

export type WorkerCompletion =
  | { status: "completed"; text: string }
  | { status: "cancelled"; text: string }
  | { status: "failed"; error: string }

export type WorkerSnapshot = WorkerState

export const WorkerInput = Schema.Struct({
  id: Schema.String,
  message: Schema.String,
  summary: Schema.optional(Schema.String),
  from: Schema.optional(Schema.String),
  createdAt: Schema.Number,
})
export type WorkerInput = Schema.Schema.Type<typeof WorkerInput>

export const TeamState = Schema.Struct({
  parentSessionID: SessionID,
  name: Schema.String,
  description: Schema.optional(Schema.String),
  leadSessionID: Schema.optional(SessionID),
  agentType: Schema.optional(Schema.String),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
})
  .annotate({ identifier: "SwarmTeamState" })
  .pipe(withStatics((s) => ({ zod: zod(s) })))
export type TeamState = Schema.Schema.Type<typeof TeamState>

export const TeamSnapshot = Schema.Struct({
  ...TeamState.fields,
  workerIDs: Schema.Array(WorkerID),
})
  .annotate({ identifier: "SwarmTeamSnapshot" })
  .pipe(withStatics((s) => ({ zod: zod(s) })))
export type TeamSnapshot = Schema.Schema.Type<typeof TeamSnapshot>

export const TeamTaskState = Schema.Struct({
  id: TeamTaskID,
  parentSessionID: SessionID,
  team: Schema.optional(Schema.String),
  subject: Schema.String,
  description: Schema.String,
  activeForm: Schema.optional(Schema.String),
  status: TeamTaskStatus,
  owner: Schema.optional(Schema.String),
  blocks: Schema.Array(TeamTaskID),
  blockedBy: Schema.Array(TeamTaskID),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  createdAt: Schema.Number,
  updatedAt: Schema.Number,
})
  .annotate({ identifier: "SwarmTeamTaskState" })
  .pipe(withStatics((s) => ({ zod: zod(s) })))
export type TeamTaskState = Schema.Schema.Type<typeof TeamTaskState>

export function initialWorkerState(spec: WorkerSpec, now = Date.now()): WorkerState {
  return {
    spec,
    status: "queued",
    startedAt: now,
    updatedAt: now,
  }
}

export * as SwarmState from "./state"
