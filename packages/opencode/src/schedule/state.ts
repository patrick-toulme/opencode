import { Identifier } from "@/id/id"
import { SessionID } from "@/session/schema"
import { WorkerID } from "@/swarm/state"
import { Schema } from "effect"
import { withStatics } from "@/util/schema"
import { zod } from "@/util/effect-zod"

export const ScheduledTaskID = Schema.String.pipe(
  Schema.brand("ScheduledTaskID"),
  withStatics((s) => ({
    ascending: (id?: string) => s.make(id ?? Identifier.create("sch", "ascending")),
    zod: zod(s),
  })),
)
export type ScheduledTaskID = Schema.Schema.Type<typeof ScheduledTaskID>

export const ScheduledTaskState = Schema.Struct({
  id: ScheduledTaskID,
  sessionID: SessionID,
  parentSessionID: SessionID,
  agent: Schema.String,
  cron: Schema.String,
  prompt: Schema.String,
  recurring: Schema.Boolean,
  durable: Schema.Boolean,
  createdAt: Schema.Number,
  lastFiredAt: Schema.optional(Schema.Number),
  targetWorkerID: Schema.optional(WorkerID),
  targetName: Schema.optional(Schema.String),
  targetTeam: Schema.optional(Schema.String),
})
  .annotate({ identifier: "ScheduledTaskState" })
  .pipe(withStatics((s) => ({ zod: zod(s) })))
export type ScheduledTaskState = Schema.Schema.Type<typeof ScheduledTaskState>

export type ScheduledTaskSnapshot = ScheduledTaskState

export * as ScheduleState from "./state"
