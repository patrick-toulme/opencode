import { BusEvent } from "@/bus/bus-event"
import { ScheduledTaskID, ScheduledTaskState } from "./state"
import { Schema } from "effect"

export const Event = {
  Created: BusEvent.define(
    "schedule.task.created",
    Schema.Struct({
      taskID: ScheduledTaskID,
      task: ScheduledTaskState,
    }),
  ),
  Fired: BusEvent.define(
    "schedule.task.fired",
    Schema.Struct({
      taskID: ScheduledTaskID,
      task: ScheduledTaskState,
      routedToWorker: Schema.optional(Schema.Boolean),
    }),
  ),
  Deleted: BusEvent.define(
    "schedule.task.deleted",
    Schema.Struct({
      taskID: ScheduledTaskID,
      task: ScheduledTaskState,
    }),
  ),
}

export * as ScheduleEvents from "./events"
