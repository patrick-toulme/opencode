import { Effect, Schema } from "effect"
import * as Tool from "./tool"
import { Session } from "@/session/session"

const CREATE_DESCRIPTION = `Create or replace the long-term goal for this session.

Use this tool only when the user has asked you to set or take ownership of a long-term goal. The goal will be re-injected into your context every turn until it is marked complete or the user clears it.

Provide a single-sentence objective that names the concrete deliverable. Do not set a tokenBudget unless the user explicitly requested a numeric token budget for this goal.`

const UPDATE_DESCRIPTION = `Update the status of the active long-term goal.

Use this tool when:
- You have actually finished the goal (status="complete"). Audit your output before calling this — do not mark complete on intent alone.
- The user has paused the work and you want to acknowledge that (status="paused").

Do not call this tool to extend, redefine, or rescope the goal. The user controls scope; you only report completion.`

export const GoalCreateParameters = Schema.Struct({
  objective: Schema.String.annotate({
    description: "One-sentence description of the long-term deliverable.",
  }),
  tokenBudget: Schema.optional(
    Schema.Number.annotate({
      description:
        "Optional token budget. Only set this when the user explicitly requested a numeric token budget for this goal.",
    }),
  ),
  budgetExplicitlyRequested: Schema.optional(
    Schema.Boolean.annotate({
      description: "Set true only when the user explicitly requested the tokenBudget value.",
    }),
  ),
})

export const GoalUpdateParameters = Schema.Struct({
  status: Schema.Literals(["complete", "paused"]).annotate({
    description: "Set the goal status. Use 'complete' only when the deliverable is actually finished.",
  }),
})

export const GoalCreateTool = Tool.define(
  "goal_create",
  Effect.gen(function* () {
    const session = yield* Session.Service
    return {
      description: CREATE_DESCRIPTION,
      parameters: GoalCreateParameters,
      execute: (args: Schema.Schema.Type<typeof GoalCreateParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const goal = yield* session.setGoal({
            sessionID: ctx.sessionID,
            objective: args.objective,
            tokenBudget: args.budgetExplicitlyRequested === true ? args.tokenBudget : undefined,
          })
          return {
            title: "Goal set",
            output: `Long-term goal set: ${goal.objective}`,
            metadata: {},
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const GoalUpdateTool = Tool.define(
  "goal_update",
  Effect.gen(function* () {
    const session = yield* Session.Service
    return {
      description: UPDATE_DESCRIPTION,
      parameters: GoalUpdateParameters,
      execute: (args: Schema.Schema.Type<typeof GoalUpdateParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const goal = yield* session.updateGoalStatus({
            sessionID: ctx.sessionID,
            status: args.status,
          })
          return {
            title: args.status === "complete" ? "Goal complete" : "Goal paused",
            output: `Goal status set to ${goal.status}.`,
            metadata: {},
          }
        }).pipe(Effect.orDie),
    }
  }),
)
