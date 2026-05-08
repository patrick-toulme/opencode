import { Effect, Option, Schema } from "effect"
import * as Tool from "./tool"
import { SwarmRuntime } from "@/swarm/runtime"
import { Identifier } from "@/id/id"
import { Session } from "@/session/session"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { MessageV2 } from "@/session/message-v2"
import type { WorkerSnapshot, WorkerStatus } from "@/swarm/state"
import { Permission } from "@/permission"
import { PermissionID } from "@/permission/schema"
import { Parameters as TaskParameters, TaskTool } from "./task"

const DESCRIPTION = `Send a follow-up message to a running background subagent or same-project peer session.

Use this when a Task was launched with run_in_background=true and you need to provide more instructions, answer a question, approve/reject a plan or permission request, request shutdown, or continue the same subagent. The recipient can be the worker_id, task_id/session_id, the optional name used when launching the background task, "team-lead" from inside a worker, "session:<session_id>" from list_peers, or "*" to broadcast to a team. Plain text sent to a stopped subagent resumes it in the background on the same task session.

Protocol: workers with plan_mode_required should send {type:"plan_approval_request", plan:"..."} to "team-lead" after inspecting. The lead responds to that worker with {type:"plan_approval_response", request_id:"...", approve:true/false, feedback:"..."}. For permission requests, the lead responds to the blocked worker with {type:"permission_response", request_id:"...", approve:true/false, always:true/false, reason:"..."}. For graceful shutdown the lead sends {type:"shutdown_request"} to a worker; the worker responds to "team-lead" with {type:"shutdown_response", request_id:"...", approve:true/false}.`

type SendMessageMetadata = {
  to: string
  team?: string
  count?: number
  inputIds?: string[]
  inputId?: string
  requestId?: string
  workerId?: string
  sessionId?: string
  messageId?: string
  approved?: boolean
  resumed?: boolean
  previousStatus?: WorkerStatus
}

const PermissionRule = Schema.Struct({
  permission: Schema.String,
  pattern: Schema.String,
  action: Schema.Literals(["allow", "deny", "ask"]),
})

const StructuredMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("shutdown_request"),
    request_id: Schema.optional(Schema.String),
    reason: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("shutdown_response"),
    request_id: Schema.String,
    approve: Schema.Boolean,
    reason: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("plan_approval_request"),
    request_id: Schema.optional(Schema.String),
    plan: Schema.String,
    plan_file_path: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("plan_approval_response"),
    request_id: Schema.String,
    approve: Schema.Boolean,
    feedback: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("permission_response"),
    request_id: Schema.String,
    approve: Schema.Boolean,
    always: Schema.optional(Schema.Boolean),
    reason: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("team_permission_update"),
    rules: Schema.Array(PermissionRule),
    directory_path: Schema.optional(Schema.String),
    tool_name: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("mode_set_request"),
    mode: Schema.Literals([
      "default",
      "ask",
      "accept_edits",
      "acceptEdits",
      "bypass_permissions",
      "bypassPermissions",
      "read_only",
      "readonly",
    ]),
  }),
])
type StructuredMessageValue = Schema.Schema.Type<typeof StructuredMessage>
type PermissionRuleValue = Schema.Schema.Type<typeof PermissionRule>
type ModeSetValue = Extract<StructuredMessageValue, { type: "mode_set_request" }>

export const Parameters = Schema.Struct({
  to: Schema.String.annotate({
    description:
      'The target subagent worker_id, task_id/session_id, launch name, "session:<session_id>" for a same-project peer session, or "*" to broadcast to a team.',
  }),
  message: Schema.Union([Schema.String, StructuredMessage]).annotate({
    description:
      "The message to send. Can be plain text or a structured protocol object such as shutdown_request, shutdown_response, plan_approval_request, plan_approval_response, permission_response, team_permission_update, or mode_set_request.",
  }),
  summary: Schema.optional(Schema.String).annotate({
    description: "A short 5-10 word summary of the message for status displays.",
  }),
  team: Schema.optional(Schema.String).annotate({
    description: 'Team name to broadcast to when to is "*".',
  }),
})

export const ListPeersParameters = Schema.Struct({
  scope: Schema.optional(Schema.Literals(["current", "all"])).annotate({
    description: 'Use "current" to list this session and its workers, or "all" to list recent project sessions too.',
  }),
  include_sessions: Schema.optional(Schema.Boolean).annotate({
    description: "Include same-project session peers addressable as session:<session_id>. Defaults to true.",
  }),
  include_workers: Schema.optional(Schema.Boolean).annotate({
    description: "Include subagent worker peers addressable by worker_id or launch name. Defaults to true.",
  }),
})

export const SendMessageTool = Tool.define(
  "send_message",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const sessions = yield* Session.Service
    const permissions = yield* Permission.Service
    const taskTool = yield* TaskTool
    const task = yield* Tool.init(taskTool)

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (args: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const parentSessionID = currentWorker?.spec.parentSessionID ?? ctx.sessionID
          const sender = currentWorker ? (currentWorker.spec.name ?? ctx.agent) : "team-lead"
          const peerSessionID = parseSessionPeer(args.to)
          if (peerSessionID) {
            if (typeof args.message !== "string") {
              return yield* Effect.fail(new Error("structured messages cannot be sent to session peers"))
            }
            const delivered = yield* deliverToPeerSession({
              sessions,
              from: `session:${ctx.sessionID}`,
              fromWorker: currentWorker,
              targetSessionID: peerSessionID,
              message: args.message,
              summary: args.summary,
            })
            return {
              title: "Peer message sent",
              metadata: {
                to: args.to,
                count: 1,
                messageId: delivered.messageID,
              } as SendMessageMetadata,
              output: [
                `Message sent to peer session: ${peerSessionID}`,
                `message_id: ${delivered.messageID}`,
                args.summary ? `summary: ${args.summary}` : undefined,
              ]
                .filter(Boolean)
                .join("\n"),
            }
          }

          if (typeof args.message !== "string") {
            if (args.to === "*" && !canBroadcastStructured(args.message)) {
              return yield* Effect.fail(new Error("structured messages cannot be broadcast"))
            }

            if (
              args.message.type === "shutdown_response" &&
              !args.message.approve &&
              !args.message.reason?.trim()
            ) {
              return yield* Effect.fail(new Error("reason is required when rejecting a shutdown request"))
            }

            if (args.message.type === "shutdown_response" && args.to === "team-lead") {
              if (!currentWorker) {
                return yield* Effect.fail(new Error("shutdown_response can only be sent by a background subagent"))
              }
              if (args.message.approve) {
                yield* swarm.approveShutdown(currentWorker.spec.workerID, args.message.request_id)
                return {
                  title: "Shutdown approved",
                  metadata: {
                    to: args.to,
                    requestId: args.message.request_id,
                    workerId: currentWorker.spec.workerID,
                    approved: true,
                  } as SendMessageMetadata,
                  output: `Shutdown approved for request ${args.message.request_id}. This subagent will exit after the current turn.`,
                }
              }

              yield* swarm.rejectShutdown(currentWorker.spec.workerID, args.message.request_id, args.message.reason!)
              return {
                title: "Shutdown rejected",
                metadata: {
                  to: args.to,
                  requestId: args.message.request_id,
                  workerId: currentWorker.spec.workerID,
                  approved: false,
                } as SendMessageMetadata,
                output: `Shutdown rejected for request ${args.message.request_id}: ${args.message.reason}`,
              }
            }

            if (args.message.type === "plan_approval_request") {
              if (args.to !== "team-lead") {
                return yield* Effect.fail(new Error("plan_approval_request must be sent to team-lead"))
              }
              if (!currentWorker) {
                return yield* Effect.fail(new Error("plan_approval_request can only be sent by a background subagent"))
              }
              const requestID = args.message.request_id ?? Identifier.create("par", "ascending")
              const message = formatMessage({ ...args.message, request_id: requestID })
              yield* swarm.requestPlanApproval(currentWorker.spec.workerID, requestID)
              const delivered = yield* deliverToTeamLead({
                sessions,
                worker: currentWorker,
                parentSessionID,
                message,
                summary: args.summary ?? "plan approval requested",
              })
              return {
                title: "Plan approval requested",
                metadata: {
                  to: args.to,
                  team: currentWorker.spec.team,
                  count: 1,
                  requestId: requestID,
                  workerId: currentWorker.spec.workerID,
                  messageId: delivered.messageID,
                } as SendMessageMetadata,
                output: [
                  "Plan approval requested from team lead.",
                  `message_id: ${delivered.messageID}`,
                  `request_id: ${requestID}`,
                  args.summary ? `summary: ${args.summary}` : undefined,
                ]
                  .filter(Boolean)
                  .join("\n"),
              }
            }

            if (args.message.type === "plan_approval_response") {
              if (currentWorker) {
                return yield* Effect.fail(new Error("plan_approval_response can only be sent by the team lead"))
              }
              if (!args.message.approve && !args.message.feedback?.trim()) {
                return yield* Effect.fail(new Error("feedback is required when rejecting a plan"))
              }
              const target = yield* swarm.resolve({ parentSessionID, to: args.to })
              if (!target) return yield* Effect.fail(new Error(`No running subagent found for: ${args.to}`))
              if (target.pendingPlanApprovalID !== args.message.request_id) {
                return yield* Effect.fail(
                  new Error(`Subagent ${args.to} is not waiting on plan approval ${args.message.request_id}`),
                )
              }
              if (args.message.approve) {
                yield* swarm.approvePlan(target.spec.workerID, args.message.request_id)
              } else {
                yield* swarm.rejectPlan(target.spec.workerID, args.message.request_id, args.message.feedback!)
              }
              const message = formatMessage(args.message)
              const input = yield* swarm.sendInput({
                parentSessionID,
                to: args.to,
                message,
                summary: args.summary ?? (args.message.approve ? "plan approved" : "plan rejected"),
                from: "team-lead",
              })
              return {
                title: args.message.approve ? "Plan approved" : "Plan rejected",
                metadata: {
                  to: args.to,
                  team: target.spec.team,
                  count: 1,
                  inputIds: [input.id],
                  inputId: input.id,
                  requestId: args.message.request_id,
                  workerId: target.spec.workerID,
                  approved: args.message.approve,
                } as SendMessageMetadata,
                output: [
                  args.message.approve ? "Plan approved." : `Plan rejected: ${args.message.feedback}`,
                  `input_id: ${input.id}`,
                  `request_id: ${args.message.request_id}`,
                ].join("\n"),
              }
            }

            if (args.message.type === "permission_response") {
              if (currentWorker) {
                return yield* Effect.fail(new Error("permission_response can only be sent by the team lead"))
              }
              if (!args.message.approve && !args.message.reason?.trim()) {
                return yield* Effect.fail(new Error("reason is required when rejecting a permission request"))
              }
              const target = yield* swarm.resolve({ parentSessionID, to: args.to })
              if (!target) return yield* Effect.fail(new Error(`No running subagent found for: ${args.to}`))
              if (target.pendingPermissionID !== args.message.request_id) {
                return yield* Effect.fail(
                  new Error(`Subagent ${args.to} is not waiting on permission ${args.message.request_id}`),
                )
              }
              yield* permissions.reply({
                requestID: PermissionID.make(args.message.request_id),
                reply: args.message.approve ? (args.message.always ? "always" : "once") : "reject",
                ...(args.message.reason ? { message: args.message.reason } : {}),
              })
              return {
                title: args.message.approve ? "Permission approved" : "Permission rejected",
                metadata: {
                  to: args.to,
                  team: target.spec.team,
                  count: 1,
                  requestId: args.message.request_id,
                  workerId: target.spec.workerID,
                  approved: args.message.approve,
                } as SendMessageMetadata,
                output: [
                  args.message.approve
                    ? `Permission approved for request ${args.message.request_id}.`
                    : `Permission rejected for request ${args.message.request_id}: ${args.message.reason}`,
                  args.message.approve && args.message.always ? "scope: always" : undefined,
                ]
                  .filter(Boolean)
                  .join("\n"),
              }
            }

            if (args.message.type === "team_permission_update") {
              if (currentWorker) {
                return yield* Effect.fail(new Error("team_permission_update can only be sent by the team lead"))
              }
              if (args.message.rules.length === 0) {
                return yield* Effect.fail(new Error("team_permission_update requires at least one rule"))
              }
              const rules = args.message.rules
              const message = formatMessage(args.message)
              if (args.to === "*") {
                if (!args.team) return yield* Effect.fail(new Error('team_permission_update with to="*" requires team'))
                const workers = (yield* swarm.list(parentSessionID)).filter((worker) => worker.spec.team === args.team)
                yield* Effect.forEach(workers, (worker) => applySessionRules(sessions, worker, rules), {
                  discard: true,
                })
                const inputs = yield* swarm.broadcast({
                  parentSessionID,
                  team: args.team,
                  message,
                  summary: args.summary ?? "team permission update",
                  from: "team-lead",
                })
                return {
                  title: "Team permission updated",
                  metadata: {
                    to: args.to,
                    team: args.team,
                    count: inputs.length,
                    inputIds: inputs.map((input) => input.id),
                  } as SendMessageMetadata,
                  output: [
                    `Team permission update sent to team: ${args.team}`,
                    `recipients: ${inputs.length}`,
                    ...inputs.map((input) => `input_id: ${input.id}`),
                  ].join("\n"),
                }
              }

              const target = yield* swarm.resolve({ parentSessionID, to: args.to })
              if (!target) return yield* Effect.fail(new Error(`No running subagent found for: ${args.to}`))
              yield* applySessionRules(sessions, target, rules)
              const input = yield* swarm.sendInput({
                parentSessionID,
                to: args.to,
                message,
                summary: args.summary ?? "team permission update",
                from: "team-lead",
              })
              return {
                title: "Permission rules updated",
                metadata: {
                  to: args.to,
                  team: target.spec.team,
                  count: 1,
                  inputIds: [input.id],
                  inputId: input.id,
                  workerId: target.spec.workerID,
                } as SendMessageMetadata,
                output: [`Permission rules updated for subagent: ${args.to}`, `input_id: ${input.id}`].join("\n"),
              }
            }

            if (args.message.type === "mode_set_request") {
              if (currentWorker) {
                return yield* Effect.fail(new Error("mode_set_request can only be sent by the team lead"))
              }
              const rules = modeRules(args.message.mode)
              const message = formatMessage(args.message)
              if (args.to === "*") {
                if (!args.team) return yield* Effect.fail(new Error('mode_set_request with to="*" requires team'))
                const workers = (yield* swarm.list(parentSessionID)).filter((worker) => worker.spec.team === args.team)
                yield* Effect.forEach(workers, (worker) => setSessionRules(sessions, worker, rules), { discard: true })
                const inputs = yield* swarm.broadcast({
                  parentSessionID,
                  team: args.team,
                  message,
                  summary: args.summary ?? `mode set: ${args.message.mode}`,
                  from: "team-lead",
                })
                return {
                  title: "Team mode updated",
                  metadata: {
                    to: args.to,
                    team: args.team,
                    count: inputs.length,
                    inputIds: inputs.map((input) => input.id),
                  } as SendMessageMetadata,
                  output: [
                    `Mode ${args.message.mode} sent to team: ${args.team}`,
                    `recipients: ${inputs.length}`,
                    ...inputs.map((input) => `input_id: ${input.id}`),
                  ].join("\n"),
                }
              }

              const target = yield* swarm.resolve({ parentSessionID, to: args.to })
              if (!target) return yield* Effect.fail(new Error(`No running subagent found for: ${args.to}`))
              yield* setSessionRules(sessions, target, rules)
              const input = yield* swarm.sendInput({
                parentSessionID,
                to: args.to,
                message,
                summary: args.summary ?? `mode set: ${args.message.mode}`,
                from: "team-lead",
              })
              return {
                title: "Mode updated",
                metadata: {
                  to: args.to,
                  team: target.spec.team,
                  count: 1,
                  inputIds: [input.id],
                  inputId: input.id,
                  workerId: target.spec.workerID,
                } as SendMessageMetadata,
                output: [`Mode ${args.message.mode} sent to subagent: ${args.to}`, `input_id: ${input.id}`].join("\n"),
              }
            }
          }

          const shutdownRequestID =
            typeof args.message !== "string" && args.message.type === "shutdown_request"
              ? (args.message.request_id ?? Identifier.create("shr", "ascending"))
              : undefined
          const message = formatMessage(
            shutdownRequestID && typeof args.message !== "string" && args.message.type === "shutdown_request"
              ? { ...args.message, request_id: shutdownRequestID }
              : args.message,
          )

          if (args.to === "team-lead" && currentWorker) {
            const delivered = yield* deliverToTeamLead({
              sessions,
              worker: currentWorker,
              parentSessionID,
              message,
              summary: args.summary,
            })
            return {
              title: "Message sent to team lead",
              metadata: {
                to: args.to,
                team: currentWorker.spec.team,
                count: 1,
                requestId: shutdownRequestID,
                workerId: currentWorker.spec.workerID,
                messageId: delivered.messageID,
              } as SendMessageMetadata,
              output: [
                "Message sent to team lead.",
                `message_id: ${delivered.messageID}`,
                shutdownRequestID ? `request_id: ${shutdownRequestID}` : undefined,
                args.summary ? `summary: ${args.summary}` : undefined,
              ]
                .filter(Boolean)
                .join("\n"),
            }
          }

          if (args.to === "*") {
            if (!args.team) return yield* Effect.fail(new Error('send_message with to="*" requires team'))
            const inputs = yield* swarm.broadcast({
              parentSessionID,
              team: args.team,
              message,
              summary: args.summary,
              from: sender,
            })
            return {
              title: "Broadcast sent",
              metadata: {
                to: args.to,
                team: args.team as string | undefined,
                count: inputs.length,
                inputIds: inputs.map((input) => input.id),
                inputId: undefined as string | undefined,
              } as SendMessageMetadata,
              output: [
                `Broadcast sent to team: ${args.team}`,
                `recipients: ${inputs.length}`,
                ...inputs.map((input) => `input_id: ${input.id}`),
                args.summary ? `summary: ${args.summary}` : undefined,
              ]
                .filter(Boolean)
                .join("\n"),
            }
          }

          const target = yield* swarm.resolve({ parentSessionID, to: args.to })
          if (target && typeof args.message === "string" && isTerminalStatus(target.status)) {
            const resumed = yield* resumeStoppedWorker({
              task,
              sessions,
              target,
              message: args.message,
              summary: args.summary,
              ctx,
            })
            return {
              title: "Subagent resumed",
              metadata: {
                to: args.to,
                team: target.spec.team,
                count: 1,
                workerId: String(resumed.metadata.workerId ?? ""),
                sessionId: String(resumed.metadata.sessionId ?? target.spec.sessionID),
                resumed: true,
                previousStatus: target.status,
              } as SendMessageMetadata,
              output: [
                `Subagent ${args.to} was ${target.status}; resumed it in the background on the same task session.`,
                `task_id: ${String(resumed.metadata.sessionId ?? target.spec.sessionID)}`,
                `worker_id: ${String(resumed.metadata.workerId ?? "")}`,
                args.summary ? `summary: ${args.summary}` : undefined,
              ]
                .filter(Boolean)
                .join("\n"),
            }
          }
          if (shutdownRequestID && target) {
            yield* swarm.requestShutdown(target.spec.workerID, shutdownRequestID)
          }
          const input = yield* swarm.sendInput({
            parentSessionID,
            to: args.to,
            message,
            summary: args.summary,
            from: sender,
          })
          return {
            title: "Message sent",
            metadata: {
              to: args.to,
              team: args.team as string | undefined,
              count: 1,
              inputIds: [input.id],
              inputId: input.id as string | undefined,
              requestId: shutdownRequestID,
            } as SendMessageMetadata,
            output: [
              `Message sent to subagent: ${args.to}`,
              `input_id: ${input.id}`,
              shutdownRequestID ? `request_id: ${shutdownRequestID}` : undefined,
              args.summary ? `summary: ${args.summary}` : undefined,
            ]
              .filter(Boolean)
              .join("\n"),
          }
        }).pipe(Effect.orDie),
    }
  }),
)

export const ListPeersTool = Tool.define(
  "list_peers",
  Effect.gen(function* () {
    const swarm = yield* SwarmRuntime.Service
    const sessions = yield* Session.Service

    return {
      description:
        "List sessions and subagents addressable by send_message. Use session:<session_id> for same-project cross-session messages, or a worker_id/name for subagent messages.",
      parameters: ListPeersParameters,
      execute: (args: Schema.Schema.Type<typeof ListPeersParameters>, ctx: Tool.Context) =>
        Effect.gen(function* () {
          const currentWorker = yield* swarm.getBySession(ctx.sessionID)
          const parentSessionID = currentWorker?.spec.parentSessionID ?? ctx.sessionID
          const includeSessions = args.include_sessions ?? true
          const includeWorkers = args.include_workers ?? true
          const peers: Array<Record<string, unknown>> = []

          if (includeSessions) {
            const sessionsList =
              args.scope === "current"
                ? [yield* sessions.get(ctx.sessionID)]
                : yield* sessions.list({
                    roots: true,
                    start: Date.now() - 30 * 24 * 60 * 60 * 1000,
                    limit: 50,
                  })
            for (const session of sessionsList) {
              peers.push({
                kind: "session",
                address: `session:${session.id}`,
                sessionID: session.id,
                title: session.title,
                current: session.id === ctx.sessionID,
                updatedAt: session.time.updated,
              })
            }
          }

          if (includeWorkers) {
            const workers = yield* swarm.list(args.scope === "all" ? undefined : parentSessionID)
            for (const worker of workers) {
              peers.push({
                kind: "subagent",
                address: worker.spec.name ?? worker.spec.workerID,
                workerID: worker.spec.workerID,
                sessionID: worker.spec.sessionID,
                name: worker.spec.name,
                team: worker.spec.team,
                status: worker.status,
                description: worker.spec.description,
              })
            }
          }

          return {
            title: "Peers",
            metadata: {
              count: peers.length,
              peers,
            },
            output: peers.length
              ? [
                  `<peers count="${peers.length}">`,
                  ...peers.map((peer) => formatPeer(peer)),
                  "</peers>",
                ].join("\n")
              : "No peers found.",
          }
        }).pipe(Effect.orDie),
    }
  }),
)

const terminalStatuses = new Set<WorkerStatus>(["completed", "cancelled", "failed", "interrupted"])
const isTerminalStatus = (status: WorkerStatus) => terminalStatuses.has(status)

const resumeStoppedWorker = Effect.fn("SendMessage.resumeStoppedWorker")(function* (input: {
  task: Tool.Def<typeof TaskParameters>
  sessions: Session.Interface
  target: WorkerSnapshot
  message: string
  summary?: string
  ctx: Tool.Context
}) {
  if (!input.ctx.extra?.promptOps) {
    return yield* Effect.fail(new Error("Cannot resume stopped subagent from send_message without promptOps"))
  }
  const parentAssistant = yield* input.sessions.findMessage(
    input.target.spec.parentSessionID,
    (msg) => msg.info.role === "assistant",
  )
  if (Option.isNone(parentAssistant)) {
    return yield* Effect.fail(new Error("Cannot resume stopped subagent without an assistant message in the parent session"))
  }
  const info = parentAssistant.value.info
  if (info.role !== "assistant") {
    return yield* Effect.fail(new Error("Cannot resume stopped subagent without an assistant message in the parent session"))
  }

  return yield* input.task.execute(
    {
      description: input.summary?.trim() || `resume ${input.target.spec.description}`,
      prompt: input.message,
      subagent_type: input.target.spec.agent,
      task_id: input.target.spec.sessionID,
      ...(input.target.spec.name ? { name: input.target.spec.name } : {}),
      ...(input.target.spec.team ? { team: input.target.spec.team } : {}),
      ...(input.target.spec.model
        ? { model: `${input.target.spec.model.providerID}/${input.target.spec.model.modelID}` }
        : {}),
      run_in_background: true,
      ...(input.target.spec.planModeRequired ? { plan_mode_required: true } : {}),
      context: "fresh",
    },
    {
      ...input.ctx,
      sessionID: input.target.spec.parentSessionID,
      messageID: info.id,
      agent: info.agent,
      extra: {
        ...input.ctx.extra,
        bypassAgentCheck: true,
      },
    },
  )
})

const formatMessage = (message: Schema.Schema.Type<typeof Parameters>["message"]) => {
  if (typeof message === "string") return message
  switch (message.type) {
    case "shutdown_request":
      return [
        "<structured-message>",
        "<type>shutdown_request</type>",
        message.request_id ? `<request-id>${xmlEscape(message.request_id)}</request-id>` : "",
        message.reason ? `<reason>${xmlEscape(message.reason)}</reason>` : "",
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "shutdown_response":
      return [
        "<structured-message>",
        "<type>shutdown_response</type>",
        `<request-id>${xmlEscape(message.request_id)}</request-id>`,
        `<approve>${message.approve ? "true" : "false"}</approve>`,
        message.reason ? `<reason>${xmlEscape(message.reason)}</reason>` : "",
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "plan_approval_request":
      return [
        "<structured-message>",
        "<type>plan_approval_request</type>",
        message.request_id ? `<request-id>${xmlEscape(message.request_id)}</request-id>` : "",
        message.plan_file_path ? `<plan-file-path>${xmlEscape(message.plan_file_path)}</plan-file-path>` : "",
        `<plan>${xmlEscape(message.plan)}</plan>`,
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "plan_approval_response":
      return [
        "<structured-message>",
        "<type>plan_approval_response</type>",
        `<request-id>${xmlEscape(message.request_id)}</request-id>`,
        `<approve>${message.approve ? "true" : "false"}</approve>`,
        message.feedback ? `<feedback>${xmlEscape(message.feedback)}</feedback>` : "",
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "permission_response":
      return [
        "<structured-message>",
        "<type>permission_response</type>",
        `<request-id>${xmlEscape(message.request_id)}</request-id>`,
        `<approve>${message.approve ? "true" : "false"}</approve>`,
        message.always ? "<always>true</always>" : "",
        message.reason ? `<reason>${xmlEscape(message.reason)}</reason>` : "",
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "team_permission_update":
      return [
        "<structured-message>",
        "<type>team_permission_update</type>",
        message.directory_path ? `<directory-path>${xmlEscape(message.directory_path)}</directory-path>` : "",
        message.tool_name ? `<tool-name>${xmlEscape(message.tool_name)}</tool-name>` : "",
        `<rules>${xmlEscape(JSON.stringify(message.rules))}</rules>`,
        "</structured-message>",
      ]
        .filter((line) => line !== "")
        .join("\n")
    case "mode_set_request":
      return [
        "<structured-message>",
        "<type>mode_set_request</type>",
        `<mode>${xmlEscape(message.mode)}</mode>`,
        "</structured-message>",
      ].join("\n")
  }
}

const canBroadcastStructured = (message: Exclude<Schema.Schema.Type<typeof Parameters>["message"], string>) =>
  message.type === "team_permission_update" || message.type === "mode_set_request"

const applySessionRules = Effect.fn("SendMessage.applySessionRules")(function* (
  sessions: Session.Interface,
  worker: WorkerSnapshot,
  rules: ReadonlyArray<PermissionRuleValue>,
) {
  const session = yield* sessions.get(worker.spec.sessionID)
  const existing = session.permission ?? []
  const withoutReplaced = existing.filter(
    (rule) => !rules.some((next) => next.permission === rule.permission && next.pattern === rule.pattern),
  )
  yield* sessions.setPermission({
    sessionID: worker.spec.sessionID,
    permission: [...withoutReplaced, ...rules],
  })
})

const setSessionRules = Effect.fn("SendMessage.setSessionRules")(function* (
  sessions: Session.Interface,
  worker: WorkerSnapshot,
  rules: Permission.Ruleset,
) {
  yield* sessions.setPermission({
    sessionID: worker.spec.sessionID,
    permission: rules,
  })
})

const modeRules = (mode: ModeSetValue["mode"]): Permission.Ruleset => {
  switch (mode) {
    case "default":
    case "ask":
      return []
    case "accept_edits":
    case "acceptEdits":
      return [{ permission: "edit", pattern: "*", action: "allow" }]
    case "bypass_permissions":
    case "bypassPermissions":
      return [{ permission: "*", pattern: "*", action: "allow" }]
    case "read_only":
    case "readonly":
      return [
        { permission: "edit", pattern: "*", action: "deny" },
        { permission: "bash", pattern: "*", action: "deny" },
      ]
  }
  return []
}

const deliverToTeamLead = Effect.fn("SendMessage.deliverToTeamLead")(function* (input: {
  sessions: Session.Interface
  worker: WorkerSnapshot
  parentSessionID: SessionID
  message: string
  summary?: string
}) {
  const latest = (yield* input.sessions.messages({ sessionID: input.parentSessionID, limit: 1 }))[0]
  const model =
    input.worker.spec.model ??
    (latest
      ? latest.info.role === "assistant"
        ? { providerID: latest.info.providerID, modelID: latest.info.modelID }
        : latest.info.model
      : undefined)
  if (!model) return yield* Effect.fail(new Error("Cannot deliver message to team lead without model metadata"))

  const messageID = MessageID.ascending()
  const from = input.worker.spec.name ?? input.worker.spec.workerID
  yield* input.sessions.updateMessage({
    id: messageID,
    role: "user",
    sessionID: input.parentSessionID,
    agent: input.worker.spec.agent,
    model,
    time: { created: Date.now() },
  })
  yield* input.sessions.updatePart({
    id: PartID.ascending(),
    messageID,
    sessionID: input.parentSessionID,
    type: "text",
    synthetic: true,
    metadata: {
      kind: "swarm-message",
      from,
      workerID: input.worker.spec.workerID,
      ...(input.worker.spec.team ? { team: input.worker.spec.team } : {}),
      ...(input.summary ? { summary: input.summary } : {}),
    },
    text: [
      "<teammate-message>",
      `<from>${xmlEscape(from)}</from>`,
      `<worker-id>${xmlEscape(input.worker.spec.workerID)}</worker-id>`,
      input.worker.spec.team ? `<team>${xmlEscape(input.worker.spec.team)}</team>` : "",
      input.summary ? `<summary>${xmlEscape(input.summary)}</summary>` : "",
      `<message>${xmlEscape(input.message)}</message>`,
      "</teammate-message>",
    ]
      .filter((line) => line !== "")
      .join("\n"),
  } satisfies MessageV2.TextPart)

  return { messageID }
})

const deliverToPeerSession = Effect.fn("SendMessage.deliverToPeerSession")(function* (input: {
  sessions: Session.Interface
  from: string
  fromWorker?: WorkerSnapshot
  targetSessionID: SessionID
  message: string
  summary?: string
}) {
  const target = yield* input.sessions.get(input.targetSessionID)
  const model = yield* resolveSessionModel(input.sessions, input.targetSessionID, input.fromWorker?.spec.model)
  const messageID = MessageID.ascending()
  yield* input.sessions.updateMessage({
    id: messageID,
    role: "user",
    sessionID: input.targetSessionID,
    agent: target.agent ?? input.fromWorker?.spec.agent ?? "build",
    model,
    time: { created: Date.now() },
  })
  yield* input.sessions.updatePart({
    id: PartID.ascending(),
    messageID,
    sessionID: input.targetSessionID,
    type: "text",
    synthetic: true,
    metadata: {
      kind: "cross-session-message",
      from: input.from,
      ...(input.fromWorker ? { workerID: input.fromWorker.spec.workerID } : {}),
      ...(input.summary ? { summary: input.summary } : {}),
    },
    text: [
      `<cross-session-message from="${xmlEscape(input.from)}">`,
      `<from>${xmlEscape(input.from)}</from>`,
      input.fromWorker ? `<worker-id>${xmlEscape(input.fromWorker.spec.workerID)}</worker-id>` : "",
      input.summary ? `<summary>${xmlEscape(input.summary)}</summary>` : "",
      `<message>${xmlEscape(input.message)}</message>`,
      "</cross-session-message>",
    ]
      .filter((line) => line !== "")
      .join("\n"),
  } satisfies MessageV2.TextPart)

  return { messageID }
})

const resolveSessionModel = Effect.fn("SendMessage.resolveSessionModel")(function* (
  sessions: Session.Interface,
  sessionID: SessionID,
  fallback?: NonNullable<WorkerSnapshot["spec"]["model"]>,
) {
  const latest = (yield* sessions.messages({ sessionID, limit: 1 }))[0]
  if (latest?.info.role === "assistant") {
    return { providerID: latest.info.providerID, modelID: latest.info.modelID }
  }
  if (latest?.info.role === "user") return latest.info.model
  const info = yield* sessions.get(sessionID)
  if (info.model) return { providerID: info.model.providerID, modelID: info.model.id }
  if (fallback) return fallback
  return yield* Effect.fail(new Error(`Cannot deliver peer message to ${sessionID} without model metadata`))
})

const parseSessionPeer = (value: string): SessionID | undefined => {
  if (value.startsWith("session:")) return SessionID.make(value.slice("session:".length))
  if (value.startsWith("opencode:")) return SessionID.make(value.slice("opencode:".length))
  return undefined
}

const formatPeer = (peer: Record<string, unknown>) =>
  [
    "<peer>",
    `<kind>${xmlEscape(String(peer.kind ?? ""))}</kind>`,
    `<address>${xmlEscape(String(peer.address ?? ""))}</address>`,
    peer.sessionID ? `<session-id>${xmlEscape(String(peer.sessionID))}</session-id>` : "",
    peer.workerID ? `<worker-id>${xmlEscape(String(peer.workerID))}</worker-id>` : "",
    peer.name ? `<name>${xmlEscape(String(peer.name))}</name>` : "",
    peer.team ? `<team>${xmlEscape(String(peer.team))}</team>` : "",
    peer.status ? `<status>${xmlEscape(String(peer.status))}</status>` : "",
    peer.title ? `<title>${xmlEscape(String(peer.title))}</title>` : "",
    peer.description ? `<description>${xmlEscape(String(peer.description))}</description>` : "",
    peer.current ? "<current>true</current>" : "",
    "</peer>",
  ]
    .filter((line) => line !== "")
    .join("\n")

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

export * as SendMessage from "./send_message"
