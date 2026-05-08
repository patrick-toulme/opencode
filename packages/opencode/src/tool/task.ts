import * as Tool from "./tool"
import DESCRIPTION from "./task.txt"
import fs from "fs/promises"
import path from "path"
import { Session } from "@/session/session"
import { SessionID, MessageID, PartID } from "../session/schema"
import { MessageV2 } from "../session/message-v2"
import { Agent } from "../agent/agent"
import type { SessionPrompt } from "../session/prompt"
import { Config } from "@/config/config"
import { Plugin } from "@/plugin"
import { Provider } from "@/provider/provider"
import { Cause, Effect, Exit, Schema } from "effect"
import { EffectBridge } from "@/effect/bridge"
import { SwarmRuntime } from "@/swarm/runtime"
import { WorkerID, type WorkerCompletion } from "@/swarm/state"
import { prepareWorkerPane } from "@/swarm/backend/launch"
import {
  appendRemoteWorkerOutput,
  cancelRemoteWorker,
  launchRemoteWorker,
  pollRemoteWorker,
  type RemoteWorkerEvent,
  type RemoteWorkerLaunchResponse,
} from "@/swarm/remote/client"
import { cleanupWorktreeIfClean, type WorkerWorktreeInfo } from "@/swarm/worktree"
import { errorMessage } from "@/util/error"
import { InstanceState } from "@/effect/instance-state"
import { InstanceRef } from "@/effect/instance-ref"
import type { InstanceContext } from "@/project/instance"
import { Global } from "@opencode-ai/core/global"
import { workerLeadToolsDisabled, workerToolsDisabled } from "@/swarm/worker-tools"
import { writeWorkerTranscriptWithSession } from "@/swarm/transcript"

export interface TaskPromptOps {
  cancel(sessionID: SessionID): Effect.Effect<void>
  resolvePromptParts(template: string): Effect.Effect<SessionPrompt.PromptInput["parts"]>
  prompt(input: SessionPrompt.PromptInput): Effect.Effect<MessageV2.WithParts>
}

const id = "task"
const FORK_BOILERPLATE_TAG = "fork-boilerplate"
const FORK_DIRECTIVE_PREFIX = "Your directive: "
const FORK_PLACEHOLDER_RESULT = "Fork started - processing in background"

export const Parameters = Schema.Struct({
  description: Schema.String.annotate({ description: "A short (3-5 words) description of the task" }),
  prompt: Schema.String.annotate({ description: "The task for the agent to perform" }),
  subagent_type: Schema.optional(Schema.String).annotate({
    description:
      "The type of specialized agent to use for a fresh subagent. Omit this to fork yourself with inherited context.",
  }),
  task_id: Schema.optional(Schema.String).annotate({
    description:
      "This should only be set if you mean to resume a previous task (you can pass a prior task_id and the task will continue the same subagent session as before instead of creating a fresh one)",
  }),
  name: Schema.optional(Schema.String).annotate({
    description:
      "Optional name for a background subagent. This makes it addressable with the send_message tool while it is running.",
  }),
  team: Schema.optional(Schema.String).annotate({
    description:
      "Optional team name for coordinating multiple background subagents. Agents in the same team can receive broadcast messages.",
  }),
  team_name: Schema.optional(Schema.String).annotate({
    description: "Compatibility alias for team.",
  }),
  model: Schema.optional(Schema.String).annotate({
    description:
      'Optional model override in "provider/model" form. When omitted, the subagent configured model or parent model is used.',
  }),
  run_in_background: Schema.optional(Schema.Boolean).annotate({
    description:
      "Compatibility field. Model-facing subagents run in the background; only trusted internal callers may set this to false for synchronous execution.",
  }),
  plan_mode_required: Schema.optional(Schema.Boolean).annotate({
    description:
      "Set true only for implementation subagents that must receive team-lead approval before mutating files or running shell commands. Set false for read-only research.",
  }),
  mode: Schema.optional(Schema.Literals(["plan"])).annotate({
    description:
      'Compatibility spawn mode alias. Set to "plan" only for implementation work that needs approval gating; omit for read-only research/explore tasks.',
  }),
  context: Schema.optional(Schema.Literals(["auto", "fork", "fresh"])).annotate({
    description:
      'Controls how much parent conversation context the subagent inherits. Use "fresh" for self-contained delegated work. Use "fork" only when the full parent conversation is required as background.',
  }),
  isolation: Schema.optional(Schema.Literals(["worktree", "remote"])).annotate({
    description:
      'Usually omit. Set "worktree" only for isolated file edits, or "remote" only when remote execution is explicitly required/configured. For read-only research, omit isolation.',
  }),
  cwd: Schema.optional(Schema.String).annotate({
    description:
      "Absolute or relative working directory override for the subagent. Do not set this together with isolation; if both are provided, cwd takes precedence and isolation is ignored.",
  }),
  command: Schema.optional(Schema.String).annotate({ description: "The command that triggered this task" }),
})

export const TaskTool = Tool.define(
  id,
  Effect.gen(function* () {
    const agent = yield* Agent.Service
    const config = yield* Config.Service
    const plugin = yield* Plugin.Service
    const sessions = yield* Session.Service
    const swarm = yield* SwarmRuntime.Service

    const run = Effect.fn("TaskTool.execute")(function* (
      params: Schema.Schema.Type<typeof Parameters>,
      ctx: Tool.Context,
    ) {
      const cfg = yield* config.get()
      const teamName = params.team ?? params.team_name
      const configuredBackend = process.env.OPENCODE_SWARM_BACKEND ?? cfg.experimental?.swarm_backend
      const taskID = params.task_id
      const existingWorker = taskID
        ? yield* swarm.getBySession(SessionID.make(taskID)).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const currentWorker = yield* swarm
        .getBySession(ctx.sessionID)
        .pipe(Effect.catchCause(() => Effect.succeed(undefined)))
      const selectedAgent = params.subagent_type ?? existingWorker?.spec.agent ?? ctx.agent
      const implicitFork = !params.subagent_type && !existingWorker
      const forkWorker = implicitFork || existingWorker?.spec.fork === true
      const next = yield* agent.get(selectedAgent)
      if (!next) {
        return yield* Effect.fail(new Error(`Unknown agent type: ${selectedAgent} is not a valid agent type`))
      }
      const planModeRequired = params.plan_mode_required ?? (next.name !== "explore" && params.mode === "plan")
      const configuredIsolation = typeof next.options?.isolation === "string" ? next.options.isolation : undefined
      const agentIsolation =
        next.isolation ??
        (configuredIsolation === "worktree" || configuredIsolation === "remote" ? configuredIsolation : undefined)
      let effectiveIsolation: "worktree" | "remote" | undefined = existingWorker
        ? undefined
        : (params.isolation ?? agentIsolation)
      const remoteEndpoint =
        (typeof ctx.extra?.swarmRemoteEndpoint === "string" ? ctx.extra.swarmRemoteEndpoint : undefined) ??
        process.env.OPENCODE_SWARM_REMOTE_ENDPOINT ??
        cfg.experimental?.swarm_remote_endpoint
      const remoteToken =
        (typeof ctx.extra?.swarmRemoteToken === "string" ? ctx.extra.swarmRemoteToken : undefined) ??
        process.env.OPENCODE_SWARM_REMOTE_TOKEN ??
        cfg.experimental?.swarm_remote_token
      if (params.cwd && effectiveIsolation) {
        effectiveIsolation = undefined
      }
      if (effectiveIsolation === "remote" && !remoteEndpoint && configuredBackend !== "remote") {
        effectiveIsolation = undefined
      }
      const remoteWorker = effectiveIsolation === "remote" || configuredBackend === "remote"
      if (remoteWorker && !remoteEndpoint) {
        return yield* Effect.fail(
          new Error('Remote subagents require experimental.swarm_remote_endpoint or OPENCODE_SWARM_REMOTE_ENDPOINT'),
        )
      }
      const allowForegroundTask = ctx.extra?.allowForegroundTask === true
      const requestedForeground = params.run_in_background === false && allowForegroundTask
      const configuredBackground =
        next.background === true || (typeof next.options?.background === "boolean" ? next.options.background : false)
      const runInBackground =
        configuredBackground ||
        !requestedForeground ||
        implicitFork ||
        existingWorker?.spec.executionStrategy === "persistent"
      if (remoteWorker && !runInBackground) {
        return yield* Effect.fail(new Error("Remote subagents must run in the background"))
      }

      if (implicitFork && (currentWorker?.spec.fork === true || (yield* isInForkChildSession(sessions, ctx.sessionID)))) {
        return yield* Effect.fail(new Error("Cannot fork from inside an implicit fork child. Execute directly instead."))
      }

      if (!ctx.extra?.bypassAgentCheck) {
        yield* ctx.ask({
          permission: id,
          patterns: [params.subagent_type ?? (forkWorker ? "fork" : selectedAgent)],
          always: ["*"],
          metadata: {
            description: params.description,
            subagent_type: params.subagent_type ?? (forkWorker ? "fork" : selectedAgent),
            ...(forkWorker ? { fork: true } : {}),
          },
        })
      }

      const canTask = false
      const canTodo = next.permission.some((rule) => rule.permission === "todowrite")

      const session = taskID
        ? yield* sessions.get(SessionID.make(taskID)).pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const parent = yield* sessions.get(ctx.sessionID)
      const parentInstance = yield* InstanceState.context
      const workerID = WorkerID.ascending()
      const outputPath = path.join(Global.Path.data, "subagent-transcripts", `${workerID}.jsonl`)
      const worktree =
        effectiveIsolation === "worktree" && !session
          ? yield* createWorktree(parentInstance, workerID, params.description)
          : undefined
      const cwd = params.cwd ? path.resolve(parentInstance.directory, params.cwd) : worktree?.path
      const childInstance: InstanceContext | undefined = cwd
        ? {
            directory: cwd,
            worktree: worktree?.path ?? parentInstance.worktree,
            project: {
              ...parentInstance.project,
              worktree: worktree?.path ?? parentInstance.project.worktree,
              sandboxes: worktree
                ? [...parentInstance.project.sandboxes, worktree.path]
                : parentInstance.project.sandboxes,
            },
          }
        : undefined
      const nextSession =
        session ??
        (yield* runInChildInstance(
          childInstance,
          sessions.create({
            parentID: ctx.sessionID,
            title: params.description + ` (@${next.name} subagent)`,
            permission: [
              ...(parent.permission ?? []).filter(
                (rule) => rule.permission === "external_directory" || rule.action === "deny",
              ),
              ...(canTodo
                ? []
                : [
                    {
                      permission: "todowrite" as const,
                      pattern: "*" as const,
                      action: "deny" as const,
                    },
                  ]),
              ...(cfg.experimental?.primary_tools?.map((item) => ({
                pattern: "*",
                action: "allow" as const,
                permission: item,
              })) ?? []),
              ...Object.keys(workerLeadToolsDisabled).map((tool) => ({
                permission: tool,
                pattern: "*" as const,
                action: "deny" as const,
              })),
            ],
          }),
        ))

      const msg = yield* Effect.sync(() => MessageV2.get({ sessionID: ctx.sessionID, messageID: ctx.messageID }))
      if (msg.info.role !== "assistant") return yield* Effect.fail(new Error("Not an assistant message"))
      const parentAssistant = msg.info

      const model = params.model
        ? Provider.parseModel(params.model)
        : (next.model ?? {
            modelID: parentAssistant.modelID,
            providerID: parentAssistant.providerID,
          })
      const contextStrategy = params.context ?? (implicitFork ? "fork" : (existingWorker?.spec.contextStrategy ?? "auto"))
      const shouldForkContext = !session && contextStrategy !== "fresh"
      const workerName = params.name ? yield* uniqueWorkerName(ctx.sessionID, params.name, swarm) : undefined

      yield* ctx.metadata({
        title: params.description,
        metadata: {
          sessionId: nextSession.id,
          workerId: workerID,
          name: workerName,
          team: teamName,
          model,
          ...(forkWorker ? { fork: true } : {}),
          ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
          status: "booting",
        },
      })

      const ops = ctx.extra?.promptOps as TaskPromptOps
      if (!ops) return yield* Effect.fail(new Error("TaskTool requires promptOps in ctx.extra"))
      const runCancel = yield* EffectBridge.make()

      const messageID = MessageID.ascending()
      const cancel = ops.cancel(nextSession.id)
      let remoteLaunch: RemoteWorkerLaunchResponse | undefined

      function onAbort() {
        if (!runInBackground) runCancel.fork(cancel)
      }

      const withBackgroundTeammateReminder = (prompt: string, currentMessageID: MessageID) => {
        if (!runInBackground || currentMessageID !== messageID) return prompt
        return [
          "<system-reminder>",
          forkWorker
            ? "You are a long-lived background fork running as a teammate."
            : "You are a long-lived background subagent running as a teammate.",
          `worker_id: ${workerID}`,
          workerName ? `name: ${workerName}` : "",
          teamName ? `team: ${teamName}` : "",
          "",
          "Communication rules:",
          "- You are a worker, not the lead. Do the assigned generic work directly: inspect, edit, run commands, test, and report as requested by the task prompt.",
          "- Do not create or update session goals, create/delete teams, manage other subagents, read other subagent transcripts, or spawn nested subagents. If the assignment needs more parallelism, ask the lead with send_message.",
          "- If inherited context includes a /goal command or prior swarm setup, treat that as lead-session background only. Do not re-run it.",
          "- Use send_message to communicate results, blockers, or questions to the lead or teammates. A normal final assistant response updates your transcript, but is not automatically visible to the user or other teammates.",
          "- Use list_team_tasks, get_task, create_task, and update_task to coordinate shared team work. You may receive auto-claimed task prompts from the task board while idle.",
          "- Use schedule_task for one-shot or recurring follow-ups that should return to you or the lead later in this opencode process. Durable schedules are only supported for parent sessions, not subagents.",
          "- When you start an assigned task, ensure it is in_progress and owned by you. When you finish it, mark it completed with update_task before going idle.",
          "- If blocked, leave the task in_progress, create or update the blocking task, and notify the lead with send_message.",
          planModeRequired
            ? "- Plan approval gates mutating work only. You may perform read-only inspection without approval. If this assignment is read-only and requires no mutations, complete it without requesting approval. Before modifying files or running shell commands, send send_message(to=\"team-lead\", message={type:\"plan_approval_request\", plan:\"...\"}) and wait for an approving plan_approval_response. If rejected, revise the plan and request approval again."
            : "",
          "</system-reminder>",
          "",
          prompt,
        ]
          .filter((line) => line !== "")
          .join("\n")
      }

      const initialPrompt = (prompt: string, currentMessageID: MessageID) => {
        const withAgentInitialPrompt =
          currentMessageID === messageID && next.initialPrompt?.trim()
            ? [next.initialPrompt.trim(), prompt].join("\n\n")
            : prompt
        const forkPrompt =
          implicitFork && currentMessageID === messageID
            ? buildForkChildMessage(
                [
                  worktree ? buildWorktreeNotice(parentInstance.directory, worktree.path) : "",
                  withAgentInitialPrompt,
                ]
                  .filter((line) => line !== "")
                  .join("\n\n"),
              )
            : withAgentInitialPrompt
        return withBackgroundTeammateReminder(forkPrompt, currentMessageID)
      }

      const runPrompt = (prompt: string, currentMessageID: MessageID, planApproved: boolean) =>
        runInChildInstance(
          childInstance,
          Effect.gen(function* () {
            const parts = yield* ops.resolvePromptParts(initialPrompt(prompt, currentMessageID))
            const result = yield* ops.prompt({
              messageID: currentMessageID,
              sessionID: nextSession.id,
              model: {
                modelID: model.modelID,
                providerID: model.providerID,
              },
              agent: next.name,
              tools: {
                ...(canTodo ? {} : { todowrite: false }),
                ...(canTask ? {} : { task: false }),
                ...Object.fromEntries((cfg.experimental?.primary_tools ?? []).map((item) => [item, false])),
                ...workerToolsDisabled({ planModeRequired, planApproved }),
              },
              persistToolPermissions: false,
              parts,
            })
            const text = result.parts.findLast((item) => item.type === "text")?.text ?? ""
            if (ctx.abort.aborted && !runInBackground) {
              return {
                status: "cancelled",
                text,
              } satisfies WorkerCompletion
            }
            return {
              status: "completed",
              text,
            } satisfies WorkerCompletion
          }),
        )

      const copyParentContext = Effect.gen(function* () {
        if (!shouldForkContext) return
        const parentMessages = yield* sessions.messages({ sessionID: ctx.sessionID })
        const idMap = new Map<MessageID, MessageID>()
        for (const item of parentMessages) {
          const isForkPrefixAssistant =
            implicitFork && item.info.id === ctx.messageID && item.info.role === "assistant"
          if (item.info.id > ctx.messageID || (!isForkPrefixAssistant && item.info.id >= ctx.messageID)) break
          const nextID = MessageID.ascending()
          idMap.set(item.info.id, nextID)
          const parentID =
            item.info.role === "assistant" && item.info.parentID ? idMap.get(item.info.parentID) : undefined
          const cloned = yield* sessions.updateMessage({
            ...item.info,
            id: nextID,
            sessionID: nextSession.id,
            ...(parentID ? { parentID } : {}),
          })
          for (const part of item.parts) {
            if (part.type === "subtask") continue
            const clonedPart = clonePartForChild(part, cloned.id, nextSession.id, isForkPrefixAssistant)
            if (clonedPart.type === "compaction" && clonedPart.tail_start_id) {
              clonedPart.tail_start_id = idMap.get(clonedPart.tail_start_id)
            }
            yield* sessions.updatePart(clonedPart)
          }
        }
        if (implicitFork) return
        const notice = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          role: "user",
          sessionID: nextSession.id,
          agent: next.name,
          model: {
            providerID: model.providerID,
            modelID: model.modelID,
          },
          time: { created: Date.now() },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          messageID: notice.id,
          sessionID: nextSession.id,
          type: "text",
          synthetic: true,
          metadata: { kind: "fork-context-notice", parentSessionID: ctx.sessionID },
          text: [
            "<system-reminder>",
            "You have inherited the parent agent's conversation context above. Use it as background, but follow the task prompt that comes next as your current assignment.",
            "</system-reminder>",
          ].join("\n"),
        } satisfies MessageV2.TextPart)
      })

      const addSubagentStartContext = Effect.gen(function* () {
        const output: { additionalContexts: string[]; additionalContext?: string } = { additionalContexts: [] }
        yield* plugin.trigger(
          "swarm.subagent.start",
          {
            workerID,
            sessionID: nextSession.id,
            parentSessionID: ctx.sessionID,
            agentType: next.name,
            description: params.description,
            prompt: params.prompt,
            ...(workerName ? { teammateName: workerName } : {}),
            ...(teamName ? { teamName } : {}),
            background: runInBackground,
            ...(forkWorker ? { fork: true } : {}),
          },
          output,
        )
        const contexts = [...output.additionalContexts, output.additionalContext]
          .map((item) => item?.trim())
          .filter((item): item is string => Boolean(item))
        if (contexts.length === 0) return

        const message = yield* sessions.updateMessage({
          id: MessageID.ascending(),
          role: "user",
          sessionID: nextSession.id,
          agent: next.name,
          model: {
            providerID: model.providerID,
            modelID: model.modelID,
          },
          time: { created: Date.now() },
        })
        yield* sessions.updatePart({
          id: PartID.ascending(),
          messageID: message.id,
          sessionID: nextSession.id,
          type: "text",
          synthetic: true,
          metadata: { kind: "hook-additional-context", hook: "swarm.subagent.start", parentSessionID: ctx.sessionID },
          text: [
            "<hook-additional-context hook=\"SubagentStart\">",
            ...contexts.map((context) => xmlEscape(context)),
            "</hook-additional-context>",
          ].join("\n"),
        } satisfies MessageV2.TextPart)
      })

      const notifyParent = (completion: WorkerCompletion) =>
        Effect.gen(function* () {
          const status = remoteWorker
            ? completion.status
            : runInBackground && completion.status === "completed"
              ? "idle"
              : completion.status
          const summary =
            status === "idle"
              ? `Subagent "${params.description}" is idle`
              : status === "completed"
              ? `Subagent "${params.description}" completed`
              : status === "failed"
                ? `Subagent "${params.description}" failed`
                : `Subagent "${params.description}" was cancelled`
          const body =
            status === "idle"
              ? ""
              : completion.status === "failed"
                ? completion.error
                : completion.text
          const parentMessage = yield* sessions.updateMessage({
            id: MessageID.ascending(),
            role: "user",
            sessionID: ctx.sessionID,
            agent: ctx.agent,
            model: {
              providerID: parentAssistant.providerID,
              modelID: parentAssistant.modelID,
            },
            time: { created: Date.now() },
          })
          yield* sessions.updatePart({
            id: PartID.ascending(),
            messageID: parentMessage.id,
            sessionID: ctx.sessionID,
            type: "text",
            synthetic: true,
            metadata: {
              kind: "task-notification",
              taskID: nextSession.id,
              workerID,
              ...(teamName ? { team: teamName } : {}),
              status,
              outputPath,
              ...(remoteLaunch?.remoteID ? { remoteID: remoteLaunch.remoteID } : {}),
              ...(remoteLaunch?.sessionURL ? { remoteSessionURL: remoteLaunch.sessionURL } : {}),
              ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
            },
            text: [
              "<task-notification>",
              `<task-id>${xmlEscape(nextSession.id)}</task-id>`,
              `<worker-id>${xmlEscape(workerID)}</worker-id>`,
              workerName ? `<name>${xmlEscape(workerName)}</name>` : "",
              teamName ? `<team>${xmlEscape(teamName)}</team>` : "",
              `<output-file>${xmlEscape(outputPath)}</output-file>`,
              remoteLaunch?.remoteID ? `<remote-id>${xmlEscape(remoteLaunch.remoteID)}</remote-id>` : "",
              remoteLaunch?.sessionURL ? `<remote-session-url>${xmlEscape(remoteLaunch.sessionURL)}</remote-session-url>` : "",
              worktree ? `<worktree-path>${xmlEscape(worktree.path)}</worktree-path>` : "",
              worktree?.branch ? `<worktree-branch>${xmlEscape(worktree.branch)}</worktree-branch>` : "",
              `<status>${xmlEscape(status)}</status>`,
              `<summary>${xmlEscape(summary)}</summary>`,
              body ? `<result>${xmlEscape(body)}</result>` : "",
              "</task-notification>",
            ]
              .filter((line) => line !== "")
              .join("\n"),
          } satisfies MessageV2.TextPart)
        })

      const runIdleHook = (completion: Extract<WorkerCompletion, { status: "completed" }>) =>
        Effect.gen(function* () {
          const output: { continue: boolean; message?: string } = { continue: true }
          yield* plugin.trigger(
            "swarm.teammate.idle",
            {
              workerID,
              sessionID: nextSession.id,
              parentSessionID: ctx.sessionID,
              ...(workerName ? { teammateName: workerName } : {}),
              ...(teamName ? { teamName } : {}),
              summary: completion.text,
            },
            output,
          )
          if (output.continue !== false) return undefined
          return output.message?.trim() || "TeammateIdle hook requested that this subagent continue working."
        })

      const writeSubagentTranscript = (fallbackLastAssistantMessage?: string) =>
        writeWorkerTranscriptWithSession(sessions, {
          workerID,
          sessionID: nextSession.id,
          outputPath,
          fallbackLastAssistantMessage,
        })
      const ensureTranscriptFile = Effect.promise(async () => {
        await fs.mkdir(path.dirname(outputPath), { recursive: true })
        const handle = await fs.open(outputPath, "a")
        await handle.close()
      })

      const remoteClient = remoteWorker
        ? {
            endpoint: remoteEndpoint!,
            ...(remoteToken ? { token: remoteToken } : {}),
          }
        : undefined

      const appendRemoteEvent = (remoteID: string, event: RemoteWorkerEvent) =>
        appendRemoteWorkerOutput(outputPath, workerID, remoteID, event).pipe(Effect.ignore)

      const launchRemote = Effect.gen(function* () {
        if (!remoteClient) return
        const remotePrompt = initialPrompt(params.prompt, messageID)
        const contextMessages = shouldForkContext ? yield* sessions.messages({ sessionID: nextSession.id }) : undefined
        remoteLaunch = yield* launchRemoteWorker(remoteClient, {
          version: 1,
          cwd: childInstance?.directory ?? parentInstance.directory,
          prompt: remotePrompt,
          ...(contextMessages ? { contextMessages } : {}),
          worker: {
            workerID,
            parentSessionID: ctx.sessionID,
            sessionID: nextSession.id,
            agent: next.name,
            ...(workerName ? { name: workerName } : {}),
            ...(teamName ? { team: teamName } : {}),
            prompt: remotePrompt,
            description: params.description,
            outputPath,
            contextStrategy,
            permissionStrategy: "bubble",
            executionStrategy: "persistent",
            backend: "remote",
            remoteEndpoint: remoteClient.endpoint,
            model: {
              modelID: model.modelID,
              providerID: model.providerID,
            },
            ...(forkWorker ? { fork: true } : {}),
            ...(planModeRequired ? { planModeRequired } : {}),
            ...(ctx.callID ? { sourceToolCallID: ctx.callID } : {}),
            sourceMessageID: ctx.messageID,
          },
        })
        yield* swarm.updateRemoteMetadata(workerID, {
          remoteEndpoint: remoteClient.endpoint,
          remoteID: remoteLaunch.remoteID,
          remoteSessionURL: remoteLaunch.sessionURL,
          remoteOutputPath: remoteLaunch.outputPath,
          remoteCursor: remoteLaunch.cursor,
        })
        yield* appendRemoteWorkerOutput(outputPath, workerID, remoteLaunch.remoteID, {
          type: "launch",
          ...(remoteLaunch.sessionURL ? { sessionURL: remoteLaunch.sessionURL } : {}),
          ...(remoteLaunch.outputPath ? { outputPath: remoteLaunch.outputPath } : {}),
        }).pipe(Effect.ignore)
      })

      const runRemoteWorker = Effect.gen(function* () {
        if (!remoteClient) return { status: "failed", error: "Remote subagent endpoint is not configured" } satisfies WorkerCompletion
        let latest = yield* swarm.get(workerID)
        let remoteID = latest?.spec.remoteID ?? remoteLaunch?.remoteID
        if (!remoteID) return { status: "failed", error: "Remote subagent launch did not return remoteID" } satisfies WorkerCompletion
        let cursor = latest?.remoteCursor ?? remoteLaunch?.cursor
        while (true) {
          latest = yield* swarm.get(workerID)
          if (latest?.status === "cancelled") {
            const completion = { status: "cancelled", text: latest.result?.text ?? "cancelled" } satisfies WorkerCompletion
            yield* notifyParent(completion)
            return completion
          }
          remoteID = latest?.spec.remoteID ?? remoteID
          const polled = yield* pollRemoteWorker(remoteClient, remoteID, cursor)
          if (polled.cursor !== undefined && polled.cursor !== cursor) {
            cursor = polled.cursor
            yield* swarm.updateRemoteCursor(workerID, cursor)
          }
          for (const event of polled.events) {
            yield* appendRemoteEvent(remoteID, event)
            switch (event.type) {
              case "progress":
                yield* swarm.updateProgress(workerID, event.message)
                break
              case "output":
                yield* swarm.updateProgress(workerID, event.text)
                break
              case "completed": {
                const completion = { status: "completed", text: event.text } satisfies WorkerCompletion
                yield* notifyParent(completion)
                return completion
              }
              case "failed": {
                const completion = { status: "failed", error: event.error } satisfies WorkerCompletion
                yield* notifyParent(completion)
                return completion
              }
              case "cancelled": {
                const completion = { status: "cancelled", text: event.text ?? "cancelled" } satisfies WorkerCompletion
                yield* notifyParent(completion)
                return completion
              }
            }
          }
          yield* Effect.sleep("250 millis")
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const completion: WorkerCompletion = Cause.hasInterruptsOnly(cause)
              ? { status: "cancelled", text: "cancelled" }
              : { status: "failed", error: errorMessage(Cause.squash(cause)) }
            yield* notifyParent(completion)
            return completion
          }),
        ),
      )

      const runSubagentStopHook = (completion: WorkerCompletion, stopHookActive: boolean) =>
        Effect.gen(function* () {
          const output: { continue: boolean; message?: string } = { continue: true }
          const transcript = yield* writeSubagentTranscript(
            completion.status === "failed" ? completion.error : completion.text,
          )
          yield* plugin.trigger(
            "swarm.subagent.stop",
            {
              workerID,
              sessionID: nextSession.id,
              parentSessionID: ctx.sessionID,
              agentType: next.name,
              status: completion.status,
              stopHookActive,
              transcriptPath: transcript.transcriptPath,
              ...(transcript.lastAssistantMessage ? { lastAssistantMessage: transcript.lastAssistantMessage } : {}),
              ...(workerName ? { teammateName: workerName } : {}),
              ...(teamName ? { teamName } : {}),
            },
            output,
          )
          if (completion.status !== "completed" || output.continue !== false) return undefined
          return output.message?.trim() || "SubagentStop hook requested that this subagent continue working."
        })

      const runBackgroundWorker = Effect.gen(function* () {
        let prompt = params.prompt
        let nextMessageID = messageID
        let planApproved = !planModeRequired
        while (true) {
          const completion = yield* runPrompt(prompt, nextMessageID, planApproved).pipe(
            Effect.catchCause((cause) =>
              Effect.succeed(
                Cause.hasInterruptsOnly(cause)
                  ? ({ status: "cancelled", text: "cancelled" } satisfies WorkerCompletion)
                : ({ status: "failed", error: errorMessage(Cause.squash(cause)) } satisfies WorkerCompletion),
              ),
            ),
          )
          yield* writeSubagentTranscript(completion.status === "failed" ? completion.error : completion.text).pipe(
            Effect.ignore,
          )
          const stateAfterTurn = yield* swarm.get(workerID)
          if (stateAfterTurn?.status === "cancelled") {
            const cancelled: WorkerCompletion = {
              status: "cancelled",
              text: stateAfterTurn.result?.text ?? "cancelled",
            }
            yield* runSubagentStopHook(cancelled, false)
            yield* notifyParent(cancelled)
            return cancelled
          }
          if (completion.status === "completed") {
            const idleHookMessage = yield* runIdleHook(completion)
            if (idleHookMessage) {
              yield* swarm.updateProgress(workerID, `idle hook feedback: ${idleHookMessage}`)
              prompt = [
                "<system-reminder>",
                "TeammateIdle hook feedback:",
                idleHookMessage,
                "</system-reminder>",
              ].join("\n")
              nextMessageID = MessageID.ascending()
              continue
            }
          }
          yield* swarm.recordResult(workerID, completion)
          yield* notifyParent(completion)
          if (completion.status !== "completed") {
            yield* runSubagentStopHook(completion, false)
            return completion
          }
          const input = yield* swarm.awaitInput(workerID)
          if (planModeRequired) {
            const response = parsePlanApprovalResponse(input.message)
            if (response?.approve === true) {
              planApproved = true
            } else if (response?.approve === false) {
              planApproved = false
            }
          }
          prompt = input.message
          nextMessageID = MessageID.ascending()
        }
      }).pipe(Effect.ensuring(writeSubagentTranscript().pipe(Effect.ignore)))
      const runForegroundWorker = Effect.gen(function* () {
        let prompt = params.prompt
        let nextMessageID = messageID
        let stopHookActive = false
        while (true) {
          const completion = yield* runPrompt(prompt, nextMessageID, true)
          const stopHookMessage = yield* runSubagentStopHook(completion, stopHookActive)
          if (!stopHookMessage) return completion
          yield* swarm.updateProgress(workerID, `stop hook feedback: ${stopHookMessage}`)
          prompt = [
            "<system-reminder>",
            "SubagentStop hook feedback:",
            stopHookMessage,
            "</system-reminder>",
          ].join("\n")
          nextMessageID = MessageID.ascending()
          stopHookActive = true
        }
      })
      const runWorker = runInBackground
        ? remoteWorker
          ? runRemoteWorker
          : worktree
          ? runBackgroundWorker.pipe(Effect.ensuring(cleanupWorktreeIfClean(worktree)))
          : runBackgroundWorker
        : runForegroundWorker

      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          ctx.abort.addEventListener("abort", onAbort)
        }),
        () =>
          Effect.gen(function* () {
            yield* copyParentContext
            yield* addSubagentStartContext
            yield* ensureTranscriptFile
            const pane = runInBackground && !remoteWorker
              ? yield* prepareWorkerPane({
                  workerID,
                  cwd: childInstance?.directory ?? parentInstance.directory,
                  name: workerName ?? params.description,
                  description: params.description,
                  backend: (configuredBackend ?? "auto") as
                    | "auto"
                    | "in-process"
                    | "tmux"
                    | "iterm2",
                })
              : undefined
            const storedPrompt = pane ? initialPrompt(params.prompt, messageID) : params.prompt
            const cancelWorker = pane
              ? Effect.promise(() => pane.kill()).pipe(Effect.asVoid, Effect.catchCause(() => Effect.void))
              : remoteWorker && remoteClient
                ? Effect.gen(function* () {
                    if (!remoteLaunch?.remoteID) return
                    yield* cancelRemoteWorker(remoteClient, remoteLaunch.remoteID).pipe(Effect.ignore)
                  })
                : cancel
            const result = yield* swarm.spawn({
              workerID,
              parentSessionID: ctx.sessionID,
              sessionID: nextSession.id,
              agent: next.name,
              name: workerName,
              team: teamName,
              prompt: storedPrompt,
              description: params.description,
              outputPath,
              model: {
                modelID: model.modelID,
                providerID: model.providerID,
              },
              sourceToolCallID: ctx.callID,
              sourceMessageID: ctx.messageID,
              contextStrategy,
              permissionStrategy: "bubble",
              executionStrategy: runInBackground ? "persistent" : "oneshot",
              backend: remoteWorker ? "remote" : (pane?.backend ?? (worktree ? "worktree" : "in-process")),
              paneID: pane?.paneID,
              paneExternalSession: pane?.useExternalSession,
              paneWindowTarget: pane?.windowTarget,
              worktreeRoot: worktree?.root,
              worktreePath: worktree?.path,
              worktreeBranch: worktree?.branch,
              remoteEndpoint: remoteClient?.endpoint,
              fork: forkWorker,
              planModeRequired,
              wait: !runInBackground,
              cancel: cancelWorker,
              ...(remoteWorker
                ? { launch: launchRemote, run: runWorker }
                : pane
                ? { launch: Effect.promise(() => pane.launch()) }
                : { run: runWorker }),
            })

            if (runInBackground) {
              if (result.completion?.status === "failed") {
                return yield* Effect.fail(new Error(result.completion.error))
              }
              yield* ctx.metadata({
                title: params.description,
                metadata: {
                  sessionId: nextSession.id,
                  workerId: workerID,
                  name: workerName,
                  team: teamName,
                  model,
                  outputPath,
                  ...(forkWorker ? { fork: true } : {}),
                  ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
                  ...(remoteLaunch
                    ? {
                        backend: "remote",
                        remoteId: remoteLaunch.remoteID,
                        remoteSessionUrl: remoteLaunch.sessionURL,
                        remoteOutputPath: remoteLaunch.outputPath,
                      }
                    : {}),
                  ...(pane ? { backend: pane.backend, paneId: pane.paneID } : {}),
                  status: "running",
                },
              })

              return {
                title: params.description,
                metadata: {
                  sessionId: nextSession.id,
                  workerId: workerID,
                  name: workerName,
                  team: teamName,
                  model,
                  outputPath,
                  ...(forkWorker ? { fork: true } : {}),
                  ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
                  ...(remoteLaunch
                    ? {
                        backend: "remote",
                        remoteId: remoteLaunch.remoteID,
                        remoteSessionUrl: remoteLaunch.sessionURL,
                        remoteOutputPath: remoteLaunch.outputPath,
                      }
                    : {}),
                  ...(pane ? { backend: pane.backend, paneId: pane.paneID } : {}),
                  status: "running",
                },
                output: [
                  forkWorker ? "Background fork launched." : "Background subagent launched.",
                  `task_id: ${nextSession.id} (for resuming or targeting this subagent)`,
                  `worker_id: ${workerID} (use send_message with this id to continue the ${forkWorker ? "fork" : "subagent"})`,
                  `output_file: ${outputPath} (transcript path; prefer read_task_output while this subagent is running)`,
                  remoteLaunch ? `backend: remote remote_id: ${remoteLaunch.remoteID}` : "",
                  remoteLaunch?.sessionURL ? `remote_session_url: ${remoteLaunch.sessionURL}` : "",
                  pane ? `backend: ${pane.backend} pane_id: ${pane.paneID}` : "",
                  workerName ? `name: ${workerName} (use send_message with this name to continue the subagent)` : "",
                  "",
                  "<task_status>",
                  "running_in_background",
                  "</task_status>",
                ]
                  .filter((line) => line !== "")
                  .join("\n"),
              }
            }

            const completion: WorkerCompletion =
              result.completion ??
              (result.state.status === "failed"
                ? { status: "failed", error: result.state.result?.error ?? "Subagent task failed" }
                : {
                    status: result.state.status === "cancelled" ? "cancelled" : "completed",
                    text: result.state.result?.text ?? "",
                  })

            yield* ctx.metadata({
              title: params.description,
              metadata: {
                sessionId: nextSession.id,
                workerId: workerID,
                name: workerName,
                team: teamName,
                model,
                outputPath,
                ...(forkWorker ? { fork: true } : {}),
                ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
                status: completion.status,
              },
            })

            if (worktree) yield* cleanupWorktreeIfClean(worktree)

            if (completion.status === "failed") {
              return yield* Effect.fail(new Error(completion.error ?? "Subagent task failed"))
            }

            return {
              title: params.description,
              metadata: {
                sessionId: nextSession.id,
                workerId: workerID,
                name: workerName,
                team: teamName,
                model,
                outputPath,
                ...(forkWorker ? { fork: true } : {}),
                ...(worktree ? { worktreePath: worktree.path, worktreeBranch: worktree.branch } : {}),
                status: completion.status,
              },
              output: [
                `task_id: ${nextSession.id} (for resuming to continue this task if needed)`,
                "",
                "<task_result>",
                completion.text,
                "</task_result>",
              ].join("\n"),
            }
          }),
        (_, exit) =>
          Effect.gen(function* () {
            if (Exit.hasInterrupts(exit) && !runInBackground) yield* swarm.cancel(workerID)
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                ctx.abort.removeEventListener("abort", onAbort)
              }),
            ),
          ),
      )
    })

    return {
      description: DESCRIPTION,
      parameters: Parameters,
      execute: (params: Schema.Schema.Type<typeof Parameters>, ctx: Tool.Context) =>
        run(params, ctx).pipe(Effect.orDie),
    }
  }),
)

const isInForkChildSession = Effect.fn("TaskTool.isInForkChildSession")(function* (
  sessions: Session.Interface,
  sessionID: SessionID,
) {
  const messages = yield* sessions.messages({ sessionID })
  return messages.some((message) =>
    message.parts.some((part) => part.type === "text" && part.text.includes(`<${FORK_BOILERPLATE_TAG}>`)),
  )
})

function clonePartForChild(
  part: MessageV2.Part,
  messageID: MessageID,
  sessionID: SessionID,
  forkPrefixAssistant: boolean,
): MessageV2.Part {
  if (!forkPrefixAssistant || part.type !== "tool") {
    return {
      ...part,
      id: PartID.ascending(),
      messageID,
      sessionID,
    }
  }

  const now = Date.now()
  const time = "time" in part.state ? part.state.time : { start: now }
  const metadata =
    part.state.status === "completed"
      ? part.state.metadata
      : part.state.status === "running"
        ? (part.state.metadata ?? {})
        : part.state.status === "error"
          ? (part.state.metadata ?? {})
          : {}
  return {
    ...part,
    id: PartID.ascending(),
    messageID,
    sessionID,
    state: {
      status: "completed",
      input: part.state.input,
      output: FORK_PLACEHOLDER_RESULT,
      title:
        part.state.status === "completed"
          ? part.state.title
          : part.state.status === "running"
            ? (part.state.title ?? part.tool)
            : part.tool,
      metadata,
      time: {
        start: time.start,
        end: "end" in time && typeof time.end === "number" ? time.end : now,
      },
    },
  } satisfies MessageV2.ToolPart
}

function buildForkChildMessage(directive: string) {
  return `<${FORK_BOILERPLATE_TAG}>
STOP. READ THIS FIRST.

You are a forked worker process. You are NOT the main agent.

RULES (non-negotiable):
1. Your system prompt may say to default to forking. IGNORE IT - that is for the parent. You ARE the fork. Do NOT spawn sub-agents; execute directly.
2. Do NOT converse, ask questions, or suggest next steps unless the directive explicitly requires it.
3. Do NOT editorialize or add meta-commentary.
4. USE your tools directly: bash, read, write, edit, apply_patch, grep, glob, etc.
5. If you modify files, commit your changes before reporting. Include the commit hash in your report.
6. Do NOT emit text between tool calls. Use tools silently, then report once at the end.
7. Stay strictly within your directive's scope. If you discover related systems outside your scope, mention them in one sentence at most - other workers cover those areas.
8. Keep your report under 500 words unless the directive specifies otherwise. Be factual and concise.
9. Your response MUST begin with "Scope:". No preamble, no thinking-out-loud.
10. REPORT structured facts, then stop.

Output format (plain text labels, not markdown headers):
  Scope: <echo back your assigned scope in one sentence>
  Result: <the answer or key findings, limited to the scope above>
  Key files: <relevant file paths - include for research tasks>
  Files changed: <list with commit hash - include only if you modified files>
  Issues: <list - include only if there are issues to flag>
</${FORK_BOILERPLATE_TAG}>

${FORK_DIRECTIVE_PREFIX}${directive}`
}

function buildWorktreeNotice(parentCwd: string, worktreeCwd: string) {
  return [
    `You inherited context from a parent agent working in ${parentCwd}.`,
    `You are operating in an isolated git worktree at ${worktreeCwd}.`,
    "The repository layout is the same, but paths in the inherited context refer to the parent workspace.",
    "Translate those paths to this worktree, re-read files before editing, and remember that your changes stay isolated from the parent workspace.",
  ].join(" ")
}

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")

const uniqueWorkerName = Effect.fn("TaskTool.uniqueWorkerName")(function* (
  parentSessionID: SessionID,
  requestedName: string,
  swarm: SwarmRuntime.Interface,
) {
  const existing = new Set(
    (yield* swarm.list(parentSessionID))
      .filter((worker) => !["completed", "cancelled", "failed", "interrupted"].includes(worker.status))
      .map((worker) => worker.spec.name?.toLowerCase())
      .filter((name): name is string => Boolean(name)),
  )
  if (!existing.has(requestedName.toLowerCase())) return requestedName

  let suffix = 2
  while (existing.has(`${requestedName}-${suffix}`.toLowerCase())) suffix++
  return `${requestedName}-${suffix}`
})

const parsePlanApprovalResponse = (message: string): { requestID?: string; approve: boolean; feedback?: string } | undefined => {
  if (!message.includes("plan_approval_response")) return undefined

  const json = message.match(/\{[\s\S]*"type"\s*:\s*"plan_approval_response"[\s\S]*\}/)
  if (json) {
    try {
      const parsed = JSON.parse(json[0]) as {
        request_id?: unknown
        requestId?: unknown
        approve?: unknown
        approved?: unknown
        feedback?: unknown
      }
      const approval = typeof parsed.approve === "boolean" ? parsed.approve : parsed.approved
      if (typeof approval === "boolean") {
        return {
          requestID:
            typeof parsed.request_id === "string"
              ? parsed.request_id
              : typeof parsed.requestId === "string"
                ? parsed.requestId
                : undefined,
          approve: approval,
          feedback: typeof parsed.feedback === "string" ? parsed.feedback : undefined,
        }
      }
    } catch {
      // Fall through to XML-ish parsing.
    }
  }

  const approve = tagValue(message, "approve")
  if (approve !== "true" && approve !== "false") return undefined
  return {
    requestID: tagValue(message, "request-id"),
    approve: approve === "true",
    feedback: tagValue(message, "feedback"),
  }
}

const tagValue = (message: string, tag: string) => {
  const match = message.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`))
  return match?.[1]
}

type WorktreeInfo = WorkerWorktreeInfo

const runInChildInstance = <A, E, R>(
  instance: InstanceContext | undefined,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => (instance ? effect.pipe(Effect.provideService(InstanceRef, instance)) : effect)

const createWorktree = Effect.fn("TaskTool.createWorktree")(function* (
  instance: InstanceContext,
  workerID: WorkerID,
  description: string,
) {
  if (instance.project.vcs !== "git" || instance.worktree === "/") {
    return yield* Effect.fail(new Error('TaskTool isolation: "worktree" requires a git repository'))
  }
  const slug = `${workerID}-${description}`
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64)
  const branch = `opencode-agent-${slug}`
  const root = instance.worktree
  const base = path.join(root, ".opencode", "worktrees")
  const worktreePath = path.join(base, slug)
  const exists = yield* Effect.promise(() =>
    fs
      .stat(worktreePath)
      .then((stat) => stat.isDirectory())
      .catch(() => false),
  )
  if (!exists) {
    yield* Effect.promise(() => fs.mkdir(base, { recursive: true }))
    yield* git(root, ["worktree", "add", "-b", branch, worktreePath, "HEAD"]).pipe(
      Effect.catchCause(() => git(root, ["worktree", "add", worktreePath, branch])),
    )
  }
  return { root, path: worktreePath, branch } satisfies WorktreeInfo
})

const git = (cwd: string, args: string[]) =>
  Effect.promise(async () => {
    const proc = Bun.spawn(["git", ...args], {
      cwd,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: {
        ...process.env,
        GIT_TERMINAL_PROMPT: "0",
        GIT_ASKPASS: "",
      },
    })
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    if (code !== 0) {
      throw new Error(`git ${args.join(" ")} failed in ${cwd}: ${stderr.trim() || stdout.trim() || `exit ${code}`}`)
    }
    return stdout.trim()
  })
