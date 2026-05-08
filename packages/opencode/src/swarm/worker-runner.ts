import { Cause, Effect } from "effect"
import { Plugin } from "@/plugin"
import { Session } from "@/session/session"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID } from "@/session/schema"
import { SessionPrompt } from "@/session/prompt"
import { errorMessage } from "@/util/error"
import { SwarmRuntime } from "./runtime"
import type { WorkerCompletion, WorkerID, WorkerSnapshot } from "./state"
import { workerToolsDisabled } from "./worker-tools"
import { writeWorkerTranscriptWithSession } from "./transcript"

export const runExternalWorker = Effect.fn("SwarmWorkerRunner.runExternalWorker")(function* (workerID: WorkerID) {
  const swarm = yield* SwarmRuntime.Service
  const promptSvc = yield* SessionPrompt.Service
  const sessions = yield* Session.Service
  const worker = yield* swarm.get(workerID)
  if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${workerID}`))

  let prompt = initialPrompt(worker)
  let nextMessageID = MessageID.ascending()
  let planApproved = !worker.spec.planModeRequired

  const runLoop = Effect.gen(function* () {
    while (true) {
      const latest = yield* swarm.get(workerID)
      if (latest?.status === "cancelled") {
        const completion: WorkerCompletion = {
          status: "cancelled",
          text: latest.result?.text ?? "cancelled",
        }
        yield* runStopHook(sessions, latest, completion, false)
        yield* notifyParent(latest, completion)
        return completion
      }

      const completion = yield* runPrompt(promptSvc, worker, prompt, nextMessageID, planApproved).pipe(
        Effect.catchCause((cause) =>
          Effect.succeed(
            Cause.hasInterruptsOnly(cause)
              ? ({ status: "cancelled", text: "cancelled" } satisfies WorkerCompletion)
              : ({ status: "failed", error: errorMessage(Cause.squash(cause)) } satisfies WorkerCompletion),
          ),
        ),
      )
      yield* writeTranscript(sessions, worker, completion.status === "failed" ? completion.error : completion.text).pipe(
        Effect.ignore,
      )
      const afterTurn = (yield* swarm.get(workerID)) ?? worker

      if (afterTurn.status === "cancelled") {
        const cancelled: WorkerCompletion = {
          status: "cancelled",
          text: afterTurn.result?.text ?? "cancelled",
        }
        yield* runStopHook(sessions, afterTurn, cancelled, false)
        yield* notifyParent(afterTurn, cancelled)
        return cancelled
      }

      if (completion.status === "completed") {
        const idleHookMessage = yield* runIdleHook(afterTurn, completion)
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
      yield* notifyParent(afterTurn, completion)

      if (completion.status !== "completed") {
        yield* runStopHook(sessions, afterTurn, completion, false)
        return completion
      }

      const input = yield* swarm.awaitInput(workerID)
      if (worker.spec.planModeRequired) {
        const response = parsePlanApprovalResponse(input.message)
        if (response?.approve === true) planApproved = true
        else if (response?.approve === false) planApproved = false
      }
      prompt = input.message
      nextMessageID = MessageID.ascending()
    }
  })

  return yield* runLoop.pipe(Effect.ensuring(writeTranscript(sessions, worker).pipe(Effect.ignore)))
})

function runPrompt(
  promptSvc: SessionPrompt.Interface,
  worker: WorkerSnapshot,
  prompt: string,
  messageID: MessageID,
  planApproved: boolean,
) {
  return Effect.gen(function* () {
    const parts = yield* promptSvc.resolvePromptParts(prompt)
    const result = yield* promptSvc.prompt({
      messageID,
      sessionID: worker.spec.sessionID,
      model: worker.spec.model,
      agent: worker.spec.agent,
      tools: workerToolsDisabled({ planModeRequired: worker.spec.planModeRequired === true, planApproved }),
      persistToolPermissions: false,
      parts,
    })
    const text = result.parts.findLast((item) => item.type === "text")?.text ?? ""
    return { status: "completed", text } satisfies WorkerCompletion
  })
}

function initialPrompt(worker: WorkerSnapshot) {
  if (worker.spec.prompt.includes("worker_id:") || worker.spec.prompt.includes("<fork-boilerplate>")) {
    return worker.spec.prompt
  }
  return [
    "<system-reminder>",
    worker.spec.fork
      ? "You are a long-lived background fork running as a teammate."
      : "You are a long-lived background subagent running as a teammate.",
    `worker_id: ${worker.spec.workerID}`,
    worker.spec.name ? `name: ${worker.spec.name}` : "",
    worker.spec.team ? `team: ${worker.spec.team}` : "",
    "",
    "Communication rules:",
    "- You are a worker, not the lead. Do the assigned generic work directly: inspect, edit, run commands, test, and report as requested by the task prompt.",
    "- Do not create or update session goals, create/delete teams, manage other subagents, read other subagent transcripts, or spawn nested subagents. If the assignment needs more parallelism, ask the lead with send_message.",
    "- If inherited context includes a /goal command or prior swarm setup, treat that as lead-session background only. Do not re-run it.",
    "- Use send_message to communicate results, blockers, or questions to the lead or teammates.",
    "- Use list_team_tasks, get_task, create_task, and update_task to coordinate shared team work.",
    "- If blocked, leave the task in_progress, create or update the blocking task, and notify the lead with send_message.",
    worker.spec.planModeRequired
      ? "- Plan approval gates mutating work only. You may perform read-only inspection without approval. If this assignment is read-only and requires no mutations, complete it without requesting approval. Before modifying files or running shell commands, send send_message(to=\"team-lead\", message={type:\"plan_approval_request\", plan:\"...\"}) and wait for an approving plan_approval_response."
      : "",
    "</system-reminder>",
    "",
    worker.spec.prompt,
  ]
    .filter((line) => line !== "")
    .join("\n")
}

const notifyParent = (worker: WorkerSnapshot, completion: WorkerCompletion) =>
  Effect.gen(function* () {
    if (!worker.spec.model) return
    const sessions = yield* Session.Service
    const status = completion.status === "completed" ? "idle" : completion.status
    const summary =
      status === "idle"
        ? `Subagent "${worker.spec.description}" is idle`
        : status === "failed"
          ? `Subagent "${worker.spec.description}" failed`
          : `Subagent "${worker.spec.description}" was cancelled`
    const body = status === "idle" ? "" : completion.status === "failed" ? completion.error : completion.text
    const message = yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: worker.spec.parentSessionID,
      agent: worker.spec.agent,
      model: worker.spec.model,
      time: { created: Date.now() },
    })
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: message.id,
      sessionID: worker.spec.parentSessionID,
      type: "text",
      synthetic: true,
      metadata: {
        kind: "task-notification",
        taskID: worker.spec.sessionID,
        workerID: worker.spec.workerID,
        ...(worker.spec.team ? { team: worker.spec.team } : {}),
        status,
        ...(worker.spec.outputPath ? { outputPath: worker.spec.outputPath } : {}),
      },
      text: [
        "<task-notification>",
        `<task-id>${xmlEscape(worker.spec.sessionID)}</task-id>`,
        `<worker-id>${xmlEscape(worker.spec.workerID)}</worker-id>`,
        worker.spec.name ? `<name>${xmlEscape(worker.spec.name)}</name>` : "",
        worker.spec.team ? `<team>${xmlEscape(worker.spec.team)}</team>` : "",
        worker.spec.outputPath ? `<output-file>${xmlEscape(worker.spec.outputPath)}</output-file>` : "",
        `<status>${xmlEscape(status)}</status>`,
        `<summary>${xmlEscape(summary)}</summary>`,
        body ? `<result>${xmlEscape(body)}</result>` : "",
        "</task-notification>",
      ]
        .filter((line) => line !== "")
        .join("\n"),
    } satisfies MessageV2.TextPart)
  })

const runIdleHook = (worker: WorkerSnapshot, completion: Extract<WorkerCompletion, { status: "completed" }>) =>
  Effect.gen(function* () {
    const plugin = yield* Plugin.Service
    const output: { continue: boolean; message?: string } = { continue: true }
    yield* plugin.trigger(
      "swarm.teammate.idle",
      {
        workerID: worker.spec.workerID,
        sessionID: worker.spec.sessionID,
        parentSessionID: worker.spec.parentSessionID,
        ...(worker.spec.name ? { teammateName: worker.spec.name } : {}),
        ...(worker.spec.team ? { teamName: worker.spec.team } : {}),
        summary: completion.text,
      },
      output,
    )
    if (output.continue !== false) return undefined
    return output.message?.trim() || "TeammateIdle hook requested that this subagent continue working."
  })

const runStopHook = (
  sessions: Session.Interface,
  worker: WorkerSnapshot,
  completion: WorkerCompletion,
  stopHookActive: boolean,
) =>
  Effect.gen(function* () {
    const plugin = yield* Plugin.Service
    const transcript = yield* writeTranscript(sessions, worker, completion.status === "failed" ? completion.error : completion.text)
    const output: { continue: boolean; message?: string } = { continue: true }
    yield* plugin.trigger(
      "swarm.subagent.stop",
      {
        workerID: worker.spec.workerID,
        sessionID: worker.spec.sessionID,
        parentSessionID: worker.spec.parentSessionID,
        agentType: worker.spec.agent,
        status: completion.status,
        stopHookActive,
        transcriptPath: transcript.transcriptPath,
        ...(transcript.lastAssistantMessage ? { lastAssistantMessage: transcript.lastAssistantMessage } : {}),
        ...(worker.spec.name ? { teammateName: worker.spec.name } : {}),
        ...(worker.spec.team ? { teamName: worker.spec.team } : {}),
      },
      output,
    )
    if (completion.status !== "completed" || output.continue !== false) return undefined
    return output.message?.trim() || "SubagentStop hook requested that this subagent continue working."
  })

const writeTranscript = (
  sessions: Session.Interface,
  worker: WorkerSnapshot,
  fallbackLastAssistantMessage?: string,
) =>
  writeWorkerTranscriptWithSession(sessions, {
    workerID: worker.spec.workerID,
    sessionID: worker.spec.sessionID,
    outputPath: worker.spec.outputPath,
    fallbackLastAssistantMessage,
  })

const parsePlanApprovalResponse = (message: string): { approve: boolean } | undefined => {
  if (!message.includes("plan_approval_response")) return undefined
  const json = message.match(/\{[\s\S]*"type"\s*:\s*"plan_approval_response"[\s\S]*\}/)
  if (json) {
    try {
      const parsed = JSON.parse(json[0]) as { approve?: unknown; approved?: unknown }
      const approval = typeof parsed.approve === "boolean" ? parsed.approve : parsed.approved
      if (typeof approval === "boolean") return { approve: approval }
    } catch {
      // Fall through to XML-ish parsing.
    }
  }
  const approve = message.match(/<approve>([\s\S]*?)<\/approve>/)?.[1]
  if (approve !== "true" && approve !== "false") return undefined
  return { approve: approve === "true" }
}

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")
