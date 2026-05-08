import { Option, Effect } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID } from "@/session/schema"
import { Session } from "@/session/session"
import type { WorkerCompletion, WorkerSnapshot, WorkerStatus } from "./state"

export const notifyParent = Effect.fn("SwarmNotification.notifyParent")(function* (
  worker: WorkerSnapshot,
  completion: WorkerCompletion,
  input?: {
    status?: WorkerStatus | "idle"
    includeResultForCompleted?: boolean
  },
) {
  if (!worker.spec.model) return
  const sessions = Option.getOrUndefined(yield* Effect.serviceOption(Session.Service))
  if (!sessions) return

  const status = input?.status ?? (completion.status === "completed" ? "idle" : completion.status)
  const summary =
    status === "idle"
      ? `Subagent "${worker.spec.description}" is idle`
      : status === "completed"
        ? `Subagent "${worker.spec.description}" completed`
        : status === "failed"
          ? `Subagent "${worker.spec.description}" failed`
          : `Subagent "${worker.spec.description}" was cancelled`
  const body =
    status === "idle" && input?.includeResultForCompleted !== true
      ? ""
      : completion.status === "failed"
        ? completion.error
        : completion.text

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
      ...(worker.spec.remoteID ? { remoteID: worker.spec.remoteID } : {}),
      ...(worker.spec.remoteSessionURL ? { remoteSessionURL: worker.spec.remoteSessionURL } : {}),
    },
    text: [
      "<task-notification>",
      `<task-id>${xmlEscape(worker.spec.sessionID)}</task-id>`,
      `<worker-id>${xmlEscape(worker.spec.workerID)}</worker-id>`,
      worker.spec.name ? `<name>${xmlEscape(worker.spec.name)}</name>` : "",
      worker.spec.team ? `<team>${xmlEscape(worker.spec.team)}</team>` : "",
      worker.spec.outputPath ? `<output-file>${xmlEscape(worker.spec.outputPath)}</output-file>` : "",
      worker.spec.remoteID ? `<remote-id>${xmlEscape(worker.spec.remoteID)}</remote-id>` : "",
      worker.spec.remoteSessionURL ? `<remote-session-url>${xmlEscape(worker.spec.remoteSessionURL)}</remote-session-url>` : "",
      `<status>${xmlEscape(status)}</status>`,
      `<summary>${xmlEscape(summary)}</summary>`,
      body ? `<result>${xmlEscape(body)}</result>` : "",
      "</task-notification>",
    ]
      .filter((line) => line !== "")
      .join("\n"),
  } satisfies MessageV2.TextPart)
})

const xmlEscape = (value: string) =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;")
