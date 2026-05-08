import fs from "fs/promises"
import path from "path"
import { Effect } from "effect"
import { Global } from "@opencode-ai/core/global"
import { Session } from "@/session/session"
import type { SessionID } from "@/session/schema"
import type { WorkerID } from "./state"

type WriteWorkerTranscriptInput = {
  workerID: WorkerID
  sessionID: SessionID
  outputPath?: string
  fallbackLastAssistantMessage?: string
}

export const writeWorkerTranscriptWithSession = (sessions: Session.Interface, input: WriteWorkerTranscriptInput) =>
  Effect.uninterruptible(
    Effect.gen(function* () {
      const messages = yield* sessions.messages({ sessionID: input.sessionID })
      const transcriptPath =
        input.outputPath ?? path.join(Global.Path.data, "subagent-transcripts", `${input.workerID}.jsonl`)
      const lines = messages.map((message) => JSON.stringify(message))
      const fallback = input.fallbackLastAssistantMessage?.trim()
      if (fallback && !lines.some((line) => line.includes(fallback))) {
        lines.push(
          JSON.stringify({
            type: "subagent-result",
            workerID: input.workerID,
            sessionID: input.sessionID,
            text: fallback,
          }),
        )
      }
      const body = lines.join("\n")
      yield* Effect.promise(async () => {
        await fs.mkdir(path.dirname(transcriptPath), { recursive: true })
        await fs.writeFile(transcriptPath, body ? `${body}\n` : "")
      })
      const lastAssistant = messages.findLast((message) => message.info.role === "assistant")
      const lastAssistantMessage =
        lastAssistant?.parts
          .map((part) => (part.type === "text" || part.type === "reasoning" ? part.text : ""))
          .join("\n")
          .trim() || fallback
      return {
        transcriptPath,
        ...(lastAssistantMessage ? { lastAssistantMessage } : {}),
      }
    }),
  )

export const writeWorkerTranscript = (input: WriteWorkerTranscriptInput) =>
  Effect.gen(function* () {
    const sessions = yield* Session.Service
    return yield* writeWorkerTranscriptWithSession(sessions, input)
  })
