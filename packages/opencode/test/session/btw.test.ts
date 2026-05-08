import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import path from "path"
import { WithInstance } from "../../src/project/with-instance"
import { Session as SessionNs } from "@/session/session"
import { MessageV2 } from "../../src/session/message-v2"
import { MessageID, PartID, type SessionID } from "../../src/session/schema"
import * as Log from "@opencode-ai/core/util/log"

const root = path.join(__dirname, "../..")
void Log.init({ print: false })

function run<A, E>(fx: Effect.Effect<A, E, SessionNs.Service>) {
  return Effect.runPromise(fx.pipe(Effect.provide(SessionNs.defaultLayer)))
}

const svc = {
  create(input?: SessionNs.CreateInput) {
    return run(SessionNs.Service.use((s) => s.create(input)))
  },
  remove(id: SessionID) {
    return run(SessionNs.Service.use((s) => s.remove(id)))
  },
  updateMessage<T extends MessageV2.Info>(msg: T) {
    return run(SessionNs.Service.use((s) => s.updateMessage(msg)))
  },
  updatePart<T extends MessageV2.Part>(part: T) {
    return run(SessionNs.Service.use((s) => s.updatePart(part)))
  },
  btw(input: { sessionID: SessionID }) {
    return run(SessionNs.Service.use((s) => s.btw(input)))
  },
  get(id: SessionID) {
    return run(SessionNs.Service.use((s) => s.get(id)))
  },
}

async function addUser(sessionID: SessionID, text: string) {
  const id = MessageID.ascending()
  await svc.updateMessage({
    id,
    sessionID,
    role: "user",
    time: { created: Date.now() },
    agent: "test",
    model: { providerID: "test", modelID: "test" },
    tools: {},
    mode: "",
  } as unknown as MessageV2.Info)
  await svc.updatePart({
    id: PartID.ascending(),
    sessionID,
    messageID: id,
    type: "text",
    text,
  })
  return id
}

describe("Session.btw", () => {
  test("inherits parent context, sets parentID, prefixes title", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const parent = await svc.create({ title: "design refactor" })
        await addUser(parent.id, "first")
        await addUser(parent.id, "second")

        const child = await svc.btw({ sessionID: parent.id })
        expect(child.parentID).toBe(parent.id)
        expect(child.title.startsWith("btw: ")).toBe(true)
        expect(child.id).not.toBe(parent.id)

        const childMsgs = Array.from(MessageV2.stream(child.id))
        const parentMsgs = Array.from(MessageV2.stream(parent.id))
        expect(childMsgs).toHaveLength(parentMsgs.length)

        const childText = childMsgs
          .flatMap((m) => m.parts)
          .filter((p) => p.type === "text")
          .map((p) => (p as { text: string }).text)
          .sort()
        expect(childText).toEqual(["first", "second"])

        await svc.remove(child.id)
        await svc.remove(parent.id)
      },
    })
  })

  test("does not mutate the parent session when child gets new messages", async () => {
    await WithInstance.provide({
      directory: root,
      fn: async () => {
        const parent = await svc.create({})
        await addUser(parent.id, "shared")
        const beforeParentCount = Array.from(MessageV2.stream(parent.id)).length

        const child = await svc.btw({ sessionID: parent.id })
        await addUser(child.id, "child-only")

        const afterParentCount = Array.from(MessageV2.stream(parent.id)).length
        expect(afterParentCount).toBe(beforeParentCount)

        const childMsgs = Array.from(MessageV2.stream(child.id))
        expect(childMsgs.length).toBe(beforeParentCount + 1)

        await svc.remove(child.id)
        await svc.remove(parent.id)
      },
    })
  })
})
