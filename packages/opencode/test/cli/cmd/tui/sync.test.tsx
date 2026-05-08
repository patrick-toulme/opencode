/** @jsxImportSource @opentui/solid */
import { describe, expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import { onMount } from "solid-js"
import { Global } from "@opencode-ai/core/global"
import { ArgsProvider } from "../../../../src/cli/cmd/tui/context/args"
import { ExitProvider } from "../../../../src/cli/cmd/tui/context/exit"
import { KVProvider, useKV } from "../../../../src/cli/cmd/tui/context/kv"
import { ProjectProvider } from "../../../../src/cli/cmd/tui/context/project"
import { SDKProvider, type EventSource } from "../../../../src/cli/cmd/tui/context/sdk"
import { SyncProvider, useSync } from "../../../../src/cli/cmd/tui/context/sync"
import { tmpdir } from "../../../fixture/fixture"

const worktree = "/tmp/opencode"
const directory = `${worktree}/packages/opencode`

async function wait(fn: () => boolean, timeout = 2000) {
  const start = Date.now()
  while (!fn()) {
    if (Date.now() - start > timeout) throw new Error("timed out waiting for condition")
    await Bun.sleep(10)
  }
}

function json(data: unknown) {
  return new Response(JSON.stringify(data), {
    headers: { "content-type": "application/json" },
  })
}

function eventSource(): EventSource {
  return {
    subscribe: async () => () => {},
  }
}

function controlledEventSource() {
  let handler: ((event: Parameters<Parameters<EventSource["subscribe"]>[0]>[0]) => void) | undefined
  let ready!: () => void
  const subscribed = new Promise<void>((resolve) => {
    ready = resolve
  })

  return {
    subscribed,
    source: {
      subscribe: async (next) => {
        handler = next
        ready()
        return () => {
          handler = undefined
        }
      },
    } satisfies EventSource,
    emit(payload: Parameters<Parameters<EventSource["subscribe"]>[0]>[0]["payload"]) {
      handler?.({
        directory,
        payload,
      })
    },
  }
}

function createFetch() {
  const session = [] as URL[]
  const fetch = (async (input: RequestInfo | URL) => {
    const url = new URL(input instanceof Request ? input.url : String(input))
    if (url.pathname === "/session") session.push(url)

    switch (url.pathname) {
      case "/agent":
      case "/command":
      case "/experimental/workspace":
      case "/experimental/workspace/status":
      case "/formatter":
      case "/lsp":
        return json([])
      case "/config":
      case "/experimental/resource":
      case "/mcp":
      case "/provider/auth":
      case "/session/status":
        return json({})
      case "/config/providers":
        return json({ providers: {}, default: {} })
      case "/experimental/console":
        return json({ consoleManagedProviders: [], switchableOrgCount: 0 })
      case "/path":
        return json({ home: "", state: "", config: "", worktree, directory })
      case "/project/current":
        return json({ id: "proj_test" })
      case "/provider":
        return json({ all: [], default: {}, connected: [] })
      case "/session":
        return json([])
      case "/swarm/ses_parent/worker":
      case "/swarm/ses_parent/team":
      case "/swarm/ses_parent/task":
        return json([])
      case "/vcs":
        return json({ branch: "main" })
    }

    throw new Error(`unexpected request: ${url.pathname}`)
  }) as typeof globalThis.fetch

  return { fetch, session }
}

async function mount(input: { events?: EventSource } = {}) {
  const calls = createFetch()
  let sync!: ReturnType<typeof useSync>
  let kv!: ReturnType<typeof useKV>
  let done!: () => void
  const ready = new Promise<void>((resolve) => {
    done = resolve
  })

  const app = await testRender(() => (
    <ArgsProvider>
      <ExitProvider>
        <KVProvider>
          <SDKProvider url="http://test" directory={directory} fetch={calls.fetch} events={input.events ?? eventSource()}>
            <ProjectProvider>
              <SyncProvider>
                <Probe
                  onReady={(ctx) => {
                    sync = ctx.sync
                    kv = ctx.kv
                    done()
                  }}
                />
              </SyncProvider>
            </ProjectProvider>
          </SDKProvider>
        </KVProvider>
      </ExitProvider>
    </ArgsProvider>
  ))

  await ready
  await wait(() => sync.status === "complete")
  return { app, kv, sync, session: calls.session }
}

function Probe(props: { onReady: (ctx: { kv: ReturnType<typeof useKV>; sync: ReturnType<typeof useSync> }) => void }) {
  const kv = useKV()
  const sync = useSync()

  onMount(() => {
    props.onReady({ kv, sync })
  })

  return <box />
}

describe("tui sync", () => {
  test("refresh scopes sessions by default and lists project sessions when disabled", async () => {
    const previous = Global.Path.state
    await using tmp = await tmpdir()
    Global.Path.state = tmp.path
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount()

    try {
      expect(kv.get("session_directory_filter_enabled", true)).toBe(true)
      expect(session.at(-1)?.searchParams.get("scope")).toBeNull()
      expect(session.at(-1)?.searchParams.get("path")).toBe("packages/opencode")

      kv.set("session_directory_filter_enabled", false)
      await sync.session.refresh()

      expect(session.at(-1)?.searchParams.get("scope")).toBe("project")
      expect(session.at(-1)?.searchParams.get("path")).toBeNull()
    } finally {
      app.renderer.destroy()
      Global.Path.state = previous
    }
  })

  test("tracks swarm worker and team events", async () => {
    const previous = Global.Path.state
    const events = controlledEventSource()
    await using tmp = await tmpdir()
    Global.Path.state = tmp.path
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, sync } = await mount({ events: events.source })

    try {
      await events.subscribed
      const team = {
        parentSessionID: "ses_parent",
        name: "blue",
        description: "parallel review",
        createdAt: 1,
        updatedAt: 1,
        workerIDs: [],
      }
      const worker = {
        spec: {
          workerID: "swa_worker",
          parentSessionID: "ses_parent",
          sessionID: "ses_child",
          agent: "general",
          name: "reviewer",
          team: "blue",
          prompt: "review",
          description: "review worker",
          contextStrategy: "auto",
          permissionStrategy: "bubble",
          executionStrategy: "persistent",
          backend: "in-process",
        },
        status: "running",
        startedAt: 2,
        updatedAt: 2,
      }

      events.emit({
        id: "evt_team_created",
        type: "swarm.team.created",
        properties: {
          parentSessionID: "ses_parent",
          name: "blue",
          team,
        },
      } as never)
      events.emit({
        id: "evt_worker_spawned",
        type: "swarm.worker.spawned",
        properties: {
          workerID: "swa_worker",
          parentSessionID: "ses_parent",
          sessionID: "ses_child",
          agent: "general",
          worker,
        },
      } as never)

      await wait(() => (sync.data.swarm.worker.ses_parent?.length ?? 0) === 1)
      expect(sync.data.swarm.team.ses_parent?.[0]?.name).toBe("blue")
      expect(sync.data.swarm.worker.ses_parent?.[0]?.spec.name).toBe("reviewer")

      const task = {
        id: "1",
        parentSessionID: "ses_parent",
        team: "blue",
        subject: "Run tests",
        description: "Run the test suite",
        status: "pending",
        owner: "reviewer",
        blocks: [],
        blockedBy: [],
        createdAt: 2,
        updatedAt: 2,
      }
      events.emit({
        id: "evt_task_created",
        type: "swarm.task.created",
        properties: {
          parentSessionID: "ses_parent",
          team: "blue",
          task,
        },
      } as never)

      await wait(() => (sync.data.swarm.task.ses_parent?.length ?? 0) === 1)
      expect(sync.data.swarm.task.ses_parent?.[0]?.subject).toBe("Run tests")

      events.emit({
        id: "evt_worker_permission",
        type: "swarm.worker.permission",
        properties: {
          workerID: "swa_worker",
          parentSessionID: "ses_parent",
          sessionID: "ses_child",
          permissionID: "per_test",
          worker: {
            ...worker,
            status: "waiting_permission",
            pendingPermissionID: "per_test",
            updatedAt: 3,
          },
        },
      } as never)

      await wait(() => sync.data.swarm.worker.ses_parent?.[0]?.status === "waiting_permission")
      expect(sync.data.swarm.worker.ses_parent?.[0]?.pendingPermissionID).toBe("per_test")

      events.emit({
        id: "evt_task_updated",
        type: "swarm.task.updated",
        properties: {
          parentSessionID: "ses_parent",
          team: "blue",
          task: {
            ...task,
            status: "completed",
            updatedAt: 3,
          },
        },
      } as never)

      await wait(() => sync.data.swarm.task.ses_parent?.[0]?.status === "completed")

      events.emit({
        id: "evt_team_deleted",
        type: "swarm.team.deleted",
        properties: {
          parentSessionID: "ses_parent",
          name: "blue",
          team: {
            ...team,
            workerIDs: ["swa_worker"],
            updatedAt: 4,
          },
        },
      } as never)

      await wait(() => (sync.data.swarm.team.ses_parent?.length ?? 0) === 0)

      events.emit({
        id: "evt_task_deleted",
        type: "swarm.task.deleted",
        properties: {
          parentSessionID: "ses_parent",
          team: "blue",
          task,
        },
      } as never)

      await wait(() => (sync.data.swarm.task.ses_parent?.length ?? 0) === 0)
    } finally {
      app.renderer.destroy()
      Global.Path.state = previous
    }
  })
})
