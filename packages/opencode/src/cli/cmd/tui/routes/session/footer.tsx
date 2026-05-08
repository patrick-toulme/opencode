import { createMemo, Match, onCleanup, onMount, Show, Switch } from "solid-js"
import { useTheme } from "../../context/theme"
import { useSync } from "../../context/sync"
import { useDirectory } from "../../context/directory"
import { useConnected } from "../../component/use-connected"
import { createStore } from "solid-js/store"
import { useRoute } from "../../context/route"

export function Footer() {
  const { theme } = useTheme()
  const sync = useSync()
  const route = useRoute()
  const mcp = createMemo(() => Object.values(sync.data.mcp).filter((x) => x.status === "connected").length)
  const mcpError = createMemo(() => Object.values(sync.data.mcp).some((x) => x.status === "failed"))
  const lsp = createMemo(() => Object.keys(sync.data.lsp))
  const permissions = createMemo(() => {
    if (route.data.type !== "session") return []
    return sync.data.permission[route.data.sessionID] ?? []
  })
  const workers = createMemo(() => {
    if (route.data.type !== "session") return []
    return sync.data.swarm.worker[route.data.sessionID] ?? []
  })
  const activeWorkers = createMemo(() =>
    workers().filter((worker) => !["completed", "cancelled", "failed", "interrupted"].includes(worker.status)),
  )
  const pendingWorkerPermissions = createMemo(
    () => activeWorkers().filter((worker) => worker.status === "waiting_permission").length,
  )
  const teamTasks = createMemo(() => {
    if (route.data.type !== "session") return []
    return sync.data.swarm.task[route.data.sessionID] ?? []
  })
  const openTeamTasks = createMemo(() => teamTasks().filter((task) => task.status !== "completed"))
  const directory = useDirectory()
  const connected = useConnected()

  const [store, setStore] = createStore({
    welcome: false,
  })

  onMount(() => {
    // Track all timeouts to ensure proper cleanup
    const timeouts: ReturnType<typeof setTimeout>[] = []

    function tick() {
      if (connected()) return
      if (!store.welcome) {
        setStore("welcome", true)
        timeouts.push(setTimeout(() => tick(), 5000))
        return
      }

      if (store.welcome) {
        setStore("welcome", false)
        timeouts.push(setTimeout(() => tick(), 10_000))
        return
      }
    }
    timeouts.push(setTimeout(() => tick(), 10_000))

    onCleanup(() => {
      timeouts.forEach(clearTimeout)
    })
  })

  return (
    <box flexDirection="row" justifyContent="space-between" gap={1} flexShrink={0}>
      <text fg={theme.textMuted}>{directory()}</text>
      <box gap={2} flexDirection="row" flexShrink={0}>
        <Switch>
          <Match when={store.welcome}>
            <text fg={theme.text}>
              Get started <span style={{ fg: theme.textMuted }}>/connect</span>
            </text>
          </Match>
          <Match when={connected()}>
            <Show when={permissions().length > 0}>
              <text fg={theme.warning}>
                <span style={{ fg: theme.warning }}>△</span> {permissions().length} Permission
                {permissions().length > 1 ? "s" : ""}
              </text>
            </Show>
            <Show when={workers().length > 0}>
              <text fg={pendingWorkerPermissions() > 0 ? theme.warning : theme.text}>
                <span style={{ fg: pendingWorkerPermissions() > 0 ? theme.warning : theme.success }}>◌</span>{" "}
                {activeWorkers().length}/{workers().length} Agents
              </text>
            </Show>
            <Show when={teamTasks().length > 0}>
              <text fg={theme.text}>
                <span style={{ fg: openTeamTasks().length > 0 ? theme.warning : theme.success }}>□</span>{" "}
                {openTeamTasks().length}/{teamTasks().length} Tasks
              </text>
            </Show>
            <Show when={workers().length > 0 || teamTasks().length > 0}>
              <text fg={theme.textMuted}>/swarm</text>
            </Show>
            <text fg={theme.text}>
              <span style={{ fg: lsp().length > 0 ? theme.success : theme.textMuted }}>•</span> {lsp().length} LSP
            </text>
            <Show when={mcp()}>
              <text fg={theme.text}>
                <Switch>
                  <Match when={mcpError()}>
                    <span style={{ fg: theme.error }}>⊙ </span>
                  </Match>
                  <Match when={true}>
                    <span style={{ fg: theme.success }}>⊙ </span>
                  </Match>
                </Switch>
                {mcp()} MCP
              </text>
            </Show>
            <text fg={theme.textMuted}>/status</text>
          </Match>
        </Switch>
      </box>
    </box>
  )
}
