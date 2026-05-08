import { Effect } from "effect"
import { buildWorkerCommand } from "./command"
import { selectPromise } from "./registry"
import { PANE_COLORS, type PaneColor, type PreparedWorkerPane, type PrepareWorkerPaneInput } from "./types"

let colorIndex = 0
const assignedColors = new Map<string, PaneColor>()

export const prepareWorkerPane = Effect.fn("SwarmBackend.prepareWorkerPane")(function* (
  input: PrepareWorkerPaneInput,
) {
  const selection = yield* Effect.promise(() => selectPromise(input.backend ?? "auto"))
  if (selection.type === "in-process") return undefined

  const color = input.color ?? assignColor(input.name)
  const pane = yield* Effect.promise(() => selection.backend.createPane({ name: input.name, color }))
  const command = buildWorkerCommand({
    workerID: input.workerID,
    cwd: input.cwd,
    backend: selection.backend.type,
    env: input.env,
  })
  return {
    ...pane,
    backend: selection.backend.type,
    launch: () => selection.backend.sendCommandToPane(pane.paneID, command, pane.useExternalSession),
    kill: () => selection.backend.killPane(pane.paneID, pane.useExternalSession),
  } satisfies PreparedWorkerPane
})

function assignColor(key: string) {
  const existing = assignedColors.get(key)
  if (existing) return existing
  const color = PANE_COLORS[colorIndex % PANE_COLORS.length]!
  assignedColors.set(key, color)
  colorIndex++
  return color
}

export function clearAssignedPaneColors() {
  assignedColors.clear()
  colorIndex = 0
}
