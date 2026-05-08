import type { WorkerID } from "@/swarm/state"

export const PANE_COLORS = ["red", "blue", "green", "yellow", "magenta", "orange", "cyan", "pink"] as const
export type PaneColor = (typeof PANE_COLORS)[number]

export type PaneBackendType = "tmux" | "iterm2"
export type BackendPreference = "auto" | "in-process" | PaneBackendType

export type PaneID = string

export type CreatePaneResult = {
  paneID: PaneID
  isFirstPane: boolean
  useExternalSession?: boolean
  windowTarget?: string
}

export type PreparedWorkerPane = CreatePaneResult & {
  backend: PaneBackendType
  launch: () => Promise<void>
  kill: () => Promise<boolean>
}

export type PrepareWorkerPaneInput = {
  workerID: WorkerID
  cwd: string
  name: string
  description: string
  color?: PaneColor
  backend?: BackendPreference
  env?: NodeJS.ProcessEnv
}

export type PaneBackend = {
  readonly type: PaneBackendType
  readonly displayName: string
  readonly supportsHideShow: boolean
  isAvailable(): Promise<boolean>
  isRunningInside(): Promise<boolean>
  createPane(input: { name: string; color: PaneColor }): Promise<CreatePaneResult>
  sendCommandToPane(paneID: PaneID, command: string, useExternalSession?: boolean): Promise<void>
  killPane(paneID: PaneID, useExternalSession?: boolean): Promise<boolean>
  hidePane?(paneID: PaneID, useExternalSession?: boolean): Promise<boolean>
  showPane?(paneID: PaneID, target: string, useExternalSession?: boolean): Promise<boolean>
}
