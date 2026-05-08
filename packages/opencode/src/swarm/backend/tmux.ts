import { Process } from "@/util/process"
import {
  HIDDEN_SESSION_NAME,
  SWARM_SESSION_NAME,
  SWARM_WINDOW_NAME,
  TMUX_COMMAND,
} from "./constants"
import {
  getLeaderPaneID,
  isInsideTmux,
  isTmuxAvailable,
  type CommandRunner,
} from "./detection"
import type { CreatePaneResult, PaneBackend, PaneColor, PaneID } from "./types"

let cachedLeaderWindowTarget: string | undefined
let paneCreationLock: Promise<void> = Promise.resolve()

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function withPaneLock<T>(fn: () => Promise<T>) {
  let release!: () => void
  const next = new Promise<void>((resolve) => {
    release = resolve
  })
  const previous = paneCreationLock
  paneCreationLock = next
  await previous
  try {
    return await fn()
  } finally {
    release()
  }
}

function colorName(color: PaneColor) {
  if (color === "orange") return "colour208"
  if (color === "pink") return "colour205"
  if (color === "magenta") return "magenta"
  return color
}

export class TmuxBackend implements PaneBackend {
  readonly type = "tmux" as const
  readonly displayName = "tmux"
  readonly supportsHideShow = true

  constructor(private readonly run: CommandRunner = (cmd) => Process.run(cmd, { nothrow: true })) {}

  isAvailable() {
    return isTmuxAvailable(this.run)
  }

  isRunningInside() {
    return isInsideTmux()
  }

  createPane(input: { name: string; color: PaneColor }): Promise<CreatePaneResult> {
    return withPaneLock(async () => {
      const inside = await this.isRunningInside()
      return inside ? this.createPaneWithLeader(input) : this.createPaneExternal(input)
    })
  }

  async sendCommandToPane(paneID: PaneID, command: string, useExternalSession = false) {
    const result = await this.tmux(["send-keys", "-t", paneID, command, "Enter"], useExternalSession)
    if (result.code !== 0) {
      throw new Error(`Failed to send command to tmux pane ${paneID}: ${result.stderr.toString().trim()}`)
    }
  }

  async killPane(paneID: PaneID, useExternalSession = false) {
    const result = await this.tmux(["kill-pane", "-t", paneID], useExternalSession)
    return result.code === 0
  }

  async hidePane(paneID: PaneID, useExternalSession = false) {
    await this.tmux(["new-session", "-d", "-s", HIDDEN_SESSION_NAME], useExternalSession)
    const result = await this.tmux(
      ["break-pane", "-d", "-s", paneID, "-t", `${HIDDEN_SESSION_NAME}:`],
      useExternalSession,
    )
    return result.code === 0
  }

  async showPane(paneID: PaneID, target: string, useExternalSession = false) {
    const result = await this.tmux(["join-pane", "-h", "-s", paneID, "-t", target], useExternalSession)
    if (result.code !== 0) return false
    await this.tmux(["select-layout", "-t", target, "main-vertical"], useExternalSession)
    return true
  }

  private tmux(args: string[], external = false) {
    return this.run([TMUX_COMMAND, ...(external ? ["-L", SWARM_SESSION_NAME] : []), ...args])
  }

  private async currentWindowTarget() {
    if (cachedLeaderWindowTarget) return cachedLeaderWindowTarget
    const leader = getLeaderPaneID()
    const args = ["display-message", ...(leader ? ["-t", leader] : []), "-p", "#{session_name}:#{window_index}"]
    const result = await this.tmux(args)
    if (result.code !== 0) return undefined
    cachedLeaderWindowTarget = result.stdout.toString().trim()
    return cachedLeaderWindowTarget
  }

  private async paneIDs(windowTarget: string, external = false) {
    const result = await this.tmux(["list-panes", "-t", windowTarget, "-F", "#{pane_id}"], external)
    if (result.code !== 0) return undefined
    return result.stdout.toString().trim().split(/\r?\n/).filter(Boolean)
  }

  private async createPaneWithLeader(input: { name: string; color: PaneColor }) {
    const leader = getLeaderPaneID()
    const windowTarget = await this.currentWindowTarget()
    if (!leader || !windowTarget) throw new Error("Could not determine current tmux pane/window")

    const panes = await this.paneIDs(windowTarget)
    if (!panes) throw new Error(`Could not list tmux panes for ${windowTarget}`)
    const isFirstPane = panes.length === 1
    const teammatePanes = panes.slice(1)
    const target = isFirstPane ? leader : (teammatePanes[Math.floor((teammatePanes.length - 1) / 2)] ?? teammatePanes.at(-1))
    const split = isFirstPane
      ? ["split-window", "-t", leader, "-h", "-l", "70%", "-P", "-F", "#{pane_id}"]
      : ["split-window", "-t", target!, teammatePanes.length % 2 === 1 ? "-v" : "-h", "-P", "-F", "#{pane_id}"]
    const result = await this.tmux(split)
    if (result.code !== 0) throw new Error(`Failed to create tmux pane: ${result.stderr.toString().trim()}`)

    const paneID = result.stdout.toString().trim()
    await this.stylePane(paneID, input.name, input.color, false)
    await this.rebalanceWithLeader(windowTarget)
    await sleep(200)
    return { paneID, isFirstPane, useExternalSession: false, windowTarget }
  }

  private async createPaneExternal(input: { name: string; color: PaneColor }) {
    const { windowTarget, paneID: firstPaneID, created } = await this.ensureExternalSession()
    const panes = await this.paneIDs(windowTarget, true)
    if (!panes) throw new Error(`Could not list tmux panes for ${windowTarget}`)

    const isFirstPane = created && panes.length === 1
    let paneID = firstPaneID
    if (!isFirstPane) {
      const target = panes[Math.floor((panes.length - 1) / 2)] ?? panes.at(-1)
      if (!target) throw new Error(`Could not find a tmux pane to split in ${windowTarget}`)
      const result = await this.tmux(
        ["split-window", "-t", target, panes.length % 2 === 1 ? "-v" : "-h", "-P", "-F", "#{pane_id}"],
        true,
      )
      if (result.code !== 0) throw new Error(`Failed to create tmux pane: ${result.stderr.toString().trim()}`)
      paneID = result.stdout.toString().trim()
    }

    await this.stylePane(paneID, input.name, input.color, true)
    await this.tmux(["select-layout", "-t", windowTarget, "tiled"], true)
    await sleep(200)
    return { paneID, isFirstPane, useExternalSession: true, windowTarget }
  }

  private async ensureExternalSession() {
    const windowTarget = `${SWARM_SESSION_NAME}:${SWARM_WINDOW_NAME}`
    const session = await this.tmux(["has-session", "-t", SWARM_SESSION_NAME], true)
    if (session.code !== 0) {
      const created = await this.tmux(
        ["new-session", "-d", "-s", SWARM_SESSION_NAME, "-n", SWARM_WINDOW_NAME, "-P", "-F", "#{pane_id}"],
        true,
      )
      if (created.code !== 0) {
        throw new Error(`Failed to create tmux swarm session: ${created.stderr.toString().trim()}`)
      }
      return { windowTarget, paneID: created.stdout.toString().trim(), created: true }
    }

    const windows = await this.tmux(["list-windows", "-t", SWARM_SESSION_NAME, "-F", "#{window_name}"], true)
    if (windows.stdout.toString().split(/\r?\n/).includes(SWARM_WINDOW_NAME)) {
      const panes = await this.paneIDs(windowTarget, true)
      return { windowTarget, paneID: panes?.[0] ?? "", created: false }
    }

    const created = await this.tmux(["new-window", "-t", SWARM_SESSION_NAME, "-n", SWARM_WINDOW_NAME, "-P", "-F", "#{pane_id}"], true)
    if (created.code !== 0) throw new Error(`Failed to create tmux swarm window: ${created.stderr.toString().trim()}`)
    return { windowTarget, paneID: created.stdout.toString().trim(), created: true }
  }

  private async stylePane(paneID: string, name: string, color: PaneColor, external: boolean) {
    const tmuxColor = colorName(color)
    await this.tmux(["select-pane", "-t", paneID, "-T", name], external)
    await this.tmux(["set-option", "-p", "-t", paneID, "pane-border-style", `fg=${tmuxColor}`], external)
    await this.tmux(["set-option", "-p", "-t", paneID, "pane-active-border-style", `fg=${tmuxColor}`], external)
    await this.tmux(["set-option", "-p", "-t", paneID, "pane-border-format", `#[fg=${tmuxColor},bold] #{pane_title} #[default]`], external)
  }

  private async rebalanceWithLeader(windowTarget: string) {
    const panes = await this.paneIDs(windowTarget)
    if (!panes || panes.length <= 2) return
    await this.tmux(["set-option", "-w", "-t", windowTarget, "pane-border-status", "top"])
    await this.tmux(["select-layout", "-t", windowTarget, "main-vertical"])
    if (panes[0]) await this.tmux(["resize-pane", "-t", panes[0], "-x", "30%"])
  }
}
