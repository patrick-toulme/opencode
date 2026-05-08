import { Process } from "@/util/process"
import { IT2_COMMAND } from "./constants"
import {
  getLeaderITermSessionID,
  isInITerm2,
  isIt2Available,
  type CommandRunner,
} from "./detection"
import type { CreatePaneResult, PaneBackend, PaneColor, PaneID } from "./types"

let firstPaneUsed = false
const teammateSessions: string[] = []
let paneCreationLock: Promise<void> = Promise.resolve()

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

function parseSplitOutput(output: string) {
  return output.match(/Created new pane:\s*(.+)/)?.[1]?.trim() ?? ""
}

export class ITermBackend implements PaneBackend {
  readonly type = "iterm2" as const
  readonly displayName = "iTerm2"
  readonly supportsHideShow = false

  constructor(private readonly run: CommandRunner = (cmd) => Process.run(cmd, { nothrow: true })) {}

  async isAvailable() {
    return isInITerm2() && (await isIt2Available(this.run))
  }

  async isRunningInside() {
    return isInITerm2()
  }

  createPane(_input: { name: string; color: PaneColor }): Promise<CreatePaneResult> {
    return withPaneLock(async () => {
      while (true) {
        const first = !firstPaneUsed
        const leader = getLeaderITermSessionID()
        const target = first ? leader : teammateSessions.at(-1)
        const args = first
          ? ["session", "split", "-v", ...(target ? ["-s", target] : [])]
          : ["session", "split", ...(target ? ["-s", target] : [])]
        const result = await this.it2(args)

        if (result.code !== 0 && target && !first) {
          const list = await this.it2(["session", "list"])
          if (list.code === 0 && !list.stdout.toString().includes(target)) {
            teammateSessions.pop()
            if (teammateSessions.length === 0) firstPaneUsed = false
            continue
          }
        }

        if (result.code !== 0) {
          throw new Error(`Failed to create iTerm2 split pane: ${result.stderr.toString().trim()}`)
        }

        const paneID = parseSplitOutput(result.stdout.toString())
        if (!paneID) throw new Error(`Failed to parse iTerm2 split output: ${result.stdout.toString().trim()}`)
        firstPaneUsed = true
        teammateSessions.push(paneID)
        return { paneID, isFirstPane: first, useExternalSession: false }
      }
    })
  }

  async sendCommandToPane(paneID: PaneID, command: string) {
    const args = paneID ? ["session", "run", "-s", paneID, command] : ["session", "run", command]
    const result = await this.it2(args)
    if (result.code !== 0) {
      throw new Error(`Failed to send command to iTerm2 pane ${paneID}: ${result.stderr.toString().trim()}`)
    }
  }

  async killPane(paneID: PaneID) {
    const result = await this.it2(["session", "close", "-f", "-s", paneID])
    const index = teammateSessions.indexOf(paneID)
    if (index >= 0) teammateSessions.splice(index, 1)
    if (teammateSessions.length === 0) firstPaneUsed = false
    return result.code === 0
  }

  async hidePane() {
    return false
  }

  async showPane() {
    return false
  }

  private it2(args: string[]) {
    return this.run([IT2_COMMAND, ...args])
  }
}
