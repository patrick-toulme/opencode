import { Process } from "@/util/process"
import { IT2_COMMAND, TMUX_COMMAND } from "./constants"

const ORIGINAL_TMUX = process.env.TMUX
const ORIGINAL_TMUX_PANE = process.env.TMUX_PANE

export type CommandRunner = (cmd: string[]) => Promise<{ code: number; stdout: Buffer; stderr: Buffer }>

const defaultRunner: CommandRunner = (cmd) => Process.run(cmd, { nothrow: true })

export function isInsideTmuxSync() {
  return Boolean(ORIGINAL_TMUX)
}

export async function isInsideTmux() {
  return isInsideTmuxSync()
}

export function getLeaderPaneID() {
  return ORIGINAL_TMUX_PANE || undefined
}

export async function isTmuxAvailable(run: CommandRunner = defaultRunner) {
  const result = await run([TMUX_COMMAND, "-V"])
  return result.code === 0
}

export function isInITerm2() {
  return process.env.TERM_PROGRAM === "iTerm.app" || Boolean(process.env.ITERM_SESSION_ID)
}

export function getLeaderITermSessionID() {
  const session = process.env.ITERM_SESSION_ID
  if (!session) return undefined
  const index = session.indexOf(":")
  return index >= 0 ? session.slice(index + 1) : session
}

export async function isIt2Available(run: CommandRunner = defaultRunner) {
  const result = await run([IT2_COMMAND, "session", "list"])
  return result.code === 0
}
