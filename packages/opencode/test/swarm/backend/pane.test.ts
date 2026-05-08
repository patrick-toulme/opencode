import { describe, expect, test } from "bun:test"
import { ITermBackend } from "@/swarm/backend/iterm"
import { TmuxBackend } from "@/swarm/backend/tmux"
import type { CommandRunner } from "@/swarm/backend/detection"

const ok = (stdout = "") => ({
  code: 0,
  stdout: Buffer.from(stdout),
  stderr: Buffer.alloc(0),
})

const fail = (stderr = "failed") => ({
  code: 1,
  stdout: Buffer.alloc(0),
  stderr: Buffer.from(stderr),
})

describe("swarm pane backends", () => {
  test("tmux external mode creates, styles, sends to, and kills panes on the swarm socket", async () => {
    const commands: string[][] = []
    const run: CommandRunner = async (cmd) => {
      commands.push(cmd)
      const joined = cmd.join(" ")
      if (joined.includes("has-session")) return fail("no session")
      if (joined.includes("new-session")) return ok("%1\n")
      if (joined.includes("list-panes")) return ok("%1\n")
      return ok()
    }
    const backend = new TmuxBackend(run)

    const pane = await backend.createPane({ name: "researcher", color: "cyan" })
    await backend.sendCommandToPane(pane.paneID, "opencode swarm worker swa_1", pane.useExternalSession)
    await backend.killPane(pane.paneID, pane.useExternalSession)

    expect(pane).toMatchObject({ paneID: "%1", isFirstPane: true, useExternalSession: true })
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm new-session -d -s opencode-swarm -n agents -P -F #{pane_id}")).toBe(true)
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm select-pane -t %1 -T researcher")).toBe(true)
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm send-keys -t %1 opencode swarm worker swa_1 Enter")).toBe(true)
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm kill-pane -t %1")).toBe(true)
  })

  test("tmux external mode splits an existing swarm window instead of reusing its first pane", async () => {
    const commands: string[][] = []
    const run: CommandRunner = async (cmd) => {
      commands.push(cmd)
      const joined = cmd.join(" ")
      if (joined.includes("has-session")) return ok()
      if (joined.includes("list-windows")) return ok("agents\n")
      if (joined.includes("list-panes")) return ok("%1\n")
      if (joined.includes("split-window")) return ok("%2\n")
      return ok()
    }
    const backend = new TmuxBackend(run)

    const pane = await backend.createPane({ name: "reviewer", color: "green" })

    expect(pane).toMatchObject({ paneID: "%2", isFirstPane: false, useExternalSession: true })
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm split-window -t %1 -v -P -F #{pane_id}")).toBe(true)
    expect(commands.some((cmd) => cmd.join(" ") === "tmux -L opencode-swarm select-pane -t %2 -T reviewer")).toBe(true)
  })

  test("iTerm2 creates a split, sends a command, and force-closes the pane", async () => {
    const commands: string[][] = []
    const run: CommandRunner = async (cmd) => {
      commands.push(cmd)
      if (cmd.join(" ") === "it2 session split -v") return ok("Created new pane: SESSION-1\n")
      return ok()
    }
    const backend = new ITermBackend(run)

    const pane = await backend.createPane({ name: "implementer", color: "green" })
    await backend.sendCommandToPane(pane.paneID, "opencode swarm worker swa_2")
    await backend.killPane(pane.paneID)

    expect(pane).toEqual({ paneID: "SESSION-1", isFirstPane: true, useExternalSession: false })
    expect(commands.map((cmd) => cmd.join(" "))).toContain("it2 session run -s SESSION-1 opencode swarm worker swa_2")
    expect(commands.map((cmd) => cmd.join(" "))).toContain("it2 session close -f -s SESSION-1")
  })
})
