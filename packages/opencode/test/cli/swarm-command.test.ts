import { describe, expect, test } from "bun:test"
import { SwarmCommand } from "@/cli/cmd/swarm"

describe("swarm cli command", () => {
  test("registers the swarm swarm supervision subcommands", () => {
    const commands: string[] = []
    const yargs = {
      command(command: { command?: string | readonly string[] }) {
        if (typeof command.command === "string") commands.push(command.command)
        else if (Array.isArray(command.command)) commands.push(...command.command)
        return this
      },
      demandCommand() {
        return this
      },
    }

    const builder = SwarmCommand.builder
    if (typeof builder !== "function") throw new Error("missing swarm command builder")
    builder(yargs as never)

    expect(SwarmCommand.command).toBe("swarm")
    expect(commands).toEqual([
      "list [session]",
      "inspect <target>",
      "send <target> <message>",
      "stop <target>",
      "pane <target> <action>",
      "teams [session]",
      "tasks <session>",
      "worker <worker>",
    ])
  })
})
