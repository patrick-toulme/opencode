import { describe, expect, test } from "bun:test"
import { WorkerID } from "@/swarm/state"
import { buildWorkerCommand, buildWorkerEnv } from "@/swarm/backend/command"

describe("swarm backend worker command", () => {
  test("builds a hidden worker command with cwd, backend, and inherited env", () => {
    const workerID = WorkerID.ascending("swa_test")
    const command = buildWorkerCommand({
      workerID,
      cwd: "/tmp/project with spaces",
      backend: "tmux",
      env: {
        OPENCODE_SWARM_WORKER_COMMAND: "opencode-dev",
        OPENCODE_CONFIG_DIR: "/tmp/config dir",
        ANTHROPIC_API_KEY: "secret value",
      },
    })

    expect(command).toContain("cd '/tmp/project with spaces'")
    expect(command).toContain("env OPENCODE='1'")
    expect(command).toContain("OPENCODE_SWARM_WORKER='1'")
    expect(command).toContain("OPENCODE_SWARM_BACKEND='tmux'")
    expect(command).toContain("ANTHROPIC_API_KEY='secret value'")
    expect(command).toContain("opencode-dev 'swarm' 'worker' 'swa_test' '--backend' 'tmux'")
  })

  test("forwards opencode-prefixed environment variables", () => {
    const env = buildWorkerEnv(
      {
        OPENCODE_CUSTOM_FLAG: "enabled",
        OPENCODE_SWARM_BACKEND: "ignored",
        UNRELATED: "nope",
      },
      "iterm2",
    )

    expect(env).toContain("OPENCODE_CUSTOM_FLAG='enabled'")
    expect(env).toContain("OPENCODE_SWARM_BACKEND='iterm2'")
    expect(env.some((item) => item.startsWith("UNRELATED="))).toBe(false)
  })
})
