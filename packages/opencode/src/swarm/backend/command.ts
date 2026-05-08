import path from "path"
import type { WorkerID } from "@/swarm/state"
import { BACKEND_ENV, WORKER_COMMAND_ENV } from "./constants"
import { envAssignments, shellQuote } from "./shell"
import type { PaneBackendType } from "./types"

const FORWARDED_ENV = [
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "ANTHROPIC_BASE_URL",
  "OPENAI_API_KEY",
  "OPENAI_BASE_URL",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "AWS_PROFILE",
  "AWS_REGION",
  "GOOGLE_APPLICATION_CREDENTIALS",
  "GITHUB_TOKEN",
  "OPENCODE_CONFIG",
  "OPENCODE_CONFIG_DIR",
  "OPENCODE_CONFIG_CONTENT",
  "OPENCODE_DISABLE_MODELS_FETCH",
  "OPENCODE_DISABLE_AUTOUPDATE",
  "HTTPS_PROXY",
  "https_proxy",
  "HTTP_PROXY",
  "http_proxy",
  "NO_PROXY",
  "no_proxy",
  "SSL_CERT_FILE",
  "NODE_EXTRA_CA_CERTS",
  "REQUESTS_CA_BUNDLE",
  "CURL_CA_BUNDLE",
] as const

export type WorkerCommandInput = {
  workerID: WorkerID
  cwd: string
  backend: PaneBackendType
  env?: NodeJS.ProcessEnv
}

export function buildWorkerCommand(input: WorkerCommandInput) {
  const env = buildWorkerEnv(input.env ?? process.env, input.backend)
  const envPrefix = env.length ? `env ${env.join(" ")} ` : ""
  return [
    `cd ${shellQuote(input.cwd)}`,
    `${envPrefix}${workerCommandPrefix(input.env ?? process.env)} ${["swarm", "worker", input.workerID, "--backend", input.backend].map(shellQuote).join(" ")}`,
  ].join(" && ")
}

export function buildWorkerEnv(source: NodeJS.ProcessEnv, backend: PaneBackendType) {
  const next: Record<string, string | undefined> = {
    OPENCODE: "1",
    AGENT: "1",
    OPENCODE_SWARM_WORKER: "1",
    [BACKEND_ENV]: backend,
  }
  for (const key of FORWARDED_ENV) next[key] = source[key]
  for (const [key, value] of Object.entries(source)) {
    if (!key.startsWith("OPENCODE_")) continue
    if (next[key] !== undefined) continue
    next[key] = value
  }
  return envAssignments(next)
}

export function workerCommandPrefix(env: NodeJS.ProcessEnv = process.env) {
  const override = env[WORKER_COMMAND_ENV]
  if (override?.trim()) return override.trim()

  const executable = process.execPath
  const script = process.argv[1]
  const parts =
    script && path.isAbsolute(script) && script !== executable
      ? [executable, script]
      : [executable]
  return parts.map(shellQuote).join(" ")
}
