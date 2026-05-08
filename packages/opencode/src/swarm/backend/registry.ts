import { Config } from "@/config/config"
import { Effect } from "effect"
import { BACKEND_ENV } from "./constants"
import { isInITerm2, isInsideTmux, isIt2Available, isTmuxAvailable } from "./detection"
import { ITermBackend } from "./iterm"
import { TmuxBackend } from "./tmux"
import type { BackendPreference, PaneBackend, PaneBackendType } from "./types"

export type BackendSelection =
  | { type: "in-process"; reason: string }
  | { type: "pane"; backend: PaneBackend; isNative: boolean }

export const resolvePreference = Effect.fn("SwarmBackend.resolvePreference")(function* (
  explicit?: BackendPreference,
) {
  const config = yield* Config.Service
  const cfg = yield* config.get()
  const raw = explicit ?? process.env[BACKEND_ENV] ?? cfg.experimental?.swarm_backend ?? "auto"
  return normalizePreference(raw)
})

export const select = Effect.fn("SwarmBackend.select")(function* (explicit?: BackendPreference) {
  if (explicit) return yield* Effect.promise(() => selectPromise(normalizePreference(explicit)))
  const preference = yield* resolvePreference(explicit)
  return yield* Effect.promise(() => selectPromise(preference))
})

export async function selectPromise(preference: BackendPreference = "auto"): Promise<BackendSelection> {
  if (preference === "in-process") return { type: "in-process", reason: "configured in-process" }

  if (preference === "tmux") {
    const backend = new TmuxBackend()
    if (!(await backend.isAvailable())) throw new Error("tmux swarm backend requested, but tmux is not available")
    return { type: "pane", backend, isNative: await backend.isRunningInside() }
  }

  if (preference === "iterm2") {
    const backend = new ITermBackend()
    if (!(await backend.isAvailable())) throw new Error("iTerm2 swarm backend requested, but it2 is not available")
    return { type: "pane", backend, isNative: true }
  }

  if (
    !process.stdout.isTTY &&
    process.env.OPENCODE_PROCESS_ROLE !== "worker" &&
    process.env.OPENCODE_SWARM_FORCE_PANE !== "1"
  ) {
    return { type: "in-process", reason: "non-interactive process" }
  }

  if (await isInsideTmux()) {
    const backend = new TmuxBackend()
    if (await backend.isAvailable()) return { type: "pane", backend, isNative: true }
  }

  if (isInITerm2() && (await isIt2Available())) {
    return { type: "pane", backend: new ITermBackend(), isNative: true }
  }

  if (await isTmuxAvailable()) {
    return { type: "pane", backend: new TmuxBackend(), isNative: false }
  }

  return { type: "in-process", reason: "no pane backend available" }
}

export function normalizePreference(value: unknown): BackendPreference {
  if (value === "tmux" || value === "iterm2" || value === "in-process" || value === "auto") return value
  return "auto"
}

export function backendByType(type: PaneBackendType): PaneBackend {
  return type === "tmux" ? new TmuxBackend() : new ITermBackend()
}

export * as SwarmBackendRegistry from "./registry"
