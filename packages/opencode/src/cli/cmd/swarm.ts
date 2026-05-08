import type { Argv } from "yargs"
import { Effect } from "effect"
import { EOL } from "os"
import { cmd } from "./cmd"
import { effectCmd, fail } from "../effect-cmd"
import { SessionID } from "@/session/schema"
import { SwarmRuntime } from "@/swarm/runtime"
import { WorkerID, type TeamSnapshot, type TeamTaskState, type WorkerCompletion, type WorkerSnapshot } from "@/swarm/state"
import { runExternalWorker } from "@/swarm/worker-runner"
import { Locale } from "@/util/locale"

type Format = "table" | "json"

type SessionArg = {
  session?: string
}

type FormatArg = {
  format: Format
}

const TERMINAL = new Set(["completed", "cancelled", "failed", "interrupted"])

export const SwarmCommand = cmd({
  command: "swarm",
  describe: "inspect and control subagent swarms",
  builder: (yargs: Argv) =>
    yargs
      .command(SwarmListCommand)
      .command(SwarmInspectCommand)
      .command(SwarmSendCommand)
      .command(SwarmStopCommand)
      .command(SwarmPaneCommand)
      .command(SwarmTeamsCommand)
      .command(SwarmTasksCommand)
      .command(SwarmWorkerCommand)
      .demandCommand(),
  async handler() {},
})

const SwarmWorkerCommand = effectCmd({
  command: "worker <worker>",
  describe: false,
  builder: (yargs: Argv) =>
    yargs
      .positional("worker", {
        type: "string",
        demandOption: true,
        describe: "worker id to adopt",
      })
      .option("backend", {
        type: "string",
        choices: ["tmux", "iterm2"] as const,
        describe: "pane backend that launched this worker",
      }),
  handler: Effect.fn("Cli.swarm.worker")(function* (args: { worker: string; backend?: "tmux" | "iterm2" }) {
    const swarm = yield* SwarmRuntime.Service
    const workerID = WorkerID.ascending(args.worker)
    const result = yield* swarm
      .adopt({
        workerID,
        wait: true,
        run: runExternalWorker(workerID).pipe(Effect.orDie) as Effect.Effect<WorkerCompletion>,
      })
      .pipe(Effect.catch((error) => fail(error instanceof Error ? error.message : String(error))))
    if (result.completion?.status === "failed") {
      return yield* fail(result.completion.error)
    }
  }),
})

const SwarmListCommand = effectCmd({
  command: "list [session]",
  describe: "list subagent workers",
  builder: (yargs: Argv) =>
    yargs
      .positional("session", {
        type: "string",
        describe: "parent session ID to scope workers",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.list")(function* (args: { session?: string } & FormatArg) {
    const swarm = yield* SwarmRuntime.Service
    const workers = yield* swarm.list(args.session ? SessionID.make(args.session) : undefined)
    print(args.format, workers, formatWorkerTable)
  }),
})

const SwarmInspectCommand = effectCmd({
  command: "inspect <target>",
  describe: "inspect a subagent by worker id, session id, or launch name",
  builder: (yargs: Argv) =>
    yargs
      .positional("target", {
        type: "string",
        demandOption: true,
        describe: "worker id, session id, or launch name",
      })
      .option("session", {
        type: "string",
        describe: "parent session ID for resolving launch names",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.inspect")(function* (args: { target: string } & SessionArg & FormatArg) {
    const swarm = yield* SwarmRuntime.Service
    const worker = yield* swarm.resolve({
      to: args.target,
      ...(args.session ? { parentSessionID: SessionID.make(args.session) } : {}),
    })
    if (!worker) return yield* fail(`No subagent found for: ${args.target}`)
    print(args.format, worker, (item) => formatWorkerDetail(item))
  }),
})

const SwarmSendCommand = effectCmd({
  command: "send <target> <message>",
  describe: "send a message to an idle or running subagent",
  builder: (yargs: Argv) =>
    yargs
      .positional("target", {
        type: "string",
        demandOption: true,
        describe: "worker id, session id, or launch name",
      })
      .positional("message", {
        type: "string",
        demandOption: true,
        describe: "message to deliver",
      })
      .option("session", {
        type: "string",
        describe: "parent session ID for resolving launch names",
      })
      .option("summary", {
        type: "string",
        describe: "short progress summary",
      })
      .option("from", {
        type: "string",
        default: "cli",
        describe: "sender label",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.send")(function* (
    args: { target: string; message: string; summary?: string; from: string } & SessionArg & FormatArg,
  ) {
    const swarm = yield* SwarmRuntime.Service
    const input = yield* swarm
      .sendInput({
        to: args.target,
        message: args.message,
        summary: args.summary,
        from: args.from,
        ...(args.session ? { parentSessionID: SessionID.make(args.session) } : {}),
      })
      .pipe(Effect.catch((error) => fail(error instanceof Error ? error.message : String(error))))
    print(args.format, { inputID: input.id }, (item) => `Queued input ${item.inputID}`)
  }),
})

const SwarmStopCommand = effectCmd({
  command: "stop <target>",
  describe: "ask a subagent to stop after its current turn",
  builder: (yargs: Argv) =>
    yargs
      .positional("target", {
        type: "string",
        demandOption: true,
        describe: "worker id, session id, or launch name",
      })
      .option("session", {
        type: "string",
        describe: "parent session ID for resolving launch names",
      })
      .option("reason", {
        type: "string",
        describe: "optional stop reason",
      })
      .option("force", {
        type: "boolean",
        default: false,
        describe: "force-cancel instead of graceful stop",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.stop")(function* (
    args: { target: string; reason?: string; force: boolean } & SessionArg & FormatArg,
  ) {
    const swarm = yield* SwarmRuntime.Service
    const worker = yield* swarm.resolve({
      to: args.target,
      ...(args.session ? { parentSessionID: SessionID.make(args.session) } : {}),
    })
    if (!worker) return yield* fail(`No subagent found for: ${args.target}`)
    if (args.force) yield* swarm.cancel(worker.spec.workerID)
    else yield* swarm.stopAfterCurrentTurn(worker.spec.workerID, args.reason)
    const stopped = (yield* swarm.get(worker.spec.workerID)) ?? worker
    print(args.format, stopped, (item) => formatWorkerDetail(item))
  }),
})

const SwarmPaneCommand = effectCmd({
  command: "pane <target> <action>",
  describe: "hide or show an external subagent terminal pane",
  builder: (yargs: Argv) =>
    yargs
      .positional("target", {
        type: "string",
        demandOption: true,
        describe: "worker id, session id, or launch name",
      })
      .positional("action", {
        type: "string",
        choices: ["hide", "show"] as const,
        demandOption: true,
        describe: "pane visibility action",
      })
      .option("session", {
        type: "string",
        describe: "parent session ID for resolving launch names",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.pane")(function* (
    args: { target: string; action: "hide" | "show" } & SessionArg & FormatArg,
  ) {
    const swarm = yield* SwarmRuntime.Service
    const worker = yield* swarm.resolve({
      to: args.target,
      ...(args.session ? { parentSessionID: SessionID.make(args.session) } : {}),
    })
    if (!worker) return yield* fail(`No subagent found for: ${args.target}`)
    const updated = yield* swarm
      .controlPane(worker.spec.workerID, args.action)
      .pipe(Effect.catch((error) => fail(error instanceof Error ? error.message : String(error))))
    print(args.format, updated, (item) => formatWorkerDetail(item))
  }),
})

const SwarmTeamsCommand = effectCmd({
  command: "teams [session]",
  describe: "list subagent teams",
  builder: (yargs: Argv) =>
    yargs
      .positional("session", {
        type: "string",
        describe: "parent session ID to scope teams",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.teams")(function* (args: { session?: string } & FormatArg) {
    const swarm = yield* SwarmRuntime.Service
    const teams = yield* swarm.listTeams(args.session ? SessionID.make(args.session) : undefined)
    print(args.format, teams, formatTeamTable)
  }),
})

const SwarmTasksCommand = effectCmd({
  command: "tasks <session>",
  describe: "list shared team-board tasks",
  builder: (yargs: Argv) =>
    yargs
      .positional("session", {
        type: "string",
        demandOption: true,
        describe: "parent session ID",
      })
      .option("team", {
        type: "string",
        describe: "team board to inspect; omit for the session board",
      })
      .option("status", {
        type: "string",
        choices: ["pending", "in_progress", "completed"] as const,
      })
      .option("owner", {
        type: "string",
        describe: "filter by task owner",
      })
      .option("format", {
        type: "string",
        choices: ["table", "json"] as const,
        default: "table" as const,
      }),
  handler: Effect.fn("Cli.swarm.tasks")(function* (
    args: { session: string; team?: string; status?: string; owner?: string } & FormatArg,
  ) {
    const swarm = yield* SwarmRuntime.Service
    const tasks = (yield* swarm.listTeamTasks({ parentSessionID: SessionID.make(args.session), team: args.team })).filter(
      (task) => (!args.status || task.status === args.status) && (!args.owner || task.owner === args.owner),
    )
    print(args.format, tasks, formatTaskTable)
  }),
})

function print<T>(format: Format, value: T, render: (value: T) => string) {
  if (format === "json") {
    console.log(JSON.stringify(value, null, 2))
    return
  }
  const output = render(value)
  if (output) console.log(output)
}

function formatWorkerTable(workers: WorkerSnapshot[]) {
  if (workers.length === 0) return "No subagents."
  const rows = workers
    .toSorted((a, b) => b.updatedAt - a.updatedAt || a.spec.workerID.localeCompare(b.spec.workerID))
    .map((worker) => [
      worker.spec.workerID,
      worker.spec.name ?? "",
      worker.spec.team ?? "",
      worker.status,
      TERMINAL.has(worker.status) ? "" : (worker.currentTool?.name ?? ""),
      Locale.truncate(worker.lastProgress ?? workerResult(worker) ?? worker.spec.description, 48),
    ])
  return table(["Worker", "Name", "Team", "Status", "Tool", "Progress"], rows)
}

function formatWorkerDetail(worker: WorkerSnapshot) {
  return [
    `worker_id: ${worker.spec.workerID}`,
    `session_id: ${worker.spec.sessionID}`,
    `parent_session_id: ${worker.spec.parentSessionID}`,
    `agent: ${worker.spec.agent}`,
    worker.spec.name ? `name: ${worker.spec.name}` : "",
    worker.spec.team ? `team: ${worker.spec.team}` : "",
    worker.spec.outputPath ? `output_path: ${worker.spec.outputPath}` : "",
    `status: ${worker.status}`,
    worker.currentTool ? `current_tool: ${formatTool(worker.currentTool)}` : "",
    worker.pendingPermissionID ? `pending_permission_id: ${worker.pendingPermissionID}` : "",
    worker.pendingPlanApprovalID ? `pending_plan_approval_id: ${worker.pendingPlanApprovalID}` : "",
    worker.pendingShutdownID ? `pending_shutdown_id: ${worker.pendingShutdownID}` : "",
    worker.mailboxSize ? `mailbox_size: ${worker.mailboxSize}` : "",
    worker.spec.backend !== "in-process" ? `backend: ${worker.spec.backend}` : "",
    worker.spec.paneID ? `pane_id: ${worker.spec.paneID}` : "",
    worker.spec.paneWindowTarget ? `pane_window_target: ${worker.spec.paneWindowTarget}` : "",
    worker.paneHidden !== undefined ? `pane_hidden: ${worker.paneHidden}` : "",
    worker.spec.worktreePath ? `worktree_path: ${worker.spec.worktreePath}` : "",
    worker.spec.worktreeBranch ? `worktree_branch: ${worker.spec.worktreeBranch}` : "",
    worker.spec.remoteEndpoint ? `remote_endpoint: ${worker.spec.remoteEndpoint}` : "",
    worker.spec.remoteID ? `remote_id: ${worker.spec.remoteID}` : "",
    worker.spec.remoteSessionURL ? `remote_session_url: ${worker.spec.remoteSessionURL}` : "",
    worker.spec.remoteOutputPath ? `remote_output_path: ${worker.spec.remoteOutputPath}` : "",
    worker.remoteCursor ? `remote_cursor: ${worker.remoteCursor}` : "",
    worker.lastProgress ? `last_progress: ${worker.lastProgress}` : "",
    workerResult(worker) ? `result: ${workerResult(worker)}` : "",
    `description: ${worker.spec.description}`,
  ]
    .filter(Boolean)
    .join(EOL)
}

function formatTeamTable(teams: TeamSnapshot[]) {
  if (teams.length === 0) return "No subagent teams."
  const rows = teams
    .toSorted((a, b) => b.updatedAt - a.updatedAt || a.name.localeCompare(b.name))
    .map((team) => [
      team.name,
      String(team.workerIDs.length),
      team.agentType ?? "",
      Locale.truncate(team.description ?? "", 56),
    ])
  return table(["Team", "Workers", "Agent", "Description"], rows)
}

function formatTaskTable(tasks: TeamTaskState[]) {
  if (tasks.length === 0) return "No team tasks."
  const rows = tasks
    .toSorted((a, b) => Number(a.id) - Number(b.id) || a.id.localeCompare(b.id))
    .map((task) => [
      `#${task.id}`,
      task.team ?? "",
      task.status,
      task.owner ?? "",
      task.blockedBy.length ? task.blockedBy.map((id) => `#${id}`).join(",") : "",
      Locale.truncate(task.subject, 56),
    ])
  return table(["Task", "Team", "Status", "Owner", "Blocked By", "Subject"], rows)
}

function table(headers: string[], rows: string[][]) {
  const widths = headers.map((header, index) =>
    Math.max(header.length, ...rows.map((row) => (row[index] ?? "").length)),
  )
  return [
    headers.map((header, index) => header.padEnd(widths[index]!)).join("  "),
    widths.map((width) => "-".repeat(width)).join("  "),
    ...rows.map((row) => row.map((cell, index) => (cell ?? "").padEnd(widths[index]!)).join("  ")),
  ].join(EOL)
}

function workerResult(worker: WorkerSnapshot) {
  return worker.result?.error ?? worker.result?.text ?? ""
}

function formatTool(tool: { name: string; title?: string }) {
  return [tool.name, tool.title].filter(Boolean).join(": ")
}
