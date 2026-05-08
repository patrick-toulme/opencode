import { TextAttributes, RGBA } from "@opentui/core"
import type { SwarmTeamSnapshot, SwarmTeamTaskState, SwarmWorkerState } from "@opencode-ai/sdk/v2"
import { createEffect, createMemo, createSignal, onMount, Show } from "solid-js"
import { useRenderer } from "@opentui/solid"
import { useDialog } from "@tui/ui/dialog"
import { DialogConfirm } from "@tui/ui/dialog-confirm"
import { DialogPrompt } from "@tui/ui/dialog-prompt"
import { DialogSelect, type DialogSelectOption } from "@tui/ui/dialog-select"
import { useRoute } from "@tui/context/route"
import { useSDK } from "@tui/context/sdk"
import { useSync } from "@tui/context/sync"
import { selectedForeground, useTheme } from "@tui/context/theme"
import { useToast } from "@tui/ui/toast"
import { Locale } from "@/util/locale"
import * as Editor from "../../util/editor"
import { formatTranscript } from "../../util/transcript"

type BoardValue =
  | {
      kind: "worker"
      id: string
    }
  | {
      kind: "team"
      name: string
    }
  | {
      kind: "task"
      id: string
      team?: string
    }

const TERMINAL = new Set(["completed", "cancelled", "failed", "interrupted"])

export function DialogSwarm(props: { sessionID: string }) {
  const sync = useSync()
  const sdk = useSDK()
  const dialog = useDialog()
  const route = useRoute()
  const toast = useToast()
  const renderer = useRenderer()
  const { theme } = useTheme()
  const [selected, setSelected] = createSignal<BoardValue>()

  onMount(() => {
    dialog.setSize("large")
  })

  const workers = createMemo(() => sync.data.swarm.worker[props.sessionID] ?? [])
  const teams = createMemo(() => sync.data.swarm.team[props.sessionID] ?? [])
  const tasks = createMemo(() => sync.data.swarm.task[props.sessionID] ?? [])
  const activeWorkers = createMemo(() => workers().filter((worker) => !TERMINAL.has(worker.status)))
  const openTasks = createMemo(() => tasks().filter((task) => task.status !== "completed"))

  const workerByID = createMemo(() => new Map(workers().map((worker) => [worker.spec.workerID, worker])))
  const workerByName = createMemo(() => {
    const map = new Map<string, SwarmWorkerState>()
    for (const worker of workers()) {
      map.set(worker.spec.workerID, worker)
      map.set(worker.spec.sessionID, worker)
      if (worker.spec.name) map.set(worker.spec.name, worker)
    }
    return map
  })

  function lookup(value: BoardValue) {
    if (value.kind === "worker") return { kind: "worker" as const, worker: workerByID().get(value.id) }
    if (value.kind === "team") return { kind: "team" as const, team: teams().find((team) => team.name === value.name) }
    return {
      kind: "task" as const,
      task: tasks().find((task) => task.id === value.id && task.team === value.team),
    }
  }

  function statusColor(status: SwarmWorkerState["status"] | SwarmTeamTaskState["status"]) {
    if (status === "waiting_permission") return theme.warning
    if (status === "failed" || status === "cancelled" || status === "interrupted") return theme.error
    if (status === "completed") return theme.success
    if (status === "in_progress" || status === "running" || status === "booting") return theme.warning
    return theme.textMuted
  }

  function workerLabel(worker: SwarmWorkerState) {
    return worker.spec.name ?? worker.spec.agent
  }

  function workerDetail(worker: SwarmWorkerState) {
    const parts = [
      worker.spec.team ? `team ${worker.spec.team}` : undefined,
      worker.currentTool ? formatTool(worker.currentTool) : undefined,
      worker.pendingPermissionID ? `permission ${worker.pendingPermissionID}` : undefined,
      worker.pendingPlanApprovalID ? `plan ${worker.pendingPlanApprovalID}` : undefined,
      worker.pendingShutdownID ? `shutdown ${worker.pendingShutdownID}` : undefined,
      worker.mailboxSize ? `${worker.mailboxSize} queued` : undefined,
      worker.lastProgress,
    ]
    return parts.filter(Boolean).join(" · ")
  }

  function taskDetail(task: SwarmTeamTaskState) {
    const parts = [
      task.team ? `team ${task.team}` : undefined,
      task.owner ? `owner ${task.owner}` : undefined,
      task.blockedBy.length ? `blocked by ${task.blockedBy.join(", ")}` : undefined,
      task.blocks.length ? `blocks ${task.blocks.join(", ")}` : undefined,
      task.activeForm,
      task.description,
    ]
    return parts.filter(Boolean).join(" · ")
  }

  function upsertWorker(worker: SwarmWorkerState) {
    sync.set("swarm", "worker", props.sessionID, (current = []) => {
      const next = [...current]
      const index = next.findIndex((item) => item.spec.workerID === worker.spec.workerID)
      if (index >= 0) next[index] = worker
      else next.push(worker)
      next.sort((a, b) => Number(a.startedAt) - Number(b.startedAt) || a.spec.workerID.localeCompare(b.spec.workerID))
      return next
    })
  }

  function target(value: BoardValue) {
    const selected = lookup(value)
    if (selected.kind === "worker" && selected.worker) return selected.worker.spec.workerID
    return undefined
  }

  async function messageSelected(value: BoardValue) {
    const selected = lookup(value)
    const label =
      selected.kind === "worker" && selected.worker
        ? workerLabel(selected.worker)
        : selected.kind === "team" && selected.team
          ? `team ${selected.team.name}`
          : undefined

    if (!label) {
      toast.show({ variant: "warning", message: "Select a worker or team to message" })
      return
    }

    const message = await DialogPrompt.show(dialog, `Message ${label}`, {
      placeholder: "Instruction or structured protocol message",
    })
    const trimmed = message?.trim()
    if (!trimmed) return

    try {
      if (selected.kind === "worker" && selected.worker) {
        await sdk.client.swarm.worker.message(
          {
            sessionID: props.sessionID,
            target: selected.worker.spec.workerID,
            message: trimmed,
            summary: Locale.truncate(trimmed, 80),
          },
          { throwOnError: true },
        )
      }
      if (selected.kind === "team" && selected.team) {
        await sdk.client.swarm.team.broadcast(
          {
            sessionID: props.sessionID,
            teamName: selected.team.name,
            message: trimmed,
            summary: Locale.truncate(trimmed, 80),
          },
          { throwOnError: true },
        )
      }
      toast.show({ variant: "success", message: `Queued message for ${label}` })
      dialog.clear()
    } catch (error) {
      toast.show({ variant: "error", message: error instanceof Error ? error.message : "Failed to queue message" })
    }
  }

  async function stopSelected(value: BoardValue) {
    const workerID = target(value)
    if (!workerID) {
      toast.show({ variant: "warning", message: "Select a worker to stop" })
      return
    }

    try {
      const result = await sdk.client.swarm.worker.stop(
        {
          sessionID: props.sessionID,
          target: workerID,
        },
        { throwOnError: true },
      )
      if (result.data) upsertWorker(result.data)
      toast.show({ variant: "success", message: `Stop requested for ${workerID}` })
    } catch (error) {
      toast.show({ variant: "error", message: error instanceof Error ? error.message : "Failed to stop worker" })
    }
  }

  async function cancelSelected(value: BoardValue) {
    const workerID = target(value)
    if (!workerID) {
      toast.show({ variant: "warning", message: "Select a worker to cancel" })
      return
    }

    const confirmed = await DialogConfirm.show(
      dialog,
      "Cancel Subagent",
      `Force cancel ${workerID}? Use stop for graceful shutdown after the current turn.`,
      "Cancel worker",
    )
    if (confirmed !== true) return

    try {
      const result = await sdk.client.swarm.worker.cancel(
        {
          sessionID: props.sessionID,
          target: workerID,
        },
        { throwOnError: true },
      )
      if (result.data) upsertWorker(result.data)
      toast.show({ variant: "success", message: `Cancelled ${workerID}` })
      dialog.clear()
    } catch (error) {
      toast.show({ variant: "error", message: error instanceof Error ? error.message : "Failed to cancel worker" })
    }
  }

  async function transcriptSelected(value: BoardValue) {
    const selected = lookup(value)
    if (selected.kind !== "worker" || !selected.worker) {
      toast.show({ variant: "warning", message: "Select a worker to view its transcript" })
      return
    }

    const worker = selected.worker
    try {
      const [session, messages] = await Promise.all([
        sdk.client.session.get({ sessionID: worker.spec.sessionID }, { throwOnError: true }),
        sdk.client.session.messages({ sessionID: worker.spec.sessionID, limit: 500 }, { throwOnError: true }),
      ])
      if (!session.data) throw new Error("Worker session was not found")
      const transcript = [
        "# Subagent Transcript",
        "",
        `Worker ID: ${worker.spec.workerID}`,
        `Agent: ${worker.spec.agent}`,
        worker.spec.name ? `Name: ${worker.spec.name}` : "",
        worker.spec.team ? `Team: ${worker.spec.team}` : "",
        `Status: ${worker.status}`,
        "",
        "---",
        "",
        formatTranscript(
          session.data,
          (messages.data ?? []).map((message) => ({ info: message.info, parts: message.parts })),
          {
            thinking: true,
            toolDetails: true,
            assistantMetadata: true,
            providers: sync.data.provider,
          },
        ),
      ]
        .filter((line) => line !== "")
        .join("\n")
      await Editor.open({ value: transcript, renderer })
    } catch (error) {
      toast.show({ variant: "error", message: error instanceof Error ? error.message : "Failed to open transcript" })
    }
  }

  function openSelected(value: BoardValue) {
    const selected = lookup(value)
    if (selected.kind === "worker" && selected.worker) {
      route.navigate({ type: "session", sessionID: selected.worker.spec.sessionID })
      dialog.clear()
      return
    }
    if (selected.kind === "team" && selected.team) {
      void messageSelected(value)
      return
    }
    if (selected.kind === "task" && selected.task) {
      const owner = selected.task.owner ? workerByName().get(selected.task.owner) : undefined
      if (owner) {
        route.navigate({ type: "session", sessionID: owner.spec.sessionID })
        dialog.clear()
        return
      }
      toast.show({ variant: "info", message: selected.task.description || selected.task.subject })
    }
  }

  const options = createMemo<DialogSelectOption<BoardValue>[]>(() => [
    ...workers().map((worker) => ({
      title: workerLabel(worker),
      description: worker.spec.description,
      category: `Workers ${activeWorkers().length}/${workers().length} active`,
      value: {
        kind: "worker" as const,
        id: worker.spec.workerID,
      },
      footer: (
        <span style={{ fg: statusColor(worker.status) }}>
          {worker.status.replaceAll("_", " ")}
          <Show when={worker.currentTool}>{(tool) => ` · ${formatTool(tool())}`}</Show>
        </span>
      ),
      gutter: () => <StatusDot color={statusColor(worker.status)} active={!TERMINAL.has(worker.status)} />,
      onSelect: () => openSelected({ kind: "worker", id: worker.spec.workerID }),
      margin: (
        <text fg={worker.pendingPermissionID || worker.pendingPlanApprovalID ? theme.warning : theme.textMuted}>
          {worker.spec.planModeRequired
            ? "P"
            : worker.spec.backend === "worktree"
              ? "W"
              : worker.spec.backend === "tmux"
                ? "T"
                : worker.spec.backend === "iterm2"
                  ? "I"
                  : " "}
        </text>
      ),
    })),
    ...teams().map((team) => ({
      title: team.name,
      description: team.description,
      category: `Teams ${teams().length}`,
      value: {
        kind: "team" as const,
        name: team.name,
      },
      footer: `${team.workerIDs.length} workers${team.leadSessionID ? " · lead" : ""}`,
      gutter: () => (
        <StatusDot
          color={theme.primary}
          active={team.workerIDs.some((id) => {
            const worker = workerByID().get(id)
            return worker ? !TERMINAL.has(worker.status) : false
          })}
        />
      ),
      onSelect: () => openSelected({ kind: "team", name: team.name }),
    })),
    ...tasks().map((task) => ({
      title: task.subject,
      description: taskDetail(task),
      category: `Tasks ${openTasks().length}/${tasks().length} open`,
      value: {
        kind: "task" as const,
        id: task.id,
        team: task.team,
      },
      footer: (
        <span style={{ fg: statusColor(task.status) }}>
          {task.status.replaceAll("_", " ")}
          <Show when={task.owner}> · {task.owner}</Show>
        </span>
      ),
      gutter: () => <StatusDot color={statusColor(task.status)} active={task.status === "in_progress"} />,
      onSelect: () => openSelected({ kind: "task", id: task.id, team: task.team }),
    })),
  ])

  createEffect(() => {
    if (selected()) return
    const first = options()[0]
    if (first) setSelected(first.value)
  })

  return (
    <box gap={1}>
      <box paddingLeft={4} paddingRight={4}>
        <box flexDirection="row" justifyContent="space-between">
          <text fg={theme.text} attributes={TextAttributes.BOLD}>
            Swarm Board
          </text>
          <text fg={theme.textMuted} onMouseUp={() => dialog.clear()}>
            esc
          </text>
        </box>
        <box paddingTop={1} flexDirection="row" gap={2}>
          <Metric label="workers" value={`${activeWorkers().length}/${workers().length}`} warning={activeWorkers().length > 0} />
          <Metric label="teams" value={String(teams().length)} warning={false} />
          <Metric label="open tasks" value={`${openTasks().length}/${tasks().length}`} warning={openTasks().length > 0} />
          <Show when={workers().some((worker) => worker.pendingPermissionID || worker.pendingPlanApprovalID)}>
            <text fg={theme.warning}>pending approval</text>
          </Show>
        </box>
      </box>
      <Show
        when={options().length > 0}
        fallback={
          <box paddingLeft={4} paddingRight={4} paddingBottom={1}>
            <text fg={theme.textMuted}>No swarm workers, teams, or shared tasks for this session.</text>
          </box>
        }
      >
        <DialogSelect
          title="Board Items"
          placeholder="Filter workers, teams, tasks..."
          options={options()}
          flat
          onMove={(option) => setSelected(option.value)}
          actions={[
            {
              command: "dialog.action.rename",
              title: "Message",
              onTrigger: (option) => void messageSelected(option.value),
            },
            {
              command: "dialog.action.details",
              title: "Transcript",
              onTrigger: (option) => void transcriptSelected(option.value),
            },
            {
              command: "dialog.action.toggle",
              title: "Stop",
              onTrigger: (option) => void stopSelected(option.value),
            },
            {
              command: "dialog.action.delete",
              title: "Cancel",
              side: "right",
              onTrigger: (option) => void cancelSelected(option.value),
            },
          ]}
        />
      </Show>
      <Show when={options().length > 0}>
        <SwarmDetail value={selected()} lookup={lookup} workerDetail={workerDetail} />
      </Show>
    </box>
  )
}

function Metric(props: { label: string; value: string; warning: boolean }) {
  const { theme } = useTheme()
  return (
    <text fg={theme.text}>
      <span style={{ fg: props.warning ? theme.warning : theme.success }}>{props.value}</span>{" "}
      <span style={{ fg: theme.textMuted }}>{props.label}</span>
    </text>
  )
}

function StatusDot(props: { color: RGBA; active: boolean }) {
  const fg = props.active ? props.color : props.color
  return (
    <text flexShrink={0} fg={fg}>
      {props.active ? "●" : "•"}
    </text>
  )
}

function SwarmDetail(props: {
  value?: BoardValue
  lookup: (value: BoardValue) =>
    | { kind: "worker"; worker: SwarmWorkerState | undefined }
    | { kind: "team"; team: SwarmTeamSnapshot | undefined }
    | { kind: "task"; task: SwarmTeamTaskState | undefined }
  workerDetail: (worker: SwarmWorkerState) => string
}) {
  const { theme } = useTheme()
  const fg = selectedForeground(theme)
  const selected = createMemo(() => (props.value ? props.lookup(props.value) : undefined))
  const selectedWorker = createMemo(() => {
    const item = selected()
    return item?.kind === "worker" ? item.worker : undefined
  })
  const selectedTeam = createMemo(() => {
    const item = selected()
    return item?.kind === "team" ? item.team : undefined
  })
  const selectedTask = createMemo(() => {
    const item = selected()
    return item?.kind === "task" ? item.task : undefined
  })

  return (
    <box paddingLeft={4} paddingRight={4} paddingBottom={1}>
      <Show when={selectedWorker()}>
        {(worker) => (
          <box>
            <text fg={fg}>
              {worker().spec.workerID} <span style={{ fg: theme.textMuted }}>{worker().spec.sessionID}</span>
            </text>
            <text fg={theme.textMuted} wrapMode="word">
              {props.workerDetail(worker()) || worker().spec.prompt}
            </text>
          </box>
        )}
      </Show>
      <Show when={selectedTeam()}>
        {(team) => (
          <box>
            <text fg={fg}>team {team().name}</text>
            <text fg={theme.textMuted} wrapMode="word">
              {team().description || `${team().workerIDs.length} workers`}
            </text>
          </box>
        )}
      </Show>
      <Show when={selectedTask()}>
        {(task) => (
          <box>
            <text fg={fg}>task {task().id}</text>
            <text fg={theme.textMuted} wrapMode="word">
              {task().description}
            </text>
          </box>
        )}
      </Show>
    </box>
  )
}

function formatTool(tool: { name: string; title?: string }) {
  return `${Locale.titlecase(tool.name)}${tool.title ? ` ${tool.title}` : ""}`
}
