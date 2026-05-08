import { Bus } from "@/bus"
import { Config } from "@/config/config"
import { InstanceState } from "@/effect/instance-state"
import { Identifier } from "@/id/id"
import type { InstanceContext } from "@/project/instance"
import { Storage } from "@/storage/storage"
import { errorMessage } from "@/util/error"
import { Flock } from "@opencode-ai/core/util/flock"
import { Cause, Context, Effect, Exit, Fiber, Layer, Option, PubSub, Queue, Scope } from "effect"
import { Event } from "./events"
import { SwarmMailbox, type EventLogEntry } from "./mailbox"
import { notifyParent } from "./notification"
import { backendByType } from "./backend/registry"
import { SwarmPeer, type PeerMessage } from "./peer"
import {
  appendRemoteWorkerOutput,
  cancelRemoteWorker,
  pollRemoteWorker,
  sendRemoteWorkerInput,
  type RemoteWorkerEvent,
} from "./remote/client"
import { cleanupWorktreeIfClean } from "./worktree"
import {
  initialWorkerState,
  WorkerBackend,
  WorkerID,
  WorkerState,
  type ContextStrategy,
  type ExecutionStrategy,
  type PermissionStrategy,
  type TeamSnapshot,
  type TeamState,
  TeamTaskID,
  type TeamTaskState,
  type TeamTaskStatus,
  type WorkerCompletion,
  type WorkerInput,
  type WorkerModel,
  type WorkerSnapshot,
  type WorkerSpec,
  type WorkerStatus,
} from "./state"
import type { MessageID, SessionID } from "@/session/schema"

type WorkerRecord = {
  state: WorkerState
  mailbox: Queue.Queue<WorkerInput>
  mailboxItems: WorkerInput[]
  fiber?: Fiber.Fiber<WorkerCompletion, never>
  cancel?: Effect.Effect<void, unknown>
}

type State = {
  persistKey: string[]
  eventLogOffset: number
  eventLogSeen: Set<string>
  workers: Map<WorkerID, WorkerRecord>
  bySession: Map<SessionID, WorkerID>
  byParent: Map<SessionID, Set<WorkerID>>
  byName: Map<string, WorkerID>
  byTeam: Map<string, Set<WorkerID>>
  teams: Map<string, TeamState>
  tasks: Map<string, Map<TeamTaskID, TeamTaskState>>
  taskSeq: Map<string, number>
  peer: PubSub.PubSub<PeerMessage>
  remotePollStarting: Set<WorkerID>
}

type PersistedWorker = {
  state: WorkerState
  mailbox: WorkerInput[]
}

type PersistedState = {
  version: 1
  workers: PersistedWorker[]
  teams: TeamState[]
  tasks: Record<string, TeamTaskState[]>
  taskSeq: Record<string, number>
}

export type SpawnInput = {
  workerID?: WorkerID
  parentSessionID: SessionID
  sessionID: SessionID
  agent: string
  name?: string
  team?: string
  prompt: string
  description: string
  outputPath?: string
  model?: WorkerModel
  sourceToolCallID?: string
  sourceMessageID?: MessageID
  contextStrategy?: ContextStrategy
  permissionStrategy?: PermissionStrategy
  executionStrategy?: ExecutionStrategy
  backend?: WorkerBackend
  paneID?: string
  paneExternalSession?: boolean
  paneWindowTarget?: string
  worktreeRoot?: string
  worktreePath?: string
  worktreeBranch?: string
  remoteEndpoint?: string
  remoteID?: string
  remoteSessionURL?: string
  remoteOutputPath?: string
  fork?: boolean
  planModeRequired?: boolean
  run?: Effect.Effect<WorkerCompletion>
  launch?: Effect.Effect<void, unknown>
  wait?: boolean
  cancel?: Effect.Effect<void, unknown>
}

export type AdoptInput = {
  workerID: WorkerID
  run: Effect.Effect<WorkerCompletion>
  wait?: boolean
  cancel?: Effect.Effect<void, unknown>
}

export type SendInput = {
  parentSessionID?: SessionID
  to: string
  message: string
  summary?: string
  from?: string
}

export type BroadcastInput = {
  parentSessionID?: SessionID
  team: string
  message: string
  summary?: string
  from?: string
}

export type CreateTeamInput = {
  parentSessionID: SessionID
  name: string
  description?: string
  leadSessionID?: SessionID
  agentType?: string
}

export type DeleteTeamInput = {
  parentSessionID: SessionID
  name: string
  cancelWorkers?: boolean
}

export type TaskBoardInput = {
  parentSessionID: SessionID
  team?: string
}

export type CreateTaskInput = TaskBoardInput & {
  subject: string
  description: string
  activeForm?: string
  owner?: string
  metadata?: Record<string, unknown>
}

export type UpdateTaskInput = TaskBoardInput & {
  taskID: string
  subject?: string
  description?: string
  activeForm?: string
  status?: TeamTaskStatus | "deleted"
  owner?: string
  addBlocks?: readonly string[]
  addBlockedBy?: readonly string[]
  metadata?: Record<string, unknown>
}

export type UpdateTaskResult = {
  success: boolean
  taskID: string
  updatedFields: string[]
  task?: TeamTaskState
  deleted?: TeamTaskState
  error?: string
  statusChange?: {
    from: TeamTaskStatus
    to: TeamTaskStatus | "deleted"
  }
}

export type GetTaskInput = TaskBoardInput & {
  taskID: string
}

export type TargetInput = {
  parentSessionID?: SessionID
  to: string
}

export type WaitInput = TargetInput & {
  timeoutMS?: number
}

export type PaneAction = "hide" | "show"

export type RemoteMetadataInput = {
  remoteEndpoint?: string
  remoteID?: string
  remoteSessionURL?: string
  remoteOutputPath?: string
  remoteCursor?: string
}

export type SpawnResult = {
  workerID: WorkerID
  sessionID: SessionID
  state: WorkerSnapshot
  completion?: WorkerCompletion
}

export interface Interface {
  readonly spawn: (input: SpawnInput) => Effect.Effect<SpawnResult>
  readonly adopt: (input: AdoptInput) => Effect.Effect<SpawnResult, Error>
  readonly get: (workerID: WorkerID) => Effect.Effect<WorkerSnapshot | undefined>
  readonly getBySession: (sessionID: SessionID) => Effect.Effect<WorkerSnapshot | undefined>
  readonly resolve: (input: TargetInput) => Effect.Effect<WorkerSnapshot | undefined>
  readonly list: (parentSessionID?: SessionID) => Effect.Effect<WorkerSnapshot[]>
  readonly wait: (input: WaitInput) => Effect.Effect<WorkerSnapshot, Error>
  readonly createTeam: (input: CreateTeamInput) => Effect.Effect<TeamSnapshot>
  readonly listTeams: (parentSessionID?: SessionID) => Effect.Effect<TeamSnapshot[]>
  readonly deleteTeam: (input: DeleteTeamInput) => Effect.Effect<TeamSnapshot | undefined, Error>
  readonly createTask: (input: CreateTaskInput) => Effect.Effect<TeamTaskState>
  readonly listTeamTasks: (input: TaskBoardInput) => Effect.Effect<TeamTaskState[]>
  readonly getTeamTask: (input: GetTaskInput) => Effect.Effect<TeamTaskState | undefined>
  readonly updateTeamTask: (input: UpdateTaskInput) => Effect.Effect<UpdateTaskResult>
  readonly claimNextTask: (workerID: WorkerID) => Effect.Effect<TeamTaskState | undefined>
  readonly updateProgress: (workerID: WorkerID, message: string) => Effect.Effect<void>
  readonly recordResult: (workerID: WorkerID, completion: WorkerCompletion) => Effect.Effect<void>
  readonly updateRemoteMetadata: (workerID: WorkerID, input: RemoteMetadataInput) => Effect.Effect<void, Error>
  readonly updateRemoteCursor: (workerID: WorkerID, cursor: string) => Effect.Effect<void>
  readonly updateCurrentTool: (
    workerID: WorkerID,
    tool: { name: string; title?: string } | undefined,
  ) => Effect.Effect<void>
  readonly markPermissionPending: (workerID: WorkerID, permissionID: string) => Effect.Effect<void>
  readonly clearPermissionPending: (workerID: WorkerID, permissionID?: string) => Effect.Effect<void>
  readonly requestShutdown: (workerID: WorkerID, requestID: string) => Effect.Effect<void>
  readonly approveShutdown: (workerID: WorkerID, requestID: string) => Effect.Effect<void>
  readonly rejectShutdown: (workerID: WorkerID, requestID: string, reason: string) => Effect.Effect<void>
  readonly stopAfterCurrentTurn: (workerID: WorkerID, reason?: string) => Effect.Effect<void>
  readonly requestPlanApproval: (workerID: WorkerID, requestID: string) => Effect.Effect<void>
  readonly approvePlan: (workerID: WorkerID, requestID: string) => Effect.Effect<void>
  readonly rejectPlan: (workerID: WorkerID, requestID: string, feedback: string) => Effect.Effect<void>
  readonly sendInput: (input: SendInput) => Effect.Effect<WorkerInput, Error>
  readonly broadcast: (input: BroadcastInput) => Effect.Effect<WorkerInput[], Error>
  readonly awaitInput: (workerID: WorkerID) => Effect.Effect<WorkerInput>
  readonly controlPane: (workerID: WorkerID, action: PaneAction) => Effect.Effect<WorkerSnapshot, Error>
  readonly cancel: (workerID: WorkerID) => Effect.Effect<void>
  readonly reload: () => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SwarmRuntime") {}

const terminalStatus = new Set<WorkerStatus>(["completed", "cancelled", "failed", "interrupted"])
const eventDefinitions: Map<string, (typeof Event)[keyof typeof Event]> = new Map(
  Object.values(Event).map((def) => [def.type, def] as const),
)

const indexWorker = (state: State, record: WorkerRecord) => {
  const { spec } = record.state
  state.workers.set(spec.workerID, record)
  state.bySession.set(spec.sessionID, spec.workerID)
  const parentSet = state.byParent.get(spec.parentSessionID) ?? new Set<WorkerID>()
  parentSet.add(spec.workerID)
  state.byParent.set(spec.parentSessionID, parentSet)
  if (spec.name) state.byName.set(nameKey(spec.parentSessionID, spec.name), spec.workerID)
  if (spec.team) {
    const key = teamKey(spec.parentSessionID, spec.team)
    const teamSet = state.byTeam.get(key) ?? new Set<WorkerID>()
    teamSet.add(spec.workerID)
    state.byTeam.set(key, teamSet)
  }
}

const serializeState = (state: State): PersistedState => ({
  version: 1,
  workers: Array.from(state.workers.values(), (worker) => ({
    state: worker.state,
    mailbox: [...worker.mailboxItems],
  })),
  teams: Array.from(state.teams.values()),
  tasks: Object.fromEntries(
    Array.from(state.tasks.entries(), ([key, tasks]) => [key, Array.from(tasks.values())] as const),
  ),
  taskSeq: Object.fromEntries(state.taskSeq.entries()),
})

export const layer: Layer.Layer<Service, never, Bus.Service | Storage.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const storage = yield* Storage.Service
    const scope = yield* Scope.Scope
    const withStateLock = <A, E, R>(body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.gen(function* () {
        const ctx = yield* InstanceState.context
        return yield* Effect.scoped(
          Flock.effect(`swarm:${ctx.project.id}`).pipe(Effect.orDie, Effect.flatMap(() => body)),
        )
      })
    const clearState = (target: State) => {
      target.workers.clear()
      target.bySession.clear()
      target.byParent.clear()
      target.byName.clear()
      target.byTeam.clear()
      target.teams.clear()
      target.tasks.clear()
      target.taskSeq.clear()
      target.remotePollStarting.clear()
    }
    const clearWorkerIndexes = (target: State) => {
      target.bySession.clear()
      target.byParent.clear()
      target.byName.clear()
      target.byTeam.clear()
    }
    const reindexWorkers = (target: State) => {
      clearWorkerIndexes(target)
      for (const worker of target.workers.values()) indexWorker(target, worker)
    }
    const setLiveControlField = (
      target: WorkerState,
      key: "pendingPermissionID" | "pendingShutdownID" | "pendingPlanApprovalID",
      value: string | undefined,
    ) => {
      switch (key) {
        case "pendingPermissionID":
          if (value === undefined) {
            const { pendingPermissionID: _pendingPermissionID, ...rest } = target
            return rest
          }
          return { ...target, pendingPermissionID: value }
        case "pendingShutdownID":
          if (value === undefined) {
            const { pendingShutdownID: _pendingShutdownID, ...rest } = target
            return rest
          }
          return { ...target, pendingShutdownID: value }
        case "pendingPlanApprovalID":
          if (value === undefined) {
            const { pendingPlanApprovalID: _pendingPlanApprovalID, ...rest } = target
            return rest
          }
          return { ...target, pendingPlanApprovalID: value }
      }
    }
    const mergeLiveControlState = (existing: WorkerRecord, persisted: WorkerState) => {
      const fields = ["pendingPermissionID", "pendingShutdownID", "pendingPlanApprovalID"] as const
      const current = existing.state
      const persistedIsNewer = persisted.updatedAt > current.updatedAt
      const persistedIsCurrentOrNewer = persisted.updatedAt >= current.updatedAt
      let next = current
      let changed = false

      for (const field of fields) {
        const persistedValue = persisted[field]
        const currentValue = current[field]
        if (persistedValue !== undefined) {
          if (persistedIsCurrentOrNewer && currentValue !== persistedValue) {
            next = setLiveControlField(next, field, persistedValue)
            changed = true
          }
          continue
        }
        if (persistedIsNewer && currentValue !== undefined) {
          next = setLiveControlField(next, field, undefined)
          changed = true
        }
      }

      if (persistedIsNewer && persisted.status === "waiting_permission" && current.status !== "waiting_permission") {
        next = { ...next, status: "waiting_permission" }
        changed = true
      }

      if (!changed) return false
      existing.state = { ...next, updatedAt: Math.max(current.updatedAt, persisted.updatedAt) }
      return true
    }
    const hydrateWorker = Effect.fn("SwarmRuntime.hydrateWorker")(function* (
      item: PersistedWorker,
      ctx: InstanceContext | undefined,
      existing?: WorkerRecord,
      options?: { interruptOwnedWorkers?: boolean },
    ) {
      if (existing?.fiber) return { record: existing, changed: mergeLiveControlState(existing, item.state) }
      const now = Date.now()
      const mailbox = yield* Queue.unbounded<WorkerInput>()
      const mailboxItems = [...(item.mailbox ?? [])]
      yield* Effect.forEach(mailboxItems, (input) => Queue.offer(mailbox, input), { discard: true })
      const liveness =
        terminalStatus.has(item.state.status) || !ctx
          ? undefined
          : yield* SwarmMailbox.inspectWorker(item.state.spec.workerID, ctx)
      if (existing && liveness?.alive && liveness.ownedByCurrent) return { record: existing, changed: false }
      const keepExternal = Boolean(liveness?.alive && !liveness.ownedByCurrent)
      const keepLiveOwnedByCurrent = Boolean(liveness?.alive && liveness.ownedByCurrent && !options?.interruptOwnedWorkers)
      const keepRemote = item.state.spec.backend === "remote" && Boolean(item.state.spec.remoteID)
      const reloaded = terminalStatus.has(item.state.status)
        ? item.state
        : keepExternal || keepLiveOwnedByCurrent || keepRemote
          ? item.state
          : {
              ...item.state,
              status: "interrupted" as const,
              currentTool: undefined,
              pendingPermissionID: undefined,
              pendingShutdownID: undefined,
              pendingPlanApprovalID: undefined,
              lastProgress: "runtime restarted before worker completed",
              result: { error: "runtime restarted before worker completed" },
              updatedAt: now,
      }
      return {
        record: { state: reloaded, mailbox, mailboxItems },
        changed: !terminalStatus.has(item.state.status) && !keepExternal && !keepLiveOwnedByCurrent && !keepRemote,
      }
    })
    const mergePersisted = Effect.fn("SwarmRuntime.mergePersisted")(function* (
      target: State,
      persisted: PersistedState,
      ctx?: InstanceContext,
      options?: { interruptOwnedWorkers?: boolean },
    ) {
      const previousWorkers = new Map(target.workers)
      let changed = false

      target.teams.clear()
      target.tasks.clear()
      target.taskSeq.clear()
      for (const team of persisted.teams ?? []) {
        target.teams.set(teamKey(team.parentSessionID, team.name), team)
      }
      for (const [key, tasks] of Object.entries(persisted.tasks ?? {})) {
        target.tasks.set(key, new Map(tasks.map((task) => [task.id, task])))
      }
      for (const [key, seq] of Object.entries(persisted.taskSeq ?? {})) {
        target.taskSeq.set(key, seq)
      }

      target.workers.clear()
      for (const item of persisted.workers ?? []) {
        const existing = previousWorkers.get(item.state.spec.workerID)
        const hydrated = yield* hydrateWorker(item, ctx, existing, options)
        changed = changed || hydrated.changed
        target.workers.set(hydrated.record.state.spec.workerID, hydrated.record)
      }
      for (const worker of previousWorkers.values()) {
        if (worker.fiber && !target.workers.has(worker.state.spec.workerID)) {
          target.workers.set(worker.state.spec.workerID, worker)
          changed = true
        }
      }
      reindexWorkers(target)

      for (const record of target.workers.values()) {
        if (terminalStatus.has(record.state.status)) {
          const unassigned = unassignWorkerTasksInState(target, record)
          yield* Effect.forEach(
            unassigned,
            (task) =>
              publishSwarm(Event.TaskUpdated, {
                parentSessionID: task.parentSessionID,
                ...(task.team ? { team: task.team } : {}),
                task,
              }),
            { discard: true },
          )
        }
      }
      if (changed) yield* storage.write(target.persistKey, serializeState(target)).pipe(Effect.ignore)
    })
    const loadPersisted = Effect.fn("SwarmRuntime.loadPersisted")(function* (
      target: State,
      ctx?: InstanceContext,
      options?: { interruptOwnedWorkers?: boolean },
    ) {
      const persisted = yield* storage
        .read<PersistedState>(target.persistKey)
        .pipe(Effect.catch(() => Effect.succeed(undefined as PersistedState | undefined)))
      if (persisted?.version !== 1) return
      yield* mergePersisted(target, persisted, ctx, options)
    })
    const publishEventLogEntry = Effect.fn("SwarmRuntime.publishEventLogEntry")(function* (
      target: State,
      entry: EventLogEntry,
    ) {
      if (entry.originID === SwarmMailbox.currentOwnerID()) return
      if (target.eventLogSeen.has(entry.id)) return
      const def = eventDefinitions.get(entry.type)
      if (!def) return
      target.eventLogSeen.add(entry.id)
      yield* bus.publish(def as never, entry.properties as never, { id: entry.id }).pipe(Effect.ignore)
    })
    const eventLogFanout = (target: State, ctx: InstanceContext) =>
      Effect.gen(function* () {
        const signal = yield* Queue.sliding<void>(1)
        const peerScope = yield* Scope.make()
        const peerSubscription = yield* Scope.provide(peerScope)(PubSub.subscribe(target.peer))
        const drain = Effect.fn("SwarmRuntime.eventLogFanout.drain")(function* () {
          const read = yield* SwarmMailbox.readEvents(target.eventLogOffset, ctx)
          target.eventLogOffset = read.offset
          yield* Effect.forEach(read.events, (entry) => publishEventLogEntry(target, entry), { discard: true })
        })
        const peerWake = Effect.gen(function* () {
          while (true) {
            const message = yield* PubSub.take(peerSubscription)
            if (message.type === "event-log") return
          }
        })
        return yield* Effect.acquireUseRelease(
          SwarmMailbox.watchEventLog(() => {
            Queue.offerUnsafe(signal, undefined)
          }, ctx),
          () =>
            Effect.gen(function* () {
              while (true) {
                yield* drain()
                yield* Effect.raceAll([Queue.take(signal), peerWake, Effect.sleep("250 millis")]).pipe(Effect.ignore)
              }
            }),
          (stop) => Effect.all([Effect.sync(stop), Scope.close(peerScope, Exit.void)], { discard: true }),
        )
      })
    const publishSwarm = <D extends (typeof Event)[keyof typeof Event]>(def: D, properties: unknown) =>
      Effect.gen(function* () {
        const id = Bus.createID()
        yield* SwarmMailbox.appendEvent({ id, type: def.type, properties }).pipe(Effect.ignore)
        const ctx = yield* InstanceState.context
        yield* SwarmPeer.notifyAll(ctx, { type: "event-log", eventID: id }).pipe(Effect.ignore)
        yield* bus.publish(def as never, properties as never, { id })
      })
    const state = yield* InstanceState.make<State>(
      Effect.fn("SwarmRuntime.state")(function* (ctx) {
        const persistKey = ["swarm", encodeURIComponent(ctx.project.id)]
        const eventLogOffset = yield* SwarmMailbox.eventLogOffset(ctx)
        const peer = yield* PubSub.unbounded<PeerMessage>()
        const peerHandle = yield* SwarmPeer.start(ctx, (message) => {
          Effect.runFork(PubSub.publish(peer, message).pipe(Effect.ignore))
        })
        const state: State = {
          persistKey,
          eventLogOffset,
          eventLogSeen: new Set(),
          workers: new Map(),
          bySession: new Map(),
          byParent: new Map(),
          byName: new Map(),
          byTeam: new Map(),
          teams: new Map(),
          tasks: new Map(),
          taskSeq: new Map(),
          peer,
          remotePollStarting: new Set(),
        }

        yield* loadPersisted(state, ctx)
        yield* eventLogFanout(state, ctx).pipe(Effect.forkScoped)

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            yield* Effect.forEach(
              Array.from(state.workers.values()),
              (worker) => (worker.fiber ? Fiber.interrupt(worker.fiber) : Effect.void),
              { concurrency: "unbounded", discard: true },
            )
            yield* SwarmPeer.stop(peerHandle).pipe(Effect.ignore)
            yield* PubSub.shutdown(peer).pipe(Effect.ignore)
            clearState(state)
          }),
        )

        return state
      }),
    )

    const refreshPersisted = Effect.fn("SwarmRuntime.refreshPersisted")(function* () {
      const s = yield* InstanceState.get(state)
      yield* loadPersisted(s, yield* InstanceState.context)
      return s
    })
    const refreshPersistedLocked = Effect.fn("SwarmRuntime.refreshPersistedLocked")(function* () {
      return yield* withStateLock(refreshPersisted())
    })

    const snapshot = (worker: WorkerRecord): WorkerSnapshot => ({ ...worker.state })
    const persist = (s: State) => storage.write(s.persistKey, serializeState(s)).pipe(Effect.ignore)
    const teamSnapshot = (s: State, team: TeamState): TeamSnapshot => ({
      ...team,
      workerIDs: Array.from(s.byTeam.get(teamKey(team.parentSessionID, team.name)) ?? []),
    })
    const acceptingInput = (worker: WorkerRecord) =>
      !["completed", "cancelled", "failed", "interrupted"].includes(worker.state.status)
    const queueableLocalWorker = (worker: WorkerRecord) => Boolean(worker.fiber)
    const remoteClientFor = Effect.fn("SwarmRuntime.remoteClientFor")(function* (worker: WorkerRecord) {
      const config = Option.getOrUndefined(yield* Effect.serviceOption(Config.Service))
      const cfg = config
        ? yield* config.get().pipe(Effect.catchCause(() => Effect.succeed(undefined)))
        : undefined
      const endpoint = worker.state.spec.remoteEndpoint ?? cfg?.experimental?.swarm_remote_endpoint
      if (!endpoint) return undefined
      const token = process.env.OPENCODE_SWARM_REMOTE_TOKEN ?? cfg?.experimental?.swarm_remote_token
      return {
        endpoint,
        ...(token ? { token } : {}),
      }
    })
    const killExternalPane = (worker: WorkerRecord) => {
      const backend = worker.state.spec.backend
      const paneID = worker.state.spec.paneID
      if (!paneID || (backend !== "tmux" && backend !== "iterm2")) return Effect.void
      return Effect.promise(() => backendByType(backend).killPane(paneID, worker.state.spec.paneExternalSession)).pipe(
        Effect.ignore,
      )
    }
    const cancelRemote = (worker: WorkerRecord) => {
      const { remoteID } = worker.state.spec
      if (worker.state.spec.backend !== "remote" || !remoteID) return Effect.void
      return remoteClientFor(worker).pipe(
        Effect.flatMap((client) => (client ? cancelRemoteWorker(client, remoteID) : Effect.void)),
        Effect.ignore,
      )
    }
    const cleanupWorkerWorktree = (worker: WorkerRecord) => {
      const { worktreeRoot, worktreePath, worktreeBranch } = worker.state.spec
      if (!worktreeRoot || !worktreePath || !worktreeBranch) return Effect.void
      return cleanupWorktreeIfClean({
        root: worktreeRoot,
        path: worktreePath,
        branch: worktreeBranch,
      }).pipe(Effect.ignore)
    }
    const interruptWorker = (worker: WorkerRecord) =>
      Effect.all(
        [
          worker.fiber ? Fiber.interrupt(worker.fiber) : Effect.void,
          worker.cancel ??
            Effect.gen(function* () {
              yield* cancelRemote(worker)
              yield* killExternalPane(worker)
            }),
        ],
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.timeout("1 second"), Effect.ignore)

    yield* Effect.gen(function* () {
      while (true) {
        const s = yield* InstanceState.get(state)
        const workers = Array.from(s.workers.values()).filter((worker) => worker.fiber && acceptingInput(worker))
        yield* Effect.forEach(workers, (worker) => SwarmMailbox.touchWorker(snapshot(worker)).pipe(Effect.ignore), {
          concurrency: "unbounded",
          discard: true,
        })
        yield* Effect.sleep("2 seconds")
      }
    }).pipe(Effect.forkIn(scope))

    const waitComplete = new Set<WorkerStatus>([
      "idle",
      "waiting_permission",
      "waiting_input",
      "completed",
      "cancelled",
      "failed",
      "interrupted",
    ])
    const taskBoardKey = (input: TaskBoardInput) =>
      input.team ? teamKey(input.parentSessionID, input.team) : `${input.parentSessionID}:session`
    const taskSnapshot = (task: TeamTaskState): TeamTaskState => ({
      ...task,
      blocks: [...task.blocks],
      blockedBy: [...task.blockedBy],
      ...(task.metadata ? { metadata: { ...task.metadata } } : {}),
    })
    const taskBoard = (s: State, input: TaskBoardInput) => {
      const key = taskBoardKey(input)
      let board = s.tasks.get(key)
      if (!board) {
        board = new Map<TeamTaskID, TeamTaskState>()
        s.tasks.set(key, board)
      }
      return { key, board }
    }
    const publishTask = (def: typeof Event.TaskCreated | typeof Event.TaskUpdated | typeof Event.TaskDeleted, task: TeamTaskState) =>
      publishSwarm(def, {
        parentSessionID: task.parentSessionID,
        ...(task.team ? { team: task.team } : {}),
        task: taskSnapshot(task),
      })
    const findAvailableTask = (board: Map<TeamTaskID, TeamTaskState>) => {
      const unresolved = new Set(
        Array.from(board.values())
          .filter((task) => task.status !== "completed")
          .map((task) => task.id),
      )
      return Array.from(board.values())
        .filter((task) => task.status === "pending")
        .filter((task) => !task.owner)
        .filter((task) => task.blockedBy.every((id) => !unresolved.has(id)))
        .toSorted((a, b) => Number(a.id) - Number(b.id) || a.id.localeCompare(b.id))[0]
    }
    const formatTaskInput = (task: TeamTaskState, assignedBy = "task_board") =>
      [
        formatTaskAssignment(task, assignedBy),
        "",
        `Complete all open tasks. Start with task #${task.id}:`,
        "",
        task.subject,
        task.description ? `\n${task.description}` : "",
      ]
        .filter((line) => line !== "")
        .join("\n")

    const publishStatus = (worker: WorkerRecord, message?: string) =>
      publishSwarm(Event.StatusChanged, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        status: worker.state.status,
        ...(message ? { message } : {}),
        worker: snapshot(worker),
      })

    const publishWorkerIdle = (worker: WorkerRecord, patch?: Partial<WorkerState>) => {
      const summary = patch?.result?.text ?? patch?.lastProgress
      const failureReason = patch?.result?.error
      const completedStatus = failureReason ? "failed" : patch?.result?.text ? "resolved" : undefined
      return publishSwarm(Event.WorkerIdle, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        ...(worker.state.spec.team ? { team: worker.state.spec.team } : {}),
        idleReason: failureReason ? "failed" : "available",
        ...(summary ? { summary } : {}),
        ...(completedStatus ? { completedStatus } : {}),
        ...(failureReason ? { failureReason } : {}),
        worker: snapshot(worker),
      })
    }

    const publishWorkerStopped = (worker: WorkerRecord, previousStatus: WorkerStatus) => {
      if (terminalStatus.has(previousStatus)) return Effect.void
      const reason = worker.state.result?.error ?? worker.state.result?.text ?? worker.state.lastProgress
      return publishSwarm(Event.WorkerStopped, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        status: worker.state.status,
        ...(reason ? { reason } : {}),
        worker: snapshot(worker),
      })
    }

    const setStatus = Effect.fn("SwarmRuntime.setStatus")(function* (
      workerID: WorkerID,
      status: WorkerStatus,
      patch?: Partial<
        Pick<
          WorkerState,
          | "lastProgress"
          | "result"
          | "currentTool"
          | "pendingPermissionID"
          | "pendingShutdownID"
          | "pendingPlanApprovalID"
        >
      >,
    ) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const worker = s.workers.get(workerID)
          if (!worker) return
          const previousStatus = worker.state.status
          const next = {
            ...worker.state,
            ...patch,
            status,
            updatedAt: Date.now(),
          }
          if (terminalStatus.has(status)) {
            const {
              currentTool: _currentTool,
              pendingPermissionID: _pendingPermissionID,
              pendingShutdownID: _pendingShutdownID,
              pendingPlanApprovalID: _pendingPlanApprovalID,
              ...rest
            } = next
            worker.state = rest
          } else {
            worker.state = next
          }
          const unassigned = terminalStatus.has(status) ? unassignWorkerTasksInState(s, worker) : []
          yield* persist(s)
          if (terminalStatus.has(status)) {
            yield* SwarmMailbox.clearWorker(workerID).pipe(Effect.ignore)
            yield* cleanupWorkerWorktree(worker)
          } else if (worker.fiber) {
            yield* SwarmMailbox.touchWorker(snapshot(worker)).pipe(Effect.ignore)
          }
          yield* Effect.forEach(unassigned, (task) => publishTask(Event.TaskUpdated, task), { discard: true })
          yield* publishStatus(worker, patch?.lastProgress)
          if (status === "idle" && previousStatus !== "idle") yield* publishWorkerIdle(worker, patch)
          if (terminalStatus.has(status)) yield* publishWorkerStopped(worker, previousStatus)
        }),
      )
    })

    const register = Effect.fn("SwarmRuntime.register")(function* (record: WorkerRecord) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const { spec } = record.state
          let teamEvent: { type: "created" | "updated"; team: TeamState } | undefined
          s.workers.set(spec.workerID, record)
          s.bySession.set(spec.sessionID, spec.workerID)
          const parentSet = s.byParent.get(spec.parentSessionID) ?? new Set<WorkerID>()
          parentSet.add(spec.workerID)
          s.byParent.set(spec.parentSessionID, parentSet)
          if (spec.name) s.byName.set(nameKey(spec.parentSessionID, spec.name), spec.workerID)
          if (spec.team) {
            const key = teamKey(spec.parentSessionID, spec.team)
            const teamSet = s.byTeam.get(key) ?? new Set<WorkerID>()
            teamSet.add(spec.workerID)
            s.byTeam.set(key, teamSet)
            const now = Date.now()
            const existing = s.teams.get(key)
            if (!existing) {
              const team = {
                parentSessionID: spec.parentSessionID,
                name: spec.team,
                description: spec.description,
                leadSessionID: spec.parentSessionID,
                agentType: spec.agent,
                createdAt: now,
                updatedAt: now,
              }
              s.teams.set(key, team)
              teamEvent = { type: "created", team }
            } else {
              const team = { ...existing, updatedAt: now }
              s.teams.set(key, team)
              teamEvent = { type: "updated", team }
            }
          }
          yield* persist(s)
          yield* SwarmMailbox.registerWorker(snapshot(record)).pipe(Effect.ignore)
          if (teamEvent) {
            const team = teamSnapshot(s, teamEvent.team)
            yield* publishSwarm(teamEvent.type === "created" ? Event.TeamCreated : Event.TeamUpdated, {
              parentSessionID: team.parentSessionID,
              name: team.name,
              team,
            })
          }
          yield* publishSwarm(Event.Spawned, {
            workerID: spec.workerID,
            parentSessionID: spec.parentSessionID,
            sessionID: spec.sessionID,
            agent: spec.agent,
            worker: snapshot(record),
          })
        }),
      )
    })

    const runWorkerRecord = Effect.fn("SwarmRuntime.runWorkerRecord")(function* (
      record: WorkerRecord,
      run: Effect.Effect<WorkerCompletion>,
      wait: boolean | undefined,
    ) {
      const workerID = record.state.spec.workerID
      const work = run.pipe(
        Effect.tap((completion) => {
          switch (completion.status) {
            case "completed":
              return setStatus(workerID, "completed", { result: { text: completion.text } })
            case "cancelled":
              return setStatus(workerID, "cancelled", { result: { text: completion.text } })
            case "failed":
              return setStatus(workerID, "failed", { result: { error: completion.error } })
          }
        }),
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const completion: WorkerCompletion = Cause.hasInterruptsOnly(cause)
              ? { status: "cancelled", text: "cancelled" }
              : { status: "failed", error: errorMessage(Cause.squash(cause)) }
            yield* setStatus(workerID, completion.status, {
              result: completion.status === "failed" ? { error: completion.error } : { text: completion.text },
            })
            return completion
          }),
        ),
      )

      yield* setStatus(workerID, "running")
      const fiber = yield* work.pipe(Effect.forkIn(scope))
      record.fiber = fiber
      yield* SwarmMailbox.touchWorker(snapshot(record)).pipe(Effect.ignore)

      if (wait === false) {
        return { workerID, sessionID: record.state.spec.sessionID, state: snapshot(record) }
      }

      const exit = yield* Fiber.await(fiber)
      if (Exit.isSuccess(exit)) {
        return { workerID, sessionID: record.state.spec.sessionID, state: snapshot(record), completion: exit.value }
      }
      const completion: WorkerCompletion = { status: "failed", error: errorMessage(exit.cause) }
      yield* setStatus(workerID, "failed", { result: { error: completion.error } })
      return { workerID, sessionID: record.state.spec.sessionID, state: snapshot(record), completion }
    })

    function ensureRemotePollers(target: State, ctx: InstanceContext): Effect.Effect<void> {
      return Effect.gen(function* () {
        const candidates = Array.from(target.workers.values()).filter((record) => {
          const spec = record.state.spec
          return (
            spec.backend === "remote" &&
            Boolean(spec.remoteEndpoint && spec.remoteID) &&
            !record.fiber &&
            !terminalStatus.has(record.state.status) &&
            !target.remotePollStarting.has(spec.workerID)
          )
        })
        yield* Effect.forEach(
          candidates,
          (record) =>
            Effect.gen(function* () {
              const liveness = yield* SwarmMailbox.inspectWorker(record.state.spec.workerID, ctx)
              if (liveness.alive && !liveness.ownedByCurrent) return
              target.remotePollStarting.add(record.state.spec.workerID)
              yield* runWorkerRecord(record, runRecoveredRemoteWorker(record), false).pipe(
                Effect.ensuring(Effect.sync(() => target.remotePollStarting.delete(record.state.spec.workerID))),
                Effect.ignore,
              )
            }),
          { concurrency: "unbounded", discard: true },
        )
      })
    }

    function runRecoveredRemoteWorker(record: WorkerRecord): Effect.Effect<WorkerCompletion> {
      const workerID = record.state.spec.workerID
      return Effect.gen(function* () {
        while (true) {
          const refreshed = yield* InstanceState.get(state)
          const latest = refreshed.workers.get(workerID)
          if (!latest) return { status: "cancelled", text: "remote worker disappeared" } satisfies WorkerCompletion
          if (terminalStatus.has(latest.state.status)) {
            return completionFromState(latest.state)
          }

          const { remoteID, outputPath } = latest.state.spec
          const remoteClient = yield* remoteClientFor(latest)
          if (!remoteClient || !remoteID) {
            return { status: "failed", error: "Remote subagent is missing remote metadata" } satisfies WorkerCompletion
          }

          const polled = yield* pollRemoteWorker(
            remoteClient,
            remoteID,
            latest.state.remoteCursor,
          ).pipe(Effect.exit)

          if (Exit.isFailure(polled)) {
            yield* updateProgress(workerID, `remote poll failed: ${errorMessage(Cause.squash(polled.cause))}`)
            yield* Effect.sleep("2 seconds")
            continue
          }

          if (polled.value.cursor !== undefined && polled.value.cursor !== latest.state.remoteCursor) {
            yield* updateRemoteCursor(workerID, polled.value.cursor)
          }

          for (const event of polled.value.events) {
            if (outputPath) yield* appendRemoteWorkerOutput(outputPath, workerID, remoteID, event).pipe(Effect.ignore)
            const completion = yield* applyRemoteEvent(workerID, event)
            if (completion) {
              yield* notifyParent(snapshot(latest), completion, {
                status: completion.status,
                includeResultForCompleted: true,
              }).pipe(Effect.ignore)
              return completion
            }
          }

          yield* Effect.sleep("250 millis")
        }
      })
    }

    function applyRemoteEvent(workerID: WorkerID, event: RemoteWorkerEvent): Effect.Effect<WorkerCompletion | undefined> {
      return Effect.gen(function* () {
        switch (event.type) {
          case "progress":
            yield* updateProgress(workerID, event.message)
            return undefined
          case "output":
            yield* updateProgress(workerID, event.text)
            return undefined
          case "completed":
            return { status: "completed", text: event.text } satisfies WorkerCompletion
          case "failed":
            return { status: "failed", error: event.error } satisfies WorkerCompletion
          case "cancelled":
            return { status: "cancelled", text: event.text ?? "cancelled" } satisfies WorkerCompletion
        }
      })
    }

    function completionFromState(state: WorkerState): WorkerCompletion {
      if (state.status === "failed") return { status: "failed", error: state.result?.error ?? "Remote subagent failed" }
      if (state.status === "cancelled") return { status: "cancelled", text: state.result?.text ?? "cancelled" }
      if (state.status === "interrupted") return { status: "failed", error: state.result?.error ?? "Remote subagent interrupted" }
      return { status: "completed", text: state.result?.text ?? "" }
    }

    function refreshActiveState(): Effect.Effect<State> {
      return Effect.gen(function* () {
        const s = yield* refreshPersistedLocked()
        yield* ensureRemotePollers(s, yield* InstanceState.context)
        return s
      })
    }

    const spawn: Interface["spawn"] = Effect.fn("SwarmRuntime.spawn")(function* (input) {
      const workerID = input.workerID ?? WorkerID.ascending()
      const spec: WorkerSpec = {
        workerID,
        parentSessionID: input.parentSessionID,
        sessionID: input.sessionID,
        agent: input.agent,
        ...(input.name ? { name: input.name } : {}),
        ...(input.team ? { team: input.team } : {}),
        prompt: input.prompt,
        description: input.description,
        ...(input.outputPath ? { outputPath: input.outputPath } : {}),
        contextStrategy: input.contextStrategy ?? "auto",
        permissionStrategy: input.permissionStrategy ?? "bubble",
        executionStrategy: input.executionStrategy ?? "oneshot",
        backend: input.backend ?? "in-process",
        ...(input.paneID ? { paneID: input.paneID } : {}),
        ...(input.paneExternalSession ? { paneExternalSession: input.paneExternalSession } : {}),
        ...(input.paneWindowTarget ? { paneWindowTarget: input.paneWindowTarget } : {}),
        ...(input.worktreeRoot ? { worktreeRoot: input.worktreeRoot } : {}),
        ...(input.worktreePath ? { worktreePath: input.worktreePath } : {}),
        ...(input.worktreeBranch ? { worktreeBranch: input.worktreeBranch } : {}),
        ...(input.remoteEndpoint ? { remoteEndpoint: input.remoteEndpoint } : {}),
        ...(input.remoteID ? { remoteID: input.remoteID } : {}),
        ...(input.remoteSessionURL ? { remoteSessionURL: input.remoteSessionURL } : {}),
        ...(input.remoteOutputPath ? { remoteOutputPath: input.remoteOutputPath } : {}),
        ...(input.model ? { model: input.model } : {}),
        ...(input.fork ? { fork: input.fork } : {}),
        ...(input.planModeRequired ? { planModeRequired: input.planModeRequired } : {}),
        ...(input.sourceToolCallID ? { sourceToolCallID: input.sourceToolCallID } : {}),
        ...(input.sourceMessageID ? { sourceMessageID: input.sourceMessageID } : {}),
      }
      const mailbox = yield* Queue.unbounded<WorkerInput>()
      const record: WorkerRecord = {
        state: initialWorkerState(spec),
        mailbox,
        mailboxItems: [],
        cancel: input.cancel,
      }
      yield* register(record)
      yield* setStatus(workerID, "booting")

      if (!input.run && !input.launch) {
        const completion: WorkerCompletion = { status: "failed", error: "SwarmRuntime.spawn requires run or launch" }
        yield* setStatus(workerID, "failed", { result: { error: completion.error } })
        return { workerID, sessionID: input.sessionID, state: snapshot(record), completion }
      }

      if (input.launch) {
        const launched = yield* input.launch.pipe(Effect.exit)
        if (Exit.isFailure(launched)) {
          const completion: WorkerCompletion = { status: "failed", error: errorMessage(Cause.squash(launched.cause)) }
          yield* (record.cancel ?? Effect.void).pipe(Effect.timeout("1 second"), Effect.ignore)
          yield* setStatus(workerID, "failed", { result: { error: completion.error } })
          return { workerID, sessionID: input.sessionID, state: snapshot(record), completion }
        }
      }

      if (!input.run) {
        yield* setStatus(workerID, "running")
        yield* SwarmMailbox.touchWorker(snapshot(record)).pipe(Effect.ignore)
        return { workerID, sessionID: input.sessionID, state: snapshot(record) }
      }

      return yield* runWorkerRecord(record, input.run, input.wait)
    })

    const adopt: Interface["adopt"] = Effect.fn("SwarmRuntime.adopt")(function* (input) {
      const s = yield* refreshPersistedLocked()
      const record = s.workers.get(input.workerID)
      if (!record) return yield* Effect.fail(new Error(`No subagent found for: ${input.workerID}`))
      record.cancel = input.cancel
      return yield* runWorkerRecord(record, input.run, input.wait)
    })

    const get: Interface["get"] = Effect.fn("SwarmRuntime.get")(function* (workerID) {
      const worker = (yield* refreshActiveState()).workers.get(workerID)
      return worker ? snapshot(worker) : undefined
    })

    const getBySession: Interface["getBySession"] = Effect.fn("SwarmRuntime.getBySession")(function* (sessionID) {
      const s = yield* refreshActiveState()
      const workerID = s.bySession.get(sessionID)
      if (!workerID) return undefined
      const worker = s.workers.get(workerID)
      return worker ? snapshot(worker) : undefined
    })

    const list: Interface["list"] = Effect.fn("SwarmRuntime.list")(function* (parentSessionID) {
      const s = yield* refreshActiveState()
      if (!parentSessionID) return Array.from(s.workers.values(), snapshot)
      const ids = s.byParent.get(parentSessionID) ?? new Set<WorkerID>()
      return Array.from(ids, (id) => s.workers.get(id))
        .filter((worker): worker is WorkerRecord => Boolean(worker))
        .map(snapshot)
    })

    const updateProgress: Interface["updateProgress"] = Effect.fn("SwarmRuntime.updateProgress")(
      function* (workerID, message) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            worker.state = {
              ...worker.state,
              lastProgress: message,
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishSwarm(Event.Progress, {
              workerID,
              parentSessionID: worker.state.spec.parentSessionID,
              sessionID: worker.state.spec.sessionID,
              message,
              worker: snapshot(worker),
            })
          }),
        )
      },
    )

    const recordResult: Interface["recordResult"] = Effect.fn("SwarmRuntime.recordResult")(
      function* (workerID, completion) {
        switch (completion.status) {
          case "completed":
            return yield* setStatus(workerID, "idle", { result: { text: completion.text } })
          case "cancelled":
            return yield* setStatus(workerID, "cancelled", { result: { text: completion.text } })
          case "failed":
            return yield* setStatus(workerID, "failed", { result: { error: completion.error } })
        }
      },
    )

    const updateRemoteMetadata: Interface["updateRemoteMetadata"] = Effect.fn("SwarmRuntime.updateRemoteMetadata")(
      function* (workerID, input) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${workerID}`))
            worker.state = {
              ...worker.state,
              spec: {
                ...worker.state.spec,
                ...(input.remoteEndpoint ? { remoteEndpoint: input.remoteEndpoint } : {}),
                ...(input.remoteID ? { remoteID: input.remoteID } : {}),
                ...(input.remoteSessionURL ? { remoteSessionURL: input.remoteSessionURL } : {}),
                ...(input.remoteOutputPath ? { remoteOutputPath: input.remoteOutputPath } : {}),
              },
              ...(input.remoteCursor ? { remoteCursor: input.remoteCursor } : {}),
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishStatus(worker, "remote subagent connected")
          }),
        )
      },
    )

    const updateRemoteCursor: Interface["updateRemoteCursor"] = Effect.fn("SwarmRuntime.updateRemoteCursor")(
      function* (workerID, cursor) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            worker.state = {
              ...worker.state,
              remoteCursor: cursor,
              updatedAt: Date.now(),
            }
            yield* persist(s)
          }),
        )
      },
    )

    const updateCurrentTool: Interface["updateCurrentTool"] = Effect.fn("SwarmRuntime.updateCurrentTool")(
      function* (workerID, tool) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            const { currentTool: _currentTool, ...rest } = worker.state
            worker.state = {
              ...(tool ? worker.state : rest),
              ...(tool ? { currentTool: tool } : {}),
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishSwarm(Event.ToolChanged, {
              workerID,
              parentSessionID: worker.state.spec.parentSessionID,
              sessionID: worker.state.spec.sessionID,
              ...(tool ? { tool } : {}),
              worker: snapshot(worker),
            })
          }),
        )
      },
    )

    const markPermissionPending: Interface["markPermissionPending"] = Effect.fn("SwarmRuntime.markPermissionPending")(
      function* (workerID, permissionID) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            worker.state = {
              ...worker.state,
              pendingPermissionID: permissionID,
              status: "waiting_permission",
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishSwarm(Event.PermissionChanged, {
              workerID,
              parentSessionID: worker.state.spec.parentSessionID,
              sessionID: worker.state.spec.sessionID,
              permissionID,
              worker: snapshot(worker),
            })
            yield* publishStatus(worker, "waiting for permission")
          }),
        )
      },
    )

    const clearPermissionPending: Interface["clearPermissionPending"] = Effect.fn(
      "SwarmRuntime.clearPermissionPending",
    )(function* (workerID, permissionID) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const worker = s.workers.get(workerID)
          if (!worker) return
          if (permissionID && worker.state.pendingPermissionID && worker.state.pendingPermissionID !== permissionID) return
          const { pendingPermissionID: _pendingPermissionID, ...rest } = worker.state
          worker.state = {
            ...rest,
            status: worker.state.status === "waiting_permission" ? "running" : worker.state.status,
            updatedAt: Date.now(),
          }
          yield* persist(s)
          yield* publishSwarm(Event.PermissionChanged, {
            workerID,
            parentSessionID: worker.state.spec.parentSessionID,
            sessionID: worker.state.spec.sessionID,
            worker: snapshot(worker),
          })
          if (worker.state.status === "running") yield* publishStatus(worker)
        }),
      )
    })

    const requestShutdown: Interface["requestShutdown"] = Effect.fn("SwarmRuntime.requestShutdown")(
      function* (workerID, requestID) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            worker.state = {
              ...worker.state,
              pendingShutdownID: requestID,
              lastProgress: "shutdown requested",
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishStatus(worker, "shutdown requested")
          }),
        )
      },
    )

    const approveShutdown: Interface["approveShutdown"] = Effect.fn("SwarmRuntime.approveShutdown")(
      function* (workerID, requestID) {
        yield* setStatus(workerID, "cancelled", {
          pendingShutdownID: undefined,
          result: { text: `shutdown approved: ${requestID}` },
          lastProgress: "shutdown approved",
          currentTool: undefined,
          pendingPermissionID: undefined,
        })
      },
    )

    const rejectShutdown: Interface["rejectShutdown"] = Effect.fn("SwarmRuntime.rejectShutdown")(
      function* (workerID, requestID, reason) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            const { pendingShutdownID: _pendingShutdownID, ...rest } = worker.state
            worker.state = {
              ...rest,
              status: worker.state.status === "cancelled" ? "idle" : worker.state.status,
              lastProgress: `shutdown rejected: ${reason}`,
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishStatus(worker, `shutdown rejected: ${requestID}`)
          }),
        )
      },
    )

    const stopAfterCurrentTurn: Interface["stopAfterCurrentTurn"] = Effect.fn("SwarmRuntime.stopAfterCurrentTurn")(
      function* (workerID, reason) {
        yield* setStatus(workerID, "cancelled", {
          result: { text: reason?.trim() ? `stopped: ${reason}` : "stopped" },
          lastProgress: reason?.trim() ? `stopped: ${reason}` : "stopped",
          currentTool: undefined,
          pendingPermissionID: undefined,
          pendingShutdownID: undefined,
          pendingPlanApprovalID: undefined,
        })
      },
    )

    const requestPlanApproval: Interface["requestPlanApproval"] = Effect.fn("SwarmRuntime.requestPlanApproval")(
      function* (workerID, requestID) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            worker.state = {
              ...worker.state,
              pendingPlanApprovalID: requestID,
              lastProgress: "plan approval requested",
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishStatus(worker, "plan approval requested")
          }),
        )
      },
    )

    const approvePlan: Interface["approvePlan"] = Effect.fn("SwarmRuntime.approvePlan")(function* (workerID, requestID) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const worker = s.workers.get(workerID)
          if (!worker) return
          const { pendingPlanApprovalID: _pendingPlanApprovalID, ...rest } = worker.state
          worker.state = {
            ...rest,
            lastProgress: `plan approved: ${requestID}`,
            updatedAt: Date.now(),
          }
          yield* persist(s)
          yield* publishStatus(worker, `plan approved: ${requestID}`)
        }),
      )
    })

    const rejectPlan: Interface["rejectPlan"] = Effect.fn("SwarmRuntime.rejectPlan")(
      function* (workerID, requestID, feedback) {
        return yield* withStateLock(
          Effect.gen(function* () {
            const s = yield* refreshPersisted()
            const worker = s.workers.get(workerID)
            if (!worker) return
            const { pendingPlanApprovalID: _pendingPlanApprovalID, ...rest } = worker.state
            worker.state = {
              ...rest,
              lastProgress: `plan rejected: ${feedback}`,
              updatedAt: Date.now(),
            }
            yield* persist(s)
            yield* publishStatus(worker, `plan rejected: ${requestID}`)
          }),
        )
      },
    )

    const resolveWorker = (s: State, input: Pick<SendInput, "to" | "parentSessionID">) => {
      const workerID = input.to as WorkerID
      const byWorker = s.workers.get(workerID)
      if (byWorker) return byWorker

      const bySessionID = s.bySession.get(input.to as SessionID)
      const bySession = bySessionID ? s.workers.get(bySessionID) : undefined
      if (bySession) return bySession

      if (input.parentSessionID) {
        const byScopedName = s.byName.get(nameKey(input.parentSessionID, input.to))
        const worker = byScopedName ? s.workers.get(byScopedName) : undefined
        if (worker) return worker
      }

      const matches = Array.from(s.workers.values()).filter((worker) => worker.state.spec.name === input.to)
      if (matches.length === 1) return matches[0]
      return undefined
    }

    const queueInput = Effect.fn("SwarmRuntime.queueInput")(function* (
      worker: WorkerRecord,
      input: Pick<SendInput, "message" | "summary" | "from">,
    ) {
      if (!acceptingInput(worker)) {
        return yield* Effect.fail(
          new Error(`Subagent ${worker.state.spec.workerID} is not accepting messages (${worker.state.status})`),
        )
      }
      const queued: WorkerInput = {
        id: Identifier.create("swi", "ascending"),
        message: input.message,
        createdAt: Date.now(),
        ...(input.summary ? { summary: input.summary } : {}),
        ...(input.from ? { from: input.from } : {}),
      }
      yield* Queue.offer(worker.mailbox, queued)
      worker.mailboxItems.push(queued)
      const mailboxSize = (worker.state.mailboxSize ?? 0) + 1
      worker.state = {
        ...worker.state,
        mailboxSize,
        lastProgress: input.summary ?? input.message,
        updatedAt: Date.now(),
      }
      const s = yield* InstanceState.get(state)
      yield* persist(s)
      yield* publishSwarm(Event.InputQueued, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        inputID: queued.id,
        mailboxSize,
        ...(queued.from ? { from: queued.from } : {}),
        ...(queued.summary ? { summary: queued.summary } : {}),
        worker: snapshot(worker),
      })
      return queued
    })

    const queueExternalInput = Effect.fn("SwarmRuntime.queueExternalInput")(function* (
      worker: WorkerRecord,
      input: Pick<SendInput, "message" | "summary" | "from">,
    ) {
      if (!acceptingInput(worker)) {
        return yield* Effect.fail(
          new Error(`Subagent ${worker.state.spec.workerID} is not accepting messages (${worker.state.status})`),
        )
      }

      const liveness = yield* SwarmMailbox.inspectWorker(worker.state.spec.workerID)
      if (!liveness.alive || liveness.ownedByCurrent) {
        return yield* Effect.fail(
          new Error(`Subagent ${worker.state.spec.workerID} is not accepting cross-process messages`),
        )
      }

      const queued = yield* SwarmMailbox.writeInput(snapshot(worker), input)
      const ctx = yield* InstanceState.context
      const ownerID = "heartbeat" in liveness ? liveness.heartbeat?.ownerID : undefined
      if (ownerID) {
        yield* SwarmPeer.notifyOwner(ctx, ownerID, {
          type: "inbox",
          workerID: worker.state.spec.workerID,
          inputID: queued.id,
        }).pipe(Effect.ignore)
      }
      const mailboxSize = (worker.state.mailboxSize ?? 0) + 1
      worker.state = {
        ...worker.state,
        mailboxSize,
        lastProgress: input.summary ?? input.message,
        updatedAt: Date.now(),
      }
      yield* publishSwarm(Event.InputQueued, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        inputID: queued.id,
        mailboxSize,
        ...(queued.from ? { from: queued.from } : {}),
        ...(queued.summary ? { summary: queued.summary } : {}),
        worker: snapshot(worker),
      })
      return queued
    })

    const queueRemoteInput = Effect.fn("SwarmRuntime.queueRemoteInput")(function* (
      worker: WorkerRecord,
      input: Pick<SendInput, "message" | "summary" | "from">,
    ) {
      if (!acceptingInput(worker)) {
        return yield* Effect.fail(
          new Error(`Subagent ${worker.state.spec.workerID} is not accepting messages (${worker.state.status})`),
        )
      }
      const { remoteID } = worker.state.spec
      const remoteClient = yield* remoteClientFor(worker)
      if (!remoteClient || !remoteID) {
        return yield* Effect.fail(new Error(`Remote subagent ${worker.state.spec.workerID} is missing remote metadata`))
      }
      const queued: WorkerInput = {
        id: Identifier.create("swi", "ascending"),
        message: input.message,
        createdAt: Date.now(),
        ...(input.summary ? { summary: input.summary } : {}),
        ...(input.from ? { from: input.from } : {}),
      }
      yield* sendRemoteWorkerInput(remoteClient, remoteID, queued)
      worker.mailboxItems.push(queued)
      const mailboxSize = (worker.state.mailboxSize ?? 0) + 1
      worker.state = {
        ...worker.state,
        mailboxSize,
        lastProgress: input.summary ?? input.message,
        updatedAt: Date.now(),
      }
      const s = yield* InstanceState.get(state)
      yield* persist(s)
      yield* publishSwarm(Event.InputQueued, {
        workerID: worker.state.spec.workerID,
        parentSessionID: worker.state.spec.parentSessionID,
        sessionID: worker.state.spec.sessionID,
        inputID: queued.id,
        mailboxSize,
        ...(queued.from ? { from: queued.from } : {}),
        ...(queued.summary ? { summary: queued.summary } : {}),
        worker: snapshot(worker),
      })
      return queued
    })

    const notifyTaskOwner = Effect.fn("SwarmRuntime.notifyTaskOwner")(function* (
      task: TeamTaskState,
      previousOwner?: string,
    ) {
      if (!task.owner || task.owner === previousOwner) return
      const s = yield* InstanceState.get(state)
      const worker = resolveWorker(s, {
        parentSessionID: task.parentSessionID,
        to: task.owner,
      })
      if (!worker) return
      if (worker.state.spec.team !== task.team) return
      if (!acceptingInput(worker)) return
      const assignment = {
        from: "task_update",
        summary: `assigned task #${task.id}`,
        message: [
          formatTaskAssignment(task, "team-lead"),
          "",
          `You have been assigned task #${task.id}: ${task.subject}`,
          "",
          task.description,
        ].join("\n"),
      }
      yield* (worker.state.spec.backend === "remote"
        ? queueRemoteInput(worker, assignment)
        : queueableLocalWorker(worker)
          ? queueInput(worker, assignment)
          : queueExternalInput(worker, assignment)
      ).pipe(Effect.ignore)
    })

    const createTask: Interface["createTask"] = Effect.fn("SwarmRuntime.createTask")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const { key, board } = taskBoard(s, input)
          const next = (s.taskSeq.get(key) ?? 0) + 1
          s.taskSeq.set(key, next)
          const now = Date.now()
          const task: TeamTaskState = {
            id: TeamTaskID.from(String(next)),
            parentSessionID: input.parentSessionID,
            ...(input.team ? { team: input.team } : {}),
            subject: input.subject,
            description: input.description,
            ...(input.activeForm ? { activeForm: input.activeForm } : {}),
            status: "pending",
            ...(input.owner ? { owner: input.owner } : {}),
            blocks: [],
            blockedBy: [],
            ...(input.metadata ? { metadata: input.metadata } : {}),
            createdAt: now,
            updatedAt: now,
          }
          board.set(task.id, task)
          yield* persist(s)
          yield* publishTask(Event.TaskCreated, task)
          yield* notifyTaskOwner(task)
          return taskSnapshot(task)
        }),
      )
    })

    const listTeamTasks: Interface["listTeamTasks"] = Effect.fn("SwarmRuntime.listTeamTasks")(function* (input) {
      const s = yield* refreshPersistedLocked()
      const board = s.tasks.get(taskBoardKey(input))
      if (!board) return []
      const completed = new Set(
        Array.from(board.values())
          .filter((task) => task.status === "completed")
          .map((task) => task.id),
      )
      return Array.from(board.values())
        .toSorted((a, b) => Number(a.id) - Number(b.id) || a.id.localeCompare(b.id))
        .map((task) => ({
          ...taskSnapshot(task),
          blockedBy: task.blockedBy.filter((id) => !completed.has(id)),
        }))
    })

    const getTeamTask: Interface["getTeamTask"] = Effect.fn("SwarmRuntime.getTeamTask")(function* (input) {
      const s = yield* refreshPersistedLocked()
      const task = s.tasks.get(taskBoardKey(input))?.get(TeamTaskID.from(input.taskID))
      return task ? taskSnapshot(task) : undefined
    })

    const updateTeamTask: Interface["updateTeamTask"] = Effect.fn("SwarmRuntime.updateTeamTask")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
      const s = yield* refreshPersisted()
      const { board } = taskBoard(s, input)
      const id = TeamTaskID.from(input.taskID)
      const existing = board.get(id)
      if (!existing) {
        return {
          success: false,
          taskID: input.taskID,
          updatedFields: [],
          error: "Task not found",
        }
      }

      if (input.status === "deleted") {
        board.delete(id)
        for (const task of board.values()) {
          const blocks = task.blocks.filter((block) => block !== id)
          const blockedBy = task.blockedBy.filter((blocker) => blocker !== id)
          if (blocks.length === task.blocks.length && blockedBy.length === task.blockedBy.length) continue
          const updated = { ...task, blocks, blockedBy, updatedAt: Date.now() }
          board.set(task.id, updated)
          yield* publishTask(Event.TaskUpdated, updated)
        }
        yield* persist(s)
        yield* publishTask(Event.TaskDeleted, existing)
        return {
          success: true,
          taskID: input.taskID,
          updatedFields: ["deleted"],
          deleted: taskSnapshot(existing),
          statusChange: { from: existing.status, to: "deleted" as const },
        }
      }

      const updatedFields: string[] = []
      const previousOwner = existing.owner
      let task = existing
      const patch: {
        subject?: string
        description?: string
        activeForm?: string
        status?: TeamTaskStatus
        owner?: string
        metadata?: Record<string, unknown>
      } = {}
      if (input.subject !== undefined && input.subject !== existing.subject) {
        patch.subject = input.subject
        updatedFields.push("subject")
      }
      if (input.description !== undefined && input.description !== existing.description) {
        patch.description = input.description
        updatedFields.push("description")
      }
      if (input.activeForm !== undefined && input.activeForm !== existing.activeForm) {
        patch.activeForm = input.activeForm
        updatedFields.push("activeForm")
      }
      if (input.owner !== undefined && input.owner !== existing.owner) {
        patch.owner = input.owner
        updatedFields.push("owner")
      }
      if (input.status !== undefined && input.status !== existing.status) {
        patch.status = input.status
        updatedFields.push("status")
      }
      if (input.metadata !== undefined) {
        const metadata = { ...(existing.metadata ?? {}) }
        for (const [key, value] of Object.entries(input.metadata)) {
          if (value === null) delete metadata[key]
          else metadata[key] = value
        }
        patch.metadata = metadata
        updatedFields.push("metadata")
      }

      if (Object.keys(patch).length > 0) {
        task = { ...task, ...patch, updatedAt: Date.now() }
        board.set(id, task)
      }

      const addRelation = (fromID: TeamTaskID, toID: TeamTaskID) => {
        const from = board.get(fromID)
        const to = board.get(toID)
        if (!from || !to) return false
        let changed = false
        if (!from.blocks.includes(toID)) {
          const updated = { ...from, blocks: [...from.blocks, toID], updatedAt: Date.now() }
          board.set(fromID, updated)
          if (fromID === id) task = updated
          changed = true
        }
        const latestTo = board.get(toID)!
        if (!latestTo.blockedBy.includes(fromID)) {
          const updated = { ...latestTo, blockedBy: [...latestTo.blockedBy, fromID], updatedAt: Date.now() }
          board.set(toID, updated)
          if (toID === id) task = updated
          changed = true
        }
        return changed
      }

      if (input.addBlocks?.length) {
        const changed = input.addBlocks
          .map((blockID) => addRelation(id, TeamTaskID.from(blockID)))
          .some(Boolean)
        if (changed) updatedFields.push("blocks")
      }
      if (input.addBlockedBy?.length) {
        const changed = input.addBlockedBy
          .map((blockerID) => addRelation(TeamTaskID.from(blockerID), id))
          .some(Boolean)
        if (changed) updatedFields.push("blockedBy")
      }

      if (updatedFields.length > 0) {
        yield* persist(s)
        yield* publishTask(Event.TaskUpdated, task)
        if (input.status === "completed" && existing.status !== "completed") {
          yield* publishSwarm(Event.TaskCompleted, {
            parentSessionID: task.parentSessionID,
            ...(task.team ? { team: task.team } : {}),
            task: taskSnapshot(task),
            completedBy: task.owner,
          })
        }
        yield* notifyTaskOwner(task, previousOwner)
      }

      return {
        success: true,
        taskID: input.taskID,
        updatedFields,
        task: taskSnapshot(task),
        ...(input.status !== undefined && input.status !== existing.status
          ? { statusChange: { from: existing.status, to: input.status as TeamTaskStatus } }
          : {}),
      }
        }),
      )
    })

    const claimNextTask: Interface["claimNextTask"] = Effect.fn("SwarmRuntime.claimNextTask")(function* (workerID) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const worker = s.workers.get(workerID)
          if (!worker?.state.spec.team) return undefined
          if (!acceptingInput(worker)) return undefined
          const key = teamKey(worker.state.spec.parentSessionID, worker.state.spec.team)
          const board = s.tasks.get(key)
          if (!board) return undefined
          const task = findAvailableTask(board)
          if (!task) return undefined

          const claimed: TeamTaskState = {
            ...task,
            owner: worker.state.spec.name ?? worker.state.spec.workerID,
            status: "in_progress",
            updatedAt: Date.now(),
          }
          board.set(task.id, claimed)
          yield* persist(s)
          yield* publishTask(Event.TaskUpdated, claimed)
          return taskSnapshot(claimed)
        }),
      )
    })

    const resolve: Interface["resolve"] = Effect.fn("SwarmRuntime.resolve")(function* (input) {
      const s = yield* refreshActiveState()
      const worker = resolveWorker(s, input)
      return worker ? snapshot(worker) : undefined
    })

    const createTeam: Interface["createTeam"] = Effect.fn("SwarmRuntime.createTeam")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const key = teamKey(input.parentSessionID, input.name)
          const now = Date.now()
          const existing = s.teams.get(key)
          const team: TeamState = {
            parentSessionID: input.parentSessionID,
            name: input.name,
            description: input.description ?? existing?.description,
            leadSessionID: input.leadSessionID ?? existing?.leadSessionID ?? input.parentSessionID,
            agentType: input.agentType ?? existing?.agentType,
            createdAt: existing?.createdAt ?? now,
            updatedAt: now,
          }
          s.teams.set(key, team)
          if (!existing) {
            s.tasks.set(key, new Map())
            s.taskSeq.set(key, 0)
          }
          const snapshot = teamSnapshot(s, team)
          yield* persist(s)
          yield* publishSwarm(existing ? Event.TeamUpdated : Event.TeamCreated, {
            parentSessionID: snapshot.parentSessionID,
            name: snapshot.name,
            team: snapshot,
          })
          return snapshot
        }),
      )
    })

    const listTeams: Interface["listTeams"] = Effect.fn("SwarmRuntime.listTeams")(function* (parentSessionID) {
      const s = yield* refreshPersistedLocked()
      return Array.from(s.teams.values())
        .filter((team) => !parentSessionID || team.parentSessionID === parentSessionID)
        .map((team) => teamSnapshot(s, team))
    })

    const deleteTeam: Interface["deleteTeam"] = Effect.fn("SwarmRuntime.deleteTeam")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const key = teamKey(input.parentSessionID, input.name)
          const team = s.teams.get(key)
          if (!team) return undefined
          const snapshot = teamSnapshot(s, team)
          const activeWorkers = snapshot.workerIDs
            .map((workerID) => s.workers.get(workerID))
            .filter((worker): worker is WorkerRecord => Boolean(worker))
            .filter(acceptingInput)
          if (activeWorkers.length > 0 && input.cancelWorkers !== true) {
            const memberNames = activeWorkers
              .map((worker) => worker.state.spec.name ?? worker.state.spec.workerID)
              .join(", ")
            return yield* Effect.fail(
              new Error(
                `Cannot delete team with ${activeWorkers.length} active member(s): ${memberNames}. Use shutdown_request to gracefully terminate teammates first, or pass cancel_workers=true to force cancel active workers.`,
              ),
            )
          }
          if (input.cancelWorkers === true) {
            yield* Effect.forEach(activeWorkers, (worker) => {
              return interruptWorker(worker)
            }, {
              concurrency: "unbounded",
              discard: true,
            })
            for (const worker of activeWorkers) {
              const previousStatus = worker.state.status
              worker.state = {
                ...worker.state,
                status: "cancelled",
                result: { text: "cancelled" },
                currentTool: undefined,
                pendingPermissionID: undefined,
                pendingShutdownID: undefined,
                pendingPlanApprovalID: undefined,
                updatedAt: Date.now(),
              }
              yield* SwarmMailbox.clearWorker(worker.state.spec.workerID).pipe(Effect.ignore)
              yield* publishStatus(worker, "cancelled")
              yield* publishWorkerStopped(worker, previousStatus)
            }
          }
          s.teams.delete(key)
          s.byTeam.delete(key)
          s.tasks.delete(key)
          s.taskSeq.delete(key)
          yield* persist(s)
          yield* publishSwarm(Event.TeamDeleted, {
            parentSessionID: snapshot.parentSessionID,
            name: snapshot.name,
            team: snapshot,
          })
          return snapshot
        }),
      )
    })

    const wait: Interface["wait"] = Effect.fn("SwarmRuntime.wait")(function* (input) {
      const deadline = input.timeoutMS === undefined ? undefined : Date.now() + input.timeoutMS
      while (true) {
        const s = yield* refreshActiveState()
        const worker = resolveWorker(s, input)
        if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${input.to}`))
        if (waitComplete.has(worker.state.status)) return snapshot(worker)

        const remaining = deadline === undefined ? undefined : deadline - Date.now()
        if (remaining !== undefined && remaining <= 0) return snapshot(worker)
        yield* Effect.sleep(`${Math.max(1, Math.min(25, remaining ?? 25))} millis`)
      }
    })

    const sendInput: Interface["sendInput"] = Effect.fn("SwarmRuntime.sendInput")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const worker = resolveWorker(s, input)
          if (!worker) return yield* Effect.fail(new Error(`No running subagent found for: ${input.to}`))
          return yield* (worker.state.spec.backend === "remote"
            ? queueRemoteInput(worker, input)
            : queueableLocalWorker(worker)
              ? queueInput(worker, input)
              : queueExternalInput(worker, input))
        }),
      )
    })

    const broadcast: Interface["broadcast"] = Effect.fn("SwarmRuntime.broadcast")(function* (input) {
      return yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* refreshPersisted()
          const workers = input.parentSessionID
            ? Array.from(s.byTeam.get(teamKey(input.parentSessionID, input.team)) ?? [], (id) => s.workers.get(id)).filter(
                (worker): worker is WorkerRecord => Boolean(worker),
              )
            : Array.from(s.workers.values()).filter((worker) => worker.state.spec.team === input.team)
          const accepting = workers.filter(acceptingInput)
          if (accepting.length === 0) {
            return yield* Effect.fail(new Error(`No accepting subagents found for team: ${input.team}`))
          }
          return yield* Effect.forEach(
            accepting,
            (worker) =>
              worker.state.spec.backend === "remote"
                ? queueRemoteInput(worker, input)
                : queueableLocalWorker(worker)
                  ? queueInput(worker, input)
                  : queueExternalInput(worker, input),
            { concurrency: "unbounded" },
          )
        }),
      )
    })

    const awaitInput: Interface["awaitInput"] = Effect.fn("SwarmRuntime.awaitInput")(function* (workerID) {
      const s = yield* InstanceState.get(state)
      const worker = s.workers.get(workerID)
      if (!worker) return yield* Effect.never
      yield* setStatus(workerID, "idle")
      const fileSignal = yield* Queue.sliding<void>(1)
      Queue.offerUnsafe(fileSignal, undefined)
      const peerScope = yield* Scope.make()
      const peerSubscription = yield* Scope.provide(peerScope)(PubSub.subscribe(s.peer))
      const peerSignal = Effect.gen(function* () {
        while (true) {
          const message = yield* PubSub.take(peerSubscription)
          if (message.type === "inbox" && message.workerID === workerID) return
        }
      })
      return yield* Effect.acquireUseRelease(
        SwarmMailbox.watchInbox(snapshot(worker), () => {
          Queue.offerUnsafe(fileSignal, undefined)
        }),
        () =>
          Effect.gen(function* () {
            while (true) {
              const signal = yield* Effect.raceAll([
                Queue.take(worker.mailbox).pipe(Effect.map((input) => ({ type: "local" as const, input }))),
                Queue.take(fileSignal).pipe(Effect.as({ type: "file" as const })),
                peerSignal.pipe(Effect.as({ type: "peer" as const })),
              ]).pipe(
                Effect.timeout("250 millis"),
                Effect.catchCause(() =>
                  Effect.succeed(
                    undefined as
                      | { type: "local"; input: WorkerInput }
                      | { type: "file" }
                      | { type: "peer" }
                      | undefined,
                  ),
                ),
              )
              if (signal?.type === "local") {
                const input = takePrioritizedMailboxItem(worker)
                if (!input) continue
                yield* withStateLock(
                  Effect.gen(function* () {
                    const refreshed = yield* refreshPersisted()
                    const latest = refreshed.workers.get(workerID) ?? worker
                    const mailboxSize = latest.mailboxItems.length
                    latest.state = {
                      ...latest.state,
                      status: "running",
                      mailboxSize,
                      lastProgress: input.summary ?? input.message,
                      updatedAt: Date.now(),
                    }
                    yield* persist(refreshed)
                    yield* publishStatus(latest, input.summary ?? "received input")
                  }),
                )
                return input
              }

              const fileInput = yield* SwarmMailbox.takeInput(snapshot(worker)).pipe(
                Effect.catch(() => Effect.succeed(undefined)),
              )
              if (fileInput) {
                yield* withStateLock(
                  Effect.gen(function* () {
                    const refreshed = yield* refreshPersisted()
                    const latest = refreshed.workers.get(workerID) ?? worker
                    latest.state = {
                      ...latest.state,
                      status: "running",
                      mailboxSize: latest.mailboxItems.length || undefined,
                      lastProgress: fileInput.summary ?? fileInput.message,
                      updatedAt: Date.now(),
                    }
                    yield* persist(refreshed)
                    yield* publishStatus(latest, fileInput.summary ?? "received input")
                  }),
                )
                return fileInput
              }

              const task = yield* claimNextTask(workerID)
              if (!task) continue
              const claimed: WorkerInput = {
                id: Identifier.create("swi", "ascending"),
                message: formatTaskInput(task),
                summary: `claimed task #${task.id}`,
                from: "task_board",
                createdAt: Date.now(),
              }
              yield* withStateLock(
                Effect.gen(function* () {
                  const refreshed = yield* refreshPersisted()
                  const latest = refreshed.workers.get(workerID) ?? worker
                  latest.state = {
                    ...latest.state,
                    status: "running",
                    mailboxSize: latest.mailboxItems.length || undefined,
                    lastProgress: claimed.summary,
                    updatedAt: Date.now(),
                  }
                  yield* persist(refreshed)
                  yield* publishStatus(latest, claimed.summary)
                }),
              )
              return claimed
            }
          }),
        (stop) => Effect.all([Effect.sync(stop), Scope.close(peerScope, Exit.void)], { discard: true }),
      )
    })

    const cancel: Interface["cancel"] = Effect.fn("SwarmRuntime.cancel")(function* (workerID) {
      const s = yield* InstanceState.get(state)
      const worker = s.workers.get(workerID)
      if (!worker) return
      yield* interruptWorker(worker)
      yield* setStatus(workerID, "cancelled", { result: { text: "cancelled" } })
    })

    const controlPane: Interface["controlPane"] = Effect.fn("SwarmRuntime.controlPane")(function* (workerID, action) {
      const s = yield* refreshPersistedLocked()
      const worker = s.workers.get(workerID)
      if (!worker) return yield* Effect.fail(new Error(`No subagent found for: ${workerID}`))

      const spec = worker.state.spec
      if (!spec.paneID || (spec.backend !== "tmux" && spec.backend !== "iterm2")) {
        return yield* Effect.fail(new Error(`Subagent ${workerID} does not have an external pane`))
      }

      const backend = backendByType(spec.backend)
      if (!backend.supportsHideShow) {
        return yield* Effect.fail(new Error(`${backend.displayName} does not support hide/show pane control`))
      }

      const result =
        action === "hide"
          ? yield* Effect.tryPromise({
              try: () => backend.hidePane!(spec.paneID!, spec.paneExternalSession),
              catch: (error) => new Error(errorMessage(error)),
            })
          : yield* Effect.gen(function* () {
              if (!spec.paneWindowTarget) {
                return yield* Effect.fail(new Error(`Subagent ${workerID} is missing its tmux pane window target`))
              }
              return yield* Effect.tryPromise({
                try: () => backend.showPane!(spec.paneID!, spec.paneWindowTarget!, spec.paneExternalSession),
                catch: (error) => new Error(errorMessage(error)),
              })
            })

      if (!result) return yield* Effect.fail(new Error(`Failed to ${action} pane for subagent ${workerID}`))

      return yield* withStateLock(
        Effect.gen(function* () {
          const refreshed = yield* refreshPersisted()
          const latest = refreshed.workers.get(workerID)
          if (!latest) return yield* Effect.fail(new Error(`No subagent found for: ${workerID}`))
          latest.state = {
            ...latest.state,
            paneHidden: action === "hide",
            lastProgress: `pane ${action === "hide" ? "hidden" : "shown"}`,
            updatedAt: Date.now(),
          }
          yield* persist(refreshed)
          yield* publishStatus(latest, latest.state.lastProgress)
          return snapshot(latest)
        }),
      )
    })

    const reload: Interface["reload"] = Effect.fn("SwarmRuntime.reload")(function* () {
      const ctx = yield* InstanceState.context
      const s = yield* withStateLock(
        Effect.gen(function* () {
          const s = yield* InstanceState.get(state)
          yield* Effect.forEach(
            Array.from(s.workers.values()),
            (worker) => (worker.fiber ? Fiber.interrupt(worker.fiber) : Effect.void),
            { concurrency: "unbounded", discard: true },
          )
          clearState(s)
          yield* loadPersisted(s, ctx, { interruptOwnedWorkers: true })
          return s
        }),
      )
      yield* ensureRemotePollers(s, ctx)
    })

    return Service.of({
      spawn,
      adopt,
      get,
      getBySession,
      resolve,
      list,
      wait,
      createTeam,
      listTeams,
      deleteTeam,
      createTask,
      listTeamTasks,
      getTeamTask,
      updateTeamTask,
      claimNextTask,
      updateProgress,
      updateRemoteMetadata,
      updateRemoteCursor,
      updateCurrentTool,
      markPermissionPending,
      clearPermissionPending,
      requestShutdown,
      approveShutdown,
      rejectShutdown,
      stopAfterCurrentTurn,
      requestPlanApproval,
      approvePlan,
      rejectPlan,
      recordResult,
      sendInput,
      broadcast,
      awaitInput,
      controlPane,
      cancel,
      reload,
    })
  }),
)

export const defaultLayer = layer.pipe(Layer.provide(Bus.layer), Layer.provide(Storage.defaultLayer))

const nameKey = (parentSessionID: SessionID, name: string) => `${parentSessionID}:${name}`
const teamKey = (parentSessionID: SessionID, team: string) => `${parentSessionID}:${team}`

const unassignWorkerTasksInState = (state: State, worker: WorkerRecord): TeamTaskState[] => {
  const { spec } = worker.state
  if (!spec.team) return []

  const board = state.tasks.get(teamKey(spec.parentSessionID, spec.team))
  if (!board) return []

  const ownerKeys = new Set([spec.workerID, spec.name].filter((value): value is string => Boolean(value)))
  const updated: TeamTaskState[] = []
  for (const task of board.values()) {
    if (task.status === "completed") continue
    if (!task.owner || !ownerKeys.has(task.owner)) continue

    const { owner: _owner, ...rest } = task
    const next: TeamTaskState = {
      ...rest,
      status: "pending",
      updatedAt: Date.now(),
    }
    board.set(task.id, next)
    updated.push(next)
  }
  return updated
}

const takePrioritizedMailboxItem = (worker: WorkerRecord): WorkerInput | undefined => {
  if (worker.mailboxItems.length === 0) return undefined

  let index = worker.mailboxItems.findIndex(isShutdownRequestInput)
  if (index === -1) index = worker.mailboxItems.findIndex((input) => input.from === "team-lead")
  if (index === -1) index = 0

  return worker.mailboxItems.splice(index, 1)[0]
}

const isShutdownRequestInput = (input: WorkerInput) =>
  input.message.includes("<type>shutdown_request</type>") || input.message.includes('"type":"shutdown_request"')

const formatTaskAssignment = (task: TeamTaskState, assignedBy: string) =>
  JSON.stringify(
    {
      type: "task_assignment",
      taskId: task.id,
      subject: task.subject,
      description: task.description,
      assignedBy,
      timestamp: new Date(task.updatedAt).toISOString(),
    },
    null,
    2,
  )

export * as SwarmRuntime from "./runtime"
