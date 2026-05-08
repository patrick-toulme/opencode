import { describe, expect, test } from "bun:test"
import { Result, Schema } from "effect"
import { toJsonSchema } from "../../src/util/effect-zod"

// Each tool exports its parameters schema at module scope so this test can
// import them without running the tool's Effect-based init. The JSON Schema
// snapshot captures what the LLM sees; the parse assertions pin down the
// accepts/rejects contract. `toJsonSchema` is the same helper `session/
// prompt.ts` uses to emit tool schemas to the LLM, so the snapshots stay
// byte-identical regardless of whether a tool has migrated from zod to Schema.

import { Parameters as ApplyPatch } from "../../src/tool/apply_patch"
import { Parameters as Edit } from "../../src/tool/edit"
import { Parameters as Glob } from "../../src/tool/glob"
import { Parameters as Grep } from "../../src/tool/grep"
import { Parameters as Invalid } from "../../src/tool/invalid"
import { Parameters as Lsp } from "../../src/tool/lsp"
import { Parameters as Plan } from "../../src/tool/plan"
import { Parameters as Question } from "../../src/tool/question"
import { Parameters as Read } from "../../src/tool/read"
import { RemoteTriggerParameters as RemoteTrigger } from "../../src/tool/remote_trigger"
import {
  DeleteScheduledTaskParameters as DeleteScheduledTask,
  ListScheduledTasksParameters as ListScheduledTasks,
  ScheduleTaskParameters as ScheduleTask,
} from "../../src/tool/schedule"
import { ListPeersParameters as ListPeers, Parameters as SendMessage } from "../../src/tool/send_message"
import { Parameters as Shell } from "../../src/tool/shell"
import { Parameters as Skill } from "../../src/tool/skill"
import { Parameters as Task } from "../../src/tool/task"
import {
  BroadcastParameters as Broadcast,
  CancelTaskParameters as CancelTask,
  ControlTaskPaneParameters as ControlTaskPane,
  CreateTaskParameters as CreateTask,
  CreateTeamParameters as CreateTeam,
  DeleteTeamParameters as DeleteTeam,
  GetTaskParameters as GetTask,
  ListTasksParameters as ListTasks,
  ListTeamTasksParameters as ListTeamTasks,
  ListTeamsParameters as ListTeams,
  ReadTaskOutputParameters as ReadTaskOutput,
  StopTaskParameters as StopTask,
  UpdateTaskParameters as UpdateTask,
  WaitTaskParameters as WaitTask,
} from "../../src/tool/task_control"
import { Parameters as Todo } from "../../src/tool/todo"
import { Parameters as WebFetch } from "../../src/tool/webfetch"
import { Parameters as WebSearch } from "../../src/tool/websearch"
import { Parameters as Write } from "../../src/tool/write"

const parse = <S extends Schema.Decoder<unknown>>(schema: S, input: unknown): S["Type"] =>
  Schema.decodeUnknownSync(schema)(input)

const accepts = (schema: Schema.Decoder<unknown>, input: unknown): boolean =>
  Result.isSuccess(Schema.decodeUnknownResult(schema)(input))

describe("tool parameters", () => {
  describe("JSON Schema (wire shape)", () => {
    test("apply_patch", () => expect(toJsonSchema(ApplyPatch)).toMatchSnapshot())
    test("bash", () => expect(toJsonSchema(Shell)).toMatchSnapshot())
    test("broadcast", () => expect(toJsonSchema(Broadcast)).toMatchSnapshot())
    test("cancel_task", () => expect(toJsonSchema(CancelTask)).toMatchSnapshot())
    test("control_task_pane", () => expect(toJsonSchema(ControlTaskPane)).toMatchSnapshot())
    test("create_task", () => expect(toJsonSchema(CreateTask)).toMatchSnapshot())
    test("create_team", () => expect(toJsonSchema(CreateTeam)).toMatchSnapshot())
    test("delete_team", () => expect(toJsonSchema(DeleteTeam)).toMatchSnapshot())
    test("edit", () => expect(toJsonSchema(Edit)).toMatchSnapshot())
    test("glob", () => expect(toJsonSchema(Glob)).toMatchSnapshot())
    test("grep", () => expect(toJsonSchema(Grep)).toMatchSnapshot())
    test("invalid", () => expect(toJsonSchema(Invalid)).toMatchSnapshot())
    test("get_task", () => expect(toJsonSchema(GetTask)).toMatchSnapshot())
    test("list_team_tasks", () => expect(toJsonSchema(ListTeamTasks)).toMatchSnapshot())
    test("list_tasks", () => expect(toJsonSchema(ListTasks)).toMatchSnapshot())
    test("list_teams", () => expect(toJsonSchema(ListTeams)).toMatchSnapshot())
    test("list_peers", () => expect(toJsonSchema(ListPeers)).toMatchSnapshot())
    test("lsp", () => expect(toJsonSchema(Lsp)).toMatchSnapshot())
    test("plan", () => expect(toJsonSchema(Plan)).toMatchSnapshot())
    test("question", () => expect(toJsonSchema(Question)).toMatchSnapshot())
    test("read", () => expect(toJsonSchema(Read)).toMatchSnapshot())
    test("read_task_output", () => expect(toJsonSchema(ReadTaskOutput)).toMatchSnapshot())
    test("remote_trigger", () => expect(toJsonSchema(RemoteTrigger)).toMatchSnapshot())
    test("schedule_task", () => expect(toJsonSchema(ScheduleTask)).toMatchSnapshot())
    test("list_scheduled_tasks", () => expect(toJsonSchema(ListScheduledTasks)).toMatchSnapshot())
    test("delete_scheduled_task", () => expect(toJsonSchema(DeleteScheduledTask)).toMatchSnapshot())
    test("send_message", () => expect(toJsonSchema(SendMessage)).toMatchSnapshot())
    test("skill", () => expect(toJsonSchema(Skill)).toMatchSnapshot())
    test("stop_task", () => expect(toJsonSchema(StopTask)).toMatchSnapshot())
    test("task", () => expect(toJsonSchema(Task)).toMatchSnapshot())
    test("todo", () => expect(toJsonSchema(Todo)).toMatchSnapshot())
    test("webfetch", () => expect(toJsonSchema(WebFetch)).toMatchSnapshot())
    test("websearch", () => expect(toJsonSchema(WebSearch)).toMatchSnapshot())
    test("update_task", () => expect(toJsonSchema(UpdateTask)).toMatchSnapshot())
    test("wait_task", () => expect(toJsonSchema(WaitTask)).toMatchSnapshot())
    test("write", () => expect(toJsonSchema(Write)).toMatchSnapshot())
  })

  describe("apply_patch", () => {
    test("accepts patchText", () => {
      expect(parse(ApplyPatch, { patchText: "*** Begin Patch\n*** End Patch" })).toEqual({
        patchText: "*** Begin Patch\n*** End Patch",
      })
    })
    test("rejects missing patchText", () => {
      expect(accepts(ApplyPatch, {})).toBe(false)
    })
    test("rejects non-string patchText", () => {
      expect(accepts(ApplyPatch, { patchText: 123 })).toBe(false)
    })
  })

  describe("shell", () => {
    test("accepts minimum: command + description", () => {
      expect(parse(Shell, { command: "ls", description: "list" })).toEqual({ command: "ls", description: "list" })
    })
    test("accepts optional timeout + workdir", () => {
      const parsed = parse(Shell, { command: "ls", description: "list", timeout: 5000, workdir: "/tmp" })
      expect(parsed.timeout).toBe(5000)
      expect(parsed.workdir).toBe("/tmp")
    })
    test("rejects missing description", () => {
      expect(accepts(Shell, { command: "ls" })).toBe(false)
    })
    test("rejects missing command", () => {
      expect(accepts(Shell, { description: "list" })).toBe(false)
    })
  })

  describe("edit", () => {
    test("accepts all four fields", () => {
      expect(parse(Edit, { filePath: "/a", oldString: "x", newString: "y", replaceAll: true })).toEqual({
        filePath: "/a",
        oldString: "x",
        newString: "y",
        replaceAll: true,
      })
    })
    test("replaceAll is optional", () => {
      const parsed = parse(Edit, { filePath: "/a", oldString: "x", newString: "y" })
      expect(parsed.replaceAll).toBeUndefined()
    })
    test("rejects missing filePath", () => {
      expect(accepts(Edit, { oldString: "x", newString: "y" })).toBe(false)
    })
  })

  describe("glob", () => {
    test("accepts pattern-only", () => {
      expect(parse(Glob, { pattern: "**/*.ts" })).toEqual({ pattern: "**/*.ts" })
    })
    test("accepts optional path", () => {
      expect(parse(Glob, { pattern: "**/*.ts", path: "/tmp" }).path).toBe("/tmp")
    })
    test("rejects missing pattern", () => {
      expect(accepts(Glob, {})).toBe(false)
    })
  })

  describe("grep", () => {
    test("accepts pattern-only", () => {
      expect(parse(Grep, { pattern: "TODO" })).toEqual({ pattern: "TODO" })
    })
    test("accepts optional path + include", () => {
      const parsed = parse(Grep, { pattern: "TODO", path: "/tmp", include: "*.ts" })
      expect(parsed.path).toBe("/tmp")
      expect(parsed.include).toBe("*.ts")
    })
    test("rejects missing pattern", () => {
      expect(accepts(Grep, {})).toBe(false)
    })
  })

  describe("invalid", () => {
    test("accepts tool + error", () => {
      expect(parse(Invalid, { tool: "foo", error: "bar" })).toEqual({ tool: "foo", error: "bar" })
    })
    test("rejects missing fields", () => {
      expect(accepts(Invalid, { tool: "foo" })).toBe(false)
      expect(accepts(Invalid, { error: "bar" })).toBe(false)
    })
  })

  describe("lsp", () => {
    test("accepts all fields", () => {
      const parsed = parse(Lsp, { operation: "hover", filePath: "/a.ts", line: 1, character: 1 })
      expect(parsed.operation).toBe("hover")
    })
    test("rejects line < 1", () => {
      expect(accepts(Lsp, { operation: "hover", filePath: "/a.ts", line: 0, character: 1 })).toBe(false)
    })
    test("rejects character < 1", () => {
      expect(accepts(Lsp, { operation: "hover", filePath: "/a.ts", line: 1, character: 0 })).toBe(false)
    })
    test("rejects unknown operation", () => {
      expect(accepts(Lsp, { operation: "bogus", filePath: "/a.ts", line: 1, character: 1 })).toBe(false)
    })
  })

  describe("plan", () => {
    test("accepts empty object", () => {
      expect(parse(Plan, {})).toEqual({})
    })
  })

  describe("question", () => {
    test("accepts questions array", () => {
      const parsed = parse(Question, {
        questions: [
          {
            question: "pick one",
            header: "Header",
            custom: false,
            options: [{ label: "a", description: "desc" }],
          },
        ],
      })
      expect(parsed.questions.length).toBe(1)
    })
    test("rejects missing questions", () => {
      expect(accepts(Question, {})).toBe(false)
    })
  })

  describe("read", () => {
    test("accepts filePath-only", () => {
      expect(parse(Read, { filePath: "/a" }).filePath).toBe("/a")
    })
    test("accepts optional offset + limit", () => {
      const parsed = parse(Read, { filePath: "/a", offset: 10, limit: 100 })
      expect(parsed.offset).toBe(10)
      expect(parsed.limit).toBe(100)
    })
  })

  describe("skill", () => {
    test("accepts name", () => {
      expect(parse(Skill, { name: "foo" }).name).toBe("foo")
    })
    test("rejects missing name", () => {
      expect(accepts(Skill, {})).toBe(false)
    })
  })

  describe("send_message", () => {
    test("accepts target and message", () => {
      expect(parse(SendMessage, { to: "worker-a", message: "continue" })).toEqual({
        to: "worker-a",
        message: "continue",
      })
    })
    test("accepts optional summary", () => {
      const parsed = parse(SendMessage, { to: "worker-a", message: "continue", summary: "next step" })
      expect(parsed.summary).toBe("next step")
    })
    test("accepts structured messages and team broadcast target", () => {
      const parsed = parse(SendMessage, {
        to: "*",
        team: "red",
        message: {
          type: "shutdown_request",
          reason: "done",
        },
      })
      expect(parsed.to).toBe("*")
      expect(parsed.team).toBe("red")
      expect(parsed.message).toEqual({ type: "shutdown_request", reason: "done" })
    })
    test("accepts structured plan approval messages", () => {
      const request = parse(SendMessage, {
        to: "team-lead",
        message: {
          type: "plan_approval_request",
          plan: "Inspect first, then edit.",
          plan_file_path: "/tmp/plan.md",
        },
      })
      expect(request.message).toEqual({
        type: "plan_approval_request",
        plan: "Inspect first, then edit.",
        plan_file_path: "/tmp/plan.md",
      })

      const response = parse(SendMessage, {
        to: "planner",
        message: {
          type: "plan_approval_response",
          request_id: "par_1",
          approve: false,
          feedback: "Add verification steps.",
        },
      })
      expect(response.message).toEqual({
        type: "plan_approval_response",
        request_id: "par_1",
        approve: false,
        feedback: "Add verification steps.",
      })

      const permission = parse(SendMessage, {
        to: "tester",
        message: {
          type: "permission_response",
          request_id: "permission_1",
          approve: true,
          always: true,
        },
      })
      expect(permission.message).toEqual({
        type: "permission_response",
        request_id: "permission_1",
        approve: true,
        always: true,
      })

      const teamPermission = parse(SendMessage, {
        to: "*",
        team: "red",
        message: {
          type: "team_permission_update",
          rules: [{ permission: "bash", pattern: "git status", action: "allow" }],
          tool_name: "bash",
        },
      })
      expect(teamPermission.message).toEqual({
        type: "team_permission_update",
        rules: [{ permission: "bash", pattern: "git status", action: "allow" }],
        tool_name: "bash",
      })

      const modeSet = parse(SendMessage, {
        to: "tester",
        message: {
          type: "mode_set_request",
          mode: "accept_edits",
        },
      })
      expect(modeSet.message).toEqual({
        type: "mode_set_request",
        mode: "accept_edits",
      })
    })
    test("accepts same-project peer session targets", () => {
      const parsed = parse(SendMessage, {
        to: "session:ses_peer",
        message: "Can you review this branch?",
        summary: "review request",
      })
      expect(parsed.to).toBe("session:ses_peer")
      expect(parsed.summary).toBe("review request")
    })
    test("rejects missing target", () => {
      expect(accepts(SendMessage, { message: "continue" })).toBe(false)
    })
  })

  describe("remote_trigger", () => {
    test("accepts list and mutation shapes", () => {
      expect(parse(RemoteTrigger, { action: "list" })).toEqual({ action: "list" })
      expect(parse(RemoteTrigger, { action: "get", trigger_id: "trigger-1" })).toEqual({
        action: "get",
        trigger_id: "trigger-1",
      })
      expect(parse(RemoteTrigger, { action: "create", body: { prompt: "run checks" } })).toEqual({
        action: "create",
        body: { prompt: "run checks" },
      })
    })
    test("rejects invalid action and trigger id", () => {
      expect(accepts(RemoteTrigger, { action: "delete" })).toBe(false)
      expect(accepts(RemoteTrigger, { action: "get", trigger_id: "../bad" })).toBe(false)
    })
  })

  describe("subagent control tools", () => {
    test("list_peers accepts optional scope and filters", () => {
      expect(parse(ListPeers, { scope: "all", include_sessions: false, include_workers: true })).toEqual({
        scope: "all",
        include_sessions: false,
        include_workers: true,
      })
    })
    test("broadcast accepts team and message", () => {
      expect(parse(Broadcast, { team: "red", message: "continue", summary: "sync" })).toEqual({
        team: "red",
        message: "continue",
        summary: "sync",
      })
    })
    test("create/list/delete team schemas", () => {
      expect(parse(CreateTeam, { team_name: "red", description: "parallel work", agent_type: "lead" })).toEqual({
        team_name: "red",
        description: "parallel work",
        agent_type: "lead",
      })
      expect(parse(ListTeams, {})).toEqual({})
      expect(parse(DeleteTeam, { team_name: "red", cancel_workers: false })).toEqual({
        team_name: "red",
        cancel_workers: false,
      })
    })
    test("list_tasks accepts optional filters", () => {
      expect(parse(ListTasks, { scope: "current", status: "running", team: "red" })).toEqual({
        scope: "current",
        status: "running",
        team: "red",
      })
    })
    test("shared task board schemas", () => {
      expect(
        parse(CreateTask, {
          team: "red",
          subject: "Run tests",
          description: "Run the test suite",
          active_form: "Running tests",
          owner: "tester",
          metadata: { priority: "high" },
        }),
      ).toEqual({
        team: "red",
        subject: "Run tests",
        description: "Run the test suite",
        active_form: "Running tests",
        owner: "tester",
        metadata: { priority: "high" },
      })
      expect(
        parse(UpdateTask, {
          team: "red",
          task_id: "1",
          status: "in_progress",
          owner: "tester",
          add_blocked_by: ["0"],
        }),
      ).toEqual({
        team: "red",
        task_id: "1",
        status: "in_progress",
        owner: "tester",
        add_blocked_by: ["0"],
      })
      expect(parse(GetTask, { task_id: "1" })).toEqual({ task_id: "1" })
      expect(parse(ListTeamTasks, { team: "red", status: "pending", owner: "tester" })).toEqual({
        team: "red",
        status: "pending",
        owner: "tester",
      })
    })
    test("wait_task accepts one or many targets", () => {
      expect(parse(WaitTask, { task_id: "swa_1", task_ids: ["ses_1"], timeout_ms: 100 })).toEqual({
        task_id: "swa_1",
        task_ids: ["ses_1"],
        timeout_ms: 100,
      })
    })
    test("cancel_task requires task_id", () => {
      expect(accepts(CancelTask, {})).toBe(false)
      expect(parse(CancelTask, { task_id: "swa_1" })).toEqual({ task_id: "swa_1" })
    })
    test("control_task_pane accepts hide and show actions", () => {
      expect(parse(ControlTaskPane, { task_id: "swa_1", action: "hide" })).toEqual({
        task_id: "swa_1",
        action: "hide",
      })
      expect(parse(ControlTaskPane, { task_id: "swa_1", action: "show" })).toEqual({
        task_id: "swa_1",
        action: "show",
      })
      expect(accepts(ControlTaskPane, { task_id: "swa_1", action: "focus" })).toBe(false)
    })
    test("stop_task accepts optional self-stop reason", () => {
      expect(parse(StopTask, {})).toEqual({})
      expect(parse(StopTask, { task_id: "swa_1", reason: "done" })).toEqual({
        task_id: "swa_1",
        reason: "done",
      })
    })
    test("read_task_output accepts transcript options", () => {
      expect(parse(ReadTaskOutput, { task_id: "swa_1", include_transcript: true, limit: 5 })).toEqual({
        task_id: "swa_1",
        include_transcript: true,
        limit: 5,
      })
    })
  })

  describe("task", () => {
    test("accepts description + prompt + subagent_type", () => {
      const parsed = parse(Task, { description: "d", prompt: "p", subagent_type: "general" })
      expect(parsed.subagent_type).toBe("general")
    })
    test("accepts omitted subagent_type for implicit fork", () => {
      const parsed = parse(Task, { description: "d", prompt: "p" })
      expect(parsed.subagent_type).toBeUndefined()
    })
    test("accepts optional background routing fields", () => {
      const parsed = parse(Task, {
        description: "d",
        prompt: "p",
        subagent_type: "general",
        run_in_background: true,
        plan_mode_required: true,
        name: "worker-a",
        team: "red",
        team_name: "blue",
        mode: "plan",
        model: "test/test-model",
        context: "fork",
        isolation: "worktree",
      })
      expect(parsed.run_in_background).toBe(true)
      expect(parsed.plan_mode_required).toBe(true)
      expect(parsed.name).toBe("worker-a")
      expect(parsed.team).toBe("red")
      expect(parsed.team_name).toBe("blue")
      expect(parsed.mode).toBe("plan")
      expect(parsed.model).toBe("test/test-model")
      expect(parsed.context).toBe("fork")
      expect(parsed.isolation).toBe("worktree")
    })
    test("accepts remote isolation", () => {
      const parsed = parse(Task, {
        description: "d",
        prompt: "p",
        subagent_type: "general",
        isolation: "remote",
      })
      expect(parsed.isolation).toBe("remote")
    })
    test("rejects missing prompt", () => {
      expect(accepts(Task, { description: "d", subagent_type: "general" })).toBe(false)
    })
  })

  describe("todo", () => {
    test("accepts todos array", () => {
      const parsed = parse(Todo, {
        todos: [{ id: "t1", content: "do x", status: "pending", priority: "medium" }],
      })
      expect(parsed.todos.length).toBe(1)
    })
    test("rejects missing todos", () => {
      expect(accepts(Todo, {})).toBe(false)
    })
  })

  describe("webfetch", () => {
    test("accepts url-only", () => {
      expect(parse(WebFetch, { url: "https://example.com" }).url).toBe("https://example.com")
    })
  })

  describe("websearch", () => {
    test("accepts query", () => {
      expect(parse(WebSearch, { query: "opencode" }).query).toBe("opencode")
    })
  })

  describe("write", () => {
    test("accepts content + filePath", () => {
      expect(parse(Write, { content: "hi", filePath: "/a" })).toEqual({ content: "hi", filePath: "/a" })
    })
    test("rejects missing filePath", () => {
      expect(accepts(Write, { content: "hi" })).toBe(false)
    })
  })
})
