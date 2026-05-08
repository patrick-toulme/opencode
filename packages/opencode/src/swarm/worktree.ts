import { Effect } from "effect"

export type WorkerWorktreeInfo = {
  root: string
  path: string
  branch: string
}

export const cleanupWorktreeIfClean = Effect.fn("SwarmWorktree.cleanupWorktreeIfClean")(function* (
  worktree: WorkerWorktreeInfo,
) {
  const status = yield* git(worktree.path, ["status", "--porcelain"]).pipe(Effect.catchCause(() => Effect.succeed("")))
  if (status.trim()) return
  yield* git(worktree.root, ["worktree", "remove", "--force", worktree.path]).pipe(Effect.catchCause(() => Effect.void))
  yield* git(worktree.root, ["branch", "-D", worktree.branch]).pipe(Effect.catchCause(() => Effect.void))
})

const git = (cwd: string, args: string[]) =>
  Effect.promise(async () => {
    const proc = Bun.spawn(["git", ...args], {
      cwd,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: {
        ...process.env,
        GIT_TERMINAL_PROMPT: "0",
        GIT_ASKPASS: "",
      },
    })
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    if (code !== 0) {
      throw new Error(`git ${args.join(" ")} failed in ${cwd}: ${stderr.trim() || stdout.trim() || `exit ${code}`}`)
    }
    return stdout.trim()
  })
