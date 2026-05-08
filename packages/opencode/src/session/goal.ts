import GOAL_ACTIVE from "./prompt/goal-active.txt"
import GOAL_BUDGET_LIMITED from "./prompt/goal-budget-limited.txt"
import type * as Session from "./session"

function formatTokens(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}

function formatDuration(ms: number) {
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`
  const hours = Math.floor(minutes / 60)
  return `${hours}h ${minutes % 60}m`
}

function budgetMeter(goal: Session.Goal) {
  if (goal.tokenBudget === undefined) return ""
  const remaining = Math.max(0, goal.tokenBudget - goal.tokensUsed)
  const pct = goal.tokenBudget > 0 ? Math.min(100, Math.round((goal.tokensUsed / goal.tokenBudget) * 100)) : 0
  return ` / ${formatTokens(goal.tokenBudget)} budget (${pct}% used, ${formatTokens(remaining)} remaining)`
}

function usageLine(goal: Session.Goal) {
  if (goal.tokenBudget === undefined) return "Token budget: none set"
  return `Tokens used: ${formatTokens(goal.tokensUsed)}${budgetMeter(goal)}`
}

export function renderSystem(goal: Session.Goal, options: { isContinuationTurn: boolean }) {
  const template = goal.status === "budget_limited" ? GOAL_BUDGET_LIMITED : GOAL_ACTIVE
  const continuationNote = options.isContinuationTurn
    ? "- This is an autonomous continuation turn — no new user input was sent. Make tangible progress or call `goal_update` to stop."
    : ""
  return template
    .replace("${objective}", goal.objective)
    .replace("${status}", goal.status)
    .replace("${tokensUsed}", formatTokens(goal.tokensUsed))
    .replaceAll("${budgetMeter}", budgetMeter(goal))
    .replace("${usageLine}", usageLine(goal))
    .replace("${timeUsed}", formatDuration(goal.timeUsedMs))
    .replace("${continuationNote}", continuationNote)
}

export function shouldAutoContinue(goal: Session.Goal | undefined): goal is Session.Goal {
  if (!goal) return false
  return goal.status === "active"
}

export const MAX_AUTONOMOUS_ITERATIONS = 50
