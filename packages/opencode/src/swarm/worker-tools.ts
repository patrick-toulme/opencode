export const mutationToolsDisabled: Record<string, boolean> = {
  bash: false,
  edit: false,
  write: false,
  apply_patch: false,
}

export const workerLeadToolsDisabled: Record<string, boolean> = {
  task: false,
  create_team: false,
  delete_team: false,
  wait_task: false,
  cancel_task: false,
  control_task_pane: false,
  read_task_output: false,
  remote_trigger: false,
  goal_create: false,
  goal_update: false,
}

export function workerToolsDisabled(input: { planModeRequired: boolean; planApproved: boolean }) {
  return {
    ...workerLeadToolsDisabled,
    ...(input.planModeRequired && !input.planApproved ? mutationToolsDisabled : {}),
  }
}
