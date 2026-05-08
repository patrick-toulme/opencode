export function shellQuote(value: string) {
  if (value.length === 0) return "''"
  return `'${value.replaceAll("'", "'\\''")}'`
}

export function envAssignments(env: Record<string, string | undefined>) {
  return Object.entries(env)
    .filter((entry): entry is [string, string] => entry[1] !== undefined && entry[1] !== "")
    .map(([key, value]) => `${key}=${shellQuote(value)}`)
}
