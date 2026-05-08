export type CronFields = {
  minute: number[]
  hour: number[]
  dayOfMonth: number[]
  month: number[]
  dayOfWeek: number[]
}

type FieldRange = { min: number; max: number }

const FIELD_RANGES: FieldRange[] = [
  { min: 0, max: 59 },
  { min: 0, max: 23 },
  { min: 1, max: 31 },
  { min: 1, max: 12 },
  { min: 0, max: 6 },
]

const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

export type CronJitterConfig = {
  recurringFrac: number
  recurringCapMs: number
  oneShotMaxMs: number
  oneShotFloorMs: number
  oneShotMinuteMod: number
  recurringMaxAgeMs: number
}

export const DEFAULT_CRON_JITTER_CONFIG: CronJitterConfig = {
  recurringFrac: 0.1,
  recurringCapMs: 15 * 60 * 1000,
  oneShotMaxMs: 90 * 1000,
  oneShotFloorMs: 0,
  oneShotMinuteMod: 30,
  recurringMaxAgeMs: 7 * 24 * 60 * 60 * 1000,
}

function expandField(field: string, range: FieldRange): number[] | null {
  const { min, max } = range
  const out = new Set<number>()

  for (const part of field.split(",")) {
    const stepMatch = part.match(/^\*(?:\/(\d+))?$/)
    if (stepMatch) {
      const step = stepMatch[1] ? Number.parseInt(stepMatch[1], 10) : 1
      if (step < 1) return null
      for (let i = min; i <= max; i += step) out.add(i)
      continue
    }

    const rangeMatch = part.match(/^(\d+)-(\d+)(?:\/(\d+))?$/)
    if (rangeMatch) {
      const lo = Number.parseInt(rangeMatch[1]!, 10)
      const hi = Number.parseInt(rangeMatch[2]!, 10)
      const step = rangeMatch[3] ? Number.parseInt(rangeMatch[3], 10) : 1
      const isDow = min === 0 && max === 6
      const effMax = isDow ? 7 : max
      if (lo > hi || step < 1 || lo < min || hi > effMax) return null
      for (let i = lo; i <= hi; i += step) out.add(isDow && i === 7 ? 0 : i)
      continue
    }

    if (/^\d+$/.test(part)) {
      let value = Number.parseInt(part, 10)
      if (min === 0 && max === 6 && value === 7) value = 0
      if (value < min || value > max) return null
      out.add(value)
      continue
    }

    return null
  }

  if (out.size === 0) return null
  return Array.from(out).sort((a, b) => a - b)
}

export function parseCronExpression(expr: string): CronFields | null {
  const parts = expr.trim().split(/\s+/)
  if (parts.length !== 5) return null

  const expanded: number[][] = []
  for (let i = 0; i < 5; i++) {
    const result = expandField(parts[i]!, FIELD_RANGES[i]!)
    if (!result) return null
    expanded.push(result)
  }

  return {
    minute: expanded[0]!,
    hour: expanded[1]!,
    dayOfMonth: expanded[2]!,
    month: expanded[3]!,
    dayOfWeek: expanded[4]!,
  }
}

export function computeNextCronRun(fields: CronFields, from: Date): Date | null {
  const minuteSet = new Set(fields.minute)
  const hourSet = new Set(fields.hour)
  const domSet = new Set(fields.dayOfMonth)
  const monthSet = new Set(fields.month)
  const dowSet = new Set(fields.dayOfWeek)
  const domWild = fields.dayOfMonth.length === 31
  const dowWild = fields.dayOfWeek.length === 7

  const t = new Date(from.getTime())
  t.setSeconds(0, 0)
  t.setMinutes(t.getMinutes() + 1)

  for (let i = 0; i < 366 * 24 * 60; i++) {
    const month = t.getMonth() + 1
    if (!monthSet.has(month)) {
      t.setMonth(t.getMonth() + 1, 1)
      t.setHours(0, 0, 0, 0)
      continue
    }

    const dom = t.getDate()
    const dow = t.getDay()
    const dayMatches =
      domWild && dowWild
        ? true
        : domWild
          ? dowSet.has(dow)
          : dowWild
            ? domSet.has(dom)
            : domSet.has(dom) || dowSet.has(dow)

    if (!dayMatches) {
      t.setDate(t.getDate() + 1)
      t.setHours(0, 0, 0, 0)
      continue
    }

    if (!hourSet.has(t.getHours())) {
      t.setHours(t.getHours() + 1, 0, 0, 0)
      continue
    }

    if (!minuteSet.has(t.getMinutes())) {
      t.setMinutes(t.getMinutes() + 1)
      continue
    }

    return t
  }

  return null
}

export function nextCronRunMs(cron: string, fromMs: number): number | null {
  const fields = parseCronExpression(cron)
  if (!fields) return null
  return computeNextCronRun(fields, new Date(fromMs))?.getTime() ?? null
}

export function jitteredNextCronRunMs(
  cron: string,
  fromMs: number,
  taskID: string,
  cfg: CronJitterConfig = DEFAULT_CRON_JITTER_CONFIG,
): number | null {
  const t1 = nextCronRunMs(cron, fromMs)
  if (t1 === null) return null
  const t2 = nextCronRunMs(cron, t1)
  if (t2 === null) return t1
  const jitter = Math.min(hashFrac(taskID) * cfg.recurringFrac * (t2 - t1), cfg.recurringCapMs)
  return t1 + jitter
}

export function oneShotJitteredNextCronRunMs(
  cron: string,
  fromMs: number,
  taskID: string,
  cfg: CronJitterConfig = DEFAULT_CRON_JITTER_CONFIG,
): number | null {
  const t1 = nextCronRunMs(cron, fromMs)
  if (t1 === null) return null
  if (new Date(t1).getMinutes() % cfg.oneShotMinuteMod !== 0) return t1
  const lead = cfg.oneShotFloorMs + hashFrac(taskID) * (cfg.oneShotMaxMs - cfg.oneShotFloorMs)
  return Math.max(t1 - lead, fromMs)
}

export function isRecurringTaskAged(createdAt: number, nowMs: number, maxAgeMs = DEFAULT_CRON_JITTER_CONFIG.recurringMaxAgeMs) {
  if (maxAgeMs === 0) return false
  return nowMs - createdAt >= maxAgeMs
}

export function cronToHuman(cron: string): string {
  const parts = cron.trim().split(/\s+/)
  if (parts.length !== 5) return cron
  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts as [string, string, string, string, string]

  const everyMinMatch = minute.match(/^\*\/(\d+)$/)
  if (everyMinMatch && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    const n = Number.parseInt(everyMinMatch[1]!, 10)
    return n === 1 ? "Every minute" : `Every ${n} minutes`
  }

  if (/^\d+$/.test(minute) && hour === "*" && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    const m = Number.parseInt(minute, 10)
    return m === 0 ? "Every hour" : `Every hour at :${m.toString().padStart(2, "0")}`
  }

  const everyHourMatch = hour.match(/^\*\/(\d+)$/)
  if (/^\d+$/.test(minute) && everyHourMatch && dayOfMonth === "*" && month === "*" && dayOfWeek === "*") {
    const n = Number.parseInt(everyHourMatch[1]!, 10)
    const m = Number.parseInt(minute, 10)
    const suffix = m === 0 ? "" : ` at :${m.toString().padStart(2, "0")}`
    return n === 1 ? `Every hour${suffix}` : `Every ${n} hours${suffix}`
  }

  if (!/^\d+$/.test(minute) || !/^\d+$/.test(hour)) return cron
  const m = Number.parseInt(minute, 10)
  const h = Number.parseInt(hour, 10)
  const time = formatLocalTime(m, h)

  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "*") return `Every day at ${time}`
  if (dayOfMonth === "*" && month === "*" && /^\d$/.test(dayOfWeek)) {
    return `Every ${DAY_NAMES[Number.parseInt(dayOfWeek, 10) % 7]} at ${time}`
  }
  if (dayOfMonth === "*" && month === "*" && dayOfWeek === "1-5") return `Weekdays at ${time}`
  return cron
}

function formatLocalTime(minute: number, hour: number): string {
  return new Date(2000, 0, 1, hour, minute).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
  })
}

function hashFrac(value: string): number {
  let hash = 2166136261
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0) / 0x1_0000_0000
}

export * as ScheduleCron from "./cron"
