import { describe, expect, test } from "bun:test"
import { computeNextCronRun, cronToHuman, parseCronExpression } from "@/schedule/cron"

describe("schedule.cron", () => {
  test("parses standard 5-field cron expressions", () => {
    expect(parseCronExpression("*/5 * * * *")?.minute.slice(0, 3)).toEqual([0, 5, 10])
    expect(parseCronExpression("30 14 28 2 *")?.hour).toEqual([14])
    expect(parseCronExpression("0 9 * * 1-5")?.dayOfWeek).toEqual([1, 2, 3, 4, 5])
    expect(parseCronExpression("0 9 * * 7")?.dayOfWeek).toEqual([0])
    expect(parseCronExpression("bad")).toBeNull()
    expect(parseCronExpression("61 * * * *")).toBeNull()
  })

  test("computes next run using local-time cron semantics", () => {
    const fields = parseCronExpression("30 14 28 2 *")
    expect(fields).not.toBeNull()
    const next = computeNextCronRun(fields!, new Date(2026, 1, 28, 14, 29, 30))
    expect(next?.getFullYear()).toBe(2026)
    expect(next?.getMonth()).toBe(1)
    expect(next?.getDate()).toBe(28)
    expect(next?.getHours()).toBe(14)
    expect(next?.getMinutes()).toBe(30)
  })

  test("renders common human descriptions", () => {
    expect(cronToHuman("*/5 * * * *")).toBe("Every 5 minutes")
    expect(cronToHuman("0 * * * *")).toBe("Every hour")
    expect(cronToHuman("0 9 * * 1-5")).toBe("Weekdays at 9:00 AM")
  })
})
