import { describe, expect, it } from "vitest";
import { classifyGaps } from "./gaps";
import type { Sample } from "./api";

const sample = (t: number, pending: number | null, ok = true, notAdmitted: number | null = 0): Sample => ({
  t: new Date(t).toISOString(), redis_ok: ok, pending, not_admitted: notAdmitted, in_flight: 0, dispatched_per_s: null, completed_per_s: null, leader_sum: 1,
});

describe("classifyGaps", () => {
  const gap = { start_ms: 10_000, end_ms: 14_500 };

  it("calls a gap with work waiting a stall", () => {
    expect(classifyGaps([gap], [sample(11_000, 0), sample(12_000, 40), sample(13_000, 90)])[0]).toMatchObject({ kind: "stall", ms: 4500 });
  });

  it("calls a gap with nothing pending idle, not an outage", () => {
    expect(classifyGaps([gap], [sample(11_000, 0), sample(12_000, 0)])[0]!.kind).toBe("idle");
  });

  it("counts work piling up unadmitted while nothing leads as waiting", () => {
    // The pending set stays empty during an outage; the ingress stream does not.
    expect(classifyGaps([gap], [sample(11_000, 0, true, 0), sample(12_000, 0, true, 180)])[0]!.kind).toBe("stall");
  });

  it("does not guess when there is no evidence", () => {
    expect(classifyGaps([gap], [sample(9_000, 50), sample(15_000, 50)])[0]!.kind).toBe("unknown");
    expect(classifyGaps([gap], [sample(11_000, 0), sample(12_000, null, false)])[0]!.kind).toBe("unknown");
    expect(classifyGaps([gap], [sample(11_000, 0, true, null)])[0]!.kind).toBe("unknown");
  });
});
