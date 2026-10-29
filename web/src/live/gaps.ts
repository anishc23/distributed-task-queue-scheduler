import type { Sample } from "./api";

export type GapKind = "stall" | "idle" | "unknown";

export interface ClassifiedGap {
  startMs: number;
  endMs: number;
  ms: number;
  kind: GapKind;
}

/**
 * Says what a gap in dispatching was, from the pending count the console
 * sampled while it lasted. The execution stream alone cannot tell an outage
 * from an idle queue: both are an interval with no entries.
 *
 * - stall: work was waiting and nothing dispatched it. Waiting means either
 *   in the pending set or submitted but not yet admitted: while no scheduler
 *   leads, nothing is admitted, so new work piles up in the ingress stream
 *   and the pending set alone would make an outage look idle.
 * - idle: every sample inside the gap shows nothing waiting anywhere.
 * - unknown: no sample fell inside it, or Redis could not be read at one.
 */
export function classifyGaps(gaps: { start_ms: number; end_ms: number }[], samples: Sample[]): ClassifiedGap[] {
  const points = samples.map((s) => ({
    t: Date.parse(s.t),
    ok: s.redis_ok,
    waiting: s.pending === null || s.not_admitted === null || s.not_admitted === undefined ? null : s.pending + s.not_admitted,
  }));
  return gaps.map((g) => {
    const inside = points.filter((p) => p.t > g.start_ms && p.t < g.end_ms);
    let kind: GapKind = "unknown";
    if (inside.length && inside.every((p) => p.ok && p.waiting !== null)) {
      kind = inside.some((p) => (p.waiting ?? 0) > 0) ? "stall" : "idle";
    }
    return { startMs: g.start_ms, endMs: g.end_ms, ms: g.end_ms - g.start_ms, kind };
  });
}
