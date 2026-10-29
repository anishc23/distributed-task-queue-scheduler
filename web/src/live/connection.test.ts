import { describe, expect, it } from "vitest";
import { backoffMs, connectionState, isStale, leaderVerdict, snapshotAgeMs } from "./connection";

const base = {
  hasData: true,
  failures: 0,
  lastError: null,
  notConsole: false,
  failingSince: null,
  nextRetryAt: null,
  ageMs: 500,
  pollMs: 1000,
  now: 10_000,
};

describe("connection state", () => {
  it("is live only with fresh data and no failures", () => {
    expect(connectionState(base).kind).toBe("live");
  });

  it("a failed request is never shown as healthy, even with data in hand", () => {
    const c = connectionState({ ...base, failures: 1, lastError: "timed out", failingSince: 9000, nextRetryAt: 11_000 });
    expect(c).toEqual({ kind: "disconnected", reason: "timed out", retryInMs: 1000, since: 9000 });
  });

  it("old data from an answering console is stale, not live", () => {
    expect(connectionState({ ...base, ageMs: 3500 }).kind).toBe("stale");
  });

  it("no console at all is distinguished from a console that stopped answering", () => {
    expect(connectionState({ ...base, hasData: false, ageMs: null, failures: 1, notConsole: true, lastError: "404" }).kind).toBe("unavailable");
    expect(connectionState({ ...base, failures: 1, notConsole: true }).kind).toBe("disconnected");
  });

  it("is connecting before the first answer", () => {
    expect(connectionState({ ...base, hasData: false, ageMs: null }).kind).toBe("connecting");
  });
});

describe("timing", () => {
  it("backs off 1, 2, 4, 8 then 10 s", () => {
    expect([1, 2, 3, 4, 5, 9].map(backoffMs)).toEqual([1000, 2000, 4000, 8000, 10_000, 10_000]);
    expect(backoffMs(0)).toBe(0);
  });

  it("measures age on the console's clock, so a skewed local clock does not lie", () => {
    // The local clock is 60 s fast; the snapshot is really 400 ms old.
    const server = "2026-10-13T10:00:01.000Z";
    const sampled = "2026-10-13T10:00:00.600Z";
    const receivedAt = Date.parse(server) + 60_000;
    expect(snapshotAgeMs(sampled, server, receivedAt, receivedAt)).toBe(400);
  });

  it("staleness is relative to the poll interval, with a floor", () => {
    expect(isStale(2900, 1000)).toBe(false);
    expect(isStale(3100, 1000)).toBe(true);
    expect(isStale(5000, 2000)).toBe(false);
  });
});

describe("leader verdict", () => {
  it("names each case and never calls an unknown count healthy", () => {
    expect(leaderVerdict(1, true).tone).toBe("ok");
    expect(leaderVerdict(null, true).tone).toBe("unknown");
    expect(leaderVerdict(0, false).tone).toBe("bad");
    expect(leaderVerdict(0, true).tone).toBe("warn");
    expect(leaderVerdict(2, true)).toMatchObject({ tone: "warn", label: "2 at once" });
  });
});
