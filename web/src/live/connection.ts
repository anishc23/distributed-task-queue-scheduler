// Pure functions that decide what the console's connection state is. Kept
// apart from React so the rules can be tested directly: a disconnected backend
// must never be shown as healthy, and old data must never pass for current.

export type Connection =
  | { kind: "connecting" }
  | { kind: "unavailable"; reason: string } // no console here at all (e.g. the public site)
  | { kind: "disconnected"; reason: string; retryInMs: number; since: number }
  | { kind: "stale"; ageMs: number } // the console answers, but its own poll has stopped
  | { kind: "live"; ageMs: number };

/** Backoff after consecutive failures: 1 s, 2 s, 4 s, 8 s, then 10 s. */
export function backoffMs(failures: number): number {
  if (failures <= 0) return 0;
  return Math.min(1000 * 2 ** (failures - 1), 10_000);
}

/**
 * How old a snapshot is, on the console's clock. The console reports its own
 * time with each response, so a skewed laptop clock does not make fresh data
 * look stale or the reverse.
 */
export function snapshotAgeMs(sampledAt: string, serverTime: string, receivedAt: number, now: number): number {
  const skew = Date.parse(serverTime) - receivedAt;
  return now + skew - Date.parse(sampledAt);
}

/** A snapshot older than three poll intervals is stale. */
export const isStale = (ageMs: number, pollMs: number) => ageMs > Math.max(3 * pollMs, 3000);

export function connectionState(input: {
  hasData: boolean;
  failures: number;
  lastError: string | null;
  notConsole: boolean;
  failingSince: number | null;
  nextRetryAt: number | null;
  ageMs: number | null;
  pollMs: number;
  now: number;
}): Connection {
  if (input.notConsole && !input.hasData) return { kind: "unavailable", reason: input.lastError ?? "no console at this address" };
  if (input.failures > 0) {
    return {
      kind: "disconnected",
      reason: input.lastError ?? "request failed",
      retryInMs: Math.max(0, (input.nextRetryAt ?? input.now) - input.now),
      since: input.failingSince ?? input.now,
    };
  }
  if (!input.hasData || input.ageMs === null) return { kind: "connecting" };
  if (isStale(input.ageMs, input.pollMs)) return { kind: "stale", ageMs: input.ageMs };
  return { kind: "live", ageMs: input.ageMs };
}

export type LeaderVerdict = { tone: "ok" | "warn" | "bad" | "unknown"; label: string; detail: string };

/** What the leadership sum means, in words. */
export function leaderVerdict(sum: number | null, held: boolean | undefined): LeaderVerdict {
  if (sum === null) {
    return {
      tone: "unknown",
      label: "unknown",
      detail: "At least one scheduler could not be read, so the console cannot count who believes it leads.",
    };
  }
  if (sum === 1) return { tone: "ok", label: "exactly one", detail: "One scheduler reports itself leader." };
  if (sum === 0) {
    return {
      tone: held ? "warn" : "bad",
      label: "none",
      detail: held
        ? "The lease is held but no reachable scheduler reports leading; a hand-over is in progress."
        : "No scheduler leads and the lease is free: nothing is dispatching until a standby acquires it.",
    };
  }
  return {
    tone: "warn",
    label: `${sum} at once`,
    detail:
      "More than one scheduler believes it leads. Only the newest epoch can write: the fence refuses the others at Redis. This is the situation the fence exists for.",
  };
}

const HEALTH_TEXT: Record<string, { tone: "ok" | "warn" | "bad" | "unknown"; label: string }> = {
  ready: { tone: "ok", label: "ready" },
  not_ready: { tone: "warn", label: "alive, not ready" },
  unhealthy: { tone: "bad", label: "unhealthy" },
  unreachable: { tone: "unknown", label: "unreachable" },
};

export function healthText(h: string) {
  return HEALTH_TEXT[h] ?? { tone: "unknown" as const, label: h };
}
