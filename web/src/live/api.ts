// Types for the console API (internal/console) and the client that talks to it.
// Everything here is current telemetry from a running system; none of it is
// published research data, and the two are never mixed.

export type Health = "unreachable" | "unhealthy" | "not_ready" | "ready";

export interface ConsoleInfo {
  mode: "demo" | "monitor";
  started_at: string;
  server_time: string;
  poll_ms: number;
  faults_enabled: boolean;
}

export interface ProcessState {
  pid?: number;
  state: "running" | "stopped" | "exited";
  exit_code?: number;
  link?: "connected" | "partitioned";
  fault?: { action: string; started_at: string; until: string };
  restarts: number;
}

export interface Component {
  name: string;
  role: "scheduler" | "worker";
  url: string;
  health: Health;
  healthz_status?: number;
  readyz_status?: number;
  error?: string;
  metrics?: Record<string, number>;
  policy?: string;
  last_ok_at?: string;
  process?: ProcessState;
}

export interface Snapshot {
  seq: number;
  sampled_at: string;
  redis: { ok: boolean; error?: string; latency_ms?: number; namespace: string };
  leadership: {
    held: boolean;
    owner?: string;
    epoch?: number;
    lease_remaining_ms?: number;
    fence?: number;
    epoch_counter?: number;
  };
  queue?: {
    pending: number;
    in_flight: number;
    unacked: number;
    dead_lettered: number;
    oldest_pending_wait_s?: number;
    not_admitted: number;
    not_admitted_capped?: boolean;
    counters: Record<string, number>;
    tenant_completed: Record<string, number>;
  };
  rates: { window_s?: number; admitted_per_s?: number; dispatched_per_s?: number; completed_per_s?: number };
  components: Component[];
  leader_sum: number | null;
  policy?: string;
}

export interface Sample {
  t: string;
  redis_ok: boolean;
  pending: number | null;
  not_admitted?: number | null;
  in_flight: number | null;
  dispatched_per_s: number | null;
  completed_per_s: number | null;
  owner?: string;
  epoch?: number;
  leader_sum: number | null;
}

export interface ConsoleEvent {
  seq: number;
  at: string;
  source: "observed" | "log" | "control";
  kind: string;
  subject?: string;
  message: string;
}

export interface DispatchWindow {
  read_at: string;
  from_ms: number;
  window_s: number;
  bucket_ms: number;
  count: number;
  truncated: boolean;
  buckets: { t_ms: number; epoch: number; n: number }[];
  epochs: number[];
  unstamped: number;
  max_gap_ms?: number;
  max_gap_end_ms?: number;
  last_dispatch_ms?: number;
  epoch_inversions: number;
  gaps: { start_ms: number; end_ms: number }[];
}

export interface Preset {
  id: string;
  label: string;
  description: string;
  type: string;
  count: number;
  offered_load: number;
}

export interface Controls {
  faults_enabled: boolean;
  reason?: string;
  actions?: Record<string, string[]>;
  processes?: Record<string, ProcessState>;
  min_duration_ms: number;
  max_duration_ms: number;
  presets?: Preset[];
  submission?: {
    preset: string;
    total: number;
    rate_per_s: number;
    started_at: string;
    finished_at?: string;
    submitted: number;
    error?: string;
    running: boolean;
  };
}

export interface DeadLetter {
  task_id: string;
  tenant: string;
  attempts: number;
  reason: string;
  failed_at: string;
}

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    /** True when the response was not the console at all (e.g. a static host's 404 page). */
    public readonly notConsole = false,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

const TOKEN_KEY = "tq-console-token";

/**
 * The console prints a URL with the session token in the fragment, which the
 * browser never sends to a server. It is moved to sessionStorage (this tab
 * only) and removed from the address bar so it does not end up in a bookmark
 * or a screen share.
 */
export function captureToken(): string | null {
  const m = window.location.hash.match(/[?&]token=([0-9a-f]{16,128})/);
  if (m) {
    try {
      sessionStorage.setItem(TOKEN_KEY, m[1]!);
    } catch {
      /* storage unavailable: the token lives only in memory for this page */
    }
    memoryToken = m[1]!;
    history.replaceState(null, "", `${window.location.pathname}${window.location.search}#/live`);
  }
  try {
    return memoryToken ?? sessionStorage.getItem(TOKEN_KEY);
  } catch {
    return memoryToken;
  }
}
let memoryToken: string | null = null;

export function forgetToken() {
  memoryToken = null;
  try {
    sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    /* nothing to remove */
  }
}

const BASE = "/api/v1";

export async function get<T>(path: string, signal?: AbortSignal, timeoutMs = 2500): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new DOMException("timed out", "TimeoutError")), timeoutMs);
  signal?.addEventListener("abort", () => ctrl.abort(signal.reason));
  try {
    const res = await fetch(BASE + path, { signal: ctrl.signal, headers: { Accept: "application/json" } });
    const type = res.headers.get("content-type") ?? "";
    if (!type.includes("application/json")) {
      throw new ApiError(`not a console response (HTTP ${res.status}, ${type || "no content type"})`, res.status, true);
    }
    const body = await res.json();
    if (!res.ok) throw new ApiError(body?.error ?? `HTTP ${res.status}`, res.status);
    return body as T;
  } catch (err) {
    if (err instanceof ApiError) throw err;
    if (ctrl.signal.aborted && !signal?.aborted) throw new ApiError("the console did not answer in time", 0);
    throw new ApiError(err instanceof Error ? err.message : String(err), 0);
  } finally {
    clearTimeout(timer);
  }
}

export async function post<T>(path: string, body: unknown, token: string): Promise<T> {
  const res = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Console-Token": token, Accept: "application/json" },
    body: JSON.stringify(body),
  });
  const type = res.headers.get("content-type") ?? "";
  const data = type.includes("application/json") ? await res.json() : null;
  if (!res.ok) throw new ApiError(data?.error ?? `HTTP ${res.status}`, res.status);
  return data as T;
}
