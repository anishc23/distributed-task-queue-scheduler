import { useEffect, useRef, useState } from "react";
import {
  ApiError,
  type ConsoleEvent,
  type ConsoleInfo,
  type Controls,
  type DeadLetter,
  type DispatchWindow,
  type Sample,
  type Snapshot,
  type TenantWindow,
  get,
} from "./api";
import { backoffMs, connectionState, snapshotAgeMs, type Connection } from "./connection";

export interface LiveData {
  info: ConsoleInfo | null;
  snapshot: Snapshot | null;
  receivedAt: number | null;
  history: Sample[];
  events: ConsoleEvent[];
  dispatches: DispatchWindow | null;
  tenants: TenantWindow | null;
  controls: Controls | null;
  dlq: DeadLetter[] | null;
  dlqError: string | null;
  connection: Connection;
  now: number;
  refresh: () => void;
}

const MAX_EVENTS = 300;

/**
 * Polls the console. Status, events and controls every poll interval; history
 * and the dispatch window every second poll; dead letters every tenth. One
 * request chain at a time, so a slow console is never stacked with requests.
 * After a failure it backs off (1, 2, 4, 8, 10 s) and keeps the last data,
 * which the page then shows as old rather than current. Polling does not slow
 * in a background tab: the staleness check would then report the client's own
 * throttling as a stalled console.
 */
export function useConsole(): LiveData {
  const [info, setInfo] = useState<ConsoleInfo | null>(null);
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [receivedAt, setReceivedAt] = useState<number | null>(null);
  const [history, setHistory] = useState<Sample[]>([]);
  const [events, setEvents] = useState<ConsoleEvent[]>([]);
  const [dispatches, setDispatches] = useState<DispatchWindow | null>(null);
  const [tenants, setTenants] = useState<TenantWindow | null>(null);
  const [controls, setControls] = useState<Controls | null>(null);
  const [dlq, setDlq] = useState<DeadLetter[] | null>(null);
  const [dlqError, setDlqError] = useState<string | null>(null);
  const [failures, setFailures] = useState(0);
  const [lastError, setLastError] = useState<string | null>(null);
  const [notConsole, setNotConsole] = useState(false);
  const [failingSince, setFailingSince] = useState<number | null>(null);
  const [nextRetryAt, setNextRetryAt] = useState<number | null>(null);
  const [now, setNow] = useState(Date.now());
  const kick = useRef<() => void>(() => {});

  useEffect(() => {
    const ctrl = new AbortController();
    let timer: number | undefined;
    let tick = 0;
    let lastSeq = 0;
    let fails = 0;
    let poll = 1000;

    const schedule = (ms: number) => {
      window.clearTimeout(timer);
      timer = window.setTimeout(run, ms);
    };

    const run = async () => {
      if (ctrl.signal.aborted) return;
      try {
        const [st, ev, ctl] = await Promise.all([
          get<{ console: ConsoleInfo; snapshot: Snapshot | null }>("/status", ctrl.signal),
          get<{ events: ConsoleEvent[] }>(`/events?since=${lastSeq}`, ctrl.signal),
          get<Controls>("/controls", ctrl.signal),
        ]);
        const received = Date.now();
        poll = st.console.poll_ms || 1000;
        setInfo(st.console);
        setSnapshot(st.snapshot);
        setReceivedAt(received);
        setControls(ctl);
        if (ev.events.length) {
          lastSeq = ev.events[ev.events.length - 1]!.seq;
          setEvents((prev) => [...prev, ...ev.events].slice(-MAX_EVENTS));
        }
        if (tick % 2 === 0) {
          const [h, d, t] = await Promise.all([
            get<{ samples: Sample[] }>("/history", ctrl.signal),
            get<DispatchWindow>("/dispatches?window_s=90", ctrl.signal, 4000),
            get<TenantWindow>("/tenants?window_s=120", ctrl.signal, 4000),
          ]);
          setHistory(h.samples);
          setDispatches(d);
          setTenants(t);
        }
        if (tick % 10 === 0) {
          get<{ entries: DeadLetter[] }>("/dlq?limit=50", ctrl.signal)
            .then((r) => {
              setDlq(r.entries);
              setDlqError(null);
            })
            .catch((e: unknown) => setDlqError(e instanceof Error ? e.message : String(e)));
        }
        fails = 0;
        setFailures(0);
        setLastError(null);
        setNotConsole(false);
        setFailingSince(null);
        setNextRetryAt(null);
        tick++;
        schedule(poll);
      } catch (err) {
        if (ctrl.signal.aborted) return;
        fails++;
        const wait = backoffMs(fails);
        setFailures(fails);
        setLastError(err instanceof Error ? err.message : String(err));
        setNotConsole(err instanceof ApiError && err.notConsole);
        setFailingSince((s) => s ?? Date.now());
        setNextRetryAt(Date.now() + wait);
        schedule(wait);
      }
    };

    kick.current = () => schedule(0);
    const onVisible = () => !document.hidden && schedule(0);
    document.addEventListener("visibilitychange", onVisible);
    run();
    const clock = window.setInterval(() => setNow(Date.now()), 500);
    return () => {
      ctrl.abort();
      window.clearTimeout(timer);
      window.clearInterval(clock);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, []);

  const ageMs = snapshot && info && receivedAt !== null ? snapshotAgeMs(snapshot.sampled_at, info.server_time, receivedAt, now) : null;
  const connection = connectionState({
    hasData: snapshot !== null,
    failures,
    lastError,
    notConsole,
    failingSince,
    nextRetryAt,
    ageMs,
    pollMs: info?.poll_ms ?? 1000,
    now,
  });

  return {
    info,
    snapshot,
    receivedAt,
    history,
    events,
    dispatches,
    tenants,
    controls,
    dlq,
    dlqError,
    connection,
    now,
    refresh: () => kick.current(),
  };
}
