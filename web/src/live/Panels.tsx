import { useMemo, useState } from "react";
import type { ConsoleEvent, DeadLetter, Snapshot } from "./api";
import { healthText, leaderVerdict } from "./connection";
import type { LiveData } from "./useConsole";
import { fmt, fmtInt } from "../lib/format";
import { Table } from "../components/Controls";

const clock = (iso: string | number) =>
  new Date(iso).toLocaleTimeString("en-GB", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" }) +
  "." +
  String(new Date(iso).getMilliseconds()).padStart(3, "0");

const secs = (ms: number) => (ms < 10_000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms / 1000)} s`);

export function ConnectionBar({ live }: { live: LiveData }) {
  const c = live.connection;
  let tone = "unknown";
  let label = "";
  let detail = "";
  switch (c.kind) {
    case "connecting":
      label = "connecting";
      break;
    case "unavailable":
      label = "no console";
      detail = c.reason;
      break;
    case "disconnected":
      tone = "bad";
      label = "console not answering";
      detail = `${c.reason} · retrying in ${secs(c.retryInMs)}`;
      break;
    case "stale":
      tone = "warn";
      label = "stale";
      detail = `newest data is ${secs(c.ageMs)} old`;
      break;
    case "live":
      tone = "ok";
      label = "live";
      detail = `updated ${secs(Math.max(0, c.ageMs))} ago`;
      break;
  }
  const s = live.snapshot;
  return (
    <div className="conn" role="status" aria-live="polite">
      <span className={`tag ${tone}`}>{label}</span>
      {detail && <span className="small muted">{detail}</span>}
      {s && (
        <span className="small muted mono">
          sampled {clock(s.sampled_at)} · {live.info?.mode ?? "?"} mode · namespace {s.redis.namespace}
          {s.policy ? ` · policy ${s.policy}` : ""}
        </span>
      )}
    </div>
  );
}

function Kpi({ label, value, unit, note }: { label: string; value: string; unit?: string; note?: string }) {
  return (
    <div className="kpi">
      <dt>{label}</dt>
      <dd>
        <span className="num">{value}</span>
        {unit && value !== "—" && <span className="unit"> {unit}</span>}
      </dd>
      {note && <dd className="kpi-note">{note}</dd>}
    </div>
  );
}

export function Kpis({ snap }: { snap: Snapshot }) {
  const q = snap.queue;
  const r = snap.rates;
  return (
    <section aria-label="Queue">
      {!snap.redis.ok && (
        <div className="note negative" role="alert">
          <div className="note-title">Redis unreachable</div>
          <p className="small">
            The console could not read the queue: {snap.redis.error}. Queue figures are unknown, not zero. Processes may
            still be running; see their own status below.
          </p>
        </div>
      )}
      <dl className="kpis">
        <Kpi label="pending" value={q ? fmtInt(q.pending) : "—"} note="in the scheduler's pending set" />
        <Kpi label="in flight" value={q ? fmtInt(q.in_flight) : "—"} note="dispatched, not finished" />
        <Kpi label="dispatched" value={fmt(r.dispatched_per_s ?? null, 0)} unit="/s" note="from Redis counters" />
        <Kpi label="completed" value={fmt(r.completed_per_s ?? null, 0)} unit="/s" note="from Redis counters" />
        <Kpi
          label="oldest waiting"
          value={q?.oldest_pending_wait_s !== undefined ? fmt(q.oldest_pending_wait_s, 2) : q ? "none" : "—"}
          unit="s"
        />
        <Kpi label="dead-lettered" value={q ? fmtInt(q.dead_lettered) : "—"} note={q ? `${fmtInt(q.counters.retried ?? 0)} retries` : undefined} />
      </dl>
    </section>
  );
}

export function Leadership({ snap }: { snap: Snapshot }) {
  const l = snap.leadership;
  const v = leaderVerdict(snap.leader_sum, l.held);
  const between = l.fence !== undefined && l.epoch_counter !== undefined && l.fence < l.epoch_counter;
  return (
    <section className="panel" aria-labelledby="lead-h">
      <h2 id="lead-h" className="panel-h">
        Leadership
      </h2>
      {!snap.redis.ok ? (
        <p className="small muted">Unknown: Redis, which holds the lease, could not be read.</p>
      ) : (
        <dl className="pairs">
          <dt>lease holder</dt>
          <dd>{l.held ? <strong>{l.owner}</strong> : <span className="tag bad">none</span>}</dd>
          <dt>epoch</dt>
          <dd className="num">{l.held ? l.epoch : "—"}</dd>
          <dt>lease expires in</dt>
          <dd className="num">{l.held && l.lease_remaining_ms !== undefined ? secs(l.lease_remaining_ms) : "—"}</dd>
          <dt>fence</dt>
          <dd className="num">
            {l.fence ?? "—"}
            {between && <span className="small muted"> · epoch {l.epoch_counter} granted, not yet written</span>}
          </dd>
          <dt>schedulers claiming to lead</dt>
          <dd>
            <span className={`tag ${v.tone}`}>{v.label}</span> <span className="small muted">{v.detail}</span>
          </dd>
        </dl>
      )}
      <p className="tiny muted">
        Holder, epoch and fence are read from Redis, the authoritative record. The claim count comes from each
        scheduler's <code>tq_scheduler_is_leader</code>: what each process believes.
      </p>
    </section>
  );
}

export function Processes({ snap, now }: { snap: Snapshot; now: number }) {
  return (
    <section className="panel" aria-labelledby="proc-h">
      <h2 id="proc-h" className="panel-h">
        Processes
      </h2>
      <div className="table-scroll">
        <Table
          caption="Process health"
          head={["process", "health", "process state", "leads", "last answered"]}
          rows={snap.components.map((c) => {
            const h = healthText(c.health);
            const p = c.process;
            let leads = "";
            if (c.role === "scheduler") {
              if (c.metrics) leads = c.metrics.tq_scheduler_is_leader === 1 ? `yes · epoch ${c.metrics.tq_scheduler_leader_epoch}` : "no";
              else if (c.process?.state === "exited") leads = "no · not running";
              else if (c.process?.state === "stopped") leads = "unknown · frozen, may still believe it leads";
              else leads = "unknown · not answering";
            }
            let proc = "not supervised";
            if (p) {
              proc = p.state === "exited" ? `exited${p.exit_code !== undefined && p.exit_code >= 0 ? ` (${p.exit_code})` : ""}` : p.state;
              if (p.state === "stopped") proc = "stopped (SIGSTOP)";
              if (p.link === "partitioned") proc += " · cut off from Redis";
              if (p.fault) proc += ` · ends in ${secs(Math.max(0, Date.parse(p.fault.until) - now))}`;
              if (p.restarts) proc += ` · restarted ${p.restarts}×`;
            }
            return [
              <span>
                <strong>{c.name}</strong> <span className="tiny muted">{c.role}</span>
              </span>,
              <span title={c.error}>
                <span className={`tag ${h.tone}`}>{h.label}</span>
                {c.error && <span className="tiny muted"> {c.error}</span>}
              </span>,
              <span className="small">{proc}</span>,
              <span className="small">{leads}</span>,
              <span className="tiny mono">{c.last_ok_at ? clock(c.last_ok_at) : "never"}</span>,
            ];
          })}
        />
      </div>
      <p className="tiny muted">
        Health follows the endpoints: unreachable means no HTTP answer, which a frozen process and a dead one share.
        In demo mode, process state comes from the supervisor that started it. A stop is confirmed from the OS
        process table.
      </p>
    </section>
  );
}

const SOURCE_NOTE: Record<string, string> = {
  observed: "seen between two polls; happened up to one poll interval earlier",
  log: "written by the process itself, with its own timestamp",
  control: "done by this console, on request",
};

export function Events({ events }: { events: ConsoleEvent[] }) {
  const [filter, setFilter] = useState<"all" | "log" | "observed" | "control">("all");
  const shown = useMemo(
    () =>
      [...events]
        .filter((e) => filter === "all" || e.source === filter)
        .sort((a, b) => Date.parse(b.at) - Date.parse(a.at) || b.seq - a.seq)
        .slice(0, 80),
    [events, filter],
  );
  return (
    <section className="panel" aria-labelledby="ev-h">
      <div className="panel-head">
        <h2 id="ev-h" className="panel-h">
          Events
        </h2>
        <div className="seg" role="group" aria-label="event source">
          {(["all", "log", "observed", "control"] as const).map((f) => (
            <button key={f} type="button" aria-pressed={filter === f} onClick={() => setFilter(f)} title={SOURCE_NOTE[f]}>
              {f}
            </button>
          ))}
        </div>
      </div>
      {shown.length === 0 ? (
        <p className="small muted">No events yet.</p>
      ) : (
        <ol className="events" aria-live="polite">
          {shown.map((e) => (
            <li key={e.seq} className={`ev ev-${e.source}`}>
              <span className="mono tiny">{clock(e.at)}</span>
              <span className={`src src-${e.source}`} title={SOURCE_NOTE[e.source]}>
                {e.source}
              </span>
              <span className="small">{e.message}</span>
            </li>
          ))}
        </ol>
      )}
      <p className="tiny muted">
        <strong>log</strong>: {SOURCE_NOTE.log}. <strong>observed</strong>: {SOURCE_NOTE.observed}.{" "}
        <strong>control</strong>: {SOURCE_NOTE.control}. Sorted by time, newest first.
      </p>
    </section>
  );
}

export function DeadLetters({ entries, error }: { entries: DeadLetter[] | null; error: string | null }) {
  return (
    <section className="panel" aria-labelledby="dlq-h">
      <h2 id="dlq-h" className="panel-h">
        Dead-letter stream
      </h2>
      {error ? (
        <p className="state-msg error">Could not read the dead-letter stream: {error}</p>
      ) : entries === null ? (
        <p className="small muted">Not read yet.</p>
      ) : entries.length === 0 ? (
        <p className="small muted">Empty: no task has exhausted its retry budget.</p>
      ) : (
        <div className="table-scroll">
          <Table
            caption="Dead letters"
            head={["task", "tenant", "attempts", "failed at", "reason"]}
            numeric={[2]}
            rows={entries.map((e) => [<code>{e.task_id}</code>, e.tenant, String(e.attempts), clock(e.failed_at), e.reason])}
          />
        </div>
      )}
      <p className="tiny muted">Task ids and metadata only. Payloads are not exposed by the console.</p>
    </section>
  );
}
