import { useEffect, useState } from "react";
import { ApiError, post } from "./api";
import type { LiveData } from "./useConsole";

const ACTION_TEXT: Record<string, { label: string; effect: string; timed?: boolean; confirm?: boolean }> = {
  kill: {
    label: "Kill",
    effect: "SIGKILL. A crash: nothing is released, so a standby waits out the lease. Report §4.7 measured 3.5–5 s.",
    confirm: true,
  },
  terminate: {
    label: "Terminate",
    effect: "SIGTERM. A rollout: the lease is released on the way out, so a standby takes over within one poll.",
    confirm: true,
  },
  freeze: {
    label: "Freeze",
    effect: "SIGSTOP, then SIGCONT after the duration. Past the lease TTL the leader is superseded while still alive. On resume the fence refuses it (§4.8).",
    timed: true,
  },
  partition: {
    label: "Partition",
    effect: "Cuts this process's path to Redis, which stays up for everyone else, then heals. The process keeps running and keeps believing it leads.",
    timed: true,
  },
  restart: { label: "Restart", effect: "Starts a process that has exited, with the same arguments." },
};

export function ControlPanel({ live, token, onForget }: { live: LiveData; token: string | null; onForget: () => void }) {
  const c = live.controls;
  const [target, setTarget] = useState("leader");
  const [duration, setDuration] = useState(10);
  const [armed, setArmed] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);

  // A destructive action needs a second click within four seconds.
  useEffect(() => {
    if (!armed) return;
    const t = window.setTimeout(() => setArmed(null), 4000);
    return () => window.clearTimeout(t);
  }, [armed]);

  const processes = c?.processes ?? {};
  const names = Object.keys(processes).sort();
  const role = target === "leader" ? "scheduler" : names.includes(target) && target.startsWith("sched") ? "scheduler" : "worker";
  const actions = c?.actions?.[role] ?? [];
  const leader = live.snapshot?.leadership.held ? live.snapshot.leadership.owner : undefined;

  const send = async (path: string, body: unknown, what: string) => {
    if (!token) return;
    setBusy(true);
    setResult(null);
    try {
      const r = await post<{ result?: string; preset?: string; total?: number }>(path, body, token);
      setResult({ ok: true, text: r.result ?? `${what}: accepted` });
      live.refresh();
    } catch (e) {
      const msg = e instanceof ApiError ? e.message : String(e);
      setResult({ ok: false, text: `${what} refused: ${msg}` });
      if (e instanceof ApiError && e.status === 401) onForget();
    } finally {
      setBusy(false);
      setArmed(null);
    }
  };

  const act = (action: string) => {
    const spec = ACTION_TEXT[action]!;
    if (spec.confirm && armed !== action) {
      setArmed(action);
      return;
    }
    const body: Record<string, unknown> = { action, target };
    if (spec.timed) body.duration_ms = duration * 1000;
    void send("/faults", body, `${spec.label} ${target}`);
  };

  return (
    <section className="panel" aria-labelledby="ctl-h">
      <h2 id="ctl-h" className="panel-h">
        Controls
      </h2>
      {!c ? (
        <p className="small muted">Reading what this console allows…</p>
      ) : (
        <>
          {c.presets && c.presets.length > 0 && (
            <fieldset className="ctl-group" disabled={!token || busy}>
              <legend>Submit a workload</legend>
              <ul className="presets">
                {c.presets.map((p) => (
                  <li key={p.id}>
                    <button type="button" className="btn" aria-describedby={`preset-${p.id}`} onClick={() => void send("/workload", { preset: p.id }, p.label)}>
                      {p.label}
                    </button>
                    <span id={`preset-${p.id}`} className="tiny muted">
                      {p.count} tasks at {p.offered_load}× capacity. {p.description}
                    </span>
                  </li>
                ))}
              </ul>
              {c.submission && (
                <p className="tiny muted">
                  {c.submission.running ? "Submitting" : "Last submission"}: {c.submission.preset}, {c.submission.total} tasks at{" "}
                  {Math.round(c.submission.rate_per_s)}/s
                  {c.submission.running ? "…" : c.submission.error ? ` · failed: ${c.submission.error}` : " · done"}
                </p>
              )}
            </fieldset>
          )}

          <fieldset className="ctl-group" disabled={!c.faults_enabled || !token || busy}>
            <legend>Inject a fault</legend>
            {!c.faults_enabled ? (
              <p className="small">
                <span className="tag unknown">off</span> {c.reason}
              </p>
            ) : (
              <>
                <div className="ctl-row">
                  <label className="control">
                    <span className="control-label">target</span>
                    <select value={target} onChange={(e) => setTarget(e.target.value)}>
                      <option value="leader">the current leader{leader ? ` (${leader})` : ""}</option>
                      {names.map((n) => (
                        <option key={n} value={n}>
                          {n} · {processes[n]!.state}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="control">
                    <span className="control-label">duration (freeze, partition)</span>
                    <span>
                      <input
                        type="number"
                        min={c.min_duration_ms / 1000}
                        max={c.max_duration_ms / 1000}
                        value={duration}
                        onChange={(e) => setDuration(Math.min(c.max_duration_ms / 1000, Math.max(c.min_duration_ms / 1000, Number(e.target.value) || 1)))}
                        style={{ width: 64 }}
                      />{" "}
                      s
                    </span>
                  </label>
                </div>
                <div className="ctl-buttons">
                  {actions.map((a) => {
                    const spec = ACTION_TEXT[a]!;
                    return (
                      <button key={a} type="button" className={`btn${armed === a ? " armed" : ""}`} onClick={() => act(a)} aria-describedby={`eff-${a}`}>
                        {armed === a ? `Confirm: ${spec.label.toLowerCase()} ${target === "leader" ? leader ?? "leader" : target}` : spec.label}
                      </button>
                    );
                  })}
                  <button type="button" className="btn" onClick={() => void send("/faults/clear", {}, "End timed faults")}>
                    End all timed faults
                  </button>
                </div>
                <dl className="effects tiny">
                  {actions.map((a) => (
                    <div key={a}>
                      <dt>{ACTION_TEXT[a]!.label}</dt>
                      <dd id={`eff-${a}`}>{ACTION_TEXT[a]!.effect}</dd>
                    </div>
                  ))}
                </dl>
              </>
            )}
          </fieldset>

          {!token && (c.faults_enabled || c.presets) && (
            <p className="small">
              <span className="tag warn">read-only</span> Controls need the session token. Open the URL the console
              printed in its terminal; it ends in <code>?token=…</code>.
            </p>
          )}
          <p className="result small" role="status" aria-live="polite">
            {result && <span className={result.ok ? "" : "error"}>{result.text}</span>}
          </p>
          <p className="tiny muted">
            Every control acts only on processes this console started, through a fixed list of operations. The console
            listens on loopback only and checks the session token and request origin on every change.
          </p>
        </>
      )}
    </section>
  );
}
