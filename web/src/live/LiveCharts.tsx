import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { fmtInt, fmtMs } from "../lib/format";
import type { DispatchWindow, Sample } from "./api";
import { classifyGaps } from "./gaps";

// One colour per epoch, cycled. Epochs are terms, not policies, so they do not
// reuse the policy palette.
// Red is kept out of the cycle: it means "something is wrong" everywhere else
// on the page, and a new term is not that.
const EPOCH_COLORS = ["#1b1b1a", "#6b2f87", "#9a6b1f", "#2f6f8f", "#6f6a2a", "#4C72B0"];
const epochColor = (e: number) => (e <= 0 ? "#b9b5a8" : EPOCH_COLORS[(e - 1) % EPOCH_COLORS.length]!);

/** Contiguous stretches of the same lease holder, for the shaded bands. */
function terms(samples: Sample[]) {
  const out: { from: Date; to: Date; owner: string; epoch: number }[] = [];
  for (const s of samples) {
    const t = new Date(s.t);
    const last = out[out.length - 1];
    if (s.owner && last && last.owner === s.owner && last.epoch === s.epoch) last.to = t;
    else if (s.owner) out.push({ from: t, to: t, owner: s.owner, epoch: s.epoch ?? 0 });
  }
  return out;
}

export function HistoryCharts({ samples }: { samples: Sample[] }) {
  const recent = samples.slice(-300);
  const pts = recent.map((s) => ({ ...s, t: new Date(s.t) }));
  const bands = terms(recent);
  const rateRows = pts.flatMap((s) => [
    { t: s.t, series: "dispatched", v: s.dispatched_per_s },
    { t: s.t, series: "completed", v: s.completed_per_s },
  ]);
  const depthRows = pts.flatMap((s) => [
    { t: s.t, series: "pending", v: s.pending },
    { t: s.t, series: "in flight", v: s.in_flight },
    { t: s.t, series: "not yet admitted", v: s.not_admitted ?? null },
  ]);
  const series = (name: string) =>
    name === "dispatched" || name === "pending" ? "var(--ink)" : name === "not yet admitted" ? "var(--bad)" : "#9a6b1f";
  const empty = pts.length < 2;

  const band = (w: number, h: number, rows: { t: Date; series: string; v: number | null }[], label: string) =>
    Plot.plot({
      ...frame(w, h),
      marginTop: 12,
      x: { type: "time", label: null },
      y: { label, grid: true, domain: [0, Math.max(5, ...rows.map((r) => r.v ?? 0)) * 1.08], tickFormat: "~s" },
      marks: [
        Plot.rectX(bands, {
          x1: "from",
          x2: "to",
          fill: (b) => epochColor(b.epoch),
          fillOpacity: 0.07,
        }),
        Plot.text(bands, {
          x: "from",
          frameAnchor: "top",
          dy: 8,
          dx: 4,
          textAnchor: "start",
          text: (b) => `${b.owner} · e${b.epoch}`,
          fill: (b) => epochColor(b.epoch),
        }),
        Plot.gridY({ ...GRID }),
        // Missing values (Redis unreadable) are left as gaps, never drawn as zero.
        Plot.line(rows, { x: "t", y: (d) => d.v ?? NaN, z: "series", stroke: (d) => series(d.series), strokeWidth: 1.5 }),
        Plot.tip(rows.filter((d) => d.v !== null), Plot.pointerX({ x: "t", y: "v", title: (d) => `${d.series}: ${fmtInt(d.v)}\n${d.t.toLocaleTimeString("en-GB")}` })),
      ],
    });

  return (
    <section className="panel" aria-labelledby="hist-h">
      <h2 id="hist-h" className="panel-h">
        The last five minutes
      </h2>
      {empty ? (
        <p className="small muted">Collecting samples…</p>
      ) : (
        <div className="two-col">
          <div>
            <div className="legend" style={{ padding: 0 }}>
              <span><i className="swatch" style={{ background: "var(--ink)" }} /> dispatched / s</span>
              <span><i className="swatch" style={{ background: "#9a6b1f" }} /> completed / s</span>
            </div>
            <Chart label="Dispatch and completion rate over the last five minutes, shaded by lease holder" deps={[samples]} height={210} build={(w) => band(w, 210, rateRows, "tasks / s")} />
          </div>
          <div>
            <div className="legend" style={{ padding: 0 }}>
              <span><i className="swatch" style={{ background: "var(--ink)" }} /> pending</span>
              <span><i className="swatch" style={{ background: "#9a6b1f" }} /> in flight</span>
              <span><i className="swatch" style={{ background: "var(--bad)" }} /> not yet admitted</span>
            </div>
            <Chart label="Pending and in-flight tasks over the last five minutes, shaded by lease holder" deps={[samples]} height={210} build={(w) => band(w, 210, depthRows, "tasks")} />
          </div>
        </div>
      )}
      <p className="tiny muted">
        One sample per poll. Rates come from differences between consecutive Redis counter readings, which survive a
        process restart. Background shading marks which scheduler held the lease, and under which epoch. Gaps are
        polls in which Redis could not be read.
      </p>
    </section>
  );
}

export function DispatchLog({ window: w, waiting, samples }: { window: DispatchWindow | null; waiting: number | null; samples: Sample[] }) {
  if (!w) {
    return (
      <section className="panel">
        <h2 className="panel-h">Dispatch log</h2>
        <p className="small muted">Reading the execution stream…</p>
      </section>
    );
  }
  const buckets = w.buckets.map((b) => ({ ...b, t: new Date(b.t_ms) }));
  const gaps = classifyGaps(w.gaps ?? [], samples).map((g) => ({ ...g, a: new Date(g.startMs), b: new Date(g.endMs) }));
  const stalls = gaps.filter((g) => g.kind === "stall");
  const longestStall = stalls.length ? Math.max(...stalls.map((g) => g.ms)) : null;
  const sinceLast = w.last_dispatch_ms !== undefined ? Date.parse(w.read_at) - w.last_dispatch_ms : null;
  return (
    <section className="panel" aria-labelledby="disp-h">
      <div className="panel-head">
        <h2 id="disp-h" className="panel-h">
          Dispatch log · last {w.window_s} s
        </h2>
        <span className="small">
          epoch inversions{" "}
          <span className={`tag ${w.epoch_inversions === 0 ? "ok" : "bad"}`}>{w.epoch_inversions}</span>
        </span>
      </div>
      <dl className="pairs inline">
        <dt>dispatches</dt>
        <dd className="num">
          {fmtInt(w.count)}
          {w.truncated ? " (read capped)" : ""}
        </dd>
        <dt>longest stall</dt>
        <dd className="num">
          {longestStall !== null ? fmtMs(longestStall) : "none"}
          <span className="small muted"> · no dispatch while work was waiting</span>
        </dd>
        <dt>since last dispatch</dt>
        <dd className="num">
          {sinceLast !== null ? fmtMs(sinceLast) : "—"}
          {sinceLast !== null && sinceLast > 1000 && waiting === 0 && <span className="small muted"> · nothing waiting, so idle is expected</span>}
          {sinceLast !== null && sinceLast > 1000 && waiting !== null && waiting > 0 && <span className="small" style={{ color: "var(--bad)" }}> · {waiting} tasks waiting</span>}
        </dd>
        <dt>epochs in window</dt>
        <dd className="num">{w.epochs.length ? w.epochs.join(", ") : "—"}</dd>
      </dl>
      {buckets.length === 0 ? (
        <p className="small muted">No dispatches in this window. Submit a workload to see one.</p>
      ) : (
        <Chart
          label={`Dispatches per quarter second over the last ${w.window_s} seconds, coloured by the epoch that dispatched them; longest gap ${w.max_gap_ms ?? 0} milliseconds`}
          deps={[w.read_at]}
          height={190}
          build={(width) =>
            Plot.plot({
              ...frame(width, 190),
              x: { type: "time", label: null, domain: [new Date(w.from_ms), new Date(w.read_at)] },
              y: { label: `dispatches / ${w.bucket_ms} ms`, grid: true },
              marks: [
                Plot.gridY({ ...GRID }),
                Plot.rectX(gaps, {
                  x1: "a",
                  x2: "b",
                  fill: (g) => (g.kind === "stall" ? "var(--bad)" : "var(--ink-3)"),
                  fillOpacity: (g) => (g.kind === "stall" ? 0.12 : 0.06),
                }),
                Plot.tip(gaps, Plot.pointerX({ x: (g) => new Date((g.startMs + g.endMs) / 2), title: (g) => `${g.kind === "stall" ? "stall: work was waiting" : g.kind === "idle" ? "idle: nothing waiting" : "gap: not enough samples to say"}\n${fmtMs(g.ms)} without a dispatch` })),
                Plot.rectY(buckets, {
                  x1: "t",
                  x2: (d) => new Date(d.t_ms + w.bucket_ms),
                  y: "n",
                  fill: (d) => epochColor(d.epoch),
                  fillOpacity: 0.85,
                }),
                Plot.tip(buckets, Plot.pointerX({ x: "t", y: "n", title: (d) => `epoch ${d.epoch || "none (retry)"}: ${d.n} dispatches\n${d.t.toLocaleTimeString("en-GB")}` })),
              ],
            })
          }
        />
      )}
      <div className="legend" style={{ padding: 0 }}>
        {w.epochs.map((e) => (
          <span key={e}>
            <i className="swatch dot" style={{ background: epochColor(e), borderRadius: 0 }} /> epoch {e}
          </span>
        ))}
        <span>
          <i className="swatch dot" style={{ background: "var(--bad)", opacity: 0.35, borderRadius: 0 }} /> stall: work waiting, nothing dispatched
        </span>
        <span>
          <i className="swatch dot" style={{ background: "var(--ink-3)", opacity: 0.25, borderRadius: 0 }} /> idle: nothing waiting
        </span>
        {w.unstamped > 0 && (
          <span>
            <i className="swatch dot" style={{ background: epochColor(0), borderRadius: 0 }} /> retries, no epoch ({w.unstamped})
          </span>
        )}
      </div>
      <p className="tiny muted">
        Read from the execution stream itself, where the dispatch script stamps every entry with its time and epoch.
        This is what was dispatched, not what any scheduler reports. A gap of a second or more is shaded, and classed
        by the pending count sampled while it lasted: a stall had work waiting, so it is how a failover looks from
        here; an idle gap had nothing to dispatch. An inversion would be a superseded leader writing after its
        successor; the fence exists to keep it at zero.
      </p>
    </section>
  );
}
