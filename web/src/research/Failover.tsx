import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Note, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { median } from "../lib/stats";
import { fmtInt, fmtMs } from "../lib/format";
import type { Dataset, FailoverRow } from "../lib/schema";

const ARMS = ["none", "graceful", "crash", "pause", "single"] as const;
const ARM_LABEL: Record<string, string> = {
  none: "none · no fault",
  graceful: "graceful · SIGTERM",
  crash: "crash · SIGKILL",
  pause: "pause · SIGSTOP, then SIGCONT",
  single: "single replica · SIGKILL",
};
const STANDBY_RETRY_MS = 1000;

/** States the fit as counted, so the sentence cannot claim more than the data. */
function insideAll(groups: { n: number; inside: number }[]): string {
  const n = groups.reduce((s, g) => s + g.n, 0);
  const inside = groups.reduce((s, g) => s + g.inside, 0);
  return inside === n ? `All ${n} trials` : `${inside} of ${n} trials`;
}

/** The two-term model: time until the lease is free, plus time until a standby next polls. */
export function predictedWindow(arm: string, ttl: number, renew: number): [number, number] | null {
  if (arm === "graceful") return [0, STANDBY_RETRY_MS];
  if (arm === "crash" || arm === "pause") return [ttl - renew, ttl + STANDBY_RETRY_MS];
  return null;
}

export function Failover() {
  const ds = useDataset("failover");
  return (
    <Block id="failover" title="What a scheduler failure costs" refs="report §4.7 · 99 trials">
      <p>
        Real scheduler processes were started and stopped with real signals. Nothing about the failure is simulated.
        The outage is the gap between the last task dispatched before the signal and the first one after, read from
        the dispatch timestamp stamped on every execution-stream entry. Before each kill the harness checks that
        exactly one replica claims leadership. After each trial it replays the dispatch log and checks that epochs
        never decrease.
      </p>
      <Loaded load={ds} what="the failover trials">
        {(d) => (
          <>
            <ByArm d={d} />
            <ByTTL d={d} />
          </>
        )}
      </Loaded>
    </Block>
  );
}

function ByArm({ d }: { d: Dataset<FailoverRow> }) {
  const rows = d.rows.filter((r) => r.run === "failover");
  const ttl = rows[0]!.lease_ttl_ms;
  const renew = rows[0]!.lease_renew_ms;
  // For the no-fault arm the meaningful number is the largest natural gap
  // between dispatches: the floor every other arm is read against.
  const points = rows
    .filter((r) => !r.censored)
    .map((r) => ({ ...r, value: r.arm === "none" ? r.max_gap_ms! : r.outage_ms! }))
    .filter((r) => r.value > 0);
  const censored = rows.filter((r) => r.censored);
  const windows = ARMS.flatMap((a) => {
    const w = predictedWindow(a, ttl, renew);
    return w ? [{ arm: a, lo: Math.max(w[0], 1), hi: w[1] }] : [];
  });
  const table = ARMS.map((a) => {
    const t = rows.filter((r) => r.arm === a);
    const vals = t.filter((r) => !r.censored).map((r) => (a === "none" ? r.max_gap_ms! : r.outage_ms!));
    const w = predictedWindow(a, ttl, renew);
    const inside = w ? vals.filter((v) => v >= w[0] && v <= w[1]).length : null;
    return {
      arm: a,
      n: t.length,
      med: vals.length ? median(vals) : null,
      lo: vals.length ? Math.min(...vals) : null,
      hi: vals.length ? Math.max(...vals) : null,
      censored: t.filter((r) => r.censored).length,
      unfinished: t.reduce((s, r) => s + r.unfinished, 0),
      inside,
      w,
    };
  });
  const inversions = d.rows.reduce((s, r) => s + r.epoch_inversions, 0);
  const dups = d.rows.reduce((s, r) => s + r.duplicate_completions, 0);

  return (
    <Figure
      id="fig-failover"
      title={`Dispatch outage by failure · ${ttl / 1000} s lease, ${renew / 1000} s renewal, 1 s standby poll`}
      dataset={d}
      caption={
        <>
          <p>
            One dot per trial, log scale. Shaded bands are the windows the two-term model predicts before looking at
            the data: <em>time until the lease is free</em> plus <em>time until a standby polls</em>. That gives [0,
            1 s] for a graceful stop, which releases the lease, and [{(ttl - renew) / 1000} s, {(ttl + STANDBY_RETRY_MS) / 1000} s]
            for a crash or a freeze. {insideAll(table.filter((t) => t.inside !== null).map((t) => ({ n: t.n - t.censored, inside: t.inside! })))} of the recovering arms fall inside their windows. The no-fault row
            shows the largest natural gap between dispatches, the floor these are read against.
          </p>
          <p>
            With a single replica no trial recovered. Those trials are censored, not plotted as a large number: the
            queue stopped dispatching permanently and {fmtInt(table.find((t) => t.arm === "single")?.unfinished)} tasks
            never finished. Across all {d.rows.length} trials, epoch inversions: {inversions}; duplicate completions:{" "}
            {dups}.
          </p>
        </>
      }
      table={
        <Table
          caption="Outage by arm"
          head={["arm", "n", "median", "range", "predicted", "inside", "censored", "unfinished"]}
          numeric={[1, 2, 5, 6, 7]}
          rows={table.map((t) => [
            ARM_LABEL[t.arm]!,
            String(t.n),
            t.med === null ? "never recovered" : fmtMs(t.med),
            t.lo === null ? "—" : `${fmtInt(t.lo)}–${fmtInt(t.hi)} ms`,
            t.w ? `[${t.w[0] / 1000} s, ${t.w[1] / 1000} s]` : "—",
            t.inside === null ? "—" : `${t.inside}/${t.n - t.censored}`,
            String(t.censored),
            fmtInt(t.unfinished),
          ])}
        />
      }
    >
      <Chart
        label="Outage per trial for each failure arm on a log scale, with predicted windows shaded; graceful stops cost under a second, crashes and freezes three and a half to five seconds, and the single-replica arm never recovered"
        deps={[]}
        height={300}
        build={(w) =>
          Plot.plot({
            ...frame(w, 300),
            marginLeft: 214,
            x: {
              label: "dispatch outage (ms, log scale)",
              type: "log",
              domain: [2, 30000],
              ticks: [2, 10, 100, 1000, 5000, 20000],
              tickFormat: (v: number) => (v >= 1000 ? `${v / 1000} s` : `${v}`),
            },
            y: { label: null, domain: [...ARMS], tickFormat: (a: string) => ARM_LABEL[a] },
            marks: [
              Plot.gridX([2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000], { ...GRID }),
              Plot.barX(windows, { x1: "lo", x2: "hi", y: "arm", fill: "var(--ink)", fillOpacity: 0.08, insetTop: 4, insetBottom: 4 }),
              Plot.dot(points, { x: "value", y: "arm", r: 3, fill: "var(--ink)", fillOpacity: 0.55 }),
              Plot.barX(censored.length ? [{ arm: "single" }] : [], {
                x1: 10000,
                x2: 30000,
                y: "arm",
                fill: "none",
                stroke: "var(--bad)",
                strokeDasharray: "3 3",
                insetTop: 6,
                insetBottom: 6,
              }),
              Plot.text(censored.length ? [{ arm: "single" }] : [], {
                x: 9000,
                y: "arm",
                text: () => `never recovered · ${censored.length} of ${censored.length} censored →`,
                textAnchor: "end",
                fill: "var(--bad)",
              }),
              Plot.tip(
                points,
                Plot.pointerX({
                  x: "value",
                  y: "arm",
                  title: (r) => `${ARM_LABEL[r.arm]} · trial ${r.rep}\n${r.arm === "none" ? "largest natural gap" : "outage"} ${fmtMs(r.value)}${r.pause_ms ? `\nfrozen for ${fmtMs(r.pause_ms)}` : ""}`,
                }),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}

function ByTTL({ d }: { d: Dataset<FailoverRow> }) {
  const crash = d.rows.filter((r) => r.arm === "crash" && !r.censored);
  const ttls = [...new Set(crash.map((r) => r.lease_ttl_ms))].sort((a, b) => a - b);
  const bands = ttls.map((t) => {
    const r = crash.find((x) => x.lease_ttl_ms === t)!;
    const [lo, hi] = predictedWindow("crash", t, r.lease_renew_ms)!;
    const vals = crash.filter((x) => x.lease_ttl_ms === t).map((x) => x.outage_ms!);
    return {
      ttl: t,
      renew: r.lease_renew_ms,
      lo,
      hi,
      n: vals.length,
      med: median(vals)!,
      inside: vals.filter((v) => v >= lo && v <= hi).length,
      range: [Math.min(...vals), Math.max(...vals)],
    };
  });
  return (
    <>
      <Figure
        id="fig-ttl"
        title="Crash outage against lease TTL"
        dataset={d}
        caption={
          <p>
            The crash arm repeated at 2 s, 5 s and 10 s leases, with renewal at TTL/3.3 and the standby poll at 1 s.{" "}
            {insideAll(bands)} fall inside the predicted envelope [TTL − renew, TTL + poll]. Medians sit slightly below the midpoint at every TTL. No mechanism is claimed for that.
            Failover time is therefore a tuning decision. The 5 s default allows a healthy leader three renewal attempts
            to survive a slow Redis before being deposed for nothing.
          </p>
        }
        table={
          <Table
            caption="Crash outage by TTL"
            head={["lease TTL", "renew", "n", "median", "range", "predicted", "inside"]}
            numeric={[2, 3, 6]}
            rows={bands.map((b) => [
              `${b.ttl / 1000} s`,
              `${b.renew / 1000} s`,
              String(b.n),
              fmtMs(b.med),
              `${fmtInt(b.range[0])}–${fmtInt(b.range[1])} ms`,
              `[${b.lo / 1000} s, ${b.hi / 1000} s]`,
              `${b.inside}/${b.n}`,
            ])}
          />
        }
      >
        <Chart
          label="Crash outage per trial at lease TTLs of 2, 5 and 10 seconds, each inside its predicted envelope"
          deps={[]}
          height={260}
          build={(w) =>
            Plot.plot({
              ...frame(w, 260),
              x: { label: "lease TTL", type: "band", domain: ttls, tickFormat: (t: number) => `${t / 1000} s`, padding: 0.4 },
              y: { label: "outage (s)", grid: true, domain: [0, 11.5], transform: (v: number) => v / 1000 },
              marks: [
                Plot.gridY({ ...GRID }),
                Plot.barY(bands, { x: "ttl", y1: "lo", y2: "hi", fill: "var(--ink)", fillOpacity: 0.08 }),
                Plot.dot(crash, { x: "lease_ttl_ms", y: "outage_ms", r: 2.8, fill: "var(--ink)", fillOpacity: 0.45 }),
                Plot.tickY(bands, { x: "ttl", y: "med", stroke: "var(--ink)", strokeWidth: 2 }),
              ],
            })
          }
        />
      </Figure>
      <Note kind="caveat" title="Lower bound, steady state">
        <p className="small">
          All replicas share one host, which removes the network from the failover path, so these outages are a lower
          bound for a multi-node deployment. Every trial stops the leader while the queue is in steady state, so this
          is the time to resume dispatching, not the time to work off the backlog the outage created.
        </p>
      </Note>
    </>
  );
}
