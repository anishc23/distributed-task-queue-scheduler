import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Note, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { median } from "../lib/stats";
import { fmtInt, fmtMs } from "../lib/format";
import { blob } from "../lib/vocab";
import type { Dataset, PartitionRow } from "../lib/schema";
import { predictedWindow } from "./Failover";

const ARMS = ["none", "brief", "long", "flapping"] as const;
const ARM_LABEL: Record<string, string> = {
  none: "none · no partition",
  brief: "brief · ½ × lease",
  long: "long · 2 × lease",
  flapping: "flapping · cut and healed",
};

/** For the no-partition arm the number is the largest natural gap; otherwise the outage around the cut. */
const value = (r: PartitionRow) => (r.partition_ms === 0 ? r.max_gap_ms : r.outage_ms);

export function Partition() {
  const ds = useDataset("partition");
  return (
    <Block id="partition" title="A scheduler cut off from a Redis that stays up" refs="report §4.11">
      <p>
        The third shape of the same failure. A killed leader stops existing; a frozen one observes nothing. A
        partitioned leader keeps running and learns that every call is failing, and still cannot tell whether Redis is
        merely unreachable or it has already been replaced. Each scheduler reaches Redis through its own proxy (
        <a href={blob("internal/netfault/proxy.go")}>
          <code>internal/netfault</code>
        </a>
        ), so the leader's path can be severed at the socket while Redis, the workers and the standby carry on.
      </p>
      <Loaded load={ds} what="the partition trials">
        {(d) => (
          <>
            <ByArm d={d} />
            <Sweep d={d} />
            <Excluded d={d} />
          </>
        )}
      </Loaded>
    </Block>
  );
}

function ByArm({ d }: { d: Dataset<PartitionRow> }) {
  const all = d.rows.filter((r) => r.run === "partition");
  const rows = all.filter((r) => !r.suspect);
  const ttl = rows[0]!.lease_ttl_ms;
  const renew = rows[0]!.lease_renew_ms;
  const longWindow = predictedWindow("crash", ttl, renew)!;
  const points = rows
    .filter((r) => !r.censored && value(r) !== null)
    .map((r) => ({ ...r, value: value(r)! }));
  const table = ARMS.map((a) => {
    const t = rows.filter((r) => r.arm === a);
    const vals = t.filter((r) => !r.censored && value(r) !== null).map((r) => value(r)!);
    const healed = t.filter((r) => r.leaders_after_heal !== null);
    return {
      arm: a,
      n: t.length,
      excluded: all.filter((r) => r.arm === a && r.suspect).length,
      cut: a === "none" ? null : median(t.map((r) => r.partition_ms)),
      cuts: a === "flapping" ? median(t.map((r) => r.cut_count)) : null,
      med: vals.length ? median(vals) : null,
      lo: vals.length ? Math.min(...vals) : null,
      hi: vals.length ? Math.max(...vals) : null,
      moved: t.filter((r) => r.failover).length,
      fenced: t.filter((r) => r.fenced).length,
      oneLeader: healed.length ? `${healed.filter((r) => r.leaders_after_heal === 1).length}/${healed.length}` : "—",
      inside: a === "long" ? vals.filter((v) => v >= longWindow[0] && v <= longWindow[1]).length : null,
    };
  });
  const row = (a: string) => table.find((t) => t.arm === a)!;
  const cutMarks = table.filter((t) => t.cut !== null).map((t) => ({ arm: t.arm, cut: t.cut! }));
  const inversions = rows.reduce((s, r) => s + r.epoch_inversions, 0);
  const dups = rows.reduce((s, r) => s + r.duplicate_completions, 0);
  const unfinished = rows.reduce((s, r) => s + r.unfinished, 0);
  const brief = row("brief");
  const long = row("long");
  const flap = row("flapping");
  const flapMaxGap = Math.max(...rows.filter((r) => r.arm === "flapping").map((r) => r.max_gap_ms ?? 0));

  return (
    <Figure
      id="fig-partition"
      title={`Dispatch outage by partition · ${ttl / 1000} s lease, ${renew / 1000} s renewal, 1 s standby poll`}
      dataset={d}
      caption={
        <>
          <p>
            One dot per trial, log scale; the vertical stroke on each row is how long the path was actually cut. A
            partition shorter than the lease cost its own length and nothing more: median {fmtMs(brief.med)} against
            a {fmtMs(brief.cut)} cut, and leadership moved in {brief.moved} of {brief.n} trials. A partition twice the
            lease cost a median {fmtMs(long.med)}, well under the {fmtMs(long.cut)} cut, because a standby took over
            in {long.moved} of {long.n}; {long.inside} of {long.n} fall inside the same [{longWindow[0] / 1000} s,{" "}
            {longWindow[1] / 1000} s] window a crash does. When the path came back, the old leader was refused by the
            fence in <strong>{long.fenced} of {long.n}</strong> and stood down on its own failed renewal in the rest.
          </p>
          <p>
            Flapping the path {flap.cuts} times in {fmtMs(flap.cut)} moved leadership in {flap.moved} of {flap.n}{" "}
            trials, and every returning leader in those {flap.moved} was fenced; no dispatch gap anywhere in the arm
            exceeded {fmtMs(flapMaxGap)}. Across all {rows.length} trials: exactly one leader after every heal, epoch
            inversions {inversions}, duplicate completions {dups}, tasks unfinished {fmtInt(unfinished)}.
          </p>
        </>
      }
      table={
        <Table
          caption="Partition outcome by arm"
          head={["arm", "n", "cut", "median", "range", "leadership moved", "fenced on heal", "one leader after", "excluded"]}
          numeric={[1, 2, 3, 5, 6, 8]}
          rows={table.map((t) => [
            ARM_LABEL[t.arm]!,
            String(t.n),
            t.cut === null ? "—" : fmtMs(t.cut),
            fmtMs(t.med),
            t.lo === null ? "—" : `${fmtInt(t.lo)}–${fmtInt(t.hi)} ms`,
            `${t.moved}/${t.n}`,
            `${t.fenced}/${t.n}`,
            t.oneLeader,
            String(t.excluded),
          ])}
        />
      }
    >
      <Chart
        label="Dispatch outage per trial for each partition arm on a log scale: a brief partition costs its own length, a long one is capped near the lease, and flapping costs under a second"
        deps={[]}
        height={260}
        build={(w) =>
          Plot.plot({
            ...frame(w, 260),
            marginLeft: 176,
            x: {
              label: "dispatch outage (ms, log scale)",
              type: "log",
              domain: [2, 20000],
              ticks: [2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000],
              tickFormat: (v: number) => (v >= 1000 ? `${v / 1000} s` : `${v}`),
            },
            y: { label: null, domain: [...ARMS], tickFormat: (a: string) => ARM_LABEL[a] },
            marks: [
              Plot.gridX([2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000], { ...GRID }),
              Plot.barX([{ arm: "long", lo: longWindow[0], hi: longWindow[1] }], {
                x1: "lo",
                x2: "hi",
                y: "arm",
                fill: "var(--ink)",
                fillOpacity: 0.08,
                insetTop: 4,
                insetBottom: 4,
              }),
              Plot.tickX(cutMarks, { x: "cut", y: "arm", stroke: "var(--bad)", strokeWidth: 2, strokeDasharray: "3 2" }),
              Plot.dot(points, { x: "value", y: "arm", r: 3, fill: "var(--ink)", fillOpacity: 0.55 }),
              Plot.tip(
                points,
                Plot.pointerX({
                  x: "value",
                  y: "arm",
                  title: (r) =>
                    `${ARM_LABEL[r.arm]} · trial ${r.rep}\n${r.partition_ms === 0 ? "largest natural gap" : "outage"} ${fmtMs(r.value)}` +
                    (r.partition_ms ? `\ncut for ${fmtMs(r.partition_ms)}\n${r.failover ? (r.fenced ? "leadership moved; returning leader fenced" : "leadership moved; returning leader stood down") : "leadership kept"}` : ""),
                }),
              ),
            ],
          })
        }
      />
      <div className="legend" style={{ paddingLeft: 0 }}>
        <span>
          <i className="swatch dot" style={{ background: "var(--ink)" }} /> outage, one trial
        </span>
        <span>
          <i className="swatch" style={{ background: "var(--bad)" }} /> partition length
        </span>
        <span>
          <i className="swatch" style={{ background: "var(--ink)", opacity: 0.15 }} /> crash window [TTL − renew, TTL + poll]
        </span>
      </div>
    </Figure>
  );
}

function Sweep({ d }: { d: Dataset<PartitionRow> }) {
  const all = d.rows.filter((r) => r.run === "partition-sweep");
  const rows = all.filter((r) => !r.suspect);
  if (!rows.length) return null;
  const ttl = rows[0]!.lease_ttl_ms;
  const renew = rows[0]!.lease_renew_ms;
  const [lo, hi] = predictedWindow("crash", ttl, renew)!;
  const mults = [...new Set(rows.map((r) => r.sweep_multiple!))].sort((a, b) => a - b);
  const groups = mults.map((m) => {
    const t = rows.filter((r) => r.sweep_multiple === m);
    const vals = t.filter((r) => !r.censored && r.outage_ms !== null).map((r) => r.outage_ms!);
    return {
      m,
      n: t.length,
      excluded: all.filter((r) => r.sweep_multiple === m && r.suspect).length,
      cut: median(t.map((r) => r.partition_ms))!,
      med: vals.length ? median(vals) : null,
      lo: vals.length ? Math.min(...vals) : null,
      hi: vals.length ? Math.max(...vals) : null,
      moved: t.filter((r) => r.failover).length,
      fenced: t.filter((r) => r.fenced).length,
      censored: t.filter((r) => r.censored).length,
    };
  });
  const points = rows.filter((r) => !r.censored && r.outage_ms !== null);
  const longest = groups[groups.length - 1]!;
  const top = Math.max(longest.cut, ...points.map((p) => p.outage_ms!)) * 1.05;
  // At the cut the grant has between TTL − renew and TTL left, depending on how
  // long ago it was renewed, so those two bounds are where leadership can and
  // must move. Counted from the data rather than asserted.
  const safe = rows.filter((r) => r.partition_ms < ttl - renew);
  const certain = rows.filter((r) => r.partition_ms >= ttl);
  const between = rows.filter((r) => r.partition_ms >= ttl - renew && r.partition_ms < ttl);
  const certainInside = certain.filter((r) => r.outage_ms !== null && r.outage_ms >= lo && r.outage_ms <= hi).length;
  const atTTL = groups.find((g) => g.m === 1);

  return (
    <Figure
      id="fig-partition-sweep"
      title={`Outage against partition length · ${mults.length} lengths, ${median(groups.map((g) => g.n))} trials each`}
      dataset={d}
      caption={
        <>
          <p>
            One dot per trial; the stroke is each length's median. The dotted diagonal is <em>outage = partition</em>;
            the shaded band is the crash window [{lo / 1000} s, {hi / 1000} s]. Short partitions cost their own length.
            From the lease upward the outage stops growing: {certainInside} of {certain.length} trials at or beyond{" "}
            {ttl / 1000} s fall inside the crash window, and the {longest.m}× partition, {fmtMs(longest.cut)} long,
            cost a median {fmtMs(longest.med)}
            {atTTL ? `, against ${fmtMs(atTTL.med)} for one of ${fmtMs(atTTL.cut)}` : ""}.
          </p>
          <p>
            Where leadership moves is predicted by the lease. At the cut the grant has between {(ttl - renew) / 1000} s
            and {ttl / 1000} s left, so a shorter partition cannot cost a failover ({safe.filter((r) => r.failover).length}{" "}
            of {safe.length}), one at or beyond the TTL always does ({certain.filter((r) => r.failover).length} of{" "}
            {certain.length}), and one in between depends on where in the renewal cycle it landed (
            {between.filter((r) => r.failover).length} of {between.length}). The share of returning leaders refused by
            the fence, rather than stopped by their own failed renewal, falls as the partition lengthens; the report
            does not interpret that at {median(groups.map((g) => g.n))} trials per length.
          </p>
        </>
      }
      table={
        <Table
          caption="Outage by partition length"
          head={["length", "cut", "n", "median", "range", "leadership moved", "fenced on heal", "excluded"]}
          numeric={[1, 2, 3, 5, 6, 7]}
          rows={groups.map((g) => [
            `${g.m}× TTL`,
            fmtMs(g.cut),
            String(g.n),
            fmtMs(g.med),
            g.lo === null ? "—" : `${fmtInt(g.lo)}–${fmtInt(g.hi)} ms`,
            `${g.moved}/${g.n}`,
            `${g.fenced}/${g.n}`,
            String(g.excluded),
          ])}
        />
      }
    >
      <Chart
        label="Dispatch outage against partition length: the outage tracks the partition while it is shorter than the lease, then levels off inside the crash window however long the partition runs"
        deps={[]}
        height={300}
        build={(w) =>
          Plot.plot({
            ...frame(w, 300),
            x: { label: "partition length (s)", type: "linear", domain: [0, top], ticks: secondTicks(top), tickFormat: (v: number) => `${v / 1000}` },
            y: { label: "dispatch outage (s)", type: "linear", domain: [0, top], ticks: secondTicks(top), tickFormat: (v: number) => `${v / 1000}` },
            marks: [
              Plot.gridY(secondTicks(top), { ...GRID }),
              Plot.gridX(secondTicks(top), { ...GRID }),
              Plot.rectY([{ lo, hi }], { x1: 0, x2: top, y1: "lo", y2: "hi", fill: "var(--ink)", fillOpacity: 0.08 }),
              Plot.line(
                [
                  [0, 0],
                  [top, top],
                ],
                { stroke: "var(--ink-3)", strokeDasharray: "2 3" },
              ),
              Plot.ruleX([ttl], { stroke: "var(--ink-3)", strokeDasharray: "4 3" }),
              Plot.text([{ x: ttl, y: top }], { x: "x", y: "y", text: () => "lease TTL", dx: 4, dy: 6, textAnchor: "start", fill: "var(--ink-3)" }),
              Plot.dot(points, { x: "partition_ms", y: "outage_ms", r: 2.8, fill: "var(--ink)", fillOpacity: 0.45 }),
              // A short horizontal rule per length rather than tickY, whose x
              // channel is a band scale and would turn this axis ordinal.
              Plot.ruleY(
                groups.filter((g) => g.med !== null),
                { y: "med", x1: (g) => g.cut - top * 0.02, x2: (g) => g.cut + top * 0.02, stroke: "var(--ink)", strokeWidth: 2 },
              ),
              Plot.tip(
                points,
                Plot.pointer({
                  x: "partition_ms",
                  y: "outage_ms",
                  title: (r) =>
                    `${r.sweep_multiple}× lease · trial ${r.rep}\ncut ${fmtMs(r.partition_ms)}, outage ${fmtMs(r.outage_ms)}\n${r.failover ? "leadership moved" : "leadership kept"}`,
                }),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}

/** Whole-second ticks, every 2 s, up to the domain's end. */
function secondTicks(top: number): number[] {
  const out: number[] = [];
  for (let t = 0; t <= top; t += 2000) out.push(t);
  return out;
}

function Excluded({ d }: { d: Dataset<PartitionRow> }) {
  const suspect = d.rows.filter((r) => r.suspect);
  return (
    <Note kind="caveat" title="What this does and does not show">
      <p className="small">
        Every trial watches for the host itself sleeping, directly rather than by inference, and marks any trial it
        finds as suspect. {suspect.length === 0
          ? "None of these trials was marked: both runs were made on mains power with sleep held off."
          : `${suspect.length} trial(s) were marked and are excluded from every number above; the rows remain in the dataset.`}{" "}
        All processes share one host, so the cut is a severed socket, not a real network: this tests how the lease and
        the fence respond to a lost path, not how long a real network takes to fail. The leader is cut off from Redis
        only; a partition that also separated workers from the store, or split the replicas from each other but not
        from Redis, is a different experiment.
      </p>
    </Note>
  );
}
