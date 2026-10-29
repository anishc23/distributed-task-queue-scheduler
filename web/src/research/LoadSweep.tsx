import { useMemo, useState } from "react";
import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Legend, Segmented, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { POLICY_COLOR, POLICY_LABEL, WORKLOAD_LABEL, policyOrder } from "../lib/vocab";
import { groupBy, mean, spreadPct, unique } from "../lib/stats";
import { fmt, fmtPct } from "../lib/format";
import type { SweepRow } from "../lib/schema";

type Metric = "latency_p99_s" | "latency_p50_s" | "deadline_miss_rate" | "wait_max_s" | "throughput_tasks_per_s";
const METRICS: { value: Metric; label: string; axis: string; digits: number }[] = [
  { value: "latency_p99_s", label: "p99 latency", axis: "p99 latency (s)", digits: 3 },
  { value: "latency_p50_s", label: "p50 latency", axis: "p50 latency (s)", digits: 3 },
  { value: "deadline_miss_rate", label: "deadline miss rate", axis: "deadline miss rate", digits: 4 },
  { value: "wait_max_s", label: "max wait", axis: "longest queue wait (s)", digits: 3 },
  { value: "throughput_tasks_per_s", label: "throughput", axis: "throughput (tasks/s)", digits: 1 },
];

export function LoadSweep() {
  const ds = useDataset("sweep");
  return (
    <Block id="load" title="When the choice of policy matters" refs="report §4.4 · 336 experiments">
      <p>
        Seven offered loads from half to three times capacity, every policy and workload, three repetitions each. Below
        capacity there is no queue, so every policy dispatches immediately. Far above it the backlog fixes the makespan
        whatever order is chosen. Policy only decides the outcome in a narrow band near capacity.
      </p>
      <Loaded load={ds} what="the load sweep">
        {(d) => <SweepExplorer rows={d.rows} dataset={d} />}
      </Loaded>
    </Block>
  );
}

function SweepExplorer({ rows, dataset }: { rows: SweepRow[]; dataset: Parameters<typeof Figure>[0]["dataset"] }) {
  const workloads = unique(rows.map((r) => r.workload));
  const [workload, setWorkload] = useState(workloads.includes("uniform") ? "uniform" : workloads[0]!);
  const [metric, setMetric] = useState<Metric>("latency_p99_s");
  const m = METRICS.find((x) => x.value === metric)!;

  const sel = useMemo(() => rows.filter((r) => r.workload === workload && r[metric] !== null), [rows, workload, metric]);
  const loads = unique(sel.map((r) => r.offered_load)).sort((a, b) => a - b);
  const policies = policyOrder(unique(sel.map((r) => r.scheduler)));
  const cells = useMemo(() => {
    const g = groupBy(sel, (r) => `${r.scheduler}|${r.offered_load}`);
    return [...g.entries()].map(([k, v]) => {
      const [scheduler, load] = k.split("|");
      return { scheduler: scheduler!, offered_load: Number(load), value: mean(v.map((r) => r[metric]))!, n: v.length };
    });
  }, [sel, metric]);
  const spreads = loads.map((load) => ({
    offered_load: load,
    spread: spreadPct(cells.filter((c) => c.offered_load === load).map((c) => c.value)),
  }));

  return (
    <>
      <Figure
        id="fig-sweep"
        title={`Offered load against ${m.label} · ${WORKLOAD_LABEL[workload]}`}
        dataset={dataset}
        controls={
          <>
            <Segmented
              label="workload"
              value={workload}
              onChange={setWorkload}
              options={workloads.map((w) => ({ value: w, label: WORKLOAD_LABEL[w] ?? w }))}
            />
            <Segmented label="metric" value={metric} onChange={setMetric} options={METRICS} />
          </>
        }
        legend={<Legend items={policies.map((p) => ({ label: POLICY_LABEL[p]!, color: POLICY_COLOR[p]! }))} />}
        caption={
          <p>
            Lines join the mean of three repetitions; faint dots are the individual repetitions. Each load is an arrival
            rate set as a multiple of this workload's own measured capacity.{" "}
            {workload === "heavy_tailed" &&
              "Heavy-tailed points are noisier than the rest: this workload needs about 25 repetitions to settle, and the sweep has 3."}
          </p>
        }
        table={
          <Table
            caption="Cell means by policy and offered load"
            head={["load", ...policies.map((p) => POLICY_LABEL[p]!), "spread"]}
            numeric={[0, ...policies.map((_, i) => i + 1), policies.length + 1]}
            rows={loads.map((load) => [
              `${load}×`,
              ...policies.map((p) => fmt(cells.find((c) => c.scheduler === p && c.offered_load === load)?.value, m.digits)),
              fmtPct(spreads.find((s) => s.offered_load === load)?.spread),
            ])}
          />
        }
      >
        <Chart
          label={`${m.axis} against offered load for each policy, ${WORKLOAD_LABEL[workload]} workload`}
          deps={[workload, metric]}
          build={(w) =>
            Plot.plot({
              ...frame(w, 330),
              x: { label: "offered load (× capacity)", type: "log", domain: [0.45, 3.3], ticks: loads, tickFormat: (d: number) => `${d}×` },
              y: { label: m.axis, grid: true, zero: metric !== "throughput_tasks_per_s" },
              marks: [
                Plot.gridY({ ...GRID }),
                Plot.ruleX([1], { stroke: "var(--ink-3)", strokeDasharray: "2 3" }),
                Plot.text([1], { x: (d: number) => d, frameAnchor: "top", dy: -8, text: () => "capacity", fill: "var(--ink-3)" }),
                Plot.dot(sel, { x: "offered_load", y: metric, fill: (d: SweepRow) => POLICY_COLOR[d.scheduler], r: 2.2, fillOpacity: 0.35 }),
                Plot.line(cells, { x: "offered_load", y: "value", z: "scheduler", stroke: (d) => POLICY_COLOR[d.scheduler], strokeWidth: 1.8, sort: "offered_load" }),
                Plot.dot(cells, { x: "offered_load", y: "value", fill: (d) => POLICY_COLOR[d.scheduler], r: 3.2, stroke: "var(--panel)", strokeWidth: 1 }),
                Plot.tip(
                  cells,
                  Plot.pointer({
                    x: "offered_load",
                    y: "value",
                    title: (d) => `${POLICY_LABEL[d.scheduler]} at ${d.offered_load}×\n${m.label}: ${fmt(d.value, m.digits)} (mean of ${d.n})`,
                  }),
                ),
              ],
            })
          }
        />
      </Figure>

      <Figure
        id="fig-spread"
        title={`How much the policy choice moves ${m.label} · ${WORKLOAD_LABEL[workload]}`}
        caption={
          <p>
            (worst − best) / best across the four policies' cell means, at each load. For uniform p99 this is the
            report's headline: 0.1% at 0.5×, 93% at 1×, 1.1% at 3×. A value near zero means nothing a scheduler does
            changes this metric at that load.
          </p>
        }
      >
        <Chart
          label={`Spread between best and worst policy in ${m.label} at each offered load`}
          deps={[workload, metric]}
          height={200}
          build={(w) =>
            Plot.plot({
              ...frame(w, 200),
              x: { label: "offered load (× capacity)", type: "band", domain: loads, tickFormat: (d: number) => `${d}×` },
              y: { label: "spread across policies (%)", grid: true },
              marks: [
                Plot.gridY({ ...GRID }),
                Plot.barY(spreads.filter((s) => s.spread !== null), { x: "offered_load", y: "spread", fill: "var(--ink-2)" }),
                Plot.text(spreads.filter((s) => s.spread !== null), {
                  x: "offered_load",
                  y: "spread",
                  text: (d) => fmtPct(d.spread),
                  dy: -7,
                  fill: "var(--ink)",
                }),
                Plot.ruleY([0], { stroke: "var(--ink-3)" }),
              ],
            })
          }
        />
      </Figure>
    </>
  );
}
