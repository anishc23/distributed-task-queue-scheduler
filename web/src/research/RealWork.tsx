import { useState } from "react";
import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Segmented, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { POLICY_COLOR, POLICY_LABEL, policyOrder } from "../lib/vocab";
import { mean, stdev, unique } from "../lib/stats";
import { fmt } from "../lib/format";
import type { Dataset, RealworkRow } from "../lib/schema";

type Metric = "deadline_miss_rate" | "latency_p99_s" | "exec_mean_s";
const METRICS: { value: Metric; label: string; axis: string; digits: number }[] = [
  { value: "deadline_miss_rate", label: "miss rate", axis: "deadline miss rate", digits: 3 },
  { value: "latency_p99_s", label: "p99 latency", axis: "p99 latency (s)", digits: 3 },
  { value: "exec_mean_s", label: "measured task time", axis: "mean measured execution (s)", digits: 4 },
];

export function RealWork() {
  const ds = useDataset("realwork");
  return (
    <Block id="realwork" title="Simulating work with sleeps is not neutral" refs="report §5.3 · 200 experiments">
      <p>
        By default workers sleep for a task's declared duration. That makes sizes exact, but a sleeping worker uses no
        CPU. To test whether the conclusions survive real work, an execution mode burns each task's duration in
        chained SHA-256, calibrated per machine. The same heavy-tailed experiment was run both ways at 8 slots on 10
        cores, 25 repetitions each.
      </p>
      <Loaded load={ds} what="the real-work comparison">
        {(d) => <Explorer d={d} />}
      </Loaded>
      <blockquote className="note">
        <p>
          <strong>WFQ's use of cost is relative; EDF's is absolute.</strong> WFQ compares tenants against one another,
          so an error that inflates every task by a similar factor is common-mode and cancels. EDF compares each task
          against a wall-clock deadline, where the same error does not cancel. Under real work EDF's miss rate rose
          8.4× and strict priority overtook it. WFQ's small-tenant advantage grew, from 31.8× to 39.9× (see the
          8-slot configurations in <a href="#isolation">tenant isolation</a>). The EDF results above should be read as
          an upper bound reachable only with perfect cost information.
        </p>
      </blockquote>
    </Block>
  );
}

function Explorer({ d }: { d: Dataset<RealworkRow> }) {
  const [metric, setMetric] = useState<Metric>("deadline_miss_rate");
  const m = METRICS.find((x) => x.value === metric)!;
  const policies = policyOrder(unique(d.rows.map((r) => r.scheduler)));
  const stats = policies.flatMap((p) =>
    (["sleep", "cpu"] as const).map((mode) => {
      const v = d.rows.filter((r) => r.scheduler === p && r.exec_mode === mode).map((r) => r[metric]);
      return { scheduler: p, mode, mean: mean(v)!, sd: stdev(v), n: v.length };
    }),
  );
  const pairs = policies.map((p) => ({
    scheduler: p,
    sleep: stats.find((s) => s.scheduler === p && s.mode === "sleep")!,
    cpu: stats.find((s) => s.scheduler === p && s.mode === "cpu")!,
  }));
  return (
    <Figure
      id="fig-realwork"
      title={`${m.axis}: sleep against real CPU · heavy-tailed, 1.25×`}
      dataset={d}
      controls={<Segmented label="metric" value={metric} onChange={setMetric} options={METRICS} />}
      caption={
        <p>
          Hollow circle: sleep. Filled: real CPU. Faint dots are the 25 repetitions of each. Real execution inflates
          measured task time by 1.5–1.9×, because workers share the machine with Redis and the Go runtime. EDF inflates
          most. The EDF sleep figure carries a standard deviation larger than its mean, so its 8.4× degradation should
          be read with that spread in mind.
        </p>
      }
      table={
        <Table
          caption="Sleep against CPU"
          head={["policy", "sleep (mean ± sd)", "cpu (mean ± sd)", "cpu / sleep"]}
          numeric={[1, 2, 3]}
          rows={pairs.map((p) => [
            POLICY_LABEL[p.scheduler]!,
            `${fmt(p.sleep.mean, m.digits)} ± ${fmt(p.sleep.sd, m.digits)}`,
            `${fmt(p.cpu.mean, m.digits)} ± ${fmt(p.cpu.sd, m.digits)}`,
            `${(p.cpu.mean / p.sleep.mean).toFixed(2)}×`,
          ])}
        />
      }
    >
      <Chart
        label={`${m.axis} for each policy under sleep and under real CPU execution`}
        deps={[metric]}
        height={260}
        build={(w) =>
          Plot.plot({
            ...frame(w, 50 + policies.length * 50),
            marginLeft: 112,
            x: { label: m.axis, grid: true, zero: true },
            y: { label: null, domain: policies, tickFormat: (p: string) => POLICY_LABEL[p], padding: 0.4 },
            marks: [
              Plot.gridX({ ...GRID }),
              ...(["sleep", "cpu"] as const).map((mode) =>
                Plot.dot(
                  d.rows.filter((r) => r.exec_mode === mode),
                  { x: metric, y: "scheduler", dy: mode === "sleep" ? -8 : 8, fill: (r: RealworkRow) => POLICY_COLOR[r.scheduler], r: 1.8, fillOpacity: 0.3 },
                ),
              ),
              Plot.link(pairs, { x1: (p) => p.sleep.mean, x2: (p) => p.cpu.mean, y: "scheduler", stroke: (p) => POLICY_COLOR[p.scheduler], strokeWidth: 2, markerEnd: "arrow" }),
              Plot.dot(pairs, { x: (p) => p.sleep.mean, y: "scheduler", r: 5, stroke: (p) => POLICY_COLOR[p.scheduler], fill: "var(--panel)", strokeWidth: 2 }),
              Plot.dot(pairs, { x: (p) => p.cpu.mean, y: "scheduler", r: 5, fill: (p) => POLICY_COLOR[p.scheduler] }),
            ],
          })
        }
      />
    </Figure>
  );
}
