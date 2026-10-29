import { useMemo, useState } from "react";
import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Note, Segmented, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { POLICY_COLOR, POLICY_LABEL, WORKLOAD_LABEL, WORKLOADS, policyOrder } from "../lib/vocab";
import { mean, stdev, unique } from "../lib/stats";
import { fmt } from "../lib/format";
import type { Dataset, MatrixRow } from "../lib/schema";

type Metric = keyof Pick<
  MatrixRow,
  "deadline_miss_rate" | "latency_p50_s" | "latency_p99_s" | "wait_max_s" | "throughput_tasks_per_s" | "worker_utilization" | "jain_fairness_service"
>;
const METRICS: { value: Metric; label: string; axis: string; digits: number }[] = [
  { value: "deadline_miss_rate", label: "miss rate", axis: "deadline miss rate", digits: 3 },
  { value: "latency_p50_s", label: "p50", axis: "p50 latency (s)", digits: 3 },
  { value: "latency_p99_s", label: "p99", axis: "p99 latency (s)", digits: 3 },
  { value: "wait_max_s", label: "max wait", axis: "longest queue wait (s)", digits: 3 },
  { value: "throughput_tasks_per_s", label: "throughput", axis: "throughput (tasks/s)", digits: 1 },
  { value: "worker_utilization", label: "utilisation", axis: "worker slot utilisation", digits: 3 },
  { value: "jain_fairness_service", label: "Jain index", axis: "Jain's fairness index (service)", digits: 3 },
];

const BURSTY_HINT = "Measured at about 1.57× capacity, not 1.25×: not load-matched to the other workloads";

export function MainMatrix() {
  const ds = useDataset("matrix");
  return (
    <Block id="matrix" title="Policy by workload, 25 repetitions each" refs="report §4.1 · 400 experiments">
      <p>
        The main matrix: every policy against every workload, 4,000 tasks per experiment, 25 repetitions per cell.
        Work-conserving schedulers change who waits, not how much gets done. Throughput is flat within 0.3% except on
        the heavy-tailed workload, where EDF defers long tasks and leaves slots idle while they drain.
      </p>
      <Loaded load={ds} what="the main matrix">
        {(d) => <MatrixExplorer d={d} />}
      </Loaded>
    </Block>
  );
}

function MatrixExplorer({ d }: { d: Dataset<MatrixRow> }) {
  const present = unique(d.rows.map((r) => r.workload));
  const workloads = WORKLOADS.filter((w) => present.includes(w));
  const [workload, setWorkload] = useState<string>("heavy_tailed");
  const [metric, setMetric] = useState<Metric>("deadline_miss_rate");
  const m = METRICS.find((x) => x.value === metric)!;

  const sel = useMemo(() => d.rows.filter((r) => r.workload === workload && r[metric] !== null), [d, workload, metric]);
  const policies = policyOrder(unique(sel.map((r) => r.scheduler)));
  const stats = policies.map((p) => {
    const v = sel.filter((r) => r.scheduler === p).map((r) => r[metric]);
    return { scheduler: p, mean: mean(v)!, sd: stdev(v), n: v.length };
  });

  return (
    <>
      {workload === "bursty" && (
        <Note kind="caveat" title="Not load-matched">
          <p>
            These four rows were run at about 1.57× capacity, not the 1.25× of the other workloads. The bursty
            workload ignores the configured arrival rate. Compare policies within this workload freely, but compare
            bursty against other workloads only in the <a href="#load">load sweep</a>. This was found while building
            this site and is recorded in the report's corrections.
          </p>
        </Note>
      )}
      <Figure
        id="fig-matrix"
        title={`${m.axis} · ${WORKLOAD_LABEL[workload]}`}
        dataset={d}
        controls={
          <>
            <Segmented
              label="workload"
              value={workload}
              onChange={setWorkload}
              options={workloads.map((w) =>
                w === "bursty"
                  ? { value: w, label: WORKLOAD_LABEL[w]!, flag: "≠", hint: BURSTY_HINT }
                  : { value: w, label: WORKLOAD_LABEL[w]! },
              )}
            />
            <Segmented label="metric" value={metric} onChange={setMetric} options={METRICS} />
          </>
        }
        caption={
          <p>
            Each dot is one experiment of 4,000 tasks; the bar is the mean of 25. Read deadline misses together with
            the longest wait: EDF buys its miss rate on the heavy-tailed workload by deferring the largest tasks.
            {metric === "jain_fairness_service" &&
              " Jain's index over completed service is demand-limited on the skewed workload, which is why every policy scores 0.246; per-tenant latency, below, is what separates them."}
          </p>
        }
        table={
          <Table
            caption="Mean and standard deviation per policy"
            head={["policy", "mean", "sd", "n"]}
            numeric={[1, 2, 3]}
            rows={stats.map((s) => [POLICY_LABEL[s.scheduler]!, fmt(s.mean, m.digits), fmt(s.sd, m.digits), String(s.n)])}
          />
        }
      >
        <Chart
          label={`${m.axis} for each policy on the ${WORKLOAD_LABEL[workload]} workload, 25 repetitions each`}
          deps={[workload, metric]}
          height={240}
          build={(w) =>
            Plot.plot({
              ...frame(w, 50 + policies.length * 52),
              marginLeft: 112,
              x: { label: m.axis, grid: true, zero: metric === "deadline_miss_rate" },
              fy: { label: null, domain: policies, tickFormat: (p: string) => POLICY_LABEL[p], padding: 0.12 },
              y: { axis: null, domain: [0, 1] },
              marks: [
                Plot.gridX({ ...GRID }),
                // Repetitions are spread vertically by index, not randomly, so
                // the chart is identical on every render.
                Plot.dot(sel, {
                  x: metric,
                  fy: "scheduler",
                  y: (r: MatrixRow) => 0.15 + 0.7 * ((r.repetition * 7) % 25) / 24,
                  fill: (r: MatrixRow) => POLICY_COLOR[r.scheduler],
                  r: 2.6,
                  fillOpacity: 0.6,
                }),
                Plot.tickX(stats, { x: "mean", fy: "scheduler", stroke: "var(--ink)", strokeWidth: 2 }),
                Plot.text(stats, {
                  x: "mean",
                  fy: "scheduler",
                  frameAnchor: "top",
                  text: (s) => fmt(s.mean, m.digits),
                  dy: 2,
                  dx: 4,
                  textAnchor: "start",
                  fill: "var(--ink)",
                }),
                Plot.tip(
                  sel,
                  Plot.pointerX({
                    x: metric,
                    fy: "scheduler",
                    y: 0.5,
                    title: (r: MatrixRow) => `${POLICY_LABEL[r.scheduler]}, repetition ${r.repetition}\n${m.label}: ${fmt(r[metric], m.digits)}\nrun ${r.run}`,
                  }),
                ),
              ],
            })
          }
        />
      </Figure>
    </>
  );
}
