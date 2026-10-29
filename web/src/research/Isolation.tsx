import { useMemo, useState } from "react";
import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Legend, Segmented, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { POLICY_COLOR, POLICY_LABEL, policyOrder } from "../lib/vocab";
import { mean, unique } from "../lib/stats";
import { fmt } from "../lib/format";
import type { Dataset, PriorityRow, TenantRow } from "../lib/schema";

export function Isolation() {
  const prio = useDataset("priority");
  const ten = useDataset("tenants");
  return (
    <Block id="isolation" title="Starvation and tenant isolation" refs="report §4.2, §4.3, §5.3">
      <p>
        Two policies exist to protect someone. Strict priority protects important tasks, and the measured cost falls
        entirely on the least important. Weighted fair queuing protects small tenants from a noisy neighbour. The
        obvious fairness metric cannot see that protection at all.
      </p>
      <Loaded load={prio} what="the priority breakdown">
        {(d) => <Starvation d={d} />}
      </Loaded>
      <Loaded load={ten} what="per-tenant latency">
        {(d) => <Tenants d={d} />}
      </Loaded>
    </Block>
  );
}

function Starvation({ d }: { d: Dataset<PriorityRow> }) {
  const policies = policyOrder(unique(d.rows.map((r) => r.scheduler)));
  const levels = unique(d.rows.map((r) => r.priority)).sort((a, b) => a - b);
  const cell = (s: string, p: number) => d.rows.find((r) => r.scheduler === s && r.priority === p);
  return (
    <Figure
      id="fig-priority"
      title="p95 latency by priority level · uniform workload"
      dataset={d}
      legend={<Legend items={policies.map((p) => ({ label: POLICY_LABEL[p]!, color: POLICY_COLOR[p]! }))} />}
      caption={
        <p>
          One p95 per policy and level, pooled over 25 repetitions (100,000 tasks per policy). Under strict priority
          every level above 0 completes in about 75 ms while level 0 waits 4.4 s, a 60× spread, and there is no ageing
          to bound it. FIFO, EDF and WFQ are flat because none of them reads the priority field.
        </p>
      }
      table={
        <Table
          caption="p95 latency by priority"
          head={["policy", ...levels.map((l) => `prio ${l}`), "0 vs 4"]}
          numeric={levels.map((_, i) => i + 1).concat(levels.length + 1)}
          rows={policies.map((s) => {
            const lo = cell(s, levels[0]!)?.latency_p95_s;
            const hi = cell(s, levels[levels.length - 1]!)?.latency_p95_s;
            return [
              POLICY_LABEL[s]!,
              ...levels.map((l) => fmt(cell(s, l)?.latency_p95_s)),
              lo && hi ? `${(lo / hi).toFixed(1)}×` : "—",
            ];
          })}
        />
      }
    >
      <Chart
        label="p95 latency at each priority level for each policy; strict priority shows 4.4 seconds at level 0 and about 0.07 seconds above it"
        deps={[]}
        height={280}
        build={(w) =>
          Plot.plot({
            ...frame(w, 280),
            x: { label: "task priority (0 lowest, 4 highest)", ticks: levels, tickFormat: "d" },
            y: { label: "p95 latency (s)", grid: true, zero: true },
            marks: [
              Plot.gridY({ ...GRID }),
              Plot.line(d.rows, { x: "priority", y: "latency_p95_s", z: "scheduler", stroke: (r) => POLICY_COLOR[r.scheduler], strokeWidth: 1.8 }),
              Plot.dot(d.rows, { x: "priority", y: "latency_p95_s", fill: (r) => POLICY_COLOR[r.scheduler], r: 3.2 }),
              Plot.tip(
                d.rows,
                Plot.pointer({
                  x: "priority",
                  y: "latency_p95_s",
                  title: (r: PriorityRow) => `${POLICY_LABEL[r.scheduler]}, priority ${r.priority}\np95 ${fmt(r.latency_p95_s)} s over ${r.tasks.toLocaleString("en-GB")} tasks`,
                }),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}

const SERIES: { value: TenantRow["series"]; label: string }[] = [
  { value: "main", label: "main matrix · 16 slots · sleep" },
  { value: "sleep", label: "8 slots · sleep" },
  { value: "cpu", label: "8 slots · real CPU" },
];

function Tenants({ d }: { d: Dataset<TenantRow> }) {
  const [series, setSeries] = useState<TenantRow["series"]>("main");
  const sel = useMemo(() => d.rows.filter((r) => r.series === series && r.latency_p95_s !== null), [d, series]);
  const policies = policyOrder(unique(sel.map((r) => r.scheduler)));
  const tenants = unique(sel.map((r) => r.tenant)).sort();
  const reps = unique(sel.map((r) => r.repetition)).length;
  const summary = policies.map((p) => {
    const a = mean(sel.filter((r) => r.scheduler === p && r.tenant === "A").map((r) => r.latency_p95_s));
    const small = mean(sel.filter((r) => r.scheduler === p && r.tenant !== "A").map((r) => r.latency_p95_s));
    return { p, a, small, ratio: a && small ? small / a : null };
  });

  return (
    <Figure
      id="fig-tenants"
      title="p95 latency per tenant under a 90 / 3 / 3 / 2 / 2 skew"
      dataset={d}
      controls={<Segmented label="configuration" value={series} onChange={setSeries} options={SERIES} />}
      legend={<Legend items={policies.map((p) => ({ label: POLICY_LABEL[p]!, color: POLICY_COLOR[p]!, shape: "dot" }))} />}
      caption={
        <>
          <p>
            Tenant A submits 90% of the work. Under FIFO, priority and EDF the four small tenants queue behind A's
            backlog and see its latency: equal treatment, not isolation. WFQ gives them about 35× lower p95 at a 2%
            cost to A. Jain's index over completed service reports 0.246 for every policy on this workload, because it
            is demand-limited. Small tenants cannot be granted service they never asked for.
          </p>
          <p>
            Log scale; one dot per repetition ({reps} here). The 8-slot configurations are the real-work comparison
            of report §5.3. Under real CPU work WFQ's advantage grew, from 31.8× to 39.9×. Compare shapes, not
            absolute values, across configurations.
          </p>
        </>
      }
      table={
        <Table
          caption="Tenant A against the mean of the small tenants"
          head={["policy", "A p95 (s)", "small tenants p95 (s)", "small / A"]}
          numeric={[1, 2, 3]}
          rows={summary.map((s) => [POLICY_LABEL[s.p]!, fmt(s.a), fmt(s.small), s.ratio ? `${s.ratio.toFixed(3)}×` : "—"])}
        />
      }
    >
      <Chart
        label="p95 latency per tenant for each policy; under WFQ the small tenants B to E are an order of magnitude faster than tenant A"
        deps={[series]}
        height={300}
        build={(w) =>
          Plot.plot({
            ...frame(w, 300),
            fx: { label: null, domain: tenants, tickFormat: (t: string) => (t === "A" ? "A (90%)" : `${t} (small)`), padding: 0.08 },
            x: { axis: null, domain: policies },
            y: { label: "p95 latency (s, log scale)", type: "log", ticks: [0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10], tickFormat: (v: number) => `${v}` },
            marks: [
              Plot.gridY({ ...GRID }),
              Plot.dot(sel, {
                fx: "tenant",
                x: "scheduler",
                y: "latency_p95_s",
                fill: (r: TenantRow) => POLICY_COLOR[r.scheduler],
                r: 2.4,
                fillOpacity: 0.55,
              }),
              Plot.tip(
                sel,
                Plot.pointer({
                  fx: "tenant",
                  x: "scheduler",
                  y: "latency_p95_s",
                  title: (r: TenantRow) => `tenant ${r.tenant} · ${POLICY_LABEL[r.scheduler]} · rep ${r.repetition}\np95 ${fmt(r.latency_p95_s)} s`,
                }),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}
