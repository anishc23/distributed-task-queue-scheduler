import { useMemo, useState } from "react";
import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Legend, Note, Segmented, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame, GRID } from "../lib/plot";
import { POLICY_COLOR, POLICY_LABEL, SYSTEM_COLOR, SYSTEM_LABEL, WORKLOAD_LABEL, WORKLOADS, policyOrder } from "../lib/vocab";
import { groupBy, mean, median, unique } from "../lib/stats";
import { fmt, fmtInt, fmtPct } from "../lib/format";
import type { BaselineRow, CeilingRow, ControlRow, Dataset } from "../lib/schema";

export function Cost() {
  const control = useDataset("control");
  const ceiling = useDataset("ceiling");
  const baseline = useDataset("baseline");
  return (
    <Block id="cost" title="What centralising the decision costs" refs="report §4.6, §4.10">
      <p>
        The scheduler is a deliberate single writer. That has a throughput price, measured three ways: against the
        same queue with the scheduler switched off, against the scheduler's own ceiling, and against Asynq, a Go and
        Redis queue whose workers pull directly with no scheduler at all.
      </p>
      <Loaded load={control} what="the control arm">
        {(d) => (
          <>
            <ControlOverhead d={d} />
            <Tradeoff d={d} />
          </>
        )}
      </Loaded>
      <Loaded load={ceiling} what="the scheduler ceiling">
        {(d) => <Ceiling d={d} />}
      </Loaded>
      <Loaded load={baseline} what="the Asynq comparison">
        {(d) => <Baseline d={d} />}
      </Loaded>
    </Block>
  );
}

function ControlOverhead({ d }: { d: Dataset<ControlRow> }) {
  const workloads = WORKLOADS.filter((w) => d.rows.some((r) => r.workload === w));
  const [workload, setWorkload] = useState<string>("uniform");
  const loads = unique(d.rows.map((r) => r.offered_load)).sort((a, b) => a - b);
  const cells = useMemo(() => {
    const sel = d.rows.filter((r) => r.workload === workload);
    const g = groupBy<ControlRow, string>(sel, (r) => `${r.scheduler}|${r.offered_load}`);
    const m = new Map<string, number | null>([...g].map(([k, v]) => [k, mean(v.map((r) => r.throughput_tasks_per_s))]));
    const out: { scheduler: string; offered_load: number; overhead: number }[] = [];
    for (const [k, v] of m) {
      const [s, load] = k.split("|");
      const none = m.get(`none|${load}`);
      if (s === "none" || v === null || v === undefined || !none) continue;
      out.push({ scheduler: s!, offered_load: Number(load), overhead: (1 - v / none) * 100 });
    }
    return out;
  }, [d, workload]);
  const policies = policyOrder(unique(cells.map((c) => c.scheduler)));

  return (
    <Figure
      id="fig-control"
      title={`Throughput lost against no scheduler · ${WORKLOAD_LABEL[workload]}`}
      dataset={d}
      controls={
        <Segmented label="workload" value={workload} onChange={setWorkload} options={workloads.map((w) => ({ value: w, label: WORKLOAD_LABEL[w]! }))} />
      }
      legend={<Legend items={policies.map((p) => ({ label: POLICY_LABEL[p]!, color: POLICY_COLOR[p]! }))} />}
      caption={
        <p>
          1 − (policy throughput / no-scheduler throughput), means of three repetitions. In the no-scheduler arm,
          workers consume the ingress stream directly in FIFO order. Below capacity there is nothing to lose. Above
          it FIFO costs about 1.5%, the extra round trip through the pending set, and buys identical ordering. FIFO
          is strictly worse than no scheduler and earns its place only as a baseline. The other policies pay more
          because they reorder work.
        </p>
      }
      table={
        <Table
          caption="Throughput overhead by load"
          head={["load", ...policies.map((p) => POLICY_LABEL[p]!)]}
          numeric={[0, ...policies.map((_, i) => i + 1)]}
          rows={loads.map((l) => [`${l}×`, ...policies.map((p) => fmtPct(cells.find((c) => c.scheduler === p && c.offered_load === l)?.overhead))])}
        />
      }
    >
      <Chart
        label="Percentage of throughput lost against no scheduler, by policy and offered load"
        deps={[workload]}
        height={260}
        build={(w) =>
          Plot.plot({
            ...frame(w, 260),
            fx: { label: "offered load (× capacity)", domain: loads, tickFormat: (x: number) => `${x}×`, padding: 0.12 },
            x: { axis: null, domain: policies, padding: 0.15 },
            y: { label: "throughput lost vs no scheduler (%)", grid: true },
            marks: [
              Plot.gridY({ ...GRID }),
              Plot.barY(cells, { fx: "offered_load", x: "scheduler", y: "overhead", fill: (c) => POLICY_COLOR[c.scheduler] }),
              Plot.ruleY([0], { stroke: "var(--ink-3)" }),
              Plot.tip(
                cells,
                Plot.pointer({
                  fx: "offered_load",
                  x: "scheduler",
                  y: "overhead",
                  title: (c) => `${POLICY_LABEL[c.scheduler]} at ${c.offered_load}×: ${fmtPct(c.overhead, 2)} throughput lost`,
                }),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}

function Tradeoff({ d }: { d: Dataset<ControlRow> }) {
  const sel = d.rows.filter((r) => r.workload === "heavy_tailed" && r.offered_load === 1.25);
  const arms = policyOrder(unique(sel.map((r) => r.scheduler)));
  const stats = arms.map((s) => {
    const v = sel.filter((r) => r.scheduler === s);
    return {
      scheduler: s,
      miss: mean(v.map((r) => r.deadline_miss_rate))!,
      wait: mean(v.map((r) => r.wait_max_s))!,
      tput: mean(v.map((r) => r.throughput_tasks_per_s))!,
    };
  });
  const none = stats.find((s) => s.scheduler === "none");
  return (
    <Figure
      id="fig-tradeoff"
      title="What acting on the decision costs, and buys · heavy-tailed at 1.25×"
      dataset={d}
      caption={
        <p>
          Each policy's mean throughput against its deadline miss rate. Faint dots are the three repetitions. EDF
          gives up about 10.5% of throughput and returns {none ? `${(none.miss / stats.find((s) => s.scheduler === "edf")!.miss).toFixed(0)}×` : "—"}{" "}
          fewer deadline misses; its price shows up as the longest wait in the table, 8.2 s against 2.1 s. WFQ's return,
          tenant isolation, is not on this axis.
        </p>
      }
      table={
        <Table
          caption="Heavy-tailed at 1.25x"
          head={["arm", "miss rate", "max wait (s)", "tasks/s", "vs none"]}
          numeric={[1, 2, 3, 4]}
          rows={stats.map((s) => [
            POLICY_LABEL[s.scheduler]!,
            fmt(s.miss),
            fmt(s.wait, 2),
            fmt(s.tput, 1),
            none && s.scheduler !== "none" ? fmtPct((s.tput / none.tput - 1) * 100) : "—",
          ])}
        />
      }
    >
      <Chart
        label="Throughput against deadline miss rate for each arm; EDF has by far the lowest miss rate and the lowest throughput"
        deps={[]}
        height={280}
        build={(w) =>
          Plot.plot({
            ...frame(w, 280),
            x: { label: "throughput (tasks/s)", grid: true },
            y: { label: "deadline miss rate", grid: true, domain: [0, 0.7] },
            marks: [
              Plot.gridY({ ...GRID }),
              Plot.dot(sel, { x: "throughput_tasks_per_s", y: "deadline_miss_rate", fill: (r) => POLICY_COLOR[r.scheduler], r: 2.5, fillOpacity: 0.4 }),
              Plot.dot(stats, { x: "tput", y: "miss", fill: (s) => POLICY_COLOR[s.scheduler], r: 5, stroke: "var(--panel)" }),
              ...([true, false] as const).map((left) =>
                Plot.text(
                  stats.filter((s) => (s.scheduler === "none") === left),
                  { x: "tput", y: "miss", text: (s) => POLICY_LABEL[s.scheduler], dx: left ? 10 : -10, dy: left ? 10 : 0, textAnchor: left ? "start" : "end", fill: "var(--ink)" },
                ),
              ),
            ],
          })
        }
      />
    </Figure>
  );
}

function Ceiling({ d }: { d: Dataset<CeilingRow> }) {
  const phase = (fenced: boolean, p: CeilingRow["phase"]) => mean(d.rows.filter((r) => r.fenced === fenced && r.phase === p).map((r) => r.rate_tasks_per_s));
  const bestDispatch = (fenced: boolean) => Math.max(...d.rows.filter((r) => r.fenced === fenced && r.phase === "dispatch").map((r) => r.rate_tasks_per_s));
  const dispatch = d.rows.filter((r) => r.phase === "dispatch");
  const dispatchMeans = [...groupBy(dispatch, (r) => `${r.fenced}|${r.batch}`)].map(([k, v]) => ({
    fenced: k.startsWith("true"),
    batch: v[0]!.batch,
    rate: mean(v.map((r) => r.rate_tasks_per_s))!,
  }));
  const rows: [string, number | null, number | null][] = [
    ["admission (ingress → pending)", phase(false, "admit"), phase(true, "admit")],
    ["end to end (both loops)", phase(false, "engine"), phase(true, "engine")],
    ["dispatch, best case (batch 256)", bestDispatch(false), bestDispatch(true)],
  ];
  return (
    <Figure
      id="fig-ceiling"
      title="The scheduler's own ceiling, with the fence inert and live"
      dataset={d}
      legend={<Legend items={[{ label: "fence inert (epoch 0)", color: "var(--ink-3)" }, { label: "fence live (epoch 1)", color: "var(--ink)" }]} />}
      caption={
        <>
          <p>
            With no workers attached, the scheduler admits about 15,000 tasks/s and runs end to end at about 11,400
            tasks/s. At a 50 ms mean task that is enough for roughly 570 worker slots, so every experiment above ran
            the scheduler at about 3% of its capacity and measured policy, not saturation. Batched dispatch is 17×
            faster than admission and was never the constraint.
          </p>
          <p>
            Turning the fence on moved end-to-end throughput by −0.5%. Read that as "inside the noise": each cell is
            one run, and dispatch came out faster fenced than unfenced. The check adds one <code>GET</code> inside a
            script that has already paid for its round trip.
          </p>
        </>
      }
      table={
        <Table
          caption="Ceiling by stage"
          head={["stage", "fence inert", "fence live", "change"]}
          numeric={[1, 2, 3]}
          rows={rows.map(([n, a, b]) => [n, fmtInt(a), fmtInt(b), a && b ? fmtPct((b / a - 1) * 100) : "—"])}
        />
      }
    >
      <Chart
        label="Dispatch rate against batch size, fence inert and live, with admission rate as a reference line"
        deps={[]}
        height={250}
        build={(w) =>
          Plot.plot({
            ...frame(w, 250),
            x: { label: "dispatch batch size", type: "log", ticks: [1, 8, 32, 64, 128, 256], tickFormat: "d" },
            y: { label: "tasks/s", grid: true, tickFormat: "s" },
            marks: [
              Plot.gridY({ ...GRID }),
              Plot.ruleY([phase(false, "admit")!], { stroke: "var(--ink-3)", strokeDasharray: "4 3" }),
              Plot.text([phase(false, "admit")!], { y: (v: number) => v, frameAnchor: "right", text: () => "admission ceiling", dy: -7, fill: "var(--ink-2)" }),
              Plot.line(dispatchMeans, { x: "batch", y: "rate", z: "fenced", stroke: (r) => (r.fenced ? "var(--ink)" : "var(--ink-3)"), strokeWidth: 1.6, sort: "batch" }),
              Plot.dot(dispatch, { x: "batch", y: "rate_tasks_per_s", fill: (r) => (r.fenced ? "var(--ink)" : "var(--ink-3)"), r: 2, fillOpacity: 0.5 }),
            ],
          })
        }
      />
    </Figure>
  );
}

function Baseline({ d }: { d: Dataset<BaselineRow> }) {
  const execs = unique(d.rows.map((r) => r.exec_ms)).sort((a, b) => a - b);
  const conc = unique(d.rows.map((r) => r.concurrency));
  const med = [...groupBy(d.rows, (r) => `${r.system}|${r.exec_ms}`)].map(([k, v]) => ({
    system: k.split("|")[0] as BaselineRow["system"],
    exec_ms: v[0]!.exec_ms,
    tput: median(v.map((r) => r.throughput_per_s))!,
    lo: Math.min(...v.map((r) => r.throughput_per_s ?? Infinity)),
    hi: Math.max(...v.map((r) => r.throughput_per_s ?? -Infinity)),
  }));
  const ceiling = conc.length === 1 ? execs.filter((e) => e > 0).map((e) => ({ exec_ms: e, ceiling: conc[0]! / (e / 1000) })) : [];
  const delta = execs.map((e) => {
    const tq = med.find((m) => m.system === "taskqueue" && m.exec_ms === e)?.tput;
    const aq = med.find((m) => m.system === "asynq" && m.exec_ms === e)?.tput;
    return { exec_ms: e, tq, aq, delta: tq && aq ? ((tq - aq) / aq) * 100 : null };
  });
  return (
    <>
      <Figure
        id="fig-asynq"
        title="This queue against Asynq, by task duration"
        dataset={d}
        legend={
          <Legend
            items={[
              { label: SYSTEM_LABEL.taskqueue!, color: SYSTEM_COLOR.taskqueue! },
              { label: SYSTEM_LABEL.asynq!, color: SYSTEM_COLOR.asynq! },
              { label: `slot ceiling (${conc[0]} / task duration)`, color: "var(--ink-3)" },
            ]}
          />
        }
        caption={
          <p>
            Medians of five repetitions, each 8,000 pre-enqueued tasks drained by {conc[0]} slots against the same
            Redis, one system at a time, order alternated. Below about 3 ms per task the central scheduler costs about
            30% of throughput. From 5 ms both sit within a few per cent of each other and of the ceiling that slot
            occupancy imposes, so the benchmark is measuring <code>sleep</code>, not queue machinery. The small lead
            this queue shows there is not interpreted.
          </p>
        }
        table={
          <Table
            caption="Median throughput"
            head={["task duration", "this queue", "Asynq", "difference"]}
            numeric={[0, 1, 2, 3]}
            rows={delta.map((r) => [`${r.exec_ms} ms`, fmt(r.tq, 1), fmt(r.aq, 1), fmtPct(r.delta)])}
          />
        }
      >
        <Chart
          label="Throughput against task duration for this queue and Asynq; Asynq is about 30 percent faster below 3 milliseconds and they converge from 5 milliseconds"
          deps={[]}
          height={300}
          build={(w) =>
            Plot.plot({
              ...frame(w, 300),
              x: { label: "simulated task duration (ms)", type: "point", domain: execs },
              y: { label: "throughput (tasks/s)", grid: true, domain: [0, 14000] },
              marks: [
                Plot.gridY({ ...GRID }),
                Plot.line(ceiling, { x: "exec_ms", y: "ceiling", stroke: "var(--ink-3)", strokeDasharray: "4 3", clip: true }),
                Plot.dot(d.rows, { x: "exec_ms", y: "throughput_per_s", fill: (r) => SYSTEM_COLOR[r.system], r: 2.4, fillOpacity: 0.4 }),
                Plot.line(med, { x: "exec_ms", y: "tput", z: "system", stroke: (r) => SYSTEM_COLOR[r.system], strokeWidth: 1.8 }),
                Plot.dot(med, { x: "exec_ms", y: "tput", fill: (r) => SYSTEM_COLOR[r.system], r: 3.6, stroke: "var(--panel)" }),
                Plot.tip(
                  med,
                  Plot.pointer({
                    x: "exec_ms",
                    y: "tput",
                    title: (r) => `${SYSTEM_LABEL[r.system]} at ${r.exec_ms} ms\nmedian ${fmt(r.tput, 1)} tasks/s (range ${fmt(r.lo, 0)}–${fmt(r.hi, 0)})`,
                  }),
                ),
              ],
            })
          }
        />
      </Figure>
      <Note title="What the Asynq comparison does not claim">
        <p className="small">
          Asynq has scheduled and recurring tasks, task groups, a web UI and years of production use. This project has
          a pluggable global policy with deadline-aware and fair-queuing disciplines. The comparison covers one axis on
          one machine: throughput against task duration. It is a larger gap than the 1.5% above because Asynq is a
          different architecture, not this queue with a feature switched off.
        </p>
      </Note>
    </>
  );
}
