import { Block } from "./ResearchPage";
import { useIndex } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Table } from "../components/Controls";
import { fmtInt } from "../lib/format";

export function Method() {
  const index = useIndex();
  return (
    <Block id="method" title="Method" refs="report §3 · Appendix B">
      <div className="two-col">
        <div>
          <h3>Design</h3>
          <p className="small">
            Four policies by four workloads: uniform, bursty, heavy-tailed (Pareto durations) and a 90/3/3/2/2 tenant
            skew. Each experiment runs in its own Redis key namespace, and every CSV row repeats the full run
            identity, so results from different runs cannot be pooled by accident.
          </p>
          <p className="small">
            Workloads are finite and seeded. For a given seed the task sequence is byte-identical on any machine,
            and a test asserts that changing the arrival rate changes nothing else. Timing is not reproducible, only
            the workload; repetitions are the mitigation.
          </p>
          <h3>Offered load</h3>
          <pre className="small">
            <code>{`capacity     = worker_slots / mean_execution_seconds
offered_load = mean_arrival_rate / capacity`}</code>
          </pre>
          <p className="small">
            Computed per workload, because equal arrival rates are not equal loads. The bursty workload ignores its
            nominal rate, and an early revision ran it at 1.57× while the others sat at 1.25×. The load sweep
            corrects this. The 25-repetition main matrix did not, and is labelled accordingly below.
          </p>
        </div>
        <div>
          <h3>What is measured, and from where</h3>
          <p className="small">
            Statistics come from task-level records the queue writes to Redis: submit, dispatch, start and finish
            times per task. They do not come from Prometheus. Histogram-derived percentiles are approximate, and a
            benchmark should not depend on a scrape.
          </p>
          <h3>Repetitions</h3>
          <p className="small">
            Re-running all four workloads at 25 repetitions showed that heavy-tailed needs them: its point estimates
            moved 8.4% between 5 and 25 repetitions, against under 1% for the others. The other workloads would have
            been fine with five. Confidence-interval coverage ranked the workloads backwards and was abandoned as the
            diagnostic.
          </p>
          <h3>Environment</h3>
          <p className="small">
            macOS 26.6 on an Apple M5 (10 cores), Redis 8.10.1 on localhost, Go 1.27. Main matrix: 4 worker
            processes × 4 slots against a capacity of about 319 tasks/s. Failover runs require an idle machine.
            Trials hit by the host sleeping are detected and dropped, not averaged.
          </p>
        </div>
      </div>

      <h3 style={{ marginTop: 10 }}>Every experiment, counted from the data</h3>
      <Loaded load={index} what="the experiment inventory">
        {(ix) => (
          <div className="table-scroll">
            <Table
              caption="Experiment inventory"
              head={["experiment", "run directory", "experiments", "tasks"]}
              numeric={[2, 3]}
              rows={[
                ...ix.inventory.rows.map((r) => [r.name, <span className="mono tiny">{r.runs}</span>, fmtInt(r.experiments), fmtInt(r.tasks)]),
                [<strong>Total</strong>, "", <strong>{fmtInt(ix.inventory.total_experiments)}</strong>, <strong>{fmtInt(ix.inventory.total_tasks)}</strong>],
              ]}
            />
          </div>
        )}
      </Loaded>
      <p className="tiny muted">
        Tasks are counted as planned per experiment. The store-failure CSV has no planned-count column, so its 2,000
        per trial comes from the harness default, which every clean trial's submission count confirms. The scheduler
        ceiling measurements drive the critical path with no workers and are not counted as experiments.
      </p>
    </Block>
  );
}
