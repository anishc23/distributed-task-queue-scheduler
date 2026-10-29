import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Note, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame } from "../lib/plot";
import { median } from "../lib/stats";
import { fmtMs } from "../lib/format";
import { blob } from "../lib/vocab";
import type { Dataset, FailoverRow } from "../lib/schema";

export function Fencing() {
  const ds = useDataset("failover");
  return (
    <Block id="fencing" title="The failure the fence is for" refs="report §4.8">
      <p>
        Killing a leader proves little about fencing: a dead process writes nothing, so a lease alone would have
        handled every arm above. Fencing tokens exist for a leader that is <em>still running</em>. It was stopped
        long enough for its lease to expire, a standby took over, and then it resumed believing it still leads. A GC
        pause, a suspended container or a swapped host all produce this. It cannot be cancelled, because it was not
        running to observe the cancellation. Nor can it check first, because a check and the write after it are
        separated by a window in which the answer can change. The check has to be in the write.
      </p>

      <Schematic />

      <Loaded load={ds} what="the pause trials">
        {(d) => <PauseOutcomes d={d} />}
      </Loaded>

      <h3 style={{ marginTop: 8 }}>How narrow the margin is</h3>
      <p className="small">
        The scheduler log from one trial. The resume, the lease loss and the fence refusal all land within the same
        millisecond.
      </p>
      <pre className="small">
        <code>{`22:53:55.978  resumed the superseded leader   owner=sched-1 successor=sched-0
22:53:55.978  lost the scheduler lease        owner=sched-1 epoch=1
22:53:55.978  fenced out by a newer epoch, standing down   operation=dispatch`}</code>
      </pre>
      <p className="tiny muted">Recorded log excerpt, reproduced from report §4.8. The exported CSV does not contain logs.</p>

      <Note kind="negative" title="The counterfactual: remove the fence and watch it fail">
        <p className="small">
          The integration test{" "}
          <a href={blob("internal/scheduler/grayfailure_integration_test.go")}>
            <code>TestAResumedLeaderCannotDispatchAfterBeingSuperseded</code>
          </a>{" "}
          drives an engine into the same state deterministically. It gives the engine a lease whose renewal cannot
          fire inside the test window, so the fence is the only thing that can stop it. With the epoch check removed
          from the Lua guard, the test fails at once: <strong>172 of 400 tasks were dispatched under a dead term</strong>,
          concurrently with the legitimate leader, and every one is visible in the dispatch log because each entry is
          stamped with its epoch. A test never observed to fail is not evidence about the mechanism it guards.
        </p>
        <p className="tiny muted">
          Source: report §4.8. This is a mutation-test result, not part of the exported data. Reproduce it by deleting
          the <code>if epoch &lt; cur then return true end</code> line of <code>fenceGuard</code> in{" "}
          <code>internal/broker/scripts.go</code> and running{" "}
          <code>TQ_TEST_REDIS_ADDR=localhost:6379 go test -tags integration ./internal/scheduler/ -run Superseded</code>.
        </p>
      </Note>

      <Note kind="caveat" title="What this does not cover">
        <p className="small">
          A stopped process resumes with its connection to Redis intact. A scheduler partitioned from a healthy Redis
          loses its path to the store as well as its sense of time. That is a different experiment. Its harness is
          written but its results are not yet published; see <a href="#limitations">open work</a>.
        </p>
      </Note>
    </Block>
  );
}

/** Explanatory only: shapes, not measurements. Labelled as such in the figure. */
function Schematic() {
  const lane = (y: number, label: string) => (
    <g>
      <text x={8} y={y + 4} fontSize={11} fill="var(--ink)" fontWeight={600}>
        {label}
      </text>
      <line x1={110} x2={872} y1={y} y2={y} stroke="var(--rule-strong)" />
    </g>
  );
  const ev = (x: number, y: number, text: string, anchor: "start" | "middle" | "end" = "middle", dy = -10) => (
    <g>
      <line x1={x} x2={x} y1={y - 5} y2={y + 5} stroke="var(--ink)" strokeWidth={1.4} />
      <text x={x} y={y + dy} fontSize={10} textAnchor={anchor} fill="var(--ink-2)">
        {text}
      </text>
    </g>
  );
  return (
    <figure className="fig" aria-labelledby="schematic-title">
      <div className="fig-head">
        <h3 id="schematic-title">The pause scenario, step by step</h3>
        <span className="tag unknown">schematic · not to scale · not measured</span>
      </div>
      <div className="fig-body diagram">
        <svg viewBox="0 0 880 210" role="img" aria-labelledby="schematic-title schematic-desc">
          <desc id="schematic-desc">
            Scheduler one leads under epoch 1 and renews its lease. It is frozen with SIGSTOP. Its lease expires; the
            standby, scheduler zero, acquires the lease as epoch 2 and its first write raises the fence in Redis to 2.
            Scheduler one is resumed with SIGCONT and attempts a dispatch with epoch 1, which the fence check refuses;
            it stands down.
          </desc>
          {lane(48, "sched-1")}
          {lane(110, "sched-0")}
          {lane(172, "Redis fence")}
          <rect x={110} y={42} width={150} height={12} fill="var(--ink)" fillOpacity={0.75} />
          <text x={116} y={36} fontSize={10} fill="var(--ink-2)">leading · epoch 1 · renewing</text>
          <rect x={260} y={44} width={440} height={8} fill="none" stroke="var(--ink-3)" strokeDasharray="3 3" />
          <text x={560} y={36} fontSize={10} textAnchor="middle" fill="var(--ink-2)">frozen (SIGSTOP) for 2 × TTL</text>
          {ev(260, 48, "SIGSTOP", "middle", 24)}
          {ev(700, 48, "SIGCONT", "middle", 24)}
          <line x1={716} x2={716} y1={56} y2={164} stroke="var(--bad)" strokeWidth={1.2} markerEnd="url(#fa)" />
          <text x={724} y={90} fontSize={10} fill="var(--bad)">dispatch with epoch 1</text>
          <text x={724} y={102} fontSize={10} fill="var(--bad)">→ TQFENCED, refused</text>
          <text x={724} y={36} fontSize={10} fill="var(--ink-2)">stands down</text>
          <rect x={110} y={106} width={330} height={8} fill="none" stroke="var(--ink-3)" />
          <text x={116} y={100} fontSize={10} fill="var(--ink-2)">standby · polling the lease</text>
          {ev(380, 110, "lease expires (TTL after last renewal)", "end", 24)}
          <rect x={440} y={104} width={432} height={12} fill="var(--ink)" fillOpacity={0.75} />
          <text x={446} y={98} fontSize={10} fill="var(--ink-2)">acquires the lease as epoch 2, dispatches</text>
          <line x1={452} x2={452} y1={118} y2={164} stroke="var(--ink-2)" strokeWidth={1.2} markerEnd="url(#fa)" />
          <text x={116} y={190} fontSize={10} fill="var(--ink-2)">fence = 1</text>
          <text x={460} y={190} fontSize={10} fill="var(--ink-2)">first write of epoch 2 raises fence = 2; epoch 1 is now refused</text>
          <defs>
            <marker id="fa" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L8,4 L0,8 z" fill="var(--ink-2)" />
            </marker>
          </defs>
        </svg>
      </div>
      <figcaption>
        <p>
          This explains the experiment's structure; it is not a recording of one trial. In the experiment the freeze
          lasts twice the 5 s lease, measured at about 10.2 s. The harness checks that the victim really is stopped,
          read from the OS process state, and that a different process holds the lease before it resumes. After the
          resume it checks that exactly one process still claims leadership.
        </p>
      </figcaption>
    </figure>
  );
}

function PauseOutcomes({ d }: { d: Dataset<FailoverRow> }) {
  const trials = d.rows.filter((r) => r.run === "failover" && r.arm === "pause").sort((a, b) => a.rep - b.rep);
  const fenced = trials.filter((t) => t.fenced_operations === 1).length;
  const leaseFirst = trials.filter((t) => t.fenced_operations === 0).length;
  const unknown = trials.length - fenced - leaseFirst;
  const cells = trials.map((t, i) => ({
    i,
    rep: t.rep,
    outcome: t.fenced_operations === 1 ? "refused by the fence" : t.fenced_operations === 0 ? "stood down on lease renewal" : "not recorded",
    pause: t.pause_ms,
    leaders: t.leaders_after_resume,
    inversions: t.epoch_inversions,
  }));
  const color = (o: string) => (o === "refused by the fence" ? "var(--ink)" : o === "stood down on lease renewal" ? "var(--ink-3)" : "var(--bad)");
  return (
    <Figure
      id="fig-pause"
      title={`What stopped the resumed leader · ${trials.length} trials`}
      dataset={d}
      caption={
        <>
          <p>
            One square per trial. In <strong>{fenced} of {trials.length}</strong> the resumed leader reached Redis with
            a write before its own lease logic noticed anything, and the epoch check refused it. In {leaseFirst} its
            failed lease renewal made it stand down first, which is the same protection one layer earlier. Both are
            correct. The first group is the measurement that matters: a deployment with a lease and no fence would have
            admitted a second writer in each of those trials.
            {unknown > 0 && ` ${unknown} trial(s) did not record which happened.`}
          </p>
          <p>
            In all {trials.length}: frozen for a median {fmtMs(median(trials.map((t) => t.pause_ms)))} against a 5 s
            lease; leadership sum after resume {trials.every((t) => t.leaders_after_resume === 1) ? "exactly 1 every time" : "not always 1, see table"};
            epoch inversions in the dispatch log {trials.reduce((s, t) => s + t.epoch_inversions, 0)}; every task
            completed.
          </p>
        </>
      }
      table={
        <Table
          caption="Pause trials"
          head={["trial", "outcome", "frozen for", "leaders after resume", "epoch inversions"]}
          numeric={[2, 3, 4]}
          rows={cells.map((c) => [String(c.rep), c.outcome, fmtMs(c.pause), String(c.leaders ?? "—"), String(c.inversions)])}
        />
      }
    >
      <Chart
        label={`${fenced} of ${trials.length} pause trials were refused by the fence; ${leaseFirst} stood down on their own lease renewal first`}
        deps={[]}
        height={110}
        build={(w) =>
          Plot.plot({
            ...frame(Math.min(w, 640), 110),
            marginLeft: 8,
            marginBottom: 30,
            x: { axis: null, domain: cells.map((c) => c.i), padding: 0.18 },
            y: { axis: null, domain: [0, 1] },
            marks: [
              Plot.barY(cells, { x: "i", y: 1, fill: (c) => color(c.outcome) }),
              Plot.text(cells, { x: "i", y: 0, text: (c) => String(c.rep), dy: 12, fill: "var(--ink-3)" }),
              Plot.tip(cells, Plot.pointerX({ x: "i", y: 0.5, title: (c) => `trial ${c.rep}: ${c.outcome}\nfrozen ${fmtMs(c.pause)}` })),
            ],
          })
        }
      />
      <div className="legend" style={{ paddingLeft: 0 }}>
        <span>
          <i className="swatch dot" style={{ background: "var(--ink)", borderRadius: 0 }} /> refused by the fence ({fenced})
        </span>
        <span>
          <i className="swatch dot" style={{ background: "var(--ink-3)", borderRadius: 0 }} /> stood down on lease renewal ({leaseFirst})
        </span>
      </div>
    </Figure>
  );
}
