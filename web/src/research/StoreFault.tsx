import { Block } from "./ResearchPage";
import { useDataset } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Figure } from "../components/Figure";
import { Note, Table } from "../components/Controls";
import { Chart } from "../components/Chart";
import { Plot, frame } from "../lib/plot";
import { fmtInt } from "../lib/format";
import type { Dataset, StorefaultRow } from "../lib/schema";

const ARMS = ["clean", "lagging", "epochwait", "guarded"];
const ARM: Record<string, { fault: string; mitigation: string }> = {
  clean: { fault: "master killed, replicas caught up", mitigation: "none" },
  lagging: { fault: "replication cut, then master killed", mitigation: "none" },
  epochwait: { fault: "replication cut, then master killed", mitigation: "lease waits for a replica to ack each new epoch" },
  guarded: { fault: "replication cut, then master killed", mitigation: "the above, plus min-replicas-to-write on the master" },
};

export function StoreFault() {
  const ds = useDataset("storefault");
  return (
    <Block id="store" title="What the fence rests on" refs="report §4.9 · 40 trials">
      <p>
        The fencing token is a counter in Redis. Its guarantee — once epoch <em>N</em> has written, nothing below{" "}
        <em>N</em> writes again — is exactly as durable as that counter. So the harness builds a real Sentinel
        cluster from local <code>redis-server</code> and <code>redis-sentinel</code> processes: one master, two
        replicas, three sentinels. It points the queue at it and breaks the store underneath a running workload.
      </p>
      <Loaded load={ds} what="the store-failure trials">
        {(d) => <Trials d={d} />}
      </Loaded>
      <div className="two-col">
        <Note kind="negative" title="The obvious fix fails, and why">
          <p className="small">
            Waiting for a replica to acknowledge each new epoch (<code>epochwait</code>) failed as often as no
            mitigation at all. The counter is incremented <em>before</em> the wait, so a master that dies in between
            has already issued it. The wait can report that the write did not survive. It cannot un-issue the token.
            Detecting a lost write is not preventing one.
          </p>
        </Note>
        <Note title="What works, and what it costs">
          <p className="small">
            With <code>min-replicas-to-write</code> the <code>INCR</code> itself fails while no replica can receive
            it (<code>NOREPLICAS</code>), so no token is issued that could be lost. The guarded arm accepted half as
            many submissions. That is not lost throughput: the unguarded arms completed the same number, and also told
            the producer they had accepted work they then destroyed. Silent loss becomes a visible, retryable error.
          </p>
        </Note>
      </div>
      <p className="small muted">
        Three details decide whether this measures anything, and each was found by getting it wrong first. A
        stopped replica is not a partitioned one: the kernel keeps acknowledging TCP, so replication runs through a
        severable proxy (<code>internal/netfault</code>). Redis waits for replicas during a graceful{" "}
        <code>SHUTDOWN</code>, so every kill is <code>SIGKILL</code>. Sentinel repairs the injected fault on its own
        refresh, so the harness verifies replication is still down at the instant the master dies, and aborts
        otherwise.
      </p>
      <Note kind="caveat" title="Scope">
        <p className="small">
          Redis Sentinel with asynchronous replication, which is what the shipped deployment uses, with every
          process on one host. Not a claim about Redis Cluster, AOF with <code>appendfsync always</code>, or a store
          with real consensus. The finding is narrower than "Redis loses data": a fencing token inherits the
          durability of whatever holds it.
        </p>
      </Note>
    </Block>
  );
}

function Trials({ d }: { d: Dataset<StorefaultRow> }) {
  const arms = ARMS.filter((a) => d.rows.some((r) => r.arm === a));
  const cells = arms.flatMap((a) =>
    d.rows
      .filter((r) => r.arm === a)
      .sort((x, y) => x.rep - y.rep)
      .map((r) => ({ ...r, outcome: r.epoch_reused ? "one token, two leaders" : "token unique" })),
  );
  const summary = arms.map((a) => {
    const t = d.rows.filter((r) => r.arm === a);
    return {
      arm: a,
      n: t.length,
      reused: t.filter((r) => r.epoch_reused).length,
      lost: t.reduce((s, r) => s + r.tasks_lost, 0),
      submitted: t.reduce((s, r) => s + r.submitted, 0),
      completed: t.reduce((s, r) => s + r.completed, 0),
    };
  });
  return (
    <Figure
      id="fig-store"
      title="One square per trial: was a fencing token issued to two leaders?"
      dataset={d}
      caption={
        <p>
          Token reuse is observed continuously by the harness and recorded outside Redis, because the fault destroys
          Redis state. Asking the promoted node afterwards under-reports: a new leader has often advanced the counter
          past the gap before it can be read. The number in each square is the acknowledged tasks that trial
          destroyed, meaning work the producer was told had been accepted and which then existed nowhere.
        </p>
      }
      table={
        <Table
          caption="Store failure by arm"
          head={["arm", "fault", "mitigation", "token reused", "tasks destroyed", "accepted", "completed"]}
          numeric={[3, 4, 5, 6]}
          rows={summary.map((s) => [
            <code>{s.arm}</code>,
            ARM[s.arm]?.fault ?? "",
            ARM[s.arm]?.mitigation ?? "",
            `${s.reused} / ${s.n}`,
            fmtInt(s.lost),
            fmtInt(s.submitted),
            fmtInt(s.completed),
          ])}
        />
      }
    >
      <Chart
        label={summary.map((s) => `${s.arm}: ${s.reused} of ${s.n} trials reused a token, ${s.lost} tasks destroyed`).join("; ")}
        deps={[]}
        height={200}
        build={(w) =>
          Plot.plot({
            ...frame(w, 200),
            marginLeft: 90,
            marginRight: 170,
            x: { axis: null, domain: [...new Set(cells.map((c) => c.rep))].sort((a, b) => a - b), padding: 0.12 },
            y: { label: null, domain: arms, padding: 0.18, tickFormat: (a: string) => a },
            marks: [
              Plot.cell(cells, {
                x: "rep",
                y: "arm",
                fill: (c) => (c.epoch_reused ? "var(--bad)" : "var(--ok)"),
                fillOpacity: (c) => (c.epoch_reused ? 0.85 : 0.25),
              }),
              Plot.text(cells, { x: "rep", y: "arm", text: (c) => (c.tasks_lost ? fmtInt(c.tasks_lost) : "0"), fill: (c) => (c.epoch_reused ? "white" : "var(--ink)"), fontSize: 10 }),
              Plot.text(summary, {
                y: "arm",
                frameAnchor: "right",
                dx: 10,
                textAnchor: "start",
                text: (s) => `${s.reused}/${s.n} reused · ${fmtInt(s.lost)} lost`,
                fill: "var(--ink)",
              }),
              Plot.tip(
                cells,
                Plot.pointer({
                  x: "rep",
                  y: "arm",
                  title: (c) =>
                    `${c.arm}, trial ${c.rep}\n${c.outcome}\n${c.tasks_lost} acknowledged tasks destroyed\n${c.submitted} accepted, ${c.completed} completed${c.failover_ms ? `\nSentinel failover ${Math.round(c.failover_ms)} ms` : ""}`,
                }),
              ),
            ],
          })
        }
      />
      <div className="legend" style={{ paddingLeft: 0 }}>
        <span>
          <i className="swatch dot" style={{ background: "var(--bad)", borderRadius: 0 }} /> one token issued to two leaders
        </span>
        <span>
          <i className="swatch dot" style={{ background: "var(--ok)", opacity: 0.35, borderRadius: 0 }} /> token unique
        </span>
      </div>
    </Figure>
  );
}
