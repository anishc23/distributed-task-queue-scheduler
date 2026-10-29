import { useIndex } from "../lib/data";
import { fmtInt } from "../lib/format";
import { REPO, blob } from "../lib/vocab";

export function Overview() {
  const index = useIndex();
  const inv = index.state === "ready" ? index.value.inventory : null;
  const claims = index.state === "ready" ? index.value.claims : null;
  const reproduced = claims ? claims.filter((c) => c.ok).length : null;

  return (
    <section className="block" id="overview" aria-labelledby="overview-h">
      <div className="kicker">Go · Redis Streams · experimental study</div>
      <h1 id="overview-h">A task queue whose scheduling policy is an experimental variable</h1>
      <p className="lede" style={{ marginTop: 12 }}>
        Most task queues hard-code one queue discipline, almost always arrival order. This one makes the discipline a
        pluggable component — FIFO, strict priority, earliest deadline first, weighted fair queuing — behind a single
        fenced scheduler, and then measures when that choice matters, what centralising the decision costs, and what
        happens when the scheduler, or the store underneath it, fails.
      </p>

      <dl className="facts">
        <div>
          <dt>Implementation</dt>
          <dd>Go, Redis Streams, Lua</dd>
        </div>
        <div>
          <dt>Policies</dt>
          <dd>FIFO · priority · EDF · WFQ</dd>
        </div>
        <div>
          <dt>Experiments</dt>
          <dd className="num">{inv ? fmtInt(inv.total_experiments) : "…"}</dd>
        </div>
        <div>
          <dt>Tasks executed</dt>
          <dd className="num">{inv ? `${(inv.total_tasks / 1e6).toFixed(2)} M` : "…"}</dd>
        </div>
      </dl>

      <h2 style={{ fontSize: "1.0625rem", marginTop: 8 }}>What the experiments found</h2>
      <ol className="findings">
        <li>
          <div>
            <p>
              <strong>Policy matters only in a narrow band around capacity.</strong> The spread between the best and
              worst policy's p99 latency is 0.1% at half capacity, 93% at capacity and 1.1% at three times capacity. An
              evaluation run at one arbitrary load can conclude that policy is irrelevant.
            </p>
            <p className="evidence muted">
              <a href="#load">Load sweep</a> · 336 experiments
            </p>
          </div>
        </li>
        <li>
          <div>
            <p>
              <strong>Centralising the decision is cheap; using it is not, and repays it.</strong> A central scheduler
              costs about 1.5% of throughput against none at all. Acting on it costs 6–10% and returns 118× fewer
              deadline misses (EDF) or 35× lower latency for small tenants (WFQ). Against Asynq, which has no central
              scheduler, this design is about 30% slower on tasks under 3 ms and level from 5 ms.
            </p>
            <p className="evidence muted">
              <a href="#cost">Cost of centralisation</a> · <a href="#isolation">tenant isolation</a>
            </p>
          </div>
        </li>
        <li>
          <div>
            <p>
              <strong>A lease alone does not keep one writer.</strong> A leader frozen past its lease and then resumed
              reached Redis with a write before its own lease logic noticed, in 11 of 15 trials; the epoch check inside
              the write refused it. With that check removed, a deterministic test dispatched 172 of 400 tasks under a
              dead term. Cut off from a Redis that stays up instead, a leader kept its role through any partition
              shorter than its lease and lost it beyond. Coming back, it was refused by the fence in 11 of 12 trials,
              and across seven partition lengths the outage grew only until the lease ran out, then stayed inside a
              crash's window.
            </p>
            <p className="evidence muted">
              <a href="#fencing">Gray-failure experiment</a> · real <code>SIGSTOP</code>/<code>SIGCONT</code> ·{" "}
              <a href="#partition">partition experiment</a> · 104 trials
            </p>
          </div>
        </li>
        <li>
          <div>
            <p>
              <strong>The fencing token is only as durable as the store that holds it.</strong> A Redis Sentinel
              failover to a lagging replica issued one token to two leaders in 10 of 10 trials. The obvious fix — wait
              for the epoch to replicate — failed 10 of 10. Refusing unreplicable writes on the master
              (<code>min-replicas-to-write</code>) held in 10 of 10.
            </p>
            <p className="evidence muted">
              <a href="#store">Store-failure experiment</a> · negative result kept
            </p>
          </div>
        </li>
        <li>
          <div>
            <p>
              <strong>Simulating work with sleeps flatters deadline scheduling.</strong> Under real CPU work EDF's miss
              rate rose 8.4× and strict priority overtook it; WFQ's isolation was unaffected, because its cost errors
              are common-mode and cancel.
            </p>
            <p className="evidence muted">
              <a href="#realwork">Sleep against real CPU</a> · 25 repetitions
            </p>
          </div>
        </li>
      </ol>

      <div className="note" style={{ marginTop: 22 }}>
        <div className="note-title">Scope, stated plainly</div>
        <p>
          This is a scheduling study on a Redis-backed queue more than a distributed system. There is one Redis, one
          scheduler dispatching at a time, and every process in every experiment ran on one laptop. Consensus,
          sharding and multi-node networks are absent or delegated to Redis. The one coordination problem it does take
          on — keeping a single writer through failures — is measured rather than asserted.
        </p>
        <p className="small muted">
          {reproduced !== null && claims ? (
            <>
              {reproduced} of {claims.length} numbers quoted in the report are recomputed from the raw CSVs every time
              this site's data is exported; the export refuses to run if any disagrees. Building that check found four
              errors in the report, now corrected — see <a href="#corrections">Corrections</a>.
            </>
          ) : (
            "Loading the reproduction check…"
          )}
        </p>
      </div>

      <p className="small muted">
        <a href={`${import.meta.env.BASE_URL}docs/report.pdf`}>Full report (PDF)</a> ·{" "}
        <a href={blob("docs/report.md")}>report source</a> · <a href={blob("docs/architecture.md")}>architecture notes</a>{" "}
        · <a href={REPO}>repository</a>
      </p>
    </section>
  );
}
