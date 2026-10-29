import { Block } from "./ResearchPage";
import { useIndex } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Note } from "../components/Controls";
import { blob } from "../lib/vocab";

export function Limitations() {
  const index = useIndex();
  return (
    <Block id="limitations" title="Limitations and open work" refs="report §5.4, §6.1">
      <Loaded load={index} what="the list of pending experiments">
        {(ix) => (
          <>
            {ix.pending.map((p) => (
              <Note key={p.id} kind="pending" title="Pending · no results published">
                <p>
                  <strong>{p.title}.</strong> {p.reason}
                </p>
                <p className="small muted">
                  Harness: <a href={blob(`${p.harness}/main.go`)}>{p.harness}</a> · to run:{" "}
                  {p.commands.map((c, i) => (
                    <span key={c}>
                      {i > 0 && " then "}
                      <code>{c}</code>
                    </span>
                  ))}
                </p>
              </Note>
            ))}
          </>
        )}
      </Loaded>
      <ol>
        <li>
          <strong>One machine.</strong> Every process in every experiment shared one laptop. Failover outages are a
          lower bound for a multi-node deployment, and timing results do not port to other hardware. Only the
          workloads are reproducible byte for byte.
        </li>
        <li>
          <strong>One scheduler dispatches at a time.</strong> Replicas buy availability, not throughput. Scheduling
          capacity is bounded by one process, about 11,400 tasks/s here, and that bounds the scale at which these
          results apply.
        </li>
        <li>
          <strong>Redis Sentinel, not Redis Cluster.</strong> The Lua scripts declare all their keys but have not been
          validated against Cluster's key-slot constraints.
        </li>
        <li>
          <strong>At-least-once only.</strong> A task body can run more than once; only completion recording is
          idempotent.
        </li>
        <li>
          <strong>Failover measured beside the load, not under it.</strong> The outage is the time to resume
          dispatching, not the time to drain the backlog it created.
        </li>
        <li>
          <strong>Simulated execution by default.</strong> The real-work comparison covers three of four workloads at
          one load on one core count, and it showed that the simulation matters for EDF.
        </li>
        <li>
          <strong>One external baseline on one axis.</strong> Asynq on throughput only. No Celery, Sidekiq or
          Temporal, and no comparison of features.
        </li>
      </ol>
      <h3>Named next steps</h3>
      <p className="small">
        Estimating cost from observed rather than declared durations, which §5.3 predicts would recover much of
        EDF's lost advantage. Ageing or lottery scheduling as a starvation-free alternative to strict priority. A
        release notification so a graceful handover does not wait out a standby poll. A tenant-sharded scheduler to
        lift the single-writer bound.
      </p>
    </Block>
  );
}
