import { Block } from "./ResearchPage";
import { blob } from "../lib/vocab";

/**
 * Drawn from the code, not from a template: every box is a key in
 * internal/broker (Keys), every arrow a call in internal/broker/broker.go or one
 * of the Lua scripts in internal/broker/scripts.go.
 */
function Diagram() {
  const box = { fill: "var(--panel)", stroke: "var(--rule-strong)" };
  const key = { fill: "var(--paper)", stroke: "var(--rule-strong)" };
  const arrow = { stroke: "var(--ink-2)", strokeWidth: 1.3, fill: "none", markerEnd: "url(#ah)" };
  const T = ({ x, y, children, anchor = "start", size = 11, weight = 400, fill = "var(--ink)" }: {
    x: number; y: number; children: React.ReactNode; anchor?: "start" | "middle" | "end"; size?: number; weight?: number; fill?: string;
  }) => (
    <text x={x} y={y} textAnchor={anchor} fontSize={size} fontWeight={weight} fill={fill}>
      {children}
    </text>
  );
  const Lbl = ({ x, y, children, anchor = "middle" }: { x: number; y: number; children: React.ReactNode; anchor?: "start" | "middle" | "end" }) => (
    <T x={x} y={y} anchor={anchor} size={10} fill="var(--ink-2)">
      {children}
    </T>
  );

  const streams = [
    { y: 40, a: "ingress", b: "stream" },
    { y: 112, a: "pending", b: "ZSET in score order + payload HASH" },
    { y: 184, a: "exec", b: "stream · each entry stamped with epoch" },
    { y: 256, a: "results", b: "stream + done HASH (HSETNX)" },
    { y: 328, a: "dead-letter", b: "stream" },
  ];
  const coord = [
    { y: 352, a: "leader", b: "owner|epoch, with TTL" },
    { y: 394, a: "leader_epoch", b: "INCR once per grant" },
    { y: 436, a: "fence", b: "highest epoch written" },
  ];
  const sched = [
    "policy: fifo | priority |",
    "        edf | wfq",
    "",
    "ingest → rank → dispatch",
    "max_in_flight caps the exec",
    "stream",
    "",
    "recovery loop, every replica:",
    "XAUTOCLAIM stalled work,",
    "retry or dead-letter",
  ];

  return (
    <svg viewBox="0 0 1000 490" role="img" aria-labelledby="arch-title arch-desc">
      <title id="arch-title">Data flow and coordination</title>
      <desc id="arch-desc">
        Producers append to an ingress stream. The leading scheduler reads it, ranks each task with the configured policy
        into a pending sorted set, and dispatches the lowest key to an execution stream in one atomic script that also
        checks the fencing epoch. Workers read the execution stream and record completion atomically to a results
        stream. A recovery loop on every scheduler replica reclaims stalled deliveries and moves exhausted tasks to a
        dead-letter stream. A standby scheduler polls a lease key and takes over when it expires.
      </desc>
      <defs>
        <marker id="ah" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L8,4 L0,8 z" fill="var(--ink-2)" />
        </marker>
      </defs>

      {/* clients */}
      <rect x={20} y={36} width={170} height={46} {...box} />
      <T x={32} y={56} weight={600}>Producer</T>
      <T x={32} y={72} size={10} fill="var(--ink-2)">submits tasks</T>

      <rect x={20} y={204} width={170} height={92} {...box} />
      <T x={32} y={224} weight={600}>Workers × N</T>
      <T x={32} y={241} size={10} fill="var(--ink-2)">k execution slots each</T>
      <T x={32} y={256} size={10} fill="var(--ink-2)">at-least-once delivery;</T>
      <T x={32} y={270} size={10} fill="var(--ink-2)">completion recorded</T>
      <T x={32} y={284} size={10} fill="var(--ink-2)">exactly once</T>

      <rect x={20} y={420} width={170} height={44} {...box} strokeDasharray="3 3" />
      <T x={32} y={439} weight={600}>tqctl</T>
      <T x={32} y={454} size={10} fill="var(--ink-2)">status · dlq · reset</T>

      {/* redis */}
      <rect x={250} y={12} width={440} height={470} fill="none" stroke="var(--ink-3)" />
      <T x={264} y={30} size={10} weight={600} fill="var(--ink-2)">REDIS · one instance (Sentinel optional)</T>
      {streams.map((k) => (
        <g key={k.a}>
          <rect x={268} y={k.y} width={256} height={38} {...key} />
          <T x={278} y={k.y + 16} weight={600}>{k.a}</T>
          <T x={278} y={k.y + 30} size={10} fill="var(--ink-2)">{k.b}</T>
        </g>
      ))}
      <T x={540} y={326} size={10} weight={600} fill="var(--ink-2)">COORDINATION</T>
      <T x={540} y={340} size={9.5} fill="var(--ink-3)">lease: SET NX PX, renewed</T>
      {coord.map((k) => (
        <g key={k.a}>
          <rect x={540} y={k.y} width={138} height={36} {...key} />
          <T x={548} y={k.y + 15} weight={600} size={10.5}>{k.a}</T>
          <T x={548} y={k.y + 29} size={9.5} fill="var(--ink-2)">{k.b}</T>
        </g>
      ))}

      {/* schedulers */}
      <rect x={770} y={36} width={214} height={200} {...box} strokeWidth={1.6} stroke="var(--ink)" />
      <T x={782} y={56} weight={600}>Scheduler · leader</T>
      {sched.map((line, i) => (
        <T key={i} x={782} y={76 + i * 13} size={10} fill="var(--ink-2)">
          {line}
        </T>
      ))}
      <T x={782} y={226} size={10} fill="var(--ink-3)">every write carries its epoch</T>

      <rect x={770} y={262} width={214} height={58} {...box} />
      <T x={782} y={282} weight={600}>Scheduler · standby</T>
      <T x={782} y={298} size={10} fill="var(--ink-2)">polls the lease every 1 s</T>
      <T x={782} y={311} size={10} fill="var(--ink-3)">also runs recovery</T>

      <rect x={770} y={420} width={214} height={44} {...box} strokeDasharray="3 3" />
      <T x={782} y={439} weight={600}>Prometheus</T>
      <T x={782} y={454} size={10} fill="var(--ink-2)">scrapes every /metrics</T>

      {/* flows */}
      <path d="M190,59 H266" {...arrow} />
      <Lbl x={228} y={52}>XADD</Lbl>

      <path d="M526,59 H768" {...arrow} />
      <Lbl x={646} y={52}>XREADGROUP</Lbl>

      <path d="M768,104 L528,131" {...arrow} />
      <Lbl x={700} y={100}>admit · fenced Lua</Lbl>

      <path d="M768,150 L528,203" {...arrow} />
      <Lbl x={668} y={206}>dispatch: ZPOPMIN + XADD</Lbl>
      <Lbl x={668} y={218}>in one fenced Lua script</Lbl>

      <path d="M268,203 C230,203 220,226 192,232" {...arrow} />
      <Lbl x={228} y={196}>XREADGROUP</Lbl>

      <path d="M192,275 H266" {...arrow} />
      <Lbl x={228} y={311}>HSETNX+XADD</Lbl>
      <Lbl x={228} y={323}>+XACK, atomic</Lbl>

      <path d="M768,196 H736 V296 H560 V347 H528" {...arrow} strokeDasharray="4 3" />
      <Lbl x={646} y={289}>retry budget exhausted</Lbl>

      <path d="M768,226 H752 V364 H680" {...arrow} />
      <path d="M768,300 H760 V380 H680" {...arrow} strokeDasharray="4 3" />

      <path d="M190,442 H248" {...arrow} strokeDasharray="3 3" />
      <Lbl x={219} y={435}>read</Lbl>
    </svg>
  );
}

export function Architecture() {
  return (
    <Block id="architecture" title="Architecture" refs="report §2 · internal/broker · internal/lease">
      <p>
        Producers write only to an ingress stream. Workers see only what the scheduler has already ranked and
        dispatched. Between them, one scheduler process turns each task into a sort key with the configured policy,
        holds the backlog in a Redis sorted set, and dispatches the smallest key. Because{" "}
        <code>max_in_flight</code> caps dispatched-but-unfinished work, the backlog stays where the policy controls it.
      </p>
      <div className="diagram fig" style={{ padding: "14px 14px 6px" }}>
        <Diagram />
      </div>

      <div className="two-col">
        <div>
          <h3>The scheduling contract</h3>
          <p className="small">
            A policy maps a task to <code>{"{Score float64, Member string}"}</code> and nothing else. Redis orders equal
            scores by member bytes, which is exactly Go's string order, so the in-memory queue in the unit tests and{" "}
            <code>ZPOPMIN</code> in production cannot disagree. An integration test checks this against a live Redis.
          </p>
          <p className="small">
            Strict priority packs <code>(255 − priority)·10¹³ + submit_ms</code> into one float64. EDF orders by absolute
            deadline. WFQ is self-clocked fair queuing over tenants, using the task's declared cost. Section 5.3 of the
            report shows that this assumption matters for EDF but not for WFQ.
          </p>
        </div>
        <div>
          <h3>The two invariants</h3>
          <p className="small">
            <strong>At-least-once delivery, completion recorded once.</strong> Dispatch is <code>ZPOPMIN</code> and{" "}
            <code>XADD</code> in one script. Completion is an <code>HSETNX</code> in the same script that writes the
            result and acknowledges the delivery.
          </p>
          <p className="small">
            <strong>One scheduler dispatches at a time.</strong> Leadership is a lease. Each grant carries an epoch
            from a counter. Every admission and dispatch script begins with this guard, so the epoch is checked inside the
            same atomic unit as the write it protects (comments added):
          </p>
          <pre className="small" tabIndex={0}>
            <code>{`local function fenced(fenceKey, epoch)
  epoch = tonumber(epoch)
  if not epoch or epoch <= 0 then return false end  -- election off
  local cur = tonumber(redis.call('GET', fenceKey) or '0')
  if epoch < cur then return true end               -- superseded: refuse
  if epoch > cur then redis.call('SET', fenceKey, epoch) end
  return false
end`}</code>
          </pre>
        </div>
      </div>
      <p className="small muted">
        Code: <a href={blob("internal/broker/scripts.go")}>internal/broker/scripts.go</a> (the Lua) ·{" "}
        <a href={blob("internal/lease/lease.go")}>internal/lease/lease.go</a> (lease and epoch) ·{" "}
        <a href={blob("docs/architecture.md")}>docs/architecture.md</a> (full design notes)
      </p>
    </Block>
  );
}
