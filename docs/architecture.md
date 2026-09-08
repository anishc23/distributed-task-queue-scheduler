# Architecture

## Purpose

The system exists to answer one question empirically:

> How does the choice of scheduling policy affect latency, throughput, deadline
> compliance, fairness and starvation in a distributed task queue under
> different workload distributions?

Everything below is shaped by that goal. The scheduler is a real, central
decision point rather than an emergent property of how workers happen to
consume, because a queue where workers pull in arrival order can only ever
demonstrate FIFO.

## Components

| Component | Package | Role |
| --- | --- | --- |
| Producer | `cmd/producer`, `internal/workload` | Generates a reproducible finite workload and appends it to the ingress stream. Knows nothing about scheduling. |
| Scheduler | `cmd/scheduler`, `internal/scheduler` | Ingests, ranks with the configured policy, and atomically dispatches to workers. Single writer. |
| Policies | `internal/scheduler/{fifo,priority,edf,wfq}` | Pluggable ranking algorithms behind one interface. |
| Worker | `cmd/worker`, `internal/worker` | Consumes the execution stream, simulates work, records terminal results idempotently. |
| Recovery | `internal/recovery` | Reclaims deliveries that exceeded the visibility timeout; retries or dead-letters them. Runs inside the scheduler process. |
| Broker | `internal/broker` | Every Redis interaction, including the Lua scripts that make hand-offs atomic. |
| Metrics | `internal/metrics` | Prometheus instrumentation for the scheduler and workers. |
| Benchmark | `cmd/benchmark`, `internal/bench` | Runs the experiment matrix and writes CSV output. |
| Inspection | `cmd/tqctl` | Live queue state and dead-letter inspection. |

## Data flow

```mermaid
flowchart TD
    P[Producer<br/>reproducible workload] -->|XADD| ING[(Redis stream<br/>tq:stream:ingress)]

    ING -->|XREADGROUP<br/>group: schedulers| SI[Scheduler: ingest loop]
    SI -->|policy.Rank| RANK{{Scheduling policy<br/>fifo / priority / edf / wfq}}
    RANK -->|admit.lua<br/>ZADD + HSET + XACK| PEND[(Pending state<br/>tq:pending ZSET<br/>tq:payloads HASH)]

    PEND -->|dispatch.lua<br/>ZPOPMIN + XADD, atomic| EXEC[(Redis stream<br/>tq:stream:exec)]
    SD[Scheduler: dispatch loop] --> PEND

    EXEC -->|XREADGROUP<br/>group: workers| W1[Worker slot]
    EXEC --> W2[Worker slot]
    EXEC --> W3[Worker slot]

    W1 -->|complete.lua<br/>HSETNX + XADD + XACK| RES[(Redis stream<br/>tq:stream:results)]
    W2 --> RES
    W3 --> RES

    EXEC -.->|XAUTOCLAIM<br/>idle > visibility timeout| REC[Recovery loop]
    REC -->|retry.lua: retry| EXEC
    REC -->|retry.lua: budget exhausted| DLQ[(Redis stream<br/>tq:stream:dead)]

    RES --> B[Benchmark harness<br/>CSV + statistics]
    SI -.-> M[/metrics :9101/]
    W1 -.-> MW[/metrics :9102/]
```

ASCII equivalent:

```text
  Producer
     |  XADD
     v
  ingress stream ---- XREADGROUP (group: schedulers) ----> Scheduler ingest
                                                              |
                                                       policy.Rank(task)
                                                              |  admit.lua (atomic)
                                                              v
                                                    pending ZSET + payload HASH
                                                              |
                                             dispatch.lua: ZPOPMIN + XADD (atomic)
                                                              v
  exec stream ---- XREADGROUP (group: workers) ----> Worker slots (simulate work)
        ^                                                     |
        |                                        complete.lua: HSETNX + XADD + XACK
        |  XAUTOCLAIM after visibility timeout                v
        +---------------- Recovery loop                results stream
                              |
                              +-- budget exhausted --> dead-letter stream
```

## Why the scheduler is a real decision point

Workers never see a task the scheduler has not chosen. Three properties enforce
that:

1. Producers write only to the ingress stream. Nothing consumes it except the
   scheduler.
2. The pending set is a Redis sorted set keyed by the policy's ordering score.
   Dispatch is `ZPOPMIN`, so the policy decides what leaves the queue.
3. `scheduler.max_in_flight` bounds how many tasks are dispatched but not yet
   terminal. Without this bound the execution stream would fill up and become
   the real queue, degrading every policy to arrival order. Setting it close to
   the total number of worker slots keeps the pending set deep and the decision
   meaningful.

## The scheduling abstraction

```go
type Policy interface {
    Name() string
    Rank(t domain.Task) Key       // ordering key; may mutate policy state
    Notify(k Key)                 // a ranked task was actually dispatched
    State() map[string]string     // state that must survive a restart
    Restore(state map[string]string) error
}

type Key struct {
    Score  float64 // lower dispatches first
    Member string  // "<zero-padded submit millis>|<task id>", the tie-break
}
```

A policy never removes tasks itself. The surrounding machinery always dispatches
the smallest `Key` first, ordering by `Score` and then lexicographically by
`Member`.

This is the design's key simplification: **Redis sorted sets order equal-score
members lexicographically by byte value, which is exactly Go string comparison.**
So the in-memory `policy.Queue` used by unit tests and the Redis `ZPOPMIN` used
in production cannot disagree. The ordering asserted by `internal/scheduler/*/…_test.go`
is the ordering the deployed system produces, and
`TestRedisDispatchMatchesPolicyOrdering` checks that claim against a real Redis.

Selecting a policy is a configuration change only (`scheduler.policy`, or
`--policy`). Producers and workers contain no policy-specific code.

## Persistent state, and why each piece is persisted

All keys are prefixed by `streams.namespace`.

| Key | Type | Contents | Why it must survive a restart |
| --- | --- | --- | --- |
| `<ns>:pending` | ZSET | member → policy score | Holds every admitted, undispatched task. Losing it loses accepted work and would force re-ranking. |
| `<ns>:pending_age` | ZSET | member → submission unix millis | Lets the oldest pending wait be found in O(1) for the starvation gauge, which the score-ordered pending set cannot answer. |
| `<ns>:payloads` | HASH | task id → task JSON | The authoritative copy of a task, including its current retry count. Deleted when the task becomes terminal. |
| `<ns>:done` | HASH | task id → completion unix millis | The idempotency guard. Also stops a reclaimed delivery from retrying an already terminal task. |
| `<ns>:sched_state` | HASH | policy state | WFQ virtual time and per-tenant virtual finish tags. Without it a restart resets every tenant's virtual clock to zero and briefly grants service to whichever tenant was furthest ahead. |
| `<ns>:inflight` | STRING | counter | Dispatched-but-not-terminal count, used to enforce `max_in_flight` across restarts. |
| `<ns>:stats` | HASH | counters | Admitted, dispatched, completed, retried, dead-lettered, deadline misses, duplicate completions. |
| `<ns>:tenant_service`, `<ns>:tenant_count` | HASH | tenant → millis / count | Completed service per tenant: the `x` vector of Jain's fairness index. |
| `<ns>:stream:{ingress,exec,results,dead}` | STREAM | messages | The four streams, with consumer groups on ingress and exec. |

Policy state is flushed every `scheduler.state_flush_interval` and once more on
graceful shutdown. Between flushes, a crash can lose at most one interval of
virtual-clock advance; the pending set itself is always durable because it is
written synchronously on admission.

## Atomicity

Four Lua scripts carry the correctness of the system. Each runs atomically
inside Redis.

**`admitScript`** — ingress → pending.
Rejects a task that is already pending, in flight or completed, then writes the
payload, adds to both pending sorted sets and bumps the admitted counter. This
is what makes a redelivered ingress entry harmless after a scheduler crash.

**`dispatchScript`** — selection → delivery.
`ZPOPMIN` and `XADD` happen in the same atomic unit. A task therefore cannot be
lost in the gap between "the scheduler chose it" and "a worker can see it":
either both the pop and the publish happened, or neither did. It also enforces
`max_in_flight` inside the script, so concurrent dispatch attempts cannot
overshoot the cap.

**`completeScript`** — terminal success.
`HSETNX` on the `done` hash is the single source of truth for idempotency. On a
first completion it deletes the payload, writes the result record, updates
counters and tenant service, and decrements the in-flight counter. On a
duplicate it increments a duplicate counter and writes nothing. In both cases it
`XACK`s the delivery **in the same script**, so an acknowledgement can never be
lost after a result was recorded.

**`retryScript`** — failure → retry or dead letter.
Refuses to act on an already terminal task, then either republishes the task
with an incremented retry count and a new attempt ID, or writes a dead-letter
record with failure metadata and marks the task terminal. The old delivery is
`XACK`ed inside the same script, so a crash can neither duplicate the retry nor
strand the entry in the pending entries list.

Deployment assumption: a single, non-clustered Redis 7+ instance. Every script
declares all the keys it touches and never derives key names from data, but the
design has not been validated against Redis Cluster key-slot constraints.

## Concurrency model

- **Scheduler**: one process, three goroutines — ingest, dispatch, observe. The
  policy is guarded by a mutex because ingest calls `Rank` and dispatch calls
  `Notify`. It is a single-writer component by design; the Kubernetes Deployment
  pins `replicas: 1` with a `Recreate` strategy.
- **Worker**: `worker.concurrency` goroutines, each its own consumer in the
  worker group, each reading one entry at a time. Reading one at a time is
  deliberate: buffering inside a worker would move queueing out of the scheduler.
- **Recovery**: one goroutine on a ticker inside the scheduler process.
- **Benchmark**: runs a scheduler, a recovery loop and N workers in-process per
  experiment against an isolated namespace.

No component busy-loops. The dispatch loop backs off by `scheduler.idle_sleep`
when there is nothing to dispatch, ingest blocks in `XREADGROUP`, workers block
in `XREADGROUP`, and recovery runs on a ticker.

## Graceful shutdown

On SIGINT or SIGTERM:

- The **scheduler** stops consuming ingress, lets the in-flight dispatch pass
  finish, flushes policy state, and shuts the metrics server down cleanly so the
  final scrape is not truncated.
- **Workers** stop reading new entries immediately. Tasks already executing
  continue on a detached context bounded by `worker.shutdown_grace` so they can
  finish and acknowledge. Anything unfinished when the grace period expires is
  left unacknowledged, which is safe: the recovery loop redelivers it after the
  visibility timeout. Kubernetes `terminationGracePeriodSeconds` is set above
  `shutdown_grace` so the pod is not killed mid-task.

## Simulated execution

Workers sleep for the task's declared duration instead of doing application
work. This makes task sizes exactly controllable, which is what allows a
heavy-tailed distribution to be specified rather than hoped for, and removes
application variance from the comparison between policies. The cost is that
utilisation numbers reflect wall-clock occupancy of a slot, not CPU work, and
that WFQ knows each task's true cost in advance — see the caveats in
[experiments.md](experiments.md).

## Health and readiness

Both the scheduler and the workers serve `/healthz` and `/readyz` from their
metrics endpoint, and the split matters:

- `/healthz` answers as soon as the process starts, **before** the broker is
  dialled. It means "this process is alive".
- `/readyz` returns 503 until the component has actually started consuming, and
  200 afterwards. It means "this process can do work".

Starting the HTTP server after connecting to Redis would be a mistake, and was
one during development: a Redis that takes a few seconds to become available
during a rollout made the liveness probe fail, Kubernetes killed the container
mid-wait, and an ordinary startup looked like a crash loop with a rising restart
count. A slow dependency must fail readiness, never liveness.

For the same reason the processes wait for Redis rather than exiting when it is
absent: `--redis-wait` (60s by default) retries with a capped backoff and logs
once when it starts waiting and once when the broker appears. Setting it to `0`
restores fail-fast behaviour. `tqctl` deliberately keeps failing immediately,
because it is an interactive command rather than a supervised service.
