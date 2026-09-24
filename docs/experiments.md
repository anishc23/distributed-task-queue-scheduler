# Experiment methodology

## Design

The primary matrix is four schedulers by four workloads, which is **16
experiments**, each repeated `--repetitions` times.

|  | uniform | bursty | heavy_tailed | multi_tenant |
| --- | :---: | :---: | :---: | :---: |
| `fifo` | ✓ | ✓ | ✓ | ✓ |
| `priority` | ✓ | ✓ | ✓ | ✓ |
| `edf` | ✓ | ✓ | ✓ | ✓ |
| `wfq` | ✓ | ✓ | ✓ | ✓ |

Each cell runs in its own Redis key namespace
(`bench-<run id>-<scheduler>-<workload>-r<rep>`) which is reset before the
experiment and deleted afterwards. Results from different cells or different
runs therefore cannot be mixed, and every CSV row repeats the full run identity
as a second line of defence.

## Procedure for one experiment

1. Generate the workload plan deterministically from `(seed, workload config)`.
2. Reset the namespace and create the streams and consumer groups.
3. Start the scheduler, the recovery loop and `--workers` worker processes with
   `--concurrency` slots each, all in-process.
4. Wait for every component to report ready.
5. Replay the plan onto the ingress stream, pacing submissions by each task's
   arrival offset. Record producer lag.
6. Poll until `completed + dead-lettered == planned`, or until the timeout.
7. Cancel every component and wait for clean shutdown.
8. Read the result stream, compute statistics, write CSVs.

The per-experiment timeout defaults to
`3 x (arrival span + total service time / worker slots)`, floored at 30 seconds
and capped at 30 minutes. A timed-out experiment is still written out with
`timed_out=true`; its numbers describe a truncated run and should not be
compared against completed ones.

## Load level

A scheduler can only matter when a queue exists. If arrivals are comfortably
below service capacity, every policy dispatches each task immediately and all
four produce identical numbers.

Service capacity is approximately:

```
capacity (tasks/s) = worker_processes x concurrency / mean_exec_seconds
```

The shipped profiles are sized against that:

| Profile | Slots | Mean exec | Capacity | Arrival rate | Offered load |
| --- | ---: | ---: | ---: | ---: | ---: |
| `experiments/quick.yaml` | 8 | 50 ms | ~160/s | 200/s | ~1.25x |
| `experiments/full.yaml` | 16 | 50 ms | ~320/s | 400/s | ~1.25x |

A sustained overload of roughly 1.25x builds a real backlog without making the
run unbounded. `scheduler.max_in_flight` is set to the total slot count so the
backlog stays in the scheduler's pending set, where the policy controls it,
rather than in the execution stream, where it would not.

If your results show all four schedulers producing the same numbers, the run was
almost certainly under-loaded. Raise `workload.arrival_rate_per_sec`, raise
`exec.max_ms`, or lower the worker count.

## Workloads

All four are finite, seeded and reproducible.

**`uniform`** — the control. Arrivals are evenly spaced at
`arrival_rate_per_sec`; durations are drawn uniformly from `exec.min_ms..max_ms`.
Deliberately deterministic in arrivals so that differences between schedulers
cannot be attributed to arrival noise.

**`bursty`** — Poisson arrivals at `burst.base_rate_per_sec`, with windows of
`burst.burst_duration` at `burst.burst_rate_per_sec` repeating every
`burst.burst_period`. Tests behaviour during transient overload, when the queue
grows faster than it drains.

**`heavy_tailed`** — Poisson arrivals; durations drawn from a Pareto
distribution, `exec_ms = min_ms * U^(-1/alpha)`, clamped at `max_ms`. Most tasks
are short and a small fraction are very long. This is the head-of-line blocking
regime. `alpha` stays above 1 so the distribution has a finite mean, and the
clamp bounds the worst case regardless.

**`multi_tenant`** — Poisson arrivals with tenant shares skewed 90/3/3/2/2
across tenants A–E. Tenant A is the noisy neighbour. This is the fairness and
isolation experiment.

The non-skewed workloads spread tasks evenly across the same five tenants, so
tenant accounting is comparable across all four.

Priorities are drawn from a configurable discrete distribution
(`workload.priority.weights`, default `[40, 25, 20, 10, 5]` for priorities 0–4),
which is intentionally bottom-heavy so that the low-priority population is large
enough for starvation to be measurable.

Deadlines are relative to actual submission:

```
deadline = submitted_at + base_ms + exec_ms * exec_multiplier + U(0, jitter_ms)
```

Tying part of the slack to a task's own duration stops long tasks from being
unconditionally doomed under EDF, which would make the deadline metric measure
the workload rather than the scheduler.

## Reproducibility

**What is exactly reproducible.** For a given `(seed, workload config)`,
`workload.Generate` returns a byte-identical sequence of task specifications —
IDs, tenants, arrival offsets, durations, priorities and deadline slacks — on
any machine, in any process. This is guaranteed by drawing every value from a
single `math/rand` source seeded with `workload.seed` and consuming draws in a
fixed order per task: arrival, tenant, duration, priority, deadline jitter.
Changing that order changes every generated workload, so it is part of the
contract and is covered by `TestSameSeedProducesIdenticalSequence` for all four
workload types.

Repetition *i* uses seed `base_seed + i`, so repetitions differ from each other
but the whole run is reproducible from the base seed.

Because task IDs are derived from the seed and `id_prefix`, submitting the same
workload twice into the same namespace regenerates identical IDs and the
scheduler rejects the repeats as duplicates. The benchmark harness avoids this
entirely by giving every experiment its own namespace; the standalone producer
warns and tells you to change `--id-prefix` or `--seed`, or to reset the
namespace.

**What is not reproducible.** Timing is not. Actual submission instants, queue
waits, latencies, throughput and utilisation depend on machine speed, Redis
round-trip time, Go scheduling and background load. Two runs with the same seed
will produce the same *workload* but not the same *measurements*.

Consequences for interpreting results:

- Report repetitions, not single runs, and pick the count per workload. The
  plotting script shows the mean with standard-deviation error bars and warns
  explicitly when there is only one repetition. Measured against 25-repetition
  baselines:

  | workload | 5 repetitions is | median shift 5 → 25 |
  | --- | --- | ---: |
  | `bursty` | sufficient | 0.13% |
  | `multi_tenant` | sufficient | 0.26% |
  | `uniform` | sufficient | 0.60% |
  | `heavy_tailed` | **not sufficient**, use 20–25 | 8.42% |

  The distinguishing property is the task-duration distribution, not the arrival
  pattern: bursty arrivals and skewed tenants both average out over 4000 tasks,
  a Pareto tail does not. Strict priority's p50 is the one metric that wants
  more repetitions on every workload, because its median sits at the boundary
  between the served and starved priority populations.
  For `heavy_tailed`, 5-repetition estimates were biased in a consistent
  direction, one 95% interval (EDF throughput, 241.6 ± 4.4 tasks/s) excluded the
  better estimate of 257.6 entirely, and one qualitative conclusion flipped.
  Scheduler *rankings* were stable at 5 repetitions on every workload; magnitudes
  were not.

  Do not use interval coverage to judge convergence; it measures how stable a
  metric is, not how converged the estimate is. Across the four re-run workloads
  the counts of 5-repetition intervals that missed the 25-repetition mean were
  uniform 8, bursty 1, multi_tenant 0, heavy_tailed 1 — the *most* stable
  workload scored worst and the genuinely unconverged one scored well, because a
  low-variance metric produces an interval so narrow that a negligible shift
  escapes it. Compare the size of the shift in the point estimate instead.

  These are between-run comparisons, so sampling variation is confounded with
  anything that differed between runs. Shift directions are not consistent across
  workloads, which argues against systematic drift but does not exclude it, and
  the four schedulers within a workload share seeds so their shifts are
  correlated. Adequate for "is 5 enough"; not a precise estimate of sampling
  error.
- Check `producer_max_lag_s` in `aggregate.csv`. If it is a significant
  fraction of the observed latencies, the load generator, not the queue, was the
  bottleneck and the run should be repeated with a lower arrival rate or on a
  quieter machine.
- Do not compare numbers across machines, or across runs with different worker
  counts, concurrency or `max_in_flight`. All of those are recorded in every
  aggregate row precisely so that mismatches are visible.
- Absolute latencies from the `quick` profile are illustrative only. Use the
  `full` profile for anything reportable.

## Metrics and their definitions

Computed in `internal/bench` from the task-level result records, not scraped
from Prometheus, so the analysis has no dependency on scrape timing.

**Queue wait** — `started_at - submitted_at`. Time from submission until a
worker began executing.

**End-to-end latency** — `finished_at - submitted_at`. Includes wait, execution
and any retries.

**Percentiles** — linear interpolation between the two closest ranks, the same
definition numpy and pandas use by default, so Go-side and Python-side numbers
agree.

**Throughput** — `completed / makespan`, where makespan is the span from the
earliest submission to the latest completion. Using makespan rather than
wall-clock run time keeps tear-down time from deflating the figure.

**Deadline miss rate** — the fraction of completed tasks with
`finished_at > deadline`. Finishing exactly on the deadline is a hit. Tasks
without a deadline never miss. Dead-lettered tasks are excluded, since they have
no completion time; the dead-letter count is reported separately so a run cannot
look good by discarding work.

**Worker utilisation** — `busy_slot_seconds / (slots x experiment_duration)`,
where busy time is measured inside the workers across the whole experiment.
Because execution is simulated with sleeps, this measures slot occupancy, not
CPU.

**Maximum wait** — the largest queue wait observed. This is the starvation
signal: a policy that keeps some tasks waiting indefinitely shows it here long
before it shows up in p99.

**Jain's fairness index**

```
J(x) = (sum(x))^2 / (n * sum(x^2))
```

`x` is **completed service seconds per tenant**. That definition is stated on
every chart and in every CSV header comment. Tenants that received no service are
included in `x` as zeros — dropping them would report perfect fairness for a
queue that served exactly one tenant, which is the failure the index exists to
catch. `J` lies in `[1/n, 1]`: 1 when every tenant received identical service,
`1/n` when one tenant received everything. By convention an all-zero vector is
reported as 1 and an empty vector as NaN. `jain_fairness_count`, the same index
over completed task counts, is reported alongside because the two diverge when
tenants submit tasks of very different sizes.

**Important caveat on fairness under skew.** Jain's index over completed service
is *demand-limited*. Under the 90/3/3/2/2 skew, tenants B–E only ever submit a
few percent of the work, so no scheduler can give them a large share of service;
every policy reports a similarly low index near `1/n`. That is a property of the
workload, not a failure of the scheduler, and it means the fairness index alone
does not distinguish policies on this workload.

The metric that does distinguish them is **per-tenant latency**, recorded in
`tenants.csv` and plotted as `tenant_latency_skew.png`. A work-conserving fair
queue protects a small tenant's *latency* even when it cannot raise the tenant's
*share*. In the 80-experiment run reported in the README, tenant B–E p95 latency
is 0.077 s under WFQ versus ~2.7 s under FIFO, EDF and strict priority — a 36x
difference — while all four report a Jain index of 0.246. Report both.

## Reading the results

**Tail latency.** Compare `latency_p99_s` against `latency_p50_s`; the
`latency_tail_ratio` chart plots exactly that. A policy that improves the median
by deferring some population usually pays for it in the tail. Strict priority is
the clearest case: it typically has the best p50 of the four and the worst p99,
because the low-priority population absorbs all of the delay.

**Starvation.** Look at `wait_max_s` and at `priority_starvation.png`, which
plots p95 latency against priority level. Under strict priority, low-priority
tasks should show visibly worse latency than high-priority ones, and the gap
should widen with load. FIFO and WFQ should show no priority gradient at all,
because neither reads the priority field.

**Deadline compliance.** EDF has the lowest `deadline_miss_rate` at every
offered load measured, from 0.5x to 3x capacity. Classical theory warns that EDF
can degrade badly under overload by preferring already-doomed tasks; that did
not appear here, because the deadlines this workload generates are generous and
scale with each task's own duration, so a deferred long task usually still meets
its deadline. What does happen is that EDF's advantage erodes as load grows:
on the heavy-tailed workload it was roughly 129x better than FIFO at 1.25x load
and only 1.16x better at 3x. If EDF is not winning on this metric, check whether
the run was under-loaded (every policy misses nothing) or so overloaded that
every policy misses nearly everything.

**Fairness and isolation.** Read `jain_fairness_service` together with
`tenant_latency_skew.png`, for the reason given above. WFQ should show small
tenants with much lower latency than the dominant tenant; FIFO should show every
tenant with roughly the same latency, which sounds fair but means the small
tenants are queued behind the noisy neighbour's backlog.

**Throughput.** Expect all four policies to be close. Work-conserving schedulers
do not change how much work gets done; they change who waits. A large throughput
difference usually indicates a configuration problem — a timed-out experiment, a
dispatch bottleneck, or an under-loaded run — rather than a scheduling insight.

## Failure-injection experiments

`experiments/failure.yaml` sets `worker.fail_before_ack_rate: 0.20` and
`worker.fail_rate: 0.05`, so a fifth of attempts are abandoned after executing
and before acknowledging. That is the crash scenario that makes duplicate
execution possible, and it is the direct test of the idempotency guarantee.

What to expect:

- `tasks_retried` is well above zero.
- `tasks_completed + tasks_dead_lettered == tasks_planned`: nothing is lost.
- Each task ID appears at most once in the results stream, no matter how many
  times it executed. `TestIntermittentFailuresStillCompleteEveryTask` asserts
  this against a real Redis.
- Latency distributions have a long tail, because a retried task waits out the
  visibility timeout before its next attempt. This is expected and is why
  `recovery.min_idle` is a trade-off rather than a value to minimise: too short
  and healthy slow tasks get duplicated, too long and genuine failures take ages
  to recover.

## Scheduler failover experiments

`cmd/failoverbench` measures what a scheduler failure costs. The rest of this
work measures the *throughput* cost of centralising scheduling; this measures
the *availability* cost, which is the other half of the argument.

```bash
make failover REPETITIONS=15
make failover-plots RUN=failover
```

**Nothing about the failure is simulated.** The tool builds the scheduler
binary, starts real processes with `os/exec` in their own process groups, and
kills one with a real signal. Workers and the load generator run in-process
against the same Redis.

### How the outage is defined

The dispatch script stamps `dispatched_at_ms` on every execution-stream entry,
which is the scheduler's own record of when it acted. The outage is the gap
between the last dispatch at or before the kill and the first one after it. This
is deliberately narrow: it is a property of the stream, not a wall-clock
inference from bookkeeping inside the measuring process.

When no dispatch ever follows the kill, the trial is **censored** rather than
recorded as a large number. The `single` arm is entirely censored by
construction, and plotting it as a finite bar would be the most misleading thing
the analysis could do.

### The arms

| arm | replicas | signal | question |
| --- | ---: | --- | --- |
| `none` | 2 | — | What is the natural gap between dispatches at this load? Every other arm is read against this floor. |
| `graceful` | 2 | `SIGTERM` | What does a rollout cost? The leader releases the lease on the way out. |
| `crash` | 2 | `SIGKILL` | What does a crash cost? Nothing is released, so Redis must expire the key. |
| `single` | 1 | `SIGKILL` | What is the lease buying? Nothing takes over. |
| `pause` | 2 | `SIGSTOP` then `SIGCONT` | What is the *fence* buying? The leader is frozen past its lease, replaced, and then resumed still believing it leads. |

### The gray failure, and why it needed its own arm

The first four arms all kill the leader, and a lease alone would have handled
every one of them: a dead process writes nothing. That is the weakness in
demonstrating a fencing token against a `SIGKILL` — the mechanism is never
actually loaded.

The failure a fence exists for is the one where the deposed leader is *still
running*. It was stopped long enough for its lease to expire — a
garbage-collection pause, a suspended container, a host that swapped — a
standby took over, and then it resumed, still holding its old epoch and with no
way to know. Cancelling its context cannot stop it, because it was not running
to observe the cancellation, and having it re-check whether it still leads is no
better: any such check is separated from the write that follows it by a window
in which the answer can change. The check has to happen at the resource, inside
the same atomic unit as the write.

The `pause` arm produces that state with real signals. The leader is stopped
with `SIGSTOP` for twice its lease TTL and then continued with `SIGCONT`. Three
things are verified rather than assumed, because a fault-injection experiment
that does not confirm the fault occurred can pass while testing nothing:

1. the victim really is stopped, read from the operating system's process state
   rather than inferred from a signal call that returned no error;
2. a *different* process holds the lease before the victim is resumed, so the
   resumed process is genuinely superseded and not merely slow;
3. after the resume, exactly one process still claims leadership.

### The dispatch log proves the invariant, rather than a counter asserting it

Every execution-stream entry carries the epoch that dispatched it. The fence's
guarantee is that once epoch *N* has written, nothing below *N* ever writes
again, which makes the invariant checkable directly from the artefact: replay
the stream in order and the epochs must never go backwards. The harness counts
inversions in every arm and aborts the run on the first one; the plotting script
re-checks the same column and refuses to chart a run that violates it.

This is worth more than the metric counter it replaces. A counter lives in a
process that has since exited, and it records that the system *noticed*
something; the stream records what the system actually *did*, and anyone can
re-derive the check from the committed CSV.

Entries with no epoch are skipped rather than counted as zero. Those are
worker-initiated retries, which no leader dispatched and which the fence
therefore does not govern; counting them would manufacture an inversion at every
retry and make the check meaningless.

### Why the kill time is jittered

The outage after a crash is the lease TTL minus however long ago the leader last
renewed. With a fixed kill offset, every trial kills at the same phase of the
renewal cycle and the measured outage collapses to a single number — real, but
not the distribution. The kill is therefore jittered uniformly over exactly one
renewal interval, from a seeded generator so a run is still reproducible. The
expected spread is `[TTL − renew_interval, TTL]`.

This was not how the experiment was first written. The first version used a
fixed offset, produced outages within 4 ms of each other across fifteen trials,
and the suspiciously tight clustering is what exposed the bias.

### Run it on an idle machine that will not sleep

The outage is a wall-clock timing measurement made by killing processes, so
anything else competing for the CPU lands directly in the number. Two separate
runs of this experiment were discarded for exactly that reason: one because a
race-detector test suite was started alongside it, and one because the laptop
idle-slept partway through an unattended run and produced two trials reporting
seventeen-minute "outages".

The second failure is now detected rather than averaged in. A crash outage is
bounded above by the lease TTL by construction, so the harness flags any trial
whose outage exceeds five times the TTL as `suspect`, logs it as an error, and
the plotting script drops those trials while printing what it dropped. The
`make failover` target also runs under `caffeinate -dims` where that exists.

### The invariant check

Every trial scrapes `tq_scheduler_is_leader` from all replicas once the queue is
running and refuses to proceed unless the sum is exactly 1. This means every
reported trial carries evidence that the single-writer invariant held while it
ran, rather than the invariant being asserted only in unit tests. The plotting
script re-checks the same column and refuses to chart a run that violates it.

### What this experiment found

The failover experiment was not only a measurement. Every killed-leader trial
reported exactly one task short of the 4,000 submitted, which led to a genuine
durability bug: `XREADGROUP` with `>` only delivers entries nobody has seen, so
an ingress entry the dead leader read and never acknowledged belongs to a
consumer name that never returns. The task was accepted and then never
scheduled — not pending, not in flight, not dead-lettered, counted nowhere.

The fix is an `XAUTOCLAIM` pass over the ingress group, run by the leader, with
`scheduler.ingest_reclaim_min_idle` defaulting to 2s. The same trials now
complete all 4,000 tasks. See `TestPromotedEngineReclaimsTheDeadLeadersIngressEntries`,
which was confirmed to fail with the reclaim loop removed.

The `pause` arm answered the question the other four could not. In **11 of 15
trials the resumed leader reached Redis with a write before its own lease
renewal had noticed anything was wrong**, and the epoch check at the resource is
what refused it; in the remaining 4 it stood down first. Both outcomes are
correct, but the split is the measurement: it says that a deployment with leases
and no fencing would have admitted a second writer in eleven of fifteen cases.
No task was dispatched under a superseded epoch in any arm.

Deleting the fence and re-running the deterministic version of that scenario
(`TestAResumedLeaderCannotDispatchAfterBeingSuperseded`) puts **172 of 400 tasks
on the wire under a dead term**, which is both the counterfactual and the
evidence that the test has teeth.

## Store failure experiments

`cmd/storefaultbench` breaks Redis underneath a running queue. Everything else
in this work measures the *scheduler* failing; this measures what happens when
the thing the scheduler's correctness argument depends on fails instead.

```bash
make storefault STOREFAULT_REPETITIONS=10
make storefault-plots RUN=storefault
```

It builds its own throwaway Sentinel cluster — one master, two replicas, three
sentinels, on ports 7301+ and 27301+ — so it never touches the Redis used by
every other target. It needs `redis-server` and `redis-sentinel` on `PATH` and
no container runtime.

### Why the fence is at stake

The fencing token is a monotonic counter held in Redis. Its guarantee is that
once epoch *N* has written, nothing below *N* writes again, and that holds only
while the counter survives. A Sentinel promotion to a replica that had not
caught up discards writes the master had already acknowledged, including that
`INCR`. The promoted node then hands the same number out again, and one token
identifies two leadership terms.

### The three things that made the experiment real

Each of these was found by getting it wrong and watching the arm pass.

**A stopped process is not a partitioned one.** `SIGSTOP` on a replica does not
stop replication. The kernel keeps accepting and acknowledging TCP segments for
a stopped process, so the stream accumulates in the socket buffer and is applied
in full on resume. The replica ends up with everything and the trial reports no
loss. Replication therefore runs through `internal/netfault`, a small proxy that
can be severed.

**A graceful shutdown is not a crash.** Redis 7 and later wait for replicas to
catch up during `SHUTDOWN`. A politely stopped master loses nothing, so every
kill in this experiment is `SIGKILL`.

**Sentinel repairs the fault.** Sentinel reconfigures a replica whose master
address it does not recognise, quietly undoing the injected partition on its own
refresh period. The fault window is short and fixed, and the harness verifies
replication is *still* down at the moment the master dies, aborting the trial
otherwise rather than reporting a clean failover as a lossy one.

### The arms

| arm | fault | mitigation |
| --- | --- | --- |
| `clean` | master killed, replicas caught up | none — the control |
| `lagging` | replication cut, then the master killed | none |
| `epochwait` | the same | lease waits for a replica to acknowledge a new epoch |
| `guarded` | the same | the above, plus `min-replicas-to-write` on the master |

The two mitigations are separated on purpose. Enabled together they plainly
work, which would leave the useful question unanswered: whether protecting the
token alone is enough. It is not.

### What this experiment found

Across 40 trials:

- **`lagging`: 10/10 trials issued one fencing token to two different leaders**,
  and destroyed about 1,000 acknowledged tasks each.
- **`epochwait`: also 10/10.** Waiting for the epoch to replicate does not help,
  because the counter is incremented *before* the wait. A lost write can be
  detected afterwards but not un-issued.
- **`guarded`: 0/10, and no tasks destroyed.** Refusing the write is what works:
  the `INCR` fails outright with `NOREPLICAS` while no replica can take it.
- **`clean`: 0/10.** The control has to be clean or the harness is measuring
  itself.

The guarded configuration accepted half as many submissions, which is the cost
and is worth stating precisely: the unguarded arms *completed* the same number
of tasks. They merely also told the producer they had accepted work they then
destroyed. The guard converts silent data loss into a visible error.

### Which column to trust

`epoch_rewound` and `epoch_reused` disagree, and the reused-token count is the
reliable one. Rewinding is detected by reading the counter from the promoted
node afterwards, by which point a new leader has often already advanced it past
where it was. Token reuse is observed continuously and recorded in the harness's
own memory, outside the store — because the fault under test destroys Redis
state, and asking the survivor to describe the part it slept through is not a
measurement.

## Known limitations

1. **Simulated execution.** Workers sleep rather than compute by default.
   Utilisation is slot occupancy, not CPU utilisation, and no cache, memory or
   I/O effects appear. `worker.exec_mode: cpu` burns the duration in real
   computation instead; running the same experiment both ways shows the sleep
   model is not neutral, because it hides the execution inflation that breaks
   EDF's absolute deadline arithmetic. WFQ is unaffected, since it compares
   tenants against one another and a uniform cost error cancels. See the
   real-work section of the README.
2. **WFQ knows exact task cost.** The virtual finish time is computed from the
   task's declared duration, which this system knows exactly. A production
   scheduler must estimate it, and estimation error degrades fairness. The
   experiments therefore report an upper bound on WFQ's achievable fairness.
3. **Non-preemptive, whole-task granularity.** Once dispatched, a task runs to
   completion. Fairness is only approximate over horizons shorter than the
   largest task, which matters most on the heavy-tailed workload.
4. **One scheduler dispatches at a time.** Extra replicas buy availability, not
   throughput: the lease guarantees exactly one leader, so scheduling capacity
   is still bounded by one process and one Redis instance. This bounds the scale
   at which these results apply; it is not a claim about how the policies would
   behave in a sharded scheduler.
5. **Single-node Redis.** No cluster, no failover. Redis is assumed available,
   and Redis failure modes are out of scope. This matters more now than it did
   before: the fencing argument assumes Redis does not lose acknowledged writes,
   which a Sentinel failover can violate. Scheduler failure is measured; store
   failure is not.
6. **Latency is measured in wall-clock time on a shared machine.** Repetitions
   and reported variance are the mitigation, not a fix.
7. **Deadline misses are computed over completed tasks.** A dead-lettered task
   has no completion time and is excluded from the miss rate, so the
   dead-letter count must always be read alongside it.

## Reasonable future work

- Ageing or lottery scheduling as a starvation-free alternative to strict
  priority, with the same measurement harness.
- WF2Q or deficit round robin, to quantify how much SCFQ's coarser virtual-time
  advance actually costs.
- Cost estimation for WFQ from historical task durations, to measure the
  fairness lost to estimation error.
- A sharded scheduler partitioned by tenant, to lift the single-writer bound,
  and a measurement of the fairness cost of partitioning.
- Real task bodies (CPU-bound, I/O-bound) alongside the simulated ones, to check
  how far the sleep-based conclusions transfer.
- Latency-aware autoscaling driven by `tq_pending_tasks` and
  `tq_longest_pending_wait_seconds` rather than CPU.
- Redis failover and network-partition injection. Scheduler failure is now
  measured; the store the fencing argument depends on is not.
- Failover under load rather than beside it: every trial here kills the leader
  while the queue is in steady state, so the measured outage is the time to
  resume dispatching, not the time to work off the backlog the outage created.
