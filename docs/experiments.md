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
  | `heavy_tailed` | **not sufficient**, use 20–25 | 8.42% |

  `uniform` has not been re-run at 25; its variance profile resembles `bursty`,
  so 5 is probably fine, but that is an expectation rather than a measurement.

  The distinguishing property is the task-duration distribution, not the arrival
  pattern: bursty arrivals average out over 4000 tasks, a Pareto tail does not.
  For `heavy_tailed`, 5-repetition estimates were biased in a consistent
  direction, one 95% interval (EDF throughput, 241.6 ± 4.4 tasks/s) excluded the
  better estimate of 257.6 entirely, and one qualitative conclusion flipped.
  Scheduler *rankings* were stable at 5 repetitions on every workload; magnitudes
  were not.

  Do not use interval coverage to judge convergence. Across the three re-run
  workloads the count of 5-repetition intervals that missed the 25-repetition
  mean was 1, 0 and 1 — no signal — because a low-variance metric produces an
  interval so narrow that a negligible shift escapes it. Compare the size of the
  shift in the point estimate instead.
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

**Deadline compliance.** EDF should have the lowest `deadline_miss_rate` when
the system can meet most deadlines. Under heavy overload EDF degrades sharply,
because it keeps preferring tasks that are already about to miss and so misses
them anyway while delaying tasks that could still have been met. If EDF is not
winning on this metric, check whether the run was under-loaded (every policy
misses nothing) or catastrophically overloaded (every policy misses everything).

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

## Known limitations

1. **Simulated execution.** Workers sleep rather than compute. Utilisation is
   slot occupancy, not CPU utilisation, and no cache, memory or I/O effects
   appear.
2. **WFQ knows exact task cost.** The virtual finish time is computed from the
   task's declared duration, which this system knows exactly. A production
   scheduler must estimate it, and estimation error degrades fairness. The
   experiments therefore report an upper bound on WFQ's achievable fairness.
3. **Non-preemptive, whole-task granularity.** Once dispatched, a task runs to
   completion. Fairness is only approximate over horizons shorter than the
   largest task, which matters most on the heavy-tailed workload.
4. **Single scheduler process.** The scheduler is a single writer, so scheduling
   throughput is bounded by one process and one Redis instance. This bounds the
   scale at which these results apply; it is not a claim about how the policies
   would behave in a sharded scheduler.
5. **Single-node Redis.** No cluster, no failover. Redis is assumed available;
   Redis failure modes are out of scope.
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
