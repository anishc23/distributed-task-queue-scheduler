# Metrics reference

Both the scheduler and the workers expose a Prometheus endpoint:

| Process | Default address | Configuration |
| --- | --- | --- |
| Scheduler | `:9101/metrics` | `scheduler.metrics_addr` or `--metrics-addr` |
| Worker | `:9102/metrics` | `worker.metrics_addr` or `--metrics-addr` |

Each endpoint also serves `/healthz` (the process is alive) and `/readyz` (the
process has finished starting and is consuming). Kubernetes uses both.

## Cardinality policy

The only dynamic label is `tenant`, drawn from a small configured set — five by
default. **Task IDs, attempt IDs, worker names and stream entry IDs are never
used as labels.** Per-task detail lives in the results stream and in the
benchmark CSVs, which is where unbounded-cardinality data belongs.

Two constant labels are attached to every series:

| Label | Values | Purpose |
| --- | --- | --- |
| `component` | `scheduler`, `worker` | Which process produced the sample. |
| `scheduler` | `fifo`, `priority`, `edf`, `wfq` | The active scheduling policy, so one Prometheus job can compare policies. |

A process exposes only the metrics it can actually produce: a scheduler does not
publish worker-side families and a worker does not publish scheduler-side ones.
Nothing is ever reported as permanently zero, because a stuck-at-zero family is
harder to interpret than an absent one.

Every tenant series is pre-created at zero at startup, so a tenant that receives
no service at all is visible as an explicit zero rather than missing. That is
what makes starvation legible in a dashboard.

## Scheduler metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tq_tasks_admitted_total` | counter | — | Tasks accepted from the ingress stream into the pending set. |
| `tq_tasks_dispatched_total` | counter | `tenant` | Tasks selected by the policy and published to the execution stream. |
| `tq_pending_tasks` | gauge | — | Tasks currently waiting in the scheduler's pending set. The queue depth. |
| `tq_inflight_tasks` | gauge | — | Tasks dispatched but not yet terminal. Compare against `scheduler.max_in_flight`. |
| `tq_longest_pending_wait_seconds` | gauge | — | Age of the oldest task currently pending. |
| `tq_max_observed_wait_seconds` | gauge | — | Longest pending wait seen since this process started. Monotonic; the primary starvation indicator. |
| `tq_recovery_reclaimed_total` | counter | — | Execution-stream entries reclaimed after exceeding the visibility timeout. |
| `tq_ingress_reclaimed_total` | counter | — | Ingress entries reclaimed from a scheduler that died before acknowledging them. A burst after a failover is expected; a sustained rate means schedulers are dying mid-batch. |
| `tq_scheduler_is_leader` | gauge | — | 1 when this replica holds the dispatch lease, 0 when it is standing by. |
| `tq_scheduler_leader_epoch` | gauge | — | Fencing token of this replica's current term; 0 when not leading. Compare across replicas to see which one believes it is newest. |
| `tq_scheduler_leader_transitions_total` | counter | — | Leadership changes seen by this process, counting acquisitions and losses. |
| `tq_scheduler_fenced_operations_total` | counter | — | Writes refused because a newer epoch had already taken over. Non-zero means a deposed leader tried to act and was stopped. |
| `tq_dispatch_errors_total` | counter | — | Errors raised by the dispatch loop. Should stay at zero. |
| `tq_duplicate_admissions_total` | counter | — | Ingress entries rejected because the task was already known. Non-zero after a scheduler crash and restart, which is expected. |

## Worker metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tq_tasks_completed_total` | counter | `tenant` | Tasks that reached successful terminal completion. |
| `tq_task_failures_total` | counter | — | Attempts that reported an application error or were abandoned before acknowledgement. |
| `tq_duplicate_completions_total` | counter | — | Completions suppressed by the idempotency guard. Non-zero means duplicate delivery happened and was handled correctly. |
| `tq_deadline_missed_total` | counter | `tenant` | Completed tasks that finished after their deadline. |
| `tq_queue_wait_seconds` | histogram | — | Seconds from submission to the start of execution. |
| `tq_task_latency_seconds` | histogram | — | End-to-end seconds from submission to terminal completion. |
| `tq_task_exec_seconds` | histogram | — | Measured seconds a worker spent executing a task. |
| `tq_worker_busy_seconds_total` | counter | — | Cumulative seconds worker slots spent executing. |
| `tq_worker_slot_seconds_total` | counter | — | Cumulative slot-seconds available (concurrency x uptime). |
| `tq_worker_utilization_ratio` | gauge | — | Busy slot-seconds divided by available slot-seconds since start. |
| `tq_worker_concurrency` | gauge | — | Configured execution slots in this process. |
| `tq_tenant_service_seconds_total` | counter | `tenant` | Completed service time per tenant. **This is the `x` vector of Jain's fairness index.** |
| `tq_tenant_completed_tasks_total` | counter | `tenant` | Completed tasks per tenant. |

## Metrics exposed by both

| Metric | Type | Meaning |
| --- | --- | --- |
| `tq_task_retries_total` | counter | Task redeliveries caused by worker failure or a lost acknowledgement. Produced by the scheduler's recovery loop and by workers reporting errors. |
| `tq_tasks_dead_lettered_total` | counter | Tasks retired to the dead-letter stream after exhausting `max_retries`. |

Standard Go runtime and process collectors are also registered on every
endpoint.

## Histogram buckets

All three histograms use exponential buckets from 1 ms to roughly 65 s
(`prometheus.ExponentialBuckets(0.001, 2, 17)`). That range covers both the
sub-second simulated tasks and the pathological queueing delays that starvation
experiments are meant to expose.

## Leadership

Every scheduler replica exposes `tq_scheduler_is_leader`, including the ones
standing by, and the reason is that the useful alert is not about any single
replica:

```promql
# The one expression worth alerting on. 0 means nothing is dispatching;
# 2 or more means a split brain, which the fence should have prevented.
sum(tq_scheduler_is_leader) != 1
```

Alerting on `tq_scheduler_is_leader == 0` instead would fire constantly, because
a healthy standby reports exactly that. This is also why a standby's readiness
probe passes: an unready pod is dropped from the Service, which would stop
Prometheus scraping the metric that tells you who leads.

```promql
# Failovers over the last hour. Each one is counted twice, once as a loss and
# once as an acquisition, so divide by two for the number of handovers.
sum(increase(tq_scheduler_leader_transitions_total[1h])) / 2

# A deposed leader tried to write and was refused. Rare and benign in itself,
# but a sustained rate means a replica is repeatedly being paused past its
# lease -- an overloaded host, or a TTL set too low for this environment.
rate(tq_scheduler_fenced_operations_total[5m]) > 0

# Tasks stranded by a dying scheduler and recovered by its successor.
increase(tq_ingress_reclaimed_total[10m]) > 0
```

## Useful queries

Throughput, tasks per second:

```promql
sum(rate(tq_tasks_completed_total[1m]))
```

p99 end-to-end latency:

```promql
histogram_quantile(0.99, sum(rate(tq_task_latency_seconds_bucket[5m])) by (le))
```

p99 queue wait, split by scheduling policy:

```promql
histogram_quantile(0.99,
  sum(rate(tq_queue_wait_seconds_bucket[5m])) by (le, scheduler))
```

Deadline miss rate:

```promql
sum(rate(tq_deadline_missed_total[5m])) / sum(rate(tq_tasks_completed_total[5m]))
```

Worker utilisation across the fleet:

```promql
sum(rate(tq_worker_busy_seconds_total[5m])) / sum(rate(tq_worker_slot_seconds_total[5m]))
```

Completed service share per tenant, the live view of what feeds Jain's index:

```promql
sum by (tenant) (rate(tq_tenant_service_seconds_total[5m]))
  / ignoring(tenant) group_left sum(rate(tq_tenant_service_seconds_total[5m]))
```

Starvation watch — the longest anything has waited:

```promql
max(tq_max_observed_wait_seconds)
```

Queue is growing faster than it drains:

```promql
sum(rate(tq_tasks_admitted_total[1m])) - sum(rate(tq_tasks_completed_total[1m])) > 0
```

Retries and dead letters:

```promql
sum(rate(tq_task_retries_total[5m]))
sum(increase(tq_tasks_dead_lettered_total[1h]))
```

Duplicate execution is happening and being handled:

```promql
sum(increase(tq_duplicate_completions_total[1h]))
```

## Why the benchmark does not read Prometheus

The benchmark harness computes its statistics from the task-level result records
in Redis, not from these metrics. Percentiles derived from histogram buckets are
approximate, scrape timing would introduce a dependency on Prometheus being up
and correctly configured, and counters cannot express per-task detail. The
metrics exist for operating the system; the CSVs exist for analysing it.
