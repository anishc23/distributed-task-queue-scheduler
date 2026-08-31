# Distributed Task Queue with Pluggable Scheduling

> An experimental distributed task queue for evaluating how scheduling policies affect latency, throughput, deadline compliance, fairness, and starvation under realistic workloads.

## Overview

Modern applications often need to execute work asynchronously. Sending emails, generating reports, processing files, and running background jobs should not force users to wait for completion.

This project implements a **distributed task queue** in which the scheduling policy is a first-class, swappable component.

Instead of hard-coding a single queue discipline, the system allows different schedulers to decide which pending task should execute next. The project then benchmarks these policies under controlled workloads to study their impact on system performance.

The central research question is:

> **How does the choice of scheduling policy affect latency, throughput, deadline compliance, fairness, and starvation in a distributed task queue under different workload distributions?**

The project is designed both as a systems engineering project and as an experimental platform for empirical scheduling research.

---

## Key Features

* Distributed task processing using Redis Streams
* Consumer groups and task acknowledgements
* Pluggable scheduler architecture
* FIFO scheduling
* Priority scheduling
* Earliest Deadline First (EDF) scheduling
* Weighted Fair Queuing (WFQ)
* Worker failure recovery using visibility timeouts
* Retry limits and dead-letter queue support
* At-least-once task delivery semantics
* Idempotent task execution model
* Reproducible workload generation using fixed random seeds
* Prometheus metrics
* Automated benchmark execution
* CSV result generation
* Automated performance plots
* Local Kubernetes deployment using `kind`
* Horizontal worker scaling
* Controlled experiments across multiple workload distributions

---

## Architecture

```text
                         ┌─────────────────────┐
                         │      Producer       │
                         │  Workload Generator │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │        Redis        │
                         │      Streams        │
                         │                     │
                         │  Pending Tasks      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │      Scheduler      │
                         │                     │
                         │  ┌───────────────┐  │
                         │  │ FIFO          │  │
                         │  │ Priority      │  │
                         │  │ EDF           │  │
                         │  │ WFQ           │  │
                         │  └───────────────┘  │
                         └──────────┬──────────┘
                                    │
                      ┌─────────────┼─────────────┐
                      ▼             ▼             ▼
                 ┌────────┐    ┌────────┐    ┌────────┐
                 │Worker 1│    │Worker 2│    │Worker N│
                 └────┬───┘    └────┬───┘    └────┬───┘
                      │             │             │
                      └─────────────┼─────────────┘
                                    ▼
                         ┌─────────────────────┐
                         │   Metrics Layer     │
                         │     Prometheus      │
                         └──────────┬──────────┘
                                    ▼
                         ┌─────────────────────┐
                         │ Benchmark Harness   │
                         │ CSV + Plots + Report│
                         └─────────────────────┘
```

The system consists of six primary components:

### 1. Broker

Redis Streams stores tasks and provides consumer groups and acknowledgement mechanisms.

Redis is intentionally used as the broker rather than implementing a custom message broker. The goal of this project is to study **task scheduling**, not broker implementation.

### 2. Producer

The producer generates tasks according to configurable workload distributions.

Each task may contain metadata such as:

* Task ID
* Submission timestamp
* Simulated execution duration
* Priority
* Deadline
* Tenant ID
* Retry count

### 3. Scheduler

The scheduler determines which waiting task should execute next.

All scheduling algorithms implement a common interface, allowing the active scheduler to be changed through configuration without modifying workers or producers.

Conceptually:

```text
select_next(pending_tasks, available_workers) -> task
```

This abstraction is the core of the project.

### 4. Workers

Workers execute tasks independently.

For controlled experiments, task execution is simulated using a configurable sleep duration. This allows scheduler behavior to be studied without application-specific computation affecting results.

Workers acknowledge successful tasks and participate in failure recovery and retry handling.

### 5. Metrics Layer

Prometheus collects operational and experimental metrics including:

* Queue wait time
* End-to-end task latency
* Execution duration
* Deadline misses
* Worker utilisation
* Throughput
* Retry counts
* Task failures

### 6. Benchmark Harness

The benchmark harness automatically runs scheduling experiments and produces:

* Raw CSV results
* Aggregated metrics
* Latency statistics
* Fairness measurements
* Performance plots

---

# Scheduling Policies

## FIFO

**First In, First Out** executes the oldest waiting task first.

FIFO acts as the baseline scheduler.

### Advantages

* Simple
* Predictable
* Low scheduling overhead

### Limitations

FIFO does not consider priority, deadlines, task size, or tenants. Under heavy-tailed workloads, long-running tasks can contribute to head-of-line blocking.

---

## Priority Scheduling

Tasks are assigned priority classes, and higher-priority tasks are scheduled before lower-priority tasks.

### Advantages

* Fast handling of important work
* Useful for latency-sensitive operations

### Limitations

Under sustained high-priority traffic, low-priority tasks may experience starvation.

The experiments intentionally measure whether this starvation occurs.

---

## Earliest Deadline First (EDF)

EDF selects the task with the nearest deadline.

### Advantages

* Explicitly deadline-aware
* Strong theoretical properties under appropriate scheduling assumptions

### Limitations

When the system is heavily overloaded, not all deadlines can necessarily be satisfied. EDF behavior under overload is therefore evaluated experimentally rather than assuming performance from theory.

---

## Weighted Fair Queuing

Weighted Fair Queuing allocates service across tenants according to configured weights.

For example:

```text
Tenant A: weight 5
Tenant B: weight 2
Tenant C: weight 1
```

The scheduler attempts to prevent a high-volume tenant from consuming all available processing capacity.

This policy is particularly relevant for multi-tenant systems and noisy-neighbor scenarios.

---

# Workloads

Each scheduler is tested against four controlled workload types.

## 1. Uniform

A steady arrival pattern with similar task durations.

This acts as the control workload.

---

## 2. Bursty

Periods of relatively low traffic are interrupted by sudden spikes.

The workload generator models arrivals using configurable stochastic processes, including Poisson-based arrivals and burst events.

This tests scheduler behavior during temporary overload.

---

## 3. Heavy-Tailed

Most tasks are short, while a small percentage are significantly longer.

Task durations can be generated using a Pareto-like heavy-tailed distribution.

Example:

```text
Most tasks: approximately 100 ms
Rare tasks: up to tens of seconds
```

This workload is intended to expose the effect of long jobs on queue latency and head-of-line blocking.

---

## 4. Multi-Tenant Skew

One tenant produces most of the workload while several other tenants submit relatively few tasks.

Example:

```text
Tenant A: 90%
Tenant B: 3%
Tenant C: 3%
Tenant D: 2%
Tenant E: 2%
```

This workload evaluates fairness and noisy-neighbor behavior.

---

# Experimental Matrix

The baseline experiment consists of:

| Scheduler             | Uniform | Bursty | Heavy-Tailed | Multi-Tenant Skew |
| --------------------- | ------: | -----: | -----------: | ----------------: |
| FIFO                  |       ✓ |      ✓ |            ✓ |                 ✓ |
| Priority              |       ✓ |      ✓ |            ✓ |                 ✓ |
| EDF                   |       ✓ |      ✓ |            ✓ |                 ✓ |
| Weighted Fair Queuing |       ✓ |      ✓ |            ✓ |                 ✓ |

This produces **16 primary scheduler-workload experiments**.

Each experiment can be repeated multiple times using controlled random seeds.

---

# Metrics

## Latency

Task latency statistics include:

* p50
* p95
* p99

The p99 latency is particularly important because it captures poor tail performance that averages can hide.

---

## Deadline Miss Rate

The proportion of tasks that complete after their configured deadline.

```text
deadline_miss_rate =
    missed_deadlines / total_tasks
```

---

## Throughput

The number of successfully completed tasks per second.

---

## Worker Utilisation

The fraction of time workers spend executing tasks rather than remaining idle.

---

## Jain's Fairness Index

Fairness across tenants is measured using Jain's Fairness Index:

```text
J(x) = (Σxi)² / (n × Σxi²)
```

where `xi` represents the service received by tenant `i`.

A value closer to `1` indicates more equal allocation.

---

## Starvation

The system records the longest waiting time observed for an individual task.

This is especially relevant for priority scheduling.

---

# Delivery Semantics and Failure Handling

The queue uses **at-least-once delivery**.

Exactly-once processing is intentionally not a project goal.

Instead, tasks are designed around the production-oriented model:

> At-least-once delivery + idempotent task execution.

## Worker Failure Recovery

If a worker receives a task but fails before acknowledging completion, the task must not disappear permanently.

The system therefore supports a visibility timeout:

```text
Task delivered
      ↓
Worker processes task
      ↓
Acknowledgement received?
      ├── Yes → task completed
      │
      └── No
            ↓
      Visibility timeout expires
            ↓
      Task becomes eligible for recovery/retry
```

## Retries

Tasks have configurable retry limits.

Repeatedly failing tasks are eventually moved to a dead-letter queue for inspection.

---

# Reproducibility

Experimental reproducibility is a core design requirement.

Each benchmark configuration records:

* Random seed
* Scheduler
* Workload type
* Arrival parameters
* Task duration parameters
* Number of workers
* Experiment duration
* System configuration

The same seed and configuration should reproduce the same generated workload.

System-level execution timing may still introduce variation, so benchmark experiments should be repeated and reported with appropriate summary statistics.

---

# Project Structure

```text
.
├── cmd/
│   ├── producer/
│   ├── scheduler/
│   ├── worker/
│   └── benchmark/
│
├── internal/
│   ├── broker/
│   ├── scheduler/
│   │   ├── fifo/
│   │   ├── priority/
│   │   ├── edf/
│   │   └── wfq/
│   ├── worker/
│   ├── metrics/
│   └── recovery/
│
├── workloads/
│   ├── uniform/
│   ├── bursty/
│   ├── heavy_tailed/
│   └── multi_tenant/
│
├── configs/
├── experiments/
├── scripts/
├── deploy/
│   ├── docker/
│   └── kubernetes/
│
├── results/
├── docs/
└── README.md
```

The final directory structure may evolve as implementation progresses.

---

# Quick Start

## Prerequisites

* Docker
* Redis
* Python or the selected implementation runtime
* Prometheus
* `kind`
* Kubernetes CLI
* Helm

### Start Redis

```bash
docker compose up -d redis
```

### Run the Producer

```bash
<implementation command>
```

### Start Workers

```bash
<implementation command>
```

### Select a Scheduler

Example configuration:

```yaml
scheduler: fifo
```

Available policies:

```text
fifo
priority
edf
wfq
```

### Run a Benchmark

```bash
<benchmark command>
```

The completed implementation will document the exact commands and configuration files here.

---

# Local Kubernetes Deployment

The project supports local deployment using `kind`.

```bash
kind create cluster --name task-queue
```

Workers can then be deployed and scaled independently.

Example:

```bash
kubectl scale deployment task-worker --replicas=4
```

This demonstrates how the queue and worker model supports horizontal scaling.

---

# Results

Experimental results will be added after the full benchmark suite has been executed.

The repository will report actual measured values only.

Example result format:

| Scheduler | Workload | p50 | p95 | p99 | Deadline Miss Rate | Throughput | Fairness |
| --------- | -------- | --: | --: | --: | -----------------: | ---------: | -------: |
| FIFO      | Uniform  | TBD | TBD | TBD |                TBD |        TBD |      TBD |
| Priority  | Uniform  | TBD | TBD | TBD |                TBD |        TBD |      TBD |
| EDF       | Uniform  | TBD | TBD | TBD |                TBD |        TBD |      TBD |
| WFQ       | Uniform  | TBD | TBD | TBD |                TBD |        TBD |      TBD |

**No performance improvement claims should be made until supported by reproducible benchmark results.**

---

# Design Decisions

## Why Redis Streams?

Redis Streams provides useful task-processing primitives such as:

* Consumer groups
* Task acknowledgement
* Pending message tracking

This allows the project to focus engineering effort on scheduling and experimentation.

---

## Why Not Build a Custom Broker?

The research and engineering focus is scheduling policy.

Building a message broker would increase scope without contributing directly to the central experimental question.

---

## Why Simulate Task Execution?

Using controlled sleep durations allows:

* Reproducible task sizes
* Controlled heavy-tailed workloads
* Direct comparison between schedulers
* Reduced application-specific noise

The queue can later be extended with real task types.

---

## Why At-Least-Once Instead of Exactly-Once?

Exactly-once processing is expensive and difficult to guarantee in distributed systems.

A more practical design is:

```text
At-least-once delivery
        +
Idempotent task handlers
        =
Practical failure-tolerant processing
```

---

# Roadmap

* [ ] End-to-end FIFO task execution
* [ ] Redis Streams consumer groups
* [ ] Worker acknowledgements
* [ ] Visibility timeout recovery
* [ ] Retry handling
* [ ] Dead-letter queue
* [ ] Scheduler interface
* [ ] Priority scheduler
* [ ] EDF scheduler
* [ ] Weighted Fair Queuing scheduler
* [ ] Prometheus instrumentation
* [ ] Reproducible workload generator
* [ ] Uniform workload
* [ ] Bursty workload
* [ ] Heavy-tailed workload
* [ ] Multi-tenant workload
* [ ] Automated benchmark harness
* [ ] CSV result generation
* [ ] Automated plots
* [ ] Full 16-experiment benchmark suite
* [ ] Local Kubernetes deployment
* [ ] Demo video
* [ ] Technical report / paper

---

# Research Direction

A potential paper framing is:

> **An Empirical Comparison of Scheduling Policies in Distributed Task Queues Under Realistic Workload Distributions**

The intended contribution is empirical rather than claiming a novel scheduling algorithm.

The study compares established policies under controlled conditions and evaluates trade-offs between:

* Tail latency
* Deadline compliance
* Throughput
* Fairness
* Starvation

---

# Status

**Active Development**

This repository is being developed incrementally. Features marked as planned or unchecked in the roadmap are not yet implemented.

---

# License

To be added.

# Author

Built as a systems and distributed computing project focused on task scheduling, queueing behavior, reproducible benchmarking, and Kubernetes-based deployment.
