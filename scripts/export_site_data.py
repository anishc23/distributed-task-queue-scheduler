#!/usr/bin/env python3
"""Export the published experiment results into the JSON the research site reads.

The site under web/ never reads results/ directly: results/ is gitignored and
1.5 GB, while the site has to build on a machine that has never run a
benchmark. This script is the only bridge between the two. It

  * reads the raw CSVs named in DATASETS below and nothing else,
  * writes one JSON file per dataset under web/public/data/, each carrying the
    SHA-256 and row count of every file it was derived from,
  * recomputes the headline numbers quoted in docs/report.md from those files
    and refuses to write anything if one of them no longer reproduces.

That last step is the point. A results page can drift from its data in exactly
the way a hand-copied table can; this one cannot, because the export fails
first. Output is deterministic (no timestamps, sorted keys), so re-running it on
unchanged results produces no diff.

Standard library only, so it runs without the plotting virtualenv:

    python3 scripts/export_site_data.py            # write web/public/data
    python3 scripts/export_site_data.py --check    # verify claims, write nothing
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import math
import os
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = 1
POLICIES = ["fifo", "priority", "edf", "wfq"]
WORKLOADS = ["uniform", "bursty", "heavy_tailed", "multi_tenant"]


# ---------------------------------------------------------------- utilities --

def rel(path: Path) -> str:
    return path.resolve().relative_to(ROOT).as_posix()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def num(value: str | None) -> float | None:
    """Parse a CSV number. Empty means missing, never zero."""
    if value is None or value == "":
        return None
    out = float(value)
    return None if math.isnan(out) else out


def boolean(value: str) -> bool:
    if value not in ("true", "false"):
        raise ValueError(f"expected true/false, got {value!r}")
    return value == "true"


def quantile(xs: list[float], q: float) -> float:
    """Linear interpolation between order statistics, as pandas does by default.

    The report's tables were produced with pandas, so matching its definition is
    what lets the claims below be checked to the printed precision.
    """
    s = sorted(xs)
    pos = (len(s) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def mean(xs):
    return statistics.fmean(xs)


def median(xs):
    return statistics.median(xs)


def r(x: float | None, digits: int = 6) -> float | None:
    return None if x is None else round(x, digits)


class Source:
    """Every file a dataset is derived from, with enough to prove which one."""

    def __init__(self) -> None:
        self.files: dict[str, dict] = {}

    def read(self, path: Path) -> list[dict]:
        rows = read_csv(path)
        self.files[rel(path)] = {"path": rel(path), "sha256": sha256(path), "rows": len(rows)}
        return rows

    def read_many(self, pattern: str) -> list[dict]:
        paths = sorted(Path(p) for p in glob.glob(str(ROOT / pattern)))
        if not paths:
            raise SystemExit(f"no files match {pattern}")
        out: list[dict] = []
        for p in paths:
            out.extend(self.read(p))
        return out

    def describe(self) -> list[dict]:
        files = sorted(self.files.values(), key=lambda f: f["path"])
        if len(files) > 12:
            # A pooled raw directory is hundreds of files; list a digest of the
            # set rather than every file, and keep the count and row total.
            h = hashlib.sha256()
            for f in files:
                h.update(f"{f['path']}:{f['sha256']}\n".encode())
            prefix = os.path.commonpath([f["path"] for f in files])
            return [{
                "path": prefix + "/",
                "files": len(files),
                "rows": sum(f["rows"] for f in files),
                "sha256_of_set": h.hexdigest(),
            }]
        return files


def manifest_subset(run: str) -> dict | None:
    """The configuration that decides what a run measured, from its manifest."""
    path = ROOT / "results" / run / "manifest.json"
    if not path.exists():
        return None
    m = json.loads(path.read_text())
    keep = ["run_id", "started_at", "finished_at", "schedulers", "workloads",
            "repetitions", "load_multipliers", "base_seed", "tasks_per_run",
            "worker_processes", "worker_concurrency", "max_in_flight"]
    out = {k: m[k] for k in keep if k in m}
    exec_modes = sorted({
        e.get("config", {}).get("workload", {}).get("exec_mode")
        or e.get("exec_mode") for e in m.get("experiments", []) if isinstance(e, dict)
    } - {None})
    if exec_modes:
        out["exec_modes"] = exec_modes
    return out


# ------------------------------------------------------------------ datasets --

def matrix() -> dict:
    src = Source()
    rows = src.read(ROOT / "results/final-25rep/aggregate.csv")
    out = [{
        "run": x["run_id"],
        "scheduler": x["scheduler"], "workload": x["workload"],
        "repetition": int(x["repetition"]), "seed": int(x["seed"]),
        "latency_p50_s": num(x["latency_p50_s"]),
        "latency_p95_s": num(x["latency_p95_s"]),
        "latency_p99_s": num(x["latency_p99_s"]),
        "wait_max_s": num(x["wait_max_s"]),
        "deadline_miss_rate": num(x["deadline_miss_rate"]),
        "throughput_tasks_per_s": num(x["throughput_tasks_per_s"]),
        "worker_utilization": num(x["worker_utilization"]),
        "jain_fairness_service": num(x["jain_fairness_service"]),
        "tasks_completed": int(x["tasks_completed"]),
        "duplicate_completions": int(x["duplicate_completions"]),
    } for x in rows]
    return {
        "id": "matrix",
        "title": "Main matrix: four policies by four workloads, 25 repetitions",
        "report_section": "4.1",
        "command": 'make merge OUT=final-25rep RUNS="uniform-25rep stable-25rep heavy-25rep"',
        "conditions": {
            "repetitions": 25, "tasks_per_experiment": 4000,
            "worker_slots": 16, "configured_arrival_rate_per_s": 400,
            "approx_capacity_tasks_per_s": 319,
        },
        "caveats": [
            "Arrival rate was configured at 400 tasks/s for every workload, which is "
            "1.25x capacity for uniform and multi_tenant and about 1.28x for heavy_tailed.",
            "The bursty workload ignores the configured rate (its arrivals come from the "
            "burst schedule), so the bursty cells of this run were measured at about 1.57x "
            "capacity, not 1.25x. This is the confound described in report section 3.2. "
            "Compare bursty against the other workloads only in the load sweep, which "
            "derives each workload's rate from measured capacity.",
        ],
        "manifests": [manifest_subset(r) for r in ("uniform-25rep", "stable-25rep", "heavy-25rep")],
        "fields": {
            "latency_p50_s": "s", "latency_p95_s": "s", "latency_p99_s": "s",
            "wait_max_s": "s", "deadline_miss_rate": "fraction",
            "throughput_tasks_per_s": "tasks/s", "worker_utilization": "fraction",
            "jain_fairness_service": "index",
        },
        "sources": src.describe(),
        "rows": out,
    }


def sweep() -> dict:
    src = Source()
    rows = src.read(ROOT / "results/sweep-loads/aggregate.csv")
    out = [{
        "scheduler": x["scheduler"], "workload": x["workload"],
        "offered_load": num(x["offered_load"]), "repetition": int(x["repetition"]),
        "latency_p50_s": num(x["latency_p50_s"]),
        "latency_p99_s": num(x["latency_p99_s"]),
        "wait_max_s": num(x["wait_max_s"]),
        "deadline_miss_rate": num(x["deadline_miss_rate"]),
        "throughput_tasks_per_s": num(x["throughput_tasks_per_s"]),
        "worker_utilization": num(x["worker_utilization"]),
    } for x in rows]
    return {
        "id": "sweep",
        "title": "Offered-load sweep: seven loads from 0.5x to 3x capacity",
        "report_section": "4.4",
        "command": "make sweep RUN=sweep-loads REPETITIONS=3",
        "conditions": {"repetitions": 3, "tasks_per_experiment": 4000, "worker_slots": 16},
        "caveats": [
            "Three repetitions per point. Section 3.3 of the report shows heavy_tailed needs "
            "about 25 for its point estimates to settle, so heavy_tailed points here are noisier "
            "than the others.",
        ],
        "manifests": [manifest_subset("sweep-loads")],
        "fields": {
            "offered_load": "x capacity", "latency_p50_s": "s", "latency_p99_s": "s",
            "wait_max_s": "s", "deadline_miss_rate": "fraction",
            "throughput_tasks_per_s": "tasks/s", "worker_utilization": "fraction",
        },
        "sources": src.describe(),
        "rows": out,
    }


def control() -> dict:
    src = Source()
    rows = src.read(ROOT / "results/control-arm/aggregate.csv")
    out = [{
        "scheduler": x["scheduler"], "workload": x["workload"],
        "offered_load": num(x["offered_load"]), "repetition": int(x["repetition"]),
        "throughput_tasks_per_s": num(x["throughput_tasks_per_s"]),
        "deadline_miss_rate": num(x["deadline_miss_rate"]),
        "wait_max_s": num(x["wait_max_s"]),
        "latency_p99_s": num(x["latency_p99_s"]),
    } for x in rows]
    return {
        "id": "control",
        "title": "No-scheduler control arm against every policy",
        "report_section": "4.6",
        "command": ("go run ./cmd/benchmark --schedulers none,fifo,priority,edf,wfq "
                    "--loads 0.75,1.0,1.25,1.5 --repetitions 3 --run-id control-arm"),
        "conditions": {"repetitions": 3, "tasks_per_experiment": 4000, "worker_slots": 16},
        "caveats": [
            "'none' means workers consume the ingress stream directly, with no scheduler "
            "process. It still routes every task through the same Redis structures, which "
            "is why it differs from the Asynq comparison.",
        ],
        "manifests": [manifest_subset("control-arm")],
        "fields": {"offered_load": "x capacity", "throughput_tasks_per_s": "tasks/s",
                   "deadline_miss_rate": "fraction", "wait_max_s": "s", "latency_p99_s": "s"},
        "sources": src.describe(),
        "rows": out,
    }


def priority_starvation() -> dict:
    src = Source()
    raw = src.read_many("results/final-25rep/raw/*_uniform_rep*.csv")
    groups: dict[tuple[str, int], list[float]] = {}
    for x in raw:
        if x["outcome"] != "completed":
            continue
        groups.setdefault((x["scheduler"], int(x["priority"])), []).append(float(x["latency_s"]))
    out = [{"scheduler": s, "priority": p, "latency_p95_s": r(quantile(v, 0.95)), "tasks": len(v)}
           for (s, p), v in sorted(groups.items())]
    return {
        "id": "priority",
        "title": "p95 latency by priority level, uniform workload, pooled over 25 repetitions",
        "report_section": "4.2",
        "command": "derived from results/final-25rep/raw by scripts/export_site_data.py",
        "conditions": {"repetitions": 25, "tasks_pooled_per_scheduler": 100000},
        "caveats": ["Pooled across repetitions: one quantile over 100,000 tasks per scheduler, "
                    "not a mean of per-repetition quantiles."],
        "fields": {"latency_p95_s": "s", "priority": "0 (lowest) to 4 (highest)"},
        "sources": src.describe(),
        "rows": out,
    }


def tenants() -> dict:
    out = []
    srcs = []
    for label, path, run in [
        ("main", "results/final-25rep/tenants.csv", "final-25rep"),
        ("sleep", "results/rw-tenant-sleep/tenants.csv", "rw-tenant-sleep"),
        ("cpu", "results/rw-tenant-cpu/tenants.csv", "rw-tenant-cpu"),
    ]:
        src = Source()
        for x in src.read(ROOT / path):
            if x["workload"] != "multi_tenant":
                continue
            out.append({
                "series": label, "scheduler": x["scheduler"], "tenant": x["tenant"],
                "repetition": int(x["repetition"]),
                "latency_p95_s": num(x["latency_p95_s"]),
                "service_share": num(x["service_share"]),
                "completed_tasks": int(x["completed_tasks"]),
            })
        srcs.extend(src.describe())
    return {
        "id": "tenants",
        "title": "Per-tenant p95 latency under a 90/3/3/2/2 tenant skew",
        "report_section": "4.3, 5.3",
        "command": "see report appendix A (main matrix; rw-tenant-sleep and rw-tenant-cpu)",
        "conditions": {
            "main": {"repetitions": 25, "worker_slots": 16, "exec_mode": "sleep"},
            "sleep": {"repetitions": 5, "worker_slots": 8, "exec_mode": "sleep"},
            "cpu": {"repetitions": 5, "worker_slots": 8, "exec_mode": "cpu (chained SHA-256)"},
        },
        "caveats": [
            "Tenant A submits 90% of the work; B to E submit 2-3% each.",
            "'main' is 16 slots at 25 repetitions; 'sleep' and 'cpu' are the 8-slot real-work "
            "comparison at 5 repetitions. Do not compare absolute values across the two configurations.",
        ],
        "fields": {"latency_p95_s": "s", "service_share": "fraction"},
        "sources": srcs,
        "rows": out,
    }


def realwork() -> dict:
    out, srcs = [], []
    for mode in ("sleep", "cpu"):
        src = Source()
        for x in src.read(ROOT / f"results/rw-heavy25-{mode}/aggregate.csv"):
            out.append({
                "exec_mode": mode, "scheduler": x["scheduler"], "repetition": int(x["repetition"]),
                "deadline_miss_rate": num(x["deadline_miss_rate"]),
                "latency_p99_s": num(x["latency_p99_s"]),
                "exec_mean_s": num(x["exec_mean_s"]),
                "wait_max_s": num(x["wait_max_s"]),
            })
        srcs.extend(src.describe())
    return {
        "id": "realwork",
        "title": "Simulated (sleep) against real CPU execution, heavy-tailed workload",
        "report_section": "5.3",
        "command": ("go run ./cmd/benchmark --config experiments/realwork.yaml "
                    "--exec-mode cpu --repetitions 25 --run-id realwork"),
        "conditions": {"repetitions": 25, "tasks_per_experiment": 2000, "worker_slots": 8,
                       "offered_load": 1.25, "cores": 10},
        "caveats": ["Real execution inflates measured task time by 1.5-1.9x because workers share "
                    "the machine with Redis and the Go runtime; the 'exec_mean_s' column shows it."],
        "manifests": [manifest_subset("rw-heavy25-sleep"), manifest_subset("rw-heavy25-cpu")],
        "fields": {"deadline_miss_rate": "fraction", "latency_p99_s": "s",
                   "exec_mean_s": "s", "wait_max_s": "s"},
        "sources": srcs,
        "rows": out,
    }


def ceiling() -> dict:
    out, srcs = [], []
    for fenced, path in [(False, "results/ceiling-unfenced/ceiling.csv"),
                         (True, "results/ceiling-fenced/ceiling.csv")]:
        src = Source()
        for x in src.read(ROOT / path):
            if boolean(x["fenced"]) != fenced:
                raise SystemExit(f"{path}: fenced column disagrees with the file it is in")
            out.append({
                "fenced": fenced, "phase": x["phase"], "policy": x["policy"],
                "batch": int(x["batch"]), "tasks": int(x["tasks"]),
                "rate_tasks_per_s": num(x["rate_tasks_per_s"]),
            })
        srcs.extend(src.describe())
    return {
        "id": "ceiling",
        "title": "The scheduler's own throughput ceiling, fence inert and live",
        "report_section": "4.6",
        "command": ("go run ./cmd/schedbench --tasks 20000 --epoch 0 --csv results/ceiling-unfenced/ceiling.csv\n"
                    "go run ./cmd/schedbench --tasks 20000 --epoch 1 --csv results/ceiling-fenced/ceiling.csv"),
        "conditions": {"tasks_per_phase": 20000, "workers": 0},
        "caveats": ["Each cell is a single run. Differences under about 2% are inside this "
                    "measurement's noise: dispatch came out faster fenced than unfenced."],
        "fields": {"rate_tasks_per_s": "tasks/s",
                   "phase": "admit: ingress to pending set; dispatch: pending set to exec stream; "
                            "engine: both loops together, the real ceiling"},
        "sources": srcs,
        "rows": out,
    }


def failover() -> dict:
    out, srcs = [], []
    for run in ("failover", "failover-ttl2s", "failover-ttl10s"):
        src = Source()
        for x in src.read(ROOT / f"results/{run}/failover.csv"):
            recovered = boolean(x["recovered"])
            outage = num(x["outage_ms"])
            # -1 is the harness's censoring sentinel. It is a fact about the
            # trial ("never recovered"), not a duration, so it must never reach
            # a chart as a number.
            censored = (outage is not None and outage < 0) or not recovered
            out.append({
                "run": run, "arm": x["arm"], "rep": int(x["rep"]), "policy": x["policy"],
                "replicas": int(x["replicas"]),
                "lease_ttl_ms": int(x["lease_ttl_ms"]), "lease_renew_ms": int(x["lease_renew_ms"]),
                "outage_ms": None if censored else outage,
                "censored": censored,
                "max_gap_ms": num(x["max_gap_ms"]),
                "tasks": int(x["tasks"]), "completed": int(x["completed"]),
                "unfinished": int(x["tasks"]) - int(x["completed"]),
                "duplicate_completions": int(x["duplicate_completions"]),
                "epoch_inversions": int(x["epoch_inversions"]),
                "leaders_before_kill": int(x["leaders_before_kill"]),
                "pause_ms": num(x["pause_ms"]) if x["arm"] == "pause" else None,
                "leaders_after_resume": int(x["leaders_after_resume"]) if x["arm"] == "pause" else None,
                "fenced_operations": int(x["fenced_operations"]) if x["arm"] == "pause" else None,
                "suspect": boolean(x["suspect"]),
            })
        srcs.extend(src.describe())
    suspect = [o for o in out if o["suspect"]]
    if suspect:
        raise SystemExit(f"{len(suspect)} failover trials are flagged suspect; the report excludes "
                         "them and so must the site. Re-run or drop them explicitly.")
    return {
        "id": "failover",
        "title": "Dispatch outage when the leading scheduler is stopped",
        "report_section": "4.7, 4.8",
        "command": ("make failover FAILOVER_REPETITIONS=15 RUN=failover\n"
                    "go run ./cmd/failoverbench --arms crash --repetitions 12 --seed 2 "
                    "--lease-ttl 2s --lease-renew 600ms --run-id failover-ttl2s\n"
                    "go run ./cmd/failoverbench --arms crash --repetitions 12 --seed 3 "
                    "--lease-ttl 10s --lease-renew 3s --run-id failover-ttl10s"),
        "conditions": {"tasks_per_trial": 4000, "submission_rate_per_s": 400, "exec_ms": 20,
                       "worker_slots": 32, "policy": "wfq", "standby_retry_ms": 1000},
        "caveats": [
            "outage_ms is null for censored trials: no dispatch ever followed the kill. "
            "Those trials are drawn as censored, never averaged.",
            "All replicas share one host, so these outages are a lower bound for a "
            "multi-node deployment.",
            "Failover is measured in steady state, not under a backlog.",
        ],
        "fields": {"outage_ms": "ms", "max_gap_ms": "ms", "pause_ms": "ms",
                   "lease_ttl_ms": "ms", "lease_renew_ms": "ms"},
        "sources": srcs,
        "rows": out,
    }


def storefault() -> dict:
    src = Source()
    out = []
    for x in src.read(ROOT / "results/storefault/storefault.csv"):
        out.append({
            "arm": x["arm"], "rep": int(x["rep"]),
            "failover_ms": num(x["failover_ms"]),
            "epoch_reused": boolean(x["epoch_reused"]),
            "reused_epoch": int(x["reused_epoch"]),
            "epoch_regressed": boolean(x["epoch_regressed"]),
            "submitted": int(x["submitted"]), "completed": int(x["completed"]),
            "tasks_lost": int(x["tasks_lost"]),
            "ingress_lost_entries": int(x["ingress_lost_entries"]),
            "duplicate_completions": int(x["duplicate_completions"]),
            "epoch_inversions": int(x["epoch_inversions"]),
            "notes": x["notes"] or None,
        })
    return {
        "id": "storefault",
        "title": "What a Redis Sentinel failover does to the fencing token",
        "report_section": "4.9",
        "command": "make storefault STOREFAULT_REPETITIONS=10 RUN=storefault",
        "conditions": {"tasks_per_trial": 2000, "lease_ttl_ms": 2000, "redis_replicas": 2,
                       "sentinels": 3, "fault_window_ms": 4000},
        "caveats": [
            "Redis Sentinel with asynchronous replication, all processes on one host. Not a "
            "claim about Redis Cluster, AOF with appendfsync always, or a consensus store.",
            "epoch_reused is observed continuously by the harness, outside Redis; "
            "epoch_regressed is read from the promoted node afterwards and under-reports.",
        ],
        "fields": {"failover_ms": "ms", "tasks_lost": "tasks", "submitted": "tasks",
                   "completed": "tasks"},
        "sources": src.describe(),
        "rows": out,
    }


def baseline() -> dict:
    src = Source()
    out = []
    for x in src.read(ROOT / "results/baseline/baseline.csv"):
        out.append({
            "system": x["system"], "rep": int(x["rep"]), "exec_ms": int(x["exec_ms"]),
            "tasks": int(x["tasks"]), "completed": int(x["completed"]),
            "concurrency": int(x["concurrency"]),
            "throughput_per_s": num(x["throughput_per_s"]),
            "latency_p99_ms": num(x["latency_p99_ms"]),
        })
    short = [o for o in out if o["completed"] < o["tasks"]]
    if short:
        raise SystemExit("a baseline run completed fewer tasks than it was given")
    return {
        "id": "baseline",
        "title": "This queue against Asynq on identical work",
        "report_section": "4.10",
        "command": "make baseline BASELINE_REPETITIONS=5 RUN=baseline",
        "conditions": {"repetitions": 5, "tasks_per_run": 8000, "concurrency": 32,
                       "this_queue_policy": "fifo, leader election off", "asynq_version": "v0.24.1"},
        "caveats": [
            "Same Redis, run one system after the other, order alternated across repetitions.",
            "Tasks are pre-enqueued, so this measures drain time, not a producer-limited rate.",
            "One machine, one axis (throughput against task duration). Asynq has features this "
            "project does not, and the reverse.",
        ],
        "fields": {"exec_ms": "ms", "throughput_per_s": "tasks/s", "latency_p99_ms": "ms"},
        "sources": src.describe(),
        "rows": out,
    }


# Experiments that exist in code but whose results are not published. The site
# renders these as pending, with the reason, and never with numbers.
PENDING = [
    {
        "id": "partition",
        "title": "A scheduler partitioned from a Redis that stays up",
        "harness": "cmd/partitionbench",
        "commands": ["make partition PARTITION_REPETITIONS=12", "make partition-sweep RUN=partition-sweep"],
        "status": "pending",
        "reason": (
            "The harness is written and tested. A first named-arm run exists but predates the "
            "host-sleep detection added to the harness, and the partition-length sweep was "
            "interrupted twice by the laptop sleeping on battery power. Neither is published; "
            "both are to be re-run on mains power."
        ),
    },
]


def inventory(datasets: dict[str, dict]) -> dict:
    """Experiment and task counts recomputed from the data, run by run."""
    def agg(run: str) -> tuple[int, int]:
        rows = read_csv(ROOT / f"results/{run}/aggregate.csv")
        return len(rows), sum(int(x["tasks_planned"]) for x in rows)

    def pair(*runs: str) -> tuple[int, int]:
        e = t = 0
        for run in runs:
            a, b = agg(run)
            e, t = e + a, t + b
        return e, t

    fo = [o for o in datasets["failover"]["rows"]]
    rows = [
        ("Main matrix, 25 repetitions", "final-25rep", *pair("final-25rep")),
        ("Offered-load sweep", "sweep-loads", *pair("sweep-loads")),
        ("No-scheduler control", "control-arm", *pair("control-arm")),
        ("Real work, heavy-tailed, 25 reps", "rw-heavy25-sleep, rw-heavy25-cpu",
         *pair("rw-heavy25-sleep", "rw-heavy25-cpu")),
        ("Real work, initial comparison", "realwork-sleep, realwork-cpu",
         *pair("realwork-sleep", "realwork-cpu")),
        ("Real work, tenant isolation", "rw-tenant-sleep, rw-tenant-cpu",
         *pair("rw-tenant-sleep", "rw-tenant-cpu")),
        ("Scheduler failover, five arms", "failover",
         sum(1 for o in fo if o["run"] == "failover"),
         sum(o["tasks"] for o in fo if o["run"] == "failover")),
        ("Failover, lease-TTL sensitivity", "failover-ttl2s, failover-ttl10s",
         sum(1 for o in fo if o["run"] != "failover"),
         sum(o["tasks"] for o in fo if o["run"] != "failover")),
        # storefault.csv has no planned-count column; 2,000 is the harness
        # default (--tasks in cmd/storefaultbench) and what every clean trial
        # shows it submitted.
        ("Store failure under Sentinel, four arms", "storefault",
         len(datasets["storefault"]["rows"]), 2000 * len(datasets["storefault"]["rows"])),
        ("External baseline against Asynq", "baseline",
         len(datasets["baseline"]["rows"]), sum(o["tasks"] for o in datasets["baseline"]["rows"])),
    ]
    out = [{"name": n, "runs": run, "experiments": e, "tasks": t} for n, run, e, t in rows]
    return {
        "id": "inventory",
        "title": "Every experiment counted, from the data",
        "report_section": "Appendix B",
        "rows": out,
        "total_experiments": sum(o["experiments"] for o in out),
        "total_tasks": sum(o["tasks"] for o in out),
    }


# -------------------------------------------------------------------- claims --

def cell_mean(rows, metric, **where):
    vals = [x[metric] for x in rows if all(x[k] == v for k, v in where.items())]
    if not vals:
        raise SystemExit(f"no rows for {where}")
    return mean(vals)


def claims(d: dict[str, dict]) -> list[dict]:
    """Numbers the report prints, recomputed. Each is (section, text, printed, computed, tol)."""
    out: list[dict] = []

    def claim(section, text, printed, computed, tol):
        out.append({"section": section, "claim": text, "printed": printed,
                    "computed": r(computed, 4), "tolerance": tol,
                    "ok": abs(computed - printed) <= tol})

    # 4.4: spread between best and worst policy p99, uniform.
    sw = d["sweep"]["rows"]
    for load, printed in [(0.5, 0.1), (0.75, 0.3), (1.0, 93.1), (1.25, 55.5),
                          (1.5, 28.4), (2.0, 2.1), (3.0, 1.1)]:
        m = [cell_mean(sw, "latency_p99_s", scheduler=s, workload="uniform", offered_load=load)
             for s in POLICIES]
        claim("4.4", f"p99 spread across policies at {load}x, uniform (%)",
              printed, (max(m) - min(m)) / min(m) * 100, 0.05)

    # 4.5: EDF under overload, heavy-tailed. The ratios are computed from the
    # unrounded rates; dividing printed three-decimal values gave 129x, which
    # is the error section 5.1 records.
    def ht(s, load):
        return cell_mean(sw, "deadline_miss_rate", scheduler=s, workload="heavy_tailed", offered_load=load)
    claim("4.5", "EDF advantage over FIFO at 1.25x (x)", 117, ht("fifo", 1.25) / ht("edf", 1.25), 0.5)
    claim("4.5", "EDF advantage over FIFO at 3x (x)", 1.16, ht("fifo", 3.0) / ht("edf", 3.0), 0.005)
    for load in (0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0):
        best = min(POLICIES, key=lambda s: ht(s, load))
        claim("4.5", f"EDF misses no more than FIFO at {load}x (1 = true)", 1,
              1.0 if ht("edf", load) <= ht("fifo", load) else 0.0, 0)
        if load <= 1.5:
            claim("4.5", f"EDF has the lowest miss rate at {load}x (1 = true)", 1,
                  1.0 if ht("edf", load) <= ht(best, load) else 0.0, 0)
        else:
            claim("4.5", f"strict priority beats EDF at {load}x (1 = true)", 1,
                  1.0 if ht("priority", load) < ht("edf", load) else 0.0, 0)

    # 4.1 main matrix, a representative cell from each workload.
    mx = d["matrix"]["rows"]
    for wl, s, metric, printed, tol in [
        ("uniform", "fifo", "latency_p99_s", 2.917, 0.0005),
        ("heavy_tailed", "edf", "deadline_miss_rate", 0.001, 0.0005),
        ("heavy_tailed", "edf", "throughput_tasks_per_s", 257.6, 0.05),
        ("heavy_tailed", "fifo", "throughput_tasks_per_s", 282.7, 0.05),
        ("multi_tenant", "wfq", "deadline_miss_rate", 0.727, 0.0005),
        ("bursty", "fifo", "deadline_miss_rate", 0.943, 0.0005),
    ]:
        claim("4.1", f"{s} {wl} {metric}", printed, cell_mean(mx, metric, scheduler=s, workload=wl), tol)

    # 4.2 starvation.
    pr = {(x["scheduler"], x["priority"]): x["latency_p95_s"] for x in d["priority"]["rows"]}
    claim("4.2", "priority policy, p95 of priority 0 (s)", 4.416, pr[("priority", 0)], 0.0005)
    claim("4.2", "priority policy, spread between priority 0 and 4 (x)", 60.4,
          pr[("priority", 0)] / pr[("priority", 4)], 0.05)

    # 4.3 tenant isolation.
    tn = [x for x in d["tenants"]["rows"] if x["series"] == "main"]
    def small_over_a(series_rows, s):
        a = mean([x["latency_p95_s"] for x in series_rows if x["scheduler"] == s and x["tenant"] == "A"])
        small = mean([x["latency_p95_s"] for x in series_rows if x["scheduler"] == s and x["tenant"] != "A"])
        return small, a
    small, a = small_over_a(tn, "wfq")
    claim("4.3", "WFQ small-tenant p95 / tenant A p95", 0.027, small / a, 0.0005)
    claim("4.3", "WFQ small-tenant p95 (s)", 0.078, small, 0.0005)

    # 4.6 ceiling and control arm.
    ce = d["ceiling"]["rows"]
    def ceil_mean(fenced, phase):
        return mean([x["rate_tasks_per_s"] for x in ce if x["fenced"] == fenced and x["phase"] == phase])
    claim("4.6", "admission, fence inert (tasks/s)", 15204, ceil_mean(False, "admit"), 0.5)
    claim("4.6", "end to end, fence inert (tasks/s)", 11381, ceil_mean(False, "engine"), 0.5)
    claim("4.6", "end to end, fence live (tasks/s)", 11325, ceil_mean(True, "engine"), 0.5)
    best = max(x["rate_tasks_per_s"] for x in ce if not x["fenced"] and x["phase"] == "dispatch")
    claim("4.6", "dispatch, best case, fence inert (tasks/s)", 261727, best, 0.5)
    co = d["control"]["rows"]
    for wl, printed in [("uniform", 1.5), ("bursty", 1.6), ("heavy_tailed", 1.2), ("multi_tenant", 1.6)]:
        f = cell_mean(co, "throughput_tasks_per_s", scheduler="fifo", workload=wl, offered_load=1.25)
        n = cell_mean(co, "throughput_tasks_per_s", scheduler="none", workload=wl, offered_load=1.25)
        claim("4.6", f"FIFO overhead against no scheduler, {wl} at 1.25x (%)", printed, (1 - f / n) * 100, 0.05)
    edf = cell_mean(co, "deadline_miss_rate", scheduler="edf", workload="heavy_tailed", offered_load=1.25)
    none = cell_mean(co, "deadline_miss_rate", scheduler="none", workload="heavy_tailed", offered_load=1.25)
    claim("4.6", "EDF reduction in deadline misses against no scheduler (x)", 118, none / edf, 0.5)

    # 4.7 / 4.8 failover.
    fo = d["failover"]["rows"]
    def arm(run, name):
        return [x for x in fo if x["run"] == run and x["arm"] == name]
    for name, printed in [("graceful", 663), ("crash", 4362), ("pause", 3879)]:
        claim("4.7", f"median outage, {name} arm (ms)", printed,
              median([x["outage_ms"] for x in arm("failover", name)]), 0.5)
    claim("4.7", "single-replica trials that recovered", 0,
          sum(1 for x in arm("failover", "single") if not x["censored"]), 0)
    claim("4.7", "tasks never finished, single arm", 37866,
          sum(x["unfinished"] for x in arm("failover", "single")), 0)
    claim("4.7", "epoch inversions across all failover trials", 0,
          sum(x["epoch_inversions"] for x in fo), 0)
    claim("4.7", "median outage, crash at 2 s TTL (ms)", 1850,
          median([x["outage_ms"] for x in arm("failover-ttl2s", "crash")]), 0.5)
    claim("4.7", "median outage, crash at 10 s TTL (ms)", 8259,
          median([x["outage_ms"] for x in arm("failover-ttl10s", "crash")]), 0.5)
    claim("4.8", "pause trials refused by the fence", 11,
          sum(x["fenced_operations"] for x in arm("failover", "pause")), 0)

    # 4.9 store failure.
    sf = d["storefault"]["rows"]
    for name, reused, lost in [("clean", 0, 0), ("lagging", 10, 10010),
                               ("epochwait", 10, 10009), ("guarded", 0, 0)]:
        rows = [x for x in sf if x["arm"] == name]
        claim("4.9", f"{name}: trials issuing one token to two leaders", reused,
              sum(1 for x in rows if x["epoch_reused"]), 0)
        claim("4.9", f"{name}: acknowledged tasks destroyed", lost, sum(x["tasks_lost"] for x in rows), 0)

    # 4.10 baseline, medians as the report tabulates them.
    bl = d["baseline"]["rows"]
    for ms, tq, aq in [(0, 8075.4, 12477.7), (2, 8611.4, 12181.4), (5, 5809.5, 5522.2), (20, 1492.1, 1465.4)]:
        claim("4.10", f"this queue, median throughput at {ms} ms (tasks/s)", tq,
              median([x["throughput_per_s"] for x in bl if x["system"] == "taskqueue" and x["exec_ms"] == ms]), 0.05)
        claim("4.10", f"Asynq, median throughput at {ms} ms (tasks/s)", aq,
              median([x["throughput_per_s"] for x in bl if x["system"] == "asynq" and x["exec_ms"] == ms]), 0.05)

    # 5.3 real work.
    rw = d["realwork"]["rows"]
    for mode, printed in [("sleep", 0.099), ("cpu", 0.829)]:
        claim("5.3", f"EDF deadline miss rate, {mode}", printed,
              cell_mean(rw, "deadline_miss_rate", scheduler="edf", exec_mode=mode), 0.0005)
    claim("5.3", "priority deadline miss rate, cpu", 0.643,
          cell_mean(rw, "deadline_miss_rate", scheduler="priority", exec_mode="cpu"), 0.0005)
    ts = [x for x in d["tenants"]["rows"] if x["series"] in ("sleep", "cpu")]
    for series, printed in [("sleep", 31.8), ("cpu", 39.9)]:
        rows = [x for x in ts if x["series"] == series and x["tenant"] != "A"]
        fifo = mean([x["latency_p95_s"] for x in rows if x["scheduler"] == "fifo"])
        wfq = mean([x["latency_p95_s"] for x in rows if x["scheduler"] == "wfq"])
        claim("5.3", f"WFQ small-tenant advantage over FIFO, {series} (x)", printed, fifo / wfq, 0.05)

    # Appendix B.
    inv = d["inventory"]
    claim("B", "total experiments", 1463, inv["total_experiments"], 0)
    claim("B", "total tasks", 5436000, inv["total_tasks"], 0)
    return out


# ---------------------------------------------------------------------- main --

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", default=str(ROOT / "web/public/data"))
    ap.add_argument("--check", action="store_true", help="verify the claims and write nothing")
    args = ap.parse_args()

    if not (ROOT / "results/final-25rep/aggregate.csv").exists():
        print("results/ is not populated on this machine. The committed JSON under "
              "web/public/data is what the site uses; nothing to regenerate.", file=sys.stderr)
        return 2

    builders = [matrix, sweep, control, priority_starvation, tenants, realwork,
                ceiling, failover, storefault, baseline]
    datasets: dict[str, dict] = {}
    for build in builders:
        ds = build()
        ds["schema_version"] = SCHEMA_VERSION
        ds["status"] = "published"
        datasets[ds["id"]] = ds
    datasets["inventory"] = inventory(datasets)
    checked = claims(datasets)

    failed = [c for c in checked if not c["ok"]]
    for c in checked:
        mark = "ok  " if c["ok"] else "FAIL"
        print(f"{mark} §{c['section']:<5} {c['claim']}: printed {c['printed']}, computed {c['computed']}")
    print(f"\n{len(checked) - len(failed)}/{len(checked)} report claims reproduce from the raw data")
    if failed:
        print("refusing to export: the report and the data disagree", file=sys.stderr)
        return 1
    if args.check:
        return 0

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    index = {"schema_version": SCHEMA_VERSION, "generated_by": "scripts/export_site_data.py",
             "datasets": [], "pending": PENDING, "claims": checked,
             "inventory": datasets.pop("inventory")}
    for ds_id, ds in datasets.items():
        path = out / f"{ds_id}.json"
        path.write_text(json.dumps(ds, sort_keys=True, separators=(",", ":")) + "\n")
        index["datasets"].append({"id": ds_id, "title": ds["title"], "file": f"{ds_id}.json",
                                  "report_section": ds["report_section"], "rows": len(ds["rows"]),
                                  "bytes": path.stat().st_size})
    (out / "index.json").write_text(json.dumps(index, sort_keys=True, indent=1) + "\n")
    total = sum(d["bytes"] for d in index["datasets"])
    print(f"wrote {len(index['datasets'])} datasets ({total / 1024:.0f} KiB) and index.json to {rel(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
