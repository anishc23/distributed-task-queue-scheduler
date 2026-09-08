#!/usr/bin/env python3
"""Combine several benchmark runs into one directory for analysis.

Useful when a matrix was filled in across several invocations — for example
when one workload needed more repetitions than the others and was re-run on its
own. The result is a directory that `plot_results.py` can chart as a unit.

Usage:
    python3 scripts/merge_runs.py --out final-25rep uniform-25rep stable-25rep heavy-25rep
    python3 scripts/plot_results.py --run final-25rep

What it does NOT do is hide where the numbers came from:

  * It refuses to merge runs whose configurations differ in any way that could
    affect results, and prints the offending fields. Only the set of workloads
    and schedulers actually exercised may differ, since that is the whole point.
  * Every copied row keeps the `run_id` of the run that produced it, so
    provenance survives into the merged CSVs.
  * The merged manifest records each source run and states plainly that the
    directory is an assembled view rather than a single benchmark invocation.

It will refuse to merge runs that cover the same scheduler/workload cell twice,
because silently averaging two runs of the same cell is exactly the kind of
mixing the benchmark harness is designed to prevent.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from collections import Counter
from pathlib import Path

# Configuration that materially affects results. Two runs must agree on all of
# it before their rows may sit in the same directory.
MATERIAL = [
    ("repetitions",), ("worker_processes",), ("worker_concurrency",),
    ("max_in_flight",), ("base_seed",), ("tasks_per_run",),
    ("config", "Workload", "ArrivalRatePerSec"),
    ("config", "Workload", "MaxRetries"),
    ("config", "Workload", "Exec"),
    ("config", "Workload", "Priority"),
    ("config", "Workload", "Deadline"),
    ("config", "Workload", "Tenants"),
    ("config", "Workload", "SkewTenants"),
    ("config", "Workload", "Burst"),
    ("config", "Workload", "HeavyTail"),
    ("config", "Scheduler", "TenantWeights"),
    ("config", "Scheduler", "DefaultTenantWeight"),
    ("config", "Scheduler", "MaxInFlight"),
    ("config", "Scheduler", "DispatchBatch"),
    ("config", "Worker", "Concurrency"),
    ("config", "Worker", "FailRate"),
    ("config", "Worker", "FailBeforeAckRate"),
    ("config", "Recovery", "MinIdle"),
    ("config", "Recovery", "MaxRetries"),
]

# Newer manifests use snake_case config keys; older ones use Go field names.
ALIASES = {
    "Workload": "workload", "Scheduler": "scheduler", "Worker": "worker",
    "Recovery": "recovery", "ArrivalRatePerSec": "arrival_rate_per_sec",
    "MaxRetries": "max_retries", "Exec": "exec", "Priority": "priority",
    "Deadline": "deadline", "Tenants": "tenants", "SkewTenants": "skew_tenants",
    "Burst": "burst", "HeavyTail": "heavy_tail", "TenantWeights": "tenant_weights",
    "DefaultTenantWeight": "default_tenant_weight", "MaxInFlight": "max_in_flight",
    "DispatchBatch": "dispatch_batch", "Concurrency": "concurrency",
    "FailRate": "fail_rate", "FailBeforeAckRate": "fail_before_ack_rate",
    "MinIdle": "min_idle",
}


def dig(obj, path):
    """Follow a key path, accepting either naming convention at each step."""
    for key in path:
        if not isinstance(obj, dict):
            return None
        if key in obj:
            obj = obj[key]
        elif ALIASES.get(key) in obj:
            obj = obj[ALIASES[key]]
        else:
            return None
    return obj


def load_manifest(run_dir: Path) -> dict:
    path = run_dir / "manifest.json"
    if not path.exists():
        raise SystemExit(f"{path} not found; is {run_dir} a benchmark run directory?")
    return json.loads(path.read_text())


def check_compatible(manifests: dict[str, dict]) -> None:
    names = list(manifests)
    ref = names[0]
    problems = []
    for name in names[1:]:
        for path in MATERIAL:
            a, b = dig(manifests[ref], path), dig(manifests[name], path)
            if a != b:
                problems.append(f"  {'.'.join(path)}: {ref}={a!r}  {name}={b!r}")
    if problems:
        raise SystemExit(
            "refusing to merge: these runs differ in configuration that affects results\n"
            + "\n".join(problems)
        )


def check_no_overlap(rows: list[dict]) -> None:
    cells = Counter((r["workload"], r["scheduler"], r["repetition"]) for r in rows)
    dupes = {k: n for k, n in cells.items() if n > 1}
    if dupes:
        listed = ", ".join(f"{w}/{s} rep {rep}" for (w, s, rep) in list(dupes)[:5])
        raise SystemExit(
            f"refusing to merge: {len(dupes)} scheduler/workload/repetition cells appear "
            f"in more than one run ({listed}). Averaging duplicated cells would mix runs; "
            "drop one of the overlapping runs instead."
        )


def concat_csv(runs: list[Path], out: Path, name: str) -> int:
    header, rows = None, []
    for run in runs:
        src = run / name
        if not src.exists():
            raise SystemExit(f"{src} not found")
        with src.open() as f:
            reader = csv.reader(f)
            head = next(reader)
            if header is None:
                header = head
            elif head != header:
                raise SystemExit(f"refusing to merge: {name} has a different schema in {run.name}")
            rows.extend(reader)
    with out.joinpath(name).open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+", help="run ids (or paths) to merge")
    ap.add_argument("--out", required=True, help="run id of the merged directory")
    ap.add_argument("--results-dir", default="results", help="root results directory")
    ap.add_argument("--force", action="store_true", help="overwrite the output directory if it exists")
    args = ap.parse_args()

    root = Path(args.results_dir)
    runs = [Path(r) if Path(r).is_dir() else root / r for r in args.runs]
    for r in runs:
        if not r.is_dir():
            raise SystemExit(f"{r} is not a directory")

    manifests = {r.name: load_manifest(r) for r in runs}
    check_compatible(manifests)
    print(f"configuration check: {len(runs)} runs agree on everything that affects results")

    out = root / args.out
    if out.exists():
        if not args.force:
            raise SystemExit(f"{out} already exists; pass --force to replace it")
        shutil.rmtree(out)
    (out / "raw").mkdir(parents=True)

    n_agg = concat_csv(runs, out, "aggregate.csv")
    with (out / "aggregate.csv").open() as f:
        check_no_overlap(list(csv.DictReader(f)))
    print(f"aggregate.csv: {n_agg} experiments")
    print(f"tenants.csv:   {concat_csv(runs, out, 'tenants.csv')} tenant rows")

    copied = 0
    for run in runs:
        for src in sorted((run / "raw").glob("*.csv")):
            dest = out / "raw" / src.name
            if dest.exists():
                raise SystemExit(f"refusing to merge: duplicate raw file {src.name}")
            shutil.copy2(src, dest)
            copied += 1
    print(f"raw/:          {copied} task-level files")

    for run in runs:
        dl = run / "dead_letters"
        if dl.is_dir():
            (out / "dead_letters").mkdir(exist_ok=True)
            for src in dl.glob("*.json"):
                shutil.copy2(src, out / "dead_letters" / src.name)

    ref = manifests[runs[0].name]
    (out / "manifest.json").write_text(json.dumps({
        "run_id": args.out,
        "assembled": True,
        "note": (
            "Assembled view, not a single benchmark invocation. Produced by "
            "scripts/merge_runs.py, which verified that the source runs agree on every "
            "configuration field affecting results. Each row keeps the run_id of the run "
            "that produced it."
        ),
        "source_runs": {
            name: {
                "workloads": m.get("workloads"), "schedulers": m.get("schedulers"),
                "repetitions": m.get("repetitions"), "base_seed": m.get("base_seed"),
                "tasks_per_run": m.get("tasks_per_run"),
                "started_at": m.get("started_at"), "finished_at": m.get("finished_at"),
                "experiments": len(m.get("experiments", [])),
            } for name, m in manifests.items()
        },
        "repetitions": ref.get("repetitions"),
        "worker_processes": ref.get("worker_processes"),
        "worker_concurrency": ref.get("worker_concurrency"),
        "max_in_flight": ref.get("max_in_flight"),
        "base_seed": ref.get("base_seed"),
        "tasks_per_run": ref.get("tasks_per_run"),
        "config": ref.get("config"),
    }, indent=2) + "\n")

    print(f"\nmerged into {out}")
    print(f"chart it with:\n  python3 scripts/plot_results.py --run {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
