#!/usr/bin/env python3
"""Plot scheduler behaviour as a function of offered load.

Reads a benchmark run produced with `--loads`, where each row carries the
offered load it was measured at, and draws one curve per scheduler against load.

Usage:
    python3 scripts/plot_load_sweep.py --run sweep-loads
    python3 scripts/plot_load_sweep.py --run-dir results/sweep-loads --format png

Offered load is the mean arrival rate divided by service capacity
(worker slots / mean execution time). Below 1.0 the system keeps up; above 1.0 a
backlog grows for as long as arrivals continue. The interesting behaviour - and
the point of the sweep - is what happens as the curves cross 1.0, because that
is where the scheduling policy starts deciding who waits rather than merely
choosing an order for work that would have run promptly anyway.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns
except ImportError as exc:  # pragma: no cover
    sys.exit(f"missing Python dependency: {exc}\nInstall with: make python-deps")

SCHEDULER_ORDER = ["fifo", "priority", "edf", "wfq"]
WORKLOAD_ORDER = ["uniform", "bursty", "heavy_tailed", "multi_tenant"]
PALETTE = {"fifo": "#4C72B0", "priority": "#DD8452", "edf": "#55A868", "wfq": "#C44E52"}
MARKERS = {"fifo": "o", "priority": "s", "edf": "^", "wfq": "D"}
WORKLOAD_LABELS = {
    "uniform": "Uniform", "bursty": "Bursty",
    "heavy_tailed": "Heavy-tailed", "multi_tenant": "Multi-tenant skew",
}


def setup_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update({
        "figure.dpi": 110, "savefig.dpi": 200, "savefig.bbox": "tight",
        "axes.titlesize": 14, "axes.labelsize": 12,
        "xtick.labelsize": 10, "ytick.labelsize": 10, "legend.fontsize": 10,
        "font.family": "DejaVu Sans",
    })


def ordered(values, order):
    present = set(values)
    return [v for v in order if v in present] + sorted(present - set(order))


def save(fig, out_dir: Path, name: str, formats: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"{name}.{fmt}"
        fig.savefig(path, format=fmt)
        print(f"  wrote {path}")
    plt.close(fig)


def load_grid(df, metric, ylabel, title, out_dir, formats, name,
              logy=False, ylim=None, hline=None):
    """One panel per workload, one curve per scheduler, x = offered load."""
    workloads = ordered(df["workload"].unique(), WORKLOAD_ORDER)
    schedulers = ordered(df["scheduler"].unique(), SCHEDULER_ORDER)

    fig, axes = plt.subplots(1, len(workloads), figsize=(5.2 * len(workloads), 5.0),
                             sharey=True, squeeze=False)
    axes = axes[0]

    for ax, wl in zip(axes, workloads):
        sub = df[df["workload"] == wl]
        for sched in schedulers:
            s = sub[sub["scheduler"] == sched].groupby("offered_load")[metric].agg(["mean", "std"])
            s = s.sort_index()
            if s.empty:
                continue
            ax.errorbar(s.index, s["mean"], yerr=s["std"].fillna(0),
                        marker=MARKERS.get(sched, "o"), markersize=5, capsize=3,
                        linewidth=1.8, color=PALETTE.get(sched), label=sched, alpha=0.9)
        # Capacity: to the right of this line the system cannot keep up.
        ax.axvline(1.0, color="grey", linestyle="--", linewidth=1.2, zorder=0)
        ax.text(1.02, 0.02, "capacity", transform=ax.get_xaxis_transform(),
                rotation=90, fontsize=8, color="grey", va="bottom")
        if hline is not None:
            ax.axhline(hline, color="grey", linestyle=":", linewidth=1)
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.set_xlabel("offered load (x capacity)")
        if logy:
            ax.set_yscale("log")
        if ylim:
            ax.set_ylim(*ylim)
    axes[0].set_ylabel(ylabel)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, title="scheduler", loc="lower center",
               ncol=len(labels), bbox_to_anchor=(0.5, -0.09), frameon=False)
    fig.suptitle(title, y=1.02, fontsize=16)
    save(fig, out_dir, name, formats)


def plot_throughput_vs_offered(df, out_dir, formats):
    """Delivered throughput against offered load, with the ideal y=x line."""
    workloads = ordered(df["workload"].unique(), WORKLOAD_ORDER)
    schedulers = ordered(df["scheduler"].unique(), SCHEDULER_ORDER)
    fig, axes = plt.subplots(1, len(workloads), figsize=(5.2 * len(workloads), 5.0),
                             sharey=True, squeeze=False)
    axes = axes[0]
    for ax, wl in zip(axes, workloads):
        sub = df[df["workload"] == wl]
        cap = sub["capacity_tasks_per_s"].mean()
        for sched in schedulers:
            s = sub[sub["scheduler"] == sched].groupby("offered_load")["throughput_tasks_per_s"].mean().sort_index()
            if s.empty:
                continue
            ax.plot(s.index, s.values / cap, marker=MARKERS.get(sched, "o"),
                    markersize=5, linewidth=1.8, color=PALETTE.get(sched), label=sched, alpha=0.9)
        lim = sorted(sub["offered_load"].unique())
        ax.plot(lim, lim, color="black", linestyle=":", linewidth=1.2, label="_ideal")
        ax.axhline(1.0, color="grey", linestyle="--", linewidth=1)
        ax.set_title(WORKLOAD_LABELS.get(wl, wl))
        ax.set_xlabel("offered load (x capacity)")
        ax.set_ylim(0, 1.25)
    axes[0].set_ylabel("delivered throughput (x capacity)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, title="scheduler", loc="lower center",
               ncol=len(labels), bbox_to_anchor=(0.5, -0.09), frameon=False)
    fig.suptitle("Delivered throughput saturates at capacity; the dotted line is offered = delivered",
                 y=1.02, fontsize=15)
    save(fig, out_dir, "sweep_throughput", formats)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--run", help="run id under the results directory")
    ap.add_argument("--run-dir", help="explicit path to the run directory")
    ap.add_argument("--format", default="png,svg")
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else Path(args.results_dir) / (args.run or "")
    agg = run_dir / "aggregate.csv"
    if not agg.exists():
        raise SystemExit(f"{agg} not found")

    df = pd.read_csv(agg)
    if "offered_load" not in df.columns:
        raise SystemExit(f"{agg} has no offered_load column; it was not produced by a --loads sweep")
    if df["offered_load"].nunique() < 2:
        raise SystemExit(
            f"{agg} contains only one offered load ({df['offered_load'].iloc[0]}); "
            "a sweep needs at least two. Re-run with --loads 0.5,1.0,1.5,...")

    formats = [f.strip() for f in args.format.split(",") if f.strip()]
    out_dir = run_dir / "plots"
    loads = sorted(df["offered_load"].unique())
    reps = df.groupby(["scheduler", "workload", "offered_load"]).size().max()
    print(f"Loaded {len(df)} experiments across {len(loads)} load levels "
          f"({loads[0]:.2f}x to {loads[-1]:.2f}x), up to {reps} repetitions each")
    if df["timed_out"].astype(str).str.lower().eq("true").any():
        n = int(df["timed_out"].astype(str).str.lower().eq("true").sum())
        print(f"Warning: {n} experiments timed out; their points describe truncated runs.")

    setup_style()
    load_grid(df, "latency_p99_s", "p99 latency (s)",
              "Tail latency against offered load", out_dir, formats,
              "sweep_latency_p99", logy=True)
    load_grid(df, "latency_p50_s", "p50 latency (s)",
              "Median latency against offered load", out_dir, formats,
              "sweep_latency_p50", logy=True)
    load_grid(df, "deadline_miss_rate", "deadline miss rate",
              "Deadline compliance against offered load", out_dir, formats,
              "sweep_deadline_miss", ylim=(-0.03, 1.03))
    load_grid(df, "wait_max_s", "longest queue wait (s)",
              "Starvation against offered load", out_dir, formats,
              "sweep_max_wait", logy=True)
    load_grid(df, "jain_fairness_service", "Jain's fairness index",
              "Fairness against offered load", out_dir, formats,
              "sweep_fairness", ylim=(0, 1.05))
    plot_throughput_vs_offered(df, out_dir, formats)

    # a tidy summary table of the sweep
    cols = ["latency_p50_s", "latency_p99_s", "wait_max_s",
            "deadline_miss_rate", "throughput_tasks_per_s", "worker_utilization"]
    tbl = df.groupby(["workload", "scheduler", "offered_load"], as_index=False)[cols].mean()
    out_dir.mkdir(parents=True, exist_ok=True)
    tbl.to_csv(out_dir / "sweep_table.csv", index=False, float_format="%.4f")
    print(f"  wrote {out_dir / 'sweep_table.csv'}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
