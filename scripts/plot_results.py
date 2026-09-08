#!/usr/bin/env python3
"""Generate publication-quality charts from a benchmark run.

Reads the CSV files written by `cmd/benchmark` and writes PNG and SVG charts
into <run directory>/plots/.

Usage:
    python3 scripts/plot_results.py                      # newest run under results/
    python3 scripts/plot_results.py --run 20240501-120000
    python3 scripts/plot_results.py --run-dir results/20240501-120000
    python3 scripts/plot_results.py --results-dir results --format png

Inputs (produced by the benchmark harness):
    <run>/aggregate.csv   one row per scheduler x workload x repetition
    <run>/tenants.csv     one row per tenant per experiment

Repetitions are aggregated with the mean, and error bars show the standard
deviation across repetitions. A single repetition therefore shows no error bars,
which is itself an honest signal that the run has not been repeated.
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
except ImportError as exc:  # pragma: no cover - environment guidance
    sys.exit(
        f"missing Python dependency: {exc}\n"
        "Install the analysis dependencies with:\n"
        "  make python-deps\n"
        "or:\n"
        "  python3 -m pip install -r requirements.txt"
    )

# Fixed scheduler order and colours so every chart in a report is comparable.
SCHEDULER_ORDER = ["fifo", "priority", "edf", "wfq"]
WORKLOAD_ORDER = ["uniform", "bursty", "heavy_tailed", "multi_tenant"]
PALETTE = {
    "fifo": "#4C72B0",
    "priority": "#DD8452",
    "edf": "#55A868",
    "wfq": "#C44E52",
}

WORKLOAD_LABELS = {
    "uniform": "Uniform",
    "bursty": "Bursty",
    "heavy_tailed": "Heavy-\ntailed",
    "multi_tenant": "Multi-tenant\nskew",
}


def setup_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.dpi": 110,
            "savefig.dpi": 200,
            "savefig.bbox": "tight",
            "axes.titlesize": 15,
            "axes.labelsize": 12,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
            "legend.fontsize": 11,
            "font.family": "DejaVu Sans",
        }
    )


def ordered(values, order):
    """Return the entries of `order` that actually occur in `values`."""
    present = set(values)
    return [v for v in order if v in present] + sorted(present - set(order))


def latest_run(results_dir: Path) -> Path:
    candidates = [p for p in results_dir.iterdir() if p.is_dir() and (p / "aggregate.csv").exists()]
    if not candidates:
        raise SystemExit(
            f"no benchmark runs found under {results_dir}.\n"
            "Run one first with:\n  make bench-quick"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def save(fig, out_dir: Path, name: str, formats: list[str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"{name}.{fmt}"
        fig.savefig(path, format=fmt)
        print(f"  wrote {path}")
    plt.close(fig)


def label_workloads(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["workload_label"] = df["workload"].map(lambda w: WORKLOAD_LABELS.get(w, w))
    return df


def grouped_bar(ax, df, value_col, err_col=None, ylabel=""):
    """Draw workloads on the x axis with one bar per scheduler."""
    workloads = ordered(df["workload"].unique(), WORKLOAD_ORDER)
    schedulers = ordered(df["scheduler"].unique(), SCHEDULER_ORDER)
    width = 0.8 / max(len(schedulers), 1)

    for i, sched in enumerate(schedulers):
        xs, ys, errs = [], [], []
        for j, wl in enumerate(workloads):
            row = df[(df["scheduler"] == sched) & (df["workload"] == wl)]
            if row.empty:
                continue
            xs.append(j - 0.4 + width * (i + 0.5))
            ys.append(float(row[value_col].iloc[0]))
            errs.append(float(row[err_col].iloc[0]) if err_col else 0.0)
        ax.bar(
            xs, ys, width=width * 0.92, label=sched,
            color=PALETTE.get(sched, None), yerr=errs if any(errs) else None,
            capsize=3, error_kw={"elinewidth": 1, "alpha": 0.7},
        )

    ax.set_xticks(range(len(workloads)))
    ax.set_xticklabels([WORKLOAD_LABELS.get(w, w) for w in workloads])
    ax.set_ylabel(ylabel)
    ax.set_xlabel("")


def aggregate_by_cell(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Mean and standard deviation across repetitions for each matrix cell."""
    grouped = df.groupby(["scheduler", "workload"])[cols].agg(["mean", "std"])
    grouped.columns = ["_".join(c).rstrip("_") for c in grouped.columns.to_flat_index()]
    return grouped.reset_index()


def plot_latency_percentiles(df, out_dir, formats):
    """p50/p95/p99 latency by scheduler and workload, one panel per percentile."""
    cols = ["latency_p50_s", "latency_p95_s", "latency_p99_s"]
    agg = aggregate_by_cell(df, cols)

    fig, axes = plt.subplots(1, 3, figsize=(18, 5.8), sharex=True)
    for ax, col, title in zip(axes, cols, ["p50 (median)", "p95", "p99 (tail)"]):
        grouped_bar(ax, agg, f"{col}_mean", f"{col}_std", ylabel="latency (s)")
        ax.set_title(title)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, title="scheduler", loc="lower center",
               ncol=len(labels), bbox_to_anchor=(0.5, -0.10), frameon=False)
    fig.suptitle("End-to-end latency by scheduler and workload", y=1.02, fontsize=17)
    save(fig, out_dir, "latency_percentiles", formats)


def plot_latency_tail_ratio(df, out_dir, formats):
    """How much worse the tail is than the median: p99 divided by p50."""
    df = df.copy()
    df["tail_ratio"] = df["latency_p99_s"] / df["latency_p50_s"].replace(0, float("nan"))
    agg = aggregate_by_cell(df, ["tail_ratio"])

    fig, ax = plt.subplots(figsize=(10, 5.5))
    grouped_bar(ax, agg, "tail_ratio_mean", "tail_ratio_std", ylabel="p99 / p50")
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1)
    ax.set_title("Tail amplification: p99 latency relative to the median")
    ax.legend(title="scheduler", loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "latency_tail_ratio", formats)


def plot_throughput(df, out_dir, formats):
    agg = aggregate_by_cell(df, ["throughput_tasks_per_s"])
    fig, ax = plt.subplots(figsize=(10, 5.5))
    grouped_bar(ax, agg, "throughput_tasks_per_s_mean", "throughput_tasks_per_s_std",
                ylabel="completed tasks per second")
    ax.set_title("Throughput by scheduler and workload")
    ax.legend(title="scheduler", loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "throughput", formats)


def plot_deadline_miss_rate(df, out_dir, formats):
    agg = aggregate_by_cell(df, ["deadline_miss_rate"])
    fig, ax = plt.subplots(figsize=(10, 5.5))
    grouped_bar(ax, agg, "deadline_miss_rate_mean", "deadline_miss_rate_std",
                ylabel="fraction of tasks finishing late")
    ax.set_ylim(0, max(1.0, float(agg["deadline_miss_rate_mean"].max()) * 1.2))
    ax.set_title("Deadline miss rate by scheduler and workload")
    ax.legend(title="scheduler", loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "deadline_miss_rate", formats)


def plot_fairness(df, out_dir, formats):
    """Jain's index over completed service per tenant, focused on the skewed workload."""
    skew = df[df["workload"] == "multi_tenant"]
    fig, axes = plt.subplots(1, 2, figsize=(15, 5.5))

    if skew.empty:
        axes[0].text(0.5, 0.5, "no multi_tenant experiments in this run",
                     ha="center", va="center")
        axes[0].axis("off")
    else:
        agg = (
            skew.groupby("scheduler")["jain_fairness_service"]
            .agg(jain_mean="mean", jain_std="std")
            .reset_index()
        )
        schedulers = ordered(agg["scheduler"], SCHEDULER_ORDER)
        agg = agg.set_index("scheduler").loc[schedulers].reset_index()
        axes[0].bar(
            agg["scheduler"], agg["jain_mean"],
            yerr=agg["jain_std"].fillna(0), capsize=4,
            color=[PALETTE.get(s) for s in agg["scheduler"]],
        )
        n = int(skew["tenant_count"].iloc[0]) if "tenant_count" in skew else 5
        axes[0].axhline(1.0, color="grey", linestyle="--", linewidth=1)
        axes[0].axhline(1.0 / n, color="firebrick", linestyle=":", linewidth=1.4)
        axes[0].text(
            0.02, 1.0 / n + 0.02, f"1/n = {1.0/n:.2f} (one tenant takes everything)",
            transform=axes[0].get_yaxis_transform(), fontsize=9, color="firebrick",
        )
        axes[0].set_ylim(0, 1.08)
        axes[0].set_ylabel("Jain's index over completed service")
        axes[0].set_title("Fairness under 90/3/3/2/2 tenant skew")

    # All workloads, for context.
    agg_all = aggregate_by_cell(df, ["jain_fairness_service"])
    grouped_bar(axes[1], agg_all, "jain_fairness_service_mean", "jain_fairness_service_std",
                ylabel="Jain's index over completed service")
    axes[1].set_ylim(0, 1.08)
    axes[1].set_title("Fairness across all workloads")
    axes[1].legend(title="scheduler", fontsize=9, loc="lower right", framealpha=0.9)

    fig.suptitle(
        "Jain's fairness index; x is completed service seconds per tenant, "
        "zero-service tenants included",
        y=1.03, fontsize=13,
    )
    save(fig, out_dir, "jain_fairness", formats)


def plot_starvation(df, out_dir, formats):
    """Longest observed queue wait: the direct starvation signal."""
    agg = aggregate_by_cell(df, ["wait_max_s", "wait_p99_s"])

    fig, axes = plt.subplots(1, 2, figsize=(16, 5.5))
    grouped_bar(axes[0], agg, "wait_max_s_mean", "wait_max_s_std", ylabel="seconds")
    axes[0].set_title("Longest observed queue wait (worst case)")
    axes[0].legend(title="scheduler", fontsize=9, loc="upper left", framealpha=0.9)

    grouped_bar(axes[1], agg, "wait_p99_s_mean", "wait_p99_s_std", ylabel="seconds")
    axes[1].set_title("p99 queue wait")

    fig.suptitle("Starvation: how long the unluckiest task waited", y=1.03, fontsize=15)
    save(fig, out_dir, "starvation_max_wait", formats)


def plot_priority_starvation(raw_dir: Path, out_dir: Path, formats):
    """Latency by priority level, which is where strict priority starves."""
    files = sorted(raw_dir.glob("*.csv")) if raw_dir.exists() else []
    if not files:
        return
    frames = []
    for path in files:
        try:
            frames.append(pd.read_csv(path))
        except Exception as exc:  # pragma: no cover
            print(f"  skipping {path}: {exc}")
    if not frames:
        return
    raw = pd.concat(frames, ignore_index=True)
    if "priority" not in raw or raw.empty:
        return

    fig, ax = plt.subplots(figsize=(11, 5.5))
    schedulers = ordered(raw["scheduler"].unique(), SCHEDULER_ORDER)
    summary = (
        raw.groupby(["scheduler", "priority"])["latency_s"]
        .quantile(0.95)
        .reset_index(name="latency_p95_s")
    )
    for sched in schedulers:
        sub = summary[summary["scheduler"] == sched].sort_values("priority")
        ax.plot(sub["priority"], sub["latency_p95_s"], marker="o",
                label=sched, color=PALETTE.get(sched))
    ax.set_xlabel("task priority (higher is more important)")
    ax.set_ylabel("p95 latency (s)")
    ax.set_title("p95 latency by priority level, pooled across workloads")
    ax.legend(title="scheduler", loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "priority_starvation", formats)


def plot_tenant_latency(tenant_csv: Path, out_dir: Path, formats):
    """Per-tenant latency under the skewed workload: the noisy-neighbour view."""
    if not tenant_csv.exists():
        return
    df = pd.read_csv(tenant_csv)
    skew = df[df["workload"] == "multi_tenant"]
    if skew.empty:
        return

    agg = (
        skew.groupby(["scheduler", "tenant"], as_index=False)["latency_p95_s"]
        .mean()
    )
    schedulers = ordered(agg["scheduler"].unique(), SCHEDULER_ORDER)
    tenants = sorted(agg["tenant"].unique())

    fig, ax = plt.subplots(figsize=(11, 5.5))
    width = 0.8 / max(len(schedulers), 1)
    for i, sched in enumerate(schedulers):
        xs, ys = [], []
        for j, tenant in enumerate(tenants):
            row = agg[(agg["scheduler"] == sched) & (agg["tenant"] == tenant)]
            if row.empty:
                continue
            xs.append(j - 0.4 + width * (i + 0.5))
            ys.append(float(row["latency_p95_s"].iloc[0]))
        ax.bar(xs, ys, width=width * 0.92, label=sched, color=PALETTE.get(sched))
    ax.set_xticks(range(len(tenants)))
    ax.set_xticklabels([f"{t}\n({'90%' if t == 'A' else 'small'})" for t in tenants])
    ax.set_ylabel("p95 latency (s)")
    ax.set_title("Per-tenant p95 latency under 90/3/3/2/2 skew\n"
                 "(tenant A is the noisy neighbour)")
    ax.legend(title="scheduler", loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "tenant_latency_skew", formats)


def plot_utilisation(df, out_dir, formats):
    agg = aggregate_by_cell(df, ["worker_utilization"])
    fig, ax = plt.subplots(figsize=(10, 5.5))
    grouped_bar(ax, agg, "worker_utilization_mean", "worker_utilization_std",
                ylabel="busy slot-seconds / available slot-seconds")
    ax.set_ylim(0, 1.05)
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1)
    ax.set_title("Worker utilisation by scheduler and workload")
    ax.legend(title="scheduler", fontsize=9, loc="center left",
              bbox_to_anchor=(1.02, 0.5), frameon=False)
    save(fig, out_dir, "worker_utilization", formats)


def write_summary_table(df: pd.DataFrame, out_dir: Path) -> None:
    """A compact CSV and Markdown table of headline numbers per matrix cell."""
    cols = [
        "latency_p50_s", "latency_p95_s", "latency_p99_s", "wait_max_s",
        "throughput_tasks_per_s", "deadline_miss_rate", "worker_utilization",
        "jain_fairness_service",
    ]
    summary = df.groupby(["workload", "scheduler"], as_index=False)[cols].mean()
    summary["workload"] = pd.Categorical(
        summary["workload"], ordered(summary["workload"].unique(), WORKLOAD_ORDER), ordered=True
    )
    summary["scheduler"] = pd.Categorical(
        summary["scheduler"], ordered(summary["scheduler"].unique(), SCHEDULER_ORDER), ordered=True
    )
    summary = summary.sort_values(["workload", "scheduler"])

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "summary_table.csv"
    summary.to_csv(csv_path, index=False, float_format="%.4f")
    print(f"  wrote {csv_path}")

    md_path = out_dir / "summary_table.md"
    with md_path.open("w") as fh:
        fh.write("| workload | scheduler | p50 s | p95 s | p99 s | max wait s | "
                 "tasks/s | deadline miss | utilisation | Jain |\n")
        fh.write("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
        for _, r in summary.iterrows():
            fh.write(
                f"| {r['workload']} | {r['scheduler']} | {r['latency_p50_s']:.3f} | "
                f"{r['latency_p95_s']:.3f} | {r['latency_p99_s']:.3f} | {r['wait_max_s']:.3f} | "
                f"{r['throughput_tasks_per_s']:.1f} | {r['deadline_miss_rate']:.3f} | "
                f"{r['worker_utilization']:.2f} | {r['jain_fairness_service']:.3f} |\n"
            )
    print(f"  wrote {md_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", default="results", help="root results directory (default: results)")
    parser.add_argument("--run", help="run id under the results directory; defaults to the newest run")
    parser.add_argument("--run-dir", help="explicit path to a run directory, overriding --results-dir/--run")
    parser.add_argument("--format", default="png,svg", help="comma-separated output formats (default: png,svg)")
    args = parser.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    else:
        results_dir = Path(args.results_dir)
        if not results_dir.exists():
            raise SystemExit(f"results directory {results_dir} does not exist; run `make bench-quick` first")
        run_dir = results_dir / args.run if args.run else latest_run(results_dir)

    aggregate = run_dir / "aggregate.csv"
    if not aggregate.exists():
        raise SystemExit(f"{aggregate} not found; is {run_dir} a benchmark run directory?")

    formats = [f.strip() for f in args.format.split(",") if f.strip()]
    df = pd.read_csv(aggregate)
    if df.empty:
        raise SystemExit(f"{aggregate} contains no experiments")

    reps = df.groupby(["scheduler", "workload"]).size().max()
    print(f"Loaded {len(df)} experiments from {aggregate} "
          f"({df['scheduler'].nunique()} schedulers x {df['workload'].nunique()} workloads, "
          f"up to {reps} repetitions per cell)")
    if reps < 2:
        print("Note: a single repetition per cell means no error bars and no variance estimate.")
    if df["timed_out"].astype(str).str.lower().eq("true").any():
        print("Warning: at least one experiment timed out; its numbers describe a truncated run.")

    setup_style()
    out_dir = run_dir / "plots"
    print(f"Writing charts to {out_dir}")

    plot_latency_percentiles(df, out_dir, formats)
    plot_latency_tail_ratio(df, out_dir, formats)
    plot_throughput(df, out_dir, formats)
    plot_deadline_miss_rate(df, out_dir, formats)
    plot_fairness(df, out_dir, formats)
    plot_starvation(df, out_dir, formats)
    plot_utilisation(df, out_dir, formats)
    plot_tenant_latency(run_dir / "tenants.csv", out_dir, formats)
    plot_priority_starvation(run_dir / "raw", out_dir, formats)
    write_summary_table(df, out_dir)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
