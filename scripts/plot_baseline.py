#!/usr/bin/env python3
"""Chart this queue against Asynq across task durations.

Reads the CSV produced by bench/asynq.

Usage:
    python3 scripts/plot_baseline.py --run baseline
    python3 scripts/plot_baseline.py --run-dir results/baseline --format png

Why the comparison is plotted against task duration rather than reported as a
single number: scheduling overhead is a fixed cost per task, so it is only
visible while the work itself is cheap enough not to hide it. A single headline
figure would be true only at whatever duration happened to be chosen, and could
be made to favour either system by picking it.

The dashed line is the ceiling no implementation can pass: with N slots each
held for exec_ms, throughput cannot exceed N / exec_ms. Where both systems sit
near it, the benchmark has stopped measuring queue machinery and is measuring
sleep.
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

LABELS = {"taskqueue": "This queue (central scheduler)", "asynq": "Asynq (workers pull directly)"}
COLOURS = {"taskqueue": "#4C72B0", "asynq": "#DD8452"}


def setup_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update({
        "figure.dpi": 110, "savefig.dpi": 200, "savefig.bbox": "tight",
        "axes.titlesize": 13, "axes.labelsize": 11,
        "xtick.labelsize": 10, "ytick.labelsize": 10, "legend.fontsize": 10,
        "font.family": "DejaVu Sans",
    })


def read_run(run_dir: Path) -> pd.DataFrame:
    csv = run_dir / "baseline.csv"
    if not csv.exists():
        raise SystemExit(f"{csv} not found. Produce it with: make baseline RUN={run_dir.name}")
    df = pd.read_csv(csv)
    missing = df[df["completed"] < df["tasks"]]
    if not missing.empty:
        # A run that did not finish its work is not a throughput measurement.
        raise SystemExit(
            f"{len(missing)} run(s) completed fewer tasks than they were given; "
            "the comparison would be between different amounts of work.")
    return df


def summarise(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for exec_ms, grp in df.groupby("exec_ms"):
        row = {"exec_ms": int(exec_ms)}
        for system in ("taskqueue", "asynq"):
            sub = grp[grp["system"] == system]
            if sub.empty:
                continue
            row[f"{system}_throughput"] = round(sub["throughput_per_s"].median(), 1)
            row[f"{system}_p99_ms"] = round(sub["latency_p99_ms"].median(), 1)
        if "asynq_throughput" in row and row["asynq_throughput"]:
            row["delta_pct"] = round(
                100 * (row["taskqueue_throughput"] - row["asynq_throughput"]) / row["asynq_throughput"], 1)
        concurrency = int(grp["concurrency"].iloc[0])
        row["ceiling"] = round(concurrency / (exec_ms / 1000.0), 1) if exec_ms > 0 else None
        rows.append(row)
    return pd.DataFrame(rows).sort_values("exec_ms").reset_index(drop=True)


def plot(df: pd.DataFrame, tbl: pd.DataFrame, out_dir: Path, formats: list[str]) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14.5, 5.4))

    ax = axes[0]
    for system in ("taskqueue", "asynq"):
        sub = df[df["system"] == system]
        med = sub.groupby("exec_ms")["throughput_per_s"].median()
        lo = sub.groupby("exec_ms")["throughput_per_s"].min()
        hi = sub.groupby("exec_ms")["throughput_per_s"].max()
        ax.errorbar(med.index, med.values,
                    yerr=[med.values - lo.values, hi.values - med.values],
                    marker="o", markersize=6, capsize=3, linewidth=2,
                    color=COLOURS[system], label=LABELS[system])
    ceil = tbl.dropna(subset=["ceiling"])
    ax.plot(ceil["exec_ms"], ceil["ceiling"], linestyle="--", color="grey",
            linewidth=1.4, label="slot ceiling (N / exec_ms)")
    ax.set_yscale("log")
    ax.set_xlabel("simulated task duration (ms)")
    ax.set_ylabel("throughput (tasks/s, log scale)")
    ax.set_title("Throughput against task duration", fontsize=12.5)
    ax.legend(frameon=False, loc="lower left", fontsize=9)

    ax = axes[1]
    d = tbl.dropna(subset=["delta_pct"])
    colours = ["#C44E52" if v < 0 else "#55A868" for v in d["delta_pct"]]
    ax.bar(range(len(d)), d["delta_pct"], color=colours, alpha=0.9)
    for i, v in enumerate(d["delta_pct"]):
        off = 1.6 if v >= 0 else -1.6
        ax.text(i, v + off, f"{v:+.1f}%", ha="center",
                va="bottom" if v >= 0 else "top", fontsize=10, weight="bold")
    ax.axhline(0, color="#333333", linewidth=1.2)
    ax.set_xticks(range(len(d)))
    ax.set_xticklabels([f"{int(v)}" for v in d["exec_ms"]])
    ax.set_xlabel("simulated task duration (ms)")
    ax.set_ylabel("this queue vs Asynq (%)")
    lim = max(abs(d["delta_pct"].min()), abs(d["delta_pct"].max())) * 1.45
    ax.set_ylim(-lim, lim)
    ax.set_title("Where centralising the decision costs,\nand where it stops costing", fontsize=12.5)

    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"baseline.{fmt}"
        fig.savefig(path, format=fmt)
        print(f"  wrote {path}")
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--run", help="run id under --results-dir")
    ap.add_argument("--run-dir", help="path to a run directory, instead of --run")
    ap.add_argument("--format", default="png")
    args = ap.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    elif args.run:
        run_dir = Path(args.results_dir) / args.run
    else:
        ap.error("pass --run or --run-dir")

    df = read_run(run_dir)
    tbl = summarise(df)
    print(f"Loaded {len(df)} runs across {df['exec_ms'].nunique()} task durations")
    setup_style()
    out_dir = run_dir / "plots"
    plot(df, tbl, out_dir, [f.strip() for f in args.format.split(",") if f.strip()])
    tbl.to_csv(out_dir / "baseline_table.csv", index=False)
    print(f"  wrote {out_dir / 'baseline_table.csv'}")
    print()
    print(tbl.to_string(index=False))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
