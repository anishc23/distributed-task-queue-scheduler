#!/usr/bin/env python3
"""Chart what a network partition costs a scheduler.

Reads the CSV produced by `cmd/partitionbench`.

Usage:
    python3 scripts/plot_partition.py --run partition
    python3 scripts/plot_partition.py --run partition --sweep-run partition-sweep

The partition here is between one scheduler and a Redis that stays healthy. That
is a different failure from the other two the project measures, and the
difference is what the leader can know. A killed leader is gone. A frozen leader
learns nothing, because it is not running. A partitioned leader learns that
every call is failing and still cannot tell whether Redis is unreachable or it
has already been replaced — two situations that call for opposite responses.

The arms:

    none      no partition. The natural dispatch gap.
    brief     shorter than the lease. Nothing should happen: standing down on a
              transient blip would turn every hiccup into a failover.
    long      longer than the lease. A standby takes over, and the returning
              leader must be refused.
    flapping  cut and healed repeatedly. Must not produce a split brain.

With --sweep-run, a second chart plots outage against partition length. The
claim being tested there is that the lease bounds the damage: outage tracks the
partition while the partition is shorter than the lease, and stops growing once
a standby can take over.
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

ARM_ORDER = ["none", "brief", "long", "flapping"]
ARM_LABELS = {
    "none": "No partition\n(natural gap)",
    "brief": "Shorter than\nthe lease",
    "long": "Longer than\nthe lease",
    "flapping": "Cut and healed\nrepeatedly",
}
PALETTE = {"none": "#4C72B0", "brief": "#55A868", "long": "#DD8452", "flapping": "#8172B3"}


def setup_style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update({
        "figure.dpi": 110, "savefig.dpi": 200, "savefig.bbox": "tight",
        "axes.titlesize": 13, "axes.labelsize": 11,
        "xtick.labelsize": 9.5, "ytick.labelsize": 10, "legend.fontsize": 10,
        "font.family": "DejaVu Sans",
    })


def ordered(values, order):
    present = set(values)
    return [v for v in order if v in present] + sorted(present - set(order))


def read_run(run_dir: Path, name: str = "partition.csv") -> pd.DataFrame:
    csv = run_dir / name
    if not csv.exists():
        raise SystemExit(f"{csv} not found. Produce it with: make partition RUN={run_dir.name}")
    df = pd.read_csv(csv)
    for col in ("failover", "unexpected_failover", "recovered", "suspect"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().eq("true")
    if "suspect" in df.columns and df["suspect"].any():
        n = int(df["suspect"].sum())
        print(f"  dropping {n} suspect trial(s): outage far beyond the partition and the lease")
        df = df[~df["suspect"]].reset_index(drop=True)
    if df.empty:
        raise SystemExit(f"{csv} has no usable trials")
    return df


def check_invariants(df: pd.DataFrame) -> None:
    """Two properties have to hold in every arm before any chart means
    anything: exactly one leader once the path is repaired, and no dispatch
    under a superseded epoch."""
    if "epoch_inversions" in df.columns:
        bad = df[df["epoch_inversions"].fillna(0) > 0]
        if not bad.empty:
            raise SystemExit(
                f"{len(bad)} trial(s) dispatched under a superseded epoch. "
                "The single-writer invariant was violated; these results must not be charted.")
    if "leaders_after_heal" in df.columns:
        healed = df[df["partition_ms"].fillna(0) > 0]
        bad = healed[healed["leaders_after_heal"] != 1]
        if not bad.empty:
            raise SystemExit(
                f"{len(bad)} of {len(healed)} partitioned trials did not end with exactly one "
                "leader after the path was repaired (0 = leaderless, 2 = split brain).")


def summarise(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for a in ordered(df["arm"].unique(), ARM_ORDER):
        sub = df[df["arm"] == a]
        rec = sub[sub["recovered"]]
        rows.append({
            "arm": a,
            "trials": len(sub),
            "partition_median_ms": round(sub["partition_ms"].median(), 1),
            "outage_median_ms": round(sub["max_gap_ms"].median(), 1) if a == "none"
            else (round(rec["outage_ms"].median(), 1) if not rec.empty else None),
            "failovers": int(sub["failover"].sum()),
            "unexpected_failovers": int(sub["unexpected_failover"].sum()),
            "fenced_on_heal": int((sub["fenced_on_heal"].fillna(0) > 0).sum()),
            "epoch_inversions": int(sub["epoch_inversions"].fillna(0).sum()),
            "tasks_unfinished": int((sub["tasks"] - sub["completed"] - sub["dead_lettered"]).sum()),
            "duplicate_completions": int(sub["duplicate_completions"].sum()),
        })
    return pd.DataFrame(rows)


def plot_arms(df: pd.DataFrame, tbl: pd.DataFrame, out_dir: Path, formats: list[str]) -> None:
    arms = list(tbl["arm"])
    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.4))

    # Left: how long dispatch stopped, with the partition drawn behind it. The
    # comparison between the two bars is the point: for a short partition the
    # outage is the partition, and for a long one it is capped well below it.
    ax = axes[0]
    x = range(len(arms))
    w = 0.38
    # NaN is truthy, so `x or 0` would let a missing value through as NaN and
    # silently produce an empty bar rather than a zero one.
    def val(col, i):
        v = tbl.loc[i, col]
        return 0.0 if v is None or pd.isna(v) else float(v)

    parts = [val("partition_median_ms", i) for i in range(len(arms))]
    gaps = [val("outage_median_ms", i) for i in range(len(arms))]
    top = max(max(parts), max(gaps), 1)
    ax.bar([i - w / 2 for i in x], parts, width=w, color="#BBBBBB", alpha=0.9, label="partition")
    ax.bar([i + w / 2 for i in x], gaps, width=w,
           color=[PALETTE.get(a, "#777777") for a in arms], alpha=0.95, label="dispatch stopped")
    for i, (pv, gv) in enumerate(zip(parts, gaps)):
        if pv > 0:
            ax.text(i - w / 2, pv + top * 0.02, f"{pv:,.0f}", ha="center", va="bottom", fontsize=8.5)
        ax.text(i + w / 2, gv + top * 0.02, f"{gv:,.0f}", ha="center", va="bottom",
                fontsize=9, weight="bold")
    ttl = df["lease_ttl_ms"].mode()
    if not ttl.empty:
        ax.axhline(ttl.iloc[0], color="#444444", linestyle="--", linewidth=1.3)
        ax.text(-0.45, ttl.iloc[0] + top * 0.015, f"lease TTL ({ttl.iloc[0]:,.0f} ms)",
                fontsize=9, color="grey", va="bottom", ha="left")
    ax.set_ylim(0, top * 1.22)
    ax.set_xticks(list(x))
    ax.set_xticklabels([ARM_LABELS.get(a, a) for a in arms])
    ax.set_ylabel("milliseconds")
    ax.set_title("What the partition costs", fontsize=12.5)
    ax.legend(frameon=False, loc="upper left", fontsize=9)

    # Right: did leadership move, and did the fence have to catch the leader
    # that came back.
    ax = axes[1]
    n = int(tbl["trials"].max())
    fails = list(tbl["failovers"])
    fenced = list(tbl["fenced_on_heal"])
    ax.bar([i - w / 2 for i in x], fails, width=w, color="#C44E52", alpha=0.9, label="leadership moved")
    ax.bar([i + w / 2 for i in x], fenced, width=w, color="#8172B3", alpha=0.9, label="returning leader fenced")
    for i, (fv, cv) in enumerate(zip(fails, fenced)):
        ax.text(i - w / 2, fv + n * 0.03, f"{fv}", ha="center", va="bottom", fontsize=9.5, weight="bold")
        ax.text(i + w / 2, cv + n * 0.03, f"{cv}", ha="center", va="bottom", fontsize=9.5, weight="bold")
    ax.set_ylim(0, n * 1.3)
    ax.set_xticks(list(x))
    ax.set_xticklabels([ARM_LABELS.get(a, a) for a in arms])
    ax.set_ylabel(f"trials (n = {n})")
    ax.set_title("Whether leadership moved,\nand what stopped the leader that returned", fontsize=12.5)
    ax.legend(frameon=False, loc="upper left", fontsize=9)

    inversions = int(tbl["epoch_inversions"].sum())
    fig.subplots_adjust(bottom=0.26)
    fig.text(0.012, 0.015,
             f"Dispatches under a superseded epoch: {inversions} across all {len(df)} trials.",
             fontsize=10, color="#444444", ha="left", va="bottom")
    fig.tight_layout(rect=(0, 0.04, 1, 1))

    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"partition.{fmt}"
        fig.savefig(path, format=fmt)
        print(f"  wrote {path}")
    plt.close(fig)


def plot_sweep(df: pd.DataFrame, out_dir: Path, formats: list[str]) -> None:
    """Outage against partition length.

    The prediction: while the partition is shorter than the lease the leader
    keeps its grant and simply cannot work, so the outage tracks the partition
    one for one. Once the partition outlasts the lease a standby takes over, and
    the outage stops growing however long the partition runs. If that holds, the
    lease TTL bounds the cost of a partition of any length."""
    rec = df[df["recovered"]]
    if rec.empty:
        return
    g = rec.groupby("partition_ms")["outage_ms"]
    med, lo, hi = g.median(), g.min(), g.max()
    ttl = df["lease_ttl_ms"].mode().iloc[0]

    fig, ax = plt.subplots(figsize=(8.2, 5.6))
    lim = max(med.index.max(), med.max()) * 1.12
    ax.plot([0, lim], [0, lim], linestyle=":", color="grey", linewidth=1.3,
            label="outage = partition length")
    ax.axhline(ttl, color="#444444", linestyle="--", linewidth=1.4,
               label=f"lease TTL ({ttl:,.0f} ms)")
    ax.axvline(ttl, color="#AAAAAA", linestyle="--", linewidth=1.0)
    ax.errorbar(med.index, med.values,
                yerr=[med.values - lo.values, hi.values - med.values],
                marker="o", markersize=7, capsize=4, linewidth=2,
                color="#DD8452", label="measured")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_xlabel("partition length (ms)")
    ax.set_ylabel("dispatch outage (ms)")
    ax.set_title("A partition costs its own length,\nuntil the lease caps it", fontsize=13)
    ax.legend(frameon=False, loc="upper left", fontsize=9.5)

    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"partition_sweep.{fmt}"
        fig.savefig(path, format=fmt)
        print(f"  wrote {path}")
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--run", help="run id under --results-dir")
    ap.add_argument("--run-dir", help="path to a run directory, instead of --run")
    ap.add_argument("--sweep-run", default="", help="run id of a --sweep run, charted separately")
    ap.add_argument("--format", default="png")
    args = ap.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    elif args.run:
        run_dir = Path(args.results_dir) / args.run
    else:
        ap.error("pass --run or --run-dir")

    df = read_run(run_dir)
    check_invariants(df)
    tbl = summarise(df)
    formats = [f.strip() for f in args.format.split(",") if f.strip()]

    print(f"Loaded {len(df)} trials across {df['arm'].nunique()} arms")
    setup_style()
    out_dir = run_dir / "plots"
    plot_arms(df, tbl, out_dir, formats)

    if args.sweep_run:
        sweep_dir = Path(args.results_dir) / args.sweep_run
        sdf = read_run(sweep_dir)
        check_invariants(sdf)
        plot_sweep(sdf, out_dir, formats)

    tbl.to_csv(out_dir / "partition_table.csv", index=False)
    print(f"  wrote {out_dir / 'partition_table.csv'}")
    print()
    print(tbl.to_string(index=False))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
