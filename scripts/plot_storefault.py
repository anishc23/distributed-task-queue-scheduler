#!/usr/bin/env python3
"""Chart what a Redis failover does to the fencing token.

Reads the CSV produced by `cmd/storefaultbench`.

Usage:
    python3 scripts/plot_storefault.py --run storefault
    python3 scripts/plot_storefault.py --run-dir results/storefault --format png

The four arms answer one question in two halves — is the fence safe when the
store fails, and what does making it safe cost?

    clean      the master is killed with its replicas caught up. Sentinel
               promotes a node that has everything. This arm exists to show the
               harness is not rigged: a violation here would mean the setup,
               not the system, was broken.
    lagging    replication is cut, the queue keeps writing, then the master is
               killed. Sentinel promotes a node missing writes the queue was
               already told had succeeded.
    epochwait  the same fault, with the lease waiting for a replica to
               acknowledge a newly issued epoch before acting on it.
    guarded    the same fault, with min-replicas-to-write on the master as
               well, so it refuses writes it cannot hand on.

Two outcomes are charted separately because they are different failures. A
fencing token issued to two leaders is a safety violation: the fence stops
excluding anything. Tasks destroyed is durability loss: work the producer was
told had been accepted, gone. A mitigation can fix one and not the other, and
averaging them into a single "failures" bar would hide exactly that.
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

ARM_ORDER = ["clean", "lagging", "epochwait", "guarded"]
ARM_LABELS = {
    "clean": "Replicas\ncaught up\n(control)",
    "lagging": "Replication\ncut, no\nmitigation",
    "epochwait": "+ epoch\nwaits for\na replica",
    "guarded": "+ master\nrefuses lone\nwrites",
}
PALETTE = {"clean": "#4C72B0", "lagging": "#C44E52",
           "epochwait": "#DD8452", "guarded": "#55A868"}


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


def read_run(run_dir: Path) -> pd.DataFrame:
    csv = run_dir / "storefault.csv"
    if not csv.exists():
        raise SystemExit(f"{csv} not found. Produce it with: make storefault RUN={run_dir.name}")
    df = pd.read_csv(csv)
    for col in ("epoch_reused", "epoch_regressed", "wedged"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().eq("true")

    # A trial whose injected fault healed before the kill measured a clean
    # failover while claiming to be a lossy one. The harness refuses to finish
    # such a trial, but drop them loudly here too in case an older CSV is read.
    if "notes" in df.columns:
        healed = df["notes"].fillna("").str.contains("fault-healed")
        if healed.any():
            print(f"  dropping {int(healed.sum())} trial(s) whose fault healed before the kill")
            df = df[~healed].reset_index(drop=True)
    if df.empty:
        raise SystemExit(f"{csv} has no usable trials")
    return df


def check_control(df: pd.DataFrame) -> None:
    """The control arm must be clean. If a caught-up failover loses data or
    reuses a token, the harness is measuring its own setup and no other number
    in the file means anything."""
    ctl = df[df["arm"] == "clean"]
    if ctl.empty:
        return
    bad_token = int(ctl["epoch_reused"].sum())
    bad_data = int((ctl.get("tasks_lost", 0) > 0).sum())
    if bad_token or bad_data:
        raise SystemExit(
            f"the control arm failed in {bad_token} trial(s) by token reuse and "
            f"{bad_data} by data loss. A caught-up failover must be survivable; "
            "these results describe the harness, not the system.")


def summarise(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for a in ordered(df["arm"].unique(), ARM_ORDER):
        sub = df[df["arm"] == a]
        rows.append({
            "arm": a,
            "trials": len(sub),
            "token_reused": int(sub["epoch_reused"].sum()),
            "epoch_rewound": int(sub["epoch_regressed"].sum()),
            "tasks_destroyed": int(sub.get("tasks_lost", pd.Series(dtype=int)).sum()),
            "ingress_entries_lost": int(sub.get("ingress_lost_entries", pd.Series(dtype=int)).sum()),
            "submissions_accepted": int(sub["submitted"].sum()),
            "completed": int(sub["completed"].sum()),
            "failover_median_ms": round(sub["failover_ms"].median(), 1),
            "epoch_inversions": int(sub.get("epoch_inversions", pd.Series(dtype=int)).sum()),
        })
    return pd.DataFrame(rows)


def plot_safety_and_cost(df: pd.DataFrame, tbl: pd.DataFrame, out_dir: Path, formats: list[str]) -> None:
    arms = list(tbl["arm"])
    n = int(tbl["trials"].max())
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.4))

    # Panel 1: the safety violation.
    ax = axes[0]
    vals = list(tbl["token_reused"])
    bars = ax.bar(range(len(arms)), vals, color=[PALETTE.get(a) for a in arms], alpha=0.9)
    for i, (b, v) in enumerate(zip(bars, vals)):
        ax.text(i, v + n * 0.03, f"{v}/{int(tbl['trials'][i])}",
                ha="center", va="bottom", fontsize=11, weight="bold")
    ax.set_ylim(0, max(n * 1.25, 1))
    ax.set_ylabel("trials")
    ax.set_title("One fencing token,\ntwo different leaders", fontsize=12.5)

    # Panel 2: the durability loss.
    ax = axes[1]
    vals = list(tbl["tasks_destroyed"])
    top = max(max(vals), 1)
    bars = ax.bar(range(len(arms)), vals, color=[PALETTE.get(a) for a in arms], alpha=0.9)
    for i, (b, v) in enumerate(zip(bars, vals)):
        ax.text(i, v + top * 0.03, f"{v:,}", ha="center", va="bottom",
                fontsize=11, weight="bold")
    ax.set_ylim(0, top * 1.25)
    ax.set_ylabel("tasks")
    ax.set_title("Acknowledged work\ndestroyed by the promotion", fontsize=12.5)

    # Panel 3: accepted against completed, which is where the trade actually
    # shows. The unguarded arms accept roughly twice what they finish, and the
    # gap is work the producer was told had been taken on. The guarded arm
    # accepts less and finishes all of it. The same amount of work completes
    # either way; what differs is whether the producer was misled about the
    # rest, and a chart of accepted counts alone would make the safe
    # configuration look simply worse.
    ax = axes[2]
    x = range(len(arms))
    w = 0.38
    acc = list(tbl["submissions_accepted"])
    done = list(tbl["completed"])
    top = max(max(acc), max(done), 1)
    ax.bar([i - w / 2 for i in x], acc, width=w, color="#999999", alpha=0.9, label="accepted")
    ax.bar([i + w / 2 for i in x], done, width=w,
           color=[PALETTE.get(a) for a in arms], alpha=0.95, label="completed")
    for i, (a_v, d_v) in enumerate(zip(acc, done)):
        if a_v == d_v:
            # Equal bars sit shoulder to shoulder, so one centred label reads
            # better than two identical ones colliding.
            ax.text(i, d_v - top * 0.03, f"{d_v:,}", ha="center", va="top",
                    fontsize=9, color="white", weight="bold")
            continue
        ax.text(i - w / 2, a_v - top * 0.03, f"{a_v:,}", ha="center", va="top",
                fontsize=8.5, color="white", weight="bold")
        ax.text(i + w / 2, d_v - top * 0.03, f"{d_v:,}", ha="center", va="top",
                fontsize=8.5, color="white", weight="bold")
        # Two short lines rather than one long one: these arms are adjacent
        # and a single-line label is wide enough to collide with its neighbour.
        ax.text(i, a_v + top * 0.035, f"−{a_v - d_v:,}\nnever run",
                ha="center", va="bottom", fontsize=8.5, color="#B03030", weight="bold",
                linespacing=0.95)
    ax.set_ylim(0, top * 1.28)
    ax.set_ylabel("tasks")
    ax.set_title("Accepted against completed:\nwho was told the truth", fontsize=12.5)
    ax.legend(frameon=False, loc="lower left", fontsize=9)

    for ax in axes:
        ax.set_xticks(list(x))
        ax.set_xticklabels([ARM_LABELS.get(a, a) for a in arms], fontsize=9)

    fig.suptitle("A fencing token is only as durable as the store that holds it",
                 fontsize=15, y=1.03)
    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        path = out_dir / f"storefault.{fmt}"
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
    check_control(df)
    tbl = summarise(df)

    print(f"Loaded {len(df)} trials across {df['arm'].nunique()} arms")
    setup_style()
    out_dir = run_dir / "plots"
    plot_safety_and_cost(df, tbl, out_dir, [f.strip() for f in args.format.split(",") if f.strip()])

    out_dir.mkdir(parents=True, exist_ok=True)
    tbl.to_csv(out_dir / "storefault_table.csv", index=False)
    print(f"  wrote {out_dir / 'storefault_table.csv'}")
    print()
    print(tbl.to_string(index=False))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
