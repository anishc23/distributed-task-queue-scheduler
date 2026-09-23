#!/usr/bin/env python3
"""Chart what a scheduler crash costs.

Reads the CSV produced by `cmd/failoverbench` and draws the dispatch outage for
each experimental arm.

Usage:
    python3 scripts/plot_failover.py --run failover
    python3 scripts/plot_failover.py --run-dir results/failover --format png
    python3 scripts/plot_failover.py --run failover --ttl-runs failover-ttl2s,failover-ttl10s

The arms answer different questions and must be read together:

    none      nobody is killed. This is the natural gap between dispatches
              under this load, and the floor every other bar sits on.
    graceful  the leader is asked to stop. It releases the lease on the way
              out, so the standby can take over without waiting for anything.
    crash     the leader is killed outright. Nothing is released, so the
              standby cannot safely start until Redis expires the key.
    single    the only scheduler is killed. Nothing takes over at all, so the
              outage has no end and these trials are censored rather than
              plotted as a number.

Plotting a censored arm as a finite bar would be the most misleading thing this
script could do, so the single arm is drawn as a hatched bar spanning the axis
and labelled with the number of tasks it left unfinished.
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

ARM_ORDER = ["none", "graceful", "crash", "single"]
ARM_LABELS = {
    "none": "No kill\n(largest natural gap)",
    "graceful": "SIGTERM\n(lease released)",
    "crash": "SIGKILL\n(lease expires)",
    "single": "SIGKILL,\none replica",
}
PALETTE = {"none": "#4C72B0", "graceful": "#55A868", "crash": "#DD8452", "single": "#C44E52"}


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


def read_run(run_dir: Path) -> pd.DataFrame:
    csv = run_dir / "failover.csv"
    if not csv.exists():
        raise SystemExit(f"{csv} not found. Produce it with: make failover RUN={run_dir.name}")
    df = pd.read_csv(csv)
    df["recovered"] = df["recovered"].astype(str).str.lower().eq("true")
    df["unfinished"] = df["tasks"] - df["completed"] - df["dead_lettered"]

    # Trials the harness flagged as interfered with are dropped, loudly. A
    # crash outage is bounded by the lease TTL, so anything far beyond it did
    # not come from the mechanism under test, and quietly averaging it in is
    # the single easiest way to publish a nonsense median.
    if "suspect" in df.columns:
        suspect = df["suspect"].astype(str).str.lower().eq("true")
        if suspect.any():
            print(f"  dropping {int(suspect.sum())} suspect trial(s) flagged by the harness:")
            for _, r in df[suspect].iterrows():
                print(f"    {r['arm']} rep {r['rep']}: outage {r['outage_ms']:,.0f} ms "
                      f"against a {r['lease_ttl_ms']:,.0f} ms lease")
            df = df[~suspect].reset_index(drop=True)
    if df.empty:
        raise SystemExit(f"{csv} has no usable trials left after dropping suspects")
    return df


def check_invariant(df: pd.DataFrame) -> None:
    """Every trial sampled tq_scheduler_is_leader across replicas before the
    kill. Anything but exactly one leader means the run is not measuring what it
    claims to, so say so loudly rather than charting it."""
    if "leaders_before_kill" not in df.columns:
        return
    bad = df[df["leaders_before_kill"] != 1]
    if not bad.empty:
        raise SystemExit(
            f"{len(bad)} of {len(df)} trials did not have exactly one leader before the kill "
            "(0 = leaderless, 2 = split brain). The results are not trustworthy.")


def plot_outage(df: pd.DataFrame, out_dir: Path, formats: list[str]) -> None:
    """One bar per arm, with every trial drawn over it.

    The `none` arm is charted on `max_gap_ms` rather than `outage_ms`, because
    nothing is killed there and its outage is zero by construction. Its
    interesting quantity is the largest gap between dispatches that occurs
    anyway, which is the floor the other arms have to be read against; a
    zero-height bar would say nothing at all."""
    arms = ordered(df["arm"].unique(), ARM_ORDER)
    fig, ax = plt.subplots(figsize=(9.5, 5.6))

    def values(sub: pd.DataFrame, arm: str) -> pd.Series:
        col = "max_gap_ms" if arm == "none" else "outage_ms"
        return sub[col]

    finite_max = df.loc[df["recovered"] & (df["arm"] != "none"), "outage_ms"].max()
    ceiling = (finite_max if pd.notna(finite_max) else 1000) * 1.45

    for i, a in enumerate(arms):
        sub = df[df["arm"] == a]
        rec = sub[sub["recovered"]]
        if rec.empty:
            # Censored: the queue never resumed. A bar to the top of the axis
            # with a break hatch says "at least this, and we stopped watching".
            ax.bar(i, ceiling, color=PALETTE.get(a), alpha=0.35,
                   hatch="///", edgecolor=PALETTE.get(a), linewidth=1.5)
            lost = int(sub["unfinished"].mean())
            ax.text(i, ceiling * 0.30, f"never\nrecovered\n({lost:,} tasks\nunfinished)",
                    ha="center", va="center", fontsize=10, color=PALETTE.get(a), weight="bold")
            continue

        vals = values(rec, a)
        med = vals.median()
        ax.bar(i, med, color=PALETTE.get(a), alpha=0.85)
        # Individual trials over the bar: with ten to fifteen points the spread
        # is worth seeing directly rather than through an error bar. They are
        # offset slightly to the right so the median label has clear space.
        ax.scatter([i + 0.18] * len(vals), vals, color="black", s=14, zorder=3, alpha=0.55)
        # The label sits above whichever is higher, the bar or the topmost
        # trial, so it never lands on a data point.
        ax.text(i - 0.1, max(med, vals.max()) + ceiling * 0.02, f"{med:,.0f} ms",
                ha="center", va="bottom", fontsize=11, weight="bold")

    ttl = df["lease_ttl_ms"].mode()
    if not ttl.empty:
        # Above the bars: this is the reference the crash arm is judged against,
        # so it has to be legible where it crosses them.
        ax.axhline(ttl.iloc[0], color="#444444", linestyle="--", linewidth=1.4, zorder=4)
        # Anchored at the left, where no arm has data, rather than over the
        # censored bar on the right.
        ax.text(-0.45, ttl.iloc[0] + ceiling * 0.012, f"lease TTL ({ttl.iloc[0]:,.0f} ms)",
                fontsize=9, color="grey", va="bottom", ha="left")

    ax.set_xticks(range(len(arms)))
    ax.set_xticklabels([ARM_LABELS.get(a, a) for a in arms])
    ax.set_ylabel("gap between dispatches (ms)")
    ax.set_ylim(0, ceiling)
    ax.set_title("What a scheduler failure costs", fontsize=15)
    save(fig, out_dir, "failover_outage", formats)


def plot_ttl_sensitivity(frames: dict[str, pd.DataFrame], out_dir: Path, formats: list[str]) -> None:
    """Crash outage against lease TTL.

    If the outage is really bounded by the lease rather than by something
    incidental, these points lie on y = x. That is the claim, so it is worth
    plotting the identity line and letting the reader check it."""
    rows = []
    for df in frames.values():
        crash = df[(df["arm"] == "crash") & df["recovered"]]
        for ttl, grp in crash.groupby("lease_ttl_ms"):
            renew = grp["lease_renew_ms"].iloc[0] if "lease_renew_ms" in grp.columns else None
            rows.append({"ttl_ms": ttl, "median_ms": grp["outage_ms"].median(),
                         "lo": grp["outage_ms"].min(), "hi": grp["outage_ms"].max(),
                         "renew_ms": renew})
    if len(rows) < 2:
        return
    pts = pd.DataFrame(rows).groupby("ttl_ms", as_index=False).median().sort_values("ttl_ms")

    fig, ax = plt.subplots(figsize=(7.2, 5.4))
    lim = pts["ttl_ms"].max() * 1.15

    # The claim being tested is not "outage equals the TTL" — it is that the
    # outage lies in [TTL - renew, TTL + retry], because the lease has to expire
    # and then a standby has to notice. Shade that envelope where the data
    # records the renewal interval; fall back to the identity line otherwise.
    if pts["renew_ms"].notna().all():
        retry_ms = 1000.0  # the shipped standby poll interval
        band = pts.sort_values("ttl_ms")
        ax.fill_between(band["ttl_ms"], band["ttl_ms"] - band["renew_ms"],
                        band["ttl_ms"] + retry_ms, color="grey", alpha=0.18,
                        label="predicted [TTL − renew, TTL + poll]")
    ax.plot([0, lim], [0, lim], color="grey", linestyle="--", linewidth=1.3,
            label="outage = lease TTL")
    ax.errorbar(pts["ttl_ms"], pts["median_ms"],
                yerr=[pts["median_ms"] - pts["lo"], pts["hi"] - pts["median_ms"]],
                marker="o", markersize=7, capsize=4, linewidth=1.8,
                color="#DD8452", label="measured")
    ax.set_xlabel("lease TTL (ms)")
    ax.set_ylabel("dispatch outage after SIGKILL (ms)")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_title("Failover time scales with the lease TTL", fontsize=14)
    ax.legend(frameon=False, loc="upper left")
    save(fig, out_dir, "failover_ttl_sensitivity", formats)


def summary_table(df: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    rows = []
    for a in ordered(df["arm"].unique(), ARM_ORDER):
        sub = df[df["arm"] == a]
        rec = sub[sub["recovered"]]
        rows.append({
            "arm": a,
            "trials": len(sub),
            "recovered": int(sub["recovered"].sum()),
            # The none arm has no kill, so its outage is zero by construction
            # and only its natural dispatch gap is meaningful.
            "outage_median_ms": None if a == "none" or rec.empty else round(rec["outage_ms"].median(), 1),
            "outage_p95_ms": None if a == "none" or rec.empty else round(rec["outage_ms"].quantile(0.95), 1),
            "outage_max_ms": None if a == "none" or rec.empty else round(rec["outage_ms"].max(), 1),
            "max_gap_median_ms": round(sub["max_gap_ms"].median(), 1),
            "tasks_unfinished_mean": round(sub["unfinished"].mean(), 2),
            "duplicate_completions_total": int(sub["duplicate_completions"].sum()),
        })
    tbl = pd.DataFrame(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    tbl.to_csv(out_dir / "failover_table.csv", index=False)
    print(f"  wrote {out_dir / 'failover_table.csv'}")
    return tbl


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--run", help="run id under --results-dir")
    ap.add_argument("--run-dir", help="path to a run directory, instead of --run")
    ap.add_argument("--ttl-runs", default="",
                    help="comma-separated run ids measured at other lease TTLs")
    ap.add_argument("--format", default="png")
    args = ap.parse_args()

    if args.run_dir:
        run_dir = Path(args.run_dir)
    elif args.run:
        run_dir = Path(args.results_dir) / args.run
    else:
        ap.error("pass --run or --run-dir")

    df = read_run(run_dir)
    check_invariant(df)

    formats = [f.strip() for f in args.format.split(",") if f.strip()]
    out_dir = run_dir / "plots"

    print(f"Loaded {len(df)} trials across {df['arm'].nunique()} arms "
          f"at a {df['lease_ttl_ms'].mode().iloc[0]:,.0f} ms lease TTL")
    setup_style()
    plot_outage(df, out_dir, formats)

    frames = {run_dir.name: df}
    for name in (r.strip() for r in args.ttl_runs.split(",") if r.strip()):
        other = Path(args.results_dir) / name
        frames[name] = read_run(other)
    plot_ttl_sensitivity(frames, out_dir, formats)

    tbl = summary_table(df, out_dir)
    print()
    print(tbl.to_string(index=False))
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
