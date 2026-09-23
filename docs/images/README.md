# Committed charts

The PNGs here are the charts embedded in the top-level `README.md`. They are the
only generated benchmark output committed to the repository apart from the tiny
`results/example/` run, because a README reporting an empirical study should show
its charts without requiring the reader to run a 30-minute benchmark first.

Most were produced from the `final-25rep` dataset: all 16 scheduler/workload
cells at 25 repetitions, 400 experiments, 1.6 million tasks. The `sweep_*`
charts come from the offered-load sweep, and `failover_*` from the failover
experiment, which are separate runs with separate plotting scripts.

## Refreshing them

These are copies, so they can drift from the data. Regenerate and re-copy with:

```bash
make merge OUT=final-25rep RUNS="uniform-25rep stable-25rep heavy-25rep"
make plots RUN=final-25rep
make docs-charts RUN=final-25rep

# The sweep and failover charts come from their own runs and scripts.
make sweep-plots RUN=sweep-loads && make docs-charts RUN=sweep-loads
make failover-plots RUN=failover && make docs-charts RUN=failover
```

`make docs-charts` refuses to run if the named run has no `plots/` directory, so
it cannot silently copy nothing. SVG versions of every chart are produced
alongside the PNGs in `results/<run>/plots/` but are not committed.
