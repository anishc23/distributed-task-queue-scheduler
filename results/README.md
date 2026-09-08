# Results directory

Benchmark output is written here, one directory per run.

```
results/
└── <run id>/                       # --run-id, or a UTC timestamp like 20240501-143022
    ├── aggregate.csv               # one row per scheduler x workload x repetition
    ├── tenants.csv                 # one row per tenant per experiment
    ├── manifest.json               # full configuration, seeds and every summary
    ├── raw/
    │   └── <scheduler>_<workload>_rep<N>.csv    # one row per completed task
    ├── dead_letters/
    │   └── <scheduler>_<workload>_rep<N>.json   # written only when non-empty
    └── plots/                      # created by scripts/plot_results.py
        ├── latency_percentiles.{png,svg}
        ├── latency_tail_ratio.{png,svg}
        ├── throughput.{png,svg}
        ├── deadline_miss_rate.{png,svg}
        ├── jain_fairness.{png,svg}
        ├── starvation_max_wait.{png,svg}
        ├── worker_utilization.{png,svg}
        ├── tenant_latency_skew.{png,svg}
        ├── priority_starvation.{png,svg}
        ├── summary_table.csv
        └── summary_table.md
```

Every row in every CSV carries `run_id`, `scheduler`, `workload`, `repetition`
and `seed`, so concatenating files from different runs can never silently mix
experiments. Each experiment also runs against its own Redis key namespace
(`bench-<run id>-<scheduler>-<workload>-r<rep>`), which is reset before and
after the experiment.

## What is committed

Generated results are **not** committed; `.gitignore` excludes `results/*`.
The one exception is `results/example/`, a deliberately tiny run kept so the
CSV schemas and chart style are visible without running anything.

`results/example/` was produced with:

```bash
make redis-up
go run ./cmd/benchmark --config experiments/quick.yaml --run-id example \
  --count 150 --workers 2 --concurrency 4 --results-dir results
python3 scripts/plot_results.py --results-dir results --run example
```

It has been trimmed for size: only one raw task-level CSV is kept, only PNG
charts are kept, and the per-tenant detail was removed from `manifest.json`.
A real run keeps all sixteen raw files, both PNG and SVG, and the full manifest.

Because it is 150 tasks with a single repetition, the numbers in
`results/example/` are an illustration of the output format, **not** a result to
cite. Use `make bench-full REPETITIONS=5` for anything reportable.

## Merging runs

When a matrix is filled in across several invocations — for example when one
workload needs more repetitions than the others — combine them for analysis:

```bash
make merge OUT=final-25rep RUNS="uniform-25rep stable-25rep heavy-25rep"
make plots RUN=final-25rep
```

`scripts/merge_runs.py` refuses to merge runs that disagree on any configuration
field affecting results, and refuses to merge runs that cover the same
scheduler/workload/repetition cell twice, since averaging a duplicated cell is
exactly the mixing the harness is designed to prevent. Merged rows keep the
`run_id` of the run that produced them, and the merged `manifest.json` is marked
`"assembled": true` with each source run recorded.

## Regenerating

```bash
make bench-quick            # 16 experiments, a couple of minutes
make plots                  # charts for the newest run
make clean-results          # delete every generated run
```
