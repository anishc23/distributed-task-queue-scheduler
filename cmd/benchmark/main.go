// Command benchmark runs the scheduler x workload experiment matrix end to end
// against a real Redis and writes task-level and aggregate CSV output.
//
// Each experiment runs a scheduler, a recovery loop and a configurable number of
// workers in-process against an isolated Redis key namespace, submits a finite
// workload, waits for every task to reach a terminal state, and then collects
// the task-level records that the workers wrote to the results stream.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/bench"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/redis/go-redis/v9"
)

func main() {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)

	schedulersFlag := fs.String("schedulers", strings.Join(scheduler.Names(), ","), "comma-separated scheduling policies to test")
	workloadsFlag := fs.String("workloads", strings.Join(config.WorkloadTypes(), ","), "comma-separated workload types to test")
	repetitions := fs.Int("repetitions", 1, "repetitions per matrix cell; repetition i uses seed base_seed+i")
	count := fs.Int("count", 0, "tasks per experiment (overrides workload.count)")
	seed := fs.Int64("seed", -1, "base random seed (overrides workload.seed)")
	rate := fs.Float64("rate", 0, "mean arrival rate in tasks per second (overrides workload.arrival_rate_per_sec)")
	loads := fs.String("loads", "", "comma-separated offered-load multipliers to sweep, e.g. 0.5,1,1.5,2 .\n"+
		"\tFor each value the workload's mean arrival rate is set to that multiple of\n"+
		"\tservice capacity (worker slots / mean execution time), computed per workload\n"+
		"\tso that workloads with different service times are compared at equal load.\n"+
		"\tEmpty uses the configured arrival rate as written.")
	workers := fs.Int("workers", 2, "worker processes per experiment")
	concurrency := fs.Int("concurrency", 4, "execution slots per worker process")
	maxInFlight := fs.Int("max-in-flight", 0, "cap on dispatched-but-unfinished tasks; 0 uses the total worker slot count")
	failRate := fs.Float64("fail-rate", -1, "probability a task reports an application error, within [0,1]")
	failBeforeAck := fs.Float64("fail-before-ack-rate", -1, "probability a task is abandoned before acknowledgement, within [0,1]")
	timeout := fs.Duration("timeout", 0, "per-experiment timeout; zero derives one from the workload size")
	resultsDir := fs.String("results-dir", "results", "root directory for benchmark output")
	runID := fs.String("run-id", "", "run identifier; defaults to a UTC timestamp")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "benchmark runs the scheduler x workload experiment matrix.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *count > 0 {
		cfg.Workload.Count = *count
	}
	if *seed >= 0 {
		cfg.Workload.Seed = *seed
	}
	if *rate > 0 {
		cfg.Workload.ArrivalRatePerSec = *rate
	}
	if *failRate >= 0 {
		cfg.Worker.FailRate = *failRate
	}
	if *failBeforeAck >= 0 {
		cfg.Worker.FailBeforeAckRate = *failBeforeAck
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	log := cli.NewLogger(cfg.Log, "benchmark")
	ctx, stop := cli.SignalContext()
	defer stop()

	id := *runID
	if id == "" {
		id = time.Now().UTC().Format("20060102-150405")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		DialTimeout:  cfg.Redis.DialTimeout.D(),
		ReadTimeout:  cfg.Redis.ReadTimeout.D(),
		WriteTimeout: cfg.Redis.WriteTimeout.D(),
		PoolSize:     maxInt(cfg.Redis.PoolSize, (*workers)*(*concurrency)+8),
	})
	defer rdb.Close()

	// Dependency validation: fail immediately with an actionable message rather
	// than producing an empty result set.
	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		fmt.Fprintf(os.Stderr,
			"Redis is not reachable at %s: %v\n\n"+
				"Start it with one of:\n"+
				"  make redis-up            # docker compose up -d redis\n"+
				"  docker run -p 6379:6379 redis:7-alpine\n"+
				"Then re-run, or point elsewhere with --redis-addr host:port.\n",
			cfg.Redis.Addr, err)
		os.Exit(1)
	}
	info, err := rdb.Info(pingCtx, "server").Result()
	if err == nil {
		log.Info("redis reachable", "addr", cfg.Redis.Addr, "version", redisVersion(info))
	}

	var loadList []float64
	for _, v := range cli.SplitList(*loads) {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid --loads value %q: %v\n", v, err)
			os.Exit(2)
		}
		loadList = append(loadList, f)
	}

	runner, err := bench.NewRunner(bench.Options{
		Base:              cfg,
		RunID:             id,
		ResultsDir:        *resultsDir,
		Schedulers:        cli.SplitList(*schedulersFlag),
		Workloads:         cli.SplitList(*workloadsFlag),
		Repetitions:       *repetitions,
		LoadMultipliers:   loadList,
		WorkerProcesses:   *workers,
		WorkerConcurrency: *concurrency,
		MaxInFlight:       *maxInFlight,
		Timeout:           *timeout,
		Logger:            log,
		Redis:             rdb,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchmark configuration error: %v\n", err)
		os.Exit(2)
	}

	log.Info("benchmark starting",
		"run_id", id,
		"schedulers", cli.SplitList(*schedulersFlag),
		"workloads", cli.SplitList(*workloadsFlag),
		"repetitions", *repetitions,
		"tasks_per_experiment", cfg.Workload.Count,
		"load_multipliers", loadList,
		"workers", *workers,
		"concurrency", *concurrency,
		"results_dir", runner.RunDir())

	manifest, runErr := runner.Run(ctx)
	if manifest != nil {
		printTable(os.Stdout, manifest)
		fmt.Printf("\nAggregate CSV: %s\n", filepath.Join(runner.RunDir(), "aggregate.csv"))
		fmt.Printf("Task-level CSVs: %s\n", filepath.Join(runner.RunDir(), "raw"))
		fmt.Printf("Manifest: %s\n", filepath.Join(runner.RunDir(), "manifest.json"))
		fmt.Printf("\nGenerate plots with:\n  make plots RUN=%s\n", id)
	}
	if runErr != nil {
		cli.Fail(log, "benchmark did not complete", runErr)
	}
	if len(manifest.Failures) > 0 {
		log.Error("some experiments failed", "count", len(manifest.Failures))
		os.Exit(1)
	}
}

// printTable renders a compact human-readable summary of the matrix.
func printTable(out *os.File, m *bench.Manifest) {
	if len(m.Experiments) == 0 {
		return
	}
	rows := append([]bench.Summary(nil), m.Experiments...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Identity.Workload != rows[j].Identity.Workload {
			return rows[i].Identity.Workload < rows[j].Identity.Workload
		}
		if rows[i].Identity.Scheduler != rows[j].Identity.Scheduler {
			return rows[i].Identity.Scheduler < rows[j].Identity.Scheduler
		}
		if rows[i].OfferedLoad != rows[j].OfferedLoad {
			return rows[i].OfferedLoad < rows[j].OfferedLoad
		}
		return rows[i].Identity.Repetition < rows[j].Identity.Repetition
	})

	fmt.Fprintf(out, "\nRun %s: %d experiments\n\n", m.RunID, len(rows))
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "workload\tscheduler\tload\trep\tdone\tp50 s\tp95 s\tp99 s\tmaxwait s\tthr/s\tmiss\tutil\tjain\tdead")
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%s\t%.2f\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.1f\t%.3f\t%.2f\t%.3f\t%d\n",
			r.Identity.Workload, r.Identity.Scheduler, r.OfferedLoad, r.Identity.Repetition,
			r.TasksCompleted, r.Latency.P50, r.Latency.P95, r.Latency.P99, r.Wait.Max,
			r.Throughput, r.DeadlineMissRate, r.WorkerUtilization, r.JainFairnessService,
			r.TasksDeadLettered)
	}
	w.Flush()
}

func redisVersion(info string) string {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		}
	}
	return "unknown"
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
