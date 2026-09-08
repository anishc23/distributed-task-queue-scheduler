package bench

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/recovery"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/anishc23/distributed-task-queue/internal/worker"
	"github.com/anishc23/distributed-task-queue/internal/workload"
	"github.com/redis/go-redis/v9"
)

// Options configures a benchmark matrix run.
type Options struct {
	// Base is the configuration every experiment starts from. Per-experiment
	// fields (scheduler policy, workload type, seed, namespace) are overridden.
	Base *config.Config
	// RunID names the output directory and is stamped on every row.
	RunID string
	// ResultsDir is the root under which results/<run id>/ is created.
	ResultsDir string
	// Schedulers and Workloads form the experiment matrix.
	Schedulers []string
	Workloads  []string
	// Repetitions is how many times each cell is repeated. Repetition i uses
	// seed BaseSeed+i, so repetitions are different but reproducible.
	Repetitions int
	// LoadMultipliers sweeps offered load: for each value the workload's mean
	// arrival rate is set to that multiple of service capacity, where capacity
	// is derived from the workload's own mean execution time and the total slot
	// count. Empty means "use the configured arrival rate as written", which is
	// the behaviour when no sweep is requested.
	//
	// Deriving the rate per workload matters: the same arrival rate is a
	// different offered load for workloads with different mean service times,
	// and the bursty workload ignores arrival_rate_per_sec entirely.
	LoadMultipliers []float64
	// WorkerProcesses is how many independent worker consumers run.
	WorkerProcesses int
	// WorkerConcurrency is execution slots per worker process.
	WorkerConcurrency int
	// MaxInFlight bounds dispatched-but-unfinished tasks. Zero means "equal to
	// the total number of worker slots", which keeps the scheduler in control
	// of ordering instead of letting the execution stream become the real queue.
	MaxInFlight int
	// Timeout bounds a single experiment. Zero derives one from the workload.
	Timeout time.Duration
	// SettleInterval is how often completion is polled.
	SettleInterval time.Duration
	Logger         *slog.Logger
	Redis          *redis.Client
}

// Runner executes the scheduler x workload matrix.
type Runner struct {
	opts Options
	log  *slog.Logger
}

// Manifest records everything needed to interpret and reproduce a run.
type Manifest struct {
	RunID             string         `json:"run_id"`
	StartedAt         time.Time      `json:"started_at"`
	FinishedAt        time.Time      `json:"finished_at"`
	Schedulers        []string       `json:"schedulers"`
	Workloads         []string       `json:"workloads"`
	Repetitions       int            `json:"repetitions"`
	LoadMultipliers   []float64      `json:"load_multipliers,omitempty"`
	BaseSeed          int64          `json:"base_seed"`
	TasksPerRun       int            `json:"tasks_per_run"`
	WorkerProcesses   int            `json:"worker_processes"`
	WorkerConcurrency int            `json:"worker_concurrency"`
	MaxInFlight       int            `json:"max_in_flight"`
	RedisAddr         string         `json:"redis_addr"`
	Config            *config.Config `json:"config"`
	Experiments       []Summary      `json:"experiments"`
	Failures          []string       `json:"failures,omitempty"`
}

// NewRunner validates options and builds a runner.
func NewRunner(opts Options) (*Runner, error) {
	switch {
	case opts.Base == nil:
		return nil, errors.New("benchmark: base configuration is required")
	case opts.Redis == nil:
		return nil, errors.New("benchmark: redis client is required")
	case opts.Logger == nil:
		return nil, errors.New("benchmark: logger is required")
	case opts.RunID == "":
		return nil, errors.New("benchmark: run id is required")
	case len(opts.Schedulers) == 0:
		return nil, errors.New("benchmark: at least one scheduler is required")
	case len(opts.Workloads) == 0:
		return nil, errors.New("benchmark: at least one workload is required")
	case opts.Repetitions <= 0:
		return nil, fmt.Errorf("benchmark: repetitions must be > 0, got %d", opts.Repetitions)
	case opts.WorkerProcesses <= 0:
		return nil, fmt.Errorf("benchmark: worker processes must be > 0, got %d", opts.WorkerProcesses)
	case opts.WorkerConcurrency <= 0:
		return nil, fmt.Errorf("benchmark: worker concurrency must be > 0, got %d", opts.WorkerConcurrency)
	}
	for _, s := range opts.Schedulers {
		if !scheduler.Valid(s) {
			return nil, fmt.Errorf("benchmark: unknown scheduler %q, expected one of %v", s, scheduler.Names())
		}
	}
	for _, w := range opts.Workloads {
		if !config.ValidWorkload(w) {
			return nil, fmt.Errorf("benchmark: unknown workload %q, expected one of %v", w, config.WorkloadTypes())
		}
	}
	for _, l := range opts.LoadMultipliers {
		if l <= 0 {
			return nil, fmt.Errorf("benchmark: load multipliers must be > 0, got %g", l)
		}
	}
	if opts.SettleInterval <= 0 {
		opts.SettleInterval = 100 * time.Millisecond
	}
	if opts.ResultsDir == "" {
		opts.ResultsDir = "results"
	}
	return &Runner{opts: opts, log: opts.Logger}, nil
}

// RunDir is the directory holding this run's output.
func (r *Runner) RunDir() string { return filepath.Join(r.opts.ResultsDir, r.opts.RunID) }

// Run executes the full matrix and writes CSV output.
func (r *Runner) Run(ctx context.Context) (*Manifest, error) {
	runDir := r.RunDir()
	if err := os.MkdirAll(filepath.Join(runDir, "raw"), 0o755); err != nil {
		return nil, fmt.Errorf("create run directory: %w", err)
	}

	agg, err := NewAggregateWriter(filepath.Join(runDir, "aggregate.csv"))
	if err != nil {
		return nil, err
	}
	defer agg.Close()

	tenantCSV, err := NewTenantWriter(filepath.Join(runDir, "tenants.csv"))
	if err != nil {
		return nil, err
	}
	defer tenantCSV.Close()

	manifest := &Manifest{
		RunID:             r.opts.RunID,
		StartedAt:         time.Now().UTC(),
		Schedulers:        r.opts.Schedulers,
		Workloads:         r.opts.Workloads,
		Repetitions:       r.opts.Repetitions,
		LoadMultipliers:   r.opts.LoadMultipliers,
		BaseSeed:          r.opts.Base.Workload.Seed,
		TasksPerRun:       r.opts.Base.Workload.Count,
		WorkerProcesses:   r.opts.WorkerProcesses,
		WorkerConcurrency: r.opts.WorkerConcurrency,
		MaxInFlight:       r.effectiveMaxInFlight(),
		RedisAddr:         r.opts.Redis.Options().Addr,
		Config:            r.opts.Base,
	}

	loads := r.opts.LoadMultipliers
	if len(loads) == 0 {
		loads = []float64{0} // 0 means "leave the configured arrival rate alone"
	}
	total := len(r.opts.Schedulers) * len(r.opts.Workloads) * len(loads) * r.opts.Repetitions
	done := 0
	for _, sched := range r.opts.Schedulers {
		for _, wl := range r.opts.Workloads {
			for _, load := range loads {
				for rep := 0; rep < r.opts.Repetitions; rep++ {
					if err := ctx.Err(); err != nil {
						manifest.FinishedAt = time.Now().UTC()
						_ = WriteJSON(filepath.Join(runDir, "manifest.json"), manifest)
						return manifest, fmt.Errorf("benchmark interrupted after %d/%d experiments: %w", done, total, err)
					}
					done++
					id := RunIdentity{
						RunID:       r.opts.RunID,
						Scheduler:   sched,
						Workload:    wl,
						Repetition:  rep,
						Seed:        r.opts.Base.Workload.Seed + int64(rep),
						OfferedLoad: load,
					}
					r.log.Info("starting experiment",
						"progress", fmt.Sprintf("%d/%d", done, total),
						"scheduler", sched, "workload", wl, "load", load, "repetition", rep, "seed", id.Seed)

					summary, err := r.runOne(ctx, id, runDir)
					if err != nil {
						msg := fmt.Sprintf("%s/%s load %.2f rep %d: %v", sched, wl, load, rep, err)
						manifest.Failures = append(manifest.Failures, msg)
						r.log.Error("experiment failed", "scheduler", sched, "workload", wl,
							"load", load, "repetition", rep, "error", err)
						continue
					}
					if err := agg.Append(summary); err != nil {
						return manifest, err
					}
					if err := tenantCSV.Append(summary); err != nil {
						return manifest, err
					}
					manifest.Experiments = append(manifest.Experiments, summary)
					r.log.Info("experiment complete",
						"scheduler", sched, "workload", wl,
						"offered_load", fmt.Sprintf("%.2f", summary.OfferedLoad),
						"repetition", rep,
						"completed", summary.TasksCompleted,
						"dead_lettered", summary.TasksDeadLettered,
						"p99_latency_s", fmt.Sprintf("%.3f", summary.Latency.P99),
						"throughput_per_s", fmt.Sprintf("%.1f", summary.Throughput),
						"deadline_miss_rate", fmt.Sprintf("%.3f", summary.DeadlineMissRate),
						"jain", fmt.Sprintf("%.3f", summary.JainFairnessService),
						"timed_out", summary.TimedOut)
				}
			}
		}
	}

	manifest.FinishedAt = time.Now().UTC()
	if err := WriteJSON(filepath.Join(runDir, "manifest.json"), manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func (r *Runner) effectiveMaxInFlight() int {
	if r.opts.MaxInFlight > 0 {
		return r.opts.MaxInFlight
	}
	return r.opts.WorkerProcesses * r.opts.WorkerConcurrency
}

// experimentConfig derives the configuration for one matrix cell.
func (r *Runner) experimentConfig(id RunIdentity) *config.Config {
	cfg := *r.opts.Base
	cfg.Streams = r.opts.Base.Streams
	cfg.Streams.Namespace = fmt.Sprintf("bench-%s-%s-%s-l%s-r%d",
		id.RunID, id.Scheduler, id.Workload, loadTag(id.OfferedLoad), id.Repetition)
	cfg.Scheduler = r.opts.Base.Scheduler
	cfg.Scheduler.Policy = id.Scheduler
	cfg.Scheduler.MaxInFlight = r.effectiveMaxInFlight()
	cfg.Scheduler.MetricsAddr = ""
	cfg.Worker = r.opts.Base.Worker
	cfg.Worker.Concurrency = r.opts.WorkerConcurrency
	cfg.Worker.MetricsAddr = ""
	cfg.Workload = r.opts.Base.Workload
	cfg.Workload.Type = id.Workload
	cfg.Workload.Seed = id.Seed
	return &cfg
}

// runOne executes a single scheduler x workload x repetition experiment in an
// isolated Redis namespace.
func (r *Runner) runOne(ctx context.Context, id RunIdentity, runDir string) (Summary, error) {
	cfg := r.experimentConfig(id)
	log := r.log.With("scheduler", id.Scheduler, "workload", id.Workload, "repetition", id.Repetition)

	// Generate once to learn the workload's own mean service time, then, if a
	// load multiplier was requested, derive the arrival rate that produces that
	// offered load and regenerate. Changing the arrival rate provably does not
	// change which tasks are generated - only when they arrive - so this varies
	// exactly one thing. See TestScalingRateDoesNotChangeTheTaskMix.
	specs, err := workload.Generate(cfg.Workload)
	if err != nil {
		return Summary{}, err
	}
	plan := workload.Summarise(specs)

	slots := r.opts.WorkerProcesses * r.opts.WorkerConcurrency
	capacity := workload.Capacity(plan.MeanExecMS, slots)
	if id.OfferedLoad > 0 {
		rate, err := workload.RateForLoad(id.OfferedLoad, plan.MeanExecMS, slots)
		if err != nil {
			return Summary{}, err
		}
		scaled, err := workload.WithArrivalRate(cfg.Workload, rate)
		if err != nil {
			return Summary{}, err
		}
		cfg.Workload = scaled
		specs, err = workload.Generate(cfg.Workload)
		if err != nil {
			return Summary{}, err
		}
		plan = workload.Summarise(specs)
	}
	meanArrival := workload.MeanArrivalRatePerSec(cfg.Workload)
	offered := workload.OfferedLoad(meanArrival, capacity)

	br := broker.NewWithClient(r.opts.Redis, cfg.Streams)
	// A fresh namespace per experiment guarantees results are never mixed.
	if err := br.Reset(ctx); err != nil {
		return Summary{}, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := br.Reset(cleanupCtx); err != nil {
			log.Warn("namespace cleanup failed", "error", err)
		}
	}()
	if err := br.EnsureStreams(ctx); err != nil {
		return Summary{}, err
	}

	pol, err := scheduler.New(scheduler.Options{
		Policy:              cfg.Scheduler.Policy,
		TenantWeights:       cfg.Scheduler.TenantWeights,
		DefaultTenantWeight: cfg.Scheduler.DefaultTenantWeight,
	})
	if err != nil {
		return Summary{}, err
	}

	timeout := r.opts.Timeout
	if timeout <= 0 {
		timeout = deriveTimeout(plan, slots)
	}
	expCtx, cancelExp := context.WithCancel(ctx)
	defer cancelExp()

	schedMetrics := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: id.Scheduler})
	schedMetrics.PreloadTenants(cfg.Workload.TenantIDs())

	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   br,
		Policy:   pol,
		Config:   cfg.Scheduler,
		Logger:   log,
		Metrics:  schedMetrics,
		Consumer: "bench-scheduler",
	})
	if err != nil {
		return Summary{}, err
	}

	recLoop, err := recovery.New(recovery.Options{
		Broker:   br,
		Config:   cfg.Recovery,
		Logger:   log,
		Metrics:  schedMetrics,
		Consumer: "bench-recovery",
	})
	if err != nil {
		return Summary{}, err
	}

	workers := make([]*worker.Worker, 0, r.opts.WorkerProcesses)
	for i := 0; i < r.opts.WorkerProcesses; i++ {
		wcfg := cfg.Worker
		wcfg.Name = fmt.Sprintf("bench-worker-%d", i)
		wm := metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: id.Scheduler})
		w, err := worker.New(worker.Options{
			Broker:   br,
			Config:   wcfg,
			Recovery: cfg.Recovery,
			Logger:   log,
			Metrics:  wm,
		})
		if err != nil {
			return Summary{}, err
		}
		workers = append(workers, w)
	}

	var wg sync.WaitGroup
	runComponent := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(expCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("component stopped with an error", "component", name, "error", err)
			}
		}()
	}
	runComponent("scheduler", engine.Run)
	runComponent("recovery", recLoop.Run)
	for _, w := range workers {
		runComponent("worker", w.Run)
	}

	if err := waitReady(expCtx, engine, workers, 10*time.Second); err != nil {
		cancelExp()
		wg.Wait()
		return Summary{}, err
	}

	producer := workload.NewProducer(br, log, 128)
	experimentStart := time.Now()
	submitStats, err := producer.Submit(expCtx, specs)
	if err != nil {
		cancelExp()
		wg.Wait()
		return Summary{}, err
	}

	timedOut := waitForCompletion(expCtx, br, len(specs), r.opts.SettleInterval, timeout, log)
	experimentDuration := time.Since(experimentStart)

	var busySeconds float64
	for _, w := range workers {
		busySeconds += w.BusyTime().Seconds()
	}
	var failures int64
	for _, w := range workers {
		failures += w.Failures()
	}

	cancelExp()
	wg.Wait()

	collectCtx, cancelCollect := context.WithTimeout(context.WithoutCancel(ctx), 60*time.Second)
	defer cancelCollect()

	results, err := collectResults(collectCtx, br)
	if err != nil {
		return Summary{}, err
	}
	stats, err := br.Stats(collectCtx)
	if err != nil {
		return Summary{}, err
	}
	deadLetters, err := br.ReadDeadLetters(collectCtx, 10000)
	if err != nil {
		return Summary{}, err
	}

	summary := Summarise(SummaryInput{
		Identity:             id,
		Tenants:              cfg.Workload.TenantIDs(),
		Results:              results,
		TasksPlanned:         len(specs),
		TasksSubmitted:       submitStats.Submitted,
		TasksDeadLettered:    len(deadLetters),
		TasksRetried:         int(stats[broker.StatRetried]),
		TaskFailures:         int(failures),
		DuplicateCompletions: int(stats[broker.StatDuplicateCompletions]),
		TimedOut:             timedOut,
		WorkerProcesses:      r.opts.WorkerProcesses,
		WorkerConcurrency:    r.opts.WorkerConcurrency,
		MaxInFlight:          cfg.Scheduler.MaxInFlight,
		ExperimentDuration:   experimentDuration,
		WorkerBusySeconds:    busySeconds,
		WorkerSlotSeconds:    experimentDuration.Seconds() * float64(slots),
		ProducerMaxLag:       submitStats.MaxLag,
		ProducerMeanLag:      submitStats.MeanLag,
		ArrivalRatePerSec:    meanArrival,
		ExecMinMillis:        cfg.Workload.Exec.MinMillis,
		ExecMaxMillis:        cfg.Workload.Exec.MaxMillis,
		OfferedLoad:          offered,
		Capacity:             capacity,
		MeanExecMillis:       plan.MeanExecMS,
	})

	base := fmt.Sprintf("%s_%s_l%s_rep%d", id.Scheduler, id.Workload, loadTag(offered), id.Repetition)
	rawID := id
	rawID.OfferedLoad = offered
	if err := WriteRawCSV(filepath.Join(runDir, "raw", base+".csv"), rawID, results); err != nil {
		return Summary{}, err
	}
	if len(deadLetters) > 0 {
		if err := WriteJSON(filepath.Join(runDir, "dead_letters", base+".json"), deadLetters); err != nil {
			return Summary{}, err
		}
	}
	return summary, nil
}

// loadTag renders an offered load for use in a file or namespace name.
func loadTag(load float64) string {
	return strings.ReplaceAll(fmt.Sprintf("%.2f", load), ".", "p")
}

// waitReady blocks until every component reports readiness.
func waitReady(ctx context.Context, engine *scheduler.Engine, workers []*worker.Worker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ready := engine.Ready()
		for _, w := range workers {
			ready = ready && w.Ready()
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("components did not become ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// waitForCompletion polls until every submitted task reached a terminal state or
// the timeout expires. It returns true when the experiment timed out.
func waitForCompletion(ctx context.Context, br *broker.Broker, expected int, interval, timeout time.Duration, log *slog.Logger) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastLog := time.Now()

	for {
		completed, err := br.ResultCount(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return true
			}
			log.Warn("completion poll failed", "error", err)
		}
		dead, err := br.DeadLetterCount(ctx)
		if err != nil && ctx.Err() != nil {
			return true
		}
		if completed+dead >= int64(expected) {
			return false
		}
		if time.Now().After(deadline) {
			log.Warn("experiment timed out before all tasks reached a terminal state",
				"completed", completed, "dead_lettered", dead, "expected", expected, "timeout", timeout.String())
			return true
		}
		if time.Since(lastLog) > 10*time.Second {
			log.Info("waiting for completion", "completed", completed, "dead_lettered", dead, "expected", expected)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return true
		case <-ticker.C:
		}
	}
}

// collectResults drains the result stream in pages.
func collectResults(ctx context.Context, br *broker.Broker) ([]domain.Result, error) {
	var out []domain.Result
	cursor := "0"
	for {
		page, next, err := br.ReadResults(ctx, cursor, 1000)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}
		out = append(out, page...)
		cursor = next
	}
}

// deriveTimeout estimates a generous per-experiment bound from the workload: the
// arrival span plus the serial service time spread across worker slots, tripled,
// with a floor.
func deriveTimeout(plan workload.Summary, slots int) time.Duration {
	if slots <= 0 {
		slots = 1
	}
	service := time.Duration(plan.TotalExecMS/int64(slots)) * time.Millisecond
	est := plan.Span + service
	est *= 3
	if est < 30*time.Second {
		est = 30 * time.Second
	}
	if est > 30*time.Minute {
		est = 30 * time.Minute
	}
	return est
}
