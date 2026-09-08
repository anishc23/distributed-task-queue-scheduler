//go:build integration

package bench_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/bench"
	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/recovery"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/anishc23/distributed-task-queue/internal/worker"
	"github.com/anishc23/distributed-task-queue/internal/workload"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	if v := os.Getenv("TQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func dialRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr(), PoolSize: 32, DialTimeout: 3 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatalf("Redis is required for integration tests but is not reachable at %s: %v\n"+
			"Start one with `make redis-up` or set TQ_TEST_REDIS_ADDR.", redisAddr(), err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// The benchmark harness must run a real matrix, complete every task and write
// both task-level and aggregate CSV files.
func TestRunnerProducesCompleteResults(t *testing.T) {
	rdb := dialRedis(t)
	cfg := config.Default()
	cfg.Workload.Count = 120
	cfg.Workload.ArrivalRatePerSec = 400
	cfg.Workload.Exec = config.ExecSpec{MinMillis: 5, MaxMillis: 15}
	cfg.Recovery.MinIdle = config.Duration(2 * time.Second)
	cfg.Log.Level = "warn"

	dir := t.TempDir()
	runner, err := bench.NewRunner(bench.Options{
		Base:              cfg,
		RunID:             "itest",
		ResultsDir:        dir,
		Schedulers:        []string{"fifo", "wfq"},
		Workloads:         []string{"uniform", "multi_tenant"},
		Repetitions:       1,
		WorkerProcesses:   2,
		WorkerConcurrency: 4,
		Timeout:           60 * time.Second,
		Logger:            testLogger(),
		Redis:             rdb,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	manifest, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(manifest.Failures) > 0 {
		t.Fatalf("experiments failed: %v", manifest.Failures)
	}
	if len(manifest.Experiments) != 4 {
		t.Fatalf("ran %d experiments, want 4", len(manifest.Experiments))
	}

	for _, e := range manifest.Experiments {
		if e.TimedOut {
			t.Errorf("%s/%s timed out", e.Identity.Scheduler, e.Identity.Workload)
		}
		if e.TasksCompleted != cfg.Workload.Count {
			t.Errorf("%s/%s completed %d of %d tasks",
				e.Identity.Scheduler, e.Identity.Workload, e.TasksCompleted, cfg.Workload.Count)
		}
		if e.Throughput <= 0 {
			t.Errorf("%s/%s throughput = %v", e.Identity.Scheduler, e.Identity.Workload, e.Throughput)
		}
		if e.Latency.P99 < e.Latency.P50 {
			t.Errorf("%s/%s p99 %v < p50 %v", e.Identity.Scheduler, e.Identity.Workload, e.Latency.P99, e.Latency.P50)
		}
		if e.JainFairnessService <= 0 || e.JainFairnessService > 1 {
			t.Errorf("%s/%s Jain index = %v, must lie in (0,1]",
				e.Identity.Scheduler, e.Identity.Workload, e.JainFairnessService)
		}
		if e.WorkerUtilization <= 0 {
			t.Errorf("%s/%s utilisation = %v", e.Identity.Scheduler, e.Identity.Workload, e.WorkerUtilization)
		}
	}

	// Aggregate CSV: one header plus one row per experiment.
	rows := readCSV(t, filepath.Join(dir, "itest", "aggregate.csv"))
	if len(rows) != 5 {
		t.Fatalf("aggregate.csv has %d rows including the header, want 5", len(rows))
	}

	// Every raw file exists and carries the run identity on each row.
	for _, sched := range []string{"fifo", "wfq"} {
		for _, wl := range []string{"uniform", "multi_tenant"} {
			path := filepath.Join(dir, "itest", "raw", fmt.Sprintf("%s_%s_rep0.csv", sched, wl))
			raw := readCSV(t, path)
			if len(raw) != cfg.Workload.Count+1 {
				t.Fatalf("%s has %d rows, want %d", path, len(raw), cfg.Workload.Count+1)
			}
			header := raw[0]
			idxOf := func(name string) int {
				for i, h := range header {
					if h == name {
						return i
					}
				}
				t.Fatalf("column %q missing from %s", name, path)
				return -1
			}
			schedCol, wlCol, runCol := idxOf("scheduler"), idxOf("workload"), idxOf("run_id")
			for _, row := range raw[1:] {
				if row[schedCol] != sched || row[wlCol] != wl || row[runCol] != "itest" {
					t.Fatalf("%s contains a row from a different experiment: %v", path, row)
				}
			}
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "itest", "manifest.json")); err != nil {
		t.Fatalf("manifest.json missing: %v", err)
	}
}

// A worker that abandons every task before acknowledging must not lose work:
// the recovery loop reclaims each delivery, retries it up to the budget, and
// then dead-letters it with failure metadata.
func TestAbandonedTasksAreRetriedThenDeadLettered(t *testing.T) {
	rdb := dialRedis(t)
	cfg := config.Default()
	cfg.Streams.Namespace = fmt.Sprintf("tqtest-dlq-%d", time.Now().UnixNano())
	cfg.Workload.Count = 12
	cfg.Workload.ArrivalRatePerSec = 200
	cfg.Workload.MaxRetries = 1
	cfg.Workload.Exec = config.ExecSpec{MinMillis: 1, MaxMillis: 3}
	cfg.Worker.FailBeforeAckRate = 1 // every attempt is abandoned before ACK
	cfg.Worker.Concurrency = 2
	cfg.Recovery.MinIdle = config.Duration(100 * time.Millisecond)
	cfg.Recovery.Interval = config.Duration(50 * time.Millisecond)
	cfg.Recovery.MaxRetries = 1

	br, completed, dead := runPipeline(t, rdb, cfg, 60*time.Second)
	if completed != 0 {
		t.Fatalf("completed %d tasks, want 0: every attempt was abandoned", completed)
	}
	if dead != int64(cfg.Workload.Count) {
		t.Fatalf("dead-lettered %d tasks, want %d", dead, cfg.Workload.Count)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entries, err := br.ReadDeadLetters(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		// Budget of 1 retry means two attempts in total.
		if e.Attempts != 2 {
			t.Fatalf("task %s dead-lettered after %d attempts, want 2", e.Task.ID, e.Attempts)
		}
		if e.Reason == "" || e.FailedAt.IsZero() || e.LastAttempt == "" {
			t.Fatalf("dead-letter entry lacks metadata: %+v", e)
		}
	}
	stats, err := br.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats[broker.StatRetried] != int64(cfg.Workload.Count) {
		t.Fatalf("retry counter = %d, want %d", stats[broker.StatRetried], cfg.Workload.Count)
	}
	if n, err := br.InFlight(ctx); err != nil || n != 0 {
		t.Fatalf("in-flight after dead-lettering = %d (err %v), want 0", n, err)
	}
}

// With intermittent pre-acknowledgement failures every task must still finish,
// because retries eventually succeed and completion is idempotent.
func TestIntermittentFailuresStillCompleteEveryTask(t *testing.T) {
	rdb := dialRedis(t)
	cfg := config.Default()
	cfg.Streams.Namespace = fmt.Sprintf("tqtest-flaky-%d", time.Now().UnixNano())
	cfg.Workload.Count = 40
	cfg.Workload.ArrivalRatePerSec = 200
	cfg.Workload.MaxRetries = 8
	cfg.Workload.Exec = config.ExecSpec{MinMillis: 1, MaxMillis: 4}
	cfg.Worker.FailBeforeAckRate = 0.35
	cfg.Worker.Concurrency = 4
	cfg.Recovery.MinIdle = config.Duration(100 * time.Millisecond)
	cfg.Recovery.Interval = config.Duration(50 * time.Millisecond)
	cfg.Recovery.MaxRetries = 8

	br, completed, dead := runPipeline(t, rdb, cfg, 90*time.Second)
	if completed+dead != int64(cfg.Workload.Count) {
		t.Fatalf("terminal tasks = %d (completed %d, dead %d), want %d",
			completed+dead, completed, dead, cfg.Workload.Count)
	}
	if completed == 0 {
		t.Fatal("no task completed despite a generous retry budget")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := br.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats[broker.StatRetried] == 0 {
		t.Fatal("injected failures produced no retries")
	}
	// Every task ID appears at most once in the results stream: the idempotency
	// guard holds even though duplicate execution happened.
	results, _, err := br.ReadResults(ctx, "0", 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range results {
		if seen[r.TaskID] {
			t.Fatalf("task %s was recorded twice", r.TaskID)
		}
		seen[r.TaskID] = true
	}
}

// The hardest at-least-once case: the visibility timeout is shorter than the
// task duration, so the recovery loop reclaims tasks that are still legitimately
// running and hands them to a second worker. Both attempts then race to record
// the same completion. This is exactly the scenario the idempotency guard
// exists for, and it is the only way a duplicate completion actually occurs:
// a worker abandoning a task before acknowledging produces duplicate execution
// but not a duplicate completion attempt.
func TestDuplicateCompletionsAreSuppressed(t *testing.T) {
	rdb := dialRedis(t)
	cfg := config.Default()
	cfg.Streams.Namespace = fmt.Sprintf("tqtest-dupe-%d", time.Now().UnixNano())
	cfg.Workload.Count = 30
	cfg.Workload.ArrivalRatePerSec = 100
	cfg.Workload.MaxRetries = 20
	// Tasks take far longer than the visibility timeout, so healthy in-progress
	// work is reclaimed and re-run while the original attempt is still going.
	cfg.Workload.Exec = config.ExecSpec{MinMillis: 250, MaxMillis: 350}
	cfg.Worker.Concurrency = 4
	cfg.Recovery.MinIdle = config.Duration(50 * time.Millisecond)
	cfg.Recovery.Interval = config.Duration(25 * time.Millisecond)
	cfg.Recovery.MaxRetries = 20

	br, completed, dead := runPipeline(t, rdb, cfg, 120*time.Second)
	if completed+dead != int64(cfg.Workload.Count) {
		t.Fatalf("terminal tasks = %d (completed %d, dead %d), want %d",
			completed+dead, completed, dead, cfg.Workload.Count)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stats, err := br.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats[broker.StatDuplicateCompletions] == 0 {
		t.Fatal("expected duplicate completions when the visibility timeout is shorter than the task duration")
	}
	t.Logf("suppressed %d duplicate completions across %d tasks",
		stats[broker.StatDuplicateCompletions], cfg.Workload.Count)

	// Despite the duplicates, each task is recorded exactly once.
	results, _, err := br.ReadResults(ctx, "0", 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range results {
		if seen[r.TaskID] {
			t.Fatalf("task %s was recorded twice despite the idempotency guard", r.TaskID)
		}
		seen[r.TaskID] = true
	}
	if int64(len(results)) != completed {
		t.Fatalf("results stream holds %d records but %d completions were counted", len(results), completed)
	}

	// Tenant service accounting must also be charged exactly once per task.
	_, counts, err := br.TenantService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	if total != completed {
		t.Fatalf("tenant completed counts sum to %d, want %d: a task was counted more than once", total, completed)
	}
}

// runPipeline runs a scheduler, a recovery loop and one worker against a fresh
// namespace, submits the configured workload and waits for every task to reach a
// terminal state. It returns the broker plus the completed and dead-lettered
// counts.
func runPipeline(t *testing.T, rdb *redis.Client, cfg *config.Config, timeout time.Duration) (*broker.Broker, int64, int64) {
	t.Helper()
	log := testLogger()

	br := broker.NewWithClient(rdb, cfg.Streams)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	if err := br.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelCleanup()
		_ = br.Reset(cleanup)
	})
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}

	pol, err := scheduler.New(scheduler.Options{Policy: cfg.Scheduler.Policy, DefaultTenantWeight: 1})
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: pol.Name()})
	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker: br, Policy: pol, Config: cfg.Scheduler,
		Logger: log, Metrics: m, Consumer: "itest-scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := recovery.New(recovery.Options{
		Broker: br, Config: cfg.Recovery, Logger: log, Metrics: m, Consumer: "itest-recovery",
	})
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(worker.Options{
		Broker: br, Config: cfg.Worker, Recovery: cfg.Recovery,
		Logger: log, Metrics: metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: pol.Name()}),
	})
	if err != nil {
		t.Fatal(err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan struct{}, 3)
	go func() { _ = engine.Run(runCtx); done <- struct{}{} }()
	go func() { _ = rec.Run(runCtx); done <- struct{}{} }()
	go func() { _ = w.Run(runCtx); done <- struct{}{} }()
	t.Cleanup(func() {
		cancelRun()
		for i := 0; i < 3; i++ {
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Error("component did not shut down within 20s")
			}
		}
	})

	specs, err := workload.Generate(cfg.Workload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workload.NewProducer(br, log, 64).Submit(ctx, specs); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(timeout - 5*time.Second)
	for {
		completed, err := br.ResultCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		dead, err := br.DeadLetterCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if completed+dead >= int64(len(specs)) {
			return br, completed, dead
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %d completed, %d dead-lettered, want %d terminal", completed, dead, len(specs))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return rows
}
