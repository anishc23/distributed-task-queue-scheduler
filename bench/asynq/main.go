// Command baselinebench compares this project's queue against Asynq.
//
// Every throughput number elsewhere in this work is internal: this scheduler
// against that scheduler, this policy against that policy, all inside one
// codebase on one machine. That answers which design choice is better here but
// says nothing about whether "here" is any good. A reader is entitled to ask
// what the absolute numbers mean, and only an outside implementation can
// answer.
//
// Asynq is the right comparison for a specific reason rather than because it is
// popular. It is Go, it is backed by Redis, and its workers pull tasks directly
// from Redis lists with no central scheduler at all. So the difference between
// the two systems is close to the single design decision this project is about:
// centralising the scheduling decision so a global policy can be applied.
// Section 4.6 measured that cost against an internal control that simply
// switched the scheduler off; this measures it against somebody else's
// production implementation of the same architecture.
//
// What the comparison is not: a claim that one queue is better. Asynq does
// things this project does not (scheduled and recurring tasks, task groups, a
// web UI, years of production use), and this project does things Asynq does not
// (a pluggable global policy, deadline-aware and fair-queuing disciplines).
// Where Asynq is faster, that is reported as plainly as anything else.
//
// Fairness is the whole difficulty in a benchmark like this, so the controls
// are explicit:
//
//   - the same Redis server, one run after the other, never concurrently;
//
//   - the same number of tasks, pre-enqueued before any worker starts, so the
//     measurement is drain time and no producer rate is in the way;
//
//   - the same simulated work: both handlers sleep for the same duration;
//
//   - the same total concurrency, expressed the way each system expresses it;
//
//   - alternating order across repetitions, so that whichever runs first does
//     not get a systematically warmer or colder Redis;
//
//   - latency measured identically, from an enqueue timestamp carried in the
//     payload to the moment the handler finishes, computed by this harness in
//     both cases rather than read from either system's own instrumentation.
//
//     go run . --tasks 20000 --repetitions 5
//     go run . --tasks 20000 --exec-ms 5 --concurrency 32 --policy fifo
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/anishc23/distributed-task-queue/internal/worker"
)

const taskType = "bench:sleep"

// payload is what both systems carry. Identical on purpose: the latency figures
// have to come from the same clock reading in both runs, not from whatever each
// system happens to instrument.
type payload struct {
	ID          string `json:"id"`
	ExecMillis  int64  `json:"exec_ms"`
	TenantIndex int    `json:"tenant"`
}

// result is one system's outcome for one repetition.
type result struct {
	System      string
	Rep         int
	Tasks       int
	ExecMS      int64
	Completed   int64
	WallSeconds float64
	Throughput  float64
	LatencyP50  float64
	LatencyP95  float64
	LatencyP99  float64
}

func main() {
	fs := flag.NewFlagSet("baselinebench", flag.ExitOnError)
	redisAddr := fs.String("redis-addr", "localhost:6379", "Redis address")
	tasks := fs.Int("tasks", 20000, "tasks per repetition")
	execList := fs.String("exec-ms-list", "0,1,2,5,10,20",
		"comma-separated simulated task durations to sweep; the comparison depends "+
			"entirely on this, because scheduling overhead only shows when the work is cheap")
	concurrency := fs.Int("concurrency", 32, "total worker concurrency, matched across both systems")
	workers := fs.Int("workers", 4, "worker processes for this project's queue; concurrency is divided between them")
	reps := fs.Int("repetitions", 5, "repetitions per system")
	policy := fs.String("policy", "fifo", "scheduling policy for this project's queue")
	runID := fs.String("run-id", "baseline", "results directory name")
	resultsDir := fs.String("results-dir", "../../results", "where to write results")
	timeout := fs.Duration("timeout", 5*time.Minute, "per-repetition time limit")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "baselinebench compares this queue against Asynq on identical work.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	if *concurrency%*workers != 0 {
		fmt.Fprintf(os.Stderr, "--concurrency (%d) must divide evenly by --workers (%d), "+
			"or the two systems are not running the same number of slots\n", *concurrency, *workers)
		os.Exit(2)
	}

	probe := redis.NewClient(&redis.Options{Addr: *redisAddr, DialTimeout: 3 * time.Second})
	if err := probe.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Redis is required but not reachable at %s: %v\n"+
			"Start one with `make redis-up` from the repository root.\n", *redisAddr, err)
		os.Exit(1)
	}
	probe.Close()

	durations, err := parseDurations(*execList)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var rows []result
	for _, execMS := range durations {
		for rep := 0; rep < *reps; rep++ {
			// Alternate which system goes first. Running one system always first
			// gives it a consistently different cache and allocator state, which
			// is a bias that costs nothing to remove.
			order := []string{"taskqueue", "asynq"}
			if rep%2 == 1 {
				order = []string{"asynq", "taskqueue"}
			}
			for _, system := range order {
				log.Info("run", "system", system, "rep", rep, "exec_ms", execMS)
				var (
					r   result
					err error
				)
				switch system {
				case "asynq":
					r, err = runAsynq(ctx, *redisAddr, *tasks, execMS, *concurrency, *timeout)
				case "taskqueue":
					r, err = runTaskQueue(ctx, *redisAddr, *tasks, execMS, *concurrency, *workers, *policy, *timeout)
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s rep %d failed: %v\n", system, rep, err)
					os.Exit(1)
				}
				r.System, r.Rep, r.Tasks, r.ExecMS = system, rep, *tasks, execMS
				log.Info("run done", "system", system, "rep", rep, "exec_ms", execMS,
					"throughput", fmt.Sprintf("%.0f/s", r.Throughput),
					"p99_ms", fmt.Sprintf("%.1f", r.LatencyP99),
					"completed", r.Completed)
				rows = append(rows, r)
			}
		}
	}

	outDir := filepath.Join(*resultsDir, *runID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cannot create %s: %v\n", outDir, err)
		os.Exit(1)
	}
	path := filepath.Join(outDir, "baseline.csv")
	if err := writeCSV(path, rows, *concurrency, *policy); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Println()
	summarise(os.Stdout, rows, *concurrency)
	fmt.Printf("\nwrote %s\n", path)
}

// runAsynq drives the baseline: enqueue everything, then start a server and
// measure how long it takes to drain.
func runAsynq(ctx context.Context, addr string, tasks int, execMS int64, concurrency int, timeout time.Duration) (result, error) {
	var r result
	opt := asynq.RedisClientOpt{Addr: addr}

	// Asynq keeps its own state under an "asynq:" prefix. Clear it so a
	// previous repetition cannot contribute to this one.
	if err := flushAsynq(ctx, addr); err != nil {
		return r, err
	}

	client := asynq.NewClient(opt)
	defer client.Close()

	for i := 0; i < tasks; i++ {
		body, err := json.Marshal(payload{
			ID:          fmt.Sprintf("a-%06d", i),
			ExecMillis:  execMS,
			TenantIndex: i % 3,
		})
		if err != nil {
			return r, err
		}
		if _, err := client.Enqueue(asynq.NewTask(taskType, body),
			asynq.MaxRetry(3), asynq.Timeout(time.Minute)); err != nil {
			return r, fmt.Errorf("enqueue: %w", err)
		}
	}

	var (
		completed atomic.Int64
		mu        sync.Mutex
		latencies []float64
	)
	done := make(chan struct{})
	var closeOnce sync.Once

	srv := asynq.NewServer(opt, asynq.Config{
		Concurrency: concurrency,
		// A single queue. Asynq's weighted queues are a different mechanism
		// from a global policy and comparing them here would confuse what is
		// being measured.
		Queues:   map[string]int{"default": 1},
		LogLevel: asynq.FatalLevel,
		// Retries are configured identically on both sides, but nothing in
		// this workload fails, so this only guards against a surprise.
		RetryDelayFunc: asynq.RetryDelayFunc(func(n int, e error, t *asynq.Task) time.Duration {
			return time.Second
		}),
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc(taskType, func(ctx context.Context, t *asynq.Task) error {
		var p payload
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return err
		}
		sleep(time.Duration(p.ExecMillis) * time.Millisecond)
		// Latency from the instant the workers were released, which is the
		// same interval the other system is measured over. Every task was
		// already queued before that instant on both sides, so measuring from
		// an individual task's enqueue time would charge each system for its
		// own harness loop rather than for its scheduling.
		lat := float64(time.Now().UnixNano()-releaseAt.Load()) / 1e6
		mu.Lock()
		latencies = append(latencies, lat)
		mu.Unlock()
		if completed.Add(1) >= int64(tasks) {
			closeOnce.Do(func() { close(done) })
		}
		return nil
	})

	// The clock starts when the workers do. Enqueueing is excluded on both
	// sides because it is the harness's cost, not the system's.
	start := time.Now()
	releaseAt.Store(start.UnixNano())
	if err := srv.Start(mux); err != nil {
		return r, fmt.Errorf("start asynq server: %w", err)
	}

	select {
	case <-done:
	case <-time.After(timeout):
		srv.Shutdown()
		return r, fmt.Errorf("asynq drained only %d of %d tasks within %s", completed.Load(), tasks, timeout)
	}
	r.WallSeconds = time.Since(start).Seconds()
	srv.Shutdown()

	r.Completed = completed.Load()
	r.Throughput = float64(r.Completed) / r.WallSeconds
	mu.Lock()
	r.LatencyP50, r.LatencyP95, r.LatencyP99 = quantile(latencies, 0.5), quantile(latencies, 0.95), quantile(latencies, 0.99)
	mu.Unlock()
	return r, nil
}

// runTaskQueue drives this project's queue over the same work.
func runTaskQueue(ctx context.Context, addr string, tasks int, execMS int64, concurrency, workers int, policyName string, timeout time.Duration) (result, error) {
	var r result
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 64})
	defer rdb.Close()

	full := config.Default()
	full.Streams.Namespace = fmt.Sprintf("tqbase:%d", time.Now().UnixNano())
	full.Scheduler.Policy = policyName
	full.Worker.Concurrency = concurrency / workers

	br := broker.NewWithClient(rdb, full.Streams)
	if err := br.EnsureStreams(ctx); err != nil {
		return r, fmt.Errorf("ensure streams: %w", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = br.Reset(c)
		cancel()
	}()

	// Submit everything before anything runs, exactly as the Asynq side does.
	batch := make([]domain.Task, 0, tasks)
	now := time.Now()
	for i := 0; i < tasks; i++ {
		id := fmt.Sprintf("b-%06d", i)
		batch = append(batch, domain.Task{
			ID:          id,
			TenantID:    []string{"A", "B", "C"}[i%3],
			SubmittedAt: now,
			ExecMillis:  execMS,
			Priority:    1,
			Deadline:    now.Add(10 * time.Minute),
			MaxRetries:  3,
			AttemptID:   id + "#0",
		})
	}
	if err := br.SubmitBatch(ctx, batch); err != nil {
		return r, fmt.Errorf("submit: %w", err)
	}

	pol, err := scheduler.New(scheduler.Options{
		Policy:              full.Scheduler.Policy,
		TenantWeights:       full.Scheduler.TenantWeights,
		DefaultTenantWeight: full.Scheduler.DefaultTenantWeight,
	})
	if err != nil {
		return r, fmt.Errorf("build policy: %w", err)
	}
	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   br,
		Policy:   pol,
		Config:   full.Scheduler,
		Logger:   quiet(),
		Metrics:  metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: full.Scheduler.Policy}),
		Consumer: "baseline-sched",
	})
	if err != nil {
		return r, fmt.Errorf("build engine: %w", err)
	}

	var wg sync.WaitGroup
	runCtx, stop := context.WithCancel(ctx)
	defer wg.Wait()
	defer stop()

	// The clock starts when the scheduler and workers do, matching the Asynq
	// side where it starts with the server.
	start := time.Now()
	wg.Add(1)
	go func() { defer wg.Done(); _ = engine.Run(runCtx) }()

	for i := 0; i < workers; i++ {
		w, err := worker.New(worker.Options{
			Broker:   br,
			Config:   full.Worker,
			Recovery: full.Recovery,
			Logger:   quiet(),
			Metrics:  metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: full.Scheduler.Policy}),
		})
		if err != nil {
			return r, fmt.Errorf("build worker: %w", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Run(runCtx) }()
	}

	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return r, fmt.Errorf("queue drained only %d of %d tasks within %s", r.Completed, tasks, timeout)
		}
		stats, err := br.Stats(ctx)
		if err == nil {
			done := stats[broker.StatCompleted] + stats[broker.StatDeadLettered]
			if done >= int64(tasks) {
				r.WallSeconds = time.Since(start).Seconds()
				r.Completed = stats[broker.StatCompleted]
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()

	r.Throughput = float64(r.Completed) / r.WallSeconds
	lat, err := latenciesFromResults(ctx, br, start)
	if err != nil {
		return r, err
	}
	r.LatencyP50, r.LatencyP95, r.LatencyP99 = quantile(lat, 0.5), quantile(lat, 0.95), quantile(lat, 0.99)
	return r, nil
}

// latenciesFromResults measures from the moment the workers started, not from
// each task's submission time.
//
// This is the correction that makes the comparison honest. Every task here was
// submitted before the clock started, so measuring from SubmittedAt would
// charge this system for the time its own harness spent enqueueing, while the
// Asynq side measures from the instant its server starts. Both now measure the
// same interval: release to completion.
func latenciesFromResults(ctx context.Context, br *broker.Broker, start time.Time) ([]float64, error) {
	var out []float64
	cursor := "0"
	for {
		results, next, err := br.ReadResults(ctx, cursor, 2000)
		if err != nil {
			return nil, fmt.Errorf("read results: %w", err)
		}
		if len(results) == 0 {
			break
		}
		for _, res := range results {
			if res.Outcome != domain.OutcomeCompleted || res.FinishedAt.IsZero() {
				continue
			}
			out = append(out, float64(res.FinishedAt.Sub(start).Nanoseconds())/1e6)
		}
		if next == cursor {
			break
		}
		cursor = next
	}
	return out, nil
}

// releaseAt is the instant the workers were let go, shared with the Asynq
// handler so both systems' latencies are measured over the same interval.
var releaseAt atomic.Int64

func flushAsynq(ctx context.Context, addr string) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	iter := rdb.Scan(ctx, 0, "asynq:*", 1000).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan asynq keys: %w", err)
	}
	if len(keys) == 0 {
		return nil
	}
	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("clear asynq state: %w", err)
	}
	return nil
}

// sleep is the simulated work, identical on both sides.
func sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	time.Sleep(d)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if q >= 1 {
		return s[len(s)-1]
	}
	idx := int(q * float64(len(s)-1))
	return s[idx]
}

func median(xs []float64) float64 { return quantile(xs, 0.5) }

// parseDurations reads the --exec-ms-list flag.
func parseDurations(list string) ([]int64, error) {
	var out []int64
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		v, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("--exec-ms-list: %q is not a number", part)
		}
		if v < 0 {
			return nil, fmt.Errorf("--exec-ms-list: %d is negative", v)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--exec-ms-list is empty")
	}
	return out, nil
}

func writeCSV(path string, rows []result, concurrency int, policy string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"system", "rep", "tasks", "exec_ms", "concurrency", "policy",
		"completed", "wall_seconds", "throughput_per_s",
		"latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.System, strconv.Itoa(r.Rep), strconv.Itoa(r.Tasks),
			strconv.FormatInt(r.ExecMS, 10), strconv.Itoa(concurrency), policy,
			strconv.FormatInt(r.Completed, 10),
			strconv.FormatFloat(r.WallSeconds, 'f', 3, 64),
			strconv.FormatFloat(r.Throughput, 'f', 1, 64),
			strconv.FormatFloat(r.LatencyP50, 'f', 2, 64),
			strconv.FormatFloat(r.LatencyP95, 'f', 2, 64),
			strconv.FormatFloat(r.LatencyP99, 'f', 2, 64),
		}); err != nil {
			return err
		}
	}
	return nil
}

func summarise(w io.Writer, rows []result, concurrency int) {
	// Grouped by task duration, because that is the variable the whole
	// comparison turns on: scheduling overhead is only visible while the work
	// is cheap enough not to hide it.
	var durations []int64
	seen := map[int64]bool{}
	for _, r := range rows {
		if !seen[r.ExecMS] {
			seen[r.ExecMS] = true
			durations = append(durations, r.ExecMS)
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	fmt.Fprintf(w, "%8s %12s %12s %10s %12s %12s\n",
		"exec ms", "taskqueue/s", "asynq/s", "delta", "tq p99 ms", "asynq p99 ms")
	for _, d := range durations {
		tq := medianOf(rows, "taskqueue", d, func(r result) float64 { return r.Throughput })
		aq := medianOf(rows, "asynq", d, func(r result) float64 { return r.Throughput })
		tqp := medianOf(rows, "taskqueue", d, func(r result) float64 { return r.LatencyP99 })
		aqp := medianOf(rows, "asynq", d, func(r result) float64 { return r.LatencyP99 })
		delta := ""
		if aq > 0 {
			delta = fmt.Sprintf("%+.1f%%", 100*(tq-aq)/aq)
		}
		fmt.Fprintf(w, "%8d %12.0f %12.0f %10s %12.1f %12.1f\n", d, tq, aq, delta, tqp, aqp)
	}

	// The ceiling both systems are really being read against: with N slots
	// each held for exec_ms, no implementation can exceed N/exec_ms per
	// second. Where both sit near it, the benchmark is measuring sleep.
	fmt.Fprintf(w, "\n%8s %14s %12s %12s\n", "exec ms", "ceiling/s", "taskqueue", "asynq")
	for _, d := range durations {
		if d == 0 {
			fmt.Fprintf(w, "%8d %14s %12s %12s\n", d, "unbounded", "-", "-")
			continue
		}
		ceiling := float64(concurrency) / (float64(d) / 1000.0)
		tq := medianOf(rows, "taskqueue", d, func(r result) float64 { return r.Throughput })
		aq := medianOf(rows, "asynq", d, func(r result) float64 { return r.Throughput })
		fmt.Fprintf(w, "%8d %14.0f %11.1f%% %11.1f%%\n", d, ceiling, 100*tq/ceiling, 100*aq/ceiling)
	}
}

func medianOf(rows []result, system string, execMS int64, pick func(result) float64) float64 {
	var xs []float64
	for _, r := range rows {
		if r.System == system && r.ExecMS == execMS {
			xs = append(xs, pick(r))
		}
	}
	return median(xs)
}
