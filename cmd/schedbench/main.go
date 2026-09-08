// Command schedbench measures the central scheduler's own throughput ceiling.
//
// Every other benchmark in this project measures the queue with workers that
// sleep, so the numbers are bounded by the simulated work, not by the scheduler.
// That leaves the central design question unanswered: the scheduler is a
// deliberate single-writer bottleneck, so how much can it actually dispatch
// before it becomes the limit?
//
// This command answers that by removing the workers entirely and driving the
// scheduler's critical path directly. It reports three things:
//
//	admit      the admission path: policy ranking plus the atomic admit script,
//	           one Redis round trip per task
//	dispatch   the dispatch path: the atomic ZPOPMIN + XADD script, swept across
//	           batch sizes, which is the parameter that trades round trips
//	           against per-call work
//	engine     the whole scheduler process end to end, ingest and dispatch loops
//	           running together against a pre-filled ingress stream
//
// The headline number is derived from the dispatch ceiling: a scheduler that
// sustains D dispatches per second can keep D * mean_execution_seconds worker
// slots busy before it, rather than the workers, is the constraint.
//
// Usage:
//
//	schedbench --tasks 20000
//	schedbench --tasks 50000 --batches 1,8,32,64,128,256 --policies fifo,wfq
//	schedbench --csv results/ceiling/ceiling.csv
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/workload"
)

// result is one measured phase.
type result struct {
	Phase     string
	Policy    string
	Batch     int
	Tasks     int
	Elapsed   time.Duration
	RatePerS  float64
	SlotsFed  float64 // worker slots this rate could keep busy at meanExecMS
	MeanExecM float64
}

func main() {
	fs := flag.NewFlagSet("schedbench", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)

	tasks := fs.Int("tasks", 20000, "tasks per measured phase")
	batchesFlag := fs.String("batches", "1,8,32,64,128,256", "dispatch batch sizes to sweep")
	policiesFlag := fs.String("policies", strings.Join(scheduler.Names(), ","), "scheduling policies to measure")
	meanExec := fs.Float64("mean-exec-ms", 50, "mean task execution time used to convert a dispatch rate into worker slots")
	csvPath := fs.String("csv", "", "also write the measurements to this CSV file")
	skipEngine := fs.Bool("skip-engine", false, "skip the end-to-end engine phase")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "schedbench measures the scheduler's dispatch ceiling with no workers attached.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *tasks <= 0 {
		fmt.Fprintln(os.Stderr, "--tasks must be > 0")
		os.Exit(2)
	}

	var batches []int
	for _, b := range cli.SplitList(*batchesFlag) {
		n, err := strconv.Atoi(b)
		if err != nil || n <= 0 {
			fmt.Fprintf(os.Stderr, "invalid --batches value %q\n", b)
			os.Exit(2)
		}
		batches = append(batches, n)
	}
	policies := cli.SplitList(*policiesFlag)
	for _, p := range policies {
		if !scheduler.Valid(p) {
			fmt.Fprintf(os.Stderr, "unknown policy %q, expected one of %v\n", p, scheduler.Names())
			os.Exit(2)
		}
	}

	log := cli.NewLogger(cfg.Log, "schedbench")
	ctx, stop := cli.SignalContext()
	defer stop()

	// A dedicated namespace so this never collides with a benchmark run.
	cfg.Streams.Namespace = fmt.Sprintf("ceiling-%d", time.Now().UnixNano())
	cfg.Scheduler.MetricsAddr = ""
	cfg.Worker.MetricsAddr = ""

	br, err := broker.New(cfg)
	if err != nil {
		cli.Fail(log, "cannot create broker", err)
	}
	defer br.Close()
	if err := br.WaitReady(ctx, 10*time.Second, log); err != nil {
		cli.Fail(log, "cannot reach Redis", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = br.Reset(cleanup)
	}()

	specs := buildSpecs(cfg, *tasks)
	fmt.Printf("scheduler throughput ceiling\n")
	fmt.Printf("  redis      %s\n", cfg.Redis.Addr)
	fmt.Printf("  tasks      %d per phase\n", *tasks)
	fmt.Printf("  namespace  %s\n\n", cfg.Streams.Namespace)

	var results []result
	for _, name := range policies {
		pol, err := scheduler.New(scheduler.Options{
			Policy:              name,
			TenantWeights:       cfg.Scheduler.TenantWeights,
			DefaultTenantWeight: cfg.Scheduler.DefaultTenantWeight,
		})
		if err != nil {
			cli.Fail(log, "cannot build policy", err)
		}

		r, err := measureAdmit(ctx, br, pol, specs, *meanExec)
		if err != nil {
			cli.Fail(log, "admit phase failed", err)
		}
		results = append(results, r)

		for _, b := range batches {
			r, err := measureDispatch(ctx, br, pol, specs, b, *meanExec)
			if err != nil {
				cli.Fail(log, "dispatch phase failed", err)
			}
			results = append(results, r)
		}

		if !*skipEngine {
			r, err := measureEngine(ctx, br, cfg, name, specs, *meanExec, log)
			if err != nil {
				cli.Fail(log, "engine phase failed", err)
			}
			results = append(results, r)
		}
	}

	printResults(results, *meanExec)
	if *csvPath != "" {
		if err := writeCSV(*csvPath, results); err != nil {
			cli.Fail(log, "cannot write CSV", err)
		}
		fmt.Printf("\nwrote %s\n", *csvPath)
	}
}

// buildSpecs generates a fixed task set. Execution duration is irrelevant here
// because no worker ever runs them; only the scheduler's own work is measured.
func buildSpecs(cfg *config.Config, n int) []domain.Task {
	w := cfg.Workload
	w.Count = n
	w.Type = config.WorkloadMultiTenant // exercises the per-tenant WFQ state
	specs, err := workload.Generate(w)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	out := make([]domain.Task, 0, len(specs))
	for i, s := range specs {
		out = append(out, s.TaskAt(now.Add(time.Duration(i)*time.Microsecond)))
	}
	return out
}

// measureAdmit times the admission path: rank the task with the policy, then run
// the atomic admit script. One Redis round trip per task.
func measureAdmit(ctx context.Context, br *broker.Broker, pol policy.Policy, tasks []domain.Task, meanExec float64) (result, error) {
	if err := br.Reset(ctx); err != nil {
		return result{}, err
	}
	if err := br.EnsureStreams(ctx); err != nil {
		return result{}, err
	}
	start := time.Now()
	for _, t := range tasks {
		if _, err := br.Admit(ctx, t, pol.Rank(t)); err != nil {
			return result{}, err
		}
	}
	elapsed := time.Since(start)
	return newResult("admit", pol.Name(), 0, len(tasks), elapsed, meanExec), nil
}

// measureDispatch times the dispatch path at one batch size: repeatedly run the
// atomic ZPOPMIN + XADD script until the pending set is empty. max_in_flight is
// unlimited because there are no workers to acknowledge anything.
func measureDispatch(ctx context.Context, br *broker.Broker, pol policy.Policy, tasks []domain.Task, batch int, meanExec float64) (result, error) {
	if err := br.Reset(ctx); err != nil {
		return result{}, err
	}
	if err := br.EnsureStreams(ctx); err != nil {
		return result{}, err
	}
	// Setup, deliberately outside the measured window.
	for _, t := range tasks {
		if _, err := br.Admit(ctx, t, pol.Rank(t)); err != nil {
			return result{}, err
		}
	}

	start := time.Now()
	dispatched := 0
	for dispatched < len(tasks) {
		batchOut, err := br.Dispatch(ctx, batch, 0, time.Now())
		if err != nil {
			return result{}, err
		}
		if len(batchOut) == 0 {
			break
		}
		for _, d := range batchOut {
			pol.Notify(d.Key)
		}
		dispatched += len(batchOut)
	}
	elapsed := time.Since(start)
	return newResult("dispatch", pol.Name(), batch, dispatched, elapsed, meanExec), nil
}

// measureEngine times the whole scheduler process: the ingest loop draining a
// pre-filled ingress stream and the dispatch loop draining the pending set,
// running concurrently as they do in production, with no workers attached.
func measureEngine(ctx context.Context, br *broker.Broker, cfg *config.Config, policyName string, tasks []domain.Task, meanExec float64, log *slog.Logger) (result, error) {
	if err := br.Reset(ctx); err != nil {
		return result{}, err
	}
	if err := br.EnsureStreams(ctx); err != nil {
		return result{}, err
	}
	const chunk = 500
	for i := 0; i < len(tasks); i += chunk {
		end := i + chunk
		if end > len(tasks) {
			end = len(tasks)
		}
		if err := br.SubmitBatch(ctx, tasks[i:end]); err != nil {
			return result{}, err
		}
	}

	pol, err := scheduler.New(scheduler.Options{
		Policy:              policyName,
		TenantWeights:       cfg.Scheduler.TenantWeights,
		DefaultTenantWeight: cfg.Scheduler.DefaultTenantWeight,
	})
	if err != nil {
		return result{}, err
	}
	scfg := cfg.Scheduler
	scfg.MaxInFlight = 0 // nothing acknowledges, so a cap would deadlock the loop
	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   br,
		Policy:   pol,
		Config:   scfg,
		Logger:   log,
		Metrics:  metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: policyName}),
		Consumer: "ceiling",
	})
	if err != nil {
		return result{}, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	start := time.Now()
	go func() { _ = engine.Run(runCtx); close(done) }()

	// Finished when everything has been admitted and the pending set is drained.
	deadline := time.Now().Add(5 * time.Minute)
	var elapsed time.Duration
	for {
		stats, err := br.Stats(runCtx)
		if err != nil {
			cancel()
			<-done
			return result{}, err
		}
		if stats[broker.StatDispatched] >= int64(len(tasks)) {
			elapsed = time.Since(start)
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			return result{}, fmt.Errorf("engine phase timed out after dispatching %d of %d",
				stats[broker.StatDispatched], len(tasks))
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	return newResult("engine", policyName, cfg.Scheduler.DispatchBatch, len(tasks), elapsed, meanExec), nil
}

func newResult(phase, pol string, batch, tasks int, elapsed time.Duration, meanExec float64) result {
	rate := 0.0
	if elapsed > 0 {
		rate = float64(tasks) / elapsed.Seconds()
	}
	return result{
		Phase: phase, Policy: pol, Batch: batch, Tasks: tasks,
		Elapsed: elapsed, RatePerS: rate,
		SlotsFed:  rate * (meanExec / 1000),
		MeanExecM: meanExec,
	}
}

func printResults(rs []result, meanExec float64) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "phase\tpolicy\tbatch\ttasks\telapsed\trate (tasks/s)\tworker slots fed")
	for _, r := range rs {
		batch := "-"
		if r.Batch > 0 {
			batch = strconv.Itoa(r.Batch)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%.0f\t%.0f\n",
			r.Phase, r.Policy, batch, r.Tasks,
			r.Elapsed.Round(time.Millisecond), r.RatePerS, r.SlotsFed)
	}
	w.Flush()

	// The ceiling is the slowest stage, not the fastest. Reporting peak dispatch
	// as "the" ceiling would overstate it by more than an order of magnitude,
	// because dispatch batches many tasks per Redis round trip while admission
	// pays one round trip per task.
	var peakDispatch, bestAdmit, engine result
	for _, r := range rs {
		switch r.Phase {
		case "dispatch":
			if r.RatePerS > peakDispatch.RatePerS {
				peakDispatch = r
			}
		case "admit":
			if bestAdmit.RatePerS == 0 || r.RatePerS > bestAdmit.RatePerS {
				bestAdmit = r
			}
		case "engine":
			if engine.RatePerS == 0 || r.RatePerS > engine.RatePerS {
				engine = r
			}
		}
	}

	binding := engine
	label := "end-to-end engine"
	if binding.RatePerS == 0 {
		binding, label = bestAdmit, "admission"
	}

	fmt.Printf("\nStage rates (higher is better):\n")
	if peakDispatch.RatePerS > 0 {
		fmt.Printf("  dispatch, best case   %9.0f tasks/s  (batch %d)\n", peakDispatch.RatePerS, peakDispatch.Batch)
	}
	if bestAdmit.RatePerS > 0 {
		fmt.Printf("  admission             %9.0f tasks/s  (one round trip per task)\n", bestAdmit.RatePerS)
	}
	if engine.RatePerS > 0 {
		fmt.Printf("  end to end            %9.0f tasks/s  (both loops together)\n", engine.RatePerS)
	}

	if peakDispatch.RatePerS > 0 && bestAdmit.RatePerS > 0 {
		fmt.Printf("\nDispatch is %.0fx faster than admission, so dispatch is not the constraint:\n",
			peakDispatch.RatePerS/bestAdmit.RatePerS)
		fmt.Printf("it amortises many tasks over one Redis round trip while admission pays one\n")
		fmt.Printf("round trip per task.\n")
	}
	if binding.RatePerS > 0 {
		fmt.Printf("\nCeiling: %.0f tasks/s, set by the %s stage.\n", binding.RatePerS, label)
		fmt.Printf("At a %.0fms mean task that keeps about %.0f worker slots busy before the\n",
			meanExec, binding.SlotsFed)
		fmt.Printf("scheduler, rather than the workers, becomes the bottleneck.\n")
	}
}

func writeCSV(path string, rs []result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"phase", "policy", "batch", "tasks", "elapsed_s", "rate_tasks_per_s", "worker_slots_fed", "mean_exec_ms"}); err != nil {
		return err
	}
	for _, r := range rs {
		if err := w.Write([]string{
			r.Phase, r.Policy, strconv.Itoa(r.Batch), strconv.Itoa(r.Tasks),
			strconv.FormatFloat(r.Elapsed.Seconds(), 'f', 6, 64),
			strconv.FormatFloat(r.RatePerS, 'f', 3, 64),
			strconv.FormatFloat(r.SlotsFed, 'f', 3, 64),
			strconv.FormatFloat(r.MeanExecM, 'f', 3, 64),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}
