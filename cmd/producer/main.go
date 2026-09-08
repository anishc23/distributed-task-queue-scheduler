// Command producer generates a reproducible synthetic workload and submits it to
// the ingress stream. It knows nothing about which scheduling policy is active.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/workload"
)

func main() {
	fs := flag.NewFlagSet("producer", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)

	kind := fs.String("workload", "", "workload type: "+fmt.Sprint(config.WorkloadTypes())+" (overrides workload.type)")
	count := fs.Int("count", 0, "number of tasks to generate (overrides workload.count)")
	seed := fs.Int64("seed", -1, "random seed; the same seed and configuration always regenerate the same task sequence")
	rate := fs.Float64("rate", 0, "mean arrival rate in tasks per second (overrides workload.arrival_rate_per_sec)")
	prefix := fs.String("id-prefix", "", "prefix for generated task IDs (overrides workload.id_prefix)")
	dryRun := fs.Bool("dry-run", false, "generate and summarise the workload without submitting it")
	redisWait := fs.Duration("redis-wait", 2*time.Minute, "how long to wait for Redis at startup before giving up; 0 fails immediately")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "producer submits a reproducible synthetic workload to the ingress stream.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *kind != "" {
		cfg.Workload.Type = *kind
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
	if *prefix != "" {
		cfg.Workload.IDPrefix = *prefix
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	log := cli.NewLogger(cfg.Log, "producer")
	ctx, stop := cli.SignalContext()
	defer stop()

	specs, err := workload.Generate(cfg.Workload)
	if err != nil {
		cli.Fail(log, "cannot generate workload", err)
	}
	plan := workload.Summarise(specs)
	log.Info("workload generated",
		"type", cfg.Workload.Type,
		"seed", cfg.Workload.Seed,
		"tasks", plan.Count,
		"arrival_span", plan.Span.String(),
		"mean_exec_ms", fmt.Sprintf("%.1f", plan.MeanExecMS),
		"max_exec_ms", plan.MaxExecMS,
		"total_service_ms", plan.TotalExecMS,
		"tenants", plan.TenantCounts)

	if *dryRun {
		log.Info("dry run: nothing was submitted")
		return
	}

	br, err := broker.New(cfg)
	if err != nil {
		cli.Fail(log, "cannot create broker", err)
	}
	defer br.Close()
	if err := br.WaitReady(ctx, *redisWait, log); err != nil {
		cli.Fail(log, "cannot reach Redis", err)
	}
	if err := br.EnsureStreams(ctx); err != nil {
		cli.Fail(log, "cannot create streams", err)
	}

	producer := workload.NewProducer(br, log, 128)
	producer.WarnOnCollisions(ctx, specs, cfg.Workload.Seed, cfg.Workload.IDPrefix)

	stats, err := producer.Submit(ctx, specs)
	if err != nil {
		cli.Fail(log, "workload submission failed", err)
	}
	log.Info("done", "submitted", stats.Submitted, "elapsed", stats.End.Sub(stats.Start).String())
}
