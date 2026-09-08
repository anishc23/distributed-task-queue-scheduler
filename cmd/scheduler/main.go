// Command scheduler runs the central scheduling process: it ingests tasks from
// the ingress stream, ranks them with the configured policy, dispatches them to
// workers, and runs the failure-recovery loop.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/recovery"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
)

func main() {
	fs := flag.NewFlagSet("scheduler", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)

	policyName := fs.String("policy", "", "scheduling policy: "+fmt.Sprint(scheduler.Names())+" (overrides scheduler.policy)")
	metricsAddr := fs.String("metrics-addr", "", "Prometheus listen address (overrides scheduler.metrics_addr)")
	consumer := fs.String("consumer", "", "consumer name inside the scheduler group (defaults to hostname-pid)")
	maxInFlight := fs.Int("max-in-flight", -1, "cap on dispatched-but-unfinished tasks; 0 means unlimited, -1 keeps the configured value")
	enableRecovery := fs.Bool("recovery", true, "run the failure-recovery loop in this process")
	redisWait := fs.Duration("redis-wait", 2*time.Minute, "how long to wait for Redis at startup before giving up; 0 fails immediately")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "scheduler ranks pending tasks and dispatches them to workers.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *policyName != "" {
		cfg.Scheduler.Policy = *policyName
	}
	if *metricsAddr != "" {
		cfg.Scheduler.MetricsAddr = *metricsAddr
	}
	if *maxInFlight >= 0 {
		cfg.Scheduler.MaxInFlight = *maxInFlight
	}
	if !*enableRecovery {
		cfg.Recovery.Enabled = false
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	log := cli.NewLogger(cfg.Log, "scheduler")
	ctx, stop := cli.SignalContext()
	defer stop()

	br, err := broker.New(cfg)
	if err != nil {
		cli.Fail(log, "cannot create broker", err)
	}
	defer br.Close()

	pol, err := scheduler.New(scheduler.Options{
		Policy:              cfg.Scheduler.Policy,
		TenantWeights:       cfg.Scheduler.TenantWeights,
		DefaultTenantWeight: cfg.Scheduler.DefaultTenantWeight,
	})
	if err != nil {
		cli.Fail(log, "cannot build scheduling policy", err)
	}

	name := *consumer
	if name == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "scheduler"
		}
		name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	m := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: pol.Name()})
	m.PreloadTenants(cfg.Workload.TenantIDs())

	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   br,
		Policy:   pol,
		Config:   cfg.Scheduler,
		Logger:   log,
		Metrics:  m,
		Consumer: name,
	})
	if err != nil {
		cli.Fail(log, "cannot build scheduling engine", err)
	}

	recLoop, err := recovery.New(recovery.Options{
		Broker:   br,
		Config:   cfg.Recovery,
		Logger:   log,
		Metrics:  m,
		Consumer: name + "-recovery",
	})
	if err != nil {
		cli.Fail(log, "cannot build recovery loop", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// The metrics endpoint starts before the broker is dialled so that the
	// liveness probe ("is the process alive?") succeeds immediately, while the
	// readiness probe ("can it serve?") stays negative until the engine is
	// running. Starting it afterwards would let a slow broker look like a dead
	// process and get the container killed during an ordinary rollout.
	if cfg.Scheduler.MetricsAddr != "" {
		srv := metrics.NewServer(cfg.Scheduler.MetricsAddr, m.Registry(), engine.Ready)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.Run(ctx, log); err != nil {
				errCh <- err
			}
		}()
	}

	if err := br.WaitReady(ctx, *redisWait, log); err != nil {
		cli.Fail(log, "cannot reach Redis", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := engine.Run(ctx); err != nil {
			errCh <- fmt.Errorf("scheduler engine: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := recLoop.Run(ctx); err != nil {
			errCh <- fmt.Errorf("recovery loop: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		cli.Fail(log, "scheduler exited with errors", err)
	}
	log.Info("scheduler shut down cleanly")
}
