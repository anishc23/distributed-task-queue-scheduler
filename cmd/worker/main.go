// Command worker consumes tasks from the worker execution stream, simulates
// their execution, and records terminal results idempotently.
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
	"github.com/anishc23/distributed-task-queue/internal/worker"
)

func main() {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)

	name := fs.String("name", "", "consumer name prefix (defaults to hostname-pid)")
	concurrency := fs.Int("concurrency", 0, "parallel execution slots (overrides worker.concurrency)")
	metricsAddr := fs.String("metrics-addr", "", "Prometheus listen address (overrides worker.metrics_addr)")
	failRate := fs.Float64("fail-rate", -1, "probability a task reports an application error, within [0,1]")
	failBeforeAck := fs.Float64("fail-before-ack-rate", -1, "probability a task is abandoned after execution and before acknowledgement, simulating a crash, within [0,1]")
	redisWait := fs.Duration("redis-wait", 2*time.Minute, "how long to wait for Redis at startup before giving up; 0 fails immediately")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "worker executes tasks dispatched by the scheduler.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	if *name != "" {
		cfg.Worker.Name = *name
	}
	if *concurrency > 0 {
		cfg.Worker.Concurrency = *concurrency
	}
	if *metricsAddr != "" {
		cfg.Worker.MetricsAddr = *metricsAddr
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

	log := cli.NewLogger(cfg.Log, "worker")
	ctx, stop := cli.SignalContext()
	defer stop()

	br, err := broker.New(cfg)
	if err != nil {
		cli.Fail(log, "cannot create broker", err)
	}
	defer br.Close()

	m := metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: cfg.Scheduler.Policy})
	m.PreloadTenants(cfg.Workload.TenantIDs())

	w, err := worker.New(worker.Options{
		Broker:   br,
		Config:   cfg.Worker,
		Recovery: cfg.Recovery,
		Logger:   log,
		Metrics:  m,
	})
	if err != nil {
		cli.Fail(log, "cannot create worker", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	// Started before the broker is dialled so the liveness probe succeeds
	// immediately while readiness stays negative until the worker is consuming.
	if cfg.Worker.MetricsAddr != "" {
		srv := metrics.NewServer(cfg.Worker.MetricsAddr, m.Registry(), w.Ready)
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
		if err := w.Run(ctx); err != nil {
			errCh <- fmt.Errorf("worker: %w", err)
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
		cli.Fail(log, "worker exited with errors", err)
	}
	log.Info("worker shut down cleanly")
}
