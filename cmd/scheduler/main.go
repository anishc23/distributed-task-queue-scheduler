// Command scheduler runs the central scheduling process: it ingests tasks from
// the ingress stream, ranks them with the configured policy, dispatches them to
// workers, and runs the failure-recovery loop.
//
// Run more than one. With leader election on, exactly one replica dispatches
// and the rest stand by, taking over within a lease TTL of a crash and almost
// immediately after a clean shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/lease"
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
	leaderElection := fs.String("leader-election", "", "enforce a single dispatching scheduler: true or false (overrides scheduler.leader_election.enabled)")
	leaseTTL := fs.Duration("lease-ttl", 0, "how long a leadership grant survives without renewal; bounds failover time after a crash (overrides scheduler.leader_election.ttl)")
	leaseRenew := fs.Duration("lease-renew", 0, "how often the leader extends its grant; must be shorter than the ttl (overrides scheduler.leader_election.renew_interval)")
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
	if *leaderElection != "" {
		on, err := strconv.ParseBool(*leaderElection)
		if err != nil {
			fmt.Fprintf(os.Stderr, "configuration error: --leader-election=%q is not a boolean\n", *leaderElection)
			os.Exit(2)
		}
		cfg.Scheduler.LeaderElection.Enabled = on
	}
	if *leaseTTL > 0 {
		cfg.Scheduler.LeaderElection.TTL = config.Duration(*leaseTTL)
	}
	if *leaseRenew > 0 {
		cfg.Scheduler.LeaderElection.RenewInterval = config.Duration(*leaseRenew)
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

	// With election on, this process campaigns and only dispatches while it
	// holds the lease. With it off it dispatches unconditionally, which is
	// correct only if exactly one scheduler is ever started.
	var schedLease *lease.Lease
	if le := cfg.Scheduler.LeaderElection; le.Enabled {
		schedLease, err = lease.New(lease.Options{
			Redis: br.Redis(),
			Keys: lease.Keys{
				Holder: br.Keys().Leader,
				Epoch:  br.Keys().LeaderEpoch,
			},
			Owner:  name,
			TTL:    le.TTL.D(),
			Renew:  le.RenewInterval.D(),
			Retry:  le.RetryInterval.D(),
			Logger: log,
			// Only meaningful behind Sentinel, where a promotion can lose the
			// epoch the fence depends on. Zero elsewhere, which is the
			// original path exactly.
			WaitForReplicas: cfg.Redis.Sentinel.WaitForReplicas,
			WaitTimeout:     cfg.Redis.Sentinel.WaitTimeout.D(),
		})
		if err != nil {
			cli.Fail(log, "cannot build the scheduler lease", err)
		}
	} else {
		log.Warn("leader election is disabled; a second scheduler would corrupt policy state")
	}

	engine, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   br,
		Policy:   pol,
		Config:   cfg.Scheduler,
		Logger:   log,
		Metrics:  m,
		Consumer: name,
		Lease:    schedLease,
	})
	if err != nil {
		cli.Fail(log, "cannot build scheduling engine", err)
	}

	// Recovery runs on every replica, leader or not, and deliberately so. It
	// reclaims execution-stream entries whose worker died, and every path it
	// can take is already idempotent: XAUTOCLAIM hands a message to exactly one
	// claimant, and the retry script checks the done hash before acting. Making
	// it leader-only would mean a leader wedged mid-term stops the queue
	// healing itself, which is the one situation recovery exists for.
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
