// Package config loads, defaults and validates the YAML configuration shared by
// every command. Validation runs at startup and is deliberately strict: a
// misconfigured experiment is worse than a failed start.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration document.
type Config struct {
	Redis     Redis     `yaml:"redis" json:"redis"`
	Streams   Streams   `yaml:"streams" json:"streams"`
	Scheduler Scheduler `yaml:"scheduler" json:"scheduler"`
	Worker    Worker    `yaml:"worker" json:"worker"`
	Recovery  Recovery  `yaml:"recovery" json:"recovery"`
	Workload  Workload  `yaml:"workload" json:"workload"`
	Log       Log       `yaml:"log" json:"log"`
}

// Redis holds broker connection settings.
type Redis struct {
	Addr         string   `yaml:"addr" json:"addr"`
	Password     string   `yaml:"password" json:"password"`
	DB           int      `yaml:"db" json:"db"`
	DialTimeout  Duration `yaml:"dial_timeout" json:"dial_timeout"`
	ReadTimeout  Duration `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout" json:"write_timeout"`
	PoolSize     int      `yaml:"pool_size" json:"pool_size"`
}

// Streams holds the Redis key and stream names. Every name is configurable and
// all of them are prefixed by Namespace, which lets independent experiments
// share one Redis instance without ever mixing state.
type Streams struct {
	Namespace        string `yaml:"namespace" json:"namespace"`
	IngressStream    string `yaml:"ingress_stream" json:"ingress_stream"`
	ExecStream       string `yaml:"exec_stream" json:"exec_stream"`
	ResultsStream    string `yaml:"results_stream" json:"results_stream"`
	DeadLetterStream string `yaml:"dead_letter_stream" json:"dead_letter_stream"`
	SchedulerGroup   string `yaml:"scheduler_group" json:"scheduler_group"`
	WorkerGroup      string `yaml:"worker_group" json:"worker_group"`
	// MaxLen approximately caps ingress and exec stream length. Zero disables
	// trimming, which is what experiments want so that nothing is lost.
	MaxLen int64 `yaml:"max_len" json:"max_len"`
}

// Scheduler configures the central scheduling process.
type Scheduler struct {
	// Policy selects the scheduling algorithm: fifo, priority, edf or wfq.
	Policy string `yaml:"policy" json:"policy"`
	// IngestBatch is how many ingress entries are admitted per read.
	IngestBatch int64 `yaml:"ingest_batch" json:"ingest_batch"`
	// IngestBlock is how long a scheduler blocks waiting for ingress entries.
	IngestBlock Duration `yaml:"ingest_block" json:"ingest_block"`
	// DispatchBatch is how many pending tasks are dispatched per loop pass.
	DispatchBatch int `yaml:"dispatch_batch" json:"dispatch_batch"`
	// IdleSleep is the back-off applied when there is nothing to dispatch. It
	// keeps the dispatch loop from becoming a busy loop.
	IdleSleep Duration `yaml:"idle_sleep" json:"idle_sleep"`
	// MaxInFlight caps tasks dispatched but not yet acknowledged. Zero means
	// unlimited. A finite value keeps the scheduling decision meaningful by
	// stopping the exec stream from becoming the real queue.
	MaxInFlight int `yaml:"max_in_flight" json:"max_in_flight"`
	// MetricsAddr is the listen address for the Prometheus endpoint.
	MetricsAddr string `yaml:"metrics_addr" json:"metrics_addr"`
	// TenantWeights are the WFQ weights per tenant.
	TenantWeights map[string]float64 `yaml:"tenant_weights" json:"tenant_weights"`
	// DefaultTenantWeight applies to tenants absent from TenantWeights.
	DefaultTenantWeight float64 `yaml:"default_tenant_weight" json:"default_tenant_weight"`
	// StateFlushInterval is how often policy state is persisted to Redis.
	StateFlushInterval Duration `yaml:"state_flush_interval" json:"state_flush_interval"`
	// LeaderElection controls how a scheduler replica acquires the right to
	// dispatch. See LeaderElection.
	LeaderElection LeaderElection `yaml:"leader_election" json:"leader_election"`
	// IngestReclaimInterval is how often the leader looks for ingress entries
	// stranded by a scheduler that died before acknowledging them.
	IngestReclaimInterval Duration `yaml:"ingest_reclaim_interval" json:"ingest_reclaim_interval"`
	// IngestReclaimMinIdle is how long an ingress entry must have been pending
	// before it is treated as stranded.
	//
	// This is much shorter than recovery.min_idle, and the asymmetry is the
	// point. A worker legitimately holds an exec entry for as long as the task
	// takes to run, so reclaiming it early would duplicate real work. A
	// scheduler holds an ingress entry only for the microseconds between
	// reading it and admitting it, so anything pending for seconds is not busy,
	// it is dead.
	IngestReclaimMinIdle Duration `yaml:"ingest_reclaim_min_idle" json:"ingest_reclaim_min_idle"`
}

// LeaderElection configures the lease that enforces one dispatching scheduler.
//
// The scheduler owns the pending index and the policy's virtual clock, so two
// of them dispatching at once does not merely duplicate work, it ranks tasks
// against two divergent clocks and then overwrites one flush with the other.
// The lease makes that impossible rather than merely discouraged.
//
// Only the scheduler command reads this. The benchmark harness runs a single
// in-process engine against a namespace it has just reset, so there is nothing
// to elect and it builds its engine without a lease; that keeps the published
// measurements free of an election that a benchmark would never exercise.
type LeaderElection struct {
	// Enabled turns on the lease. With it off, the process dispatches
	// immediately and assumes it is alone, which is correct only when
	// something outside the system guarantees that.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// TTL is how long a grant survives without renewal, and therefore the
	// worst-case dispatch outage after a leader crashes hard: a standby cannot
	// safely take over until Redis has expired the key. Lowering it shortens
	// failover and raises the chance that an ordinary latency spike is
	// mistaken for a death.
	TTL Duration `yaml:"ttl" json:"ttl"`
	// RenewInterval is how often the holder extends its grant. It must be
	// comfortably shorter than TTL, since the leader gets TTL/RenewInterval
	// attempts to survive a transient Redis hiccup.
	RenewInterval Duration `yaml:"renew_interval" json:"renew_interval"`
	// RetryInterval is how often a standby tries to acquire.
	//
	// This, not TTL, is what bounds a graceful handover. A leader shutting
	// down cleanly releases the lease at once, so the only remaining delay is
	// however long the standby takes to poll. Measured at a median of roughly
	// half this value, which is what a uniform poll offset predicts.
	RetryInterval Duration `yaml:"retry_interval" json:"retry_interval"`
}

// Worker configures a worker process.
type Worker struct {
	// Name identifies the consumer inside the worker consumer group. Empty
	// means "derive from hostname and PID".
	Name string `yaml:"name" json:"name"`
	// Concurrency is the number of tasks executed in parallel per process.
	Concurrency int `yaml:"concurrency" json:"concurrency"`
	// ExecMode selects how a task's simulated duration is consumed: "sleep"
	// occupies a slot without using CPU, "cpu" burns the duration in real
	// computation so that slots contend for cores as real work would.
	ExecMode string `yaml:"exec_mode" json:"exec_mode"`
	// Block is how long XREADGROUP waits for new work before looping.
	Block Duration `yaml:"block" json:"block"`
	// MetricsAddr is the listen address for the Prometheus endpoint.
	MetricsAddr string `yaml:"metrics_addr" json:"metrics_addr"`
	// FailBeforeAckRate is the probability that a worker abandons a task after
	// executing it and before acknowledging, simulating a crash. Used to
	// exercise the recovery path. Range [0,1].
	FailBeforeAckRate float64 `yaml:"fail_before_ack_rate" json:"fail_before_ack_rate"`
	// FailRate is the probability that a task reports an application error.
	// Failed tasks are retried through the same recovery path. Range [0,1].
	FailRate float64 `yaml:"fail_rate" json:"fail_rate"`
	// FailSeed makes injected failures reproducible. Zero derives a seed from
	// the worker name.
	FailSeed int64 `yaml:"fail_seed" json:"fail_seed"`
	// ShutdownGrace bounds how long graceful shutdown waits for in-flight work.
	ShutdownGrace Duration `yaml:"shutdown_grace" json:"shutdown_grace"`
}

// Recovery configures the redelivery loop.
type Recovery struct {
	// Enabled turns the recovery loop on. It runs inside the scheduler process.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Interval is how often XAUTOCLAIM is run.
	Interval Duration `yaml:"interval" json:"interval"`
	// MinIdle is how long a pending exec-stream entry must be untouched before
	// it is considered lost. This is the visibility timeout.
	MinIdle Duration `yaml:"min_idle" json:"min_idle"`
	// Batch is the maximum number of entries reclaimed per pass.
	Batch int64 `yaml:"batch" json:"batch"`
	// MaxRetries is the default retry budget applied to tasks that do not carry
	// their own.
	MaxRetries int `yaml:"max_retries" json:"max_retries"`
}

// Log configures structured logging.
type Log struct {
	// Level is one of debug, info, warn, error.
	Level string `yaml:"level" json:"level"`
	// Format is "text" or "json".
	Format string `yaml:"format" json:"format"`
}

// Load reads a YAML configuration file, applies defaults and validates it.
func Load(path string) (*Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", displayPath(path), err)
	}
	return cfg, nil
}

func displayPath(path string) string {
	if path == "" {
		return "(built-in defaults)"
	}
	return path
}

// Validate checks every field that could produce a silently wrong experiment.
func (c *Config) Validate() error {
	var errs []error

	if c.Redis.Addr == "" {
		errs = append(errs, errors.New("redis.addr is required"))
	}
	if c.Redis.DB < 0 {
		errs = append(errs, fmt.Errorf("redis.db must be >= 0, got %d", c.Redis.DB))
	}
	if c.Redis.PoolSize <= 0 {
		errs = append(errs, fmt.Errorf("redis.pool_size must be > 0, got %d", c.Redis.PoolSize))
	}

	if c.Streams.Namespace == "" {
		errs = append(errs, errors.New("streams.namespace is required"))
	}
	if strings.ContainsAny(c.Streams.Namespace, " \t\n") {
		errs = append(errs, fmt.Errorf("streams.namespace %q must not contain whitespace", c.Streams.Namespace))
	}
	for name, v := range map[string]string{
		"streams.ingress_stream":     c.Streams.IngressStream,
		"streams.exec_stream":        c.Streams.ExecStream,
		"streams.results_stream":     c.Streams.ResultsStream,
		"streams.dead_letter_stream": c.Streams.DeadLetterStream,
		"streams.scheduler_group":    c.Streams.SchedulerGroup,
		"streams.worker_group":       c.Streams.WorkerGroup,
	} {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if c.Streams.MaxLen < 0 {
		errs = append(errs, fmt.Errorf("streams.max_len must be >= 0, got %d", c.Streams.MaxLen))
	}

	if !policy.Valid(c.Scheduler.Policy) {
		errs = append(errs, fmt.Errorf("scheduler.policy %q is not one of %v", c.Scheduler.Policy, policy.Names()))
	}
	if c.Scheduler.IngestBatch <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.ingest_batch must be > 0, got %d", c.Scheduler.IngestBatch))
	}
	if c.Scheduler.DispatchBatch <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.dispatch_batch must be > 0, got %d", c.Scheduler.DispatchBatch))
	}
	if c.Scheduler.IdleSleep.D() <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.idle_sleep must be > 0 to avoid a busy loop, got %s", c.Scheduler.IdleSleep))
	}
	if c.Scheduler.MaxInFlight < 0 {
		errs = append(errs, fmt.Errorf("scheduler.max_in_flight must be >= 0, got %d", c.Scheduler.MaxInFlight))
	}
	if c.Scheduler.DefaultTenantWeight <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.default_tenant_weight must be > 0, got %g", c.Scheduler.DefaultTenantWeight))
	}
	for tenant, w := range c.Scheduler.TenantWeights {
		if w <= 0 {
			errs = append(errs, fmt.Errorf("scheduler.tenant_weights[%s] must be > 0, got %g", tenant, w))
		}
	}
	if c.Scheduler.StateFlushInterval.D() <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.state_flush_interval must be > 0, got %s", c.Scheduler.StateFlushInterval))
	}

	if c.Worker.Concurrency <= 0 {
		errs = append(errs, fmt.Errorf("worker.concurrency must be > 0, got %d", c.Worker.Concurrency))
	}
	if !ValidExecMode(c.Worker.ExecMode) {
		errs = append(errs, fmt.Errorf("worker.exec_mode %q must be one of %v", c.Worker.ExecMode, ExecModes()))
	}
	if c.Scheduler.IngestReclaimInterval.D() <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.ingest_reclaim_interval must be > 0, got %s", c.Scheduler.IngestReclaimInterval))
	}
	if c.Scheduler.IngestReclaimMinIdle.D() <= 0 {
		errs = append(errs, fmt.Errorf("scheduler.ingest_reclaim_min_idle must be > 0, got %s", c.Scheduler.IngestReclaimMinIdle))
	}

	if le := c.Scheduler.LeaderElection; le.Enabled {
		switch {
		case le.TTL.D() <= 0:
			errs = append(errs, fmt.Errorf("scheduler.leader_election.ttl must be > 0, got %s", le.TTL))
		case le.RenewInterval.D() <= 0:
			errs = append(errs, fmt.Errorf("scheduler.leader_election.renew_interval must be > 0, got %s", le.RenewInterval))
		case le.RenewInterval.D() >= le.TTL.D():
			// Renewing no more often than the TTL leaves no margin at all: the
			// first slow round trip drops the lease and triggers a failover
			// that nothing was actually wrong with.
			errs = append(errs, fmt.Errorf("scheduler.leader_election.renew_interval (%s) must be shorter than ttl (%s)", le.RenewInterval, le.TTL))
		}
		if le.RetryInterval.D() <= 0 {
			errs = append(errs, fmt.Errorf("scheduler.leader_election.retry_interval must be > 0, got %s", le.RetryInterval))
		}
	}

	if c.Worker.Block.D() <= 0 {
		errs = append(errs, fmt.Errorf("worker.block must be > 0, got %s", c.Worker.Block))
	}
	if c.Worker.FailBeforeAckRate < 0 || c.Worker.FailBeforeAckRate > 1 {
		errs = append(errs, fmt.Errorf("worker.fail_before_ack_rate must be within [0,1], got %g", c.Worker.FailBeforeAckRate))
	}
	if c.Worker.FailRate < 0 || c.Worker.FailRate > 1 {
		errs = append(errs, fmt.Errorf("worker.fail_rate must be within [0,1], got %g", c.Worker.FailRate))
	}
	if c.Worker.ShutdownGrace.D() < 0 {
		errs = append(errs, fmt.Errorf("worker.shutdown_grace must be >= 0, got %s", c.Worker.ShutdownGrace))
	}

	if c.Recovery.Enabled {
		if c.Recovery.Interval.D() <= 0 {
			errs = append(errs, fmt.Errorf("recovery.interval must be > 0, got %s", c.Recovery.Interval))
		}
		if c.Recovery.MinIdle.D() <= 0 {
			errs = append(errs, fmt.Errorf("recovery.min_idle must be > 0, got %s", c.Recovery.MinIdle))
		}
		if c.Recovery.Batch <= 0 {
			errs = append(errs, fmt.Errorf("recovery.batch must be > 0, got %d", c.Recovery.Batch))
		}
	}
	if c.Recovery.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("recovery.max_retries must be >= 0, got %d", c.Recovery.MaxRetries))
	}

	if err := c.Workload.validate(); err != nil {
		errs = append(errs, err)
	}

	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("log.level %q must be one of debug, info, warn, error", c.Log.Level))
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		errs = append(errs, fmt.Errorf("log.format %q must be text or json", c.Log.Format))
	}

	return errors.Join(errs...)
}
