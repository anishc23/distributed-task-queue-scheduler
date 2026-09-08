package config

import "time"

// Default returns a configuration that runs a short local experiment end to end
// without any editing: a few hundred tasks against a local Redis.
func Default() *Config {
	return &Config{
		Redis: Redis{
			Addr:         "localhost:6379",
			DB:           0,
			DialTimeout:  Duration(5 * time.Second),
			ReadTimeout:  Duration(3 * time.Second),
			WriteTimeout: Duration(3 * time.Second),
			PoolSize:     32,
		},
		Streams: Streams{
			Namespace:        "tq",
			IngressStream:    "ingress",
			ExecStream:       "exec",
			ResultsStream:    "results",
			DeadLetterStream: "dead",
			SchedulerGroup:   "schedulers",
			WorkerGroup:      "workers",
			MaxLen:           0,
		},
		Scheduler: Scheduler{
			Policy:              "fifo",
			IngestBatch:         256,
			IngestBlock:         Duration(200 * time.Millisecond),
			DispatchBatch:       32,
			IdleSleep:           Duration(2 * time.Millisecond),
			MaxInFlight:         64,
			MetricsAddr:         ":9101",
			DefaultTenantWeight: 1,
			TenantWeights: map[string]float64{
				"A": 1, "B": 1, "C": 1, "D": 1, "E": 1,
			},
			StateFlushInterval: Duration(time.Second),
		},
		Worker: Worker{
			Concurrency:       4,
			Block:             Duration(500 * time.Millisecond),
			MetricsAddr:       ":9102",
			FailBeforeAckRate: 0,
			FailRate:          0,
			ShutdownGrace:     Duration(15 * time.Second),
		},
		Recovery: Recovery{
			Enabled:    true,
			Interval:   Duration(500 * time.Millisecond),
			MinIdle:    Duration(10 * time.Second),
			Batch:      128,
			MaxRetries: 3,
		},
		Workload: Workload{
			Type:              WorkloadUniform,
			Seed:              42,
			Count:             500,
			IDPrefix:          "task",
			ArrivalRatePerSec: 120,
			MaxRetries:        3,
			Exec:              ExecSpec{MinMillis: 40, MaxMillis: 60},
			Priority:          PrioritySpec{Weights: []float64{40, 25, 20, 10, 5}},
			Deadline:          DeadlineSpec{BaseMillis: 250, ExecMultiplier: 3, JitterMillis: 250},
			Tenants: []TenantShare{
				{ID: "A", Share: 0.2},
				{ID: "B", Share: 0.2},
				{ID: "C", Share: 0.2},
				{ID: "D", Share: 0.2},
				{ID: "E", Share: 0.2},
			},
			SkewTenants: []TenantShare{
				{ID: "A", Share: 0.90},
				{ID: "B", Share: 0.03},
				{ID: "C", Share: 0.03},
				{ID: "D", Share: 0.02},
				{ID: "E", Share: 0.02},
			},
			Burst: BurstSpec{
				BaseRatePerSec:  60,
				BurstRatePerSec: 600,
				BurstDuration:   Duration(500 * time.Millisecond),
				BurstPeriod:     Duration(2 * time.Second),
			},
			HeavyTail: HeavyTailSpec{MinMillis: 15, MaxMillis: 4000, Alpha: 1.3},
		},
		Log: Log{Level: "info", Format: "text"},
	}
}

// applyDefaults fills in values that a partial YAML document left at their zero
// value but that must not be zero. It is intentionally conservative: only
// fields where zero is never a meaningful choice are patched.
func (c *Config) applyDefaults() {
	d := Default()
	if c.Redis.Addr == "" {
		c.Redis.Addr = d.Redis.Addr
	}
	if c.Redis.DialTimeout == 0 {
		c.Redis.DialTimeout = d.Redis.DialTimeout
	}
	if c.Redis.ReadTimeout == 0 {
		c.Redis.ReadTimeout = d.Redis.ReadTimeout
	}
	if c.Redis.WriteTimeout == 0 {
		c.Redis.WriteTimeout = d.Redis.WriteTimeout
	}
	if c.Redis.PoolSize == 0 {
		c.Redis.PoolSize = d.Redis.PoolSize
	}

	if c.Streams.Namespace == "" {
		c.Streams.Namespace = d.Streams.Namespace
	}
	if c.Streams.IngressStream == "" {
		c.Streams.IngressStream = d.Streams.IngressStream
	}
	if c.Streams.ExecStream == "" {
		c.Streams.ExecStream = d.Streams.ExecStream
	}
	if c.Streams.ResultsStream == "" {
		c.Streams.ResultsStream = d.Streams.ResultsStream
	}
	if c.Streams.DeadLetterStream == "" {
		c.Streams.DeadLetterStream = d.Streams.DeadLetterStream
	}
	if c.Streams.SchedulerGroup == "" {
		c.Streams.SchedulerGroup = d.Streams.SchedulerGroup
	}
	if c.Streams.WorkerGroup == "" {
		c.Streams.WorkerGroup = d.Streams.WorkerGroup
	}

	if c.Scheduler.Policy == "" {
		c.Scheduler.Policy = d.Scheduler.Policy
	}
	if c.Scheduler.IngestBatch == 0 {
		c.Scheduler.IngestBatch = d.Scheduler.IngestBatch
	}
	if c.Scheduler.IngestBlock == 0 {
		c.Scheduler.IngestBlock = d.Scheduler.IngestBlock
	}
	if c.Scheduler.DispatchBatch == 0 {
		c.Scheduler.DispatchBatch = d.Scheduler.DispatchBatch
	}
	if c.Scheduler.IdleSleep == 0 {
		c.Scheduler.IdleSleep = d.Scheduler.IdleSleep
	}
	if c.Scheduler.MetricsAddr == "" {
		c.Scheduler.MetricsAddr = d.Scheduler.MetricsAddr
	}
	if c.Scheduler.DefaultTenantWeight == 0 {
		c.Scheduler.DefaultTenantWeight = d.Scheduler.DefaultTenantWeight
	}
	if c.Scheduler.StateFlushInterval == 0 {
		c.Scheduler.StateFlushInterval = d.Scheduler.StateFlushInterval
	}

	if c.Worker.Concurrency == 0 {
		c.Worker.Concurrency = d.Worker.Concurrency
	}
	if c.Worker.Block == 0 {
		c.Worker.Block = d.Worker.Block
	}
	if c.Worker.MetricsAddr == "" {
		c.Worker.MetricsAddr = d.Worker.MetricsAddr
	}
	if c.Worker.ShutdownGrace == 0 {
		c.Worker.ShutdownGrace = d.Worker.ShutdownGrace
	}

	if c.Recovery.Interval == 0 {
		c.Recovery.Interval = d.Recovery.Interval
	}
	if c.Recovery.MinIdle == 0 {
		c.Recovery.MinIdle = d.Recovery.MinIdle
	}
	if c.Recovery.Batch == 0 {
		c.Recovery.Batch = d.Recovery.Batch
	}

	if c.Workload.Type == "" {
		c.Workload.Type = d.Workload.Type
	}
	if c.Workload.Count == 0 {
		c.Workload.Count = d.Workload.Count
	}
	if c.Workload.IDPrefix == "" {
		c.Workload.IDPrefix = d.Workload.IDPrefix
	}
	if c.Workload.ArrivalRatePerSec == 0 {
		c.Workload.ArrivalRatePerSec = d.Workload.ArrivalRatePerSec
	}
	if c.Workload.Exec.MaxMillis == 0 {
		c.Workload.Exec = d.Workload.Exec
	}
	if len(c.Workload.Priority.Weights) == 0 {
		c.Workload.Priority = d.Workload.Priority
	}
	if c.Workload.Deadline == (DeadlineSpec{}) {
		c.Workload.Deadline = d.Workload.Deadline
	}
	if len(c.Workload.Tenants) == 0 {
		c.Workload.Tenants = d.Workload.Tenants
	}
	if len(c.Workload.SkewTenants) == 0 {
		c.Workload.SkewTenants = d.Workload.SkewTenants
	}
	if c.Workload.Burst == (BurstSpec{}) {
		c.Workload.Burst = d.Workload.Burst
	}
	if c.Workload.HeavyTail == (HeavyTailSpec{}) {
		c.Workload.HeavyTail = d.Workload.HeavyTail
	}

	if c.Log.Level == "" {
		c.Log.Level = d.Log.Level
	}
	if c.Log.Format == "" {
		c.Log.Format = d.Log.Format
	}
}
