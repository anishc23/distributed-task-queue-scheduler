package bench

import (
	"math"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// RunIdentity uniquely names one experiment inside a benchmark run. Every raw
// row and every aggregate row carries it, which is what makes it impossible to
// mix results from different runs.
type RunIdentity struct {
	RunID      string `json:"run_id"`
	Scheduler  string `json:"scheduler"`
	Workload   string `json:"workload"`
	Repetition int    `json:"repetition"`
	Seed       int64  `json:"seed"`
}

// Summary is the aggregate record for one experiment.
type Summary struct {
	Identity  RunIdentity `json:"identity"`
	Timestamp time.Time   `json:"timestamp"`

	TasksPlanned         int  `json:"tasks_planned"`
	TasksSubmitted       int  `json:"tasks_submitted"`
	TasksCompleted       int  `json:"tasks_completed"`
	TasksDeadLettered    int  `json:"tasks_dead_lettered"`
	TasksRetried         int  `json:"tasks_retried"`
	TaskFailures         int  `json:"task_failures"`
	DuplicateCompletions int  `json:"duplicate_completions"`
	TimedOut             bool `json:"timed_out"`

	WorkerProcesses   int `json:"worker_processes"`
	WorkerConcurrency int `json:"worker_concurrency"`
	TotalSlots        int `json:"total_slots"`
	MaxInFlight       int `json:"max_in_flight"`

	// ExperimentDuration is wall-clock time from the first submission to the
	// moment the run was declared complete.
	ExperimentDuration time.Duration `json:"experiment_duration"`
	// Makespan is the span from the earliest submission to the latest
	// completion across recorded results. Throughput uses this so that idle
	// tear-down time does not deflate the number.
	Makespan   time.Duration `json:"makespan"`
	Throughput float64       `json:"throughput_tasks_per_s"`

	Latency LatencyStats `json:"latency_seconds"`
	Wait    LatencyStats `json:"queue_wait_seconds"`
	Exec    LatencyStats `json:"exec_seconds"`

	DeadlineMissed   int     `json:"deadline_missed"`
	DeadlineMissRate float64 `json:"deadline_miss_rate"`

	WorkerBusySeconds float64 `json:"worker_busy_seconds"`
	WorkerSlotSeconds float64 `json:"worker_slot_seconds"`
	WorkerUtilization float64 `json:"worker_utilization"`

	// JainFairnessService is Jain's index over completed service seconds per
	// tenant. This is the headline fairness number reported in the analysis.
	JainFairnessService float64 `json:"jain_fairness_service"`
	// JainFairnessCount is the same index over completed task counts per
	// tenant, reported alongside because the two diverge when tenants have very
	// different task sizes.
	JainFairnessCount float64            `json:"jain_fairness_count"`
	TenantCount       int                `json:"tenant_count"`
	TenantService     map[string]float64 `json:"tenant_service_seconds"`
	TenantCompleted   map[string]int     `json:"tenant_completed_tasks"`
	// Tenants holds per-tenant latency and service statistics. Under a skewed
	// workload these, not the fairness index, are what distinguish policies.
	Tenants []TenantStat `json:"tenants"`
	Spread  TenantSpread `json:"tenant_spread"`

	ProducerMaxLag  time.Duration `json:"producer_max_lag"`
	ProducerMeanLag time.Duration `json:"producer_mean_lag"`

	ArrivalRatePerSec float64 `json:"arrival_rate_per_sec"`
	ExecMinMillis     int64   `json:"exec_min_ms"`
	ExecMaxMillis     int64   `json:"exec_max_ms"`
}

// SummaryInput carries everything needed to build a Summary that is not derived
// from the result records themselves.
type SummaryInput struct {
	Identity             RunIdentity
	Tenants              []string
	Results              []domain.Result
	TasksPlanned         int
	TasksSubmitted       int
	TasksDeadLettered    int
	TasksRetried         int
	TaskFailures         int
	DuplicateCompletions int
	TimedOut             bool
	WorkerProcesses      int
	WorkerConcurrency    int
	MaxInFlight          int
	ExperimentDuration   time.Duration
	WorkerBusySeconds    float64
	WorkerSlotSeconds    float64
	ProducerMaxLag       time.Duration
	ProducerMeanLag      time.Duration
	ArrivalRatePerSec    float64
	ExecMinMillis        int64
	ExecMaxMillis        int64
}

// Summarise turns raw task records into the aggregate experiment summary.
func Summarise(in SummaryInput) Summary {
	s := Summary{
		Identity:             in.Identity,
		Timestamp:            time.Now().UTC(),
		TasksPlanned:         in.TasksPlanned,
		TasksSubmitted:       in.TasksSubmitted,
		TasksCompleted:       len(in.Results),
		TasksDeadLettered:    in.TasksDeadLettered,
		TasksRetried:         in.TasksRetried,
		TaskFailures:         in.TaskFailures,
		DuplicateCompletions: in.DuplicateCompletions,
		TimedOut:             in.TimedOut,
		WorkerProcesses:      in.WorkerProcesses,
		WorkerConcurrency:    in.WorkerConcurrency,
		TotalSlots:           in.WorkerProcesses * in.WorkerConcurrency,
		MaxInFlight:          in.MaxInFlight,
		ExperimentDuration:   in.ExperimentDuration,
		WorkerBusySeconds:    in.WorkerBusySeconds,
		WorkerSlotSeconds:    in.WorkerSlotSeconds,
		ProducerMaxLag:       in.ProducerMaxLag,
		ProducerMeanLag:      in.ProducerMeanLag,
		ArrivalRatePerSec:    in.ArrivalRatePerSec,
		ExecMinMillis:        in.ExecMinMillis,
		ExecMaxMillis:        in.ExecMaxMillis,
		TenantCount:          len(in.Tenants),
	}

	s.Latency = SummariseSeconds(secondsOf(in.Results, domain.Result.Latency))
	s.Wait = SummariseSeconds(secondsOf(in.Results, domain.Result.QueueWait))
	s.Exec = SummariseSeconds(secondsOf(in.Results, func(r domain.Result) time.Duration {
		if r.ActualExecMillis > 0 {
			return time.Duration(r.ActualExecMillis) * time.Millisecond
		}
		return time.Duration(r.ExecMillis) * time.Millisecond
	}))

	s.DeadlineMissed = DeadlineMissCount(in.Results)
	s.DeadlineMissRate = DeadlineMissRate(in.Results)

	if in.WorkerSlotSeconds > 0 {
		s.WorkerUtilization = in.WorkerBusySeconds / in.WorkerSlotSeconds
	}

	s.TenantService = TenantService(in.Results, in.Tenants)
	s.TenantCompleted = TenantCounts(in.Results, in.Tenants)
	s.JainFairnessService = JainFairness(orderedValues(s.TenantService, in.Tenants))
	counts := make([]float64, 0, len(in.Tenants))
	for _, tenant := range in.Tenants {
		counts = append(counts, float64(s.TenantCompleted[tenant]))
	}
	s.JainFairnessCount = JainFairness(counts)

	s.Tenants = TenantStats(in.Results, in.Tenants)
	s.Spread = Spread(s.Tenants)

	s.Makespan = makespan(in.Results)
	if secs := s.Makespan.Seconds(); secs > 0 {
		s.Throughput = float64(len(in.Results)) / secs
	} else if len(in.Results) > 0 && in.ExperimentDuration > 0 {
		s.Throughput = float64(len(in.Results)) / in.ExperimentDuration.Seconds()
	}
	if math.IsInf(s.Throughput, 0) || math.IsNaN(s.Throughput) {
		s.Throughput = 0
	}
	return s
}

// makespan is the span from the earliest submission to the latest completion.
func makespan(results []domain.Result) time.Duration {
	if len(results) == 0 {
		return 0
	}
	first, last := results[0].SubmittedAt, results[0].FinishedAt
	for _, r := range results[1:] {
		if r.SubmittedAt.Before(first) {
			first = r.SubmittedAt
		}
		if r.FinishedAt.After(last) {
			last = r.FinishedAt
		}
	}
	d := last.Sub(first)
	if d < 0 {
		return 0
	}
	return d
}
