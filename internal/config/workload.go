package config

import (
	"errors"
	"fmt"
	"math"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Workload type identifiers.
const (
	WorkloadUniform     = "uniform"
	WorkloadBursty      = "bursty"
	WorkloadHeavyTailed = "heavy_tailed"
	WorkloadMultiTenant = "multi_tenant"
)

// WorkloadTypes lists every supported workload generator.
func WorkloadTypes() []string {
	return []string{WorkloadUniform, WorkloadBursty, WorkloadHeavyTailed, WorkloadMultiTenant}
}

// ValidWorkload reports whether name is a known workload type.
func ValidWorkload(name string) bool {
	for _, t := range WorkloadTypes() {
		if t == name {
			return true
		}
	}
	return false
}

// Workload describes a reproducible synthetic workload.
type Workload struct {
	// Type is one of uniform, bursty, heavy_tailed, multi_tenant.
	Type string `yaml:"type" json:"type"`
	// Seed fixes the pseudo-random stream. The same seed and the same workload
	// configuration always produce byte-identical task sequences.
	Seed int64 `yaml:"seed" json:"seed"`
	// Count is the total number of tasks generated. The workload is finite so
	// benchmarks terminate deterministically.
	Count int `yaml:"count" json:"count"`
	// IDPrefix prefixes generated task IDs.
	IDPrefix string `yaml:"id_prefix" json:"id_prefix"`
	// ArrivalRatePerSec is the mean arrival rate used by uniform,
	// heavy_tailed and multi_tenant workloads, in tasks per second.
	ArrivalRatePerSec float64 `yaml:"arrival_rate_per_sec" json:"arrival_rate_per_sec"`
	// MaxRetries is written into every generated task.
	MaxRetries int `yaml:"max_retries" json:"max_retries"`

	Exec     ExecSpec      `yaml:"exec" json:"exec"`
	Priority PrioritySpec  `yaml:"priority" json:"priority"`
	Deadline DeadlineSpec  `yaml:"deadline" json:"deadline"`
	Tenants  []TenantShare `yaml:"tenants" json:"tenants"`
	// SkewTenants replaces Tenants when Type is multi_tenant. It defaults to the
	// documented 90/3/3/2/2 skew used to study noisy-neighbour behaviour.
	SkewTenants []TenantShare `yaml:"skew_tenants" json:"skew_tenants"`
	Burst       BurstSpec     `yaml:"burst" json:"burst"`
	HeavyTail   HeavyTailSpec `yaml:"heavy_tail" json:"heavy_tail"`
}

// ExecSpec bounds the simulated execution duration for non-heavy-tailed
// workloads. Durations are sampled uniformly from [MinMillis, MaxMillis].
type ExecSpec struct {
	MinMillis int64 `yaml:"min_ms" json:"min_ms"`
	MaxMillis int64 `yaml:"max_ms" json:"max_ms"`
}

// PrioritySpec gives the relative frequency of each priority level. Index i is
// the weight of priority i, so higher indexes are more important.
type PrioritySpec struct {
	Weights []float64 `yaml:"weights" json:"weights"`
}

// DeadlineSpec derives a per-task relative deadline:
//
//	deadline = submitted_at + base_ms + exec_ms*exec_multiplier + U(0, jitter_ms)
//
// Tying part of the slack to the task's own duration keeps large tasks from
// being unconditionally doomed under EDF.
type DeadlineSpec struct {
	BaseMillis     int64   `yaml:"base_ms" json:"base_ms"`
	ExecMultiplier float64 `yaml:"exec_multiplier" json:"exec_multiplier"`
	JitterMillis   int64   `yaml:"jitter_ms" json:"jitter_ms"`
}

// TenantShare is the probability that a generated task belongs to a tenant.
type TenantShare struct {
	ID    string  `yaml:"id" json:"id"`
	Share float64 `yaml:"share" json:"share"`
}

// BurstSpec configures the bursty workload: Poisson arrivals at BaseRatePerSec
// with periodic windows of BurstDuration at BurstRatePerSec, repeating every
// BurstPeriod.
type BurstSpec struct {
	BaseRatePerSec  float64  `yaml:"base_rate_per_sec" json:"base_rate_per_sec"`
	BurstRatePerSec float64  `yaml:"burst_rate_per_sec" json:"burst_rate_per_sec"`
	BurstDuration   Duration `yaml:"burst_duration" json:"burst_duration"`
	BurstPeriod     Duration `yaml:"burst_period" json:"burst_period"`
}

// HeavyTailSpec configures Pareto-distributed execution durations:
//
//	exec_ms = min_ms * U^(-1/alpha), clamped to max_ms
//
// Smaller alpha means a heavier tail. alpha <= 1 has infinite mean, so the
// default stays above 1 and the clamp bounds the worst case regardless.
type HeavyTailSpec struct {
	MinMillis int64   `yaml:"min_ms" json:"min_ms"`
	MaxMillis int64   `yaml:"max_ms" json:"max_ms"`
	Alpha     float64 `yaml:"alpha" json:"alpha"`
}

func (w *Workload) validate() error {
	var errs []error
	if !ValidWorkload(w.Type) {
		errs = append(errs, fmt.Errorf("workload.type %q is not one of %v", w.Type, WorkloadTypes()))
	}
	if w.Count <= 0 {
		errs = append(errs, fmt.Errorf("workload.count must be > 0, got %d", w.Count))
	}
	if w.IDPrefix == "" {
		errs = append(errs, errors.New("workload.id_prefix is required"))
	}
	if w.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("workload.max_retries must be >= 0, got %d", w.MaxRetries))
	}
	if w.ArrivalRatePerSec <= 0 {
		errs = append(errs, fmt.Errorf("workload.arrival_rate_per_sec must be > 0, got %g", w.ArrivalRatePerSec))
	}

	if w.Exec.MinMillis < 0 || w.Exec.MaxMillis < w.Exec.MinMillis {
		errs = append(errs, fmt.Errorf("workload.exec requires 0 <= min_ms <= max_ms, got %d..%d", w.Exec.MinMillis, w.Exec.MaxMillis))
	}
	if w.Exec.MaxMillis == 0 {
		errs = append(errs, errors.New("workload.exec.max_ms must be > 0"))
	}

	if len(w.Priority.Weights) == 0 {
		errs = append(errs, errors.New("workload.priority.weights must contain at least one weight"))
	}
	if len(w.Priority.Weights) > domain.MaxPriority+1 {
		errs = append(errs, fmt.Errorf("workload.priority.weights has %d entries, maximum is %d", len(w.Priority.Weights), domain.MaxPriority+1))
	}
	var prioSum float64
	for i, v := range w.Priority.Weights {
		if v < 0 {
			errs = append(errs, fmt.Errorf("workload.priority.weights[%d] must be >= 0, got %g", i, v))
		}
		prioSum += v
	}
	if prioSum <= 0 {
		errs = append(errs, errors.New("workload.priority.weights must sum to > 0"))
	}

	if w.Deadline.BaseMillis < 0 {
		errs = append(errs, fmt.Errorf("workload.deadline.base_ms must be >= 0, got %d", w.Deadline.BaseMillis))
	}
	if w.Deadline.ExecMultiplier < 0 {
		errs = append(errs, fmt.Errorf("workload.deadline.exec_multiplier must be >= 0, got %g", w.Deadline.ExecMultiplier))
	}
	if w.Deadline.JitterMillis < 0 {
		errs = append(errs, fmt.Errorf("workload.deadline.jitter_ms must be >= 0, got %d", w.Deadline.JitterMillis))
	}

	if len(w.Tenants) == 0 {
		errs = append(errs, errors.New("workload.tenants must contain at least one tenant"))
	}
	errs = append(errs, validateShares("workload.tenants", w.Tenants))
	if w.Type == WorkloadMultiTenant {
		if len(w.SkewTenants) == 0 {
			errs = append(errs, errors.New("workload.skew_tenants must contain at least one tenant for the multi_tenant workload"))
		}
		errs = append(errs, validateShares("workload.skew_tenants", w.SkewTenants))
	}

	if w.Type == WorkloadBursty {
		if w.Burst.BaseRatePerSec <= 0 {
			errs = append(errs, fmt.Errorf("workload.burst.base_rate_per_sec must be > 0, got %g", w.Burst.BaseRatePerSec))
		}
		if w.Burst.BurstRatePerSec <= 0 {
			errs = append(errs, fmt.Errorf("workload.burst.burst_rate_per_sec must be > 0, got %g", w.Burst.BurstRatePerSec))
		}
		if w.Burst.BurstDuration.D() <= 0 {
			errs = append(errs, fmt.Errorf("workload.burst.burst_duration must be > 0, got %s", w.Burst.BurstDuration))
		}
		if w.Burst.BurstPeriod.D() <= w.Burst.BurstDuration.D() {
			errs = append(errs, fmt.Errorf("workload.burst.burst_period (%s) must exceed burst_duration (%s)", w.Burst.BurstPeriod, w.Burst.BurstDuration))
		}
	}

	if w.Type == WorkloadHeavyTailed {
		if w.HeavyTail.MinMillis <= 0 {
			errs = append(errs, fmt.Errorf("workload.heavy_tail.min_ms must be > 0, got %d", w.HeavyTail.MinMillis))
		}
		if w.HeavyTail.MaxMillis < w.HeavyTail.MinMillis {
			errs = append(errs, fmt.Errorf("workload.heavy_tail.max_ms (%d) must be >= min_ms (%d)", w.HeavyTail.MaxMillis, w.HeavyTail.MinMillis))
		}
		if w.HeavyTail.Alpha <= 0 {
			errs = append(errs, fmt.Errorf("workload.heavy_tail.alpha must be > 0, got %g", w.HeavyTail.Alpha))
		}
	}

	return errors.Join(errs...)
}

func validateShares(field string, tenants []TenantShare) error {
	if len(tenants) == 0 {
		return nil
	}
	var errs []error
	seen := make(map[string]bool, len(tenants))
	var sum float64
	for i, t := range tenants {
		if t.ID == "" {
			errs = append(errs, fmt.Errorf("%s[%d].id is required", field, i))
		}
		if seen[t.ID] {
			errs = append(errs, fmt.Errorf("%s contains duplicate id %q", field, t.ID))
		}
		seen[t.ID] = true
		if t.Share <= 0 {
			errs = append(errs, fmt.Errorf("%s[%d].share must be > 0, got %g", field, i, t.Share))
		}
		sum += t.Share
	}
	if math.Abs(sum-1) > 1e-6 {
		errs = append(errs, fmt.Errorf("%s shares must sum to 1.0, got %g", field, sum))
	}
	return errors.Join(errs...)
}

// ActiveTenants returns the tenant distribution that applies to this workload
// type: the skewed distribution for multi_tenant, the plain one otherwise.
func (w *Workload) ActiveTenants() []TenantShare {
	if w.Type == WorkloadMultiTenant && len(w.SkewTenants) > 0 {
		return w.SkewTenants
	}
	return w.Tenants
}

// TenantIDs returns the configured tenant identifiers in configuration order.
func (w *Workload) TenantIDs() []string {
	active := w.ActiveTenants()
	ids := make([]string, 0, len(active))
	for _, t := range active {
		ids = append(ids, t.ID)
	}
	return ids
}
