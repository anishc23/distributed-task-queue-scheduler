// Package workload generates reproducible synthetic task streams.
//
// Reproducibility contract: for a given seed and a given workload
// configuration, Generate always returns the identical slice of Specs, in the
// identical order, on any machine and in any process. This is achieved by
// drawing every random value from a single math/rand source seeded with
// Workload.Seed and by consuming draws in a fixed order per task. Absolute
// wall-clock times are deliberately absent from a Spec; they are attached at
// submission time by the producer, because only relative arrival offsets can be
// reproduced across runs.
package workload

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Spec is one generated task, described relative to the start of the workload.
type Spec struct {
	// ID is the deterministic task identifier.
	ID string
	// TenantID is the owning tenant.
	TenantID string
	// Offset is the arrival time measured from the start of the workload.
	Offset time.Duration
	// ExecMillis is the simulated execution duration in milliseconds.
	ExecMillis int64
	// Priority is the numeric priority; higher is more important.
	Priority int
	// DeadlineSlackMillis is added to the actual submission time to obtain the
	// absolute deadline.
	DeadlineSlackMillis int64
	// MaxRetries is the retry budget for the task.
	MaxRetries int
}

// TaskAt materialises the spec into a domain.Task submitted at the given
// instant. The deadline is always relative to the actual submission time, not
// to the planned arrival time, so producer lag inflates neither latency nor
// deadline slack. Lag is reported separately as a run diagnostic.
func (s Spec) TaskAt(submitted time.Time) domain.Task {
	return domain.Task{
		ID:          s.ID,
		TenantID:    s.TenantID,
		SubmittedAt: submitted,
		ExecMillis:  s.ExecMillis,
		Priority:    s.Priority,
		Deadline:    submitted.Add(time.Duration(s.DeadlineSlackMillis) * time.Millisecond),
		RetryCount:  0,
		MaxRetries:  s.MaxRetries,
		AttemptID:   s.ID + "#0",
	}
}

// Generate builds the full finite workload described by cfg.
func Generate(cfg config.Workload) ([]Spec, error) {
	if err := validate(cfg); err != nil {
		return nil, err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	tenants := cfg.ActiveTenants()
	tenantCDF := buildCDF(sharesOf(tenants))
	prioCDF := buildCDF(cfg.Priority.Weights)

	specs := make([]Spec, 0, cfg.Count)
	var elapsed time.Duration

	for i := 0; i < cfg.Count; i++ {
		// Draw order is fixed: arrival, tenant, duration, priority, deadline
		// jitter. Changing it changes every generated workload, so it is part
		// of the reproducibility contract and covered by tests.
		if i > 0 || arrivalIsStochastic(cfg.Type) {
			elapsed += nextGap(rng, cfg, elapsed)
		}
		tenant := tenants[pick(rng.Float64(), tenantCDF)].ID
		execMS := nextExecMillis(rng, cfg)
		priority := pick(rng.Float64(), prioCDF)
		jitter := int64(0)
		if cfg.Deadline.JitterMillis > 0 {
			jitter = rng.Int63n(cfg.Deadline.JitterMillis + 1)
		}
		slack := cfg.Deadline.BaseMillis +
			int64(math.Round(float64(execMS)*cfg.Deadline.ExecMultiplier)) +
			jitter

		specs = append(specs, Spec{
			ID:                  fmt.Sprintf("%s-%06d", cfg.IDPrefix, i),
			TenantID:            tenant,
			Offset:              elapsed,
			ExecMillis:          execMS,
			Priority:            priority,
			DeadlineSlackMillis: slack,
			MaxRetries:          cfg.MaxRetries,
		})
	}
	return specs, nil
}

// arrivalIsStochastic reports whether the first arrival consumes a random draw.
// Uniform arrivals are deterministic and evenly spaced, so the first task always
// arrives at offset zero and no draw is made for it.
func arrivalIsStochastic(kind string) bool {
	return kind != config.WorkloadUniform
}

func nextGap(rng *rand.Rand, cfg config.Workload, elapsed time.Duration) time.Duration {
	switch cfg.Type {
	case config.WorkloadUniform:
		return time.Duration(float64(time.Second) / cfg.ArrivalRatePerSec)
	case config.WorkloadBursty:
		rate := cfg.Burst.BaseRatePerSec
		if inBurst(elapsed, cfg.Burst) {
			rate = cfg.Burst.BurstRatePerSec
		}
		return exponential(rng, rate)
	default:
		return exponential(rng, cfg.ArrivalRatePerSec)
	}
}

func inBurst(elapsed time.Duration, b config.BurstSpec) bool {
	period := b.BurstPeriod.D()
	if period <= 0 {
		return false
	}
	return elapsed%period < b.BurstDuration.D()
}

// exponential samples an inter-arrival gap of a Poisson process with the given
// rate in events per second.
func exponential(rng *rand.Rand, ratePerSec float64) time.Duration {
	u := rng.Float64()
	// rand.Float64 returns [0,1); guard against log(0).
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	seconds := -math.Log(u) / ratePerSec
	return time.Duration(seconds * float64(time.Second))
}

func nextExecMillis(rng *rand.Rand, cfg config.Workload) int64 {
	if cfg.Type == config.WorkloadHeavyTailed {
		return paretoMillis(rng, cfg.HeavyTail)
	}
	span := cfg.Exec.MaxMillis - cfg.Exec.MinMillis
	if span <= 0 {
		return cfg.Exec.MaxMillis
	}
	return cfg.Exec.MinMillis + rng.Int63n(span+1)
}

// paretoMillis samples a Pareto(alpha, min) duration clamped to max. Most
// samples sit close to min while a small fraction reaches the clamp, which is
// the head-of-line-blocking regime the heavy-tailed experiment targets.
func paretoMillis(rng *rand.Rand, ht config.HeavyTailSpec) int64 {
	u := rng.Float64()
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	v := float64(ht.MinMillis) * math.Pow(u, -1/ht.Alpha)
	if v > float64(ht.MaxMillis) {
		return ht.MaxMillis
	}
	return int64(v)
}

func sharesOf(tenants []config.TenantShare) []float64 {
	out := make([]float64, len(tenants))
	for i, t := range tenants {
		out[i] = t.Share
	}
	return out
}

// buildCDF converts relative weights into a normalised cumulative distribution.
func buildCDF(weights []float64) []float64 {
	cdf := make([]float64, len(weights))
	var total float64
	for _, w := range weights {
		total += w
	}
	var acc float64
	for i, w := range weights {
		acc += w
		cdf[i] = acc / total
	}
	if len(cdf) > 0 {
		cdf[len(cdf)-1] = 1
	}
	return cdf
}

// pick maps a uniform draw in [0,1) onto a CDF index.
func pick(u float64, cdf []float64) int {
	for i, c := range cdf {
		if u < c {
			return i
		}
	}
	return len(cdf) - 1
}

func validate(cfg config.Workload) error {
	if !config.ValidWorkload(cfg.Type) {
		return fmt.Errorf("workload: unknown type %q, expected one of %v", cfg.Type, config.WorkloadTypes())
	}
	if cfg.Count <= 0 {
		return fmt.Errorf("workload: count must be > 0, got %d", cfg.Count)
	}
	if len(cfg.ActiveTenants()) == 0 {
		return fmt.Errorf("workload: no tenants configured for type %q", cfg.Type)
	}
	if len(cfg.Priority.Weights) == 0 {
		return fmt.Errorf("workload: priority weights are required")
	}
	return nil
}

// Summary describes a generated plan. It is recorded alongside benchmark output
// so a result row can always be traced back to the workload that produced it.
type Summary struct {
	Count        int
	Span         time.Duration
	TotalExecMS  int64
	MeanExecMS   float64
	MaxExecMS    int64
	TenantCounts map[string]int
}

// Summarise computes descriptive statistics for a generated plan.
func Summarise(specs []Spec) Summary {
	s := Summary{Count: len(specs), TenantCounts: map[string]int{}}
	for _, sp := range specs {
		s.TotalExecMS += sp.ExecMillis
		if sp.ExecMillis > s.MaxExecMS {
			s.MaxExecMS = sp.ExecMillis
		}
		if sp.Offset > s.Span {
			s.Span = sp.Offset
		}
		s.TenantCounts[sp.TenantID]++
	}
	if len(specs) > 0 {
		s.MeanExecMS = float64(s.TotalExecMS) / float64(len(specs))
	}
	return s
}
