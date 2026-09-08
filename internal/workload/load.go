package workload

import (
	"fmt"

	"github.com/anishc23/distributed-task-queue/internal/config"
)

// Offered load
//
// Comparing scheduling policies across workloads is only meaningful when the
// workloads present the same load relative to what the system can serve.
// "Offered load" expresses that as a dimensionless multiple of capacity:
//
//	capacity      = worker_slots / mean_execution_seconds     (tasks/second)
//	offered_load  = mean_arrival_rate / capacity
//
// An offered load below 1 means the system can keep up and queues stay short;
// above 1 a backlog grows for as long as arrivals continue, which is the regime
// where scheduling policy actually decides who waits.
//
// Two subtleties this file exists to handle:
//
//  1. Mean execution time is not identical across workloads. The heavy-tailed
//     workload draws from a Pareto distribution and has a different mean from
//     the uniform ones, so the same arrival rate is a different offered load.
//  2. The bursty workload's arrival rate is not `arrival_rate_per_sec` at all;
//     it is driven by the burst schedule. Its long-run mean is the
//     duration-weighted average of the burst and baseline rates.
//
// Ignoring either produces experiments that look comparable and are not.

// MeanArrivalRatePerSec returns the long-run mean arrival rate the workload
// will actually generate, in tasks per second.
//
// For the bursty workload this is the duration-weighted average across a burst
// period, not the `arrival_rate_per_sec` field, which that workload ignores.
func MeanArrivalRatePerSec(cfg config.Workload) float64 {
	if cfg.Type != config.WorkloadBursty {
		return cfg.ArrivalRatePerSec
	}
	period := cfg.Burst.BurstPeriod.D().Seconds()
	burst := cfg.Burst.BurstDuration.D().Seconds()
	if period <= 0 || burst < 0 || burst > period {
		return cfg.Burst.BaseRatePerSec
	}
	return (burst*cfg.Burst.BurstRatePerSec + (period-burst)*cfg.Burst.BaseRatePerSec) / period
}

// WithArrivalRate returns a copy of cfg whose mean arrival rate is target
// tasks per second.
//
// For bursty workloads both the baseline and burst rates are scaled by the same
// factor, which preserves the burstiness ratio while moving the mean. Scaling
// only one of them would change the shape of the workload as well as its
// intensity, confounding the two.
func WithArrivalRate(cfg config.Workload, target float64) (config.Workload, error) {
	if target <= 0 {
		return cfg, fmt.Errorf("workload: target arrival rate must be > 0, got %g", target)
	}
	out := cfg
	if cfg.Type != config.WorkloadBursty {
		out.ArrivalRatePerSec = target
		return out, nil
	}
	current := MeanArrivalRatePerSec(cfg)
	if current <= 0 {
		return cfg, fmt.Errorf("workload: bursty mean arrival rate is %g, cannot scale", current)
	}
	factor := target / current
	out.Burst = cfg.Burst
	out.Burst.BaseRatePerSec = cfg.Burst.BaseRatePerSec * factor
	out.Burst.BurstRatePerSec = cfg.Burst.BurstRatePerSec * factor
	// Keep the nominal field consistent so it is not misleading in the manifest.
	out.ArrivalRatePerSec = target
	return out, nil
}

// Capacity returns the tasks per second the workers can serve, given the mean
// simulated execution time of the generated plan and the total number of
// execution slots.
func Capacity(meanExecMillis float64, slots int) float64 {
	if meanExecMillis <= 0 || slots <= 0 {
		return 0
	}
	return float64(slots) / (meanExecMillis / 1000)
}

// OfferedLoad returns the mean arrival rate as a multiple of capacity.
func OfferedLoad(meanArrivalRate, capacity float64) float64 {
	if capacity <= 0 {
		return 0
	}
	return meanArrivalRate / capacity
}

// RateForLoad returns the mean arrival rate that produces the requested offered
// load for a plan with the given mean execution time and slot count.
func RateForLoad(load, meanExecMillis float64, slots int) (float64, error) {
	c := Capacity(meanExecMillis, slots)
	if c <= 0 {
		return 0, fmt.Errorf("workload: cannot derive a rate from mean exec %gms and %d slots", meanExecMillis, slots)
	}
	if load <= 0 {
		return 0, fmt.Errorf("workload: offered load must be > 0, got %g", load)
	}
	return load * c, nil
}
