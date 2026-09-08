package workload_test

import (
	"math"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/workload"
)

func closeTo(t *testing.T, got, want, tol float64, what string) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s = %g, want %g (tolerance %g)", what, got, want, tol)
	}
}

func TestMeanArrivalRateIgnoresTheNominalFieldForBursty(t *testing.T) {
	cfg := config.Default().Workload
	cfg.Type = config.WorkloadBursty
	cfg.ArrivalRatePerSec = 999 // bursty does not use this
	cfg.Burst = config.BurstSpec{
		BaseRatePerSec:  200,
		BurstRatePerSec: 2000,
		BurstDuration:   config.Duration(500_000_000),   // 500ms
		BurstPeriod:     config.Duration(3_000_000_000), // 3s
	}
	// (0.5*2000 + 2.5*200)/3 = 500
	closeTo(t, workload.MeanArrivalRatePerSec(cfg), 500, 1e-9, "bursty mean arrival rate")

	cfg.Type = config.WorkloadUniform
	closeTo(t, workload.MeanArrivalRatePerSec(cfg), 999, 1e-9, "non-bursty mean arrival rate")
}

func TestWithArrivalRateHitsTheTarget(t *testing.T) {
	for _, kind := range config.WorkloadTypes() {
		t.Run(kind, func(t *testing.T) {
			cfg := config.Default().Workload
			cfg.Type = kind
			for _, target := range []float64{50, 320, 960} {
				scaled, err := workload.WithArrivalRate(cfg, target)
				if err != nil {
					t.Fatalf("WithArrivalRate: %v", err)
				}
				closeTo(t, workload.MeanArrivalRatePerSec(scaled), target, 1e-6,
					"mean arrival rate after scaling")
			}
		})
	}
}

// Scaling a bursty workload must change its intensity without changing its
// shape, otherwise a load sweep would confound the two.
func TestScalingBurstyPreservesTheBurstinessRatio(t *testing.T) {
	cfg := config.Default().Workload
	cfg.Type = config.WorkloadBursty
	before := cfg.Burst.BurstRatePerSec / cfg.Burst.BaseRatePerSec

	scaled, err := workload.WithArrivalRate(cfg, workload.MeanArrivalRatePerSec(cfg)*2.5)
	if err != nil {
		t.Fatalf("WithArrivalRate: %v", err)
	}
	after := scaled.Burst.BurstRatePerSec / scaled.Burst.BaseRatePerSec
	closeTo(t, after, before, 1e-9, "burstiness ratio")

	if scaled.Burst.BurstDuration != cfg.Burst.BurstDuration ||
		scaled.Burst.BurstPeriod != cfg.Burst.BurstPeriod {
		t.Fatal("scaling must not change the burst timing, only the rates")
	}
}

// Changing the arrival rate must not change which tasks are generated: the
// durations, priorities and tenants must be identical, or a load sweep would
// vary two things at once.
func TestScalingRateDoesNotChangeTheTaskMix(t *testing.T) {
	for _, kind := range config.WorkloadTypes() {
		t.Run(kind, func(t *testing.T) {
			cfg := config.Default().Workload
			cfg.Type = kind
			cfg.Count = 2000

			slow, err := workload.WithArrivalRate(cfg, 100)
			if err != nil {
				t.Fatal(err)
			}
			fast, err := workload.WithArrivalRate(cfg, 900)
			if err != nil {
				t.Fatal(err)
			}
			a, err := workload.Generate(slow)
			if err != nil {
				t.Fatal(err)
			}
			b, err := workload.Generate(fast)
			if err != nil {
				t.Fatal(err)
			}
			if len(a) != len(b) {
				t.Fatalf("task counts differ: %d vs %d", len(a), len(b))
			}
			for i := range a {
				if a[i].ID != b[i].ID || a[i].TenantID != b[i].TenantID ||
					a[i].ExecMillis != b[i].ExecMillis || a[i].Priority != b[i].Priority ||
					a[i].DeadlineSlackMillis != b[i].DeadlineSlackMillis {
					t.Fatalf("task %d differs between arrival rates:\n  slow %+v\n  fast %+v", i, a[i], b[i])
				}
			}
			// Only the arrival timing should differ.
			if a[len(a)-1].Offset <= b[len(b)-1].Offset {
				t.Fatalf("a faster arrival rate must compress the arrival span: %s vs %s",
					a[len(a)-1].Offset, b[len(b)-1].Offset)
			}
		})
	}
}

func TestCapacityAndOfferedLoad(t *testing.T) {
	// 16 slots at 50ms mean service = 320 tasks/s.
	closeTo(t, workload.Capacity(50, 16), 320, 1e-9, "capacity")
	closeTo(t, workload.OfferedLoad(400, 320), 1.25, 1e-9, "offered load")

	rate, err := workload.RateForLoad(1.5, 50, 16)
	if err != nil {
		t.Fatal(err)
	}
	closeTo(t, rate, 480, 1e-9, "rate for 1.5x load")

	// A workload with a longer mean service time has lower capacity, so the same
	// arrival rate is a higher offered load. This is the trap the helpers exist
	// to avoid.
	closeTo(t, workload.Capacity(51.4, 16), 311.28, 0.01, "heavy-tailed capacity")
	if workload.OfferedLoad(400, workload.Capacity(51.4, 16)) <= workload.OfferedLoad(400, workload.Capacity(50.1, 16)) {
		t.Fatal("a slower workload at the same arrival rate must present a higher offered load")
	}
}

func TestLoadHelpersRejectNonsense(t *testing.T) {
	if _, err := workload.WithArrivalRate(config.Default().Workload, 0); err == nil {
		t.Fatal("expected an error for a zero target rate")
	}
	if got := workload.Capacity(0, 16); got != 0 {
		t.Fatalf("capacity with zero exec time = %g, want 0", got)
	}
	if got := workload.OfferedLoad(400, 0); got != 0 {
		t.Fatalf("offered load with zero capacity = %g, want 0", got)
	}
	if _, err := workload.RateForLoad(1, 0, 16); err == nil {
		t.Fatal("expected an error when capacity cannot be derived")
	}
}
