package workload_test

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/workload"
)

func base(t *testing.T, kind string) config.Workload {
	t.Helper()
	cfg := config.Default().Workload
	cfg.Type = kind
	cfg.Count = 400
	return cfg
}

// The reproducibility contract: identical seed plus identical configuration
// yields a byte-identical task sequence.
func TestSameSeedProducesIdenticalSequence(t *testing.T) {
	for _, kind := range config.WorkloadTypes() {
		t.Run(kind, func(t *testing.T) {
			cfg := base(t, kind)
			cfg.Seed = 20240501

			first, err := workload.Generate(cfg)
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			second, err := workload.Generate(cfg)
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatal("two generations with the same seed differ")
			}
			if len(first) != cfg.Count {
				t.Fatalf("generated %d tasks, want %d", len(first), cfg.Count)
			}
		})
	}
}

func TestDifferentSeedProducesDifferentSequence(t *testing.T) {
	// Uniform arrivals are deterministic, so compare the stochastic fields.
	cfg := base(t, config.WorkloadBursty)
	cfg.Seed = 1
	a, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	cfg.Seed = 2
	b, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if reflect.DeepEqual(a, b) {
		t.Fatal("different seeds produced identical sequences")
	}
}

func TestGeneratedTasksAreComplete(t *testing.T) {
	for _, kind := range config.WorkloadTypes() {
		t.Run(kind, func(t *testing.T) {
			cfg := base(t, kind)
			specs, err := workload.Generate(cfg)
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			now := time.Now()
			seen := map[string]bool{}
			for _, s := range specs {
				if s.ID == "" || seen[s.ID] {
					t.Fatalf("duplicate or empty task id %q", s.ID)
				}
				seen[s.ID] = true
				if s.TenantID == "" {
					t.Fatalf("task %s has no tenant", s.ID)
				}
				if s.ExecMillis <= 0 {
					t.Fatalf("task %s has non-positive exec_ms %d", s.ID, s.ExecMillis)
				}
				if s.DeadlineSlackMillis <= 0 {
					t.Fatalf("task %s has non-positive deadline slack", s.ID)
				}
				if s.Offset < 0 {
					t.Fatalf("task %s has a negative arrival offset", s.ID)
				}
				task := s.TaskAt(now)
				if err := task.Validate(); err != nil {
					t.Fatalf("generated task is invalid: %v", err)
				}
				if !task.Deadline.After(task.SubmittedAt) {
					t.Fatalf("task %s deadline is not after submission", s.ID)
				}
			}
		})
	}
}

func TestArrivalOffsetsAreMonotonic(t *testing.T) {
	for _, kind := range config.WorkloadTypes() {
		t.Run(kind, func(t *testing.T) {
			specs, err := workload.Generate(base(t, kind))
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			for i := 1; i < len(specs); i++ {
				if specs[i].Offset < specs[i-1].Offset {
					t.Fatalf("arrival offsets went backwards at index %d", i)
				}
			}
		})
	}
}

func TestUniformArrivalsAreEvenlySpaced(t *testing.T) {
	cfg := base(t, config.WorkloadUniform)
	cfg.ArrivalRatePerSec = 100
	specs, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	want := 10 * time.Millisecond
	for i := 1; i < len(specs); i++ {
		if gap := specs[i].Offset - specs[i-1].Offset; gap != want {
			t.Fatalf("gap %d = %s, want %s", i, gap, want)
		}
	}
}

func TestMultiTenantUsesTheSkewedDistribution(t *testing.T) {
	cfg := base(t, config.WorkloadMultiTenant)
	cfg.Count = 20000
	specs, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	counts := map[string]int{}
	for _, s := range specs {
		counts[s.TenantID]++
	}
	shareA := float64(counts["A"]) / float64(len(specs))
	if math.Abs(shareA-0.90) > 0.02 {
		t.Fatalf("tenant A share = %.3f, want ~0.90", shareA)
	}
	for _, tenant := range []string{"B", "C", "D", "E"} {
		if counts[tenant] == 0 {
			t.Fatalf("tenant %s received no tasks", tenant)
		}
	}
}

func TestNonSkewedWorkloadsSpreadTenantsEvenly(t *testing.T) {
	cfg := base(t, config.WorkloadUniform)
	cfg.Count = 20000
	specs, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	counts := map[string]int{}
	for _, s := range specs {
		counts[s.TenantID]++
	}
	for tenant, n := range counts {
		share := float64(n) / float64(len(specs))
		if math.Abs(share-0.2) > 0.02 {
			t.Fatalf("tenant %s share = %.3f, want ~0.20", tenant, share)
		}
	}
}

func TestHeavyTailedHasAShortMedianAndALongTail(t *testing.T) {
	cfg := base(t, config.WorkloadHeavyTailed)
	cfg.Count = 20000
	specs, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	var short, long int
	var max int64
	for _, s := range specs {
		if s.ExecMillis <= 2*cfg.HeavyTail.MinMillis {
			short++
		}
		if s.ExecMillis > 20*cfg.HeavyTail.MinMillis {
			long++
		}
		if s.ExecMillis > max {
			max = s.ExecMillis
		}
		if s.ExecMillis > cfg.HeavyTail.MaxMillis {
			t.Fatalf("exec_ms %d exceeds the clamp %d", s.ExecMillis, cfg.HeavyTail.MaxMillis)
		}
	}
	if frac := float64(short) / float64(len(specs)); frac < 0.5 {
		t.Fatalf("only %.2f of tasks are short, expected a majority", frac)
	}
	if long == 0 {
		t.Fatal("heavy-tailed workload produced no long tasks")
	}
	if max < 10*cfg.HeavyTail.MinMillis {
		t.Fatalf("longest task %dms is not a tail", max)
	}
}

func TestBurstyWorkloadHasDenserPeriods(t *testing.T) {
	cfg := base(t, config.WorkloadBursty)
	cfg.Count = 4000
	specs, err := workload.Generate(cfg)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	period := cfg.Burst.BurstPeriod.D()
	burst := cfg.Burst.BurstDuration.D()

	var inBurst, outside int
	for _, s := range specs {
		if s.Offset%period < burst {
			inBurst++
		} else {
			outside++
		}
	}
	span := specs[len(specs)-1].Offset
	burstFraction := float64(burst) / float64(period)
	// Arrivals should concentrate far above the burst windows' share of time.
	if got := float64(inBurst) / float64(inBurst+outside); got < burstFraction*2 {
		t.Fatalf("burst windows hold %.3f of arrivals but only %.3f of time; expected concentration (span %s)",
			got, burstFraction, span)
	}
}

func TestGenerateRejectsInvalidConfiguration(t *testing.T) {
	cfg := base(t, config.WorkloadUniform)
	cfg.Type = "nonsense"
	if _, err := workload.Generate(cfg); err == nil {
		t.Fatal("expected an error for an unknown workload type")
	}
	cfg = base(t, config.WorkloadUniform)
	cfg.Count = 0
	if _, err := workload.Generate(cfg); err == nil {
		t.Fatal("expected an error for a zero task count")
	}
}

func TestSummarise(t *testing.T) {
	specs, err := workload.Generate(base(t, config.WorkloadUniform))
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	s := workload.Summarise(specs)
	if s.Count != len(specs) {
		t.Fatalf("count = %d, want %d", s.Count, len(specs))
	}
	if s.MeanExecMS <= 0 || s.MaxExecMS <= 0 || s.Span <= 0 {
		t.Fatalf("summary looks empty: %+v", s)
	}
	total := 0
	for _, n := range s.TenantCounts {
		total += n
	}
	if total != len(specs) {
		t.Fatalf("tenant counts sum to %d, want %d", total, len(specs))
	}
}
