package bench_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/bench"
	"github.com/anishc23/distributed-task-queue/internal/domain"
)

const eps = 1e-9

func approx(t *testing.T, got, want float64, what string) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Fatalf("%s = %.9f, want %.9f", what, got, want)
	}
}

func TestJainFairnessPerfectlyEqual(t *testing.T) {
	approx(t, bench.JainFairness([]float64{5, 5, 5, 5}), 1, "J")
}

func TestJainFairnessSingleTenantTakesEverything(t *testing.T) {
	// One of five tenants receives all the service: J must be exactly 1/n.
	got := bench.JainFairness([]float64{100, 0, 0, 0, 0})
	approx(t, got, 0.2, "J")
}

func TestJainFairnessZeroServiceTenantsCount(t *testing.T) {
	// Dropping the zero-service tenants would report perfect fairness, which is
	// precisely the mistake the index exists to catch.
	withZeros := bench.JainFairness([]float64{10, 10, 0, 0})
	withoutZeros := bench.JainFairness([]float64{10, 10})
	approx(t, withZeros, 0.5, "J with zeros")
	approx(t, withoutZeros, 1, "J without zeros")
	if withZeros >= withoutZeros {
		t.Fatal("zero-service tenants must reduce the fairness index")
	}
}

func TestJainFairnessKnownValue(t *testing.T) {
	// x = [1,2,3]: (6^2) / (3 * 14) = 36/42.
	approx(t, bench.JainFairness([]float64{1, 2, 3}), 36.0/42.0, "J")
}

func TestJainFairnessEdgeCases(t *testing.T) {
	if got := bench.JainFairness(nil); !math.IsNaN(got) {
		t.Fatalf("J(empty) = %v, want NaN", got)
	}
	if got := bench.JainFairness([]float64{0, 0, 0}); got != 1 {
		t.Fatalf("J(all zero) = %v, want 1", got)
	}
	if got := bench.JainFairness([]float64{7}); got != 1 {
		t.Fatalf("J(single tenant) = %v, want 1", got)
	}
	// Negative service is meaningless and is treated as zero.
	approx(t, bench.JainFairness([]float64{-5, 10, 10}), bench.JainFairness([]float64{0, 10, 10}), "J with negatives")
}

func TestJainFairnessStaysInRange(t *testing.T) {
	for _, x := range [][]float64{
		{1, 1, 1, 1, 1},
		{90, 3, 3, 2, 2},
		{1000, 0, 0, 0, 0},
		{0.5, 0.25, 0.125},
	} {
		j := bench.JainFairness(x)
		lower := 1 / float64(len(x))
		if j < lower-eps || j > 1+eps {
			t.Fatalf("J(%v) = %v outside [%v, 1]", x, j, lower)
		}
	}
}

func TestPercentile(t *testing.T) {
	xs := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	approx(t, bench.Percentile(append([]float64(nil), xs...), 0), 1, "p0")
	approx(t, bench.Percentile(append([]float64(nil), xs...), 1), 10, "p100")
	approx(t, bench.Percentile(append([]float64(nil), xs...), 0.5), 5.5, "p50")
	// Linear interpolation, matching numpy's default: pos = 0.95*9 = 8.55.
	approx(t, bench.Percentile(append([]float64(nil), xs...), 0.95), 9.55, "p95")
}

func TestPercentileSortsInput(t *testing.T) {
	xs := []float64{9, 1, 5, 3, 7}
	approx(t, bench.Percentile(xs, 0.5), 5, "p50 of unsorted input")
}

func TestPercentileEmpty(t *testing.T) {
	if got := bench.Percentile(nil, 0.5); !math.IsNaN(got) {
		t.Fatalf("percentile of empty sample = %v, want NaN", got)
	}
}

func result(id string, finishOffset, deadlineOffset time.Duration, tenant string, execMS int64) domain.Result {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return domain.Result{
		TaskID:           id,
		TenantID:         tenant,
		Outcome:          domain.OutcomeCompleted,
		SubmittedAt:      start,
		StartedAt:        start.Add(finishOffset / 2),
		FinishedAt:       start.Add(finishOffset),
		Deadline:         start.Add(deadlineOffset),
		ExecMillis:       execMS,
		ActualExecMillis: execMS,
	}
}

func TestDeadlineMissCalculation(t *testing.T) {
	results := []domain.Result{
		result("hit-early", 100*time.Millisecond, time.Second, "A", 50),
		result("hit-exact", time.Second, time.Second, "A", 50), // finishing exactly on the deadline is a hit
		result("miss", 2*time.Second, time.Second, "A", 50),
		result("miss-barely", time.Second+time.Millisecond, time.Second, "A", 50),
	}
	if got := bench.DeadlineMissCount(results); got != 2 {
		t.Fatalf("miss count = %d, want 2", got)
	}
	approx(t, bench.DeadlineMissRate(results), 0.5, "miss rate")
}

func TestTasksWithoutDeadlineNeverMiss(t *testing.T) {
	r := result("no-deadline", 10*time.Second, 0, "A", 50)
	r.Deadline = time.Time{}
	if r.MissedDeadline() {
		t.Fatal("a task without a deadline must never count as a miss")
	}
	if got := bench.DeadlineMissRate([]domain.Result{r}); got != 0 {
		t.Fatalf("miss rate = %v, want 0", got)
	}
}

func TestDeadlineMissRateOfEmptySample(t *testing.T) {
	if got := bench.DeadlineMissRate(nil); got != 0 {
		t.Fatalf("miss rate = %v, want 0", got)
	}
}

func TestTenantServiceIncludesStarvedTenants(t *testing.T) {
	results := []domain.Result{
		result("a1", time.Second, 2*time.Second, "A", 1000),
		result("a2", time.Second, 2*time.Second, "A", 500),
	}
	svc := bench.TenantService(results, []string{"A", "B", "C"})
	approx(t, svc["A"], 1.5, "tenant A service seconds")
	if _, ok := svc["B"]; !ok {
		t.Fatal("starved tenant B must appear with zero service")
	}
	approx(t, svc["B"], 0, "tenant B service seconds")

	counts := bench.TenantCounts(results, []string{"A", "B"})
	if counts["A"] != 2 || counts["B"] != 0 {
		t.Fatalf("tenant counts = %v", counts)
	}
}

func TestSummariseSeconds(t *testing.T) {
	s := bench.SummariseSeconds([]float64{1, 2, 3, 4})
	if s.Count != 4 {
		t.Fatalf("count = %d, want 4", s.Count)
	}
	approx(t, s.Mean, 2.5, "mean")
	approx(t, s.P50, 2.5, "p50")
	approx(t, s.Max, 4, "max")

	empty := bench.SummariseSeconds(nil)
	if empty.Count != 0 {
		t.Fatalf("empty summary count = %d, want 0", empty.Count)
	}
}

// A plain run must keep the documented <scheduler>_<workload>_rep<N> layout,
// while a sweep must tag every file with its load or experiments at different
// loads would overwrite each other. This rule is easy to break accidentally
// when adding a dimension, and doing so silently destroys results.
func TestLoadSuffixOnlyAppearsDuringASweep(t *testing.T) {
	if got := bench.LoadSuffix(0, "_"); got != "" {
		t.Fatalf("no sweep requested should produce no suffix, got %q", got)
	}
	if got := bench.LoadSuffix(-1, "_"); got != "" {
		t.Fatalf("a non-positive load should produce no suffix, got %q", got)
	}
	if got := bench.LoadSuffix(1.25, "_"); got != "_l1p25" {
		t.Fatalf("file suffix = %q, want %q", got, "_l1p25")
	}
	if got := bench.LoadSuffix(1.25, "-"); got != "-l1p25" {
		t.Fatalf("namespace suffix = %q, want %q", got, "-l1p25")
	}
	// Distinct loads must produce distinct suffixes, including ones that differ
	// only after the decimal point.
	seen := map[string]bool{}
	for _, l := range []float64{0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0} {
		s := bench.LoadSuffix(l, "_")
		if seen[s] {
			t.Fatalf("load %g produced a duplicate suffix %q", l, s)
		}
		seen[s] = true
	}
	// No dots, which would be awkward in a filename.
	if strings.Contains(bench.LoadSuffix(1.5, "_"), ".") {
		t.Fatal("suffix must not contain a dot")
	}
}
