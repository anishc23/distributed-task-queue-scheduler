// Package bench contains the experiment runner, the statistics computed from
// task-level results, and the CSV writers used by the benchmark harness.
package bench

import (
	"math"
	"sort"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Percentile returns the q-th percentile of xs, with q expressed as a fraction
// in [0,1]. It uses linear interpolation between the two closest ranks, which
// is the same definition numpy and pandas use by default, so Go-side and
// Python-side numbers agree. xs is sorted in place.
func Percentile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return minOf(xs)
	}
	if q >= 1 {
		return maxOf(xs)
	}
	sort.Float64s(xs)
	pos := q * float64(len(xs)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return xs[lo]
	}
	frac := pos - float64(lo)
	return xs[lo]*(1-frac) + xs[hi]*frac
}

func minOf(xs []float64) float64 {
	m := xs[0]
	for _, v := range xs[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxOf(xs []float64) float64 {
	m := xs[0]
	for _, v := range xs[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

// JainFairness computes Jain's fairness index:
//
//	J(x) = (sum(x))^2 / (n * sum(x^2))
//
// x is the vector of completed service per tenant. Tenants that received no
// service must be present in x as zeros: including them is what makes the index
// detect starvation, since dropping them would report perfect fairness for a
// queue that served only one tenant.
//
// The result lies in [1/n, 1]. It is 1 when every tenant received identical
// service and 1/n when a single tenant received everything. By convention, a
// vector that is entirely zero is reported as 1 (nobody was treated
// differently) and an empty vector is reported as NaN.
//
// Negative entries are not meaningful for a service vector and are treated as
// zero.
func JainFairness(x []float64) float64 {
	if len(x) == 0 {
		return math.NaN()
	}
	var sum, sumSq float64
	for _, v := range x {
		if v < 0 {
			v = 0
		}
		sum += v
		sumSq += v * v
	}
	if sumSq == 0 {
		return 1
	}
	return (sum * sum) / (float64(len(x)) * sumSq)
}

// DeadlineMissRate returns the fraction of results that finished after their
// deadline. Results without a deadline never count as a miss. An empty input
// yields 0, which is reported alongside a completed count of zero so it can
// never be mistaken for a perfect run.
func DeadlineMissRate(results []domain.Result) float64 {
	if len(results) == 0 {
		return 0
	}
	missed := 0
	for _, r := range results {
		if r.MissedDeadline() {
			missed++
		}
	}
	return float64(missed) / float64(len(results))
}

// DeadlineMissCount counts results that finished after their deadline.
func DeadlineMissCount(results []domain.Result) int {
	n := 0
	for _, r := range results {
		if r.MissedDeadline() {
			n++
		}
	}
	return n
}

// TenantService returns completed service seconds per tenant, seeded with the
// full tenant list so that tenants which received nothing appear as zeros.
func TenantService(results []domain.Result, tenants []string) map[string]float64 {
	out := make(map[string]float64, len(tenants))
	for _, t := range tenants {
		out[t] = 0
	}
	for _, r := range results {
		service := float64(r.ActualExecMillis)
		if service <= 0 {
			service = float64(r.ExecMillis)
		}
		out[r.TenantID] += service / 1000
	}
	return out
}

// TenantCounts returns completed task counts per tenant, seeded with zeros.
func TenantCounts(results []domain.Result, tenants []string) map[string]int {
	out := make(map[string]int, len(tenants))
	for _, t := range tenants {
		out[t] = 0
	}
	for _, r := range results {
		out[r.TenantID]++
	}
	return out
}

// orderedValues returns map values in the order given by keys.
func orderedValues(m map[string]float64, keys []string) []float64 {
	out := make([]float64, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}
	return out
}

// LatencyStats holds the distribution summary of a duration sample.
type LatencyStats struct {
	Count int
	Mean  float64
	P50   float64
	P95   float64
	P99   float64
	Max   float64
}

// SummariseSeconds computes distribution statistics for a sample of seconds.
func SummariseSeconds(xs []float64) LatencyStats {
	if len(xs) == 0 {
		return LatencyStats{}
	}
	var sum float64
	for _, v := range xs {
		sum += v
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	return LatencyStats{
		Count: len(cp),
		Mean:  sum / float64(len(cp)),
		P50:   Percentile(cp, 0.50),
		P95:   Percentile(cp, 0.95),
		P99:   Percentile(cp, 0.99),
		Max:   cp[len(cp)-1],
	}
}

// secondsOf maps results through a duration accessor.
func secondsOf(results []domain.Result, f func(domain.Result) time.Duration) []float64 {
	out := make([]float64, 0, len(results))
	for _, r := range results {
		out = append(out, f(r).Seconds())
	}
	return out
}
