package console

import (
	"sort"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// TenantStat is one tenant's completions over a window, from the results
// stream: what each task actually experienced, end to end.
type TenantStat struct {
	Tenant         string  `json:"tenant"`
	Completed      int     `json:"completed"`
	LatencyP50MS   float64 `json:"latency_p50_ms"`
	LatencyP95MS   float64 `json:"latency_p95_ms"`
	LatencyMaxMS   float64 `json:"latency_max_ms"`
	DeadlineMissed int     `json:"deadline_missed"`
}

// TenantWindow summarises recent completions per tenant.
type TenantWindow struct {
	ReadAt    time.Time    `json:"read_at"`
	WindowS   int          `json:"window_s"`
	Count     int          `json:"count"`
	Truncated bool         `json:"truncated"`
	Tenants   []TenantStat `json:"tenants"`
}

// SummariseTenants groups completion records by tenant. Latency is finish
// minus submit; percentiles interpolate between order statistics, as the
// benchmark's analysis does. Records missing either time are skipped rather
// than counted as zero.
func SummariseTenants(results []domain.Result, window time.Duration, limit int) TenantWindow {
	out := TenantWindow{WindowS: int(window / time.Second), Count: len(results), Truncated: limit > 0 && len(results) >= limit, Tenants: []TenantStat{}}
	lat := map[string][]float64{}
	missed := map[string]int{}
	for _, r := range results {
		if r.SubmittedAt.IsZero() || r.FinishedAt.IsZero() {
			continue
		}
		lat[r.TenantID] = append(lat[r.TenantID], float64(r.FinishedAt.Sub(r.SubmittedAt).Microseconds())/1000)
		if !r.Deadline.IsZero() && r.FinishedAt.After(r.Deadline) {
			missed[r.TenantID]++
		}
	}
	for tenant, v := range lat {
		sort.Float64s(v)
		out.Tenants = append(out.Tenants, TenantStat{
			Tenant: tenant, Completed: len(v), DeadlineMissed: missed[tenant],
			LatencyP50MS: quantile(v, 0.5), LatencyP95MS: quantile(v, 0.95), LatencyMaxMS: v[len(v)-1],
		})
	}
	sort.Slice(out.Tenants, func(i, j int) bool { return out.Tenants[i].Tenant < out.Tenants[j].Tenant })
	return out
}

// quantile interpolates linearly between order statistics of sorted values.
func quantile(sorted []float64, q float64) float64 {
	pos := float64(len(sorted)-1) * q
	lo := int(pos)
	if lo+1 >= len(sorted) {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[lo+1]-sorted[lo])*(pos-float64(lo))
}
