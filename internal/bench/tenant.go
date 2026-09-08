package bench

import (
	"math"
	"sort"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// TenantStat is the per-tenant view of one experiment.
//
// Jain's index over completed service is demand-limited: under the 90/3/3/2/2
// skew a small tenant cannot receive a large share of service because it never
// asks for one, so every scheduler reports a similarly low index. The signal
// that actually distinguishes policies under skew is per-tenant latency, which
// is why these statistics are recorded alongside the fairness index.
type TenantStat struct {
	Tenant           string       `json:"tenant"`
	Completed        int          `json:"completed"`
	ServiceSeconds   float64      `json:"service_seconds"`
	ServiceShare     float64      `json:"service_share"`
	Latency          LatencyStats `json:"latency_seconds"`
	Wait             LatencyStats `json:"queue_wait_seconds"`
	DeadlineMissed   int          `json:"deadline_missed"`
	DeadlineMissRate float64      `json:"deadline_miss_rate"`
}

// TenantStats computes per-tenant statistics, including tenants that received
// no service at all so that starvation is visible rather than absent.
func TenantStats(results []domain.Result, tenants []string) []TenantStat {
	byTenant := make(map[string][]domain.Result, len(tenants))
	for _, t := range tenants {
		byTenant[t] = nil
	}
	for _, r := range results {
		byTenant[r.TenantID] = append(byTenant[r.TenantID], r)
	}

	names := make([]string, 0, len(byTenant))
	for t := range byTenant {
		names = append(names, t)
	}
	sort.Strings(names)

	var totalService float64
	service := make(map[string]float64, len(names))
	for _, t := range names {
		var s float64
		for _, r := range byTenant[t] {
			ms := r.ActualExecMillis
			if ms <= 0 {
				ms = r.ExecMillis
			}
			s += float64(ms) / 1000
		}
		service[t] = s
		totalService += s
	}

	out := make([]TenantStat, 0, len(names))
	for _, t := range names {
		rs := byTenant[t]
		st := TenantStat{
			Tenant:         t,
			Completed:      len(rs),
			ServiceSeconds: service[t],
			Latency:        SummariseSeconds(secondsOf(rs, domain.Result.Latency)),
			Wait:           SummariseSeconds(secondsOf(rs, domain.Result.QueueWait)),
			DeadlineMissed: DeadlineMissCount(rs),
		}
		if len(rs) > 0 {
			st.DeadlineMissRate = DeadlineMissRate(rs)
		}
		if totalService > 0 {
			st.ServiceShare = service[t] / totalService
		}
		out = append(out, st)
	}
	return out
}

// TenantSpread summarises how unevenly latency is distributed across tenants.
// Ratio is the worst tenant's p99 divided by the best tenant's p99, computed
// only over tenants that completed at least one task.
type TenantSpread struct {
	MaxP99           float64 `json:"max_p99_seconds"`
	MinP99           float64 `json:"min_p99_seconds"`
	Ratio            float64 `json:"p99_ratio"`
	MinServiceShare  float64 `json:"min_service_share"`
	MaxWaitAnyTenant float64 `json:"max_wait_any_tenant_seconds"`
}

// Spread computes the tenant latency spread for an experiment.
func Spread(stats []TenantStat) TenantSpread {
	s := TenantSpread{MinP99: math.NaN(), MaxP99: math.NaN(), MinServiceShare: math.NaN()}
	first := true
	for _, st := range stats {
		if st.MaxWait() > s.MaxWaitAnyTenant {
			s.MaxWaitAnyTenant = st.MaxWait()
		}
		if math.IsNaN(s.MinServiceShare) || st.ServiceShare < s.MinServiceShare {
			s.MinServiceShare = st.ServiceShare
		}
		if st.Completed == 0 {
			continue
		}
		if first {
			s.MinP99, s.MaxP99 = st.Latency.P99, st.Latency.P99
			first = false
			continue
		}
		if st.Latency.P99 < s.MinP99 {
			s.MinP99 = st.Latency.P99
		}
		if st.Latency.P99 > s.MaxP99 {
			s.MaxP99 = st.Latency.P99
		}
	}
	if !first && s.MinP99 > 0 {
		s.Ratio = s.MaxP99 / s.MinP99
	}
	return s
}

// MaxWait returns the tenant's longest observed queue wait in seconds.
func (t TenantStat) MaxWait() float64 { return t.Wait.Max }
