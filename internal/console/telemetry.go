package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// Health is what the console can say about one process from its endpoints.
// The values follow the endpoints' own semantics (internal/metrics): /healthz
// answers 200 while the process is alive, /readyz answers 200 once it has
// finished starting and is consuming. A standby scheduler is ready; readiness
// is not leadership.
type Health string

const (
	// HealthUnreachable: no HTTP response at all. The process may be dead,
	// frozen, or simply not listening; the console cannot tell which, and says
	// so rather than guessing.
	HealthUnreachable Health = "unreachable"
	// HealthUnhealthy: it answered, but /healthz was not 200.
	HealthUnhealthy Health = "unhealthy"
	// HealthNotReady: alive, but /readyz was not 200.
	HealthNotReady Health = "not_ready"
	// HealthReady: both endpoints answered 200.
	HealthReady Health = "ready"
)

// Classify turns the two probe results into a Health. healthzErr is a
// transport error (no response); a response with any status is not an error.
func Classify(healthzErr error, healthz, readyz int) Health {
	switch {
	case healthzErr != nil:
		return HealthUnreachable
	case healthz != http.StatusOK:
		return HealthUnhealthy
	case readyz != http.StatusOK:
		return HealthNotReady
	default:
		return HealthReady
	}
}

// Target is one process the console scrapes.
type Target struct {
	Name string `json:"name"`
	Role string `json:"role"` // "scheduler" or "worker"
	URL  string `json:"url"`  // base, e.g. http://127.0.0.1:9101
}

// Probe is the result of one scrape of one target.
type Probe struct {
	Health        Health             `json:"health"`
	HealthzStatus int                `json:"healthz_status,omitempty"`
	ReadyzStatus  int                `json:"readyz_status,omitempty"`
	Error         string             `json:"error,omitempty"`
	Metrics       map[string]float64 `json:"metrics,omitempty"`
	Policy        string             `json:"policy,omitempty"`
}

// The series the console reads. Counters summed across their tenant label;
// gauges read as-is. Anything not listed is ignored, so a process exposing
// more does not change what the console reports.
var wantedMetrics = map[string]bool{
	"tq_scheduler_is_leader":                true,
	"tq_scheduler_leader_epoch":             true,
	"tq_scheduler_leader_transitions_total": true,
	"tq_scheduler_fenced_operations_total":  true,
	"tq_pending_tasks":                      true,
	"tq_inflight_tasks":                     true,
	"tq_longest_pending_wait_seconds":       true,
	"tq_tasks_admitted_total":               true,
	"tq_tasks_dispatched_total":             true,
	"tq_ingress_reclaimed_total":            true,
	"tq_recovery_reclaimed_total":           true,
	"tq_dispatch_errors_total":              true,
	"tq_tasks_completed_total":              true,
	"tq_task_failures_total":                true,
	"tq_task_retries_total":                 true,
	"tq_tasks_dead_lettered_total":          true,
	"tq_duplicate_completions_total":        true,
	"tq_worker_utilization_ratio":           true,
	"tq_worker_concurrency":                 true,
}

// Scraper probes targets over HTTP with a hard per-request timeout.
type Scraper struct {
	Client  *http.Client
	Timeout time.Duration
}

// NewScraper returns a scraper whose every request is bounded by timeout.
func NewScraper(timeout time.Duration) *Scraper {
	return &Scraper{Client: &http.Client{Timeout: timeout}, Timeout: timeout}
}

func (s *Scraper) status(ctx context.Context, url string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	res, err := s.Client.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	res.Body.Close()
	return res.StatusCode, nil
}

// Probe checks /healthz and /readyz and, if the process answers, reads
// /metrics. A failed metrics read leaves Metrics nil and says why; it does not
// change Health, because the endpoints that define health answered.
func (s *Scraper) Probe(ctx context.Context, t Target) Probe {
	var p Probe
	hz, err := s.status(ctx, t.URL+"/healthz")
	if err != nil {
		p.Health = Classify(err, 0, 0)
		p.Error = shortErr(err)
		return p
	}
	p.HealthzStatus = hz
	rz, err := s.status(ctx, t.URL+"/readyz")
	if err != nil {
		// Alive a moment ago and gone now: report what was observed.
		p.Health = HealthUnreachable
		p.Error = "readyz: " + shortErr(err)
		return p
	}
	p.ReadyzStatus = rz
	p.Health = Classify(nil, hz, rz)

	m, policy, err := s.metrics(ctx, t.URL+"/metrics")
	if err != nil {
		p.Error = "metrics: " + shortErr(err)
		return p
	}
	p.Metrics, p.Policy = m, policy
	return p
}

func (s *Scraper) metrics(ctx context.Context, url string) (map[string]float64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "text/plain")
	res, err := s.Client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("status %d", res.StatusCode)
	}
	return ParseMetrics(io.LimitReader(res.Body, 4<<20))
}

// ParseMetrics reads Prometheus text exposition and returns the wanted series,
// summing counters and gauges across label values, and the value of the
// constant "scheduler" label, which every series carries and which names the
// active policy.
func ParseMetrics(r io.Reader) (map[string]float64, string, error) {
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, "", err
	}
	out := make(map[string]float64)
	policy := ""
	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			if policy == "" {
				for _, l := range m.GetLabel() {
					if l.GetName() == "scheduler" {
						policy = l.GetValue()
					}
				}
			}
			if !wantedMetrics[name] {
				continue
			}
			v, ok := value(fam.GetType(), m)
			if ok {
				out[name] += v
			}
		}
	}
	return out, policy, nil
}

func value(t dto.MetricType, m *dto.Metric) (float64, bool) {
	switch t {
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue(), true
	case dto.MetricType_GAUGE:
		return m.GetGauge().GetValue(), true
	case dto.MetricType_UNTYPED:
		return m.GetUntyped().GetValue(), true
	}
	return 0, false
}

func shortErr(err error) string {
	var ue interface{ Timeout() bool }
	if errors.As(err, &ue) && ue.Timeout() {
		return "timed out"
	}
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 && strings.Contains(s, "dial tcp") {
		return "connection failed: " + s[i+2:]
	}
	return s
}

// Rate turns two readings of a monotonic counter into a per-second rate. It
// returns false rather than a number when the answer would be invented: too
// little time between readings, or a counter that went backwards (a reset).
func Rate(prev, cur float64, dt time.Duration) (float64, bool) {
	if dt < 200*time.Millisecond || cur < prev {
		return 0, false
	}
	return (cur - prev) / dt.Seconds(), true
}
