package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/domain"
)

func TestClassifyFollowsTheEndpointSemantics(t *testing.T) {
	cases := []struct {
		err      error
		hz, rz   int
		expected Health
	}{
		{errors.New("connection refused"), 0, 0, HealthUnreachable},
		{nil, 500, 200, HealthUnhealthy},
		{nil, 200, 503, HealthNotReady},
		{nil, 200, 200, HealthReady},
	}
	for _, c := range cases {
		if got := Classify(c.err, c.hz, c.rz); got != c.expected {
			t.Errorf("Classify(%v, %d, %d) = %s, want %s", c.err, c.hz, c.rz, got, c.expected)
		}
	}
}

const exposition = `# HELP tq_scheduler_is_leader 1 when leading.
# TYPE tq_scheduler_is_leader gauge
tq_scheduler_is_leader{component="scheduler",scheduler="wfq"} 1
# TYPE tq_tasks_dispatched_total counter
tq_tasks_dispatched_total{component="scheduler",scheduler="wfq",tenant="A"} 10
tq_tasks_dispatched_total{component="scheduler",scheduler="wfq",tenant="B"} 5
# TYPE go_goroutines gauge
go_goroutines 12
`

func TestParseMetricsSumsAcrossTenantsAndReadsThePolicy(t *testing.T) {
	m, policy, err := ParseMetrics(strings.NewReader(exposition))
	if err != nil {
		t.Fatal(err)
	}
	if policy != "wfq" {
		t.Errorf("policy = %q, want wfq", policy)
	}
	if m["tq_tasks_dispatched_total"] != 15 {
		t.Errorf("dispatched = %v, want 15 summed over tenants", m["tq_tasks_dispatched_total"])
	}
	if m["tq_scheduler_is_leader"] != 1 {
		t.Errorf("is_leader = %v, want 1", m["tq_scheduler_is_leader"])
	}
	if _, ok := m["go_goroutines"]; ok {
		t.Error("series the console does not use leaked into its metrics")
	}
}

func TestProbeDistinguishesEveryState(t *testing.T) {
	ready := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/readyz":
			if !ready {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		case "/metrics":
			_, _ = w.Write([]byte(exposition))
		}
	}))
	s := NewScraper(time.Second)
	target := Target{Name: "s", Role: "scheduler", URL: srv.URL}

	p := s.Probe(context.Background(), target)
	if p.Health != HealthReady || p.Metrics["tq_scheduler_is_leader"] != 1 {
		t.Errorf("ready process: %+v", p)
	}
	ready = false
	if p := s.Probe(context.Background(), target); p.Health != HealthNotReady {
		t.Errorf("unready process reported %s", p.Health)
	}
	srv.Close()
	p = s.Probe(context.Background(), target)
	if p.Health != HealthUnreachable || p.Metrics != nil || p.Error == "" {
		t.Errorf("closed server must be unreachable with no metrics and a reason, got %+v", p)
	}
}

func TestRateRefusesToInventANumber(t *testing.T) {
	if r, ok := Rate(100, 150, time.Second); !ok || r != 50 {
		t.Errorf("Rate = %v, %v; want 50, true", r, ok)
	}
	if _, ok := Rate(150, 100, time.Second); ok {
		t.Error("a counter that went backwards was turned into a rate")
	}
	if _, ok := Rate(100, 150, 10*time.Millisecond); ok {
		t.Error("a rate was computed over too short an interval")
	}
}

func i64(v int64) *int64 { return &v }
func intp(v int) *int    { return &v }

func TestDiffReportsLeadershipFenceAndHealthChanges(t *testing.T) {
	prev := &Snapshot{
		Redis:      RedisState{OK: true},
		Leadership: Leadership{Held: true, Owner: "sched-0", Epoch: 1, Fence: i64(1)},
		Components: []Component{{Target: Target{Name: "sched-0"}, Probe: Probe{Health: HealthReady, Metrics: map[string]float64{"tq_scheduler_fenced_operations_total": 0}}}},
		LeaderSum:  intp(1),
	}
	cur := &Snapshot{
		SampledAt:  time.Unix(100, 0),
		Redis:      RedisState{OK: true},
		Leadership: Leadership{Held: true, Owner: "sched-1", Epoch: 2, Fence: i64(2)},
		Components: []Component{{Target: Target{Name: "sched-0"}, Probe: Probe{Health: HealthReady, Metrics: map[string]float64{"tq_scheduler_fenced_operations_total": 1}}}},
		LeaderSum:  intp(2),
	}
	kinds := map[string]bool{}
	for _, e := range Diff(prev, cur) {
		kinds[e.Kind] = true
		if e.Source != SourceObserved || !e.At.Equal(cur.SampledAt) {
			t.Errorf("%s: observed events carry the poll's time and source, got %s at %s", e.Kind, e.Source, e.At)
		}
	}
	for _, k := range []string{"lease_holder", "fence_advanced", "fenced_write", "several_leaders"} {
		if !kinds[k] {
			t.Errorf("missing %s event; got %v", k, kinds)
		}
	}
	if len(Diff(cur, cur)) != 0 {
		t.Error("an unchanged snapshot produced events")
	}
}

func TestDiffDoesNotReportLeadershipWhenRedisCouldNotBeRead(t *testing.T) {
	prev := &Snapshot{Redis: RedisState{OK: true}, Leadership: Leadership{Held: true, Owner: "a", Epoch: 1}}
	cur := &Snapshot{Redis: RedisState{OK: false, Error: "timeout"}}
	for _, e := range Diff(prev, cur) {
		if e.Kind == "lease_vacant" {
			t.Fatal("an unreadable lease was reported as a vacant one")
		}
	}
}

func execAt(ms int64, epoch int64) broker.ExecMessage {
	return broker.ExecMessage{Task: domain.Task{TenantID: "A"}, DispatchedAt: time.UnixMilli(ms), Epoch: epoch}
}

func TestSummariseDispatchesMeasuresGapsAndInversions(t *testing.T) {
	from := time.UnixMilli(10_000)
	msgs := []broker.ExecMessage{
		execAt(10_000, 1), execAt(10_100, 1), execAt(10_200, 0),
		execAt(14_500, 2), // a 4.3 s outage, then a new term
		execAt(14_600, 1), // a superseded leader writing after its successor
		execAt(14_700, 2),
	}
	w := SummariseDispatches(msgs, from, time.Minute, 250*time.Millisecond, 100)
	if w.MaxGapMS == nil || *w.MaxGapMS != 4300 || *w.MaxGapEndMS != 14_500 {
		t.Errorf("max gap = %v ending %v, want 4300 ending 14500", deref(w.MaxGapMS), deref(w.MaxGapEndMS))
	}
	if w.EpochInversions != 1 {
		t.Errorf("inversions = %d, want 1", w.EpochInversions)
	}
	if w.Unstamped != 1 {
		t.Errorf("unstamped = %d, want 1", w.Unstamped)
	}
	if len(w.Epochs) != 2 || w.Epochs[0] != 1 || w.Epochs[1] != 2 {
		t.Errorf("epochs = %v, want [1 2]", w.Epochs)
	}
	total := 0
	for _, b := range w.Buckets {
		total += b.N
		if (b.TMS-w.FromMS)%w.BucketMS != 0 {
			t.Errorf("bucket %d is not aligned to the window", b.TMS)
		}
	}
	if total != len(msgs) {
		t.Errorf("buckets hold %d dispatches, want %d", total, len(msgs))
	}
	if w.Truncated {
		t.Error("a read under the cap was marked truncated")
	}
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestEventLogIsBoundedAndOrdered(t *testing.T) {
	l := NewEventLog(3)
	for i := 0; i < 5; i++ {
		l.Add(Event{Kind: "k"})
	}
	all := l.Since(0)
	if len(all) != 3 || all[0].Seq != 3 || all[2].Seq != 5 {
		t.Fatalf("kept %v, want seq 3..5", all)
	}
	if got := l.Since(4); len(got) != 1 || got[0].Seq != 5 {
		t.Errorf("Since(4) = %v", got)
	}
}

func TestValidateAllowsOnlyTheTable(t *testing.T) {
	ok := []FaultRequest{
		{Action: ActionKill, Target: "leader"},
		{Action: ActionFreeze, Target: "sched-0", DurationMS: 10_000},
		{Action: ActionPartition, Target: "sched-1", DurationMS: 1_000},
		{Action: ActionRestart, Target: "worker-0"},
	}
	for _, r := range ok {
		if _, err := Validate(r); err != nil {
			t.Errorf("%+v rejected: %v", r, err)
		}
	}
	bad := []FaultRequest{
		{Action: "exec", Target: "sched-0"},
		{Action: ActionKill},
		{Action: ActionFreeze, Target: "sched-0"},
		{Action: ActionFreeze, Target: "sched-0", DurationMS: 600_000},
		{Action: ActionPartition, Target: "sched-0", DurationMS: 10},
		{Action: ActionKill, Target: "sched-0", DurationMS: 1000},
	}
	for _, r := range bad {
		if _, err := Validate(r); err == nil {
			t.Errorf("%+v accepted", r)
		}
	}
}

func TestLoopbackHostsRefusesOtherInterfaces(t *testing.T) {
	if _, err := LoopbackHosts("127.0.0.1:8088"); err != nil {
		t.Error(err)
	}
	if _, err := LoopbackHosts("[::1]:8088"); err != nil {
		t.Error(err)
	}
	for _, a := range []string{"0.0.0.0:8088", "192.168.1.4:8088", ":8088", "example.com:80"} {
		if _, err := LoopbackHosts(a); err == nil {
			t.Errorf("%s accepted as loopback", a)
		}
	}
}
