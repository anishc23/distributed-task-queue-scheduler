package wfq_test

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/testutil"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/wfq"
)

func newPolicy(t *testing.T, weights map[string]float64) *wfq.Policy {
	t.Helper()
	p, err := wfq.New(wfq.Options{Weights: weights, DefaultWeight: 1})
	if err != nil {
		t.Fatalf("wfq.New: %v", err)
	}
	return p
}

func TestRejectsNonPositiveWeights(t *testing.T) {
	if _, err := wfq.New(wfq.Options{Weights: map[string]float64{"A": 0}}); err == nil {
		t.Fatal("expected an error for a zero weight")
	}
	if _, err := wfq.New(wfq.Options{DefaultWeight: -1}); err == nil {
		t.Fatal("expected an error for a negative default weight")
	}
}

func TestVirtualFinishOrdering(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 1, "B": 1})
	// Two tenants, equal weights, equal cost: virtual finish times must
	// interleave 1,1,2,2,3,3... producing strict alternation.
	q := policy.NewQueue(p)
	for i := 0; i < 4; i++ {
		q.Admit(testutil.Task(fmt.Sprintf("a%d", i), int64(i), testutil.Tenant("A"), testutil.Exec(100)))
		q.Admit(testutil.Task(fmt.Sprintf("b%d", i), int64(i), testutil.Tenant("B"), testutil.Exec(100)))
	}
	got := testutil.Tenants(q.Drain())
	want := []string{"A", "B", "A", "B", "A", "B", "A", "B"}
	if !slices.Equal(got, want) {
		t.Fatalf("service order = %v, want strict alternation %v", got, want)
	}
}

func TestWeightsControlServiceShare(t *testing.T) {
	// A has three times B's weight, so over a backlogged window A should
	// receive roughly three times the service.
	p := newPolicy(t, map[string]float64{"A": 3, "B": 1})
	q := policy.NewQueue(p)
	const perTenant = 200
	for i := 0; i < perTenant; i++ {
		q.Admit(testutil.Task(fmt.Sprintf("a%03d", i), 0, testutil.Tenant("A"), testutil.Exec(30)))
		q.Admit(testutil.Task(fmt.Sprintf("b%03d", i), 0, testutil.Tenant("B"), testutil.Exec(30)))
	}
	order := testutil.Tenants(q.Drain())

	const window = 120
	counts := map[string]int{}
	for _, tenant := range order[:window] {
		counts[tenant]++
	}
	ratio := float64(counts["A"]) / float64(counts["B"])
	if math.Abs(ratio-3) > 0.15 {
		t.Fatalf("service ratio A:B = %.3f (A=%d, B=%d), want ~3.0", ratio, counts["A"], counts["B"])
	}
}

// A dominant tenant must not be able to shut everyone else out. This is the
// central claim WFQ makes and the reason the multi-tenant experiment exists.
func TestDominantTenantCannotStarveOthers(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 1, "B": 1})
	q := policy.NewQueue(p)
	// A floods with 1000 tasks; B submits only 10, all at the same instant.
	for i := 0; i < 1000; i++ {
		q.Admit(testutil.Task(fmt.Sprintf("a%04d", i), 0, testutil.Tenant("A"), testutil.Exec(50)))
	}
	for i := 0; i < 10; i++ {
		q.Admit(testutil.Task(fmt.Sprintf("b%04d", i), 0, testutil.Tenant("B"), testutil.Exec(50)))
	}
	order := testutil.Tenants(q.Drain())

	servedB := 0
	for _, tenant := range order[:40] {
		if tenant == "B" {
			servedB++
		}
	}
	if servedB != 10 {
		t.Fatalf("B received %d of its 10 tasks within the first 40 dispatches, want all 10", servedB)
	}
}

func TestNewlyActiveTenantCannotBankCredit(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 1, "B": 1})

	// A is served alone for a while, advancing virtual time.
	for i := 0; i < 20; i++ {
		k := p.Rank(testutil.Task(fmt.Sprintf("a%02d", i), 0, testutil.Tenant("A"), testutil.Exec(100)))
		p.Notify(k)
	}
	vt := p.VirtualTime()
	if vt <= 0 {
		t.Fatalf("virtual time did not advance: %v", vt)
	}

	// B now becomes active for the first time. Its first tag must start at the
	// current virtual time, not at zero, so it cannot claim 20 tasks' worth of
	// backlogged credit and monopolise the queue.
	first := p.Rank(testutil.Task("b00", 0, testutil.Tenant("B"), testutil.Exec(100)))
	if first.Score <= vt {
		t.Fatalf("new tenant tag %v should start after virtual time %v", first.Score, vt)
	}
	if first.Score > vt+101 {
		t.Fatalf("new tenant tag %v is too far past virtual time %v", first.Score, vt)
	}
}

func TestZeroCostTasksAreCharged(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 1})
	first := p.Rank(testutil.Task("a", 0, testutil.Tenant("A"), testutil.Exec(0)))
	second := p.Rank(testutil.Task("b", 0, testutil.Tenant("A"), testutil.Exec(0)))
	if !(second.Score > first.Score) {
		t.Fatalf("zero-cost tasks must still advance the tenant clock: %v then %v", first.Score, second.Score)
	}
}

func TestUnknownTenantUsesDefaultWeight(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 4})
	if got := p.Weight("A"); got != 4 {
		t.Fatalf("weight(A) = %v, want 4", got)
	}
	if got := p.Weight("unconfigured"); got != 1 {
		t.Fatalf("weight(unconfigured) = %v, want the default 1", got)
	}
}

func TestStateRoundTripsAcrossRestart(t *testing.T) {
	p := newPolicy(t, map[string]float64{"A": 2, "B": 1})
	for i := 0; i < 7; i++ {
		k := p.Rank(testutil.Task(fmt.Sprintf("a%d", i), 0, testutil.Tenant("A"), testutil.Exec(70)))
		p.Notify(k)
	}
	p.Rank(testutil.Task("b0", 0, testutil.Tenant("B"), testutil.Exec(70)))

	saved := p.State()
	if len(saved) == 0 {
		t.Fatal("wfq must persist state")
	}

	restarted := newPolicy(t, map[string]float64{"A": 2, "B": 1})
	if err := restarted.Restore(saved); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restarted.VirtualTime() != p.VirtualTime() {
		t.Fatalf("virtual time after restore = %v, want %v", restarted.VirtualTime(), p.VirtualTime())
	}
	// The next tag issued by the restarted policy must match what the original
	// would have issued; otherwise a restart silently re-grants service.
	next := testutil.Task("a-next", 0, testutil.Tenant("A"), testutil.Exec(70))
	if got, want := restarted.Rank(next).Score, p.Rank(next).Score; got != want {
		t.Fatalf("tag after restore = %v, want %v", got, want)
	}
}

func TestRestoreRejectsUnknownKeys(t *testing.T) {
	p := newPolicy(t, nil)
	if err := p.Restore(map[string]string{"bogus": "1"}); err == nil {
		t.Fatal("expected an error for an unknown state key")
	}
	if err := p.Restore(map[string]string{"vtime": "not-a-number"}); err == nil {
		t.Fatal("expected an error for a malformed value")
	}
}

func TestRestoreEmptyIsInitialState(t *testing.T) {
	p := newPolicy(t, nil)
	if err := p.Restore(nil); err != nil {
		t.Fatalf("restore nil: %v", err)
	}
	if p.VirtualTime() != 0 {
		t.Fatalf("virtual time = %v, want 0", p.VirtualTime())
	}
	if p.Name() != policy.WFQ {
		t.Fatalf("name = %q, want %q", p.Name(), policy.WFQ)
	}
}
