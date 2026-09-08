package priority_test

import (
	"slices"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/priority"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/testutil"
)

func TestHigherPriorityFirst(t *testing.T) {
	q := policy.NewQueue(priority.New())
	q.Admit(testutil.Task("low", 100, testutil.Priority(0)))
	q.Admit(testutil.Task("high", 200, testutil.Priority(9)))
	q.Admit(testutil.Task("mid", 300, testutil.Priority(5)))

	got := testutil.IDs(q.Drain())
	want := []string{"high", "mid", "low"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

func TestTieBreaksBySubmissionThenID(t *testing.T) {
	q := policy.NewQueue(priority.New())
	// All the same priority. Two share a submission millisecond.
	for _, task := range []domain.Task{
		testutil.Task("later", 300, testutil.Priority(4)),
		testutil.Task("zeta", 100, testutil.Priority(4)),
		testutil.Task("alpha", 100, testutil.Priority(4)),
	} {
		q.Admit(task)
	}
	got := testutil.IDs(q.Drain())
	want := []string{"alpha", "zeta", "later"}
	if !slices.Equal(got, want) {
		t.Fatalf("tie-break order = %v, want %v", got, want)
	}
}

func TestOrderingIsDeterministicAcrossAdmissionOrder(t *testing.T) {
	build := func() []domain.Task {
		return []domain.Task{
			testutil.Task("t1", 100, testutil.Priority(3)),
			testutil.Task("t2", 100, testutil.Priority(3)),
			testutil.Task("t3", 100, testutil.Priority(7)),
			testutil.Task("t4", 50, testutil.Priority(3)),
		}
	}
	forward := policy.NewQueue(priority.New())
	for _, task := range build() {
		forward.Admit(task)
	}
	reverse := policy.NewQueue(priority.New())
	tasks := build()
	for i := len(tasks) - 1; i >= 0; i-- {
		reverse.Admit(tasks[i])
	}
	a, b := testutil.IDs(forward.Drain()), testutil.IDs(reverse.Drain())
	if !slices.Equal(a, b) {
		t.Fatalf("dispatch order depends on admission order: %v vs %v", a, b)
	}
	want := []string{"t3", "t4", "t1", "t2"}
	if !slices.Equal(a, want) {
		t.Fatalf("dispatch order = %v, want %v", a, want)
	}
}

// A task submitted far in the future at a high priority still outranks an
// ancient low-priority task: strict priority does not age. This is the
// starvation behaviour the experiments are meant to measure.
func TestNoAgeing(t *testing.T) {
	q := policy.NewQueue(priority.New())
	q.Admit(testutil.Task("ancient", 0, testutil.Priority(1)))
	q.Admit(testutil.Task("fresh", 10_000_000, testutil.Priority(2)))

	got := testutil.IDs(q.Drain())
	want := []string{"fresh", "ancient"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

func TestScoreBandsNeverOverlap(t *testing.T) {
	p := priority.New()
	// The highest possible submission time inside a band must still rank ahead
	// of the lowest submission time in the next band down.
	high := p.Rank(testutil.Task("high", 4_102_444_800_000, testutil.Priority(1)))
	low := p.Rank(testutil.Task("low", 0, testutil.Priority(0)))
	if !high.Less(low) {
		t.Fatalf("priority band leaked: high score %v not less than low score %v", high.Score, low.Score)
	}
}

func TestClampsOutOfRangePriority(t *testing.T) {
	p := priority.New()
	above := p.Rank(testutil.Task("above", 0, testutil.Priority(domain.MaxPriority+50)))
	atMax := p.Rank(testutil.Task("at-max", 0, testutil.Priority(domain.MaxPriority)))
	if above.Score != atMax.Score {
		t.Fatalf("priority above the maximum should clamp: %v vs %v", above.Score, atMax.Score)
	}
}

// Strict priority is stateless. A stateless policy that accidentally returned
// non-nil state would write junk into the persisted scheduler-state hash.
func TestStatelessContract(t *testing.T) {
	p := priority.New()
	if p.Name() != policy.Priority {
		t.Fatalf("name = %q, want %q", p.Name(), policy.Priority)
	}
	if got := p.State(); len(got) != 0 {
		t.Fatalf("priority should be stateless, got state %v", got)
	}
	if err := p.Restore(nil); err != nil {
		t.Fatalf("restoring nil state: %v", err)
	}
	if err := p.Restore(map[string]string{"anything": "1"}); err != nil {
		t.Fatalf("a stateless policy must ignore restored state, got: %v", err)
	}

	task := testutil.Task("t", 100, testutil.Priority(3))
	before := p.Rank(task)
	p.Notify(before)
	if after := p.Rank(task); after != before {
		t.Fatalf("Notify changed ranking: %v then %v", before, after)
	}
}

func TestClampsNegativePriority(t *testing.T) {
	p := priority.New()
	below := p.Rank(testutil.Task("below", 0, testutil.Priority(-5)))
	atZero := p.Rank(testutil.Task("at-zero", 0, testutil.Priority(0)))
	if below.Score != atZero.Score {
		t.Fatalf("priority below zero should clamp: %v vs %v", below.Score, atZero.Score)
	}
}
