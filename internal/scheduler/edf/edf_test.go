package edf_test

import (
	"slices"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/scheduler/edf"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/testutil"
)

func TestEarliestDeadlineFirst(t *testing.T) {
	q := policy.NewQueue(edf.New())
	q.Admit(testutil.Task("far", 100, testutil.DeadlineAt(9000)))
	q.Admit(testutil.Task("near", 200, testutil.DeadlineAt(1000)))
	q.Admit(testutil.Task("mid", 300, testutil.DeadlineAt(5000)))

	got := testutil.IDs(q.Drain())
	want := []string{"near", "mid", "far"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

func TestTieBreaksBySubmissionThenID(t *testing.T) {
	q := policy.NewQueue(edf.New())
	q.Admit(testutil.Task("late-submit", 900, testutil.DeadlineAt(5000)))
	q.Admit(testutil.Task("zulu", 100, testutil.DeadlineAt(5000)))
	q.Admit(testutil.Task("alpha", 100, testutil.DeadlineAt(5000)))

	got := testutil.IDs(q.Drain())
	want := []string{"alpha", "zulu", "late-submit"}
	if !slices.Equal(got, want) {
		t.Fatalf("tie-break order = %v, want %v", got, want)
	}
}

func TestEDFIgnoresPriority(t *testing.T) {
	q := policy.NewQueue(edf.New())
	q.Admit(testutil.Task("urgent-deadline", 100, testutil.Priority(0), testutil.DeadlineAt(500)))
	q.Admit(testutil.Task("high-priority", 100, testutil.Priority(200), testutil.DeadlineAt(5000)))

	got := testutil.IDs(q.Drain())
	want := []string{"urgent-deadline", "high-priority"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

func TestTasksWithoutDeadlineGoLast(t *testing.T) {
	q := policy.NewQueue(edf.New())
	q.Admit(testutil.Task("no-deadline", 0, testutil.NoDeadline()))
	q.Admit(testutil.Task("very-far", 5000, testutil.DeadlineAt(4_000_000_000)))

	got := testutil.IDs(q.Drain())
	want := []string{"very-far", "no-deadline"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

// EDF is stateless. A stateless policy that accidentally returned non-nil state
// would write junk into the persisted scheduler-state hash and fail to restore.
func TestStatelessContract(t *testing.T) {
	p := edf.New()
	if p.Name() != policy.EDF {
		t.Fatalf("name = %q, want %q", p.Name(), policy.EDF)
	}
	if got := p.State(); len(got) != 0 {
		t.Fatalf("edf should be stateless, got state %v", got)
	}
	if err := p.Restore(nil); err != nil {
		t.Fatalf("restoring nil state: %v", err)
	}
	if err := p.Restore(map[string]string{"anything": "1"}); err != nil {
		t.Fatalf("a stateless policy must ignore restored state, got: %v", err)
	}

	// Notify must not change how subsequent tasks are ranked.
	task := testutil.Task("t", 100, testutil.DeadlineAt(5000))
	before := p.Rank(task)
	p.Notify(before)
	if after := p.Rank(task); after != before {
		t.Fatalf("Notify changed ranking: %v then %v", before, after)
	}
}
