package fifo_test

import (
	"slices"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/fifo"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/testutil"
)

func TestDispatchesOldestFirst(t *testing.T) {
	q := policy.NewQueue(fifo.New())
	// Admitted out of submission order on purpose.
	for _, task := range []domain.Task{
		testutil.Task("c", 300),
		testutil.Task("a", 100),
		testutil.Task("d", 400),
		testutil.Task("b", 200),
	} {
		q.Admit(task)
	}

	got := testutil.IDs(q.Drain())
	want := []string{"a", "b", "c", "d"}
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
}

func TestIgnoresPriorityAndDeadline(t *testing.T) {
	q := policy.NewQueue(fifo.New())
	q.Admit(testutil.Task("old-low", 100, testutil.Priority(0), testutil.DeadlineAt(99_000)))
	q.Admit(testutil.Task("new-high", 200, testutil.Priority(200), testutil.DeadlineAt(150)))

	got := testutil.IDs(q.Drain())
	want := []string{"old-low", "new-high"}
	if !slices.Equal(got, want) {
		t.Fatalf("FIFO must ignore priority and deadline: got %v, want %v", got, want)
	}
}

func TestTiesBreakByTaskID(t *testing.T) {
	q := policy.NewQueue(fifo.New())
	// Identical submission millisecond: the tie-break member orders by task ID.
	for _, id := range []string{"zz", "mm", "aa"} {
		q.Admit(testutil.Task(id, 500))
	}
	got := testutil.IDs(q.Drain())
	want := []string{"aa", "mm", "zz"}
	if !slices.Equal(got, want) {
		t.Fatalf("tie-break order = %v, want %v", got, want)
	}
}

func TestStateIsEmpty(t *testing.T) {
	p := fifo.New()
	if got := p.State(); len(got) != 0 {
		t.Fatalf("fifo should be stateless, got %v", got)
	}
	if err := p.Restore(nil); err != nil {
		t.Fatalf("restoring empty state: %v", err)
	}
	if p.Name() != policy.FIFO {
		t.Fatalf("name = %q, want %q", p.Name(), policy.FIFO)
	}
}

func TestNotifyDoesNotChangeRanking(t *testing.T) {
	p := fifo.New()
	task := testutil.Task("t", 100)
	before := p.Rank(task)
	p.Notify(before)
	if after := p.Rank(task); after != before {
		t.Fatalf("Notify changed ranking: %v then %v", before, after)
	}
	if err := p.Restore(map[string]string{"anything": "1"}); err != nil {
		t.Fatalf("a stateless policy must ignore restored state, got: %v", err)
	}
}
