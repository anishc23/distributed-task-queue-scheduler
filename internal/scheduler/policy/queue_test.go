package policy_test

import (
	"slices"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/scheduler/fifo"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/testutil"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/wfq"
)

func TestKeyOrderingMatchesRedisSortedSetSemantics(t *testing.T) {
	// Redis orders sorted-set members by score and breaks ties lexicographically
	// on the member. Key.Less must do exactly the same or the in-memory tests
	// would not describe the deployed behaviour.
	a := policy.Key{Score: 1, Member: "0000000000100|aaa"}
	b := policy.Key{Score: 1, Member: "0000000000100|bbb"}
	c := policy.Key{Score: 2, Member: "0000000000000|aaa"}

	if !a.Less(b) {
		t.Fatal("equal scores must break ties by member")
	}
	if b.Less(a) {
		t.Fatal("tie-break must be antisymmetric")
	}
	if !a.Less(c) {
		t.Fatal("lower score must win regardless of member")
	}
}

func TestMemberEncodesSubmissionThenID(t *testing.T) {
	m := policy.MemberFor(testutil.Task("task-7", 1234))
	id, err := policy.TaskIDFromMember(m)
	if err != nil {
		t.Fatalf("TaskIDFromMember: %v", err)
	}
	if id != "task-7" {
		t.Fatalf("task id = %q, want %q", id, "task-7")
	}
	// Zero padding means lexicographic order equals numeric order.
	early := policy.MemberFor(testutil.Task("z", 9))
	late := policy.MemberFor(testutil.Task("a", 1000))
	if !(early < late) {
		t.Fatalf("member %q should sort before %q", early, late)
	}
}

func TestTaskIDFromMemberRejectsMalformed(t *testing.T) {
	if _, err := policy.TaskIDFromMember("no-separator"); err == nil {
		t.Fatal("expected an error for a member without a separator")
	}
}

func TestQueueReportsPendingCount(t *testing.T) {
	q := policy.NewQueue(fifo.New())
	if q.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", q.Pending())
	}
	q.Admit(testutil.Task("a", 1))
	q.Admit(testutil.Task("b", 2))
	if q.Pending() != 2 {
		t.Fatalf("pending = %d, want 2", q.Pending())
	}
	if _, ok := q.Next(); !ok {
		t.Fatal("Next should return a task")
	}
	if q.Pending() != 1 {
		t.Fatalf("pending = %d, want 1", q.Pending())
	}
	q.Drain()
	if _, ok := q.Next(); ok {
		t.Fatal("Next on an empty queue should report false")
	}
}

// FIFO starves a small tenant behind a flood while WFQ does not. This is the
// core experimental contrast, asserted directly on the scheduling policies.
func TestFIFOStarvesSmallTenantWhereWFQDoesNot(t *testing.T) {
	build := func(p policy.Policy) []string {
		q := policy.NewQueue(p)
		for i := 0; i < 200; i++ {
			q.Admit(testutil.Task(idOf("a", i), int64(i), testutil.Tenant("A"), testutil.Exec(50)))
		}
		for i := 0; i < 5; i++ {
			// The small tenant arrives after the flood.
			q.Admit(testutil.Task(idOf("b", i), int64(200+i), testutil.Tenant("B"), testutil.Exec(50)))
		}
		return testutil.Tenants(q.Drain())
	}

	fifoOrder := build(fifo.New())
	if slices.Contains(fifoOrder[:50], "B") {
		t.Fatal("FIFO unexpectedly served the late tenant early")
	}

	w, err := wfq.New(wfq.Options{DefaultWeight: 1})
	if err != nil {
		t.Fatalf("wfq.New: %v", err)
	}
	wfqOrder := build(w)
	served := 0
	for _, tenant := range wfqOrder[:50] {
		if tenant == "B" {
			served++
		}
	}
	if served != 5 {
		t.Fatalf("WFQ served %d of 5 small-tenant tasks in the first 50 dispatches, want 5", served)
	}
}

func idOf(prefix string, i int) string {
	const digits = "0123456789"
	return prefix + string([]byte{digits[(i/100)%10], digits[(i/10)%10], digits[i%10]})
}
