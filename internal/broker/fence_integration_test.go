//go:build integration

package broker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/lease"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/fifo"
)

// The fence exists for one specific failure: a scheduler that was paused past
// its lease TTL, was replaced, and then resumed still believing it leads. That
// process cannot be stopped by cancelling a context, because it was not running
// to see the cancellation. These tests therefore do not simulate a pause at
// all; they simply call the guarded operations with an old epoch, which is
// exactly what such a process does when it wakes up.

// A superseded leader must not be able to dispatch, and must not consume
// pending tasks in the attempt.
func TestDispatchIsRefusedAfterANewerEpochDispatches(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	for _, id := range []string{"t-1", "t-2", "t-3", "t-4"} {
		tk := task(id, 0)
		if _, err := br.Admit(ctx, tk, pol.Rank(tk), lease.NoEpoch); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}

	// Epoch 7 is the current leader and dispatches one task.
	got, err := br.Dispatch(ctx, 1, 0, time.Now(), 7)
	if err != nil {
		t.Fatalf("dispatch at epoch 7: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("dispatched %d tasks at epoch 7, want 1", len(got))
	}

	pendingBefore, err := br.PendingCount(ctx)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}

	// Epoch 5 is the deposed leader waking up.
	stale, err := br.Dispatch(ctx, 10, 0, time.Now(), 5)
	if !errors.Is(err, broker.ErrFenced) {
		t.Fatalf("dispatch at stale epoch 5 = %v (%d tasks), want ErrFenced; a deposed leader could still hand work to workers", err, len(stale))
	}
	if len(stale) != 0 {
		t.Fatalf("stale dispatch returned %d tasks; the refusal must yield nothing", len(stale))
	}

	// The refusal has to be total. A script that popped from the pending set
	// before checking would lose tasks that no leader ever dispatched.
	pendingAfter, err := br.PendingCount(ctx)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if pendingAfter != pendingBefore {
		t.Errorf("pending went from %d to %d across a fenced dispatch; the refused call had side effects", pendingBefore, pendingAfter)
	}

	// The real leader is unaffected and can still drain the queue.
	rest, err := br.Dispatch(ctx, 10, 0, time.Now(), 7)
	if err != nil {
		t.Fatalf("dispatch at epoch 7 after the fenced attempt: %v", err)
	}
	if len(rest) != 3 {
		t.Errorf("leader dispatched %d tasks, want the remaining 3", len(rest))
	}
}

// The same leader keeps working across many calls: the fence orders terms, it
// does not require each write to be newer than the last.
func TestTheCurrentEpochIsNotFencedOut(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	for i, id := range []string{"t-1", "t-2", "t-3"} {
		tk := task(id, int64(i))
		if _, err := br.Admit(ctx, tk, pol.Rank(tk), lease.NoEpoch); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	for i := 0; i < 3; i++ {
		got, err := br.Dispatch(ctx, 1, 0, time.Now(), 4)
		if err != nil {
			t.Fatalf("dispatch %d at epoch 4: %v", i, err)
		}
		if len(got) != 1 {
			t.Fatalf("dispatch %d returned %d tasks, want 1", i, len(got))
		}
	}
}

// Election off means no fencing at all, which is what keeps the benchmark
// harness and every pre-existing caller on exactly the path they had before.
func TestNoEpochIsNeverFenced(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	tk := task("t-1", 0)
	if _, err := br.Admit(ctx, tk, pol.Rank(tk), lease.NoEpoch); err != nil {
		t.Fatalf("admit: %v", err)
	}
	// Push the fence high, then dispatch with election disabled.
	if err := br.SaveSchedulerState(ctx, map[string]string{"vt": "1"}, 99); err != nil {
		t.Fatalf("save state at epoch 99: %v", err)
	}
	got, err := br.Dispatch(ctx, 1, 0, time.Now(), lease.NoEpoch)
	if err != nil {
		t.Fatalf("dispatch with election disabled: %v; the guard must be inert at NoEpoch", err)
	}
	if len(got) != 1 {
		t.Fatalf("dispatched %d tasks, want 1", len(got))
	}
}

// Policy state is the quietest thing a split brain can corrupt: the write
// always succeeds, and only the fairness numbers are wrong afterwards.
func TestStaleEpochCannotOverwritePolicyState(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)

	// The deposed leader's last known state, written while it still led.
	if err := br.SaveSchedulerState(ctx, map[string]string{"virtual_time": "100"}, 3); err != nil {
		t.Fatalf("save state at epoch 3: %v", err)
	}
	// Its successor takes over and advances the clock.
	if err := br.SaveSchedulerState(ctx, map[string]string{"virtual_time": "500"}, 4); err != nil {
		t.Fatalf("save state at epoch 4: %v", err)
	}
	// The deposed leader wakes up and flushes what it remembers.
	err := br.SaveSchedulerState(ctx, map[string]string{"virtual_time": "150"}, 3)
	if !errors.Is(err, broker.ErrFenced) {
		t.Fatalf("stale state flush = %v, want ErrFenced", err)
	}

	state, err := br.LoadSchedulerState(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if got := state["virtual_time"]; got != "500" {
		t.Errorf("virtual_time = %q, want 500: a deposed leader rolled the virtual clock backwards", got)
	}
}

// Policy state is one consistent snapshot, so a flush replaces it rather than
// merging into it. Merging would leave a tenant's finish tag from an older term
// sitting beside a newer term's virtual time, a combination no scheduler ever
// held.
func TestStateFlushReplacesRatherThanMerges(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)

	if err := br.SaveSchedulerState(ctx, map[string]string{
		"virtual_time": "100",
		"finish:A":     "100",
		"finish:B":     "120",
	}, 1); err != nil {
		t.Fatalf("save state: %v", err)
	}
	// Tenant B has gone idle and dropped out of the policy's state entirely.
	if err := br.SaveSchedulerState(ctx, map[string]string{
		"virtual_time": "200",
		"finish:A":     "200",
	}, 2); err != nil {
		t.Fatalf("save state: %v", err)
	}

	state, err := br.LoadSchedulerState(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if _, ok := state["finish:B"]; ok {
		t.Errorf("finish:B survived a flush that omitted it; state = %v", state)
	}
	if got := state["virtual_time"]; got != "200" {
		t.Errorf("virtual_time = %q, want 200", got)
	}
}

// Admission is fenced too, and the reason is less obvious than for dispatch.
// The score written at admission is not a property of the task; it comes from
// the policy's virtual clock, which lives in the scheduler's memory. A
// superseded leader that could still admit would insert tasks at positions
// derived from a clock it no longer owns, and the pending set would be wrong in
// a way nothing reports.
func TestAdmissionIsRefusedAfterANewerEpochActs(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	current := task("t-current", 0)
	if _, err := br.Admit(ctx, current, pol.Rank(current), 9); err != nil {
		t.Fatalf("admit at epoch 9: %v", err)
	}

	stale := task("t-stale", 1)
	admitted, err := br.Admit(ctx, stale, pol.Rank(stale), 4)
	if !errors.Is(err, broker.ErrFenced) {
		t.Fatalf("admit at stale epoch 4 = %v (admitted=%v), want ErrFenced", err, admitted)
	}

	// Nothing from the refused call may reach the pending set.
	n, err := br.PendingCount(ctx)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if n != 1 {
		t.Errorf("pending = %d, want 1: the fenced admission had side effects", n)
	}
	known, err := br.Known(ctx, []string{"t-stale"})
	if err != nil {
		t.Fatalf("known: %v", err)
	}
	if known["t-stale"] {
		t.Error("the fenced task was recorded as known, so no later leader would ever admit it")
	}
}

// Every dispatched entry records the term that dispatched it. The queue does
// not need this field to run; the analysis does. It is what turns "no
// superseded leader wrote after its successor" from a claim about the code into
// a property of the artefact, checkable by replaying the stream and confirming
// the epochs never go backwards.
func TestDispatchStampsTheEpochOnEveryEntry(t *testing.T) {
	br, rdb := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	for _, id := range []string{"e-1", "e-2"} {
		tk := task(id, 0)
		if _, err := br.Admit(ctx, tk, pol.Rank(tk), lease.NoEpoch); err != nil {
			t.Fatalf("admit %s: %v", id, err)
		}
	}
	if _, err := br.Dispatch(ctx, 2, 0, time.Now(), 11); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	msgs, err := rdb.XRange(ctx, br.Keys().Exec, "-", "+").Result()
	if err != nil {
		t.Fatalf("read the execution stream: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("execution stream holds %d entries, want 2", len(msgs))
	}
	for _, m := range msgs {
		got, ok := m.Values["epoch"].(string)
		if !ok {
			t.Fatalf("entry %s carries no epoch field; the dispatch log cannot be audited without it", m.ID)
		}
		if got != "11" {
			t.Errorf("entry %s was stamped epoch %s, want 11", m.ID, got)
		}
	}
}

// With election off there is no term to record, and the stamp must not invent
// one. A zero here is what tells a reader that the entry is outside the fence's
// scope rather than dispatched by term zero.
func TestDispatchStampsNoEpochWhenElectionIsOff(t *testing.T) {
	br, rdb := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	pol := fifo.New()
	tk := task("e-off", 0)
	if _, err := br.Admit(ctx, tk, pol.Rank(tk), lease.NoEpoch); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if _, err := br.Dispatch(ctx, 1, 0, time.Now(), lease.NoEpoch); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	msgs, err := rdb.XRange(ctx, br.Keys().Exec, "-", "+").Result()
	if err != nil {
		t.Fatalf("read the execution stream: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("execution stream holds %d entries, want 1", len(msgs))
	}
	if got := msgs[0].Values["epoch"]; got != "0" {
		t.Errorf("entry was stamped epoch %v, want 0 when leader election is off", got)
	}
}
