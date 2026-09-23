//go:build integration

// The gray failure: a leader that is slow rather than dead.
//
// Every other failover test in this package kills a leader, and a lease alone
// would have handled all of them — a dead process writes nothing. The case the
// fence exists for is the one where the deposed leader is still running. It was
// stopped long enough for its lease to expire, a successor took over, and then
// it resumed still believing it leads. Cancelling its context cannot stop it,
// because it was not running to observe the cancellation, and asking it to
// check whether it is still the leader is no better: any such check is
// separated from the write that follows it by a window in which the answer can
// change. The check has to happen at the resource, in the same atomic unit as
// the write. That is what these tests exercise.
//
// Reproducing the state without freezing a process: give the leader a lease
// whose renewal interval is far longer than the test, then delete the holder
// key. That is exactly what Redis does on its own when a frozen process stops
// renewing, and it leaves the engine in precisely the state a resumed process
// is in — still looping, still carrying its old epoch, with no way to know it
// has been replaced. Nothing here is stubbed: the engine, the broker, the
// scripts and Redis are all real.
package scheduler_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// A superseded leader that is still running must not be able to dispatch, and
// the proof is required to come from the dispatch log rather than from the
// engine's own opinion of itself: every execution-stream entry carries the
// epoch that wrote it, so an entry from the old term appearing after the new
// term has written would be the corruption, recorded.
func TestAResumedLeaderCannotDispatchAfterBeingSuperseded(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Work arrives in two batches on purpose. The first gives A something to
	// dispatch so that its term is real rather than nominal; the second is
	// submitted only after A has been superseded, so B has work of its own and
	// the test is not reduced to watching an empty queue.
	const (
		firstBatch  = 40
		secondBatch = 360
		tasks       = firstBatch + secondBatch
	)

	// A long TTL with a renewal interval longer than this whole test. The
	// point is that A's own lease machinery will not rescue it: within the
	// window under test A never attempts a renewal, so if anything stops it
	// from dispatching, that thing is the fence and nothing else.
	aEngine, aMetrics := h.engineWithMetrics(t, "sched-a", 60*time.Second, 55*time.Second, time.Second)

	aCtx, stopA := context.WithCancel(ctx)
	defer stopA()
	var wg sync.WaitGroup
	wg.Add(1)
	aDone := make(chan error, 1)
	go func() { defer wg.Done(); aDone <- aEngine.Run(aCtx) }()
	defer wg.Wait()

	waitUntil(t, 20*time.Second, "A to take the lease", func() bool {
		return aEngine.Epoch() > 0
	})
	epochA := aEngine.Epoch()

	h.submit(t, ctx, firstBatch, "first")
	waitUntil(t, 20*time.Second, "A to dispatch something", func() bool {
		return h.execCount(ctx, t) > 0
	})

	// A is now frozen, as far as the rest of the world can tell: it is not
	// renewing, so Redis drops its grant. Deleting the key is that expiry,
	// made to happen on the test's schedule instead of the TTL's.
	if err := h.rdb.Del(ctx, h.keys.Leader).Err(); err != nil {
		t.Fatalf("expire A's lease: %v", err)
	}

	// B takes over and starts writing, which is what moves the fence.
	bEngine, _ := h.engineWithMetrics(t, "sched-b", 60*time.Second, 15*time.Second, 200*time.Millisecond)
	bCtx, stopB := context.WithCancel(ctx)
	defer stopB()
	wg.Add(1)
	go func() { defer wg.Done(); _ = bEngine.Run(bCtx) }()

	waitUntil(t, 30*time.Second, "B to take over", func() bool {
		return bEngine.Epoch() > epochA
	})
	epochB := bEngine.Epoch()

	h.submit(t, ctx, secondBatch, "second")

	// Nothing is asserted until B has actually written. The fence is moved by
	// a write, not by an election: until B dispatches, A is not yet stale and
	// a passing assertion here would be measuring the wrong moment.
	waitUntil(t, 30*time.Second, "B to write under its own epoch", func() bool {
		return maxDispatchEpoch(ctx, t, h.rdb, h.keys.Exec) >= epochB
	})

	// A must now be refused. It is still running its loops with epoch A, so
	// give it several dispatch intervals in which to try.
	waitUntil(t, 30*time.Second, "A to be fenced out", func() bool {
		v, ok := counterValue(t, aMetrics, "tq_scheduler_fenced_operations_total")
		return ok && v > 0
	})

	// And it must stand down rather than spin: its term ends when it learns it
	// has been superseded.
	waitUntil(t, 30*time.Second, "A to stand down", func() bool {
		return aEngine.Epoch() != epochA
	})

	// Let the work finish under B.
	waitUntil(t, 60*time.Second, "every task to be dispatched", func() bool {
		return h.execCount(ctx, t) >= int64(tasks)
	})

	// The invariant itself, read back from what was written.
	entries := dispatchEpochs(ctx, t, h.rdb, h.keys.Exec)
	if inversions := epochInversions(entries); inversions != 0 {
		t.Fatalf("%d entries were dispatched under a superseded epoch; the single-writer invariant was violated", inversions)
	}
	if n := int64(len(entries)); n != int64(tasks) {
		t.Fatalf("dispatched %d entries, want exactly %d: a duplicate dispatch is a task executed twice for no reason a retry explains", n, tasks)
	}

	// A's own epoch must never appear after B's first write. This is the same
	// statement as the inversion count, made specific to the two terms in play,
	// so a failure says which term misbehaved.
	var sawB bool
	for _, e := range entries {
		if e >= epochB {
			sawB = true
			continue
		}
		if sawB && e == epochA {
			t.Fatalf("the superseded leader (epoch %d) dispatched after its successor (epoch %d) had written", epochA, epochB)
		}
	}

	stopA()
	if err := <-aDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("A exited with an unexpected error: %v", err)
	}
}

// dispatchEpochs reads the epoch stamped on each execution-stream entry, in
// stream order, which is the order they were written.
func dispatchEpochs(ctx context.Context, t *testing.T, rdb *redis.Client, stream string) []int64 {
	t.Helper()
	msgs, err := rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read %s: %v", stream, err)
	}
	out := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		s, ok := m.Values["epoch"].(string)
		if !ok {
			continue
		}
		e, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, e)
	}
	return out
}

func maxDispatchEpoch(ctx context.Context, t *testing.T, rdb *redis.Client, stream string) int64 {
	t.Helper()
	var high int64
	for _, e := range dispatchEpochs(ctx, t, rdb, stream) {
		if e > high {
			high = e
		}
	}
	return high
}

// epochInversions counts entries written under a term older than one that had
// already written. The fence's guarantee is that this is always zero.
func epochInversions(epochs []int64) int {
	var high int64
	n := 0
	for _, e := range epochs {
		if e <= 0 {
			continue
		}
		if e < high {
			n++
			continue
		}
		high = e
	}
	return n
}

func counterValue(t *testing.T, m *metrics.Metrics, name string) (float64, bool) {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if c := metric.GetCounter(); c != nil {
				return c.GetValue(), true
			}
		}
	}
	return 0, false
}
