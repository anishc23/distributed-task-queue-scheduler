//go:build integration

// Integration tests exercise the full Redis-backed flow against a real Redis 7+
// server. They are excluded from the default build so that `go test ./...`
// needs no external service.
//
// Run them with:
//
//	make test-integration                 # starts nothing, expects Redis on localhost:6379
//	TQ_TEST_REDIS_ADDR=host:port make test-integration
//
// Each test uses its own randomly named key namespace and deletes it afterwards,
// so tests are safe to run against a shared development Redis and can run in
// parallel.
package broker_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/fifo"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/priority"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	if v := os.Getenv("TQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// newBroker returns a broker bound to a namespace unique to this test.
func newBroker(t *testing.T) (*broker.Broker, *redis.Client) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr(), DialTimeout: 3 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatalf("Redis is required for integration tests but is not reachable at %s: %v\n"+
			"Start one with `make redis-up` or set TQ_TEST_REDIS_ADDR.", redisAddr(), err)
	}

	streams := config.Default().Streams
	streams.Namespace = fmt.Sprintf("tqtest:%s:%d", sanitise(t.Name()), time.Now().UnixNano())
	br := broker.NewWithClient(rdb, streams)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = br.Reset(cleanupCtx)
		rdb.Close()
	})
	return br, rdb
}

func sanitise(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			out = append(out, c)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func task(id string, submitOffsetMS int64, opts ...func(*domain.Task)) domain.Task {
	base := time.Now().Add(-time.Second)
	tk := domain.Task{
		ID:          id,
		TenantID:    "A",
		SubmittedAt: base.Add(time.Duration(submitOffsetMS) * time.Millisecond),
		ExecMillis:  10,
		Priority:    1,
		Deadline:    base.Add(time.Minute),
		MaxRetries:  2,
		AttemptID:   id + "#0",
	}
	for _, o := range opts {
		o(&tk)
	}
	return tk
}

func TestWaitReadyReturnsImmediatelyWhenRedisIsUp(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	start := time.Now()
	if err := br.WaitReady(ctx, 30*time.Second, nil); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("WaitReady took %s against a healthy Redis, want near-instant", elapsed)
	}
}

// A process started before its broker must wait rather than exit, otherwise the
// orchestrator restarts it in a crash loop during an ordinary rollout.
func TestWaitReadyGivesUpWithAnActionableError(t *testing.T) {
	streams := config.Default().Streams
	streams.Namespace = "tqtest-unreachable"
	// Port 1 is reserved and never listening.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	defer rdb.Close()
	br := broker.NewWithClient(rdb, streams)

	start := time.Now()
	err := br.WaitReady(context.Background(), 900*time.Millisecond, nil)
	if err == nil {
		t.Fatal("expected an error when Redis is unreachable")
	}
	if elapsed := time.Since(start); elapsed < 700*time.Millisecond {
		t.Fatalf("WaitReady gave up after %s, want it to use the full timeout", elapsed)
	}
	if !strings.Contains(err.Error(), "127.0.0.1:1") || !strings.Contains(err.Error(), "make redis-up") {
		t.Fatalf("error should name the address and the fix, got: %v", err)
	}
}

func TestWaitReadyRespectsContextCancellation(t *testing.T) {
	streams := config.Default().Streams
	streams.Namespace = "tqtest-cancelled"
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	defer rdb.Close()
	br := broker.NewWithClient(rdb, streams)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := br.WaitReady(ctx, time.Minute, nil); err == nil {
		t.Fatal("expected an error when the context is cancelled")
	}
}

func TestEnsureStreamsIsIdempotent(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	for i := 0; i < 3; i++ {
		if err := br.EnsureStreams(ctx); err != nil {
			t.Fatalf("EnsureStreams call %d: %v", i, err)
		}
	}
}

// End-to-end flow: ingress -> scheduler admission -> atomic dispatch -> worker
// consumption -> idempotent completion -> results stream.
func TestFullFlowIngressToResults(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}

	tasks := []domain.Task{task("t-c", 300), task("t-a", 100), task("t-b", 200)}
	if err := br.SubmitBatch(ctx, tasks); err != nil {
		t.Fatalf("submit: %v", err)
	}

	msgs, err := br.ReadIngress(ctx, "sched-1", 10, time.Second)
	if err != nil {
		t.Fatalf("read ingress: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("read %d ingress entries, want 3", len(msgs))
	}

	pol := fifo.New()
	for _, m := range msgs {
		admitted, err := br.Admit(ctx, m.Task, pol.Rank(m.Task))
		if err != nil {
			t.Fatalf("admit: %v", err)
		}
		if !admitted {
			t.Fatalf("task %s should have been admitted", m.Task.ID)
		}
	}
	if err := br.AckIngress(ctx, idsOf(msgs)...); err != nil {
		t.Fatalf("ack ingress: %v", err)
	}

	if n, err := br.PendingCount(ctx); err != nil || n != 3 {
		t.Fatalf("pending = %d (err %v), want 3", n, err)
	}

	dispatched, err := br.Dispatch(ctx, 10, 0, time.Now())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(dispatched) != 3 {
		t.Fatalf("dispatched %d tasks, want 3", len(dispatched))
	}
	// FIFO: oldest submission first.
	wantOrder := []string{"t-a", "t-b", "t-c"}
	for i, d := range dispatched {
		if d.Task.ID != wantOrder[i] {
			t.Fatalf("dispatch order = %v, want %v", idsOfDispatched(dispatched), wantOrder)
		}
	}
	if n, err := br.PendingCount(ctx); err != nil || n != 0 {
		t.Fatalf("pending after dispatch = %d (err %v), want 0", n, err)
	}
	if n, err := br.InFlight(ctx); err != nil || n != 3 {
		t.Fatalf("in flight = %d (err %v), want 3", n, err)
	}

	execMsgs, err := br.ReadExec(ctx, "worker-1", 10, time.Second)
	if err != nil {
		t.Fatalf("read exec: %v", err)
	}
	if len(execMsgs) != 3 {
		t.Fatalf("worker read %d entries, want 3", len(execMsgs))
	}
	if execMsgs[0].DispatchedAt.IsZero() {
		t.Fatal("dispatch timestamp was not propagated to the worker")
	}

	for _, m := range execMsgs {
		recorded, err := br.Complete(ctx, resultFor(m.Task), m.ID)
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
		if !recorded {
			t.Fatalf("first completion of %s should be recorded", m.Task.ID)
		}
	}

	results, _, err := br.ReadResults(ctx, "0", 100)
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	if n, err := br.InFlight(ctx); err != nil || n != 0 {
		t.Fatalf("in flight after completion = %d (err %v), want 0", n, err)
	}
	if n, err := br.ExecPending(ctx); err != nil || n != 0 {
		t.Fatalf("unacked deliveries = %d (err %v), want 0", n, err)
	}

	stats, err := br.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats[broker.StatAdmitted] != 3 || stats[broker.StatDispatched] != 3 || stats[broker.StatCompleted] != 3 {
		t.Fatalf("counters = %v", stats)
	}
}

// The priority policy's ordering must survive the trip through a Redis sorted
// set, including the deterministic tie-break.
func TestRedisDispatchMatchesPolicyOrdering(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}

	pol := priority.New()
	inMemory := policy.NewQueue(priority.New())

	base := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	tasks := []domain.Task{
		mk("z-low", base, 0), mk("a-low", base, 0), mk("high", base.Add(time.Second), 9),
		mk("mid", base.Add(2*time.Second), 5), mk("a-low2", base.Add(time.Millisecond), 0),
	}
	for _, tk := range tasks {
		inMemory.Admit(tk)
		if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
			t.Fatalf("admit: %v", err)
		}
	}

	dispatched, err := br.Dispatch(ctx, 100, 0, time.Now())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	want := idsOfTasks(inMemory.Drain())
	got := idsOfDispatched(dispatched)
	if len(got) != len(want) {
		t.Fatalf("dispatched %d tasks, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Redis dispatch order %v does not match the in-memory policy order %v", got, want)
		}
	}
}

func TestDispatchRespectsMaxInFlight(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	pol := fifo.New()
	for i := 0; i < 10; i++ {
		tk := task(fmt.Sprintf("t-%02d", i), int64(i))
		if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
			t.Fatal(err)
		}
	}
	dispatched, err := br.Dispatch(ctx, 10, 3, time.Now())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(dispatched) != 3 {
		t.Fatalf("dispatched %d with max_in_flight=3, want 3", len(dispatched))
	}
	again, err := br.Dispatch(ctx, 10, 3, time.Now())
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("dispatched %d more while at the in-flight cap, want 0", len(again))
	}
	if n, _ := br.PendingCount(ctx); n != 7 {
		t.Fatalf("pending = %d, want 7", n)
	}
}

// Idempotent completion: a duplicate delivery must be acknowledged but must not
// produce a second result record or double-count tenant service.
func TestCompletionIsIdempotent(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	tk := task("dupe", 0)
	pol := fifo.New()
	if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
		t.Fatal(err)
	}
	dispatched, err := br.Dispatch(ctx, 1, 0, time.Now())
	if err != nil || len(dispatched) != 1 {
		t.Fatalf("dispatch: %v (%d)", err, len(dispatched))
	}
	msgs, err := br.ReadExec(ctx, "w1", 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read exec: %v (%d)", err, len(msgs))
	}

	recorded, err := br.Complete(ctx, resultFor(tk), msgs[0].ID)
	if err != nil || !recorded {
		t.Fatalf("first completion: recorded=%v err=%v", recorded, err)
	}
	// Same task delivered again (as at-least-once delivery permits).
	recorded, err = br.Complete(ctx, resultFor(tk), "")
	if err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	if recorded {
		t.Fatal("duplicate completion must not be recorded a second time")
	}

	results, _, err := br.ReadResults(ctx, "0", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want exactly 1 despite duplicate delivery", len(results))
	}
	service, counts, err := br.TenantService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts["A"] != 1 {
		t.Fatalf("tenant completed count = %d, want 1", counts["A"])
	}
	if service["A"] != 10 {
		t.Fatalf("tenant service = %dms, want 10ms (not double counted)", service["A"])
	}
	stats, _ := br.Stats(ctx)
	if stats[broker.StatDuplicateCompletions] != 1 {
		t.Fatalf("duplicate counter = %d, want 1", stats[broker.StatDuplicateCompletions])
	}
}

// Admission is idempotent too: a redelivered ingress entry must not create a
// second pending copy of the same task.
func TestAdmissionIsIdempotent(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	tk := task("once", 0)
	pol := fifo.New()

	first, err := br.Admit(ctx, tk, pol.Rank(tk))
	if err != nil || !first {
		t.Fatalf("first admit: %v %v", first, err)
	}
	second, err := br.Admit(ctx, tk, pol.Rank(tk))
	if err != nil {
		t.Fatal(err)
	}
	if second {
		t.Fatal("re-admitting a pending task must be rejected")
	}
	if n, _ := br.PendingCount(ctx); n != 1 {
		t.Fatalf("pending = %d, want 1", n)
	}

	known, err := br.Known(ctx, []string{"once", "never"})
	if err != nil {
		t.Fatal(err)
	}
	if !known["once"] || known["never"] {
		t.Fatalf("Known = %v", known)
	}
}

// Retry path: an unacknowledged delivery is reclaimed and retried until the
// budget is exhausted, then dead-lettered with failure metadata.
func TestRetryThenDeadLetter(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	const budget = 2
	tk := task("flaky", 0, func(t *domain.Task) { t.MaxRetries = budget })
	pol := fifo.New()
	if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
		t.Fatal(err)
	}
	if _, err := br.Dispatch(ctx, 1, 0, time.Now()); err != nil {
		t.Fatal(err)
	}

	current := tk
	for attempt := 1; attempt <= budget; attempt++ {
		msgs, err := br.ReadExec(ctx, "w1", 1, time.Second)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("attempt %d read: %v (%d)", attempt, err, len(msgs))
		}
		outcome, next, err := br.RetryOrDeadLetter(ctx, msgs[0].Task, budget, "worker failed", msgs[0].ID)
		if err != nil {
			t.Fatal(err)
		}
		if outcome != broker.RetryRedelivered {
			t.Fatalf("attempt %d outcome = %v, want redelivered", attempt, outcome)
		}
		if next.RetryCount != attempt {
			t.Fatalf("retry count = %d, want %d", next.RetryCount, attempt)
		}
		if next.AttemptID == current.AttemptID {
			t.Fatal("attempt id must change on retry")
		}
		current = next
	}

	// The budget is now exhausted; the next failure must dead-letter.
	msgs, err := br.ReadExec(ctx, "w1", 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("final read: %v (%d)", err, len(msgs))
	}
	outcome, _, err := br.RetryOrDeadLetter(ctx, msgs[0].Task, budget, "worker failed", msgs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != broker.RetryDeadLettered {
		t.Fatalf("outcome = %v, want dead-lettered", outcome)
	}

	entries, err := br.ReadDeadLetters(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("dead-letter entries = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.Task.ID != "flaky" {
		t.Fatalf("dead-letter task id = %s", e.Task.ID)
	}
	if e.Attempts != budget+1 {
		t.Fatalf("dead-letter attempts = %d, want %d", e.Attempts, budget+1)
	}
	if e.Reason == "" || e.FailedAt.IsZero() {
		t.Fatalf("dead-letter entry is missing failure metadata: %+v", e)
	}
	if n, err := br.InFlight(ctx); err != nil || n != 0 {
		t.Fatalf("in flight after dead-letter = %d (err %v), want 0", n, err)
	}
	if n, _ := br.ExecPending(ctx); n != 0 {
		t.Fatalf("unacked deliveries = %d, want 0: dead-lettering must acknowledge", n)
	}

	// A completed task must never be retried afterwards.
	done := task("finished", 0)
	if _, err := br.Admit(ctx, done, pol.Rank(done)); err != nil {
		t.Fatal(err)
	}
	if _, err := br.Dispatch(ctx, 1, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	dm, _ := br.ReadExec(ctx, "w1", 1, time.Second)
	if _, err := br.Complete(ctx, resultFor(done), dm[0].ID); err != nil {
		t.Fatal(err)
	}
	outcome, _, err = br.RetryOrDeadLetter(ctx, done, budget, "late reclaim", "")
	if err != nil {
		t.Fatal(err)
	}
	if outcome != broker.RetryAlreadyTerminal {
		t.Fatalf("outcome = %v, want already-terminal", outcome)
	}
}

// XAUTOCLAIM reclaims deliveries whose idle time exceeds the visibility timeout.
func TestAutoClaimReclaimsIdleDeliveries(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	tk := task("abandoned", 0)
	pol := fifo.New()
	if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
		t.Fatal(err)
	}
	if _, err := br.Dispatch(ctx, 1, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Deliver to a worker that then "crashes": never acknowledges.
	if _, err := br.ReadExec(ctx, "doomed-worker", 1, time.Second); err != nil {
		t.Fatal(err)
	}

	// Nothing is idle enough yet.
	claimed, _, err := br.AutoClaim(ctx, "recovery", time.Hour, "0-0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d entries before the visibility timeout, want 0", len(claimed))
	}

	time.Sleep(150 * time.Millisecond)
	claimed, _, err = br.AutoClaim(ctx, "recovery", 50*time.Millisecond, "0-0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d entries after the visibility timeout, want 1", len(claimed))
	}
	if claimed[0].Task.ID != "abandoned" {
		t.Fatalf("claimed the wrong task: %s", claimed[0].Task.ID)
	}
}

// Scheduler policy state must round-trip through Redis so a restart resumes.
func TestSchedulerStatePersistence(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)

	state := map[string]string{"vtime": "1234.5", "lf:A": "2000", "lf:B": "1500"}
	if err := br.SaveSchedulerState(ctx, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := br.LoadSchedulerState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range state {
		if loaded[k] != v {
			t.Fatalf("state[%s] = %q, want %q", k, loaded[k], v)
		}
	}
}

// A crash between selection and dispatch must not lose the task: the dispatch
// script performs ZPOPMIN and XADD atomically, so either both happened or
// neither did. This test asserts the invariant that every popped task appears on
// the exec stream.
func TestDispatchIsAtomic(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	pol := fifo.New()
	const n = 50
	for i := 0; i < n; i++ {
		tk := task(fmt.Sprintf("atomic-%02d", i), int64(i))
		if _, err := br.Admit(ctx, tk, pol.Rank(tk)); err != nil {
			t.Fatal(err)
		}
	}
	var dispatchedIDs []string
	for {
		batch, err := br.Dispatch(ctx, 7, 0, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		dispatchedIDs = append(dispatchedIDs, idsOfDispatched(batch)...)
	}
	if len(dispatchedIDs) != n {
		t.Fatalf("dispatched %d tasks, want %d", len(dispatchedIDs), n)
	}
	execMsgs, err := br.ReadExec(ctx, "w1", n, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(execMsgs) != n {
		t.Fatalf("exec stream holds %d entries, want %d: a task was lost between selection and dispatch", len(execMsgs), n)
	}
	seen := map[string]bool{}
	for _, m := range execMsgs {
		seen[m.Task.ID] = true
	}
	for _, id := range dispatchedIDs {
		if !seen[id] {
			t.Fatalf("task %s was selected but never reached the exec stream", id)
		}
	}
}

func TestResetClearsNamespace(t *testing.T) {
	br, _ := newBroker(t)
	ctx := testContext(t)
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	tk := task("gone", 0)
	if _, err := br.Admit(ctx, tk, fifo.New().Rank(tk)); err != nil {
		t.Fatal(err)
	}
	if err := br.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := br.PendingCount(ctx); n != 0 {
		t.Fatalf("pending after reset = %d, want 0", n)
	}
	stats, _ := br.Stats(ctx)
	if len(stats) != 0 {
		t.Fatalf("stats after reset = %v, want empty", stats)
	}
}

func mk(id string, submitted time.Time, prio int) domain.Task {
	return domain.Task{
		ID:          id,
		TenantID:    "A",
		SubmittedAt: submitted,
		ExecMillis:  10,
		Priority:    prio,
		Deadline:    submitted.Add(time.Minute),
		MaxRetries:  2,
		AttemptID:   id + "#0",
	}
}

func resultFor(t domain.Task) domain.Result {
	now := time.Now()
	return domain.Result{
		TaskID:           t.ID,
		AttemptID:        t.AttemptID,
		TenantID:         t.TenantID,
		Outcome:          domain.OutcomeCompleted,
		Priority:         t.Priority,
		SubmittedAt:      t.SubmittedAt,
		StartedAt:        now.Add(-time.Duration(t.ExecMillis) * time.Millisecond),
		FinishedAt:       now,
		Deadline:         t.Deadline,
		ExecMillis:       t.ExecMillis,
		ActualExecMillis: t.ExecMillis,
		RetryCount:       t.RetryCount,
		Worker:           "test-worker",
	}
}

func idsOf(msgs []broker.IngressMessage) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

func idsOfDispatched(ds []broker.Dispatched) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Task.ID
	}
	return out
}

func idsOfTasks(ts []domain.Task) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.ID
	}
	return out
}
