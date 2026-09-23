//go:build integration

// Failover tests for the scheduling engine. They check the two properties that
// make running more than one scheduler safe rather than merely possible: only
// one engine dispatches at a time, and the survivor resumes from the departed
// leader's policy state rather than from zero.
package scheduler_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/lease"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/scheduler"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	if v := os.Getenv("TQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type harness struct {
	br     *broker.Broker
	rdb    *redis.Client
	keys   broker.Keys
	cfg    config.Scheduler
	leaseK lease.Keys
}

func newHarness(t *testing.T) *harness {
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
	streams.Namespace = fmt.Sprintf("tqtest:failover:%d", time.Now().UnixNano())
	br := broker.NewWithClient(rdb, streams)

	cfg := config.Default().Scheduler
	cfg.Policy = "wfq"
	cfg.DispatchBatch = 8
	cfg.MaxInFlight = 0
	cfg.IngestBlock = config.Duration(50 * time.Millisecond)
	cfg.IdleSleep = config.Duration(2 * time.Millisecond)
	cfg.StateFlushInterval = config.Duration(100 * time.Millisecond)

	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = br.Reset(c)
		rdb.Del(c, br.Keys().LeaderEpoch)
		rdb.Close()
	})

	return &harness{
		br:   br,
		rdb:  rdb,
		keys: br.Keys(),
		cfg:  cfg,
		leaseK: lease.Keys{
			Holder: br.Keys().Leader,
			Epoch:  br.Keys().LeaderEpoch,
		},
	}
}

func (h *harness) engine(t *testing.T, name string, ttl time.Duration) *scheduler.Engine {
	t.Helper()
	eng, _ := h.engineWithMetrics(t, name, ttl, ttl/4, ttl/8)
	return eng
}

// engineWithMetrics builds an engine with the lease timings spelled out and
// hands back its metrics, so a test can read what the engine recorded about
// itself rather than only what it did to Redis.
func (h *harness) engineWithMetrics(t *testing.T, name string, ttl, renew, retry time.Duration) (*scheduler.Engine, *metrics.Metrics) {
	t.Helper()
	pol, err := scheduler.New(scheduler.Options{
		Policy:              h.cfg.Policy,
		TenantWeights:       h.cfg.TenantWeights,
		DefaultTenantWeight: h.cfg.DefaultTenantWeight,
	})
	if err != nil {
		t.Fatalf("build policy: %v", err)
	}
	l, err := lease.New(lease.Options{
		Redis: h.rdb, Keys: h.leaseK, Owner: name,
		TTL: ttl, Renew: renew, Retry: retry,
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatalf("build lease: %v", err)
	}
	m := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: h.cfg.Policy})
	eng, err := scheduler.NewEngine(scheduler.EngineOptions{
		Broker:   h.br,
		Policy:   pol,
		Config:   h.cfg,
		Logger:   discardLogger(),
		Metrics:  m,
		Consumer: name,
		Lease:    l,
	})
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	return eng, m
}

func (h *harness) submit(t *testing.T, ctx context.Context, n int, tenant string) {
	t.Helper()
	tasks := make([]domain.Task, 0, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		tasks = append(tasks, domain.Task{
			ID:          fmt.Sprintf("%s-%03d", tenant, i),
			TenantID:    tenant,
			SubmittedAt: now.Add(time.Duration(i) * time.Millisecond),
			ExecMillis:  5,
			Priority:    1,
			Deadline:    now.Add(time.Minute),
			MaxRetries:  1,
			AttemptID:   fmt.Sprintf("%s-%03d#0", tenant, i),
		})
	}
	if err := h.br.SubmitBatch(ctx, tasks); err != nil {
		t.Fatalf("submit: %v", err)
	}
}

// execCount counts entries on the worker execution stream, which is what the
// scheduler actually produced.
func (h *harness) execCount(ctx context.Context, t *testing.T) int64 {
	t.Helper()
	n, err := h.rdb.XLen(ctx, h.keys.Exec).Result()
	if err != nil {
		t.Fatalf("XLEN exec: %v", err)
	}
	return n
}

func waitUntil(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", limit, what)
}

// Two engines against one queue: exactly one leads, the other stands by, and
// when the leader goes away the standby finishes the work. Every task must be
// dispatched, and none of them twice, because a duplicate dispatch here is a
// task executed twice for no reason a retry can explain.
func TestSecondEngineStandsByThenTakesOver(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := h.br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const ttl = 400 * time.Millisecond
	a := h.engine(t, "sched-a", ttl)
	b := h.engine(t, "sched-b", ttl)

	ctxA, stopA := context.WithCancel(ctx)
	ctxB, stopB := context.WithCancel(ctx)
	defer stopB()

	var wg sync.WaitGroup
	wg.Add(1)
	doneA := make(chan struct{})
	go func() { defer wg.Done(); defer close(doneA); _ = a.Run(ctxA) }()
	waitUntil(t, 5*time.Second, "engine a to take the lease", a.IsLeader)

	wg.Add(1)
	doneB := make(chan struct{})
	go func() { defer wg.Done(); defer close(doneB); _ = b.Run(ctxB) }()

	// First half of the work, dispatched by a.
	h.submit(t, ctx, 40, "A")
	waitUntil(t, 10*time.Second, "a to dispatch the first batch", func() bool {
		return h.execCount(ctx, t) >= 40
	})

	if b.IsLeader() {
		t.Fatal("both engines report leadership; the single-writer invariant is broken")
	}
	epochA := a.Epoch()
	if epochA == lease.NoEpoch {
		t.Fatal("the leading engine has no epoch, so its writes would not be fenced")
	}

	// a departs. More work arrives while nobody is leading yet.
	stopA()
	<-doneA
	h.submit(t, ctx, 40, "B")

	waitUntil(t, 10*time.Second, "b to take over", b.IsLeader)
	if epochB := b.Epoch(); epochB <= epochA {
		t.Errorf("successor epoch %d is not newer than %d; the fence could not tell the terms apart", epochB, epochA)
	}

	waitUntil(t, 20*time.Second, "b to dispatch the rest", func() bool {
		return h.execCount(ctx, t) >= 80
	})

	stopB()
	<-doneB
	wg.Wait()

	if got := h.execCount(ctx, t); got != 80 {
		t.Errorf("execution stream holds %d entries, want exactly 80: the handover lost or duplicated tasks", got)
	}
	if pending, err := h.br.PendingCount(ctx); err != nil || pending != 0 {
		t.Errorf("pending = %d (err %v), want 0", pending, err)
	}
}

// The reason policy state is persisted at all: a promoted standby must inherit
// the virtual clock. If it started from zero, every tenant would look unserved
// and the queue would re-grant service to whichever tenant was already ahead.
func TestPromotedEngineInheritsTheVirtualClock(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := h.br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}

	const ttl = 400 * time.Millisecond
	a := h.engine(t, "sched-a", ttl)
	ctxA, stopA := context.WithCancel(ctx)

	doneA := make(chan struct{})
	go func() { defer close(doneA); _ = a.Run(ctxA) }()
	waitUntil(t, 5*time.Second, "engine a to take the lease", a.IsLeader)

	h.submit(t, ctx, 60, "A")
	waitUntil(t, 10*time.Second, "a to dispatch", func() bool { return h.execCount(ctx, t) >= 60 })

	// Let the periodic flush land, then take a down.
	time.Sleep(2 * h.cfg.StateFlushInterval.D())
	stopA()
	<-doneA

	before, err := h.br.LoadSchedulerState(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	vtBefore, err := strconv.ParseFloat(before["vtime"], 64)
	if err != nil {
		t.Fatalf("persisted state has no usable vtime %v: %v", before, err)
	}
	if vtBefore <= 0 {
		t.Fatalf("vtime = %v after dispatching 60 tasks; the clock never advanced", vtBefore)
	}

	b := h.engine(t, "sched-b", ttl)
	ctxB, stopB := context.WithCancel(ctx)
	doneB := make(chan struct{})
	go func() { defer close(doneB); _ = b.Run(ctxB) }()
	waitUntil(t, 5*time.Second, "engine b to take over", b.IsLeader)

	// b dispatches a second tenant's work, and the two batches are deliberately
	// identical in size and cost. That makes the assertion discriminating: with
	// the clock inherited, tenant B starts at a's virtual time and the clock
	// roughly doubles; with the clock reset, B starts at zero and lands back on
	// almost exactly the value a left behind. "Not lower" would pass either way,
	// so the test demands strictly higher.
	h.submit(t, ctx, 60, "B")
	waitUntil(t, 10*time.Second, "b to dispatch", func() bool { return h.execCount(ctx, t) >= 120 })
	time.Sleep(2 * h.cfg.StateFlushInterval.D())

	stopB()
	<-doneB

	after, err := h.br.LoadSchedulerState(ctx)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	vtAfter, err := strconv.ParseFloat(after["vtime"], 64)
	if err != nil {
		t.Fatalf("state after failover has no usable vtime %v: %v", after, err)
	}
	if vtAfter <= vtBefore {
		t.Errorf("virtual time went from %v to %v across a failover; the promoted scheduler restarted the clock "+
			"instead of inheriting it, which would re-grant service to whichever tenant was already ahead", vtBefore, vtAfter)
	}
	t.Logf("virtual time %v -> %v across the failover", vtBefore, vtAfter)
}

// A scheduler that dies between reading an ingress entry and acknowledging it
// leaves that entry pending in its own consumer name. XREADGROUP with ">" only
// ever delivers entries nobody has seen, so the successor -- which necessarily
// runs under a different consumer name -- never sees it again. The task was
// accepted from the client's point of view and then silently never scheduled:
// not pending, not dispatched, not dead-lettered.
//
// This is not hypothetical. It showed up as exactly one stranded task in every
// killed-leader trial of cmd/failoverbench before the reclaim loop existed.
func TestPromotedEngineReclaimsTheDeadLeadersIngressEntries(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := h.br.EnsureStreams(ctx); err != nil {
		t.Fatalf("ensure streams: %v", err)
	}
	h.submit(t, ctx, 5, "A")

	// A scheduler reads the batch and dies before acknowledging any of it.
	dead, err := h.br.ReadIngress(ctx, "dead-leader", 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("read ingress as the doomed leader: %v", err)
	}
	if len(dead) != 5 {
		t.Fatalf("the doomed leader read %d entries, want 5; the test is not reproducing the situation", len(dead))
	}

	// Its successor starts up under a different consumer name.
	h.cfg.IngestReclaimMinIdle = config.Duration(200 * time.Millisecond)
	h.cfg.IngestReclaimInterval = config.Duration(100 * time.Millisecond)
	b := h.engine(t, "sched-b", 400*time.Millisecond)
	ctxB, stopB := context.WithCancel(ctx)
	defer stopB()
	doneB := make(chan struct{})
	go func() { defer close(doneB); _ = b.Run(ctxB) }()
	waitUntil(t, 5*time.Second, "engine b to take the lease", b.IsLeader)

	waitUntil(t, 15*time.Second, "b to reclaim and dispatch the stranded entries", func() bool {
		return h.execCount(ctx, t) >= 5
	})

	stopB()
	<-doneB

	if got := h.execCount(ctx, t); got != 5 {
		t.Errorf("execution stream holds %d entries, want exactly 5", got)
	}
}
