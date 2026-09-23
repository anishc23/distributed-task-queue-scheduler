//go:build integration

// These tests need a real Redis because the properties under test live in
// Redis: key expiry, the atomicity of the acquire script, and the monotonicity
// of the epoch counter. A fake would be testing the fake.
//
//	make test-integration
//	TQ_TEST_REDIS_ADDR=host:port make test-integration
package lease_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/lease"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	if v := os.Getenv("TQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// newKeys returns a key pair unique to this test so tests can run in parallel
// against a shared Redis.
func newKeys(t *testing.T) (*redis.Client, lease.Keys) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr(), DialTimeout: 3 * time.Second})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Fatalf("Redis is required for integration tests but is not reachable at %s: %v\n"+
			"Start one with `make redis-up` or set TQ_TEST_REDIS_ADDR.", redisAddr(), err)
	}

	prefix := fmt.Sprintf("tqtest:lease:%d", time.Now().UnixNano())
	keys := lease.Keys{Holder: prefix + ":holder", Epoch: prefix + ":epoch"}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rdb.Del(c, keys.Holder, keys.Epoch)
		rdb.Close()
	})
	return rdb, keys
}

func newLease(t *testing.T, rdb *redis.Client, keys lease.Keys, owner string, ttl time.Duration) *lease.Lease {
	t.Helper()
	l, err := lease.New(lease.Options{
		Redis: rdb, Keys: keys, Owner: owner,
		TTL: ttl, Renew: ttl / 4, Retry: ttl / 10,
	})
	if err != nil {
		t.Fatalf("lease.New(%s): %v", owner, err)
	}
	return l
}

// Exactly one of two contenders may hold the lease, and the loser must not be
// handed an epoch: a token it never held could later be used to fence out a
// leader that is perfectly healthy.
func TestOnlyOneHolderAtATime(t *testing.T) {
	ctx := context.Background()
	rdb, keys := newKeys(t)

	a := newLease(t, rdb, keys, "a", 5*time.Second)
	b := newLease(t, rdb, keys, "b", 5*time.Second)

	epochA, okA, err := a.Acquire(ctx)
	if err != nil || !okA {
		t.Fatalf("a.Acquire() = %d, %v, %v; want a grant", epochA, okA, err)
	}
	epochB, okB, err := b.Acquire(ctx)
	if err != nil {
		t.Fatalf("b.Acquire() error: %v", err)
	}
	if okB {
		t.Fatalf("b acquired the lease while a held it (epoch %d); the single-writer invariant is broken", epochB)
	}
	if epochB != lease.NoEpoch {
		t.Errorf("a failed acquisition returned epoch %d; want NoEpoch, or the counter leaks tokens to non-leaders", epochB)
	}

	if err := a.Release(ctx); err != nil {
		t.Fatalf("a.Release(): %v", err)
	}
	epochB, okB, err = b.Acquire(ctx)
	if err != nil || !okB {
		t.Fatalf("b.Acquire() after release = %d, %v, %v; want a grant", epochB, okB, err)
	}
	if epochB <= epochA {
		t.Errorf("epoch went from %d to %d; tokens must strictly increase or fencing cannot order terms", epochA, epochB)
	}
}

// Concurrent acquisition is the case the acquire script's atomicity exists for.
// Without EXISTS and SET in one unit, several contenders read "free" and all
// proceed.
func TestConcurrentAcquireGrantsExactlyOne(t *testing.T) {
	ctx := context.Background()
	rdb, keys := newKeys(t)

	const contenders = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := map[int64]string{}

	start := make(chan struct{})
	for i := 0; i < contenders; i++ {
		owner := fmt.Sprintf("c%d", i)
		l := newLease(t, rdb, keys, owner, 5*time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			epoch, ok, err := l.Acquire(ctx)
			if err != nil {
				t.Errorf("%s.Acquire(): %v", owner, err)
				return
			}
			if ok {
				mu.Lock()
				granted[epoch] = owner
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(granted) != 1 {
		t.Fatalf("%d contenders produced %d grants %v; want exactly 1", contenders, len(granted), granted)
	}
}

// A lease that stops being renewed must expire, or a crashed leader would block
// the queue permanently.
func TestLeaseExpiresWhenNotRenewed(t *testing.T) {
	ctx := context.Background()
	rdb, keys := newKeys(t)

	const ttl = 300 * time.Millisecond
	a := newLease(t, rdb, keys, "a", ttl)
	b := newLease(t, rdb, keys, "b", ttl)

	if _, ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a.Acquire(): %v, %v", ok, err)
	}

	// a is now a crashed leader: it renews nothing and releases nothing.
	deadline := time.Now().Add(5 * time.Second)
	var took time.Duration
	acquired := false
	startedAt := time.Now()
	for time.Now().Before(deadline) {
		if _, ok, err := b.Acquire(ctx); err == nil && ok {
			took, acquired = time.Since(startedAt), true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !acquired {
		t.Fatal("b never acquired the lease after a stopped renewing; a crashed leader would wedge the queue")
	}
	if took < ttl/2 {
		t.Errorf("b took over after %s with a %s ttl; taking over early means the key is not actually holding the lease", took, ttl)
	}
	if took > 3*ttl {
		t.Errorf("b took %s to take over with a %s ttl; failover is far slower than the ttl implies", took, ttl)
	}
}

// Renewal keeps a healthy leader in place. Without this the queue would fail
// over every TTL for no reason.
func TestRenewKeepsTheGrant(t *testing.T) {
	ctx := context.Background()
	rdb, keys := newKeys(t)

	const ttl = 400 * time.Millisecond
	a := newLease(t, rdb, keys, "a", ttl)
	b := newLease(t, rdb, keys, "b", ttl)

	if _, ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a.Acquire(): %v, %v", ok, err)
	}
	for i := 0; i < 8; i++ {
		time.Sleep(ttl / 4)
		ok, err := a.Renew(ctx)
		if err != nil {
			t.Fatalf("a.Renew(): %v", err)
		}
		if !ok {
			t.Fatalf("a lost its grant on renewal %d despite renewing every %s with a %s ttl", i, ttl/4, ttl)
		}
		if _, ok, err := b.Acquire(ctx); err == nil && ok {
			t.Fatalf("b acquired the lease while a was renewing it")
		}
	}
}

// Release must not evict a successor. A leader that lost its lease, then shut
// down and deleted the key unconditionally, would depose the healthy leader
// that had already replaced it.
func TestReleaseDoesNotEvictASuccessor(t *testing.T) {
	ctx := context.Background()
	rdb, keys := newKeys(t)

	const ttl = 250 * time.Millisecond
	a := newLease(t, rdb, keys, "a", ttl)
	b := newLease(t, rdb, keys, "b", ttl)

	if _, ok, err := a.Acquire(ctx); err != nil || !ok {
		t.Fatalf("a.Acquire(): %v, %v", ok, err)
	}
	time.Sleep(2 * ttl) // a's grant lapses
	epochB, ok, err := b.Acquire(ctx)
	if err != nil || !ok {
		t.Fatalf("b.Acquire() after expiry: %v, %v", ok, err)
	}

	// a now wakes up and shuts down politely, still believing it leads.
	if err := a.Release(ctx); err != nil {
		t.Fatalf("a.Release(): %v", err)
	}

	held, err := rdb.Get(ctx, keys.Holder).Result()
	if err != nil {
		t.Fatalf("holder key is gone after a stale release: %v; b was deposed by a process that no longer led", err)
	}
	if want := fmt.Sprintf("b|%d", epochB); held != want {
		t.Errorf("holder = %q, want %q", held, want)
	}
}

// Campaign is the loop the scheduler runs. A standby must take over after the
// leader stops, and a process that loses leadership must go back to standby
// rather than exiting.
func TestCampaignFailsOver(t *testing.T) {
	rdb, keys := newKeys(t)
	const ttl = 300 * time.Millisecond

	var mu sync.Mutex
	var order []string
	leading := func(name string) func(context.Context, int64) error {
		return func(ctx context.Context, epoch int64) error {
			mu.Lock()
			order = append(order, fmt.Sprintf("%s@%d", name, epoch))
			mu.Unlock()
			<-ctx.Done()
			return nil
		}
	}

	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()

	a := newLease(t, rdb, keys, "a", ttl)
	b := newLease(t, rdb, keys, "b", ttl)

	doneA := make(chan struct{})
	go func() { defer close(doneA); _ = a.Campaign(ctxA, leading("a"), nil) }()

	// Let a win before b starts, so the expected order is deterministic.
	waitFor(t, 2*time.Second, func() bool { return a.Epoch() != lease.NoEpoch })

	doneB := make(chan struct{})
	go func() { defer close(doneB); _ = b.Campaign(ctxB, leading("b"), nil) }()

	// b must not lead while a is healthy.
	time.Sleep(3 * ttl)
	mu.Lock()
	if len(order) != 1 {
		mu.Unlock()
		t.Fatalf("leadership log = %v; want only a leading while a is healthy", order)
	}
	mu.Unlock()

	// a shuts down cleanly and releases, so b should take over quickly.
	stopA()
	<-doneA
	waitFor(t, 3*time.Second, func() bool { return b.Epoch() != lease.NoEpoch })

	stopB()
	<-doneB

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "a@1" {
		t.Fatalf("leadership log = %v; want a then b, with strictly increasing epochs", order)
	}
	if order[1] != "b@2" {
		t.Errorf("second term = %q, want b@2: the successor must get a strictly newer epoch", order[1])
	}
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", limit)
}

// A renew interval at or above the TTL leaves a healthy leader no margin for a
// single slow round trip, so it is rejected at construction rather than
// producing a queue that fails over continuously.
func TestRenewIntervalMustBeShorterThanTTL(t *testing.T) {
	rdb, keys := newKeys(t)
	if _, err := lease.New(lease.Options{
		Redis: rdb, Keys: keys, Owner: "a",
		TTL: time.Second, Renew: time.Second,
	}); err == nil {
		t.Fatal("lease.New accepted a renew interval equal to the ttl; every transient hiccup would trigger a failover")
	}
}

// A leader whose work ends on its own, while the lease is still perfectly
// healthy, must not wedge the campaign loop.
//
// This is the path fencing takes: a fenced engine stops its term because
// somebody else has taken over at the resource, and it does so without the
// lease having expired or been lost. An earlier version of serveTerm collected
// the result once inside the select and then waited on the same channel again
// afterwards, which deadlocked here and nowhere else — the ordinary shutdown
// path cancels the context first, so it never took this branch.
func TestTermEndingOnItsOwnDoesNotWedgeTheCampaign(t *testing.T) {
	rdb, keys := newKeys(t)
	const ttl = 400 * time.Millisecond

	l, err := lease.New(lease.Options{
		Redis: rdb, Keys: keys, Owner: "a",
		TTL: ttl, Renew: ttl / 4, Retry: 20 * time.Millisecond,
		Logger: nil,
	})
	if err != nil {
		t.Fatalf("lease.New: %v", err)
	}

	var terms atomic.Int64
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	done := make(chan error, 1)
	go func() {
		done <- l.Campaign(ctx, func(context.Context, int64) error {
			// Return immediately, as a fenced engine does.
			terms.Add(1)
			return nil
		}, nil)
	}()

	// Several terms must start and finish. A wedged campaign stops at one.
	deadline := time.Now().Add(5 * time.Second)
	for terms.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := terms.Load(); got < 3 {
		t.Fatalf("only %d terms ran in 5s; the campaign loop is wedged after a term that ended on its own", got)
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Campaign returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Campaign did not return after its context was cancelled")
	}
}

// A term that fails for a real reason is reported, and still does not wedge the
// loop.
func TestTermReturningAnErrorIsReported(t *testing.T) {
	rdb, keys := newKeys(t)
	const ttl = 400 * time.Millisecond

	l := newLease(t, rdb, keys, "a", ttl)
	want := errors.New("policy state is unreadable")

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	done := make(chan error, 1)
	go func() { done <- l.Campaign(ctx, func(context.Context, int64) error { return want }, nil) }()

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("Campaign returned %v, want %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Campaign did not return after its leader loop failed")
	}
}
