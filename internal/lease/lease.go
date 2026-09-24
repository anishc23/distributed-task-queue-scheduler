// Package lease implements the leader lease that enforces the scheduler's
// single-writer invariant.
//
// The scheduler is the only component that decides execution order, and two
// schedulers ranking concurrently is not merely redundant, it is wrong: each
// carries its own copy of the policy state, so two WFQ engines advance two
// divergent virtual clocks and then overwrite each other's persisted state on
// the next flush. Nothing about that failure is loud. Fairness is simply
// computed against the wrong clock.
//
// Enforcing "exactly one scheduler dispatches" therefore has to be a property
// of the system rather than a property of the deployment manifest. This package
// provides the mechanism in two parts:
//
//	the lease   a Redis key held with SET NX PX, renewed by its holder and
//	            released on clean shutdown. It elects a leader.
//	the epoch   a monotonically increasing token handed out with each grant.
//
// The epoch is the part that actually makes this safe, and it is worth being
// precise about why the lease alone is not. A leader can lose its lease without
// knowing: a long garbage-collection pause, a suspended container, or a network
// partition that outlasts the TTL. Redis expires the key, a standby wins the
// next grant, and only then does the old leader resume, believing it still
// leads. No amount of careful renewal removes that window, because the old
// leader is not running during it.
//
// So the epoch travels with every write the leader makes, and the write itself
// refuses to apply a token older than the newest one Redis has already seen
// (see fenceGuard in the broker's Lua scripts). The check lives at the resource,
// not in the client, which is what makes it sound: a process that has been
// stopped for a minute cannot pass a stale token, whatever it believes about
// its own leadership. Cancelling the deposed leader's context is a courtesy
// that makes the common case fast; the fence is the guarantee.
//
// Epochs are monotonic but not gapless, and nothing depends on them being
// contiguous — only on a later grant carrying a strictly larger number.
package lease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// NoEpoch is the token used when leader election is disabled. The fence guard
// in the Lua scripts treats a non-positive epoch as "unfenced" and skips the
// check entirely, so a single-scheduler deployment, the benchmark harness and
// every existing test behave exactly as they did before this package existed.
const NoEpoch int64 = 0

// Keys names the two Redis keys a lease needs.
type Keys struct {
	// Holder is a STRING holding "<owner>|<epoch>" with a TTL. Its existence
	// is leadership.
	Holder string
	// Epoch is a STRING counter, INCRed once per grant. It is never reset, so
	// tokens stay monotonic across restarts of every process in the system.
	Epoch string
}

// Options configures a Lease.
type Options struct {
	Redis  *redis.Client
	Keys   Keys
	Owner  string        // defaults to hostname-pid
	TTL    time.Duration // how long a grant survives without renewal
	Renew  time.Duration // how often the holder extends it
	Retry  time.Duration // how often a standby retries
	Logger *slog.Logger

	// WaitForReplicas makes a freshly acquired epoch wait for this many
	// replicas to acknowledge it before the holder is told it leads. Zero
	// disables the wait, which is correct for a single Redis and is the
	// behaviour everything had before this option existed.
	//
	// This exists because of a measured failure, not a hypothetical one. The
	// fence's guarantee is that once epoch N has written, nothing below N ever
	// writes again, and that rests entirely on the epoch counter surviving. A
	// Redis failover to a replica that had not caught up loses acknowledged
	// writes: in the store-failure experiment the counter came back *missing*,
	// which resets the floor to zero and lets every superseded leader through.
	// Waiting here makes exactly the one write that the fence depends on
	// durable, and leaves every other write on the fast path.
	WaitForReplicas int
	// WaitTimeout bounds that wait. Exceeding it is not a slow success: it
	// means the epoch may not survive a failover, so the acquisition is
	// abandoned and the lease released.
	WaitTimeout time.Duration
}

// Lease is a Redis-backed leader lease. It is safe for concurrent use.
type Lease struct {
	rdb         *redis.Client
	keys        Keys
	owner       string
	ttl         time.Duration
	renew       time.Duration
	retry       time.Duration
	waitRepl    int
	waitTimeout time.Duration
	log         *slog.Logger

	mu    sync.Mutex
	value string // "<owner>|<epoch>" of the grant currently held, empty if none
	epoch int64
}

// New builds a lease. It does not touch Redis.
func New(opts Options) (*Lease, error) {
	switch {
	case opts.Redis == nil:
		return nil, errors.New("lease: redis client is required")
	case opts.Keys.Holder == "" || opts.Keys.Epoch == "":
		return nil, errors.New("lease: holder and epoch keys are required")
	case opts.TTL <= 0:
		return nil, errors.New("lease: ttl must be > 0")
	}

	renew := opts.Renew
	if renew <= 0 {
		renew = opts.TTL / 3
	}
	// A holder that renews no more often than the TTL will drop the lease on
	// the first slow round trip, which turns every transient hiccup into a
	// failover. Refuse the configuration rather than electing leaders in a loop.
	if renew >= opts.TTL {
		return nil, fmt.Errorf("lease: renew interval %s must be shorter than ttl %s", renew, opts.TTL)
	}
	retry := opts.Retry
	if retry <= 0 {
		retry = opts.TTL / 3
	}
	owner := opts.Owner
	if owner == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "scheduler"
		}
		owner = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	if opts.WaitForReplicas < 0 {
		return nil, fmt.Errorf("lease: wait_for_replicas must be >= 0, got %d", opts.WaitForReplicas)
	}
	waitTimeout := opts.WaitTimeout
	if opts.WaitForReplicas > 0 && waitTimeout <= 0 {
		// Defaulting rather than failing, but not to something unbounded: a
		// wait that can outlive the lease it is protecting is worse than none.
		waitTimeout = opts.TTL / 2
	}

	return &Lease{
		rdb:         opts.Redis,
		keys:        opts.Keys,
		owner:       owner,
		ttl:         opts.TTL,
		renew:       renew,
		retry:       retry,
		waitRepl:    opts.WaitForReplicas,
		waitTimeout: waitTimeout,
		log:         log.With("component", "lease", "owner", owner),
	}, nil
}

// Owner returns this lease's identity.
func (l *Lease) Owner() string { return l.owner }

// TTL returns how long a grant survives without renewal. It is the upper bound
// on failover time after a hard crash.
func (l *Lease) TTL() time.Duration { return l.ttl }

// RenewInterval returns how often the holder extends its grant.
func (l *Lease) RenewInterval() time.Duration { return l.renew }

// Epoch returns the token of the grant currently held, or NoEpoch.
func (l *Lease) Epoch() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

// acquireScript takes the lease if it is free and stamps the grant with a fresh
// epoch. EXISTS and SET run in one atomic unit, so the obvious race between
// "nobody holds it" and "I hold it" cannot happen, and the counter is only
// advanced on a grant that actually succeeded.
//
// KEYS: holder, epochCounter
// ARGV: owner, ttlMillis
// Returns {epoch, value} on success and {0, ”} when somebody else holds it.
var acquireScript = redis.NewScript(`
local holder = KEYS[1]
local epochK = KEYS[2]

local owner  = ARGV[1]
local ttlMS  = ARGV[2]

if redis.call('EXISTS', holder) == 1 then
  return {0, ''}
end

local epoch = redis.call('INCR', epochK)
local value = owner .. '|' .. epoch
redis.call('SET', holder, value, 'PX', ttlMS)
return {epoch, value}
`)

// renewScript extends the grant only when this exact grant still holds it. The
// value carries the epoch, so a lease that lapsed and was re-granted to this
// same process under a new epoch will not be extended by the old term's ticker.
//
// KEYS: holder
// ARGV: value, ttlMillis
// Returns 1 when extended, 0 when the grant is gone.
var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

// releaseScript hands the lease back, but only if we still hold it. Deleting
// unconditionally would let a departing process evict the leader that had
// already replaced it.
//
// KEYS: holder
// ARGV: value
// Returns 1 when released, 0 when the grant had already moved on.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

// Acquire tries once to take the lease. It reports the epoch of the new grant
// when it succeeds, and NoEpoch when another process holds it.
func (l *Lease) Acquire(ctx context.Context) (int64, bool, error) {
	res, err := acquireScript.Run(ctx, l.rdb,
		[]string{l.keys.Holder, l.keys.Epoch},
		l.owner, l.ttl.Milliseconds(),
	).Slice()
	if err != nil {
		return NoEpoch, false, fmt.Errorf("lease acquire: %w", err)
	}
	if len(res) != 2 {
		return NoEpoch, false, fmt.Errorf("lease acquire: malformed reply of length %d", len(res))
	}
	epoch, _ := res[0].(int64)
	value, _ := res[1].(string)
	if epoch <= 0 || value == "" {
		return NoEpoch, false, nil
	}

	// Make the epoch durable before acting on it. Until this returns, the
	// counter that the entire fencing argument rests on exists only on one
	// machine.
	if err := l.awaitEpochDurable(ctx, epoch, value); err != nil {
		return NoEpoch, false, err
	}

	l.mu.Lock()
	l.value, l.epoch = value, epoch
	l.mu.Unlock()
	return epoch, true, nil
}

// awaitEpochDurable blocks until the write that produced this epoch has reached
// the configured number of replicas, and gives the grant back if it cannot.
//
// WAIT is not a consensus protocol and this does not pretend otherwise. It
// reports how many replicas acknowledged the writes issued so far, which turns
// "the master said yes" into "at least N machines have it". That is enough to
// stop a Sentinel promotion from silently rewinding the fence, which is the
// failure this guards; it is not enough to make Redis linearizable, and a
// deployment that needs that should hold the epoch somewhere with real
// consensus.
//
// Releasing on failure is the important half. A leader that keeps a grant whose
// epoch might not survive a failover is exactly the situation the fence cannot
// recover from, so not leading is the safer outcome.
func (l *Lease) awaitEpochDurable(ctx context.Context, epoch int64, value string) error {
	if l.waitRepl <= 0 {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, l.waitTimeout)
	defer cancel()

	acked, err := l.rdb.Wait(waitCtx, l.waitRepl, l.waitTimeout).Result()
	if err == nil && int(acked) >= l.waitRepl {
		return nil
	}

	// Hand the grant back so a peer can take it, using a context that is not
	// the one that just timed out.
	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer releaseCancel()
	if _, rerr := releaseScript.Run(releaseCtx, l.rdb, []string{l.keys.Holder}, value).Result(); rerr != nil {
		l.log.Warn("could not release a grant whose epoch is not durable",
			"epoch", epoch, "error", rerr)
	}
	if err != nil {
		return fmt.Errorf("lease acquire: epoch %d durability wait failed: %w", epoch, err)
	}
	return fmt.Errorf("lease acquire: epoch %d reached %d of %d replicas within %s; standing down rather than leading on an epoch that may not survive a failover",
		epoch, acked, l.waitRepl, l.waitTimeout)
}

// Renew extends the current grant. It returns false when the grant is gone,
// which means another process may already be leading.
//
// Renewal deliberately does not wait for replicas, even when Acquire does. A
// renewal carries no new epoch: it re-sets a holder key whose loss costs at
// most one lease TTL of availability, which a standby then takes over. The
// epoch is different in kind — losing it rewinds the fence floor for every
// term that follows — so it is the one write worth paying for. Waiting on
// every renewal would put a replication round trip on the critical path
// several times a second to protect something that expires on its own.
func (l *Lease) Renew(ctx context.Context) (bool, error) {
	l.mu.Lock()
	value := l.value
	l.mu.Unlock()
	if value == "" {
		return false, nil
	}

	n, err := renewScript.Run(ctx, l.rdb, []string{l.keys.Holder}, value, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("lease renew: %w", err)
	}
	if n != 1 {
		l.forget(value)
		return false, nil
	}
	return true, nil
}

// Release gives the lease up so a standby can take it immediately instead of
// waiting out the TTL. This is what makes a planned rollout fast; an unplanned
// crash necessarily costs the remaining TTL.
func (l *Lease) Release(ctx context.Context) error {
	l.mu.Lock()
	value := l.value
	l.mu.Unlock()
	if value == "" {
		return nil
	}

	_, err := releaseScript.Run(ctx, l.rdb, []string{l.keys.Holder}, value).Int64()
	l.forget(value)
	if err != nil {
		return fmt.Errorf("lease release: %w", err)
	}
	return nil
}

// forget clears the held grant, but only if it is still the one passed in. A
// renewal failing for a term that has already been superseded must not wipe the
// newer term's state.
func (l *Lease) forget(value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.value == value {
		l.value, l.epoch = "", NoEpoch
	}
}

// Observer is notified on every leadership transition. It is called
// synchronously, so it must not block.
type Observer func(leader bool, epoch int64)

// Campaign runs until ctx is done. Whenever this process wins the lease it
// calls lead with a context that is cancelled the moment the grant is lost, and
// with the epoch to stamp on every write made during that term.
//
// Losing the lease is not an error. The process drops back to standby and keeps
// campaigning, which is what lets a recovered leader rejoin as a follower
// rather than exiting and relying on a supervisor to restart it.
func (l *Lease) Campaign(ctx context.Context, lead func(ctx context.Context, epoch int64) error, observe Observer) error {
	notify := func(leader bool, epoch int64) {
		if observe != nil {
			observe(leader, epoch)
		}
	}
	notify(false, NoEpoch)

	for {
		if ctx.Err() != nil {
			return nil
		}

		epoch, ok, err := l.Acquire(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.log.Error("lease acquisition failed", "error", err)
		}
		if !ok {
			if sleepCtx(ctx, l.retry) != nil {
				return nil
			}
			continue
		}

		l.log.Info("became scheduler leader", "epoch", epoch, "ttl", l.ttl)
		notify(true, epoch)

		if err := l.serveTerm(ctx, epoch, lead); err != nil {
			notify(false, NoEpoch)
			return err
		}
		notify(false, NoEpoch)

		if ctx.Err() != nil {
			return nil
		}
		// The term ended without ctx being cancelled: the lease lapsed, or the
		// leader loop stood down because it was fenced. Wait a beat before
		// campaigning again, so a process with a genuinely unhealthy connection
		// does not spin on the epoch counter.
		if sleepCtx(ctx, l.retry) != nil {
			return nil
		}
	}
}

// serveTerm runs one leadership term: it starts lead, keeps the grant alive,
// and returns once the term ends for any reason.
func (l *Lease) serveTerm(ctx context.Context, epoch int64, lead func(context.Context, int64) error) error {
	termCtx, endTerm := context.WithCancel(ctx)
	defer endTerm()

	done := make(chan error, 1)
	go func() { done <- lead(termCtx, epoch) }()

	ticker := time.NewTicker(l.renew)
	defer ticker.Stop()

	var leadErr error
	// returned tracks whether lead has already been collected from done.
	// Without it, the "lead finished on its own" path would try to read the
	// channel a second time and block forever -- and that path is the one
	// fencing takes, since a fenced engine ends its term while the lease still
	// looks healthy.
	returned := false
	lost := false

	for keep := true; keep; {
		select {
		case <-ctx.Done():
			keep = false

		case leadErr = <-done:
			returned = true
			keep = false

		case <-ticker.C:
			ok, err := l.Renew(termCtx)
			if err != nil {
				if termCtx.Err() != nil {
					keep = false
					break
				}
				// A failed round trip is not proof the grant is gone, but it is
				// no proof it survives either. Keep trying until the TTL runs
				// out; the fence protects the writes in the meantime.
				l.log.Warn("lease renewal failed, will retry", "epoch", epoch, "error", err)
				break
			}
			if !ok {
				l.log.Warn("lost the scheduler lease", "epoch", epoch)
				lost = true
				keep = false
			}
		}
	}

	endTerm()
	if !returned {
		// Wait for lead to observe the cancellation and unwind, so a deposed
		// leader is never still dispatching while its successor starts. This is
		// best effort and bounded by lead itself; the epoch fence is what makes
		// the overlap harmless if it takes longer than expected.
		leadErr = <-done
	}
	if errors.Is(leadErr, context.Canceled) {
		leadErr = nil
	}
	if leadErr != nil {
		l.log.Error("leader loop failed", "epoch", epoch, "error", leadErr)
	}

	if !lost {
		// Released from a context that may already be cancelled, so the release
		// gets its own deadline. Skipping it would make every clean shutdown
		// cost a full TTL of dispatch downtime.
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		if err := l.Release(relCtx); err != nil {
			l.log.Warn("lease release failed, standby will wait out the ttl", "error", err)
		}
		cancel()
	}
	return leadErr
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = time.Millisecond
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
