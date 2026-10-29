// Package console is the backend of the live operations console: it polls a
// running queue — Redis directly, and each process's /healthz, /readyz and
// /metrics — keeps a short history, derives events from what changes, and, in
// demo mode, supervises a disposable local cluster it can inject faults into.
//
// Everything it reports has a source it can name. Values it cannot read are
// absent, never zero, and every snapshot carries the time it was taken so the
// interface can tell stale from current.
package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/anishc23/distributed-task-queue/internal/broker"
)

// RedisState says whether the console could read the queue's state at all.
type RedisState struct {
	OK        bool    `json:"ok"`
	Error     string  `json:"error,omitempty"`
	LatencyMS float64 `json:"latency_ms,omitempty"`
	Namespace string  `json:"namespace"`
}

// Leadership is read from Redis, the authoritative record, not from any
// scheduler's opinion of itself.
type Leadership struct {
	Held             bool   `json:"held"`
	Owner            string `json:"owner,omitempty"`
	Epoch            int64  `json:"epoch,omitempty"`
	LeaseRemainingMS int64  `json:"lease_remaining_ms,omitempty"`
	// Fence is the highest epoch that has written; EpochCounter the last
	// epoch granted. They differ briefly after a grant, before the new
	// leader's first write.
	Fence        *int64 `json:"fence,omitempty"`
	EpochCounter *int64 `json:"epoch_counter,omitempty"`
}

// QueueState is the queue as Redis records it.
type QueueState struct {
	Pending            int64            `json:"pending"`
	InFlight           int64            `json:"in_flight"`
	Unacked            int64            `json:"unacked"`
	DeadLettered       int64            `json:"dead_lettered"`
	OldestPendingWaitS *float64         `json:"oldest_pending_wait_s,omitempty"`
	Counters           map[string]int64 `json:"counters"`
	TenantCompleted    map[string]int64 `json:"tenant_completed"`
}

// Rates are derived from consecutive readings of the Redis counters, which,
// unlike process metrics, survive a process restart. Nil means not enough
// information: the first sample, a gap in Redis reads, or a counter reset.
type Rates struct {
	WindowS        float64  `json:"window_s,omitempty"`
	AdmittedPerS   *float64 `json:"admitted_per_s,omitempty"`
	DispatchedPerS *float64 `json:"dispatched_per_s,omitempty"`
	CompletedPerS  *float64 `json:"completed_per_s,omitempty"`
}

// Component is one scraped process.
type Component struct {
	Target
	Probe
	LastOKAt *time.Time    `json:"last_ok_at,omitempty"`
	Process  *ProcessState `json:"process,omitempty"`
}

// Snapshot is everything the console knows at one poll.
type Snapshot struct {
	Seq        uint64      `json:"seq"`
	SampledAt  time.Time   `json:"sampled_at"`
	Redis      RedisState  `json:"redis"`
	Leadership Leadership  `json:"leadership"`
	Queue      *QueueState `json:"queue,omitempty"`
	Rates      Rates       `json:"rates"`
	Components []Component `json:"components"`
	// LeaderSum is the number of schedulers reporting tq_scheduler_is_leader
	// = 1. Nil when any scheduler could not be read, because a sum over the
	// processes that answered is not the sum the invariant is about.
	LeaderSum *int   `json:"leader_sum"`
	Policy    string `json:"policy,omitempty"`
}

// Sample is one point of history, small enough to keep ten minutes of.
type Sample struct {
	At             time.Time `json:"t"`
	RedisOK        bool      `json:"redis_ok"`
	Pending        *int64    `json:"pending"`
	InFlight       *int64    `json:"in_flight"`
	DispatchedPerS *float64  `json:"dispatched_per_s"`
	CompletedPerS  *float64  `json:"completed_per_s"`
	Owner          string    `json:"owner,omitempty"`
	Epoch          int64     `json:"epoch,omitempty"`
	LeaderSum      *int      `json:"leader_sum"`
}

// Options configures a Console.
type Options struct {
	Broker     *broker.Broker
	Targets    []Target
	Supervisor *Supervisor // nil outside demo mode
	Poll       time.Duration
	History    int
	Events     *EventLog
	Log        *slog.Logger
	Mode       string
}

// Console polls and holds state for the HTTP layer.
type Console struct {
	opt     Options
	scraper *Scraper
	started time.Time

	mu       sync.RWMutex
	latest   *Snapshot
	history  []Sample
	lastOK   map[string]time.Time
	prevCtrs map[string]int64
	prevAt   time.Time
	seq      uint64
}

// New builds a console; Run starts polling.
func New(opt Options) *Console {
	if opt.Poll <= 0 {
		opt.Poll = time.Second
	}
	if opt.History <= 0 {
		opt.History = 600
	}
	if opt.Events == nil {
		opt.Events = NewEventLog(300)
	}
	timeout := opt.Poll * 8 / 10
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	return &Console{opt: opt, scraper: NewScraper(timeout), started: time.Now(), lastOK: map[string]time.Time{}}
}

// Events exposes the event log.
func (c *Console) Events() *EventLog { return c.opt.Events }

// Run polls until ctx is done.
func (c *Console) Run(ctx context.Context) {
	t := time.NewTicker(c.opt.Poll)
	defer t.Stop()
	for {
		c.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Console) pollOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, c.opt.Poll)
	defer cancel()
	snap := &Snapshot{SampledAt: time.Now()}
	snap.Redis.Namespace = c.opt.Broker.Keys().Namespace

	var wg sync.WaitGroup
	comps := make([]Component, len(c.opt.Targets))
	for i, t := range c.opt.Targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			comps[i] = Component{Target: t, Probe: c.scraper.Probe(ctx, t)}
		}()
	}
	c.readRedis(ctx, snap)
	wg.Wait()

	var states map[string]ProcessState
	if c.opt.Supervisor != nil {
		states = c.opt.Supervisor.States()
	}
	leaders, known := 0, true
	for i := range comps {
		comp := &comps[i]
		if comp.Health != HealthUnreachable {
			c.lastOK[comp.Name] = snap.SampledAt
		}
		if t, ok := c.lastOK[comp.Name]; ok {
			t := t
			comp.LastOKAt = &t
		}
		if st, ok := states[comp.Name]; ok {
			st := st
			comp.Process = &st
		}
		if comp.Policy != "" && comp.Role == "scheduler" {
			snap.Policy = comp.Policy
		}
		if comp.Role == "scheduler" {
			v, ok := comp.Metrics["tq_scheduler_is_leader"]
			switch {
			case ok:
				if v >= 1 {
					leaders++
				}
			case comp.Process != nil && comp.Process.State == StateExited:
				// A process the supervisor saw exit leads nothing. A frozen
				// or partitioned one is different: it may still believe it
				// leads, which is exactly the case the fence is for, so it
				// leaves the sum unknown.
			default:
				known = false
			}
		}
	}
	snap.Components = comps
	if known {
		snap.LeaderSum = &leaders
	}

	c.mu.Lock()
	c.seq++
	snap.Seq = c.seq
	prev := c.latest
	c.latest = snap
	c.history = append(c.history, sampleOf(snap))
	if len(c.history) > c.opt.History {
		c.history = c.history[len(c.history)-c.opt.History:]
	}
	c.mu.Unlock()

	for _, e := range Diff(prev, snap) {
		c.opt.Events.Add(e)
	}
}

func (c *Console) readRedis(ctx context.Context, snap *Snapshot) {
	br := c.opt.Broker
	start := time.Now()
	if err := br.Ping(ctx); err != nil {
		snap.Redis.Error = err.Error()
		c.prevCtrs = nil
		return
	}
	snap.Redis.LatencyMS = float64(time.Since(start).Microseconds()) / 1000

	q := &QueueState{}
	var errs []error
	collect := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err error
	q.Counters, err = br.Stats(ctx)
	collect(err)
	q.Pending, err = br.PendingCount(ctx)
	collect(err)
	q.InFlight, err = br.InFlight(ctx)
	collect(err)
	q.Unacked, err = br.ExecPending(ctx)
	collect(err)
	q.DeadLettered, err = br.DeadLetterCount(ctx)
	collect(err)
	_, q.TenantCompleted, err = br.TenantService(ctx)
	collect(err)
	if oldest, ok, err := br.OldestPendingSubmit(ctx); err != nil {
		collect(err)
	} else if ok {
		w := time.Since(oldest).Seconds()
		q.OldestPendingWaitS = &w
	}
	owner, epoch, ttl, held, err := br.Leadership(ctx)
	collect(err)
	snap.Leadership = Leadership{Held: held, Epoch: epoch, LeaseRemainingMS: ttl.Milliseconds()}
	if held {
		snap.Leadership.Owner = owner
	}
	snap.Leadership.Fence = readInt(ctx, br.Redis(), br.Keys().Fence, &errs)
	snap.Leadership.EpochCounter = readInt(ctx, br.Redis(), br.Keys().LeaderEpoch, &errs)

	if len(errs) > 0 {
		snap.Redis.Error = errors.Join(errs...).Error()
		c.prevCtrs = nil
		return
	}
	snap.Redis.OK = true
	snap.Queue = q

	now := snap.SampledAt
	if c.prevCtrs != nil {
		dt := now.Sub(c.prevAt)
		snap.Rates.WindowS = dt.Seconds()
		rate := func(k string) *float64 {
			if v, ok := Rate(float64(c.prevCtrs[k]), float64(q.Counters[k]), dt); ok {
				return &v
			}
			return nil
		}
		snap.Rates.AdmittedPerS = rate("admitted")
		snap.Rates.DispatchedPerS = rate("dispatched")
		snap.Rates.CompletedPerS = rate("completed")
	}
	c.prevCtrs, c.prevAt = q.Counters, now
}

func readInt(ctx context.Context, rdb *redis.Client, key string, errs *[]error) *int64 {
	s, err := rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		*errs = append(*errs, fmt.Errorf("read %s: %w", key, err))
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s is not an integer: %q", key, s))
		return nil
	}
	return &n
}

func sampleOf(s *Snapshot) Sample {
	out := Sample{At: s.SampledAt, RedisOK: s.Redis.OK, LeaderSum: s.LeaderSum,
		DispatchedPerS: s.Rates.DispatchedPerS, CompletedPerS: s.Rates.CompletedPerS}
	if s.Queue != nil {
		p, f := s.Queue.Pending, s.Queue.InFlight
		out.Pending, out.InFlight = &p, &f
	}
	if s.Leadership.Held {
		out.Owner, out.Epoch = s.Leadership.Owner, s.Leadership.Epoch
	}
	return out
}

// Latest returns the newest snapshot, or nil before the first poll.
func (c *Console) Latest() *Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latest
}

// History returns a copy of the retained samples, oldest first.
func (c *Console) History() []Sample {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]Sample(nil), c.history...)
}

// LeaseOwner is the current lease holder from the newest snapshot.
func (c *Console) LeaseOwner() string {
	if s := c.Latest(); s != nil && s.Redis.OK && s.Leadership.Held {
		return s.Leadership.Owner
	}
	return ""
}
