package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/lease"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
)

// Engine is the central scheduling process. It is the only component that
// decides execution order: producers only append to the ingress stream and
// workers only consume whatever the engine has already ranked and dispatched.
//
// The engine runs three cooperating loops:
//
//	ingest   ingress stream -> rank with the policy -> persistent pending set
//	dispatch pending set    -> atomic ZPOPMIN + XADD -> worker execution stream
//	reclaim  ingress entries stranded by a scheduler that died mid-batch
//	observe  periodic gauges and policy-state persistence
//
// Persistent state and why it is persisted:
//
//	pending      ZSET of ranked tasks. Survives restart so no admitted task is
//	             lost and the ordering decision does not have to be recomputed.
//	pending_age  ZSET keyed by submission time, used for the starvation gauge.
//	payloads     HASH of task JSON, the authoritative copy including retry count.
//	sched_state  HASH of policy state (WFQ virtual time and per-tenant finish
//	             tags). Without it, a restart would reset every tenant's virtual
//	             clock to zero and briefly hand service to the wrong tenant.
//	done         HASH of completed task IDs, the idempotency guard.
//	inflight     dispatched-but-not-terminal counter used to bound dispatch.
//
// Leadership. The engine is a single-writer component: two engines ranking the
// same stream concurrently would each advance their own copy of the policy
// state and then overwrite each other's flush, which corrupts WFQ silently.
// When a lease is supplied the engine campaigns for leadership and only ingests
// and dispatches while it holds the lease, stamping every write with the term's
// epoch so that a superseded leader is refused at the resource. When no lease is
// supplied the engine leads unconditionally under lease.NoEpoch, which is the
// single-process behaviour the benchmark harness and the tests rely on.
type Engine struct {
	br     *broker.Broker
	pol    policy.Policy
	cfg    config.Scheduler
	log    *slog.Logger
	m      *metrics.Metrics
	name   string
	polMu  sync.Mutex
	ready  chan struct{}
	closed sync.Once

	lease  *lease.Lease // nil disables election: this process always leads
	epoch  atomic.Int64
	leader atomic.Bool
}

// EngineOptions configures an Engine.
type EngineOptions struct {
	Broker   *broker.Broker
	Policy   policy.Policy
	Config   config.Scheduler
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
	Consumer string
	// Lease, when non-nil, makes this engine campaign for leadership instead
	// of assuming it. Leave it nil for a single-scheduler deployment.
	Lease *lease.Lease
}

// NewEngine builds a scheduling engine.
func NewEngine(opts EngineOptions) (*Engine, error) {
	switch {
	case opts.Broker == nil:
		return nil, errors.New("scheduler engine: broker is required")
	case opts.Policy == nil:
		return nil, errors.New("scheduler engine: policy is required")
	case opts.Logger == nil:
		return nil, errors.New("scheduler engine: logger is required")
	case opts.Metrics == nil:
		return nil, errors.New("scheduler engine: metrics are required")
	case opts.Consumer == "":
		return nil, errors.New("scheduler engine: consumer name is required")
	}
	return &Engine{
		br:    opts.Broker,
		pol:   opts.Policy,
		cfg:   opts.Config,
		log:   opts.Logger.With("component", "scheduler", "policy", opts.Policy.Name()),
		m:     opts.Metrics,
		name:  opts.Consumer,
		ready: make(chan struct{}),
		lease: opts.Lease,
	}, nil
}

// Ready reports whether the engine has finished starting up. Used by the
// readiness probe.
//
// A standby is ready. It is connected, campaigning, and one lease expiry away
// from serving, so reporting it unready would be wrong in two practical ways:
// Kubernetes would drop it from the Service and stop Prometheus scraping the
// very metrics that say who leads, and a Deployment whose spare replica never
// becomes ready cannot complete a rolling update.
func (e *Engine) Ready() bool {
	select {
	case <-e.ready:
		return true
	default:
		return false
	}
}

// IsLeader reports whether this engine currently holds the scheduling lease.
// An engine running without a lease always leads.
func (e *Engine) IsLeader() bool { return e.leader.Load() }

// Epoch returns the fencing token of the current term, or lease.NoEpoch.
func (e *Engine) Epoch() int64 { return e.epoch.Load() }

// Run starts the engine and blocks until ctx is cancelled.
//
// Without a lease it leads immediately, which is the original behaviour. With
// one it campaigns, leading only for as long as it holds the lease and standing
// by in between. Either way, cancellation stops ingest, lets the current
// dispatch pass finish, and flushes policy state so the next leader, in this
// process or another, resumes cleanly.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.br.EnsureStreams(ctx); err != nil {
		return err
	}

	if e.lease == nil {
		if err := e.restoreState(ctx); err != nil {
			return err
		}
		e.markReady()
		e.setLeadership(true, lease.NoEpoch)
		defer e.setLeadership(false, lease.NoEpoch)
		return e.serveLoops(ctx, lease.NoEpoch)
	}

	// Ready before the first campaign: a standby that cannot yet lead is still
	// a healthy process, and saying otherwise breaks rollouts.
	e.markReady()
	e.log.Info("campaigning for the scheduler lease",
		"consumer", e.name,
		"owner", e.lease.Owner(),
		"lease_ttl", e.lease.TTL(),
		"renew_interval", e.lease.RenewInterval())
	return e.lease.Campaign(ctx, e.leadTerm, e.setLeadership)
}

func (e *Engine) markReady() { e.closed.Do(func() { close(e.ready) }) }

// setLeadership records a leadership transition. It is the lease's observer, so
// it runs on every acquisition and every loss and must not block.
func (e *Engine) setLeadership(leader bool, epoch int64) {
	was := e.leader.Swap(leader)
	e.epoch.Store(epoch)
	e.m.SetLeader(leader, epoch)
	if was != leader {
		e.m.LeaderTransitions.Inc()
	}
}

// leadTerm runs one leadership term. Policy state is reloaded at the start of
// every term rather than once at process start, because a standby promoted an
// hour after it booted must adopt the virtual clock as the previous leader left
// it, not as it was when this process happened to start.
func (e *Engine) leadTerm(ctx context.Context, epoch int64) error {
	if err := e.restoreState(ctx); err != nil {
		return err
	}
	return e.serveLoops(ctx, epoch)
}

// serveLoops runs the three cooperating loops until ctx is cancelled or this
// engine is fenced out by a newer leader.
func (e *Engine) serveLoops(ctx context.Context, epoch int64) error {
	// A fence means some other process is already leading. Ending the term
	// promptly is the whole point, so the loops share a context that the first
	// one to be refused cancels.
	ctx, demote := context.WithCancel(ctx)
	defer demote()

	e.log.Info("scheduler started",
		"consumer", e.name,
		"epoch", epoch,
		"dispatch_batch", e.cfg.DispatchBatch,
		"max_in_flight", e.cfg.MaxInFlight,
		"namespace", e.br.Keys().Namespace)

	var fencedOut atomic.Bool
	onFenced := func(op string) {
		if !fencedOut.Swap(true) {
			e.m.FencedOperations.Inc()
			e.log.Warn("fenced out by a newer scheduler epoch, standing down",
				"epoch", epoch, "operation", op)
		}
		demote()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}
	run("ingest", func(ctx context.Context) error { return e.ingestLoop(ctx, epoch, onFenced) })
	run("reclaim", func(ctx context.Context) error { return e.reclaimIngressLoop(ctx, epoch, onFenced) })
	run("dispatch", func(ctx context.Context) error { return e.dispatchLoop(ctx, epoch, onFenced) })
	run("observe", func(ctx context.Context) error { return e.observeLoop(ctx, epoch, onFenced) })

	wg.Wait()
	close(errCh)

	// Persist final policy state on the way out, unless a newer leader has
	// already taken over: its state is the current one and this term's is
	// stale. The fence would refuse the write anyway; skipping it keeps the
	// shutdown path quiet rather than logging an error that is expected.
	if !fencedOut.Load() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := e.flushState(flushCtx, epoch); err != nil && !errors.Is(err, broker.ErrFenced) {
			e.log.Error("final scheduler state flush failed", "error", err)
		}
		cancel()
	}
	e.log.Info("scheduler stopped", "epoch", epoch)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (e *Engine) restoreState(ctx context.Context) error {
	state, err := e.br.LoadSchedulerState(ctx)
	if err != nil {
		return err
	}
	if len(state) == 0 {
		return nil
	}
	e.polMu.Lock()
	defer e.polMu.Unlock()
	if err := e.pol.Restore(state); err != nil {
		return fmt.Errorf("restore %s policy state: %w", e.pol.Name(), err)
	}
	e.log.Info("restored scheduler policy state", "entries", len(state))
	return nil
}

func (e *Engine) flushState(ctx context.Context, epoch int64) error {
	e.polMu.Lock()
	state := e.pol.State()
	e.polMu.Unlock()
	return e.br.SaveSchedulerState(ctx, state, epoch)
}

// ingestLoop moves ingress entries into the ranked pending set.
func (e *Engine) ingestLoop(ctx context.Context, epoch int64, onFenced func(string)) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := e.br.ReadIngress(ctx, e.name, e.cfg.IngestBatch, e.cfg.IngestBlock.D())
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.log.Error("ingress read failed", "error", err)
			if err := sleepCtx(ctx, e.cfg.IdleSleep.D()); err != nil {
				return nil
			}
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		if err := e.admitBatch(ctx, msgs, epoch); err != nil {
			if errors.Is(err, broker.ErrFenced) {
				onFenced("admission")
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			e.log.Error("admission failed", "error", err)
		}
	}
}

func (e *Engine) admitBatch(ctx context.Context, msgs []broker.IngressMessage, epoch int64) error {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.Task.ID
	}
	known, err := e.br.Known(ctx, ids)
	if err != nil {
		// Fall back to relying on the atomic admit guard alone.
		e.log.Warn("duplicate pre-check failed, relying on atomic admit", "error", err)
		known = map[string]bool{}
	}

	acks := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if known[m.Task.ID] {
			e.m.DuplicateAdmissions.Inc()
			acks = append(acks, m.ID)
			continue
		}
		if err := m.Task.Validate(); err != nil {
			e.log.Error("rejecting invalid task", "task_id", m.Task.ID, "error", err)
			acks = append(acks, m.ID)
			continue
		}

		e.polMu.Lock()
		key := e.pol.Rank(m.Task)
		e.polMu.Unlock()

		admitted, err := e.br.Admit(ctx, m.Task, key, epoch)
		if err != nil {
			// Leave the entry unacknowledged so it is retried later. When the
			// cause is a fence, "later" means the next leader's reclaim pass,
			// which is what stops a fenced batch from being lost.
			if ackErr := e.br.AckIngress(ctx, acks...); ackErr != nil {
				return errors.Join(err, ackErr)
			}
			return err
		}
		if admitted {
			e.m.TasksAdmitted.Inc()
		} else {
			e.m.DuplicateAdmissions.Inc()
		}
		acks = append(acks, m.ID)
	}
	return e.br.AckIngress(ctx, acks...)
}

// reclaimIngressLoop re-delivers ingress entries that a previous scheduler read
// and never acknowledged.
//
// It closes a gap that the pending set and the dead-letter stream both miss. An
// entry stranded in a dead consumer's pending list has been accepted from the
// producer and will never be offered to anyone again, because XREADGROUP with
// ">" only delivers entries nobody has seen. Nothing counts it: it is not
// pending, not in flight, not dead-lettered. The task simply never runs.
//
// This runs only while leading, because re-admitting is an admission and
// admissions are the leader's job. Re-admitting is safe regardless: the admit
// script refuses a task that is already known or already done, so an entry
// stranded after it was admitted is acknowledged and dropped rather than
// scheduled twice.
func (e *Engine) reclaimIngressLoop(ctx context.Context, epoch int64, onFenced func(string)) error {
	ticker := time.NewTicker(e.cfg.IngestReclaimInterval.D())
	defer ticker.Stop()

	// XAUTOCLAIM is a cursor over the pending list, so the scan position has to
	// persist across passes or a large backlog would never get past its first
	// page.
	cursor := "0-0"
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		msgs, next, err := e.br.AutoClaimIngress(ctx, e.name, e.cfg.IngestReclaimMinIdle.D(), cursor, e.cfg.IngestBatch)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.log.Error("ingress reclaim failed", "error", err)
			continue
		}
		// An empty cursor would make the next XAUTOCLAIM invalid, and "0-0"
		// is what Redis means by "start from the beginning" anyway.
		cursor = next
		if cursor == "" {
			cursor = "0-0"
		}
		if len(msgs) == 0 {
			continue
		}

		e.m.IngressReclaimed.Add(float64(len(msgs)))
		e.log.Warn("reclaimed ingress entries stranded by a previous scheduler",
			"count", len(msgs), "consumer", e.name)
		if err := e.admitBatch(ctx, msgs, epoch); err != nil {
			if errors.Is(err, broker.ErrFenced) {
				onFenced("reclaimed admission")
				return nil
			}
			if ctx.Err() == nil {
				e.log.Error("admitting reclaimed ingress entries failed", "error", err)
			}
		}
	}
}

// dispatchLoop repeatedly selects the highest ranked pending tasks and hands
// them to workers. It backs off when there is nothing to do so that it never
// becomes a busy loop.
func (e *Engine) dispatchLoop(ctx context.Context, epoch int64, onFenced func(string)) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		dispatched, err := e.br.Dispatch(ctx, e.cfg.DispatchBatch, e.cfg.MaxInFlight, time.Now(), epoch)
		if errors.Is(err, broker.ErrFenced) {
			onFenced("dispatch")
			return nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.m.DispatchErrors.Inc()
			e.log.Error("dispatch failed", "error", err)
			if err := sleepCtx(ctx, e.cfg.IdleSleep.D()); err != nil {
				return nil
			}
			continue
		}
		if len(dispatched) == 0 {
			if err := sleepCtx(ctx, e.cfg.IdleSleep.D()); err != nil {
				return nil
			}
			continue
		}
		e.polMu.Lock()
		for _, d := range dispatched {
			e.pol.Notify(d.Key)
		}
		e.polMu.Unlock()

		for _, d := range dispatched {
			e.m.TasksDispatched.WithLabelValues(d.Task.TenantID).Inc()
		}
		e.log.Debug("dispatched batch", "count", len(dispatched))
	}
}

// observeLoop refreshes gauges and periodically persists policy state.
func (e *Engine) observeLoop(ctx context.Context, epoch int64, onFenced func(string)) error {
	gaugeTicker := time.NewTicker(250 * time.Millisecond)
	defer gaugeTicker.Stop()
	flushTicker := time.NewTicker(e.cfg.StateFlushInterval.D())
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-gaugeTicker.C:
			e.refreshGauges(ctx)
		case <-flushTicker.C:
			err := e.flushState(ctx, epoch)
			if errors.Is(err, broker.ErrFenced) {
				onFenced("state flush")
				return nil
			}
			if err != nil && ctx.Err() == nil {
				e.log.Error("scheduler state flush failed", "error", err)
			}
		}
	}
}

func (e *Engine) refreshGauges(ctx context.Context) {
	pending, err := e.br.PendingCount(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Debug("pending gauge refresh failed", "error", err)
		}
		return
	}
	e.m.PendingTasks.Set(float64(pending))

	if inflight, err := e.br.InFlight(ctx); err == nil {
		e.m.InFlightTasks.Set(float64(inflight))
	}

	oldest, ok, err := e.br.OldestPendingSubmit(ctx)
	if err != nil {
		return
	}
	if !ok {
		e.m.LongestPendingWait.Set(0)
		return
	}
	e.m.ObserveWait(time.Since(oldest))
}

// sleepCtx sleeps for d unless ctx is cancelled first.
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
