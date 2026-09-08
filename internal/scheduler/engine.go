package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
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
}

// EngineOptions configures an Engine.
type EngineOptions struct {
	Broker   *broker.Broker
	Policy   policy.Policy
	Config   config.Scheduler
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
	Consumer string
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
	}, nil
}

// Ready reports whether the engine has finished starting up. Used by the
// readiness probe.
func (e *Engine) Ready() bool {
	select {
	case <-e.ready:
		return true
	default:
		return false
	}
}

// Run starts the engine and blocks until ctx is cancelled. On cancellation it
// stops consuming new ingress entries, lets the current dispatch pass finish,
// and flushes policy state so a restart resumes cleanly.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.br.EnsureStreams(ctx); err != nil {
		return err
	}
	if err := e.restoreState(ctx); err != nil {
		return err
	}
	e.closed.Do(func() { close(e.ready) })
	e.log.Info("scheduler started",
		"consumer", e.name,
		"dispatch_batch", e.cfg.DispatchBatch,
		"max_in_flight", e.cfg.MaxInFlight,
		"namespace", e.br.Keys().Namespace)

	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}
	run("ingest", e.ingestLoop)
	run("dispatch", e.dispatchLoop)
	run("observe", e.observeLoop)

	wg.Wait()
	close(errCh)

	// Persist final policy state on the way out.
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.flushState(flushCtx); err != nil {
		e.log.Error("final scheduler state flush failed", "error", err)
	}
	e.log.Info("scheduler stopped")

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

func (e *Engine) flushState(ctx context.Context) error {
	e.polMu.Lock()
	state := e.pol.State()
	e.polMu.Unlock()
	return e.br.SaveSchedulerState(ctx, state)
}

// ingestLoop moves ingress entries into the ranked pending set.
func (e *Engine) ingestLoop(ctx context.Context) error {
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
		if err := e.admitBatch(ctx, msgs); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			e.log.Error("admission failed", "error", err)
		}
	}
}

func (e *Engine) admitBatch(ctx context.Context, msgs []broker.IngressMessage) error {
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

		admitted, err := e.br.Admit(ctx, m.Task, key)
		if err != nil {
			// Leave the entry unacknowledged so it is retried later.
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

// dispatchLoop repeatedly selects the highest ranked pending tasks and hands
// them to workers. It backs off when there is nothing to do so that it never
// becomes a busy loop.
func (e *Engine) dispatchLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		dispatched, err := e.br.Dispatch(ctx, e.cfg.DispatchBatch, e.cfg.MaxInFlight, time.Now())
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
func (e *Engine) observeLoop(ctx context.Context) error {
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
			if err := e.flushState(ctx); err != nil && ctx.Err() == nil {
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
