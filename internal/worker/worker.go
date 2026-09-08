// Package worker executes tasks delivered on the worker execution stream.
//
// Execution is simulated: a worker sleeps for the task's declared duration
// instead of doing application work, so that scheduling behaviour can be
// measured without application noise. Everything else about the worker -
// consumer groups, acknowledgement, failure injection, retries and graceful
// shutdown - behaves exactly as it would with real work.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
)

// Worker is a pool of execution slots consuming one Redis Streams consumer
// group.
type Worker struct {
	br   *broker.Broker
	cfg  config.Worker
	rec  config.Recovery
	log  *slog.Logger
	m    *metrics.Metrics
	name string

	rngMu sync.Mutex
	rng   *rand.Rand

	busyNanos atomic.Int64
	failures  atomic.Int64
	started   time.Time
	ready     chan struct{}
	closeOnce sync.Once
}

// Options configures a worker.
type Options struct {
	Broker   *broker.Broker
	Config   config.Worker
	Recovery config.Recovery
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
}

// DefaultName derives a stable consumer name from the hostname and PID.
func DefaultName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

// New builds a worker.
func New(opts Options) (*Worker, error) {
	switch {
	case opts.Broker == nil:
		return nil, errors.New("worker: broker is required")
	case opts.Logger == nil:
		return nil, errors.New("worker: logger is required")
	case opts.Metrics == nil:
		return nil, errors.New("worker: metrics are required")
	}
	name := opts.Config.Name
	if name == "" {
		name = DefaultName()
	}
	seed := opts.Config.FailSeed
	if seed == 0 {
		// Derive a deterministic seed from the worker name so that injected
		// failures are reproducible for a given name.
		var h int64 = 1469598103934665603
		for _, c := range name {
			h = (h ^ int64(c)) * 1099511628211
		}
		seed = h
	}
	return &Worker{
		br:    opts.Broker,
		cfg:   opts.Config,
		rec:   opts.Recovery,
		log:   opts.Logger.With("component", "worker", "worker", name),
		m:     opts.Metrics,
		name:  name,
		rng:   rand.New(rand.NewSource(seed)),
		ready: make(chan struct{}),
	}, nil
}

// Name returns the worker's consumer name prefix.
func (w *Worker) Name() string { return w.name }

// Ready reports whether the worker has started consuming.
func (w *Worker) Ready() bool {
	select {
	case <-w.ready:
		return true
	default:
		return false
	}
}

// Run consumes and executes tasks until ctx is cancelled.
//
// Graceful shutdown: cancelling ctx stops new consumption immediately. Tasks
// already executing keep running on a detached context bounded by
// worker.shutdown_grace so they can finish and acknowledge. Anything still
// unfinished when the grace period expires is left unacknowledged, which is
// safe: the recovery loop redelivers it after the visibility timeout.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.br.EnsureStreams(ctx); err != nil {
		return err
	}
	w.started = time.Now()
	w.m.WorkerConcurrency.Set(float64(w.cfg.Concurrency))
	w.closeOnce.Do(func() { close(w.ready) })
	w.log.Info("worker started",
		"concurrency", w.cfg.Concurrency,
		"fail_rate", w.cfg.FailRate,
		"fail_before_ack_rate", w.cfg.FailBeforeAckRate,
		"namespace", w.br.Keys().Namespace)

	// execCtx outlives ctx by the shutdown grace period so in-flight tasks can
	// finish and acknowledge cleanly.
	execCtx, cancelExec := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelExec()
	go func() {
		<-ctx.Done()
		grace := w.cfg.ShutdownGrace.D()
		if grace <= 0 {
			cancelExec()
			return
		}
		t := time.NewTimer(grace)
		defer t.Stop()
		<-t.C
		cancelExec()
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, w.cfg.Concurrency+1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.utilizationLoop(ctx)
	}()

	for i := 0; i < w.cfg.Concurrency; i++ {
		consumer := fmt.Sprintf("%s-%d", w.name, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.slotLoop(ctx, execCtx, consumer); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("slot %s: %w", consumer, err)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	w.updateUtilization()
	w.log.Info("worker stopped",
		"busy_seconds", time.Duration(w.busyNanos.Load()).Seconds(),
		"uptime_seconds", time.Since(w.started).Seconds())

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// slotLoop is one execution slot: it reads a single task at a time so that
// unconsumed work stays in the scheduler's pending set rather than piling up in
// a worker-local buffer, which would defeat the point of central scheduling.
func (w *Worker) slotLoop(ctx, execCtx context.Context, consumer string) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := w.br.ReadExec(ctx, consumer, 1, w.cfg.Block.D())
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.log.Error("exec stream read failed", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		for _, msg := range msgs {
			if err := w.execute(execCtx, consumer, msg); err != nil {
				if execCtx.Err() != nil {
					return nil
				}
				w.log.Error("task execution failed", "task_id", msg.Task.ID, "error", err)
			}
		}
	}
}

// execute runs one delivered task and records its terminal state.
func (w *Worker) execute(ctx context.Context, consumer string, msg broker.ExecMessage) error {
	task := msg.Task
	started := time.Now()

	appFailure := w.roll(w.cfg.FailRate)
	abandon := w.roll(w.cfg.FailBeforeAckRate)

	w.simulateWork(ctx, task.ExecDuration())

	finished := time.Now()
	elapsed := finished.Sub(started)
	w.busyNanos.Add(int64(elapsed))
	w.m.WorkerBusySeconds.Add(elapsed.Seconds())
	w.m.ExecSeconds.Observe(elapsed.Seconds())

	// Simulated crash: the task ran but the worker dies before acknowledging.
	// The entry stays in the consumer group PEL and the recovery loop reclaims
	// it once the visibility timeout expires. This is the failure mode that
	// makes duplicate execution possible, which is why completion is idempotent.
	if abandon {
		w.m.TasksFailed.Inc()
		w.failures.Add(1)
		w.log.Warn("abandoning task before acknowledgement (injected failure)",
			"task_id", task.ID, "attempt_id", task.AttemptID)
		return nil
	}

	if appFailure {
		w.m.TasksFailed.Inc()
		w.failures.Add(1)
		budget := w.retryBudget(task.MaxRetries)
		outcome, next, err := w.br.RetryOrDeadLetter(ctx, task, budget, "worker reported an application error", msg.ID)
		if err != nil {
			return err
		}
		switch outcome {
		case broker.RetryRedelivered:
			w.m.TaskRetries.Inc()
			w.log.Warn("task failed, redelivered (injected failure)",
				"task_id", task.ID, "retry_count", next.RetryCount, "budget", budget)
		case broker.RetryDeadLettered:
			w.m.TasksDeadLettered.Inc()
			w.log.Warn("task failed, dead-lettered",
				"task_id", task.ID, "attempts", task.RetryCount+1, "budget", budget)
		}
		return nil
	}

	res := domain.Result{
		TaskID:           task.ID,
		AttemptID:        task.AttemptID,
		TenantID:         task.TenantID,
		Outcome:          domain.OutcomeCompleted,
		Priority:         task.Priority,
		SubmittedAt:      task.SubmittedAt,
		DispatchedAt:     msg.DispatchedAt,
		StartedAt:        started,
		FinishedAt:       finished,
		Deadline:         task.Deadline,
		ExecMillis:       task.ExecMillis,
		ActualExecMillis: elapsed.Milliseconds(),
		RetryCount:       task.RetryCount,
		Worker:           consumer,
	}

	recorded, err := w.br.Complete(ctx, res, msg.ID)
	if err != nil {
		return err
	}
	if !recorded {
		// Duplicate delivery of an already completed task. The idempotency
		// guard suppressed the second result record; the delivery is still
		// acknowledged inside the same atomic script.
		w.m.DuplicateCompletions.Inc()
		w.log.Info("suppressed duplicate completion", "task_id", task.ID, "attempt_id", task.AttemptID)
		return nil
	}

	w.m.TasksCompleted.WithLabelValues(task.TenantID).Inc()
	w.m.TenantCompletedTasks.WithLabelValues(task.TenantID).Inc()
	w.m.TenantServiceSeconds.WithLabelValues(task.TenantID).Add(elapsed.Seconds())
	w.m.QueueWaitSeconds.Observe(res.QueueWait().Seconds())
	w.m.LatencySeconds.Observe(res.Latency().Seconds())
	if res.MissedDeadline() {
		w.m.DeadlineMissed.WithLabelValues(task.TenantID).Inc()
	}
	return nil
}

// simulateWork sleeps for the task's declared duration. It returns early if the
// execution context is cancelled, which only happens after the shutdown grace
// period has elapsed.
func (w *Worker) simulateWork(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (w *Worker) retryBudget(taskMaxRetries int) int {
	if taskMaxRetries > 0 {
		return taskMaxRetries
	}
	return w.rec.MaxRetries
}

// roll draws a Bernoulli sample for failure injection.
func (w *Worker) roll(p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return w.rng.Float64() < p
}

// utilizationLoop maintains the slot-seconds counters that make worker
// utilisation computable as busy_seconds / slot_seconds.
func (w *Worker) utilizationLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			w.m.WorkerUpSeconds.Add(time.Since(last).Seconds() * float64(w.cfg.Concurrency))
			w.updateUtilization()
			return
		case now := <-ticker.C:
			w.m.WorkerUpSeconds.Add(now.Sub(last).Seconds() * float64(w.cfg.Concurrency))
			last = now
			w.updateUtilization()
		}
	}
}

func (w *Worker) updateUtilization() {
	uptime := time.Since(w.started).Seconds()
	if uptime <= 0 {
		return
	}
	available := uptime * float64(w.cfg.Concurrency)
	busy := time.Duration(w.busyNanos.Load()).Seconds()
	w.m.WorkerUtilization.Set(busy / available)
}

// Failures returns how many attempts failed or were abandoned, which the
// benchmark harness records per experiment.
func (w *Worker) Failures() int64 { return w.failures.Load() }

// BusyTime returns cumulative execution time across all slots.
func (w *Worker) BusyTime() time.Duration { return time.Duration(w.busyNanos.Load()) }

// Uptime returns how long the worker has been running.
func (w *Worker) Uptime() time.Duration {
	if w.started.IsZero() {
		return 0
	}
	return time.Since(w.started)
}
