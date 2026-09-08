// Package recovery redelivers work that a worker took but never acknowledged.
//
// Redis Streams consumer groups keep an entry in the group's pending entries
// list (PEL) from the moment it is delivered until it is acknowledged. If a
// worker crashes, is killed, or is deliberately configured to abandon work
// before acknowledging, the entry stays in the PEL and simply gets older. This
// loop uses XAUTOCLAIM to reclaim entries whose idle time exceeds the
// visibility timeout and to hand them back for another attempt.
//
// Delivery semantics: at-least-once. A reclaimed task may have already run to
// completion on the crashed worker, so a retry can execute the same task body
// twice. Duplicate execution is made harmless by the idempotent completion
// guard, which records a task's terminal result exactly once. This system does
// not provide exactly-once delivery and does not claim to.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
)

// Loop reclaims and retries timed-out worker deliveries.
type Loop struct {
	br       *broker.Broker
	cfg      config.Recovery
	log      *slog.Logger
	m        *metrics.Metrics
	consumer string
	cursor   string
}

// Options configures a recovery loop.
type Options struct {
	Broker   *broker.Broker
	Config   config.Recovery
	Logger   *slog.Logger
	Metrics  *metrics.Metrics
	Consumer string
}

// New builds a recovery loop.
func New(opts Options) (*Loop, error) {
	switch {
	case opts.Broker == nil:
		return nil, errors.New("recovery: broker is required")
	case opts.Logger == nil:
		return nil, errors.New("recovery: logger is required")
	case opts.Metrics == nil:
		return nil, errors.New("recovery: metrics are required")
	case opts.Consumer == "":
		return nil, errors.New("recovery: consumer name is required")
	}
	return &Loop{
		br:       opts.Broker,
		cfg:      opts.Config,
		log:      opts.Logger.With("component", "recovery"),
		m:        opts.Metrics,
		consumer: opts.Consumer,
		cursor:   "0-0",
	}, nil
}

// Budget returns the effective retry budget for a task.
//
// A task carries its own max_retries. A value of zero is treated as "unset" and
// falls back to recovery.max_retries; set both to zero to disable retries.
func (l *Loop) Budget(taskMaxRetries int) int {
	if taskMaxRetries > 0 {
		return taskMaxRetries
	}
	return l.cfg.MaxRetries
}

// Run reclaims timed-out deliveries until ctx is cancelled.
func (l *Loop) Run(ctx context.Context) error {
	if !l.cfg.Enabled {
		l.log.Info("recovery loop disabled by configuration")
		<-ctx.Done()
		return nil
	}
	l.log.Info("recovery loop started",
		"interval", l.cfg.Interval.String(),
		"visibility_timeout", l.cfg.MinIdle.String(),
		"batch", l.cfg.Batch)

	ticker := time.NewTicker(l.cfg.Interval.D())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			l.log.Info("recovery loop stopped")
			return nil
		case <-ticker.C:
			if n, err := l.Once(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				l.log.Error("recovery pass failed", "error", err)
			} else if n > 0 {
				l.log.Info("recovery pass reclaimed deliveries", "count", n)
			}
		}
	}
}

// Once performs a single reclaim pass and returns how many entries it handled.
// It is exported so that tests and the benchmark harness can drive recovery
// deterministically instead of waiting for a tick.
func (l *Loop) Once(ctx context.Context) (int, error) {
	msgs, next, err := l.br.AutoClaim(ctx, l.consumer, l.cfg.MinIdle.D(), l.cursor, l.cfg.Batch)
	if err != nil {
		return 0, err
	}
	l.cursor = next
	if next == "" {
		l.cursor = "0-0"
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	handled := 0
	for _, msg := range msgs {
		l.m.RecoveryReclaimed.Inc()
		budget := l.Budget(msg.Task.MaxRetries)
		reason := fmt.Sprintf("no acknowledgement within visibility timeout %s", l.cfg.MinIdle)
		outcome, next, err := l.br.RetryOrDeadLetter(ctx, msg.Task, budget, reason, msg.ID)
		if err != nil {
			return handled, err
		}
		handled++
		switch outcome {
		case broker.RetryRedelivered:
			l.m.TaskRetries.Inc()
			l.log.Debug("task redelivered",
				"task_id", msg.Task.ID, "retry_count", next.RetryCount, "budget", budget)
		case broker.RetryDeadLettered:
			l.m.TasksDeadLettered.Inc()
			l.log.Warn("task dead-lettered",
				"task_id", msg.Task.ID, "attempts", msg.Task.RetryCount+1, "budget", budget)
		case broker.RetryAlreadyTerminal:
			l.log.Debug("reclaimed delivery for an already terminal task",
				"task_id", msg.Task.ID)
		}
	}
	return handled, nil
}
