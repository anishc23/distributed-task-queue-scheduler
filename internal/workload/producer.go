package workload

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Producer replays a generated plan onto the ingress stream, pacing submissions
// according to each spec's arrival offset.
type Producer struct {
	br    *broker.Broker
	log   *slog.Logger
	batch int
}

// NewProducer builds a producer. batch bounds how many due tasks are pipelined
// into a single round trip.
func NewProducer(br *broker.Broker, log *slog.Logger, batch int) *Producer {
	if batch <= 0 {
		batch = 64
	}
	return &Producer{br: br, log: log.With("component", "producer"), batch: batch}
}

// SubmitStats reports how faithfully the plan was replayed.
type SubmitStats struct {
	// Submitted is the number of tasks appended to the ingress stream.
	Submitted int
	// Start is when replay began.
	Start time.Time
	// End is when the last task was submitted.
	End time.Time
	// MaxLag is the largest gap between a task's planned arrival and its actual
	// submission. A large value means the load generator, not the queue, was
	// the bottleneck, which is a reproducibility caveat worth reporting.
	MaxLag time.Duration
	// MeanLag is the average planned-to-actual gap.
	MeanLag time.Duration
}

// knownChunk bounds how many task IDs are checked per round trip.
const knownChunk = 1000

// CheckCollisions reports how many of the planned task IDs the broker has
// already seen, either as pending work or as a completed task.
//
// Task IDs are a deterministic function of the workload seed and id_prefix,
// which is what makes a run reproducible. The flip side is that submitting the
// same seed and prefix twice regenerates identical IDs, and the scheduler's
// admission guard correctly rejects the repeats as duplicates. That is safe but
// surprising, so the producer checks up front and says so plainly.
func (p *Producer) CheckCollisions(ctx context.Context, specs []Spec) (int, error) {
	collisions := 0
	for start := 0; start < len(specs); start += knownChunk {
		end := start + knownChunk
		if end > len(specs) {
			end = len(specs)
		}
		ids := make([]string, 0, end-start)
		for _, s := range specs[start:end] {
			ids = append(ids, s.ID)
		}
		known, err := p.br.Known(ctx, ids)
		if err != nil {
			return collisions, err
		}
		collisions += len(known)
	}
	return collisions, nil
}

// WarnOnCollisions logs an actionable warning when some or all of the planned
// task IDs already exist. It never blocks submission: re-submitting is a
// legitimate way to exercise the idempotency guard.
func (p *Producer) WarnOnCollisions(ctx context.Context, specs []Spec, seed int64, prefix string) {
	collisions, err := p.CheckCollisions(ctx, specs)
	if err != nil {
		p.log.Debug("could not check for existing task IDs", "error", err)
		return
	}
	if collisions == 0 {
		return
	}
	msg := "some task IDs already exist and will be rejected as duplicates"
	if collisions == len(specs) {
		msg = "every task ID already exists; the scheduler will reject all of them as duplicates"
	}
	p.log.Warn(msg,
		"colliding", collisions,
		"planned", len(specs),
		"seed", seed,
		"id_prefix", prefix,
		"why", "task IDs are a deterministic function of the seed and id_prefix, which is what makes runs reproducible",
		"fix", "pass a different --id-prefix or --seed, or clear the namespace with `tqctl reset --yes`")
}

// Submit replays specs onto the ingress stream and blocks until all of them
// have been submitted or ctx is cancelled.
func (p *Producer) Submit(ctx context.Context, specs []Spec) (SubmitStats, error) {
	stats := SubmitStats{Start: time.Now()}
	if len(specs) == 0 {
		stats.End = stats.Start
		return stats, nil
	}

	var totalLag time.Duration
	batch := make([]domain.Task, 0, p.batch)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := p.br.SubmitBatch(ctx, batch); err != nil {
			return err
		}
		stats.Submitted += len(batch)
		batch = batch[:0]
		return nil
	}

	for i := 0; i < len(specs); i++ {
		due := stats.Start.Add(specs[i].Offset)
		if wait := time.Until(due); wait > 0 {
			// Flush what is already due before sleeping so pacing stays honest.
			if err := flush(); err != nil {
				return stats, err
			}
			if err := sleepCtx(ctx, wait); err != nil {
				return stats, ctx.Err()
			}
		}
		now := time.Now()
		lag := now.Sub(due)
		if lag < 0 {
			lag = 0
		}
		if lag > stats.MaxLag {
			stats.MaxLag = lag
		}
		totalLag += lag

		batch = append(batch, specs[i].TaskAt(now))
		if len(batch) >= p.batch {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}

	stats.End = time.Now()
	stats.MeanLag = totalLag / time.Duration(len(specs))
	p.log.Info("workload submitted",
		"tasks", stats.Submitted,
		"span", stats.End.Sub(stats.Start).String(),
		"max_lag", stats.MaxLag.String(),
		"mean_lag", stats.MeanLag.String())
	return stats, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("submission interrupted: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}
