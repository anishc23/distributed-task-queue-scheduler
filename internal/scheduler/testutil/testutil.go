// Package testutil builds deterministic tasks for scheduler tests.
package testutil

import (
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Base is a fixed reference instant so that every test builds identical tasks.
var Base = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// Task builds a task submitted submitMS milliseconds after Base.
func Task(id string, submitMS int64, opts ...Option) domain.Task {
	t := domain.Task{
		ID:          id,
		TenantID:    "A",
		SubmittedAt: Base.Add(time.Duration(submitMS) * time.Millisecond),
		ExecMillis:  100,
		Priority:    0,
		Deadline:    Base.Add(time.Duration(submitMS+1000) * time.Millisecond),
		MaxRetries:  3,
		AttemptID:   id + "#0",
	}
	for _, o := range opts {
		o(&t)
	}
	return t
}

// Option customises a generated task.
type Option func(*domain.Task)

// Tenant sets the tenant ID.
func Tenant(id string) Option { return func(t *domain.Task) { t.TenantID = id } }

// Priority sets the numeric priority.
func Priority(p int) Option { return func(t *domain.Task) { t.Priority = p } }

// Exec sets the simulated execution duration in milliseconds.
func Exec(ms int64) Option { return func(t *domain.Task) { t.ExecMillis = ms } }

// DeadlineAt sets the deadline deadlineMS milliseconds after Base.
func DeadlineAt(deadlineMS int64) Option {
	return func(t *domain.Task) {
		t.Deadline = Base.Add(time.Duration(deadlineMS) * time.Millisecond)
	}
}

// NoDeadline clears the deadline.
func NoDeadline() Option { return func(t *domain.Task) { t.Deadline = time.Time{} } }

// IDs extracts task IDs in order.
func IDs(tasks []domain.Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.ID
	}
	return out
}

// Tenants extracts tenant IDs in order.
func Tenants(tasks []domain.Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.TenantID
	}
	return out
}
