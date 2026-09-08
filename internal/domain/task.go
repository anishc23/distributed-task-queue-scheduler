// Package domain contains the core data types exchanged between the producer,
// the scheduler and the workers. Everything in this package is transport
// agnostic and JSON serialisable.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Task is the unit of work that flows through the system.
//
// All timestamps are absolute wall-clock times. All durations are expressed in
// milliseconds and named with an explicit "_ms" suffix on the wire so the unit
// is never ambiguous.
type Task struct {
	// ID uniquely identifies the logical task. It is stable across retries and
	// is the key used for idempotent completion.
	ID string `json:"id"`
	// TenantID identifies the submitting tenant. Used by WFQ and by fairness
	// accounting. Kept to a small bounded set so it is safe as a metric label.
	TenantID string `json:"tenant_id"`
	// SubmittedAt is the wall-clock time at which the producer created the task.
	SubmittedAt time.Time `json:"submitted_at"`
	// ExecMillis is the simulated execution duration in milliseconds. Workers
	// sleep for this long instead of doing real work so that scheduler
	// behaviour can be studied without application noise.
	ExecMillis int64 `json:"exec_ms"`
	// Priority is a numeric priority. Higher values are more important.
	Priority int `json:"priority"`
	// Deadline is the absolute wall-clock time by which the task should have
	// completed. Completing after this instant counts as a deadline miss.
	Deadline time.Time `json:"deadline"`
	// RetryCount is the number of times this task has already been redelivered
	// after a failed or lost attempt. The first delivery has RetryCount 0.
	RetryCount int `json:"retry_count"`
	// MaxRetries is the maximum number of retries before the task is moved to
	// the dead-letter stream.
	MaxRetries int `json:"max_retries"`
	// AttemptID uniquely identifies one delivery attempt of the task. It
	// changes on every retry while ID stays the same.
	AttemptID string `json:"attempt_id"`
}

// MaxPriority is the largest priority value the system accepts. The bound
// exists so that priority can be packed into a Redis sorted-set score without
// losing floating point precision.
const MaxPriority = 255

// ErrInvalidTask is returned when a task fails validation.
var ErrInvalidTask = errors.New("invalid task")

// Validate reports whether the task is structurally usable.
func (t Task) Validate() error {
	switch {
	case t.ID == "":
		return fmt.Errorf("%w: empty id", ErrInvalidTask)
	case t.TenantID == "":
		return fmt.Errorf("%w: task %s has empty tenant_id", ErrInvalidTask, t.ID)
	case t.SubmittedAt.IsZero():
		return fmt.Errorf("%w: task %s has zero submitted_at", ErrInvalidTask, t.ID)
	case t.ExecMillis < 0:
		return fmt.Errorf("%w: task %s has negative exec_ms", ErrInvalidTask, t.ID)
	case t.Priority < 0 || t.Priority > MaxPriority:
		return fmt.Errorf("%w: task %s priority %d outside [0,%d]", ErrInvalidTask, t.ID, t.Priority, MaxPriority)
	case t.MaxRetries < 0:
		return fmt.Errorf("%w: task %s has negative max_retries", ErrInvalidTask, t.ID)
	case t.RetryCount < 0:
		return fmt.Errorf("%w: task %s has negative retry_count", ErrInvalidTask, t.ID)
	}
	return nil
}

// ExecDuration returns the simulated execution duration as a time.Duration.
func (t Task) ExecDuration() time.Duration {
	return time.Duration(t.ExecMillis) * time.Millisecond
}

// SubmittedAtMillis returns the submission time as Unix milliseconds.
func (t Task) SubmittedAtMillis() int64 { return t.SubmittedAt.UnixMilli() }

// DeadlineMillis returns the deadline as Unix milliseconds. A zero deadline is
// reported as zero, meaning "no deadline".
func (t Task) DeadlineMillis() int64 {
	if t.Deadline.IsZero() {
		return 0
	}
	return t.Deadline.UnixMilli()
}

// MarshalJSONString serialises the task to a JSON string.
func (t Task) MarshalJSONString() (string, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("marshal task %s: %w", t.ID, err)
	}
	return string(b), nil
}

// UnmarshalTask parses a JSON encoded task.
func UnmarshalTask(payload string) (Task, error) {
	var t Task
	if err := json.Unmarshal([]byte(payload), &t); err != nil {
		return Task{}, fmt.Errorf("unmarshal task: %w", err)
	}
	return t, nil
}
