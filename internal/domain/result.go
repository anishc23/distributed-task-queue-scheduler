package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// Outcome describes how a task attempt finished.
type Outcome string

const (
	// OutcomeCompleted means the worker executed the task and acknowledged it.
	OutcomeCompleted Outcome = "completed"
	// OutcomeFailed means the worker executed the task and reported an error.
	// The task is eligible for retry.
	OutcomeFailed Outcome = "failed"
	// OutcomeDeadLettered means the task exhausted its retry budget.
	OutcomeDeadLettered Outcome = "dead_lettered"
)

// Result is the record written to the results stream when a task reaches a
// terminal state. It carries everything the benchmark harness needs so that
// analysis never has to join against Prometheus.
type Result struct {
	TaskID       string    `json:"task_id"`
	AttemptID    string    `json:"attempt_id"`
	TenantID     string    `json:"tenant_id"`
	Outcome      Outcome   `json:"outcome"`
	Priority     int       `json:"priority"`
	SubmittedAt  time.Time `json:"submitted_at"`
	DispatchedAt time.Time `json:"dispatched_at"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Deadline     time.Time `json:"deadline"`
	// ExecMillis is the requested simulated duration.
	ExecMillis int64 `json:"exec_ms"`
	// ActualExecMillis is the measured time the worker spent on the task.
	ActualExecMillis int64  `json:"actual_exec_ms"`
	RetryCount       int    `json:"retry_count"`
	Worker           string `json:"worker"`
	Error            string `json:"error,omitempty"`
}

// QueueWait is the time between submission and the worker starting execution.
func (r Result) QueueWait() time.Duration { return r.StartedAt.Sub(r.SubmittedAt) }

// Latency is the end-to-end time from submission to terminal completion.
func (r Result) Latency() time.Duration { return r.FinishedAt.Sub(r.SubmittedAt) }

// MissedDeadline reports whether the task finished after its deadline. Tasks
// without a deadline never miss.
func (r Result) MissedDeadline() bool {
	if r.Deadline.IsZero() {
		return false
	}
	return r.FinishedAt.After(r.Deadline)
}

// MarshalJSONString serialises the result to a JSON string.
func (r Result) MarshalJSONString() (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("marshal result %s: %w", r.TaskID, err)
	}
	return string(b), nil
}

// UnmarshalResult parses a JSON encoded result.
func UnmarshalResult(payload string) (Result, error) {
	var r Result
	if err := json.Unmarshal([]byte(payload), &r); err != nil {
		return Result{}, fmt.Errorf("unmarshal result: %w", err)
	}
	return r, nil
}

// DeadLetter is the record written to the dead-letter stream.
type DeadLetter struct {
	Task        Task      `json:"task"`
	Reason      string    `json:"reason"`
	Attempts    int       `json:"attempts"`
	LastAttempt string    `json:"last_attempt_id"`
	FailedAt    time.Time `json:"failed_at"`
}

// MarshalJSONString serialises the dead-letter record to a JSON string.
func (d DeadLetter) MarshalJSONString() (string, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("marshal dead letter %s: %w", d.Task.ID, err)
	}
	return string(b), nil
}

// UnmarshalDeadLetter parses a JSON encoded dead-letter record.
func UnmarshalDeadLetter(payload string) (DeadLetter, error) {
	var d DeadLetter
	if err := json.Unmarshal([]byte(payload), &d); err != nil {
		return DeadLetter{}, fmt.Errorf("unmarshal dead letter: %w", err)
	}
	return d, nil
}
