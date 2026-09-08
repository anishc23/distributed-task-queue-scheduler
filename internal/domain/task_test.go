package domain_test

import (
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

func sample() domain.Task {
	submitted := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	return domain.Task{
		ID:          "task-1",
		TenantID:    "A",
		SubmittedAt: submitted,
		ExecMillis:  250,
		Priority:    3,
		Deadline:    submitted.Add(2 * time.Second),
		RetryCount:  1,
		MaxRetries:  3,
		AttemptID:   "task-1#1",
	}
}

func TestTaskJSONRoundTrip(t *testing.T) {
	orig := sample()
	payload, err := orig.MarshalJSONString()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back, err := domain.UnmarshalTask(payload)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !back.SubmittedAt.Equal(orig.SubmittedAt) || !back.Deadline.Equal(orig.Deadline) {
		t.Fatalf("timestamps did not survive the round trip: %+v", back)
	}
	back.SubmittedAt, back.Deadline = orig.SubmittedAt, orig.Deadline
	if back != orig {
		t.Fatalf("round trip changed the task:\n got %+v\nwant %+v", back, orig)
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	if _, err := domain.UnmarshalTask("{not json"); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
	if _, err := domain.UnmarshalResult("{not json"); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
	if _, err := domain.UnmarshalDeadLetter("{not json"); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]func(*domain.Task){
		"empty id":         func(t *domain.Task) { t.ID = "" },
		"empty tenant":     func(t *domain.Task) { t.TenantID = "" },
		"zero submitted":   func(t *domain.Task) { t.SubmittedAt = time.Time{} },
		"negative exec":    func(t *domain.Task) { t.ExecMillis = -1 },
		"priority too big": func(t *domain.Task) { t.Priority = domain.MaxPriority + 1 },
		"negative retries": func(t *domain.Task) { t.MaxRetries = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			task := sample()
			mutate(&task)
			if err := task.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
	if err := sample().Validate(); err != nil {
		t.Fatalf("a well-formed task must validate: %v", err)
	}
}

func TestTimeHelpers(t *testing.T) {
	task := sample()
	if got := task.ExecDuration(); got != 250*time.Millisecond {
		t.Fatalf("exec duration = %s, want 250ms", got)
	}
	if task.DeadlineMillis() == 0 {
		t.Fatal("deadline millis should be non-zero")
	}
	task.Deadline = time.Time{}
	if task.DeadlineMillis() != 0 {
		t.Fatal("a zero deadline must report zero millis")
	}
}

func TestResultDerivedMetrics(t *testing.T) {
	submitted := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	r := domain.Result{
		SubmittedAt: submitted,
		StartedAt:   submitted.Add(400 * time.Millisecond),
		FinishedAt:  submitted.Add(900 * time.Millisecond),
		Deadline:    submitted.Add(time.Second),
	}
	if got := r.QueueWait(); got != 400*time.Millisecond {
		t.Fatalf("queue wait = %s, want 400ms", got)
	}
	if got := r.Latency(); got != 900*time.Millisecond {
		t.Fatalf("latency = %s, want 900ms", got)
	}
	if r.MissedDeadline() {
		t.Fatal("finishing before the deadline is not a miss")
	}
	r.FinishedAt = submitted.Add(1500 * time.Millisecond)
	if !r.MissedDeadline() {
		t.Fatal("finishing after the deadline is a miss")
	}
}

func TestDeadLetterRoundTrip(t *testing.T) {
	d := domain.DeadLetter{
		Task:        sample(),
		Reason:      "visibility timeout",
		Attempts:    4,
		LastAttempt: "task-1#3",
		FailedAt:    time.Date(2024, 5, 1, 12, 0, 5, 0, time.UTC),
	}
	payload, err := d.MarshalJSONString()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back, err := domain.UnmarshalDeadLetter(payload)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Reason != d.Reason || back.Attempts != d.Attempts || back.Task.ID != d.Task.ID {
		t.Fatalf("round trip lost data: %+v", back)
	}
}
