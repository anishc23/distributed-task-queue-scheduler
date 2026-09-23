package broker

import (
	"fmt"

	"github.com/anishc23/distributed-task-queue/internal/config"
)

// Keys holds every Redis key the system uses. All keys derive from a single
// configurable namespace so that independent experiments can share one Redis
// server without ever mixing state.
type Keys struct {
	Namespace string

	// Streams.
	Ingress    string
	Exec       string
	Results    string
	DeadLetter string

	// Consumer groups.
	SchedulerGroup string
	WorkerGroup    string

	// Scheduler state.
	Pending    string // ZSET member -> policy score (dispatch order)
	PendingAge string // ZSET member -> submission unix millis (queue age)
	Payloads   string // HASH task id -> task JSON
	Done       string // HASH task id -> completion unix millis (idempotency marker)
	SchedState string // HASH policy state (e.g. WFQ virtual time)

	// Leader election.
	Leader      string // STRING "<owner>|<epoch>" with a TTL; holding it is leadership
	LeaderEpoch string // STRING monotonic counter, one INCR per leadership grant
	Fence       string // STRING highest epoch that has written; rejects older ones

	// Accounting.
	Stats         string // HASH counter name -> value
	TenantService string // HASH tenant -> completed service millis
	TenantCount   string // HASH tenant -> completed task count
	InFlight      string // STRING dispatched-but-not-terminal counter
}

// Stats counter field names stored in the Stats hash.
const (
	StatAdmitted             = "admitted"
	StatDispatched           = "dispatched"
	StatCompleted            = "completed"
	StatFailed               = "failed"
	StatRetried              = "retried"
	StatDeadLettered         = "dead_lettered"
	StatDeadlineMissed       = "deadline_missed"
	StatDuplicateCompletions = "duplicate_completions"
)

// NewKeys builds the key set for a stream configuration.
func NewKeys(s config.Streams) Keys {
	p := func(parts ...string) string {
		out := s.Namespace
		for _, part := range parts {
			out += ":" + part
		}
		return out
	}
	return Keys{
		Namespace:      s.Namespace,
		Ingress:        p("stream", s.IngressStream),
		Exec:           p("stream", s.ExecStream),
		Results:        p("stream", s.ResultsStream),
		DeadLetter:     p("stream", s.DeadLetterStream),
		SchedulerGroup: s.SchedulerGroup,
		WorkerGroup:    s.WorkerGroup,
		Pending:        p("pending"),
		PendingAge:     p("pending_age"),
		Payloads:       p("payloads"),
		Done:           p("done"),
		SchedState:     p("sched_state"),
		Leader:         p("leader"),
		LeaderEpoch:    p("leader_epoch"),
		Fence:          p("fence"),
		Stats:          p("stats"),
		TenantService:  p("tenant_service"),
		TenantCount:    p("tenant_count"),
		InFlight:       p("inflight"),
	}
}

// All returns every key Reset deletes.
//
// LeaderEpoch and Fence are deliberately absent, and it is the one place this
// package keeps state across a reset.
//
// Both are monotonic counters whose correctness depends on never going
// backwards. Reset clears the lease itself, so a standby can take over
// immediately, and the new leader is handed a strictly larger epoch than the
// one any surviving process is carrying — which is precisely what fences the
// old leader out. Clearing the counter would reissue an epoch a live process
// may still hold; clearing the fence would reopen the window the fence exists
// to close. Between them they are two small integers, so there is nothing to
// reclaim by deleting them.
//
// This does not leak across experiments. The benchmark harness runs with
// election disabled, where the guard is inert, and every experiment uses its
// own namespace regardless.
func (k Keys) All() []string {
	return []string{
		k.Ingress, k.Exec, k.Results, k.DeadLetter,
		k.Pending, k.PendingAge, k.Payloads, k.Done, k.SchedState,
		k.Stats, k.TenantService, k.TenantCount, k.InFlight,
		k.Leader,
	}
}

// String renders the namespace for logs.
func (k Keys) String() string { return fmt.Sprintf("namespace=%s", k.Namespace) }
