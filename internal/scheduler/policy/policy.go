// Package policy defines the scheduling abstraction shared by every scheduling
// algorithm and by the Redis-backed scheduling engine.
//
// A policy never removes tasks itself. It converts a task into an ordering Key,
// and the surrounding machinery (an in-memory heap in tests, a Redis sorted set
// in production) always dispatches the smallest Key first. Because both the
// in-memory queue and Redis order by (Score, Member) with identical semantics,
// the unit-tested ordering is exactly the ordering the deployed system uses.
package policy

import (
	"fmt"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// Key is the total ordering key of a pending task. The task with the smallest
// Key is dispatched first; Member breaks score ties deterministically.
//
// Member is always "<zero-padded submission unix millis>|<task id>", so a score
// tie is resolved by submission time and then by task ID. Redis sorted sets
// order equal-score members lexicographically by byte value, which matches Go
// string comparison, so the two implementations cannot diverge.
type Key struct {
	Score  float64
	Member string
}

// MemberFor builds the tie-break member for a task.
func MemberFor(t domain.Task) string {
	return fmt.Sprintf("%013d|%s", t.SubmittedAtMillis(), t.ID)
}

// TaskIDFromMember extracts the task ID from a Key member.
func TaskIDFromMember(member string) (string, error) {
	for i := 0; i < len(member); i++ {
		if member[i] == '|' {
			return member[i+1:], nil
		}
	}
	return "", fmt.Errorf("malformed pending member %q", member)
}

// Less reports whether k should be dispatched before other.
func (k Key) Less(other Key) bool {
	if k.Score != other.Score {
		return k.Score < other.Score
	}
	return k.Member < other.Member
}

// Policy is the pluggable scheduling algorithm. Implementations are selected by
// configuration only; producers and workers never know which one is active.
//
// Implementations are not safe for concurrent use. The scheduling engine calls
// them from a single goroutine.
type Policy interface {
	// Name is the stable identifier used in configuration and metrics.
	Name() string

	// Rank returns the ordering key for a task that is being admitted to the
	// pending set. Stateful policies (WFQ) may update their internal state.
	Rank(t domain.Task) Key

	// Notify is called after a previously ranked task has actually been
	// dispatched to a worker. Stateful policies use it to advance virtual time.
	Notify(k Key)

	// State returns the policy state that must survive a scheduler restart.
	// Stateless policies return nil.
	State() map[string]string

	// Restore reloads state previously returned by State. Restoring nil or an
	// empty map must leave the policy in its initial state.
	Restore(state map[string]string) error
}
