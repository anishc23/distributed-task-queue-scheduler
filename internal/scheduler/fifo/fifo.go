// Package fifo implements first-in-first-out scheduling: the task submitted
// earliest is dispatched first.
package fifo

import (
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
)

// Name is the configuration identifier for this policy.
const Name = policy.FIFO

// Policy dispatches tasks in submission order. It is the experimental baseline:
// no priority, deadline or tenant awareness.
type Policy struct{}

// New returns a FIFO policy.
func New() *Policy { return &Policy{} }

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Rank orders purely by submission time. Tasks submitted in the same
// millisecond are ordered by task ID through the tie-break member.
func (p *Policy) Rank(t domain.Task) policy.Key {
	return policy.Key{
		Score:  float64(t.SubmittedAtMillis()),
		Member: policy.MemberFor(t),
	}
}

// Notify is a no-op: FIFO is stateless.
func (p *Policy) Notify(policy.Key) {}

// State implements policy.Policy. FIFO has no state to persist.
func (p *Policy) State() map[string]string { return nil }

// Restore implements policy.Policy.
func (p *Policy) Restore(map[string]string) error { return nil }
