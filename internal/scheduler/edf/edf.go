// Package edf implements earliest-deadline-first scheduling.
package edf

import (
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
)

// Name is the configuration identifier for this policy.
const Name = policy.EDF

// NoDeadlineScore is the score given to tasks that carry no deadline so that
// they sort after every task that has one. It is far beyond any realistic Unix
// millisecond deadline (which is around 1.8e12) and still an exactly
// representable float64 integer.
const NoDeadlineScore = 1e15

// Policy dispatches the task with the earliest absolute deadline first, breaking
// ties by submission time and then by task ID.
//
// EDF is optimal for uniprocessor scheduling only when the system is not
// overloaded. Classical theory warns that under overload it can degrade badly,
// because it keeps preferring tasks that are already doomed and misses them
// anyway while delaying tasks that were still achievable.
//
// Measured behaviour in this system does not show that pathology. Across an
// offered load sweep from 0.5x to 3x capacity, EDF had the lowest deadline miss
// rate at every load level tested; its advantage narrowed from roughly 129x
// better than FIFO at 1.25x load to 1.16x at 3x, but never inverted. The
// classical failure needs deadlines that are tight relative to service time,
// whereas the deadlines here are generous and scale with each task's own
// duration, so a deferred long task usually still meets its deadline.
//
// What EDF does pay under overload is starvation: its longest queue wait was
// several times FIFO's, because it defers exactly the large tasks whose
// deadlines are furthest away. Treat the miss rate and the maximum wait as a
// pair; neither alone describes this policy.
type Policy struct{}

// New returns an EDF policy.
func New() *Policy { return &Policy{} }

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Rank orders by absolute deadline.
func (p *Policy) Rank(t domain.Task) policy.Key {
	score := NoDeadlineScore
	if ms := t.DeadlineMillis(); ms != 0 {
		score = float64(ms)
	}
	return policy.Key{Score: score, Member: policy.MemberFor(t)}
}

// Notify is a no-op: EDF is stateless.
func (p *Policy) Notify(policy.Key) {}

// State implements policy.Policy.
func (p *Policy) State() map[string]string { return nil }

// Restore implements policy.Policy.
func (p *Policy) Restore(map[string]string) error { return nil }
