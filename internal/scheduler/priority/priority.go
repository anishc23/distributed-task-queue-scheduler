// Package priority implements strict priority scheduling: the highest numeric
// priority is dispatched first.
package priority

import (
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
)

// Name is the configuration identifier for this policy.
const Name = policy.Priority

// bandWidth separates priority bands inside a single float64 score.
//
// A score is  (MaxPriority-priority)*bandWidth + submittedAtMillis.  Unix
// milliseconds are below 1e13 until the year 2286, so a submission time can
// never leak into the next priority band. The largest score is
// 255*1e13 + ~1.8e12 which is well inside the 2^53 range where float64
// represents integers exactly, so scores are exact and comparisons are stable.
const bandWidth = 1e13

// Policy dispatches higher priorities first, breaking ties by submission time
// and then by task ID.
//
// Strict priority is deliberately starvation-prone: a sustained stream of
// high-priority work can hold low-priority tasks back indefinitely. The
// benchmark harness measures that effect rather than hiding it behind ageing.
type Policy struct{}

// New returns a strict priority policy.
func New() *Policy { return &Policy{} }

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Rank packs priority and submission time into a single ordering score.
func (p *Policy) Rank(t domain.Task) policy.Key {
	prio := t.Priority
	if prio < 0 {
		prio = 0
	}
	if prio > domain.MaxPriority {
		prio = domain.MaxPriority
	}
	band := float64(domain.MaxPriority - prio)
	return policy.Key{
		Score:  band*bandWidth + float64(t.SubmittedAtMillis()),
		Member: policy.MemberFor(t),
	}
}

// Notify is a no-op: strict priority is stateless.
func (p *Policy) Notify(policy.Key) {}

// State implements policy.Policy.
func (p *Policy) State() map[string]string { return nil }

// Restore implements policy.Policy.
func (p *Policy) Restore(map[string]string) error { return nil }
