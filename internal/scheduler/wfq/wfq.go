// Package wfq implements per-tenant weighted fair queuing using self-clocked
// virtual finish times.
package wfq

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
)

// Name is the configuration identifier for this policy.
const Name = policy.WFQ

const (
	stateVirtualTime = "vtime"
	stateFinishPfx   = "lf:"
)

// Policy implements weighted fair queuing over tenants.
//
// Algorithm (self-clocked fair queuing, SCFQ):
//
//	On admission of task t belonging to tenant k with weight w_k:
//	    start_t  = max(virtualTime, lastFinish[k])
//	    finish_t = start_t + cost(t) / w_k
//	    lastFinish[k] = finish_t
//	    order t by finish_t
//
//	On dispatch of a task with virtual finish f:
//	    virtualTime = max(virtualTime, f)
//
// cost(t) is the task's simulated execution time in milliseconds. Dividing by
// the tenant weight means a tenant with weight 5 advances its virtual clock
// five times more slowly per unit of work than a tenant with weight 1, so it
// receives roughly five times the service. Because a newly active tenant starts
// at max(virtualTime, lastFinish), an idle tenant cannot accumulate credit and
// then flood the queue, and a backlogged tenant cannot run ahead of virtual
// time: this is what stops a dominant tenant from consuming all capacity.
//
// Documented approximations:
//
//  1. Virtual time is advanced by dispatch events (SCFQ) rather than by
//     emulating a GPS fluid server. SCFQ is cheaper and needs no per-packet
//     simulation, at the cost of a larger worst-case delay bound than WF2Q.
//  2. Scheduling is non-preemptive and work-conserving at whole-task
//     granularity: once a task is dispatched it runs to completion, so fairness
//     is only approximate over horizons shorter than the largest task.
//  3. cost(t) uses the task's declared simulated duration, which this system
//     knows exactly. A production scheduler would have to estimate it; that
//     estimation error is out of scope here and is stated as a caveat in the
//     experiment methodology.
//  4. Ordering is over the whole pending set rather than per-tenant FIFO
//     sub-queues. Since virtual finish times within one tenant are
//     monotonically increasing by construction, per-tenant FIFO order is
//     preserved anyway.
type Policy struct {
	weights       map[string]float64
	defaultWeight float64
	virtualTime   float64
	lastFinish    map[string]float64
}

// Options configures a WFQ policy.
type Options struct {
	// Weights maps tenant ID to relative weight. Must be positive.
	Weights map[string]float64
	// DefaultWeight applies to tenants absent from Weights. Defaults to 1.
	DefaultWeight float64
}

// New returns a WFQ policy. It fails if any configured weight is not positive.
func New(opts Options) (*Policy, error) {
	def := opts.DefaultWeight
	if def == 0 {
		def = 1
	}
	if def <= 0 {
		return nil, fmt.Errorf("wfq: default_weight must be > 0, got %g", def)
	}
	weights := make(map[string]float64, len(opts.Weights))
	for tenant, w := range opts.Weights {
		if w <= 0 {
			return nil, fmt.Errorf("wfq: weight for tenant %q must be > 0, got %g", tenant, w)
		}
		weights[tenant] = w
	}
	return &Policy{
		weights:       weights,
		defaultWeight: def,
		lastFinish:    make(map[string]float64),
	}, nil
}

// Name implements policy.Policy.
func (p *Policy) Name() string { return Name }

// Weight returns the effective weight for a tenant.
func (p *Policy) Weight(tenant string) float64 {
	if w, ok := p.weights[tenant]; ok {
		return w
	}
	return p.defaultWeight
}

// VirtualTime exposes the current virtual clock, for tests and diagnostics.
func (p *Policy) VirtualTime() float64 { return p.virtualTime }

// Rank assigns the task its virtual finish time.
func (p *Policy) Rank(t domain.Task) policy.Key {
	w := p.Weight(t.TenantID)
	start := p.virtualTime
	if lf, ok := p.lastFinish[t.TenantID]; ok && lf > start {
		start = lf
	}
	cost := float64(t.ExecMillis)
	if cost <= 0 {
		// Zero-duration tasks still consume a dispatch slot; charge a minimal
		// cost so a tenant cannot obtain unbounded service for free.
		cost = 1
	}
	finish := start + cost/w
	p.lastFinish[t.TenantID] = finish
	return policy.Key{Score: finish, Member: policy.MemberFor(t)}
}

// Notify advances virtual time to the finish tag of the dispatched task.
func (p *Policy) Notify(k policy.Key) {
	if k.Score > p.virtualTime {
		p.virtualTime = k.Score
	}
}

// State returns the virtual clock and per-tenant finish tags. This is the state
// that must survive a scheduler restart: without it, a restarted scheduler
// would reset every tenant's virtual clock to zero and briefly re-grant service
// to whichever tenant happened to be ahead.
func (p *Policy) State() map[string]string {
	state := make(map[string]string, len(p.lastFinish)+1)
	state[stateVirtualTime] = strconv.FormatFloat(p.virtualTime, 'g', 17, 64)
	for tenant, f := range p.lastFinish {
		state[stateFinishPfx+tenant] = strconv.FormatFloat(f, 'g', 17, 64)
	}
	return state
}

// Restore reloads state previously produced by State.
func (p *Policy) Restore(state map[string]string) error {
	p.virtualTime = 0
	p.lastFinish = make(map[string]float64, len(state))
	// Sort keys so that a malformed entry always produces the same error.
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, err := strconv.ParseFloat(state[k], 64)
		if err != nil {
			return fmt.Errorf("wfq: restore %q: %w", k, err)
		}
		switch {
		case k == stateVirtualTime:
			p.virtualTime = v
		case strings.HasPrefix(k, stateFinishPfx):
			p.lastFinish[strings.TrimPrefix(k, stateFinishPfx)] = v
		default:
			return fmt.Errorf("wfq: restore: unknown state key %q", k)
		}
	}
	return nil
}
