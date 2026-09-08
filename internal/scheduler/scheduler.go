// Package scheduler wires the configured scheduling policy name to a concrete
// implementation. Selecting a scheduler is a configuration change only:
// producers and workers are unaware of which policy is active.
package scheduler

import (
	"fmt"

	"github.com/anishc23/distributed-task-queue/internal/scheduler/edf"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/fifo"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/priority"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/wfq"
)

// Options selects and configures a scheduling policy.
type Options struct {
	// Policy is one of the names returned by Names.
	Policy string
	// TenantWeights is used by the wfq policy and ignored by the others.
	TenantWeights map[string]float64
	// DefaultTenantWeight applies to tenants missing from TenantWeights.
	DefaultTenantWeight float64
}

// Names lists every available scheduling policy, sorted.
func Names() []string { return policy.Names() }

// Valid reports whether name identifies a known scheduling policy.
func Valid(name string) bool { return policy.Valid(name) }

// New builds the configured policy.
func New(opts Options) (policy.Policy, error) {
	switch opts.Policy {
	case fifo.Name:
		return fifo.New(), nil
	case priority.Name:
		return priority.New(), nil
	case edf.Name:
		return edf.New(), nil
	case wfq.Name:
		p, err := wfq.New(wfq.Options{
			Weights:       opts.TenantWeights,
			DefaultWeight: opts.DefaultTenantWeight,
		})
		if err != nil {
			return nil, err
		}
		return p, nil
	case "":
		return nil, fmt.Errorf("scheduler policy is required, expected one of %v", Names())
	default:
		return nil, fmt.Errorf("unknown scheduler policy %q, expected one of %v", opts.Policy, Names())
	}
}
