package console

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/workload"
)

// Preset is a fixed, bounded workload the demo can submit. There is no way to
// submit anything else through the console: the HTTP layer accepts a preset
// id and nothing more.
type Preset struct {
	ID          string  `json:"id"`
	Label       string  `json:"label"`
	Description string  `json:"description"`
	Type        string  `json:"type"`
	Count       int     `json:"count"`
	Load        float64 `json:"offered_load"`
}

// Presets are deliberately few: each demonstrates one thing.
var Presets = []Preset{
	{ID: "steady", Label: "Steady, below capacity", Type: config.WorkloadUniform, Count: 1500, Load: 0.7,
		Description: "Uniform tasks at 0.7x capacity: no backlog builds, so any policy dispatches immediately. A quiet baseline for a failover."},
	{ID: "overload", Label: "Overload", Type: config.WorkloadUniform, Count: 2000, Load: 1.5,
		Description: "Uniform tasks at 1.5x capacity: a backlog builds in the pending set, which is where the policy decides who waits."},
	{ID: "tenant-skew", Label: "Noisy neighbour", Type: config.WorkloadMultiTenant, Count: 2000, Load: 1.25,
		Description: "Tenant A submits 90% of the work at 1.25x capacity. Under WFQ the small tenants keep low latency; under FIFO they queue behind A."},
}

// SubmissionStatus is the state of the one submission the console allows at a
// time.
type SubmissionStatus struct {
	Preset    string     `json:"preset"`
	Total     int        `json:"total"`
	RatePerS  float64    `json:"rate_per_s"`
	Started   time.Time  `json:"started_at"`
	Finished  *time.Time `json:"finished_at,omitempty"`
	Submitted int        `json:"submitted"`
	Error     string     `json:"error,omitempty"`
	Running   bool       `json:"running"`
}

// Submitter runs presets against the demo namespace.
type Submitter struct {
	br    *broker.Broker
	slots int
	log   *slog.Logger
	onEv  func(Event)

	mu     sync.Mutex
	status *SubmissionStatus
	n      int
}

// NewSubmitter needs the cluster's slot count to turn an offered load into an
// arrival rate.
func NewSubmitter(br *broker.Broker, slots int, log *slog.Logger, onEvent func(Event)) *Submitter {
	return &Submitter{br: br, slots: slots, log: log, onEv: onEvent}
}

// Status returns the newest submission, or nil if there has been none.
func (s *Submitter) Status() *SubmissionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == nil {
		return nil
	}
	cp := *s.status
	return &cp
}

// Plan builds the task list for a preset without submitting it.
func Plan(p Preset, slots int, seq int) ([]workload.Spec, float64, error) {
	cfg := config.Default().Workload
	cfg.Type = p.Type
	cfg.Count = p.Count
	cfg.Seed = int64(1000 + seq)
	// Unique ids per submission: the admit script rejects a task id it has
	// already seen, so re-running a preset must not reuse them.
	cfg.IDPrefix = fmt.Sprintf("demo%d-%s", seq, p.ID)
	specs, err := workload.Generate(cfg)
	if err != nil {
		return nil, 0, err
	}
	rate, err := workload.RateForLoad(p.Load, workload.Summarise(specs).MeanExecMS, slots)
	if err != nil {
		return nil, 0, err
	}
	if cfg, err = workload.WithArrivalRate(cfg, rate); err != nil {
		return nil, 0, err
	}
	specs, err = workload.Generate(cfg)
	return specs, rate, err
}

// Start begins submitting a preset in the background. It refuses while
// another submission is running.
func (s *Submitter) Start(ctx context.Context, id string) (*SubmissionStatus, error) {
	var preset *Preset
	for i := range Presets {
		if Presets[i].ID == id {
			preset = &Presets[i]
		}
	}
	if preset == nil {
		return nil, fmt.Errorf("unknown preset %q", id)
	}
	s.mu.Lock()
	if s.status != nil && s.status.Running {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s is still being submitted", ErrRejected, s.status.Preset)
	}
	s.n++
	seq := s.n
	s.mu.Unlock()

	specs, rate, err := Plan(*preset, s.slots, seq)
	if err != nil {
		return nil, err
	}
	st := &SubmissionStatus{Preset: preset.ID, Total: len(specs), RatePerS: rate, Started: time.Now(), Running: true}
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
	s.onEv(Event{At: st.Started, Source: SourceControl, Kind: "workload_started", Subject: preset.ID,
		Message: fmt.Sprintf("submitting %d tasks (%s) at %.0f tasks/s, %.2gx capacity", len(specs), preset.Label, rate, preset.Load)})

	go func() {
		stats, err := workload.NewProducer(s.br, s.log, 64).Submit(ctx, specs)
		now := time.Now()
		s.mu.Lock()
		st.Running, st.Finished, st.Submitted = false, &now, stats.Submitted
		if err != nil {
			st.Error = err.Error()
		}
		s.mu.Unlock()
		msg := fmt.Sprintf("submitted %d of %d tasks", stats.Submitted, len(specs))
		if err != nil {
			msg += ": " + err.Error()
		}
		s.onEv(Event{At: now, Source: SourceControl, Kind: "workload_finished", Subject: preset.ID, Message: msg})
	}()
	cp := *st
	return &cp, nil
}
