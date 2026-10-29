package console

import (
	"fmt"
	"sync"
	"time"
)

// Event sources. They differ in what a timestamp means, so they are never
// mixed up in the interface:
//
//   - observed: the console noticed a change between two polls. The time is
//     the poll that saw it; the change happened up to one poll interval
//     earlier.
//   - log: a supervised process wrote it to its own structured log. The time
//     is the process's own.
//   - control: the console did it, at that time, because someone asked.
const (
	SourceObserved = "observed"
	SourceLog      = "log"
	SourceControl  = "control"
)

// Event is one entry in the console's event log.
type Event struct {
	Seq     uint64    `json:"seq"`
	At      time.Time `json:"at"`
	Source  string    `json:"source"`
	Kind    string    `json:"kind"`
	Subject string    `json:"subject,omitempty"`
	Message string    `json:"message"`
}

// EventLog is a bounded, concurrency-safe ring of events.
type EventLog struct {
	mu   sync.Mutex
	buf  []Event
	next uint64
	max  int
}

// NewEventLog keeps the newest max events.
func NewEventLog(max int) *EventLog { return &EventLog{max: max} }

// Add appends an event and assigns its sequence number.
func (l *EventLog) Add(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	e.Seq = l.next
	l.buf = append(l.buf, e)
	if len(l.buf) > l.max {
		l.buf = l.buf[len(l.buf)-l.max:]
	}
}

// Since returns events with a sequence number above seq, oldest first.
func (l *EventLog) Since(seq uint64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, 0, len(l.buf))
	for _, e := range l.buf {
		if e.Seq > seq {
			out = append(out, e)
		}
	}
	return out
}

// Diff compares two consecutive snapshots and returns what changed, as
// observed events. It is a pure function so it can be tested directly.
func Diff(prev, cur *Snapshot) []Event {
	if prev == nil || cur == nil {
		return nil
	}
	at := cur.SampledAt
	var out []Event
	add := func(kind, subject, format string, args ...any) {
		out = append(out, Event{At: at, Source: SourceObserved, Kind: kind, Subject: subject, Message: fmt.Sprintf(format, args...)})
	}

	if prev.Redis.OK != cur.Redis.OK {
		if cur.Redis.OK {
			add("redis_reachable", "redis", "Redis answered again")
		} else {
			add("redis_unreachable", "redis", "Redis did not answer: %s", cur.Redis.Error)
		}
	}
	if cur.Redis.OK && prev.Redis.OK {
		pl, cl := prev.Leadership, cur.Leadership
		switch {
		case pl.Held && !cl.Held:
			add("lease_vacant", pl.Owner, "the lease is free: no scheduler holds it (was %s, epoch %d)", pl.Owner, pl.Epoch)
		case cl.Held && (!pl.Held || pl.Owner != cl.Owner || pl.Epoch != cl.Epoch):
			add("lease_holder", cl.Owner, "the lease is held by %s, epoch %d", cl.Owner, cl.Epoch)
		}
		if pl.Fence != nil && cl.Fence != nil && *cl.Fence != *pl.Fence {
			add("fence_advanced", "redis", "the fence moved from epoch %d to %d: the first write of a new term", *pl.Fence, *cl.Fence)
		}
	}

	prevComp := map[string]Component{}
	for _, c := range prev.Components {
		prevComp[c.Name] = c
	}
	for _, c := range cur.Components {
		p, ok := prevComp[c.Name]
		if !ok {
			continue
		}
		if p.Health != c.Health {
			add("health_changed", c.Name, "%s: %s → %s", c.Name, p.Health, c.Health)
		}
		if f0, f1 := p.Metrics["tq_scheduler_fenced_operations_total"], c.Metrics["tq_scheduler_fenced_operations_total"]; c.Metrics != nil && p.Metrics != nil && f1 > f0 {
			add("fenced_write", c.Name, "%s attempted a write under a superseded epoch and Redis refused it", c.Name)
		}
		if p.Process != nil && c.Process != nil && p.Process.Link != c.Process.Link && c.Process.Link != "" {
			add("link_"+c.Process.Link, c.Name, "%s's path to Redis is %s", c.Name, c.Process.Link)
		}
	}

	if prev.LeaderSum != nil && cur.LeaderSum != nil && *prev.LeaderSum != *cur.LeaderSum {
		switch n := *cur.LeaderSum; {
		case n == 0:
			add("no_leader", "schedulers", "no scheduler reports itself leader: nothing is dispatching")
		case n == 1:
			add("one_leader", "schedulers", "exactly one scheduler reports itself leader")
		default:
			add("several_leaders", "schedulers", "%d schedulers report themselves leader at this poll", n)
		}
	}
	return out
}
