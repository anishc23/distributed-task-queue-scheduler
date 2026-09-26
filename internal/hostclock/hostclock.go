// Package hostclock detects the machine having been asleep.
//
// Every timing experiment in this repository has now been spoiled at least once
// by a laptop suspending mid-run. The symptoms differ each time and none of them
// look like what they are: a seventeen-minute dispatch "outage", a trial that
// reports two leaders because both processes were frozen mid-handover and woke
// with stale gauges, a throughput figure computed over a wall-clock interval the
// process spent not running. The results are not merely noisy, they are
// measurements of the wrong thing, and they are plausible enough to publish.
//
// Previous defences inferred the suspension from its effects — an outage beyond
// five lease TTLs cannot have come from the lease, so drop it. That works, but
// it is a heuristic per experiment, it needs a threshold chosen per metric, and
// it catches nothing in an experiment whose numbers happen to stay in range.
//
// This detects the cause instead. Go's time.Time carries both a wall-clock
// reading and a monotonic one, and they diverge precisely when the host is
// suspended: the monotonic clock stops while the machine is asleep, and the
// wall clock does not. Subtracting two times keeps the monotonic reading when
// both have one, while stripping it with Round(0) forces wall-clock arithmetic.
// The difference between those two elapsed times is how long the process was
// not running.
//
// This is a property of the host, not of the code under test, so a suspension
// invalidates a trial rather than telling you anything about the system.
package hostclock

import "time"

// Watch records a starting instant on both clocks.
type Watch struct {
	mono time.Time // carries a monotonic reading
	wall time.Time // monotonic reading stripped
}

// Start begins watching.
func Start() Watch {
	now := time.Now()
	return Watch{mono: now, wall: now.Round(0)}
}

// Elapsed returns how much time passed on each clock. They agree on a machine
// that stayed awake.
func (w Watch) Elapsed() (monotonic, wall time.Duration) {
	now := time.Now()
	return now.Sub(w.mono), now.Round(0).Sub(w.wall)
}

// Suspended reports how long the host was asleep since Start.
//
// Small positive values are ordinary clock adjustment — NTP slew, a leap
// smear — rather than suspension, so callers should compare against a
// threshold rather than testing for non-zero. Negative differences (the wall
// clock stepping backwards) are reported as zero, because they say nothing
// about suspension.
func (w Watch) Suspended() time.Duration {
	mono, wall := w.Elapsed()
	if d := wall - mono; d > 0 {
		return d
	}
	return 0
}

// DefaultThreshold is the gap beyond which a difference between the two clocks
// is suspension rather than clock adjustment.
//
// NTP corrections on a normally-running machine are milliseconds; a suspend is
// seconds at the very least. Two seconds sits far above the first and far below
// the second, so the choice is not delicate.
const DefaultThreshold = 2 * time.Second

// Suspected reports whether the host appears to have slept by more than
// DefaultThreshold.
func (w Watch) Suspected() bool { return w.Suspended() > DefaultThreshold }
