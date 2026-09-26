package hostclock_test

import (
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/hostclock"
)

// On a machine that stays awake the two clocks agree, so an ordinary interval
// must not be mistaken for a suspension. This is the property that matters most:
// a false positive here would discard good trials.
func TestAnAwakeMachineReportsNoSuspension(t *testing.T) {
	w := hostclock.Start()
	time.Sleep(50 * time.Millisecond)

	if got := w.Suspended(); got > hostclock.DefaultThreshold {
		t.Errorf("Suspended() = %s after a 50ms wait on an awake machine, want under %s",
			got, hostclock.DefaultThreshold)
	}
	if w.Suspected() {
		t.Error("Suspected() is true on a machine that did not sleep")
	}
}

// Both clocks must advance, and by roughly the same amount.
func TestBothClocksAdvanceTogether(t *testing.T) {
	w := hostclock.Start()
	time.Sleep(80 * time.Millisecond)

	mono, wall := w.Elapsed()
	if mono < 50*time.Millisecond {
		t.Errorf("monotonic elapsed = %s, want at least 50ms", mono)
	}
	if wall < 50*time.Millisecond {
		t.Errorf("wall elapsed = %s, want at least 50ms", wall)
	}
	diff := wall - mono
	if diff < 0 {
		diff = -diff
	}
	// Generous: this asserts the two clocks track each other, not that the
	// scheduler is precise.
	if diff > time.Second {
		t.Errorf("the two clocks disagree by %s over an 80ms interval; "+
			"suspension detection would fire on an awake machine", diff)
	}
}

// A zero Watch must not claim the host slept for the time since the epoch,
// which is what a naive wall-minus-monotonic would report.
func TestZeroValueDoesNotClaimSuspension(t *testing.T) {
	var w hostclock.Watch
	// Both fields are the zero time, so both elapsed values are enormous and
	// nearly equal; the difference, not the magnitude, is what is reported.
	if got := w.Suspended(); got > hostclock.DefaultThreshold {
		t.Errorf("zero Watch reports %s of suspension", got)
	}
}
