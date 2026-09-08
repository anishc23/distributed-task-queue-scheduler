package worker

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// Calibration is the whole basis of the CPU mode: if it is wrong, every task
// runs for the wrong length of time and the comparison against sleep mode is
// meaningless.
func TestCalibrationProducesAUsableRate(t *testing.T) {
	c, err := calibrate()
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	if c.itersPerMs <= 0 {
		t.Fatalf("iters per ms = %g, want > 0", c.itersPerMs)
	}
	if c.itersFor(10*time.Millisecond) <= c.itersFor(time.Millisecond) {
		t.Fatal("a longer duration must require more iterations")
	}
	if got := c.itersFor(0); got != 0 {
		t.Fatalf("itersFor(0) = %d, want 0", got)
	}
	if got := c.itersFor(-time.Second); got != 0 {
		t.Fatalf("a negative duration must require no work, got %d", got)
	}
	// A duration too small for one iteration must still do something rather
	// than silently completing instantly.
	if got := c.itersFor(time.Nanosecond); got < 1 {
		t.Fatalf("itersFor(1ns) = %d, want at least 1", got)
	}
}

// The point of the mode is that a task actually takes the time it claims. This
// is checked uncontended and with generous tolerance, because a shared CI
// machine cannot promise tight timing.
func TestCPUBurnApproximatesTheRequestedDuration(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("needs more than one core to measure reliably")
	}
	c, err := calibrate()
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	for _, want := range []time.Duration{20 * time.Millisecond, 50 * time.Millisecond} {
		start := time.Now()
		c.burnFor(context.Background(), want)
		got := time.Since(start)
		ratio := float64(got) / float64(want)
		if ratio < 0.5 || ratio > 2.5 {
			t.Errorf("burnFor(%s) took %s (%.2fx); calibration is badly off", want, got, ratio)
		}
	}
}

// A long CPU task must not delay shutdown past the grace period, so the burn
// loop has to observe context cancellation rather than running to completion.
func TestCPUBurnStopsOnContextCancellation(t *testing.T) {
	c, err := calibrate()
	if err != nil {
		t.Fatalf("calibrate: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	c.burnFor(ctx, 5*time.Second)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Fatalf("burnFor ignored cancellation: ran %s of a 5s task after a 30ms deadline", elapsed)
	}
}

// The burn loop must not be optimised away, or "CPU mode" would silently be a
// no-op that looks impossibly fast.
func TestBurnActuallyConsumesTime(t *testing.T) {
	start := time.Now()
	cpuSink += burn(200000)
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Fatalf("200k iterations took %s; the loop is being eliminated", elapsed)
	}
}
