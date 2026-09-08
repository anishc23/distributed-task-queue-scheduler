package worker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"
)

// Execution modes.
//
// Every result in this project so far comes from workers that sleep for the
// task's declared duration. That is defensible - it makes task sizes exactly
// controllable and removes application variance from a comparison between
// scheduling policies - but it has an obvious objection: a sleeping worker
// consumes no CPU, so N slots always deliver N-way parallelism no matter how
// many cores exist, and "utilisation" measures slot occupancy rather than work.
//
// ExecModeCPU exists to test whether the conclusions survive real work. It
// burns CPU for the requested duration instead of sleeping, which means slots
// contend for cores exactly as real tasks would.
// The mode names themselves live in the config package so that configuration
// validation can use them without importing this one.

// cpuSink prevents the compiler from eliminating the calibration and burn
// loops, whose results are otherwise unused.
var cpuSink uint64

// burn performs iters rounds of SHA-256 chaining. SHA-256 is used because it is
// data-dependent and cannot be strength-reduced or vectorised away, so the work
// actually happens.
func burn(iters int) uint64 {
	var buf [32]byte
	var acc uint64
	for i := 0; i < iters; i++ {
		buf = sha256.Sum256(buf[:])
		acc += uint64(buf[0])
	}
	return acc
}

// calibration is how many burn iterations this machine completes per
// millisecond, measured once per process.
type calibration struct {
	itersPerMs float64
}

// calibrate measures the machine's burn rate. It warms up first so that CPU
// frequency scaling and instruction cache effects do not make the first
// measurement unrepresentative.
//
// Calibration runs on an otherwise idle process, so a task executed while other
// slots are also burning will take longer than its declared duration. That is
// not a calibration error: it is core contention, which is precisely the effect
// this mode exists to expose. The measured duration is recorded per task as
// actual_exec_ms so the gap is visible rather than hidden.
func calibrate() (calibration, error) {
	const warmup = 5000
	cpuSink += burn(warmup)

	// Grow the sample until it takes long enough to time accurately.
	iters := 20000
	for attempt := 0; attempt < 12; attempt++ {
		start := time.Now()
		cpuSink += burn(iters)
		elapsed := time.Since(start)
		if elapsed >= 20*time.Millisecond {
			ms := float64(elapsed.Nanoseconds()) / 1e6
			return calibration{itersPerMs: float64(iters) / ms}, nil
		}
		iters *= 2
	}
	return calibration{}, fmt.Errorf("worker: CPU calibration did not converge")
}

// itersFor returns the iteration count that approximates d of CPU time.
func (c calibration) itersFor(d time.Duration) int {
	if c.itersPerMs <= 0 || d <= 0 {
		return 0
	}
	n := c.itersPerMs * (float64(d.Nanoseconds()) / 1e6)
	if n < 1 {
		return 1
	}
	return int(n)
}

// burnFor consumes approximately d of CPU time, checking ctx periodically so a
// shutdown is not delayed by a long task. The chunk is small enough to stay
// responsive and large enough that the context check is not itself a cost.
func (c calibration) burnFor(ctx context.Context, d time.Duration) {
	total := c.itersFor(d)
	const chunk = 2000
	for done := 0; done < total; done += chunk {
		if ctx.Err() != nil {
			return
		}
		n := chunk
		if remaining := total - done; remaining < chunk {
			n = remaining
		}
		cpuSink += burn(n)
	}
}
