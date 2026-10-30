//go:build integration

package console

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
)

// These tests run the console's demo mode for real: built scheduler and worker
// binaries, real signals, a real proxy, a real Redis. What they check is that
// the console reports what happened, not what was asked for.

func redisAddr() string {
	if v := os.Getenv("TQ_TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

func buildBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{"scheduler", "worker"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, b), "./cmd/"+b)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", b, err, out)
		}
	}
	return dir
}

type demo struct {
	sup    *Supervisor
	con    *Console
	br     *broker.Broker
	events *EventLog
}

func startDemo(t *testing.T, ttl, renew time.Duration) *demo {
	t.Helper()
	cfg := config.Default()
	cfg.Redis.Addr = redisAddr()
	cfg.Streams.Namespace = fmt.Sprintf("tq-console-test-%d", time.Now().UnixNano())
	br, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := br.Ping(ctx); err != nil {
		t.Fatalf("Redis is required at %s: %v", cfg.Redis.Addr, err)
	}
	if err := br.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	events := NewEventLog(500)
	sup, err := NewSupervisor(SupervisorOptions{
		BinDir: buildBinaries(t), RedisAddr: cfg.Redis.Addr, Namespace: cfg.Streams.Namespace, Policy: "fifo",
		Schedulers: 2, Workers: 1, Concurrency: 4, LeaseTTL: ttl, LeaseRenew: renew,
		MetricsPort: 19701, OnEvent: events.Add,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sup.Start(ctx); err != nil {
		t.Fatal(err)
	}
	con := New(Options{Broker: br, Targets: sup.Targets(), Supervisor: sup, Poll: 250 * time.Millisecond, Events: events})
	go con.Run(ctx)
	t.Cleanup(func() {
		sup.Stop()
		cancel()
		for name, st := range sup.States() {
			if st.State != StateExited {
				t.Errorf("%s is still %s after Stop", name, st.State)
			}
		}
		// Reset keeps the epoch counter and fence on purpose; a test namespace
		// is never reused, so remove them too.
		_ = br.Reset(context.Background())
		_ = br.Redis().Del(context.Background(), br.Keys().LeaderEpoch, br.Keys().Fence).Err()
		br.Close()
	})
	return &demo{sup: sup, con: con, br: br, events: events}
}

func (d *demo) waitFor(t *testing.T, what string, limit time.Duration, ok func(*Snapshot) bool) *Snapshot {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if s := d.con.Latest(); s != nil && ok(s) {
			return s
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s; last snapshot %+v", limit, what, d.con.Latest())
	return nil
}

func (d *demo) hasEvent(kind, subject string) bool {
	for _, e := range d.events.Since(0) {
		if e.Kind == kind && (subject == "" || e.Subject == subject) {
			return true
		}
	}
	return false
}

func TestDemoReportsAKilledLeaderAndItsSuccessor(t *testing.T) {
	d := startDemo(t, 2*time.Second, 600*time.Millisecond)
	first := d.waitFor(t, "a leader and every process ready", 20*time.Second, func(s *Snapshot) bool {
		if !s.Redis.OK || !s.Leadership.Held || s.LeaderSum == nil || *s.LeaderSum != 1 {
			return false
		}
		for _, c := range s.Components {
			if c.Health != HealthReady {
				return false
			}
		}
		return true
	})
	victim := first.Leadership.Owner

	sub := NewSubmitter(d.br, d.sup.Slots(), quietLog(), d.events.Add)
	if _, err := sub.Start(context.Background(), "steady"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	if _, err := d.sup.Apply(FaultRequest{Action: ActionKill, Target: "leader"}, d.con.LeaseOwner()); err != nil {
		t.Fatal(err)
	}
	// While nothing leads, nothing is admitted: submitted work has to show up
	// as not yet admitted, or the outage would look like an idle queue.
	var queuedUnadmitted int64
	after := d.waitFor(t, "a different scheduler to take the lease", 10*time.Second, func(s *Snapshot) bool {
		if s.Queue != nil && s.Queue.NotAdmitted > queuedUnadmitted {
			queuedUnadmitted = s.Queue.NotAdmitted
		}
		return s.Redis.OK && s.Leadership.Held && s.Leadership.Owner != victim
	})
	if queuedUnadmitted == 0 {
		t.Error("no work was ever reported as not yet admitted during the outage")
	}
	if after.Leadership.Epoch <= first.Leadership.Epoch {
		t.Errorf("successor epoch %d is not above %d", after.Leadership.Epoch, first.Leadership.Epoch)
	}
	if st := d.sup.States()[victim]; st.State != StateExited {
		t.Errorf("killed %s is reported %s", victim, st.State)
	}
	// The killed process is known not to lead, so the sum stays known.
	d.waitFor(t, "a known leader sum of exactly 1", 5*time.Second, func(s *Snapshot) bool {
		return s.LeaderSum != nil && *s.LeaderSum == 1
	})
	if !d.hasEvent("process_exited", victim) {
		t.Error("no process_exited event for the victim")
	}
	if !d.hasEvent("lease_acquired", after.Leadership.Owner) {
		t.Error("the successor's own log line was not turned into an event")
	}

	w := d.window(t)
	if w.EpochInversions != 0 {
		t.Errorf("%d epoch inversions in the dispatch log", w.EpochInversions)
	}
	if w.MaxGapMS == nil || *w.MaxGapMS < 1000 {
		t.Errorf("max dispatch gap %v ms; a crash with a 2 s lease should leave a gap of over a second", deref(w.MaxGapMS))
	}

	// The per-tenant view is read from the results stream: every completion in
	// the window, attributed to its tenant, with a latency that is positive.
	results, err := d.br.ResultsSince(context.Background(), time.Now().Add(-2*time.Minute), resultsCap)
	if err != nil {
		t.Fatal(err)
	}
	tw := SummariseTenants(results, 2*time.Minute, resultsCap)
	total := 0
	for _, ts := range tw.Tenants {
		total += ts.Completed
		if ts.LatencyP50MS <= 0 || ts.LatencyP95MS < ts.LatencyP50MS {
			t.Errorf("tenant %s has implausible latency: %+v", ts.Tenant, ts)
		}
	}
	if total == 0 || total != len(results) {
		t.Errorf("tenant completions sum to %d over %d results", total, len(results))
	}

	if _, err := d.sup.Apply(FaultRequest{Action: ActionRestart, Target: victim}, ""); err != nil {
		t.Fatal(err)
	}
	d.waitFor(t, "the restarted process to be ready", 15*time.Second, func(s *Snapshot) bool {
		for _, c := range s.Components {
			if c.Name == victim {
				return c.Health == HealthReady && c.Process != nil && c.Process.Restarts == 1
			}
		}
		return false
	})
}

func TestDemoFrozenLeaderIsSupersededAndStoppedOnResume(t *testing.T) {
	d := startDemo(t, 2*time.Second, 600*time.Millisecond)
	first := d.waitFor(t, "a leader", 20*time.Second, func(s *Snapshot) bool { return s.Redis.OK && s.Leadership.Held })
	victim := first.Leadership.Owner

	sub := NewSubmitter(d.br, d.sup.Slots(), quietLog(), d.events.Add)
	if _, err := sub.Start(context.Background(), "steady"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	msg, err := d.sup.Apply(FaultRequest{Action: ActionFreeze, Target: "leader", DurationMS: 5000}, d.con.LeaseOwner())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "verified stopped") {
		t.Errorf("the freeze was not verified from the process table: %s", msg)
	}
	if st := d.sup.States()[victim]; st.State != StateStopped || st.Fault == nil {
		t.Errorf("frozen %s reported %+v", victim, st)
	}
	d.waitFor(t, "a successor while the leader is frozen", 8*time.Second, func(s *Snapshot) bool {
		return s.Redis.OK && s.Leadership.Held && s.Leadership.Owner != victim
	})
	d.waitFor(t, "the frozen process to resume and stand down", 10*time.Second, func(s *Snapshot) bool {
		for _, c := range s.Components {
			if c.Name == victim {
				return c.Process != nil && c.Process.State == StateRunning && c.Metrics["tq_scheduler_is_leader"] == 0
			}
		}
		return false
	})
	if !d.hasEvent("fenced", victim) && !d.hasEvent("lease_lost", victim) {
		t.Error("the resumed leader neither logged a fence refusal nor a lost lease")
	}
	if w := d.window(t); w.EpochInversions != 0 {
		t.Errorf("%d epoch inversions after the resume", w.EpochInversions)
	}
}

func (d *demo) window(t *testing.T) DispatchWindow {
	t.Helper()
	from := time.Now().Add(-2 * time.Minute)
	msgs, err := d.br.DispatchesSince(context.Background(), from, dispatchCap)
	if err != nil {
		t.Fatal(err)
	}
	return SummariseDispatches(msgs, from, 2*time.Minute, 250*time.Millisecond, dispatchCap)
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
