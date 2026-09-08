package metrics_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// gather renders the registry in the Prometheus text exposition format.
func gather(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	// Vector metrics only appear once they have at least one child series.
	m.PreloadTenants([]string{"A"})
	var sb strings.Builder
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		sb.WriteString(f.GetName())
		sb.WriteString("\n")
	}
	return sb.String()
}

// A scheduler must not publish worker-side families and vice versa; a family
// that is permanently zero on one component is worse than absent, because it
// looks like the system stopped working.
func TestComponentsExposeDisjointInstruments(t *testing.T) {
	sched := gather(t, metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: "fifo"}))
	work := gather(t, metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: "fifo"}))

	schedulerOnly := []string{
		"tq_tasks_admitted_total", "tq_tasks_dispatched_total", "tq_pending_tasks",
		"tq_inflight_tasks", "tq_longest_pending_wait_seconds", "tq_max_observed_wait_seconds",
		"tq_recovery_reclaimed_total", "tq_dispatch_errors_total", "tq_duplicate_admissions_total",
	}
	workerOnly := []string{
		"tq_tasks_completed_total", "tq_task_failures_total", "tq_duplicate_completions_total",
		"tq_deadline_missed_total", "tq_queue_wait_seconds", "tq_task_latency_seconds",
		"tq_task_exec_seconds", "tq_worker_busy_seconds_total", "tq_worker_slot_seconds_total",
		"tq_worker_utilization_ratio", "tq_worker_concurrency",
		"tq_tenant_service_seconds_total", "tq_tenant_completed_tasks_total",
	}
	shared := []string{"tq_task_retries_total", "tq_tasks_dead_lettered_total"}

	for _, name := range schedulerOnly {
		if !strings.Contains(sched, name+"\n") {
			t.Errorf("scheduler is missing %s", name)
		}
		if strings.Contains(work, name+"\n") {
			t.Errorf("worker should not expose the scheduler metric %s", name)
		}
	}
	for _, name := range workerOnly {
		if !strings.Contains(work, name+"\n") {
			t.Errorf("worker is missing %s", name)
		}
		if strings.Contains(sched, name+"\n") {
			t.Errorf("scheduler should not expose the worker metric %s", name)
		}
	}
	for _, name := range shared {
		if !strings.Contains(sched, name+"\n") || !strings.Contains(work, name+"\n") {
			t.Errorf("%s should be exposed by both components", name)
		}
	}
}

// Recording on an instrument that this component does not register must be a
// safe no-op rather than a nil dereference.
func TestUnregisteredInstrumentsAreSafeToUse(t *testing.T) {
	sched := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: "wfq"})
	sched.TasksCompleted.WithLabelValues("A").Inc()
	sched.LatencySeconds.Observe(0.5)
	sched.WorkerUtilization.Set(0.5)

	work := metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: "wfq"})
	work.TasksAdmitted.Inc()
	work.PendingTasks.Set(3)
}

func TestPreloadTenantsMakesStarvedTenantsVisible(t *testing.T) {
	m := metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: "wfq"})
	m.PreloadTenants([]string{"A", "B", "C"})
	m.TasksCompleted.WithLabelValues("A").Inc()

	// B and C received nothing but must still be present as explicit zeros.
	if got := testutil.ToFloat64(m.TasksCompleted.WithLabelValues("B")); got != 0 {
		t.Fatalf("tenant B completed = %v, want 0", got)
	}
	if n := testutil.CollectAndCount(m.TasksCompleted); n != 3 {
		t.Fatalf("tq_tasks_completed_total has %d series, want 3 (one per tenant)", n)
	}
}

func TestObserveWaitTracksTheRunningMaximum(t *testing.T) {
	m := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: "priority"})
	m.ObserveWait(2 * time.Second)
	m.ObserveWait(500 * time.Millisecond)

	if got := testutil.ToFloat64(m.LongestPendingWait); got != 0.5 {
		t.Fatalf("current wait gauge = %v, want 0.5", got)
	}
	if got := testutil.ToFloat64(m.MaxObservedWait); got != 2 {
		t.Fatalf("max observed wait = %v, want 2 (the maximum must not decrease)", got)
	}
}

func TestServerExposesMetricsAndProbes(t *testing.T) {
	m := metrics.New(metrics.Options{Component: metrics.ComponentScheduler, Scheduler: "edf"})
	m.TasksAdmitted.Inc()

	ready := false
	srv := metrics.NewServer("127.0.0.1:19187", m.Registry(), func() bool { return ready })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("metrics server: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("metrics server did not shut down")
		}
	})

	base := "http://127.0.0.1:19187"
	waitForServer(t, base+"/healthz")

	if code := statusOf(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready = %d, want 503", code)
	}
	ready = true
	if code := statusOf(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("readyz once ready = %d, want 200", code)
	}

	body := bodyOf(t, base+"/metrics")
	if !strings.Contains(body, "tq_tasks_admitted_total") {
		t.Fatal("/metrics does not expose tq_tasks_admitted_total")
	}
	if !strings.Contains(body, `scheduler="edf"`) {
		t.Fatal("/metrics does not carry the scheduler constant label")
	}
}

// client disables keep-alives so the server can shut down promptly at the end
// of the test instead of waiting out an idle pooled connection.
var client = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{DisableKeepAlives: true},
}

func waitForServer(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("metrics server at %s never became reachable", url)
}

func statusOf(t *testing.T, url string) int {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func bodyOf(t *testing.T, url string) string {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(b)
}
