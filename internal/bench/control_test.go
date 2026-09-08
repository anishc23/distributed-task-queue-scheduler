package bench

import (
	"io"
	"log/slog"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/redis/go-redis/v9"
)

func testRunner(t *testing.T, schedulers []string) *Runner {
	t.Helper()
	r, err := NewRunner(Options{
		Base:              config.Default(),
		RunID:             "unit",
		ResultsDir:        t.TempDir(),
		Schedulers:        schedulers,
		Workloads:         []string{"uniform"},
		Repetitions:       1,
		WorkerProcesses:   2,
		WorkerConcurrency: 4,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Redis:             redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return r
}

// The control arm must differ from the real system in exactly one respect:
// workers read the ingress stream directly and nothing dispatches. If the
// execution stream were left pointing elsewhere the control would simply
// consume nothing and look infinitely fast, which is the failure mode this
// guards against.
func TestControlArmPointsWorkersAtIngress(t *testing.T) {
	r := testRunner(t, []string{SchedulerNone, "fifo"})

	ctrl := r.ExperimentConfig(RunIdentity{Scheduler: SchedulerNone, Workload: "uniform"})
	if ctrl.Streams.ExecStream != ctrl.Streams.IngressStream {
		t.Fatalf("control arm exec stream = %q, want it to equal the ingress stream %q",
			ctrl.Streams.ExecStream, ctrl.Streams.IngressStream)
	}

	central := r.ExperimentConfig(RunIdentity{Scheduler: "fifo", Workload: "uniform"})
	if central.Streams.ExecStream == central.Streams.IngressStream {
		t.Fatal("a scheduled arm must keep the execution stream separate from ingress")
	}
	if central.Scheduler.Policy != "fifo" {
		t.Fatalf("scheduled arm policy = %q, want fifo", central.Scheduler.Policy)
	}
}

// Each arm must still get its own Redis namespace, or the control and the
// scheduled runs would share state.
func TestControlArmGetsItsOwnNamespace(t *testing.T) {
	r := testRunner(t, []string{SchedulerNone, "fifo"})
	a := r.ExperimentConfig(RunIdentity{Scheduler: SchedulerNone, Workload: "uniform"}).Streams.Namespace
	b := r.ExperimentConfig(RunIdentity{Scheduler: "fifo", Workload: "uniform"}).Streams.Namespace
	if a == b {
		t.Fatalf("both arms share the namespace %q", a)
	}
}

func TestRunnerAcceptsTheControlArmAndRejectsNonsense(t *testing.T) {
	if _, err := NewRunner(Options{
		Base: config.Default(), RunID: "u", Schedulers: []string{"nope"},
		Workloads: []string{"uniform"}, Repetitions: 1,
		WorkerProcesses: 1, WorkerConcurrency: 1,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Redis:  redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
	}); err == nil {
		t.Fatal("expected an error for an unknown scheduler")
	}
	testRunner(t, []string{SchedulerNone}) // must not error
}
