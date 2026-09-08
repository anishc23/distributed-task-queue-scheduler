// Package metrics defines the Prometheus instrumentation exposed by the
// scheduler and the workers.
//
// Cardinality policy: the only dynamic label is tenant, which comes from a
// small configured set (five by default). Task IDs, attempt IDs, worker names
// and stream entry IDs are never used as labels. Constant labels identify the
// component and the active scheduling policy so that a single Prometheus job
// can scrape every process.
//
// A process exposes only the metrics it can actually produce. A scheduler does
// not publish worker-side series and a worker does not publish scheduler-side
// series, so no endpoint reports a family that is permanently zero. The
// instrument fields are always non-nil, so callers never need to check the
// component before recording; instruments that do not belong to this component
// are simply left unregistered and are never scraped.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// latencyBuckets spans one millisecond to about one minute, which covers both
// the sub-second simulated tasks and the pathological queueing delays that
// starvation experiments are meant to expose.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 17)

// Metrics is the instrument set. A process creates one and registers it on its
// own registry.
type Metrics struct {
	registry *prometheus.Registry

	// Scheduler-side.
	TasksAdmitted       prometheus.Counter
	TasksDispatched     *prometheus.CounterVec
	PendingTasks        prometheus.Gauge
	InFlightTasks       prometheus.Gauge
	LongestPendingWait  prometheus.Gauge
	MaxObservedWait     prometheus.Gauge
	RecoveryReclaimed   prometheus.Counter
	TaskRetries         prometheus.Counter
	TasksDeadLettered   prometheus.Counter
	DispatchErrors      prometheus.Counter
	DuplicateAdmissions prometheus.Counter

	// Worker-side.
	TasksCompleted       *prometheus.CounterVec
	TasksFailed          prometheus.Counter
	DuplicateCompletions prometheus.Counter
	DeadlineMissed       *prometheus.CounterVec
	QueueWaitSeconds     prometheus.Histogram
	LatencySeconds       prometheus.Histogram
	ExecSeconds          prometheus.Histogram
	WorkerBusySeconds    prometheus.Counter
	WorkerUpSeconds      prometheus.Counter
	WorkerUtilization    prometheus.Gauge
	WorkerConcurrency    prometheus.Gauge
	TenantServiceSeconds *prometheus.CounterVec
	TenantCompletedTasks *prometheus.CounterVec

	maxWait time.Duration
}

// Component identifies which process is exposing the metrics. It selects both
// the constant label and which instruments are registered.
const (
	ComponentScheduler = "scheduler"
	ComponentWorker    = "worker"
)

// Options names the process for constant labels.
type Options struct {
	// Component is ComponentScheduler or ComponentWorker.
	Component string
	// Scheduler is the active scheduling policy name.
	Scheduler string
}

// New builds and registers the instrument set on a fresh registry.
func New(opts Options) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	constLabels := prometheus.Labels{
		"component": opts.Component,
		"scheduler": opts.Scheduler,
	}
	isScheduler := opts.Component != ComponentWorker
	isWorker := opts.Component != ComponentScheduler

	// sched, work and both decide whether an instrument is registered on this
	// process's registry. Unregistered instruments still record values; they
	// are simply not exposed.
	sched := factory{reg: reg, labels: constLabels, register: isScheduler}
	work := factory{reg: reg, labels: constLabels, register: isWorker}
	both := factory{reg: reg, labels: constLabels, register: true}

	m := &Metrics{registry: reg}

	f := sched
	m.TasksAdmitted = f.counter("tq_tasks_admitted_total",
		"Tasks accepted from the ingress stream into the pending set.")
	m.TasksDispatched = f.counterVec("tq_tasks_dispatched_total",
		"Tasks selected by the scheduler and published to the worker execution stream.", "tenant")
	m.PendingTasks = f.gauge("tq_pending_tasks",
		"Tasks currently waiting in the scheduler pending set.")
	m.InFlightTasks = f.gauge("tq_inflight_tasks",
		"Tasks dispatched to workers but not yet in a terminal state.")
	m.LongestPendingWait = f.gauge("tq_longest_pending_wait_seconds",
		"Age of the oldest task currently pending, in seconds.")
	m.MaxObservedWait = f.gauge("tq_max_observed_wait_seconds",
		"Longest pending wait observed since this process started, in seconds. Starvation indicator.")
	m.RecoveryReclaimed = f.counter("tq_recovery_reclaimed_total",
		"Execution-stream entries reclaimed by the recovery loop after exceeding the visibility timeout.")
	// Retries and dead letters are produced by the recovery loop in the
	// scheduler and by workers reporting application errors, so both processes
	// expose them.
	m.TaskRetries = both.counter("tq_task_retries_total",
		"Task redeliveries caused by worker failure or lost acknowledgement.")
	m.TasksDeadLettered = both.counter("tq_tasks_dead_lettered_total",
		"Tasks retired to the dead-letter stream after exhausting max_retries.")
	m.DispatchErrors = f.counter("tq_dispatch_errors_total",
		"Errors raised by the dispatch loop.")
	m.DuplicateAdmissions = f.counter("tq_duplicate_admissions_total",
		"Ingress entries rejected because the task was already known.")

	f = work
	m.TasksCompleted = f.counterVec("tq_tasks_completed_total",
		"Tasks that reached successful terminal completion.", "tenant")
	m.TasksFailed = work.counter("tq_task_failures_total",
		"Task attempts that reported an application error or were abandoned before acknowledgement.")
	m.DuplicateCompletions = f.counter("tq_duplicate_completions_total",
		"Completions suppressed by the idempotency guard because the task was already recorded.")
	m.DeadlineMissed = f.counterVec("tq_deadline_missed_total",
		"Completed tasks whose finish time was after their deadline.", "tenant")
	m.QueueWaitSeconds = f.histogram("tq_queue_wait_seconds",
		"Seconds between task submission and the start of execution.")
	m.LatencySeconds = f.histogram("tq_task_latency_seconds",
		"End-to-end seconds between task submission and terminal completion.")
	m.ExecSeconds = f.histogram("tq_task_exec_seconds",
		"Measured seconds a worker spent executing a task.")
	m.WorkerBusySeconds = f.counter("tq_worker_busy_seconds_total",
		"Cumulative seconds worker slots spent executing tasks.")
	m.WorkerUpSeconds = f.counter("tq_worker_slot_seconds_total",
		"Cumulative worker slot-seconds available (concurrency multiplied by uptime).")
	m.WorkerUtilization = f.gauge("tq_worker_utilization_ratio",
		"Busy slot-seconds divided by available slot-seconds since process start.")
	m.WorkerConcurrency = f.gauge("tq_worker_concurrency",
		"Configured number of parallel execution slots in this worker process.")
	m.TenantServiceSeconds = f.counterVec("tq_tenant_service_seconds_total",
		"Completed service time per tenant, in seconds. This is the x vector of Jain's fairness index.", "tenant")
	m.TenantCompletedTasks = f.counterVec("tq_tenant_completed_tasks_total",
		"Completed tasks per tenant.", "tenant")

	return m
}

// Registry exposes the registry so a process can serve it.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveWait records a pending wait and keeps the running maximum, which is the
// starvation signal for priority scheduling.
func (m *Metrics) ObserveWait(d time.Duration) {
	m.LongestPendingWait.Set(d.Seconds())
	if d > m.maxWait {
		m.maxWait = d
		m.MaxObservedWait.Set(d.Seconds())
	}
}

// PreloadTenants initialises every tenant series to zero so that a tenant which
// receives no service still appears in the metrics output. Without this, a
// starved tenant would be invisible rather than visibly at zero. Series that
// belong to the other component are not registered and so stay invisible.
func (m *Metrics) PreloadTenants(tenants []string) {
	for _, t := range tenants {
		m.TasksDispatched.WithLabelValues(t).Add(0)
		m.TasksCompleted.WithLabelValues(t).Add(0)
		m.DeadlineMissed.WithLabelValues(t).Add(0)
		m.TenantServiceSeconds.WithLabelValues(t).Add(0)
		m.TenantCompletedTasks.WithLabelValues(t).Add(0)
	}
}

// Server wraps the HTTP endpoint that exposes /metrics, /healthz and /readyz.
type Server struct {
	srv   *http.Server
	ready func() bool
}

// NewServer builds the metrics HTTP server. ready may be nil, in which case the
// readiness probe always succeeds once the server is listening.
func NewServer(addr string, reg *prometheus.Registry, ready func() bool) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s := &Server{ready: ready}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.ready != nil && !s.ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Run serves until ctx is cancelled, then shuts down gracefully so the final
// scrape is not truncated.
func (s *Server) Run(ctx context.Context, log *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		log.Info("metrics endpoint listening", "addr", s.srv.Addr)
		err := s.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("metrics server on %s: %w", s.srv.Addr, err)
		}
		return nil
	case <-ctx.Done():
	}

	// Graceful shutdown lets an in-progress scrape finish. A scraper holding a
	// keep-alive connection can outlast the grace period, in which case the
	// listener is closed forcibly: the process is stopping either way, and
	// failing shutdown over a held connection would be noise.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("graceful metrics shutdown timed out, closing the listener", "error", err)
		_ = s.srv.Close()
	}
	return <-errCh
}

// factory builds collectors with shared constant labels, registering them only
// when they belong to this process's component.
type factory struct {
	reg      *prometheus.Registry
	labels   prometheus.Labels
	register bool
}

func (f factory) maybeRegister(c prometheus.Collector) {
	if f.register {
		f.reg.MustRegister(c)
	}
}

func (f factory) counter(name, help string) prometheus.Counter {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: f.labels})
	f.maybeRegister(c)
	return c
}

func (f factory) counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: f.labels}, labels)
	f.maybeRegister(c)
	return c
}

func (f factory) gauge(name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help, ConstLabels: f.labels})
	f.maybeRegister(g)
	return g
}

func (f factory) histogram(name, help string) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: name, Help: help, ConstLabels: f.labels, Buckets: latencyBuckets,
	})
	f.maybeRegister(h)
	return h
}
