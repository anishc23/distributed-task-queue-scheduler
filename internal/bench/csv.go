package bench

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// rawHeader is the schema of the per-task CSV. Every row repeats the run
// identity columns so that concatenating files from different runs can never
// silently mix experiments.
var rawHeader = []string{
	"run_id", "scheduler", "workload", "repetition", "seed",
	"task_id", "tenant_id", "priority", "outcome",
	"submitted_at", "dispatched_at", "started_at", "finished_at", "deadline",
	"exec_ms", "actual_exec_ms", "retry_count",
	"queue_wait_s", "latency_s", "deadline_missed", "worker",
}

// WriteRawCSV writes the task-level records for one experiment.
func WriteRawCSV(path string, id RunIdentity, results []domain.Result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create raw results directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write(rawHeader); err != nil {
		return fmt.Errorf("write header to %s: %w", path, err)
	}
	sorted := append([]domain.Result(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].FinishedAt.Before(sorted[j].FinishedAt) })

	for _, r := range sorted {
		row := []string{
			id.RunID, id.Scheduler, id.Workload, strconv.Itoa(id.Repetition), strconv.FormatInt(id.Seed, 10),
			r.TaskID, r.TenantID, strconv.Itoa(r.Priority), string(r.Outcome),
			ts(r.SubmittedAt), ts(r.DispatchedAt), ts(r.StartedAt), ts(r.FinishedAt), ts(r.Deadline),
			strconv.FormatInt(r.ExecMillis, 10), strconv.FormatInt(r.ActualExecMillis, 10), strconv.Itoa(r.RetryCount),
			f6(r.QueueWait().Seconds()), f6(r.Latency().Seconds()), boolStr(r.MissedDeadline()), r.Worker,
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write row to %s: %w", path, err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return fmt.Errorf("flush %s: %w", path, err)
	}
	return nil
}

// aggregateHeader is the schema of the one-row-per-experiment CSV.
var aggregateHeader = []string{
	"run_id", "timestamp_utc", "scheduler", "workload", "repetition", "seed",
	"tasks_planned", "tasks_submitted", "tasks_completed", "tasks_dead_lettered",
	"tasks_retried", "task_failures", "duplicate_completions", "timed_out",
	"worker_processes", "worker_concurrency", "total_slots", "max_in_flight",
	"experiment_duration_s", "makespan_s", "throughput_tasks_per_s",
	"latency_mean_s", "latency_p50_s", "latency_p95_s", "latency_p99_s", "latency_max_s",
	"wait_mean_s", "wait_p50_s", "wait_p95_s", "wait_p99_s", "wait_max_s",
	"exec_mean_s", "exec_p99_s",
	"deadline_missed", "deadline_miss_rate",
	"worker_busy_s", "worker_slot_s", "worker_utilization",
	"jain_fairness_service", "jain_fairness_count", "tenant_count",
	"tenant_p99_max_s", "tenant_p99_min_s", "tenant_p99_ratio", "min_tenant_service_share",
	"producer_max_lag_s", "producer_mean_lag_s",
	"arrival_rate_per_sec", "exec_min_ms", "exec_max_ms",
}

// AggregateWriter appends one row per experiment to a single CSV.
type AggregateWriter struct {
	f *os.File
	w *csv.Writer
}

// NewAggregateWriter creates the aggregate CSV and writes its header.
func NewAggregateWriter(path string) (*AggregateWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create results directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(aggregateHeader); err != nil {
		f.Close()
		return nil, fmt.Errorf("write aggregate header: %w", err)
	}
	w.Flush()
	return &AggregateWriter{f: f, w: w}, nil
}

// Append writes one experiment summary and flushes immediately, so a benchmark
// interrupted halfway still leaves a readable file.
func (a *AggregateWriter) Append(s Summary) error {
	row := []string{
		s.Identity.RunID, s.Timestamp.UTC().Format(time.RFC3339), s.Identity.Scheduler, s.Identity.Workload,
		strconv.Itoa(s.Identity.Repetition), strconv.FormatInt(s.Identity.Seed, 10),
		strconv.Itoa(s.TasksPlanned), strconv.Itoa(s.TasksSubmitted), strconv.Itoa(s.TasksCompleted),
		strconv.Itoa(s.TasksDeadLettered), strconv.Itoa(s.TasksRetried), strconv.Itoa(s.TaskFailures),
		strconv.Itoa(s.DuplicateCompletions), boolStr(s.TimedOut),
		strconv.Itoa(s.WorkerProcesses), strconv.Itoa(s.WorkerConcurrency), strconv.Itoa(s.TotalSlots),
		strconv.Itoa(s.MaxInFlight),
		f6(s.ExperimentDuration.Seconds()), f6(s.Makespan.Seconds()), f6(s.Throughput),
		f6(s.Latency.Mean), f6(s.Latency.P50), f6(s.Latency.P95), f6(s.Latency.P99), f6(s.Latency.Max),
		f6(s.Wait.Mean), f6(s.Wait.P50), f6(s.Wait.P95), f6(s.Wait.P99), f6(s.Wait.Max),
		f6(s.Exec.Mean), f6(s.Exec.P99),
		strconv.Itoa(s.DeadlineMissed), f6(s.DeadlineMissRate),
		f6(s.WorkerBusySeconds), f6(s.WorkerSlotSeconds), f6(s.WorkerUtilization),
		f6(s.JainFairnessService), f6(s.JainFairnessCount), strconv.Itoa(s.TenantCount),
		f6(s.Spread.MaxP99), f6(s.Spread.MinP99), f6(s.Spread.Ratio), f6(s.Spread.MinServiceShare),
		f6(s.ProducerMaxLag.Seconds()), f6(s.ProducerMeanLag.Seconds()),
		f6(s.ArrivalRatePerSec), strconv.FormatInt(s.ExecMinMillis, 10), strconv.FormatInt(s.ExecMaxMillis, 10),
	}
	if err := a.w.Write(row); err != nil {
		return fmt.Errorf("write aggregate row: %w", err)
	}
	a.w.Flush()
	return a.w.Error()
}

// Close flushes and closes the aggregate CSV.
func (a *AggregateWriter) Close() error {
	a.w.Flush()
	if err := a.w.Error(); err != nil {
		a.f.Close()
		return err
	}
	return a.f.Close()
}

// tenantHeader is the schema of the per-tenant CSV: one row per tenant per
// experiment.
var tenantHeader = []string{
	"run_id", "scheduler", "workload", "repetition", "seed", "tenant",
	"completed_tasks", "service_seconds", "service_share",
	"latency_mean_s", "latency_p50_s", "latency_p95_s", "latency_p99_s", "latency_max_s",
	"wait_p99_s", "wait_max_s", "deadline_missed", "deadline_miss_rate",
}

// TenantWriter appends one row per tenant per experiment to a single CSV.
type TenantWriter struct {
	f *os.File
	w *csv.Writer
}

// NewTenantWriter creates the per-tenant CSV and writes its header.
func NewTenantWriter(path string) (*TenantWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create results directory: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(tenantHeader); err != nil {
		f.Close()
		return nil, fmt.Errorf("write tenant header: %w", err)
	}
	w.Flush()
	return &TenantWriter{f: f, w: w}, nil
}

// Append writes every tenant row for one experiment.
func (tw *TenantWriter) Append(s Summary) error {
	for _, t := range s.Tenants {
		row := []string{
			s.Identity.RunID, s.Identity.Scheduler, s.Identity.Workload,
			strconv.Itoa(s.Identity.Repetition), strconv.FormatInt(s.Identity.Seed, 10), t.Tenant,
			strconv.Itoa(t.Completed), f6(t.ServiceSeconds), f6(t.ServiceShare),
			f6(t.Latency.Mean), f6(t.Latency.P50), f6(t.Latency.P95), f6(t.Latency.P99), f6(t.Latency.Max),
			f6(t.Wait.P99), f6(t.Wait.Max),
			strconv.Itoa(t.DeadlineMissed), f6(t.DeadlineMissRate),
		}
		if err := tw.w.Write(row); err != nil {
			return fmt.Errorf("write tenant row: %w", err)
		}
	}
	tw.w.Flush()
	return tw.w.Error()
}

// Close flushes and closes the per-tenant CSV.
func (tw *TenantWriter) Close() error {
	tw.w.Flush()
	if err := tw.w.Error(); err != nil {
		tw.f.Close()
		return err
	}
	return tw.f.Close()
}

// WriteJSON writes any value as indented JSON, creating parent directories.
func WriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func ts(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func f6(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
