package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anishc23/distributed-task-queue/internal/config"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestDefaultsAreValid(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("built-in defaults must validate: %v", err)
	}
	if cfg.Scheduler.Policy != "fifo" {
		t.Fatalf("default policy = %q, want fifo", cfg.Scheduler.Policy)
	}
	if cfg.Scheduler.IdleSleep.D() <= 0 {
		t.Fatal("default idle sleep must be positive to avoid a busy loop")
	}
}

func TestLoadAppliesDefaultsToPartialDocument(t *testing.T) {
	path := writeConfig(t, `
scheduler:
  policy: edf
workload:
  type: bursty
  count: 25
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Scheduler.Policy != "edf" {
		t.Fatalf("policy = %q, want edf", cfg.Scheduler.Policy)
	}
	if cfg.Workload.Count != 25 {
		t.Fatalf("count = %d, want 25", cfg.Workload.Count)
	}
	if cfg.Redis.Addr == "" || cfg.Streams.Namespace == "" {
		t.Fatal("defaults were not applied to unspecified sections")
	}
	if len(cfg.Workload.Tenants) == 0 {
		t.Fatal("default tenants were not applied")
	}
}

func TestDurationsParseWithExplicitUnits(t *testing.T) {
	path := writeConfig(t, `
scheduler:
  idle_sleep: 7ms
recovery:
  min_idle: 2m30s
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := cfg.Scheduler.IdleSleep.String(); got != "7ms" {
		t.Fatalf("idle_sleep = %s, want 7ms", got)
	}
	if got := cfg.Recovery.MinIdle.D().Minutes(); got != 2.5 {
		t.Fatalf("min_idle = %v minutes, want 2.5", got)
	}
}

func TestRejectsBadDuration(t *testing.T) {
	path := writeConfig(t, "scheduler:\n  idle_sleep: 5\n")
	if _, err := config.Load(path); err == nil {
		t.Fatal("expected an error for a duration without units")
	}
}

func TestRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, "scheduler:\n  polcy: fifo\n")
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected an error for a misspelled field")
	}
	if !strings.Contains(err.Error(), "polcy") {
		t.Fatalf("error should name the offending field, got: %v", err)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"unknown scheduler", "scheduler:\n  policy: sjf\n", "scheduler.policy"},
		{"unknown workload", "workload:\n  type: gaussian\n", "workload.type"},
		{"negative fail rate", "worker:\n  fail_rate: -0.2\n", "worker.fail_rate"},
		{"fail rate above one", "worker:\n  fail_rate: 1.5\n", "worker.fail_rate"},
		{"zero concurrency", "worker:\n  concurrency: -1\n", "worker.concurrency"},
		{"bad log level", "log:\n  level: verbose\n", "log.level"},
		{"non-positive tenant weight", "scheduler:\n  tenant_weights:\n    A: 0\n", "tenant_weights"},
		{"tenant shares must sum to one", "workload:\n  tenants:\n    - {id: A, share: 0.5}\n    - {id: B, share: 0.2}\n", "sum to 1.0"},
		{"duplicate tenant", "workload:\n  tenants:\n    - {id: A, share: 0.5}\n    - {id: A, share: 0.5}\n", "duplicate"},
		{"burst period must exceed burst duration", "workload:\n  type: bursty\n  burst:\n    base_rate_per_sec: 10\n    burst_rate_per_sec: 100\n    burst_duration: 2s\n    burst_period: 1s\n", "burst_period"},
		{"heavy tail alpha", "workload:\n  type: heavy_tailed\n  heavy_tail:\n    min_ms: 10\n    max_ms: 100\n    alpha: 0\n", "alpha"},
		{"empty redis addr", "redis:\n  addr: \"\"\n  pool_size: -3\n", "redis.pool_size"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.body)
			_, err := config.Load(path)
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q should mention %q", err, tc.want)
			}
		})
	}
}

func TestMissingFileIsReported(t *testing.T) {
	_, err := config.Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("expected an error for a missing config file")
	}
}

func TestMultiTenantSkewDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	cfg.Workload.Type = config.WorkloadMultiTenant
	active := cfg.Workload.ActiveTenants()
	if len(active) != 5 {
		t.Fatalf("expected 5 skewed tenants, got %d", len(active))
	}
	if active[0].ID != "A" || active[0].Share != 0.90 {
		t.Fatalf("dominant tenant = %+v, want A at 0.90", active[0])
	}
	ids := cfg.Workload.TenantIDs()
	if len(ids) != 5 || ids[4] != "E" {
		t.Fatalf("tenant ids = %v", ids)
	}
}

func TestShippedConfigsAreValid(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join("..", "..", "configs", "*.yaml"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	experiments, err := filepath.Glob(filepath.Join("..", "..", "experiments", "*.yaml"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	matches = append(matches, experiments...)
	if len(matches) == 0 {
		t.Skip("no shipped configuration files found")
	}
	for _, path := range matches {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := config.Load(path); err != nil {
				t.Fatalf("shipped config %s does not validate: %v", path, err)
			}
		})
	}
}
