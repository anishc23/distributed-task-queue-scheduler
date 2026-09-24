package config_test

import (
	"encoding/json"
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

// The benchmark manifest embeds the whole configuration as JSON. It is a
// reproducibility artefact, so its field names must match the YAML the run was
// configured from rather than Go's struct field names, and durations must stay
// human-readable.
func TestConfigJSONMatchesYAMLFieldNames(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(b)

	for _, want := range []string{
		`"redis"`, `"streams"`, `"scheduler"`, `"worker"`, `"recovery"`, `"workload"`, `"log"`,
		`"max_in_flight"`, `"tenant_weights"`, `"default_tenant_weight"`,
		`"fail_before_ack_rate"`, `"arrival_rate_per_sec"`, `"skew_tenants"`,
		`"exec_multiplier"`, `"heavy_tail"`, `"min_ms"`, `"max_ms"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("manifest JSON is missing the snake_case key %s", want)
		}
	}
	for _, bad := range []string{
		`"MaxInFlight"`, `"TenantWeights"`, `"ArrivalRatePerSec"`, `"HeavyTail"`,
		`"Redis"`, `"Workload"`, `"MinMillis"`,
	} {
		if strings.Contains(body, bad) {
			t.Errorf("manifest JSON leaks the Go field name %s", bad)
		}
	}

	// Durations must be strings with units, not raw nanosecond counts.
	if !strings.Contains(body, `"idle_sleep":"2ms"`) {
		t.Errorf("durations should serialise with explicit units, got: %s", body)
	}
	if strings.Contains(body, `"idle_sleep":2000000`) {
		t.Error("durations must not serialise as raw nanosecond counts")
	}
}

func TestConfigJSONRoundTrips(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back config.Config
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Scheduler.IdleSleep != cfg.Scheduler.IdleSleep {
		t.Fatalf("idle_sleep did not round-trip: %s vs %s", back.Scheduler.IdleSleep, cfg.Scheduler.IdleSleep)
	}
	if back.Recovery.MinIdle != cfg.Recovery.MinIdle {
		t.Fatalf("min_idle did not round-trip: %s vs %s", back.Recovery.MinIdle, cfg.Recovery.MinIdle)
	}
	if err := back.Validate(); err != nil {
		t.Fatalf("a config that round-tripped through JSON must still validate: %v", err)
	}
}

// Leader election is on by default. A scheduler that dispatches without first
// checking whether another one is already doing so is unsafe, and a default
// that is only correct when the operator remembers to change it is the wrong
// default.
func TestLeaderElectionIsOnByDefault(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	le := cfg.Scheduler.LeaderElection
	if !le.Enabled {
		t.Error("leader election is off by default; two schedulers would silently corrupt policy state")
	}
	if le.RenewInterval.D() >= le.TTL.D() {
		t.Errorf("default renew interval %s is not shorter than the ttl %s, so a single slow round trip would depose a healthy leader",
			le.RenewInterval, le.TTL)
	}
}

// Enabled is a bool, so "unset" and "explicitly false" are the same value once
// decoded. Defaults must therefore never be backfilled onto it, or turning
// election off in a config file would quietly do nothing.
func TestLeaderElectionCanBeTurnedOffInAFile(t *testing.T) {
	path := writeConfig(t, `
scheduler:
  leader_election:
    enabled: false
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Scheduler.LeaderElection.Enabled {
		t.Fatal("enabled: false was overwritten by the default; an operator who turned election off would still be running it")
	}
	// The intervals are still backfilled, so turning it on later needs no
	// further edits.
	if cfg.Scheduler.LeaderElection.TTL.D() <= 0 {
		t.Error("ttl was not backfilled")
	}
}

// Omitting the block entirely is not a request to disable anything.
func TestOmittingLeaderElectionKeepsItOn(t *testing.T) {
	path := writeConfig(t, `
scheduler:
  policy: wfq
`)
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.Scheduler.LeaderElection.Enabled {
		t.Fatal("a config file that says nothing about leader election turned it off")
	}
}

func TestLeaderElectionValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "renew at least as long as the ttl",
			body: "scheduler:\n  leader_election:\n    enabled: true\n    ttl: 1s\n    renew_interval: 1s\n",
			want: "renew_interval",
		},
		{
			name: "zero retry interval",
			body: "scheduler:\n  leader_election:\n    enabled: true\n    retry_interval: -1s\n",
			want: "retry_interval",
		},
		{
			name: "zero ingress reclaim min idle",
			body: "scheduler:\n  ingest_reclaim_min_idle: -1s\n",
			want: "ingest_reclaim_min_idle",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(writeConfig(t, tc.body))
			if err == nil {
				t.Fatalf("expected a validation error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending field %q", err, tc.want)
			}
		})
	}
}

// The scheduler holds an ingress entry for microseconds; a worker holds an exec
// entry for as long as the task runs. Reclaiming the two on the same timescale
// would either duplicate real work or leave stranded tasks sitting for far
// longer than necessary.
func TestIngressReclaimIsFarMoreAggressiveThanExecRecovery(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if cfg.Scheduler.IngestReclaimMinIdle.D() >= cfg.Recovery.MinIdle.D() {
		t.Errorf("ingest reclaim min idle %s is not shorter than the exec visibility timeout %s",
			cfg.Scheduler.IngestReclaimMinIdle, cfg.Recovery.MinIdle)
	}
}

// Sentinel discovery replaces the fixed address rather than supplementing it,
// so the fields that make discovery possible become the required ones. Leaving
// redis.addr required here would invite someone to set a stale address and then
// wonder why a failover changed nothing.
func TestSentinelValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "enabled without a master name",
			body: "redis:\n  sentinel:\n    enabled: true\n    addrs: [\"127.0.0.1:26379\"]\n",
			want: "master_name",
		},
		{
			name: "enabled without any sentinel addresses",
			body: "redis:\n  sentinel:\n    enabled: true\n    master_name: mymaster\n",
			want: "addrs",
		},
		{
			name: "negative replica wait",
			body: "redis:\n  sentinel:\n    enabled: true\n    master_name: mymaster\n    addrs: [\"127.0.0.1:26379\"]\n    wait_for_replicas: -1\n",
			want: "wait_for_replicas",
		},
		{
			name: "replica wait without a bound on it",
			body: "redis:\n  sentinel:\n    enabled: true\n    master_name: mymaster\n    addrs: [\"127.0.0.1:26379\"]\n    wait_for_replicas: 1\n    wait_timeout: 0s\n",
			want: "wait_timeout",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := config.Load(writeConfig(t, tc.body))
			if err == nil {
				t.Fatalf("expected a validation error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name the offending field %q", err, tc.want)
			}
		})
	}
}

// With Sentinel on, a missing redis.addr is fine: the address is discovered.
func TestSentinelMakesTheFixedAddressOptional(t *testing.T) {
	body := "redis:\n  addr: \"\"\n  sentinel:\n    enabled: true\n    master_name: mymaster\n    addrs: [\"127.0.0.1:26379\", \"127.0.0.1:26380\"]\n"
	cfg, err := config.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("a Sentinel configuration without redis.addr was rejected: %v", err)
	}
	if !cfg.Redis.Sentinel.Enabled {
		t.Error("sentinel.enabled did not survive loading")
	}
	if got := len(cfg.Redis.Sentinel.Addrs); got != 2 {
		t.Errorf("loaded %d sentinel addresses, want 2", got)
	}
}

// Sentinel is off unless asked for, so every existing deployment and every
// experiment keeps the single-address path it had.
func TestSentinelIsOffByDefault(t *testing.T) {
	cfg := config.Default()
	if cfg.Redis.Sentinel.Enabled {
		t.Error("sentinel is enabled by default; a single-Redis deployment would change behaviour on upgrade")
	}
	if cfg.Redis.Sentinel.WaitForReplicas != 0 {
		t.Errorf("wait_for_replicas defaults to %d; it must default to 0 so the acquire path is unchanged",
			cfg.Redis.Sentinel.WaitForReplicas)
	}
}
