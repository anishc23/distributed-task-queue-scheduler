// Flag wiring is the kind of code that looks too simple to test and then
// silently stops working. A flag that no longer reaches the configuration
// produces a process that runs happily with the wrong settings, which is worse
// than one that fails: the Sentinel flags below decide which Redis the queue
// talks to and whether a leadership epoch is made durable before it is used.
package cli_test

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/config"
)

// apply parses argv through the real flag set and returns the resulting config.
func apply(t *testing.T, args ...string) (*config.Config, error) {
	t.Helper()
	var c cli.CommonFlags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	c.Register(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return c.Apply()
}

func TestDefaultsSurviveAnEmptyCommandLine(t *testing.T) {
	cfg, err := apply(t)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	def := config.Default()
	if cfg.Redis.Addr != def.Redis.Addr {
		t.Errorf("redis addr = %q with no flags, want the default %q", cfg.Redis.Addr, def.Redis.Addr)
	}
	if cfg.Redis.Sentinel.Enabled {
		t.Error("sentinel is enabled with no flags given")
	}
}

func TestOverridesReachTheConfiguration(t *testing.T) {
	cfg, err := apply(t,
		"--redis-addr", "10.0.0.5:6380",
		"--redis-db", "3",
		"--namespace", "tq-test",
		"--log-level", "debug",
		"--log-format", "json",
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, tc := range []struct{ name, got, want string }{
		{"redis addr", cfg.Redis.Addr, "10.0.0.5:6380"},
		{"namespace", cfg.Streams.Namespace, "tq-test"},
		{"log level", cfg.Log.Level, "debug"},
		{"log format", cfg.Log.Format, "json"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if cfg.Redis.DB != 3 {
		t.Errorf("redis db = %d, want 3", cfg.Redis.DB)
	}
}

// -1 is the "leave it alone" value for redis-db, because 0 is a real database
// number and cannot double as "unset".
func TestRedisDBSentinelValueLeavesTheConfiguredDatabase(t *testing.T) {
	cfg, err := apply(t, "--redis-db", "-1")
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cfg.Redis.DB != config.Default().Redis.DB {
		t.Errorf("redis db = %d after --redis-db=-1, want the configured default", cfg.Redis.DB)
	}
}

// Passing Sentinel addresses is what switches discovery on. Requiring a
// separate --sentinel-enabled flag would let the two disagree.
func TestSentinelAddressesEnableDiscovery(t *testing.T) {
	cfg, err := apply(t,
		"--sentinel-addrs", "127.0.0.1:26379, 127.0.0.1:26380 ,",
		"--sentinel-master", "mymaster",
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	s := cfg.Redis.Sentinel
	if !s.Enabled {
		t.Fatal("sentinel addresses were given but discovery is off")
	}
	if s.MasterName != "mymaster" {
		t.Errorf("master name = %q, want %q", s.MasterName, "mymaster")
	}
	if len(s.Addrs) != 2 {
		t.Fatalf("parsed %d addresses from a list with whitespace and a trailing comma, want 2: %v", len(s.Addrs), s.Addrs)
	}
	if s.Addrs[0] != "127.0.0.1:26379" || s.Addrs[1] != "127.0.0.1:26380" {
		t.Errorf("addresses = %v, want them trimmed", s.Addrs)
	}
}

func TestEpochDurabilityFlagsReachTheLeaseConfiguration(t *testing.T) {
	cfg, err := apply(t,
		"--sentinel-addrs", "127.0.0.1:26379",
		"--sentinel-master", "mymaster",
		"--wait-for-replicas", "2",
		"--wait-timeout", "1500ms",
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := cfg.Redis.Sentinel.WaitForReplicas; got != 2 {
		t.Errorf("wait_for_replicas = %d, want 2", got)
	}
	if got := cfg.Redis.Sentinel.WaitTimeout.D(); got != 1500*time.Millisecond {
		t.Errorf("wait_timeout = %s, want 1.5s", got)
	}
}

// Zero has to be distinguishable from "not given", because zero is how the
// durability wait is switched off and -1 is how it is left as configured.
func TestWaitForReplicasZeroIsDistinctFromUnset(t *testing.T) {
	cfg, err := apply(t,
		"--sentinel-addrs", "127.0.0.1:26379",
		"--sentinel-master", "mymaster",
		"--wait-for-replicas", "0",
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := cfg.Redis.Sentinel.WaitForReplicas; got != 0 {
		t.Errorf("wait_for_replicas = %d after being set to 0 explicitly, want 0", got)
	}
}

// A flag combination that cannot work must fail here rather than at the first
// Redis call, so a misconfigured process dies at startup.
func TestInvalidOverridesAreRejected(t *testing.T) {
	if _, err := apply(t, "--sentinel-addrs", "127.0.0.1:26379"); err == nil {
		t.Fatal("sentinel addresses without a master name were accepted")
	}
}

func TestSplitList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{" a , b ,, c ", []string{"a", "b", "c"}},
		{",,,", nil},
	}
	for _, tc := range cases {
		got := cli.SplitList(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("SplitList(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("SplitList(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

func TestNewLoggerHonoursLevelAndFormat(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		for _, level := range []string{"debug", "info", "warn", "error", "nonsense"} {
			if got := cli.NewLogger(config.Log{Level: level, Format: format}, "test"); got == nil {
				t.Fatalf("NewLogger(%q, %q) returned nil", level, format)
			}
		}
	}
}
