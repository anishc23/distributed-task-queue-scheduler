// Package cli holds the flag parsing, logging and signal handling shared by the
// producer, scheduler, worker and benchmark commands.
package cli

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/anishc23/distributed-task-queue/internal/config"
)

// CommonFlags are the flags every command understands. Flags always win over
// the configuration file so that a single YAML file can drive many processes.
type CommonFlags struct {
	ConfigPath string
	RedisAddr  string
	RedisDB    int
	Namespace  string
	LogLevel   string
	LogFormat  string
}

// Register binds the common flags onto a flag set.
func (c *CommonFlags) Register(fs *flag.FlagSet) {
	fs.StringVar(&c.ConfigPath, "config", envOr("TQ_CONFIG", ""), "path to a YAML configuration file (built-in defaults are used when empty)")
	fs.StringVar(&c.RedisAddr, "redis-addr", envOr("TQ_REDIS_ADDR", ""), "Redis address host:port (overrides redis.addr)")
	fs.IntVar(&c.RedisDB, "redis-db", -1, "Redis database number (overrides redis.db; -1 keeps the configured value)")
	fs.StringVar(&c.Namespace, "namespace", envOr("TQ_NAMESPACE", ""), "Redis key namespace (overrides streams.namespace)")
	fs.StringVar(&c.LogLevel, "log-level", envOr("TQ_LOG_LEVEL", ""), "log level: debug, info, warn, error")
	fs.StringVar(&c.LogFormat, "log-format", envOr("TQ_LOG_FORMAT", ""), "log format: text or json")
}

// Apply loads the configuration file and applies flag overrides, validating the
// result. Every command calls this before doing anything else, so a
// misconfigured process fails immediately and loudly.
func (c *CommonFlags) Apply() (*config.Config, error) {
	cfg, err := config.Load(c.ConfigPath)
	if err != nil {
		return nil, err
	}
	if c.RedisAddr != "" {
		cfg.Redis.Addr = c.RedisAddr
	}
	if c.RedisDB >= 0 {
		cfg.Redis.DB = c.RedisDB
	}
	if c.Namespace != "" {
		cfg.Streams.Namespace = c.Namespace
	}
	if c.LogLevel != "" {
		cfg.Log.Level = c.LogLevel
	}
	if c.LogFormat != "" {
		cfg.Log.Format = c.LogFormat
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration after applying flags: %w", err)
	}
	return cfg, nil
}

// NewLogger builds the structured logger described by the configuration.
func NewLogger(cfg config.Log, service string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(handler).With("service", service)
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM, which is what
// drives graceful shutdown in every long-running command.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Fail prints an error and exits with status 1.
func Fail(log *slog.Logger, msg string, err error) {
	log.Error(msg, "error", err)
	os.Exit(1)
}

// SplitList parses a comma-separated flag value, trimming whitespace and
// dropping empty entries.
func SplitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
