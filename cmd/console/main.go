// Command console serves the live operations console: a loopback-only web
// interface that shows a running queue's health, leadership, queue depth and
// dispatch log, and, in demo mode, can inject faults into a cluster it started
// itself.
//
// Monitor mode watches an existing deployment and changes nothing:
//
//	console --redis-addr localhost:6379 --targets sched-a=http://127.0.0.1:9101,sched-b=http://127.0.0.1:9103
//
// Demo mode starts two schedulers and two workers against a dedicated
// namespace and, with --enable-faults, lets the interface kill, freeze or
// partition them:
//
//	make build && console --demo --enable-faults
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
	"github.com/anishc23/distributed-task-queue/internal/console"
)

// The demo resets its namespace on start. It refuses any namespace that does
// not carry this prefix, so it cannot be pointed at real data by accident.
const demoPrefix = "tq-console"

func main() {
	fs := flag.NewFlagSet("console", flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)
	listen := fs.String("listen", "127.0.0.1:8088", "loopback address to serve on; non-loopback addresses are refused")
	static := fs.String("static", "web/dist", "directory holding the built frontend (cd web && npm run build); empty serves the API only")
	poll := fs.Duration("poll", time.Second, "how often to read Redis and scrape every process")
	targets := fs.String("targets", "", "monitor mode: comma-separated name=url scrape targets; a name starting with 'sched' is treated as a scheduler")
	origins := fs.String("allow-origin", "", "extra Origin allowed to send control requests, e.g. http://127.0.0.1:5173 for the Vite dev server")

	demo := fs.Bool("demo", false, "start and supervise a local demo cluster")
	faults := fs.Bool("enable-faults", false, "demo mode: allow the interface to kill, freeze and partition the demo processes")
	binDir := fs.String("bin-dir", "bin", "demo mode: directory with the scheduler and worker binaries (make build)")
	schedulers := fs.Int("schedulers", 2, "demo mode: scheduler replicas")
	workers := fs.Int("workers", 2, "demo mode: worker processes")
	concurrency := fs.Int("concurrency", 4, "demo mode: slots per worker")
	policy := fs.String("policy", "wfq", "demo mode: scheduling policy")
	leaseTTL := fs.Duration("lease-ttl", 5*time.Second, "demo mode: scheduler lease ttl")
	leaseRenew := fs.Duration("lease-renew", 1500*time.Millisecond, "demo mode: scheduler lease renewal interval")
	metricsPort := fs.Int("metrics-port", 19501, "demo mode: first metrics port for the demo processes")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "console serves the live operations console on a loopback address.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	if *faults && !*demo {
		fail("--enable-faults needs --demo: the console only signals processes it started")
	}
	hosts, err := console.LoopbackHosts(*listen)
	if err != nil {
		fail("--listen must be a loopback address: %v", err)
	}
	if *demo && common.Namespace == "" {
		common.Namespace = demoPrefix + "-demo"
	}
	if *demo && !strings.HasPrefix(common.Namespace, demoPrefix) {
		fail("demo mode resets its namespace on start, so it must begin with %q; got %q", demoPrefix, common.Namespace)
	}
	cfg, err := common.Apply()
	if err != nil {
		fail("configuration error: %v", err)
	}
	log := cli.NewLogger(cfg.Log, "console")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	br, err := broker.New(cfg)
	if err != nil {
		fail("redis: %v", err)
	}
	defer br.Close()
	if err := br.WaitReady(ctx, 10*time.Second, log); err != nil {
		fail("redis at %s is not reachable: %v", cfg.Redis.Addr, err)
	}

	// Bind before starting anything: a port clash must fail here, not after
	// child processes exist that an early exit would orphan.
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fail("listen: %v", err)
	}

	events := console.NewEventLog(300)
	var sup *console.Supervisor
	var sub *console.Submitter
	var scrape []console.Target
	mode := "monitor"
	if *demo {
		mode = "demo"
		if err := br.Reset(ctx); err != nil {
			fail("reset demo namespace: %v", err)
		}
		if err := br.EnsureStreams(ctx); err != nil {
			fail("create streams: %v", err)
		}
		sup, err = console.NewSupervisor(console.SupervisorOptions{
			BinDir: *binDir, RedisAddr: cfg.Redis.Addr, Namespace: cfg.Streams.Namespace, Policy: *policy,
			Schedulers: *schedulers, Workers: *workers, Concurrency: *concurrency,
			LeaseTTL: *leaseTTL, LeaseRenew: *leaseRenew, MetricsPort: *metricsPort,
			Log: log, OnEvent: events.Add,
		})
		if err != nil {
			fail("demo cluster: %v", err)
		}
		if err := sup.Start(ctx); err != nil {
			ln.Close()
			fail("start demo cluster: %v", err)
		}
		defer sup.Stop()
		scrape = sup.Targets()
		sub = console.NewSubmitter(br, sup.Slots(), log, events.Add)
	} else {
		scrape, err = parseTargets(*targets)
		if err != nil {
			fail("--targets: %v", err)
		}
	}

	con := console.New(console.Options{Broker: br, Targets: scrape, Supervisor: sup, Poll: *poll, Events: events, Log: log, Mode: mode})
	go con.Run(ctx)

	token := ""
	if *demo {
		token = newToken()
	}
	staticDir := ""
	if *static != "" {
		if st, err := os.Stat(filepath.Join(*static, "index.html")); err == nil && !st.IsDir() {
			staticDir = *static
		} else {
			log.Warn("no built frontend found; serving the API only", "dir", *static)
		}
	}
	var extra []string
	if *origins != "" {
		extra = strings.Split(*origins, ",")
	}
	handler := console.Handler(console.ServerOptions{
		Console: con, Broker: br, Supervisor: sup, Submitter: sub, Mode: mode, FaultsEnabled: *faults,
		Token: token, Hosts: hosts, Origins: extra, Static: staticDir, Poll: *poll, Log: log,
	})
	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}

	url := "http://" + *listen + "/#/live"
	fmt.Fprintf(os.Stderr, "\nlive console (%s mode) at %s\n", mode, url)
	if token != "" {
		// The token goes in the URL fragment, which browsers never send to a
		// server, and is printed only here.
		fmt.Fprintf(os.Stderr, "controls need the session token; open this URL to use them:\n  %s?token=%s\n", url, token)
	}
	if *demo && !*faults {
		fmt.Fprintf(os.Stderr, "fault injection is off; restart with --enable-faults to turn it on\n")
	}
	fmt.Fprintln(os.Stderr)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	log.Info("console stopped")
}

func parseTargets(s string) ([]console.Target, error) {
	if s == "" {
		// The Compose stack publishes both schedulers' metrics; workers are
		// not published there, so they are not scraped by default.
		return []console.Target{
			{Name: "scheduler", Role: "scheduler", URL: "http://127.0.0.1:9101"},
			{Name: "scheduler-standby", Role: "scheduler", URL: "http://127.0.0.1:9103"},
		}, nil
	}
	var out []console.Target
	for _, part := range strings.Split(s, ",") {
		name, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" || !strings.HasPrefix(url, "http://") {
			return nil, fmt.Errorf("%q is not name=http://host:port", part)
		}
		role := "worker"
		if strings.HasPrefix(name, "sched") {
			role = "scheduler"
		}
		out = append(out, console.Target{Name: name, Role: role, URL: strings.TrimRight(url, "/")})
	}
	return out, nil
}

func newToken() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		fail("token: %v", err)
	}
	return hex.EncodeToString(b)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "console: "+format+"\n", args...)
	os.Exit(2)
}
