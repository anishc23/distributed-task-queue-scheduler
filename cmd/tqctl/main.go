// Command tqctl inspects live queue state: pending depth, counters, tenant
// service accounting and the dead-letter stream.
//
//	tqctl status                 # counters, pending depth, in-flight, tenants
//	tqctl dlq [--limit N]        # list dead-letter entries with failure metadata
//	tqctl dlq --json             # same, as JSON for scripting
//	tqctl reset --yes            # delete every key in the namespace
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/cli"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	sub := os.Args[1]

	fs := flag.NewFlagSet("tqctl "+sub, flag.ExitOnError)
	var common cli.CommonFlags
	common.Register(fs)
	limit := fs.Int64("limit", 50, "maximum dead-letter entries to show")
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	confirm := fs.Bool("yes", false, "confirm a destructive operation")
	fs.Usage = usage

	switch sub {
	case "status", "dlq", "reset":
		_ = fs.Parse(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", sub)
		usage()
		os.Exit(2)
	}

	cfg, err := common.Apply()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}
	log := cli.NewLogger(cfg.Log, "tqctl")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	br, err := broker.New(cfg)
	if err != nil {
		cli.Fail(log, "cannot create broker", err)
	}
	defer br.Close()
	if err := br.Ping(ctx); err != nil {
		cli.Fail(log, "cannot reach Redis", err)
	}

	switch sub {
	case "status":
		if err := status(ctx, br, *asJSON); err != nil {
			cli.Fail(log, "status failed", err)
		}
	case "dlq":
		if err := dlq(ctx, br, *limit, *asJSON); err != nil {
			cli.Fail(log, "dead-letter inspection failed", err)
		}
	case "reset":
		if !*confirm {
			fmt.Fprintf(os.Stderr, "refusing to delete namespace %q without --yes\n", cfg.Streams.Namespace)
			os.Exit(1)
		}
		if err := br.Reset(ctx); err != nil {
			cli.Fail(log, "reset failed", err)
		}
		fmt.Printf("namespace %q cleared\n", cfg.Streams.Namespace)
	}
}

func status(ctx context.Context, br *broker.Broker, asJSON bool) error {
	stats, err := br.Stats(ctx)
	if err != nil {
		return err
	}
	pending, err := br.PendingCount(ctx)
	if err != nil {
		return err
	}
	inflight, err := br.InFlight(ctx)
	if err != nil {
		return err
	}
	execPending, err := br.ExecPending(ctx)
	if err != nil {
		return err
	}
	dead, err := br.DeadLetterCount(ctx)
	if err != nil {
		return err
	}
	service, counts, err := br.TenantService(ctx)
	if err != nil {
		return err
	}
	oldest, hasOldest, err := br.OldestPendingSubmit(ctx)
	if err != nil {
		return err
	}

	type report struct {
		Namespace          string           `json:"namespace"`
		Pending            int64            `json:"pending"`
		InFlight           int64            `json:"in_flight"`
		UnackedDeliveries  int64            `json:"unacked_deliveries"`
		DeadLettered       int64            `json:"dead_lettered"`
		OldestPendingWaitS float64          `json:"oldest_pending_wait_seconds"`
		Counters           map[string]int64 `json:"counters"`
		TenantServiceMS    map[string]int64 `json:"tenant_service_ms"`
		TenantCompleted    map[string]int64 `json:"tenant_completed"`
	}
	rep := report{
		Namespace:         br.Keys().Namespace,
		Pending:           pending,
		InFlight:          inflight,
		UnackedDeliveries: execPending,
		DeadLettered:      dead,
		Counters:          stats,
		TenantServiceMS:   service,
		TenantCompleted:   counts,
	}
	if hasOldest {
		rep.OldestPendingWaitS = time.Since(oldest).Seconds()
	}

	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(rep)
	}

	fmt.Printf("namespace            %s\n", rep.Namespace)
	fmt.Printf("pending              %d\n", rep.Pending)
	fmt.Printf("in flight            %d\n", rep.InFlight)
	fmt.Printf("unacked deliveries   %d\n", rep.UnackedDeliveries)
	fmt.Printf("dead lettered        %d\n", rep.DeadLettered)
	fmt.Printf("oldest pending wait  %.3fs\n", rep.OldestPendingWaitS)

	fmt.Println("\ncounters")
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-22s %d\n", k, stats[k])
	}

	if len(service) > 0 || len(counts) > 0 {
		fmt.Println("\ncompleted service per tenant")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  tenant\ttasks\tservice_ms")
		tenants := make([]string, 0, len(service))
		seen := map[string]bool{}
		for t := range service {
			tenants, seen[t] = append(tenants, t), true
		}
		for t := range counts {
			if !seen[t] {
				tenants = append(tenants, t)
			}
		}
		sort.Strings(tenants)
		for _, t := range tenants {
			fmt.Fprintf(w, "  %s\t%d\t%d\n", t, counts[t], service[t])
		}
		w.Flush()
	}
	return nil
}

func dlq(ctx context.Context, br *broker.Broker, limit int64, asJSON bool) error {
	entries, err := br.ReadDeadLetters(ctx, limit)
	if err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(entries)
	}
	if len(entries) == 0 {
		fmt.Println("dead-letter stream is empty")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "task_id\ttenant\tattempts\tlast_attempt\tfailed_at\treason")
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n",
			e.Task.ID, e.Task.TenantID, e.Attempts, e.LastAttempt,
			e.FailedAt.Format(time.RFC3339), e.Reason)
	}
	w.Flush()
	fmt.Printf("\n%d dead-letter entries shown\n", len(entries))
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `tqctl inspects distributed-task-queue state in Redis.

Usage:
  tqctl status [flags]        show counters, queue depth and per-tenant service
  tqctl dlq [flags]           list dead-letter entries with failure metadata
  tqctl reset --yes [flags]   delete every key in the namespace

Flags:
  --config PATH        YAML configuration file
  --redis-addr ADDR    Redis address host:port
  --namespace NAME     Redis key namespace
  --limit N            maximum dead-letter entries to show (default 50)
  --json               emit JSON instead of a table
  --yes                confirm a destructive operation
`)
}
