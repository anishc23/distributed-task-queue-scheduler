// Command partitionbench cuts a scheduler off from a Redis that stays up.
//
// This is the third shape of the same failure and the only one the other two
// cannot produce. Section 4.7 kills a leader: it stops existing and writes
// nothing. Section 4.8 freezes one with SIGSTOP: it observes nothing at all,
// because it is not running to observe. Section 4.9 breaks the store itself.
// None of those is the case where the leader is running normally, the store is
// healthy, and only the path between them is gone.
//
// That case is distinct because of what the leader can and cannot know. A
// frozen process learns nothing; a partitioned process learns a great deal --
// every call it makes fails -- and still cannot answer the one question that
// matters, which is whether it is merely unable to reach Redis or has already
// been replaced. Those two situations are indistinguishable from inside the
// process and call for opposite responses. Standing down on the first failed
// round trip turns every transient blip into an unnecessary failover; carrying
// on regardless is how two schedulers end up dispatching.
//
// What this queue does is neither: the lease keeps retrying while the fence
// makes the ambiguity harmless. A deposed leader may keep believing it leads,
// and its writes are refused at the resource when it turns out to be wrong.
// This command checks both halves of that bargain.
//
// Four arms:
//
//	none      no partition. The natural dispatch gap, which every other number
//	          is read against.
//	brief     the leader is cut off for less than its lease TTL, then healed.
//	          Nothing should happen: no failover, no fencing, no interruption
//	          beyond the partition itself. An unnecessary failover here is a
//	          real operational cost, not a curiosity -- it is the reason the
//	          lease does not stand down on the first failed renewal.
//	long      the leader is cut off for longer than its lease TTL. The lease
//	          expires, a standby takes over, and when the partition heals the
//	          old leader is still running and still believes it leads. It must
//	          be refused, and exactly one leader must remain.
//	flapping  the path is cut and healed repeatedly, faster than the lease can
//	          settle. This is the case that punishes a design which reacts to
//	          every failed round trip: it should not produce a split brain and
//	          it should not produce a storm of leadership changes.
//
// The partition is real at the socket level: each scheduler reaches Redis
// through its own proxy (internal/netfault), and cutting one severs that
// replica's path while Redis and every other replica carry on untouched.
//
//	go run ./cmd/partitionbench --repetitions 10
//	go run ./cmd/partitionbench --arms long --lease-ttl 2s
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/hostclock"
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/netfault"
	"github.com/anishc23/distributed-task-queue/internal/worker"
	"github.com/anishc23/distributed-task-queue/internal/workload"
	"github.com/redis/go-redis/v9"
)

type arm struct {
	name string
	// cut is how long the leader's path to Redis is severed. Zero means no
	// partition at all.
	cut func(ttl time.Duration) time.Duration
	// flap cuts and heals repeatedly instead of once.
	flap bool
	// expectFailover says whether leadership is supposed to move. It is an
	// assertion, not a description: the brief arm failing it means a transient
	// blip cost a failover, which is the thing the retry behaviour exists to
	// avoid.
	expectFailover bool
}

var arms = []arm{
	{name: "none", cut: func(time.Duration) time.Duration { return 0 }},
	{name: "brief", cut: func(ttl time.Duration) time.Duration { return ttl / 2 }, expectFailover: false},
	{name: "long", cut: func(ttl time.Duration) time.Duration { return 2 * ttl }, expectFailover: true},
	{name: "flapping", cut: func(ttl time.Duration) time.Duration { return 3 * ttl }, flap: true},
}

type trial struct {
	Arm        string
	Rep        int
	Policy     string
	Replicas   int
	LeaseTTL   time.Duration
	LeaseRenew time.Duration
	Tasks      int
	Submitted  int
	Completed  int64
	Dispatched int64
	Duplicates int64
	DeadLetter int64

	// PartitionMS is how long the path was actually severed, measured rather
	// than assumed.
	PartitionMS float64
	// CutCount is how many times the path was severed; more than one only in
	// the flapping arm.
	CutCount int
	// LeaderBefore and LeaderAfter name the lease holder either side of the
	// partition. Whether they differ is the failover.
	LeaderBefore string
	LeaderAfter  string
	Failover     bool
	// UnexpectedFailover marks a trial where leadership moved but should not
	// have. This is the brief arm's failure mode and it costs real
	// availability, so it is recorded rather than tolerated.
	UnexpectedFailover bool
	// Transitions is the epoch counter at the end: one increment per term.
	Transitions int
	// FencedOnHeal is tq_scheduler_fenced_operations_total on the replica that
	// was cut off. One means it came back, tried to write under a superseded
	// epoch and Redis refused it. Zero means its own lease renewal noticed
	// first. Both are correct; the split says how often the lease alone would
	// not have sufficed.
	FencedOnHeal float64
	// LeadersAfterHeal is the sum of tq_scheduler_is_leader once the partition
	// is repaired and everything has settled. Anything but 1 is a split brain
	// or a leaderless queue.
	LeadersAfterHeal float64
	// EpochInversions counts dispatches under a superseded epoch, read back
	// from the execution stream. Must be 0.
	EpochInversions int

	OutageMS    float64
	MaxGapMS    float64
	Recovered   bool
	WallSeconds float64
	// Suspect marks a trial that measured something other than the system:
	// either an outage far beyond what the partition and the lease can
	// explain, or a host that went to sleep partway through.
	Suspect bool
	// SuspendedMS is how long the machine itself was asleep during the trial,
	// detected directly rather than inferred from a suspicious result.
	SuspendedMS float64
}

func main() {
	fs := flag.NewFlagSet("partitionbench", flag.ExitOnError)
	redisAddr := fs.String("redis-addr", "localhost:6379", "Redis address; it stays healthy throughout")
	runID := fs.String("run-id", "partition", "results directory name")
	resultsDir := fs.String("results-dir", "results", "where to write results")
	reps := fs.Int("repetitions", 10, "trials per arm")
	tasks := fs.Int("tasks", 4000, "tasks submitted per trial")
	rate := fs.Float64("rate", 400, "submission rate in tasks per second")
	execMS := fs.Int64("exec-ms", 20, "simulated task duration in milliseconds")
	workers := fs.Int("workers", 4, "in-process workers; these are never partitioned")
	concurrency := fs.Int("concurrency", 8, "slots per worker")
	policy := fs.String("policy", "wfq", "scheduling policy under test")
	replicas := fs.Int("replicas", 2, "scheduler replicas")
	leaseTTL := fs.Duration("lease-ttl", 5*time.Second, "leadership lease ttl")
	leaseRenew := fs.Duration("lease-renew", 1500*time.Millisecond, "lease renewal interval")
	cutAfter := fs.Duration("cut-after", 3*time.Second, "earliest point at which the leader is cut off")
	cutJitter := fs.Duration("cut-jitter", 0, "random extra delay before the cut; defaults to the renewal interval")
	flapPeriod := fs.Duration("flap-period", 700*time.Millisecond, "cut/heal half-period in the flapping arm")
	settle := fs.Duration("settle", 4*time.Second, "how long to observe after the path is repaired")
	seed := fs.Int64("seed", 1, "seed for the cut-time jitter, so a run is reproducible")
	timeout := fs.Duration("timeout", 3*time.Minute, "per-trial time limit")
	armList := fs.String("arms", "", "comma-separated subset: none, brief, long, flapping (all when empty)")
	sweep := fs.String("sweep", "", "instead of the named arms, sweep partition length as multiples of the lease TTL, e.g. \"0.25,0.5,1,1.5,2,3\". This is what shows the outage being capped by the lease rather than tracking the partition.")
	binary := fs.String("scheduler-binary", "", "path to a prebuilt scheduler binary (built into a temp dir when empty)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "partitionbench severs a scheduler's path to a healthy Redis.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	// Same reasoning as the failover experiment: a fixed offset lands every
	// cut at the same phase of the renewal cycle and collapses the measured
	// distribution to one value.
	if *cutJitter <= 0 {
		*cutJitter = *leaseRenew
	}
	rng := rand.New(rand.NewSource(*seed))

	var err error
	bin := *binary
	if bin == "" {
		var cleanup func()
		bin, cleanup, err = buildScheduler(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot build the scheduler binary: %v\n", err)
			os.Exit(1)
		}
		defer cleanup()
	}
	log.Info("using scheduler binary", "path", bin)

	outDir := filepath.Join(*resultsDir, *runID)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cannot create %s: %v\n", outDir, err)
		os.Exit(1)
	}

	cfg := &runConfig{
		redisAddr: *redisAddr, tasks: *tasks, rate: *rate, execMS: *execMS,
		workers: *workers, concurrency: *concurrency, policy: *policy,
		replicas: *replicas, leaseTTL: *leaseTTL, leaseRenew: *leaseRenew,
		cutAfter: *cutAfter, cutJitter: *cutJitter, flapPeriod: *flapPeriod,
		settle: *settle, timeout: *timeout, binary: bin, log: log,
	}

	var selected []arm
	if strings.TrimSpace(*sweep) != "" {
		selected, err = sweepArms(*sweep)
	} else {
		selected, err = selectArms(*armList)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var rows []trial
	for _, a := range selected {
		for rep := 0; rep < *reps; rep++ {
			log.Info("trial", "arm", a.name, "rep", rep)
			row, err := runTrial(ctx, cfg, a, rep, rng.Float64())
			if err != nil {
				fmt.Fprintf(os.Stderr, "trial %s/%d failed: %v\n", a.name, rep, err)
				os.Exit(1)
			}
			log.Info("trial done", "arm", a.name, "rep", rep,
				"failover", row.Failover, "fenced", row.FencedOnHeal,
				"outage_ms", row.OutageMS, "completed", row.Completed,
				"inversions", row.EpochInversions)
			rows = append(rows, row)
		}
	}

	path := filepath.Join(outDir, "partition.csv")
	if err := writeCSV(path, rows); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Println()
	summarise(os.Stdout, rows)
	fmt.Printf("\nwrote %s\n", path)
}

type runConfig struct {
	redisAddr   string
	tasks       int
	rate        float64
	execMS      int64
	workers     int
	concurrency int
	policy      string
	replicas    int
	leaseTTL    time.Duration
	leaseRenew  time.Duration
	cutAfter    time.Duration
	cutJitter   time.Duration
	flapPeriod  time.Duration
	settle      time.Duration
	timeout     time.Duration
	binary      string
	log         *slog.Logger
}

func buildScheduler(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "partitionbench-*")
	if err != nil {
		return "", nil, err
	}
	bin := filepath.Join(dir, "scheduler")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/scheduler")
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.RemoveAll(dir)
		return "", nil, err
	}
	return bin, func() { os.RemoveAll(dir) }, nil
}

// proc is one scheduler replica and the path it reaches Redis through.
type proc struct {
	owner string
	index int
	cmd   *exec.Cmd
	link  *netfault.Proxy
}

func runTrial(ctx context.Context, cfg *runConfig, a arm, rep int, phase float64) (trial, error) {
	row := trial{
		Arm: a.name, Rep: rep, Policy: cfg.policy, Replicas: cfg.replicas,
		LeaseTTL: cfg.leaseTTL, LeaseRenew: cfg.leaseRenew, Tasks: cfg.tasks,
	}
	start := time.Now()
	// Watched for the whole trial. A machine that suspends mid-run produces
	// results that look like findings: frozen leadership gauges read as a split
	// brain, and a gap in the dispatch record reads as an outage. Neither is
	// about the system under test.
	clock := hostclock.Start()
	defer func() { row.WallSeconds = time.Since(start).Seconds() }()

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	namespace := fmt.Sprintf("tqpart:%s:%d:%d", a.name, rep, time.Now().UnixNano())

	// The harness and the workers talk to Redis directly. Only the schedulers
	// are behind proxies, because only the schedulers are being partitioned.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr, PoolSize: 64})
	defer rdb.Close()

	full := config.Default()
	full.Redis.Addr = cfg.redisAddr
	full.Streams.Namespace = namespace
	full.Scheduler.Policy = cfg.policy
	full.Worker.Concurrency = cfg.concurrency

	br := broker.NewWithClient(rdb, full.Streams)
	if err := br.EnsureStreams(ctx); err != nil {
		return row, fmt.Errorf("ensure streams: %w", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = br.Reset(c)
		rdb.Del(c, br.Keys().LeaderEpoch)
		cancel()
	}()

	procs, err := startSchedulers(ctx, cfg, namespace)
	if err != nil {
		return row, err
	}
	defer stopAll(procs)

	if err := waitForLeader(ctx, rdb, br.Keys().Leader, 30*time.Second); err != nil {
		return row, err
	}
	if sum, err := leaderGauges(ctx, cfg.replicas); err == nil && sum != 1 {
		return row, fmt.Errorf("%v replicas reported leadership before the partition, want exactly 1", sum)
	}

	var workerWG sync.WaitGroup
	defer workerWG.Wait()
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	for i := 0; i < cfg.workers; i++ {
		w, err := worker.New(worker.Options{
			Broker:   br,
			Config:   full.Worker,
			Recovery: full.Recovery,
			Logger:   quiet(),
			Metrics:  metrics.New(metrics.Options{Component: metrics.ComponentWorker, Scheduler: cfg.policy}),
		})
		if err != nil {
			return row, fmt.Errorf("build worker: %w", err)
		}
		workerWG.Add(1)
		go func() { defer workerWG.Done(); _ = w.Run(workerCtx) }()
	}

	specs := steadySpecs(cfg.tasks, cfg.rate, cfg.execMS)
	prod := workload.NewProducer(br, quiet(), 64)
	submitDone := make(chan error, 1)
	go func() {
		st, err := prod.Submit(ctx, specs)
		row.Submitted = st.Submitted
		submitDone <- err
	}()

	var cutAt time.Time
	cutFor := a.cut(cfg.leaseTTL)
	if cutFor > 0 {
		select {
		case <-ctx.Done():
			return row, ctx.Err()
		case <-time.After(cfg.cutAfter + time.Duration(phase*float64(cfg.cutJitter))):
		}

		// Generously bounded by the lease TTL: if no leader appears within a
		// full TTL, something is wrong with the run rather than with timing.
		victim, err := leaderProcess(ctx, rdb, br.Keys().Leader, procs, cfg.leaseTTL)
		if err != nil {
			return row, err
		}
		row.LeaderBefore = victim.owner
		cutAt = time.Now()

		if a.flap {
			row.CutCount = flap(ctx, victim, cutFor, cfg.flapPeriod, cfg.log)
		} else {
			victim.link.Cut()
			row.CutCount = 1
			cfg.log.Info("cut the leader off from Redis",
				"owner", victim.owner, "for", cutFor, "redis", "healthy")
			select {
			case <-ctx.Done():
				return row, ctx.Err()
			case <-time.After(cutFor):
			}
		}
		victim.link.Heal()
		row.PartitionMS = time.Since(cutAt).Seconds() * 1000
		cfg.log.Info("repaired the path", "owner", victim.owner, "partition_ms", row.PartitionMS)

		// Let the reconnected replica act. Its dispatch loop ticks several
		// times in this window, and if it has been superseded every one of
		// those ticks is a write carrying a stale epoch.
		select {
		case <-ctx.Done():
			return row, ctx.Err()
		case <-time.After(cfg.settle):
		}

		owner, err := leaseOwner(ctx, rdb, br.Keys().Leader)
		if err != nil {
			return row, err
		}
		row.LeaderAfter = owner
		row.Failover = owner != "" && owner != row.LeaderBefore
		if !a.flap && row.Failover != a.expectFailover {
			// Recorded rather than fatal. In the brief arm this is the
			// interesting outcome, not a broken run: it means a partition
			// shorter than the lease cost a leadership change anyway.
			row.UnexpectedFailover = true
		}

		client := &http.Client{Timeout: 2 * time.Second}
		url := "http://" + metricsAddr(victim.index) + "/metrics"
		if v, err := scrapeGauge(ctx, client, url, "tq_scheduler_fenced_operations_total"); err == nil {
			row.FencedOnHeal = v
		}
		if sum, err := leaderGauges(ctx, cfg.replicas); err == nil {
			row.LeadersAfterHeal = sum
			if sum != 1 {
				// Before calling this a split brain, ask whether the host was
				// running. A suspended machine freezes both replicas mid
				// handover and they wake with stale gauges, which scrapes as
				// two leaders and is not one. Aborting the run on that would
				// discard a correct experiment because a laptop slept, and
				// reporting it as a violation would be worse.
				if d := clock.Suspended(); d > hostclock.DefaultThreshold {
					row.Suspect = true
					row.SuspendedMS = d.Seconds() * 1000
					cfg.log.Error("the host slept during this trial; discarding it rather than reporting its numbers",
						"arm", a.name, "rep", rep, "slept", d, "leaders_seen", sum,
						"hint", "run under `caffeinate -dims`, and check nothing closed the lid")
					return row, nil
				}
				return row, fmt.Errorf("%v replicas claim leadership after the path was repaired, want exactly 1", sum)
			}
		}
	}

	if err := <-submitDone; err != nil {
		return row, fmt.Errorf("submit: %w", err)
	}
	drained := waitForDrain(ctx, br, cfg.tasks)

	stats, err := br.Stats(ctx)
	if err != nil {
		return row, fmt.Errorf("stats: %w", err)
	}
	row.Completed = stats[broker.StatCompleted]
	row.Dispatched = stats[broker.StatDispatched]
	row.Duplicates = stats[broker.StatDuplicateCompletions]
	row.DeadLetter = stats[broker.StatDeadLettered]

	epoch, _ := rdb.Get(ctx, br.Keys().LeaderEpoch).Int()
	row.Transitions = epoch

	entries, err := dispatchRecord(ctx, rdb, br.Keys().Exec)
	if err != nil {
		return row, err
	}
	stamps := dispatchStamps(entries)
	row.MaxGapMS = maxGapMS(stamps)
	row.EpochInversions = epochInversions(entries)
	if row.EpochInversions != 0 {
		return row, fmt.Errorf("%d entries were dispatched under a superseded epoch; the single-writer invariant was violated", row.EpochInversions)
	}

	if cutFor == 0 {
		row.OutageMS, row.Recovered = 0, true
	} else {
		outage, ok := outageAround(stamps, cutAt)
		row.OutageMS, row.Recovered = outage, ok
		if !ok {
			row.OutageMS = -1
		}
	}
	if !drained {
		cfg.log.Warn("trial did not drain", "arm", a.name, "rep", rep, "completed", row.Completed)
	}

	// A partition-induced outage cannot exceed the partition itself by much:
	// the standby takes over within a TTL and the cut leader is only away for
	// as long as the cut. Far beyond that did not come from the mechanism.
	if limit := row.PartitionMS + 5*float64(cfg.leaseTTL.Milliseconds()); row.Recovered && row.OutageMS > limit {
		row.Suspect = true
		cfg.log.Error("outage far exceeds the partition and the lease; something outside the experiment interfered",
			"arm", a.name, "rep", rep, "outage_ms", row.OutageMS,
			"hint", "on a laptop this is usually idle sleep: run under `caffeinate -dims`")
	}
	// The direct check, which catches suspensions the outage heuristic misses
	// — a machine that slept while the queue happened to be idle leaves no
	// unusual gap at all, and every other number in the trial is still wrong.
	if d := clock.Suspended(); d > hostclock.DefaultThreshold {
		row.Suspect = true
		row.SuspendedMS = d.Seconds() * 1000
		cfg.log.Error("the host slept during this trial; its numbers are discarded",
			"arm", a.name, "rep", rep, "slept", d)
	}
	return row, nil
}

// flap cuts and heals repeatedly for total, and reports how many cuts it made.
func flap(ctx context.Context, victim *proc, total, half time.Duration, log *slog.Logger) int {
	log.Info("flapping the leader's path", "owner", victim.owner, "for", total, "half_period", half)
	deadline := time.Now().Add(total)
	cuts := 0
	for time.Now().Before(deadline) {
		victim.link.Cut()
		cuts++
		select {
		case <-ctx.Done():
			return cuts
		case <-time.After(half):
		}
		victim.link.Heal()
		select {
		case <-ctx.Done():
			return cuts
		case <-time.After(half):
		}
	}
	return cuts
}

func startSchedulers(ctx context.Context, cfg *runConfig, namespace string) ([]*proc, error) {
	out := make([]*proc, 0, cfg.replicas)
	for i := 0; i < cfg.replicas; i++ {
		link, err := netfault.New("127.0.0.1:0", cfg.redisAddr)
		if err != nil {
			stopAll(out)
			return nil, fmt.Errorf("build the path for replica %d: %w", i, err)
		}
		go link.Serve(ctx)

		owner := fmt.Sprintf("sched-%d", i)
		cmd := exec.CommandContext(ctx, cfg.binary,
			// Through its own proxy, which is what makes a per-replica
			// partition possible while Redis stays up for everyone else.
			"--redis-addr", link.Addr(),
			"--namespace", namespace,
			"--policy", cfg.policy,
			"--consumer", owner,
			"--metrics-addr", metricsAddr(i),
			"--leader-election", "true",
			"--lease-ttl", cfg.leaseTTL.String(),
			"--lease-renew", cfg.leaseRenew.String(),
			"--log-level", "warn",
		)
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			_ = link.Close()
			stopAll(out)
			return nil, fmt.Errorf("start %s: %w", owner, err)
		}
		out = append(out, &proc{owner: owner, index: i, cmd: cmd, link: link})
	}
	return out, nil
}

func metricsAddr(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19401+i) }

func stopAll(procs []*proc) {
	for _, p := range procs {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(syscall.SIGKILL)
		}
	}
	for _, p := range procs {
		if p.cmd != nil {
			_ = p.cmd.Wait()
		}
		if p.link != nil {
			_ = p.link.Close()
		}
	}
}

func leaderGauges(ctx context.Context, n int) (sum float64, err error) {
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < n; i++ {
		v, err := scrapeGauge(ctx, client, "http://"+metricsAddr(i)+"/metrics", "tq_scheduler_is_leader")
		if err != nil {
			continue
		}
		sum += v
	}
	return sum, nil
}

func scrapeGauge(ctx context.Context, client *http.Client, url, name string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name) || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		return strconv.ParseFloat(fields[len(fields)-1], 64)
	}
	return 0, fmt.Errorf("%s not found at %s", name, url)
}

func waitForLeader(ctx context.Context, rdb *redis.Client, key string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if v, err := rdb.Get(ctx, key).Result(); err == nil && v != "" {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("no scheduler took the lease within the startup limit")
}

func leaseOwner(ctx context.Context, rdb *redis.Client, key string) (string, error) {
	v, err := rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the lease holder: %w", err)
	}
	if i := strings.LastIndex(v, "|"); i >= 0 {
		return v[:i], nil
	}
	return v, nil
}

// leaderProcess resolves the current lease holder to the process that holds it,
// waiting briefly for one to exist.
//
// The lease is genuinely unheld for short stretches during normal operation:
// between a term ending and a standby's next poll, nobody holds it. Treating
// that instant as a fatal error makes the harness fail for a reason that has
// nothing to do with what it is measuring, which is how a run died after the
// host woke from a sleep and found the grant expired.
func leaderProcess(ctx context.Context, rdb *redis.Client, key string, procs []*proc, limit time.Duration) (*proc, error) {
	deadline := time.Now().Add(limit)
	var lastOwner string
	for {
		owner, err := leaseOwner(ctx, rdb, key)
		if err != nil {
			return nil, err
		}
		lastOwner = owner
		for _, p := range procs {
			if p.owner == owner {
				return p, nil
			}
		}
		if time.Now().After(deadline) {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastOwner == "" {
		return nil, fmt.Errorf("no scheduler held the lease at any point in %s", limit)
	}
	return nil, fmt.Errorf("lease holder %q is not one of the processes under test", lastOwner)
}

func waitForDrain(ctx context.Context, br *broker.Broker, want int) bool {
	idle := 0
	var last int64 = -1
	for ctx.Err() == nil {
		stats, err := br.Stats(ctx)
		if err != nil {
			return false
		}
		done := stats[broker.StatCompleted] + stats[broker.StatDeadLettered]
		if done >= int64(want) {
			return true
		}
		if done == last {
			idle++
			if idle > 200 {
				return false
			}
		} else {
			idle, last = 0, done
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

type dispatchEntry struct {
	At    time.Time
	Epoch int64
}

func dispatchRecord(ctx context.Context, rdb *redis.Client, stream string) ([]dispatchEntry, error) {
	var out []dispatchEntry
	start := "-"
	for {
		msgs, err := rdb.XRangeN(ctx, stream, start, "+", 5000).Result()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", stream, err)
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			raw, ok := m.Values["dispatched_at_ms"].(string)
			if !ok {
				continue
			}
			ms, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				continue
			}
			e := dispatchEntry{At: time.UnixMilli(ms)}
			if s, ok := m.Values["epoch"].(string); ok {
				e.Epoch, _ = strconv.ParseInt(s, 10, 64)
			}
			out = append(out, e)
		}
		if len(msgs) < 5000 {
			break
		}
		start = "(" + msgs[len(msgs)-1].ID
	}
	return out, nil
}

func dispatchStamps(entries []dispatchEntry) []time.Time {
	out := make([]time.Time, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.At)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// epochInversions counts entries dispatched under a term older than one that
// had already written. Entries with no epoch are worker-initiated retries,
// which no leader dispatched and the fence does not govern.
func epochInversions(entries []dispatchEntry) int {
	var high int64
	n := 0
	for _, e := range entries {
		if e.Epoch <= 0 {
			continue
		}
		if e.Epoch < high {
			n++
			continue
		}
		high = e.Epoch
	}
	return n
}

func maxGapMS(stamps []time.Time) float64 {
	var worst float64
	for i := 1; i < len(stamps); i++ {
		if g := stamps[i].Sub(stamps[i-1]).Seconds() * 1000; g > worst {
			worst = g
		}
	}
	return worst
}

func outageAround(stamps []time.Time, at time.Time) (float64, bool) {
	var before, after time.Time
	for _, s := range stamps {
		if !s.After(at) {
			before = s
			continue
		}
		after = s
		break
	}
	if after.IsZero() {
		return 0, false
	}
	if before.IsZero() {
		before = at
	}
	return after.Sub(before).Seconds() * 1000, true
}

func steadySpecs(n int, rate float64, execMS int64) []workload.Spec {
	if rate <= 0 {
		rate = 1
	}
	gap := time.Duration(float64(time.Second) / rate)
	specs := make([]workload.Spec, 0, n)
	tenants := []string{"A", "B", "C"}
	for i := 0; i < n; i++ {
		specs = append(specs, workload.Spec{
			ID:                  fmt.Sprintf("p-%06d", i),
			TenantID:            tenants[i%len(tenants)],
			Offset:              time.Duration(i) * gap,
			ExecMillis:          execMS,
			Priority:            1,
			DeadlineSlackMillis: 60000,
			MaxRetries:          3,
		})
	}
	return specs
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// sweepArms builds one arm per partition length, expressed as a multiple of
// the lease TTL.
//
// The named arms answer whether each behaviour is correct. This answers the
// quantitative question behind them: how the outage grows with the length of
// the partition. The prediction is that it tracks the partition while the
// partition is shorter than the lease, because the leader keeps its grant and
// simply cannot work, and then stops growing once the lease expires and a
// standby takes over. If that holds, the lease TTL is an upper bound on the
// cost of a partition of any length, which is a stronger statement than any of
// the named arms makes on its own.
func sweepArms(list string) ([]arm, error) {
	var out []arm
	for _, part := range strings.Split(list, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		mult, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return nil, fmt.Errorf("--sweep: %q is not a number", part)
		}
		if mult <= 0 {
			return nil, fmt.Errorf("--sweep: %g must be greater than zero", mult)
		}
		m := mult
		out = append(out, arm{
			name: strings.TrimSuffix(strings.TrimSuffix(part, "0"), ".") + "x",
			cut: func(ttl time.Duration) time.Duration {
				return time.Duration(m * float64(ttl))
			},
			// A partition longer than the lease is expected to move
			// leadership; a shorter one is not.
			expectFailover: m > 1,
		})
	}
	if len(out) == 0 {
		return nil, errors.New("--sweep listed no partition lengths")
	}
	return out, nil
}

func selectArms(list string) ([]arm, error) {
	if strings.TrimSpace(list) == "" {
		return arms, nil
	}
	want := map[string]bool{}
	for _, name := range strings.Split(list, ",") {
		want[strings.TrimSpace(name)] = true
	}
	var out []arm
	for _, a := range arms {
		if want[a.name] {
			out = append(out, a)
			delete(want, a.name)
		}
	}
	for name := range want {
		return nil, fmt.Errorf("unknown arm %q", name)
	}
	if len(out) == 0 {
		return nil, errors.New("no arms selected")
	}
	return out, nil
}

func writeCSV(path string, rows []trial) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"arm", "rep", "policy", "replicas", "lease_ttl_ms", "lease_renew_ms", "tasks",
		"submitted", "dispatched", "completed", "duplicate_completions", "dead_lettered",
		"partition_ms", "cut_count", "leader_before", "leader_after", "failover",
		"unexpected_failover", "leader_epochs", "fenced_on_heal", "leaders_after_heal",
		"epoch_inversions", "outage_ms", "max_gap_ms", "recovered", "wall_seconds",
		"suspect", "host_suspended_ms",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.Arm, strconv.Itoa(r.Rep), r.Policy, strconv.Itoa(r.Replicas),
			strconv.FormatInt(r.LeaseTTL.Milliseconds(), 10),
			strconv.FormatInt(r.LeaseRenew.Milliseconds(), 10),
			strconv.Itoa(r.Tasks), strconv.Itoa(r.Submitted),
			strconv.FormatInt(r.Dispatched, 10), strconv.FormatInt(r.Completed, 10),
			strconv.FormatInt(r.Duplicates, 10), strconv.FormatInt(r.DeadLetter, 10),
			strconv.FormatFloat(r.PartitionMS, 'f', 1, 64), strconv.Itoa(r.CutCount),
			r.LeaderBefore, r.LeaderAfter, strconv.FormatBool(r.Failover),
			strconv.FormatBool(r.UnexpectedFailover), strconv.Itoa(r.Transitions),
			strconv.FormatFloat(r.FencedOnHeal, 'f', 0, 64),
			strconv.FormatFloat(r.LeadersAfterHeal, 'f', 0, 64),
			strconv.Itoa(r.EpochInversions),
			strconv.FormatFloat(r.OutageMS, 'f', 1, 64),
			strconv.FormatFloat(r.MaxGapMS, 'f', 1, 64),
			strconv.FormatBool(r.Recovered),
			strconv.FormatFloat(r.WallSeconds, 'f', 1, 64),
			strconv.FormatBool(r.Suspect),
			strconv.FormatFloat(r.SuspendedMS, 'f', 0, 64),
		}); err != nil {
			return err
		}
	}
	return nil
}

func summarise(w io.Writer, rows []trial) {
	fmt.Fprintf(w, "%-10s %4s %12s %12s %12s %10s %12s %11s\n",
		"arm", "n", "partition", "median gap", "failovers", "fenced", "inversions", "unfinished")
	// Arms in the order they first appear, so a swept run summarises its own
	// generated arms rather than only the four named ones.
	var order []arm
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.Arm] {
			continue
		}
		seen[r.Arm] = true
		found := false
		for _, a := range arms {
			if a.name == r.Arm {
				order = append(order, a)
				found = true
				break
			}
		}
		if !found {
			order = append(order, arm{name: r.Arm})
		}
	}
	for _, a := range order {
		var gaps, parts []float64
		n, failovers, fenced, inversions, suspect := 0, 0, 0, 0, 0
		var unfinished int64
		for _, r := range rows {
			if r.Arm != a.name {
				continue
			}
			n++
			if r.Suspect {
				suspect++
				continue
			}
			if r.PartitionMS > 0 {
				parts = append(parts, r.PartitionMS)
			}
			if a.name == "none" {
				gaps = append(gaps, r.MaxGapMS)
			} else if r.Recovered {
				gaps = append(gaps, r.OutageMS)
			}
			if r.Failover {
				failovers++
			}
			if r.FencedOnHeal > 0 {
				fenced++
			}
			inversions += r.EpochInversions
			unfinished += int64(r.Tasks) - r.Completed - r.DeadLetter
		}
		if n == 0 {
			continue
		}
		partStr := "-"
		if len(parts) > 0 {
			partStr = fmt.Sprintf("%.0fms", quantile(parts, 0.5))
		}
		fmt.Fprintf(w, "%-10s %4d %12s %10.0fms %9d/%-3d %8d/%-3d %12d %11d\n",
			a.name, n, partStr, quantile(gaps, 0.5), failovers, n, fenced, n, inversions, unfinished)
		if suspect != 0 {
			fmt.Fprintf(w, "%-10s EXCLUDED %d suspect trial(s)\n", "", suspect)
		}
		switch a.name {
		case "brief":
			fmt.Fprintf(w, "%-10s %s\n", "",
				fmt.Sprintf("a partition shorter than the lease cost a failover in %d/%d trials (want 0)", failovers, n))
		case "long":
			fmt.Fprintf(w, "%-10s %s\n", "",
				fmt.Sprintf("the returning leader was refused by the fence in %d/%d trials", fenced, n))
		}
	}
}

func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return -1
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if q >= 1 {
		return s[len(s)-1]
	}
	return s[int(q*float64(len(s)-1))]
}
