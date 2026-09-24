// Command storefaultbench measures what happens to the scheduler's
// single-writer invariant when the *store* fails, rather than the scheduler.
//
// Section 4.7 of the report measures a scheduler crash and Section 4.8 measures
// a scheduler frozen past its lease. Both of those rest on an assumption that
// was stated but never tested: that Redis does not lose acknowledged writes.
// The fencing token is a monotonic counter held in Redis, and its whole
// guarantee — that once epoch N has written, nothing below N ever writes again
// — is only as durable as that counter. A Sentinel failover to a replica that
// had not caught up breaks the assumption directly.
//
// So this command builds a real Sentinel cluster out of local redis-server and
// redis-sentinel processes, runs the queue against it through Sentinel
// discovery, and then breaks the store underneath it.
//
// Three arms:
//
//	clean     SIGKILL the master while its replicas are caught up. Sentinel
//	          promotes a replica that has everything. This is the good case and
//	          it is here to show the harness is not rigged: if the invariant
//	          broke here, the experiment would be measuring its own setup.
//	lagging   Stall the replicas with SIGSTOP, let the queue keep writing to
//	          the master, then SIGKILL the master and let the replicas back.
//	          Sentinel now promotes a replica that is missing writes the queue
//	          was already told had succeeded. This is the failure mode.
//	guarded   The same fault as `lagging`, with the two mitigations enabled:
//	          min-replicas-to-write on the master, so it refuses writes it
//	          cannot hand on, and a durability wait on the leadership epoch, so
//	          a leader does not act on a token that exists on one machine only.
//
// The measurement that matters is not "did it crash". It is whether the fence
// floor went backwards, because a floor that rewinds stops excluding anything.
// Two different corruptions follow from it, and the harness records both: a
// superseded leader whose epoch is now above the rewound floor can write again,
// and a legitimate new leader whose epoch is below the floor left by that
// zombie is locked out of its own queue.
//
//	go run ./cmd/storefaultbench --repetitions 5
//	go run ./cmd/storefaultbench --arms lagging,guarded --repetitions 5
//
// Every kill in here is SIGKILL. Redis waits for replicas during a graceful
// shutdown, so a politely stopped master loses nothing and the interesting case
// never occurs; see cluster.go.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/worker"
	"github.com/anishc23/distributed-task-queue/internal/workload"
	"github.com/redis/go-redis/v9"
)

type arm struct {
	name string
	// stallReplicas makes the replicas fall behind before the master dies,
	// which is what turns a survivable failover into a lossy one.
	stallReplicas bool
	// churnLeader kills the scheduler leader while the replicas are stalled.
	//
	// Without this the arm proves nothing, which the first version of this
	// experiment demonstrated by passing. The epoch counter only advances on a
	// leadership change, so a trial with one uneventful term reaches epoch 1
	// and a rewind has nothing to take away. Forcing an election during the
	// stall puts an INCR on a master that no replica will ever see, which is
	// the write whose loss actually matters.
	churnLeader bool
	// minReplicas is min-replicas-to-write on the master: the server-side
	// mitigation. It makes the master refuse writes it cannot hand to a
	// replica, turning silent loss into visible back-pressure.
	minReplicas int
	// waitReplicas is the lease's epoch durability wait: the client-side
	// mitigation. It protects one write — the leadership token — and leaves
	// everything else on the fast path.
	waitReplicas int
}

// The two mitigations are separated into their own arms on purpose. Enabled
// together they plainly work, and that would leave the useful question
// unanswered: whether protecting only the fencing token is enough, or whether
// the data has to be protected too. They defend different things and cost
// different amounts, so they are measured apart.
var arms = []arm{
	{name: "clean", stallReplicas: false, churnLeader: true},
	{name: "lagging", stallReplicas: true, churnLeader: true},
	{name: "epochwait", stallReplicas: true, churnLeader: true, waitReplicas: 1},
	{name: "guarded", stallReplicas: true, churnLeader: true, minReplicas: 1, waitReplicas: 1},
}

type trial struct {
	Arm  string
	Rep  int
	Seed int64

	// Store-level outcome.
	FailoverMS float64
	OldMaster  string
	NewMaster  string

	// The fence floor, which is the thing the whole argument depends on.
	EpochBefore int64
	EpochAfter  int64
	FenceBefore int64
	FenceAfter  int64
	// EpochRegressed is the headline. True means the monotonic counter went
	// backwards or vanished, so the fence no longer excludes any epoch issued
	// before the failover.
	EpochRegressed bool
	LostEpochs     int64
	// MaxEpochSeen is the highest epoch this harness watched a leader actually
	// hold, sampled continuously into this process's own memory. It is kept
	// separately from what Redis reports because the fault under test can
	// destroy Redis's record of it: asking the promoted node what happened is
	// asking the survivor of an accident to describe the part it slept through.
	MaxEpochSeen int64
	// EpochReused is the violation in its sharpest form. After the failover a
	// new leader campaigns and INCRs a counter that has rewound, so it can be
	// handed a token that a previous, different leader already held. Two
	// distinct terms with the same fencing token is precisely what the token
	// exists to make impossible.
	EpochReused  bool
	ReusedEpoch  int64
	EpochsIssued int

	// Queue-level outcome.
	Submitted   int
	Completed   int64
	Dispatched  int64
	Duplicates  int64
	DeadLetter  int64
	Unfinished  int64
	Inversions  int
	WriteErrors int64
	// PendingAtEnd separates two very different endings that look identical
	// from the outside. Work still sitting in the pending set with nothing
	// dispatching it is a queue that is stuck; an empty pending set with tasks
	// unaccounted for is a queue whose work was destroyed by the failover.
	PendingAtEnd int64
	// TasksLost is the second of those: submissions the producer was told had
	// succeeded, which no longer exist anywhere.
	TasksLost int64
	Wedged    bool

	// Store-level loss, measured by comparing the dying master with the node
	// that replaced it. Independent of anything the queue records.
	IngressLostEntries int64
	ExecLostEntries    int64
	ReplOffsetLost     int64

	RecoveredDispatch bool
	OutageMS          float64
	WallSeconds       float64
	Notes             string
}

func main() {
	fs := flag.NewFlagSet("storefaultbench", flag.ExitOnError)
	runID := fs.String("run-id", "storefault", "results directory name")
	resultsDir := fs.String("results-dir", "results", "where to write results")
	reps := fs.Int("repetitions", 5, "trials per arm")
	tasks := fs.Int("tasks", 2000, "tasks submitted per trial")
	rate := fs.Float64("rate", 300, "submission rate in tasks per second")
	execMS := fs.Int64("exec-ms", 15, "simulated task duration in milliseconds")
	workers := fs.Int("workers", 4, "in-process workers")
	concurrency := fs.Int("concurrency", 8, "slots per worker")
	policy := fs.String("policy", "wfq", "scheduling policy under test")
	replicas := fs.Int("replicas", 2, "Redis replicas behind the master")
	redisPort := fs.Int("redis-port", 7301, "first Redis data port; replicas take the next ones")
	sentinelPort := fs.Int("sentinel-port", 27301, "first Sentinel port; three are started")
	// A shorter lease than the shipped 5s default, for a reason worth stating.
	// A term can only be taken once the dead leader's grant expires, so with a
	// 5s lease and a fault window short enough to beat Sentinel's topology
	// repair, no election happens inside the window at all and the arm reports
	// no violation — not because none is possible, but because the experiment
	// never gave one room to occur. Two seconds puts an election comfortably
	// inside the window.
	leaseTTL := fs.Duration("lease-ttl", 2*time.Second, "leadership lease ttl; shorter than the shipped default so an election lands inside the fault window")
	leaseRenew := fs.Duration("lease-renew", 600*time.Millisecond, "lease renewal interval")
	faultAfter := fs.Duration("fault-after", 4*time.Second, "when to break the store, measured from the start of load")
	stallFor := fs.Duration("fault-window", 4*time.Second, "how long replication stays cut before the master is killed; keep it well under Sentinel's refresh period, which repairs the topology this fault depends on")
	downAfter := fs.Duration("sentinel-down-after", time.Second, "sentinel down-after-milliseconds")
	failoverIn := fs.Duration("sentinel-failover-timeout", 8*time.Second, "sentinel failover-timeout")
	timeout := fs.Duration("timeout", 4*time.Minute, "per-trial time limit")
	armList := fs.String("arms", "", "comma-separated subset: clean, lagging, guarded (all when empty)")
	binary := fs.String("scheduler-binary", "", "path to a prebuilt scheduler binary (built into a temp dir when empty)")
	keepDirs := fs.Bool("keep-redis-dirs", false, "keep the temporary Redis and Sentinel directories for inspection")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "storefaultbench breaks Redis underneath a running queue and measures the fence.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	for _, tool := range []string{"redis-server", "redis-sentinel"} {
		if _, err := exec.LookPath(tool); err != nil {
			fmt.Fprintf(os.Stderr, "%s is required but was not found on PATH.\n"+
				"Install Redis (brew install redis, or your distribution's package) and retry.\n", tool)
			os.Exit(1)
		}
	}
	if *replicas < 1 {
		fmt.Fprintln(os.Stderr, "--replicas must be at least 1; with none there is nothing to promote")
		os.Exit(2)
	}

	bin := *binary
	if bin == "" {
		var cleanup func()
		var err error
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
		tasks: *tasks, rate: *rate, execMS: *execMS,
		workers: *workers, concurrency: *concurrency, policy: *policy,
		replicas: *replicas, redisPort: *redisPort, sentinelPort: *sentinelPort,
		leaseTTL: *leaseTTL, leaseRenew: *leaseRenew,
		faultAfter: *faultAfter, stallFor: *stallFor,
		downAfter: *downAfter, failoverIn: *failoverIn,
		timeout: *timeout, binary: bin, keepDirs: *keepDirs, log: log,
	}

	selected, err := selectArms(*armList)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var rows []trial
	for _, a := range selected {
		for rep := 0; rep < *reps; rep++ {
			log.Info("trial", "arm", a.name, "rep", rep)
			row, err := runTrial(ctx, cfg, a, rep)
			if err != nil {
				// A trial that cannot be set up is fatal; a trial that ends in
				// a broken queue is a result and is recorded.
				fmt.Fprintf(os.Stderr, "trial %s/%d failed: %v\n", a.name, rep, err)
				os.Exit(1)
			}
			log.Info("trial done",
				"arm", a.name, "rep", rep,
				"epoch_regressed", row.EpochRegressed,
				"lost_epochs", row.LostEpochs,
				"inversions", row.Inversions,
				"wedged", row.Wedged,
				"completed", row.Completed)
			rows = append(rows, row)
		}
	}

	path := filepath.Join(outDir, "storefault.csv")
	if err := writeCSV(path, rows); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Println()
	summarise(os.Stdout, rows)
	fmt.Printf("\nwrote %s\n", path)
}

type runConfig struct {
	tasks        int
	rate         float64
	execMS       int64
	workers      int
	concurrency  int
	policy       string
	replicas     int
	redisPort    int
	sentinelPort int
	leaseTTL     time.Duration
	leaseRenew   time.Duration
	faultAfter   time.Duration
	stallFor     time.Duration
	downAfter    time.Duration
	failoverIn   time.Duration
	timeout      time.Duration
	binary       string
	keepDirs     bool
	log          *slog.Logger
}

func buildScheduler(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "storefaultbench-*")
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

func runTrial(ctx context.Context, cfg *runConfig, a arm, rep int) (trial, error) {
	row := trial{Arm: a.name, Rep: rep}
	start := time.Now()
	defer func() { row.WallSeconds = time.Since(start).Seconds() }()

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	baseDir, err := os.MkdirTemp("", fmt.Sprintf("tqstore-%s-%d-*", a.name, rep))
	if err != nil {
		return row, err
	}
	if !cfg.keepDirs {
		defer os.RemoveAll(baseDir)
	} else {
		defer func() { row.Notes = strings.TrimSpace(row.Notes + " dir=" + baseDir) }()
	}

	masterName := "tqmaster"
	minRepl := a.minReplicas
	cl, err := startCluster(ctx, clusterOptions{
		baseDir:      baseDir,
		redisPort:    cfg.redisPort,
		sentinelPort: cfg.sentinelPort,
		replicas:     cfg.replicas,
		masterName:   masterName,
		minReplicas:  minRepl,
		downAfter:    cfg.downAfter,
		failoverIn:   cfg.failoverIn,
	})
	if err != nil {
		return row, fmt.Errorf("start cluster: %w", err)
	}
	defer cl.stopAll()

	namespace := fmt.Sprintf("tqstore:%s:%d:%d", a.name, rep, time.Now().UnixNano())

	// Everything in this process talks to Redis through Sentinel too, so the
	// harness fails over alongside the system under test rather than holding a
	// dead address and blaming the queue for it.
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: cl.sentinelAddrs(),
		PoolSize:      64,
		DialTimeout:   2 * time.Second,
		// Reads and writes must fail rather than hang while the store is
		// changing hands, or the harness's own bookkeeping stalls with it.
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   -1,
	})
	defer rdb.Close()

	full := config.Default()
	full.Streams.Namespace = namespace
	full.Scheduler.Policy = cfg.policy
	full.Worker.Concurrency = cfg.concurrency

	br := broker.NewWithClient(rdb, full.Streams)
	if err := br.EnsureStreams(ctx); err != nil {
		return row, fmt.Errorf("ensure streams: %w", err)
	}

	procs, err := startSchedulers(ctx, cfg, a, namespace, masterName, cl.sentinelAddrs())
	if err != nil {
		return row, err
	}
	defer stopAll(procs)

	if err := waitForLeader(ctx, rdb, br.Keys().Leader, 40*time.Second); err != nil {
		return row, err
	}

	watch := newEpochWatch()
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go watchEpochs(watchCtx, rdb, br.Keys().Leader, watch, func(epoch int64, prev, now string) {
		if !row.EpochReused {
			row.EpochReused, row.ReusedEpoch = true, epoch
			cfg.log.Error("a fencing token was issued twice to different leaders",
				"epoch", epoch, "first_holder", prev, "second_holder", now)
		}
	})

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

	// Let the queue reach steady state and issue some epochs before breaking
	// anything underneath it.
	select {
	case <-ctx.Done():
		return row, ctx.Err()
	case <-time.After(cfg.faultAfter):
	}

	oldMaster, err := cl.currentMaster(ctx)
	if err != nil {
		return row, fmt.Errorf("resolve master: %w", err)
	}
	row.OldMaster = oldMaster
	masterNode := cl.nodeByAddr(oldMaster)
	if masterNode == nil {
		return row, fmt.Errorf("sentinel names master %s, which is not a node this harness started", oldMaster)
	}

	// Read the fence floor directly off the master that is about to die. This
	// is the number whose survival the whole fencing argument depends on.
	row.EpochBefore, row.FenceBefore = readFence(ctx, oldMaster, br.Keys())

	if a.stallReplicas {
		// Cut the replication path rather than stopping the replica
		// processes. A stopped process keeps acknowledging and buffering the
		// stream in the kernel and applies all of it on resume, so the earlier
		// version of this arm lost nothing and reported a safety property it
		// had not tested. See internal/netfault.
		cl.cutReplication()
		if err := cl.waitReplicationDown(ctx, 20*time.Second); err != nil {
			return row, fmt.Errorf("the replication cut did not take effect: %w", err)
		}
		cfg.log.Info("replication is down to every replica; the master now writes to nobody",
			"replicas", len(cl.replicas))
	}

	// Force an election while the replicas cannot see it. The new leader's
	// INCR of the epoch counter lands on a master that is about to die and on
	// nothing else, so the token it is holding is about to stop existing
	// anywhere. Without this step the counter never leaves 1 and the arm
	// measures nothing; that is not hypothetical, it is what the first version
	// of this experiment did.
	if a.churnLeader {
		if err := killLeaseHolder(ctx, rdb, br.Keys().Leader, procs, cfg.log); err != nil {
			return row, err
		}
	}

	// A fixed, short window, not an adaptive wait.
	//
	// Sentinel repairs a replica whose master address it does not recognise,
	// on its own refresh period, so the injected fault has a shelf life. An
	// earlier version waited up to 30 seconds for a new term to appear, which
	// in the guarded arms never comes — correctly, because the whole point of
	// the guard is that no token is issued while it cannot be made durable —
	// and by the time the wait gave up, Sentinel had quietly undone the fault.
	// Whether a term was taken is measured afterwards rather than waited on.
	select {
	case <-ctx.Done():
		return row, ctx.Err()
	case <-time.After(cfg.stallFor):
	}
	if a.churnLeader {
		if _, issued := watch.snapshot(); issued <= 1 {
			row.Notes = strings.TrimSpace(row.Notes + " no-new-term-during-fault")
		}
	}
	// Read the floor again: this is what the master acknowledged, possibly
	// while nothing was replicating it.
	if e, f := readFence(ctx, oldMaster, br.Keys()); e > 0 {
		row.EpochBefore, row.FenceBefore = e, f
	}

	// The fault has to still be in effect when the master dies, or the trial
	// is measuring a healthy failover and calling it a lossy one. Sentinel
	// repairs replica topology it does not recognise, so this is a real
	// possibility rather than a defensive flourish.
	if a.stallReplicas {
		if live, err := cl.replicationLive(ctx); err != nil {
			return row, fmt.Errorf("check replication before the kill: %w", err)
		} else if live {
			row.Notes = strings.TrimSpace(row.Notes + " fault-healed-before-kill")
			return row, fmt.Errorf("replication was restored before the master was killed, " +
				"so this trial would measure a clean failover; shorten the fault window")
		}
	}

	// The last thing the master held, taken immediately before it dies. Any
	// difference between this and the promoted node is data the store lost.
	before := snapshotNode(ctx, oldMaster, br.Keys())

	cfg.log.Info("killing the Redis master", "addr", oldMaster,
		"epoch", row.EpochBefore, "fence", row.FenceBefore,
		"ingress_len", before.IngressLen, "exec_len", before.ExecLen)
	masterNode.kill()

	if a.stallReplicas {
		// Heal after the master is gone. The replicas reconnect, find nothing
		// to reconnect to, and Sentinel promotes one of them with whatever it
		// managed to receive before the cut.
		cl.healReplication()
	}

	newMaster, took, err := cl.waitForFailover(ctx, oldMaster, 60*time.Second)
	row.FailoverMS = took.Seconds() * 1000
	if err != nil {
		row.Notes = strings.TrimSpace(row.Notes + " no-failover")
		return row, nil
	}
	row.NewMaster = newMaster
	cfg.log.Info("sentinel promoted a replica", "addr", newMaster, "took_ms", row.FailoverMS)

	// Give the promoted node a moment to settle before reading the floor.
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
	after := snapshotNode(ctx, newMaster, br.Keys())
	if d := before.IngressLen - after.IngressLen; d > 0 {
		row.IngressLostEntries = d
	}
	if d := before.ExecLen - after.ExecLen; d > 0 {
		row.ExecLostEntries = d
	}
	if d := before.ReplOffset - after.ReplOffset; d > 0 {
		row.ReplOffsetLost = d
	}
	if row.IngressLostEntries > 0 || row.ExecLostEntries > 0 {
		cfg.log.Warn("the promoted node is missing data the master had acknowledged",
			"ingress_entries", row.IngressLostEntries,
			"exec_entries", row.ExecLostEntries,
			"replication_bytes", row.ReplOffsetLost)
	}

	row.EpochAfter, row.FenceAfter = readFence(ctx, newMaster, br.Keys())
	row.MaxEpochSeen, row.EpochsIssued = watch.snapshot()
	if row.MaxEpochSeen > row.EpochBefore {
		// Trust the continuous observation over a single sample taken from a
		// node that was already dying.
		row.EpochBefore = row.MaxEpochSeen
	}

	// The headline. A counter that went backwards, or vanished, no longer
	// excludes any epoch issued before the failover.
	if row.EpochAfter < row.EpochBefore {
		row.EpochRegressed = true
		row.LostEpochs = row.EpochBefore - row.EpochAfter
	}

	// Let the system run on afterwards, so that whatever the rewound floor
	// permits has time to actually happen.
	<-submitDone
	drained := waitForDrain(ctx, br, cfg.tasks, 30*time.Second)

	stats, err := br.Stats(ctx)
	if err == nil {
		row.Completed = stats[broker.StatCompleted]
		row.Dispatched = stats[broker.StatDispatched]
		row.Duplicates = stats[broker.StatDuplicateCompletions]
		row.DeadLetter = stats[broker.StatDeadLettered]
	}
	row.Unfinished = int64(row.Submitted) - row.Completed - row.DeadLetter
	if n, err := br.PendingCount(ctx); err == nil {
		row.PendingAtEnd = n
	}
	// Work that is neither finished nor still queued was destroyed with the
	// master. Work that is still queued while nothing dispatches is a queue
	// that cannot make progress. They are different failures and the summary
	// must not merge them.
	if row.Unfinished > row.PendingAtEnd {
		row.TasksLost = row.Unfinished - row.PendingAtEnd
	}
	row.Wedged = !drained && row.PendingAtEnd > 0

	// Force one more election, now that the store has been through a failover.
	// This is the step that turns a rewound counter from an observation into a
	// consequence: the next term's token is drawn from whatever survived, and
	// if that is lower than a token already issued, the same token identifies
	// two different terms.
	if a.churnLeader {
		if err := killLeaseHolder(ctx, rdb, br.Keys().Leader, procs, cfg.log); err != nil {
			cfg.log.Warn("could not force a post-failover election", "error", err)
		} else {
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) && !row.EpochReused {
				if _, issued := watch.snapshot(); issued > row.EpochsIssued {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	row.MaxEpochSeen, row.EpochsIssued = watch.snapshot()
	entries := dispatchRecord(ctx, rdb, br.Keys().Exec)
	row.Inversions = epochInversions(entries)
	row.RecoveredDispatch = dispatchedAfter(entries, time.Now().Add(-25*time.Second))
	return row, nil
}

// epochWatch records every leadership token this harness observes, in this
// process's memory rather than in Redis.
//
// Keeping the record outside the store is the point. The fault under test
// destroys Redis state, so a post-mortem that asks the promoted node which
// epochs were issued can only ever report the ones that survived — which would
// hide exactly the evidence the experiment is looking for.
type epochWatch struct {
	mu     sync.Mutex
	seen   map[int64]string // epoch -> the owner that held it
	max    int64
	issued int
}

func newEpochWatch() *epochWatch {
	return &epochWatch{seen: map[int64]string{}}
}

// observe records a *transition* to a new grant, and reports whether that
// grant's token had already been issued before.
//
// Keying on the term rather than on the holder matters. After a rewind the
// same process frequently wins the election again and is handed the token it
// used to have, and treating that as benign because the owner matches would
// miss the violation entirely: what the fence needs is that a token identifies
// one term, not one process. Two terms separated by a gap in which nobody held
// the lease are two terms, whoever ends up running them.
//
// The caller is responsible for only passing genuine transitions; see
// watchEpochs, which compares whole holder values.
func (w *epochWatch) observe(owner string, epoch int64) (prev string, reused bool) {
	if epoch <= 0 {
		return "", false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if had, ok := w.seen[epoch]; ok {
		return had, true
	}
	w.seen[epoch] = owner
	w.issued++
	if epoch > w.max {
		w.max = epoch
	}
	return "", false
}

func (w *epochWatch) snapshot() (max int64, issued int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max, w.issued
}

// watchEpochs polls the holder key and feeds each new grant to the watch.
//
// Only changes in the stored value count as a new term, so a leader holding
// and renewing one grant is recorded once however many times it is polled.
// Polling can miss a term shorter than its interval, which understates how many
// were issued; it cannot invent one, so a reported reuse is real.
func watchEpochs(ctx context.Context, rdb *redis.Client, key string, w *epochWatch,
	onReuse func(epoch int64, prev, now string)) {
	t := time.NewTicker(25 * time.Millisecond)
	defer t.Stop()
	last := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		v, err := rdb.Get(readCtx, key).Result()
		cancel()
		if err != nil {
			// The store may be changing hands; absence is not a transition.
			continue
		}
		if v == "" || v == last {
			continue
		}
		last = v
		owner, epoch := splitHolder(v)
		if prev, reused := w.observe(owner, epoch); reused && onReuse != nil {
			onReuse(epoch, prev, owner)
		}
	}
}

// splitHolder parses the "<owner>|<epoch>" value the lease stores.
func splitHolder(v string) (string, int64) {
	i := strings.LastIndex(v, "|")
	if i < 0 {
		return v, 0
	}
	epoch, _ := strconv.ParseInt(v[i+1:], 10, 64)
	return v[:i], epoch
}

// storeSnapshot is what one Redis node actually holds, read straight from that
// node rather than through Sentinel.
//
// The queue's own counters cannot answer "did the store lose data", because
// they live in the store. Comparing the dying master against the node promoted
// in its place gives a loss figure that does not depend on the queue's
// bookkeeping surviving.
type storeSnapshot struct {
	DBSize     int64
	IngressLen int64
	ExecLen    int64
	Epoch      int64
	Fence      int64
	ReplOffset int64
}

func snapshotNode(ctx context.Context, addr string, keys broker.Keys) storeSnapshot {
	cl := redis.NewClient(&redis.Options{
		Addr: addr, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
	})
	defer cl.Close()
	var s storeSnapshot
	s.DBSize, _ = cl.DBSize(ctx).Result()
	s.IngressLen, _ = cl.XLen(ctx, keys.Ingress).Result()
	s.ExecLen, _ = cl.XLen(ctx, keys.Exec).Result()
	s.Epoch, _ = cl.Get(ctx, keys.LeaderEpoch).Int64()
	s.Fence, _ = cl.Get(ctx, keys.Fence).Int64()
	if info, err := cl.Info(ctx, "replication").Result(); err == nil {
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			for _, pfx := range []string{"master_repl_offset:", "slave_repl_offset:"} {
				if strings.HasPrefix(line, pfx) {
					if v, err := strconv.ParseInt(strings.TrimPrefix(line, pfx), 10, 64); err == nil && v > s.ReplOffset {
						s.ReplOffset = v
					}
				}
			}
		}
	}
	return s
}

// readFence reads the epoch counter and the fence floor straight from one node,
// bypassing Sentinel discovery, because the question is what *that* machine
// holds rather than what the cluster would route to.
func readFence(ctx context.Context, addr string, keys broker.Keys) (epoch, fence int64) {
	cl := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second})
	defer cl.Close()
	epoch, _ = cl.Get(ctx, keys.LeaderEpoch).Int64()
	fence, _ = cl.Get(ctx, keys.Fence).Int64()
	return epoch, fence
}

type proc struct {
	owner string
	cmd   *exec.Cmd
}

// startSchedulers starts three replicas, not two.
//
// Two elections have to be forced in a trial: one before the store dies, to put
// an epoch on a master that is about to lose it, and one after, to see which
// token the rewound counter hands out next. Each election is forced by killing
// the current leader, so two processes are consumed and a third has to be left
// standing to answer the second one.
func startSchedulers(ctx context.Context, cfg *runConfig, a arm, namespace, masterName string, sentinels []string) ([]*proc, error) {
	const replicas = 3
	out := make([]*proc, 0, replicas)
	for i := 0; i < replicas; i++ {
		owner := fmt.Sprintf("sched-%d", i)
		args := []string{
			"--sentinel-addrs", strings.Join(sentinels, ","),
			"--sentinel-master", masterName,
			"--namespace", namespace,
			"--policy", cfg.policy,
			"--consumer", owner,
			"--metrics-addr", fmt.Sprintf("127.0.0.1:%d", 19301+i),
			"--leader-election", "true",
			"--lease-ttl", cfg.leaseTTL.String(),
			"--lease-renew", cfg.leaseRenew.String(),
			"--log-level", "warn",
		}
		// The client-side mitigation: do not act on a leadership epoch that
		// exists on one machine only.
		args = append(args,
			"--wait-for-replicas", strconv.Itoa(a.waitReplicas),
			"--wait-timeout", "2s")
		cmd := exec.CommandContext(ctx, cfg.binary, args...)
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			stopAll(out)
			return nil, fmt.Errorf("start %s: %w", owner, err)
		}
		out = append(out, &proc{owner: owner, cmd: cmd})
	}
	return out, nil
}

// killLeaseHolder resolves the current lease holder to its process and kills
// it, so that a standby has to take a fresh epoch.
func killLeaseHolder(ctx context.Context, rdb *redis.Client, key string, procs []*proc, log *slog.Logger) error {
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	v, err := rdb.Get(readCtx, key).Result()
	if err != nil {
		return fmt.Errorf("read the lease holder: %w", err)
	}
	owner, epoch := splitHolder(v)
	for _, p := range procs {
		if p.owner != owner {
			continue
		}
		if p.cmd.Process == nil {
			return fmt.Errorf("lease holder %s has no process", owner)
		}
		if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
			return fmt.Errorf("kill lease holder %s: %w", owner, err)
		}
		log.Info("killed the scheduler leader to force a new epoch", "owner", owner, "epoch", epoch)
		return nil
	}
	return fmt.Errorf("lease holder %q is not one of the scheduler processes", owner)
}

func stopAll(procs []*proc) {
	for _, p := range procs {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(syscall.SIGKILL)
		}
	}
	for _, p := range procs {
		_ = p.cmd.Wait()
	}
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
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("no scheduler took the lease within the startup limit")
}

func waitForDrain(ctx context.Context, br *broker.Broker, want int, idleLimit time.Duration) bool {
	var last int64 = -1
	lastChange := time.Now()
	for ctx.Err() == nil {
		stats, err := br.Stats(ctx)
		if err == nil {
			done := stats[broker.StatCompleted] + stats[broker.StatDeadLettered]
			if done >= int64(want) {
				return true
			}
			if done != last {
				last, lastChange = done, time.Now()
			}
		}
		if time.Since(lastChange) > idleLimit {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

type dispatchEntry struct {
	At    time.Time
	Epoch int64
}

func dispatchRecord(ctx context.Context, rdb *redis.Client, stream string) []dispatchEntry {
	var out []dispatchEntry
	start := "-"
	for {
		msgs, err := rdb.XRangeN(ctx, stream, start, "+", 5000).Result()
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			var e dispatchEntry
			if s, ok := m.Values["dispatched_at_ms"].(string); ok {
				if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
					e.At = time.UnixMilli(ms)
				}
			}
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
	return out
}

// epochInversions counts entries dispatched under a term older than one that
// had already written. Entries with no epoch are worker retries, which no
// leader dispatched and the fence does not govern.
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

func dispatchedAfter(entries []dispatchEntry, t time.Time) bool {
	for _, e := range entries {
		if e.At.After(t) {
			return true
		}
	}
	return false
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
			ID:                  fmt.Sprintf("s-%06d", i),
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
		"arm", "rep", "old_master", "new_master", "failover_ms",
		"epoch_before", "epoch_after", "fence_before", "fence_after",
		"epoch_regressed", "lost_epochs", "max_epoch_seen", "epochs_issued",
		"epoch_reused", "reused_epoch",
		"submitted", "dispatched", "completed", "duplicate_completions",
		"dead_lettered", "unfinished", "pending_at_end", "tasks_lost",
		"ingress_lost_entries", "exec_lost_entries", "repl_offset_lost",
		"epoch_inversions", "wedged",
		"recovered_dispatch", "wall_seconds", "notes",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.Arm, strconv.Itoa(r.Rep), r.OldMaster, r.NewMaster,
			strconv.FormatFloat(r.FailoverMS, 'f', 1, 64),
			strconv.FormatInt(r.EpochBefore, 10), strconv.FormatInt(r.EpochAfter, 10),
			strconv.FormatInt(r.FenceBefore, 10), strconv.FormatInt(r.FenceAfter, 10),
			strconv.FormatBool(r.EpochRegressed), strconv.FormatInt(r.LostEpochs, 10),
			strconv.FormatInt(r.MaxEpochSeen, 10), strconv.Itoa(r.EpochsIssued),
			strconv.FormatBool(r.EpochReused), strconv.FormatInt(r.ReusedEpoch, 10),
			strconv.Itoa(r.Submitted), strconv.FormatInt(r.Dispatched, 10),
			strconv.FormatInt(r.Completed, 10), strconv.FormatInt(r.Duplicates, 10),
			strconv.FormatInt(r.DeadLetter, 10), strconv.FormatInt(r.Unfinished, 10),
			strconv.FormatInt(r.PendingAtEnd, 10), strconv.FormatInt(r.TasksLost, 10),
			strconv.FormatInt(r.IngressLostEntries, 10), strconv.FormatInt(r.ExecLostEntries, 10),
			strconv.FormatInt(r.ReplOffsetLost, 10),
			strconv.Itoa(r.Inversions), strconv.FormatBool(r.Wedged),
			strconv.FormatBool(r.RecoveredDispatch),
			strconv.FormatFloat(r.WallSeconds, 'f', 1, 64),
			r.Notes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func summarise(w io.Writer, rows []trial) {
	fmt.Fprintf(w, "%-9s %4s %10s %14s %12s %13s %10s %8s\n",
		"arm", "n", "failover", "epoch rewound", "lost epochs", "token reused", "inversions", "wedged")
	for _, a := range arms {
		var fail []float64
		n, regressed, wedged, inversions, reused := 0, 0, 0, 0, 0
		var lost int64
		var lostTasks, stillQueued int64
		for _, r := range rows {
			if r.Arm != a.name {
				continue
			}
			n++
			fail = append(fail, r.FailoverMS)
			if r.EpochRegressed {
				regressed++
			}
			if r.Wedged {
				wedged++
			}
			if r.EpochReused {
				reused++
			}
			inversions += r.Inversions
			lost += r.LostEpochs
			lostTasks += r.TasksLost
			stillQueued += r.PendingAtEnd
		}
		if n == 0 {
			continue
		}
		fmt.Fprintf(w, "%-9s %4d %8.0fms %11d/%-3d %12d %10d/%-3d %10d %6d/%-3d\n",
			a.name, n, median(fail), regressed, n, lost, reused, n, inversions, wedged, n)
		if lostTasks != 0 {
			fmt.Fprintf(w, "%-9s acknowledged tasks destroyed by the failover: %d\n", "", lostTasks)
		}
		if stillQueued != 0 {
			fmt.Fprintf(w, "%-9s tasks still queued at the end: %d\n", "", stillQueued)
		}
	}
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}
