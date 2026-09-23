// Command failoverbench measures what a scheduler crash costs.
//
// The rest of this project measures the throughput cost of centralising
// scheduling, which turned out to be about 1.5%. That is only half of the
// argument against a central scheduler; the other half is availability, and an
// availability claim that rests on reading the code is not a measurement. This
// command produces the other half.
//
// The experiment is deliberately crude in the way that makes it trustworthy:
// real scheduler processes are started with os/exec and killed with a real
// signal, so nothing about the failure is simulated. Workers and the load
// generator run in this process, against the same Redis.
//
// Five arms, each of which answers a different question:
//
//	none      no kill. Measures the natural gap between dispatches under this
//	          load, which is the floor every other arm has to be read against.
//	graceful  SIGTERM the leader. It releases the lease on the way out, so the
//	          standby should take over almost immediately. This is a rollout.
//	crash     SIGKILL the leader. Nothing is released, so the standby cannot
//	          safely start until Redis expires the key. This should cost about
//	          one lease TTL, and is the number that matters.
//	single    SIGKILL the only scheduler. There is nobody to take over. This is
//	          the control: it shows what the lease is actually buying.
//	pause     SIGSTOP the leader for longer than its lease, then SIGCONT it.
//	          This is the gray failure, and it is the only arm that exercises
//	          the fence. A killed leader is gone and a lease alone would have
//	          sufficed; a paused one comes back still believing it leads, and
//	          nothing in its own process can tell it otherwise, because it was
//	          not running to be told. It is the case Kleppmann's fencing-token
//	          argument is about, and a clean SIGKILL cannot produce it.
//
// The outage is defined without any wiggle room: the gap between the last task
// dispatched at or before the kill and the first task dispatched after it,
// read from the dispatched_at_ms field the dispatch script stamps on every
// execution-stream entry. In the single arm the run is censored, since no such
// dispatch ever happens.
//
// Measuring from the last dispatch rather than from the kill makes the number a
// slight over-estimate of failover time, because it also includes whatever
// natural idle gap happened to precede the kill. That is deliberate: it is a
// property of the stream rather than of this process's bookkeeping, and the
// none arm measures exactly how large that inflation can be.
//
// What the pause arm asserts is stronger than "it did not crash". Every
// execution-stream entry carries the epoch that dispatched it, so after the
// resumed leader has had several dispatch ticks to misbehave, the stream is
// replayed in order and the epochs must never go backwards. One entry out of
// order would be a superseded leader that wrote after its successor: the exact
// corruption the fence exists to prevent, visible in the artefact rather than
// inferred from a counter in a process that has since exited.
//
// The kill time is jittered over one renewal interval. Without that, every
// trial kills at the same phase of the renewal cycle and the measured outage
// collapses to a single value: real, but not the distribution an operator would
// see. The jitter comes from a seeded generator, so a run is still reproducible.
//
//	go run ./cmd/failoverbench --repetitions 15 --run-id failover
//	go run ./cmd/failoverbench --arms crash --lease-ttl 2s --lease-renew 600ms
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
	"github.com/anishc23/distributed-task-queue/internal/metrics"
	"github.com/anishc23/distributed-task-queue/internal/worker"
	"github.com/anishc23/distributed-task-queue/internal/workload"
	"github.com/redis/go-redis/v9"
)

// arm is one experimental condition.
type arm struct {
	name     string
	replicas int
	signal   syscall.Signal // zero means no kill
	// resume marks the gray-failure arm: the victim is stopped rather than
	// killed, and is continued again once its lease has expired and a
	// successor has taken over. Everything downstream that says "the kill"
	// means "the stop" for this arm.
	resume bool
}

var arms = []arm{
	{name: "none", replicas: 2, signal: 0},
	{name: "graceful", replicas: 2, signal: syscall.SIGTERM},
	{name: "crash", replicas: 2, signal: syscall.SIGKILL},
	{name: "single", replicas: 1, signal: syscall.SIGKILL},
	{name: "pause", replicas: 2, signal: syscall.SIGSTOP, resume: true},
}

// trial is one row of the output.
type trial struct {
	Arm      string
	Rep      int
	Policy   string
	Replicas int
	LeaseTTL time.Duration
	// LeaseRenew is recorded because the predicted failover envelope is
	// [TTL - renew, TTL + retry]; without it a CSV cannot be checked against
	// the model that produced it.
	LeaseRenew time.Duration
	Tasks      int
	Submitted  int
	Completed  int64
	Dispatched int64
	Duplicates int64
	DeadLetter int64
	// OutageMS is the dispatch gap bracketing the kill. Negative means the
	// queue never resumed within the trial's time limit.
	OutageMS float64
	// MaxGapMS is the largest gap between consecutive dispatches anywhere in
	// the trial, which is what the outage has to be read against.
	MaxGapMS    float64
	Recovered   bool
	Transitions int
	// WallSeconds is how long the trial took end to end. A trial that takes far
	// longer than the work it does is evidence that something outside the
	// experiment interfered.
	WallSeconds float64
	// Suspect marks a trial whose outage cannot be explained by the lease. The
	// outage after a crash is bounded above by the lease TTL, so anything far
	// beyond it did not come from the mechanism under test. This exists
	// because it happened: an unattended run on a laptop that idle-slept
	// produced two trials reporting 17-minute outages, which would have been a
	// nonsense median had they been averaged in silently.
	Suspect bool
	// LeadersBeforeKill is the sum of tq_scheduler_is_leader across replicas,
	// sampled once the queue is running. Anything but 1 is a split brain or a
	// leaderless queue.
	LeadersBeforeKill float64

	// The fields below are only meaningful in the pause arm.

	// PauseMS is how long the victim was actually stopped, measured rather
	// than assumed, because a stopped process is resumed by this harness and
	// the scheduling of that wake-up is not instantaneous.
	PauseMS float64
	// LeadersAfterResume is the same leadership sum, sampled after the victim
	// has been continued and given time to act. It must still be 1: the
	// resumed process must have stood down, not joined a split brain.
	LeadersAfterResume float64
	// FencedOperations is tq_scheduler_fenced_operations_total on the resumed
	// replica. One means it tried to write with a superseded epoch and Redis
	// refused it: the fence did the work. Zero means its own lease renewal
	// noticed first and it stood down before attempting anything, which is the
	// same outcome reached one layer earlier. Both are correct; the split
	// between them is the interesting number, because it says how often the
	// lease alone would not have been enough.
	FencedOperations float64
	// EpochInversions counts execution-stream entries dispatched under an
	// epoch lower than one already written. This is the invariant itself, read
	// back from the data. It must be 0 in every arm.
	EpochInversions int
}

func main() {
	fs := flag.NewFlagSet("failoverbench", flag.ExitOnError)
	redisAddr := fs.String("redis-addr", "localhost:6379", "Redis address")
	runID := fs.String("run-id", "failover", "results directory name")
	resultsDir := fs.String("results-dir", "results", "where to write results")
	reps := fs.Int("repetitions", 10, "trials per arm")
	tasks := fs.Int("tasks", 4000, "tasks submitted per trial")
	rate := fs.Float64("rate", 400, "submission rate in tasks per second")
	execMS := fs.Int64("exec-ms", 20, "simulated task duration in milliseconds")
	workers := fs.Int("workers", 4, "in-process workers")
	concurrency := fs.Int("concurrency", 8, "slots per worker")
	policy := fs.String("policy", "wfq", "scheduling policy under test")
	leaseTTL := fs.Duration("lease-ttl", 5*time.Second, "leadership lease ttl")
	leaseRenew := fs.Duration("lease-renew", 1500*time.Millisecond, "lease renewal interval")
	killAfter := fs.Duration("kill-after", 3*time.Second, "earliest point in the trial at which the leader is killed")
	killJitter := fs.Duration("kill-jitter", 0, "random extra delay before the kill, uniform in [0, jitter); defaults to the renewal interval")
	seed := fs.Int64("seed", 1, "seed for the kill-time jitter, so a run is reproducible")
	timeout := fs.Duration("timeout", 3*time.Minute, "per-trial time limit")
	pauseFor := fs.Duration("pause-for", 0, "how long the pause arm keeps the leader stopped; defaults to twice the lease ttl, which guarantees the lease expires and a successor takes over")
	resumeSettle := fs.Duration("resume-settle", 3*time.Second, "how long the pause arm lets the resumed leader run before the trial is allowed to finish, so it has several dispatch ticks in which to misbehave")
	armList := fs.String("arms", "", "comma-separated subset of arms to run: none, graceful, crash, single, pause (all when empty)")
	binary := fs.String("scheduler-binary", "", "path to a prebuilt scheduler binary (built into a temp dir when empty)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "failoverbench measures the dispatch outage caused by killing the scheduler.\n\nUsage:\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	// Without jitter every trial kills the leader at the same offset from its
	// startup, so the kill always lands at the same phase of the renewal cycle
	// and the measured outage collapses to a single value. That value is real
	// but it is not the distribution: the outage is the TTL minus however long
	// ago the lease was last renewed, which in practice is uniform over a
	// renewal interval. Jittering the kill over exactly one renewal interval
	// recovers the spread an operator would actually see.
	if *killJitter <= 0 {
		*killJitter = *leaseRenew
	}
	// The pause has to outlast the lease, or the victim renews on the way back
	// and nothing was ever superseded: the arm would report a clean run while
	// testing nothing at all.
	if *pauseFor <= 0 {
		*pauseFor = 2 * *leaseTTL
	}
	if *pauseFor <= *leaseTTL {
		fmt.Fprintf(os.Stderr, "--pause-for (%s) must exceed --lease-ttl (%s), or the paused leader is never superseded\n", *pauseFor, *leaseTTL)
		os.Exit(2)
	}
	rng := rand.New(rand.NewSource(*seed))

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
		redisAddr:    *redisAddr,
		tasks:        *tasks,
		rate:         *rate,
		execMS:       *execMS,
		workers:      *workers,
		concurrency:  *concurrency,
		policy:       *policy,
		leaseTTL:     *leaseTTL,
		leaseRenew:   *leaseRenew,
		killAfter:    *killAfter,
		killJitter:   *killJitter,
		pauseFor:     *pauseFor,
		resumeSettle: *resumeSettle,
		timeout:      *timeout,
		binary:       bin,
		log:          log,
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
			row, err := runTrial(ctx, cfg, a, rep, rng.Float64())
			if err != nil {
				fmt.Fprintf(os.Stderr, "trial %s/%d failed: %v\n", a.name, rep, err)
				os.Exit(1)
			}
			log.Info("trial done",
				"arm", a.name, "rep", rep,
				"outage_ms", row.OutageMS, "recovered", row.Recovered,
				"completed", row.Completed, "duplicates", row.Duplicates)
			rows = append(rows, row)
		}
	}

	path := filepath.Join(outDir, "failover.csv")
	if err := writeCSV(path, rows); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Println()
	summarise(os.Stdout, rows)
	fmt.Printf("\nwrote %s\n", path)
}

type runConfig struct {
	redisAddr    string
	tasks        int
	rate         float64
	execMS       int64
	workers      int
	concurrency  int
	policy       string
	leaseTTL     time.Duration
	leaseRenew   time.Duration
	killAfter    time.Duration
	killJitter   time.Duration
	pauseFor     time.Duration
	resumeSettle time.Duration
	timeout      time.Duration
	binary       string
	log          *slog.Logger
}

// buildScheduler compiles the scheduler command into a temporary directory.
// Building it here rather than trusting whatever is in bin/ removes the most
// annoying way to get a wrong answer: measuring yesterday's code.
func buildScheduler(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "failoverbench-*")
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

// runTrial runs one trial. phase in [0,1) places the kill within the renewal
// cycle; it is drawn once per trial from a seeded generator so a whole run is
// reproducible.
func runTrial(ctx context.Context, cfg *runConfig, a arm, rep int, phase float64) (trial, error) {
	row := trial{
		Arm: a.name, Rep: rep, Policy: cfg.policy,
		Replicas: a.replicas, LeaseTTL: cfg.leaseTTL, LeaseRenew: cfg.leaseRenew,
		Tasks: cfg.tasks,
	}
	trialStart := time.Now()
	defer func() { row.WallSeconds = time.Since(trialStart).Seconds() }()

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	namespace := fmt.Sprintf("tqfail:%s:%d:%d", a.name, rep, time.Now().UnixNano())
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

	// Schedulers first, so somebody is leading before any task arrives.
	procs, err := startSchedulers(ctx, cfg, namespace, a.replicas)
	if err != nil {
		return row, err
	}
	defer stopAll(procs)

	if err := waitForLeader(ctx, rdb, br.Keys().Leader, 30*time.Second); err != nil {
		return row, err
	}

	// Exactly one replica must claim leadership before any work is submitted.
	// Checking it here rather than only asserting it in a unit test means every
	// reported trial carries evidence that the invariant held while it ran.
	if sum, err := leaderGauges(ctx, a.replicas); err == nil {
		row.LeadersBeforeKill = sum
		if sum != 1 {
			return row, fmt.Errorf("%v replicas reported leadership before the kill, want exactly 1", sum)
		}
	}

	// Workers. The wait is registered before the cancel so that deferred
	// execution runs them in the order that actually terminates: cancel first,
	// then wait. Registered the other way round, the wait blocks on workers
	// that have not been told to stop, and every trial burns its full timeout.
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

	// Load. A steady arrival rate is what makes the outage legible: any gap in
	// the dispatch record is the scheduler's, not the generator's.
	specs := steadySpecs(cfg.tasks, cfg.rate, cfg.execMS)
	prod := workload.NewProducer(br, quiet(), 64)

	submitDone := make(chan error, 1)
	go func() {
		st, err := prod.Submit(ctx, specs)
		row.Submitted = st.Submitted
		submitDone <- err
	}()

	// The kill.
	var killAt time.Time
	if a.signal != 0 {
		select {
		case <-ctx.Done():
			return row, ctx.Err()
		case <-time.After(cfg.killAfter + time.Duration(phase*float64(cfg.killJitter))):
		}
		victim, err := leaderProcess(ctx, rdb, br.Keys().Leader, procs)
		if err != nil {
			return row, err
		}
		killAt = time.Now()
		if err := victim.cmd.Process.Signal(a.signal); err != nil {
			return row, fmt.Errorf("signal leader: %w", err)
		}
		cfg.log.Info("killed the leader", "owner", victim.owner, "signal", a.signal.String())

		if a.resume {
			// A stopped process must be continued no matter how this trial
			// ends. Left stopped it cannot be reaped, and the harness would
			// hang in Wait behind a process that is not running to exit.
			defer func() { _ = victim.cmd.Process.Signal(syscall.SIGCONT) }()
			if err := pauseAndResume(ctx, cfg, rdb, br.Keys().Leader, &row, victim); err != nil {
				return row, err
			}
		}
	}

	if err := <-submitDone; err != nil {
		return row, fmt.Errorf("submit: %w", err)
	}

	// Wait for the queue to drain, or give up when it plainly cannot.
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

	// The invariant, read back from the dispatch log rather than asserted in a
	// test: no superseded term may write after its successor has. This is
	// checked in every arm, not only the one that pauses a leader, because an
	// invariant that is only checked where it is expected to be interesting is
	// not being checked at all.
	row.EpochInversions = epochInversions(entries)
	if row.EpochInversions != 0 {
		return row, fmt.Errorf("%d execution-stream entries were dispatched under a superseded epoch; the single-writer invariant was violated", row.EpochInversions)
	}
	if a.signal == 0 {
		row.OutageMS = 0
		row.Recovered = true
	} else {
		outage, ok := outageAround(stamps, killAt)
		row.OutageMS, row.Recovered = outage, ok
		if !ok {
			row.OutageMS = -1
		}
	}
	if !drained && a.name != "single" {
		cfg.log.Warn("trial did not drain", "arm", a.name, "rep", rep, "completed", row.Completed)
	}

	// A crash outage is bounded above by the lease TTL by construction, so a
	// value far beyond it did not come from the mechanism being measured. Flag
	// it rather than letting it into a median.
	if limit := 5 * float64(cfg.leaseTTL.Milliseconds()); row.Recovered && row.OutageMS > limit {
		row.Suspect = true
		cfg.log.Error("trial outage far exceeds the lease ttl; something outside the experiment interfered",
			"arm", a.name, "rep", rep, "outage_ms", row.OutageMS, "lease_ttl_ms", cfg.leaseTTL.Milliseconds(),
			"hint", "on a laptop this is usually idle sleep: run under `caffeinate -dims`")
	}
	return row, nil
}

// proc is one scheduler process under test.
type proc struct {
	owner string
	index int // which metrics port this replica listens on
	cmd   *exec.Cmd
}

func startSchedulers(ctx context.Context, cfg *runConfig, namespace string, n int) ([]*proc, error) {
	out := make([]*proc, 0, n)
	for i := 0; i < n; i++ {
		owner := fmt.Sprintf("sched-%d", i)
		cmd := exec.CommandContext(ctx, cfg.binary,
			"--redis-addr", cfg.redisAddr,
			"--namespace", namespace,
			"--policy", cfg.policy,
			"--consumer", owner,
			// A distinct port per replica, so the trial can scrape every one
			// of them and check that exactly one claims leadership.
			"--metrics-addr", metricsAddr(i),
			"--leader-election", "true",
			"--lease-ttl", cfg.leaseTTL.String(),
			"--lease-renew", cfg.leaseRenew.String(),
			"--log-level", "warn",
		)
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
		// Its own process group, so SIGKILL reaches this replica alone.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			stopAll(out)
			return nil, fmt.Errorf("start %s: %w", owner, err)
		}
		out = append(out, &proc{owner: owner, index: i, cmd: cmd})
	}
	return out, nil
}

// metricsAddr gives replica i its own listener. The replicas share a host here,
// which they would not in a real deployment.
func metricsAddr(i int) string { return fmt.Sprintf("127.0.0.1:%d", 19101+i) }

// leaderGauges scrapes tq_scheduler_is_leader from every replica. The sum is
// the expression an operator would actually alert on: 0 means the queue has
// nobody dispatching, and 2 means a split brain.
func leaderGauges(ctx context.Context, n int) (sum float64, err error) {
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < n; i++ {
		v, err := scrapeGauge(ctx, client, "http://"+metricsAddr(i)+"/metrics", "tq_scheduler_is_leader")
		if err != nil {
			// A replica that has just been killed stops answering, which is
			// not a failure of the check.
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

// waitForLeader blocks until somebody holds the lease, so a trial never starts
// measuring an outage that is really just a slow startup.
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

// leaderProcess resolves the lease holder to the process that holds it. The
// holder value is "<owner>|<epoch>" and owner is the --consumer name we chose.
func leaderProcess(ctx context.Context, rdb *redis.Client, key string, procs []*proc) (*proc, error) {
	owner, err := leaseOwner(ctx, rdb, key)
	if err != nil {
		return nil, err
	}
	for _, p := range procs {
		if p.owner == owner {
			return p, nil
		}
	}
	return nil, fmt.Errorf("lease holder %q is not one of the processes under test", owner)
}

// pauseAndResume runs the gray failure: the leader is already stopped when this
// is called, and it stays stopped for longer than its lease before being
// continued again.
//
// The three checks in here are the point of the arm, and each one would be
// worth nothing on its own:
//
//   - the victim really is stopped, read from the operating system rather than
//     assumed from the fact that a signal was sent;
//   - a different process holds the lease before the victim is resumed, so the
//     resumed process really is superseded and not merely slow;
//   - after resuming, exactly one process still claims leadership.
//
// Without the second check the arm could pass while testing nothing, which is
// the failure mode of every fault-injection test that does not verify the fault
// actually happened.
func pauseAndResume(ctx context.Context, cfg *runConfig, rdb *redis.Client, leaseKey string, row *trial, victim *proc) error {
	pausedAt := time.Now()

	// Give the stop a moment to take effect, then confirm it did.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}
	if state, err := processState(victim.cmd.Process.Pid); err == nil && !strings.HasPrefix(state, "T") {
		return fmt.Errorf("victim %s is in state %q after SIGSTOP, want a stopped state", victim.owner, state)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(cfg.pauseFor):
	}

	// The victim must have been superseded while it was stopped. If it still
	// holds the lease the pause was too short, or nothing else was running,
	// and resuming it would prove nothing.
	successor, err := leaseOwner(ctx, rdb, leaseKey)
	if err != nil {
		return err
	}
	if successor == victim.owner {
		return fmt.Errorf("%s still holds the lease after being stopped for %s; the pause did not outlast the lease", victim.owner, cfg.pauseFor)
	}
	if successor == "" {
		return fmt.Errorf("nobody holds the lease after %s was stopped for %s", victim.owner, cfg.pauseFor)
	}

	if err := victim.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		return fmt.Errorf("resume %s: %w", victim.owner, err)
	}
	row.PauseMS = time.Since(pausedAt).Seconds() * 1000
	cfg.log.Info("resumed the superseded leader",
		"owner", victim.owner, "paused_ms", row.PauseMS, "successor", successor)

	// Let it run. Its dispatch loop ticks several times in this window, and
	// every one of those ticks is an attempt to write with an epoch that has
	// been superseded.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(cfg.resumeSettle):
	}

	client := &http.Client{Timeout: 2 * time.Second}
	url := "http://" + metricsAddr(victim.index) + "/metrics"
	if v, err := scrapeGauge(ctx, client, url, "tq_scheduler_fenced_operations_total"); err == nil {
		row.FencedOperations = v
	}
	if sum, err := leaderGauges(ctx, row.Replicas); err == nil {
		row.LeadersAfterResume = sum
		if sum != 1 {
			return fmt.Errorf("%v replicas claim leadership after the superseded leader resumed, want exactly 1", sum)
		}
	}
	return nil
}

// processState reads the operating system's view of a process, so that "it was
// stopped" is an observation rather than an inference from a signal call that
// returned nil. The first letter of the state is what matters: T is stopped.
func processState(pid int) (string, error) {
	out, err := exec.Command("ps", "-o", "state=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// leaseOwner reads the current lease holder's name, or "" when the lease is
// free. The stored value is "<owner>|<epoch>".
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

// waitForDrain waits until every task reached a terminal state, and reports
// whether it got there. The single arm is expected to return false.
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
			// Fifteen seconds of no progress at all means nothing is
			// dispatching, which is the single arm's expected end state.
			if idle > 150 {
				return false
			}
		} else {
			idle, last = 0, done
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// dispatchEntry is one execution-stream entry as the analysis sees it: when the
// scheduler dispatched it, and which leadership term did so.
type dispatchEntry struct {
	At    time.Time
	Epoch int64
}

// dispatchRecord reads the execution stream in stream order, which is dispatch
// order, since stream IDs are assigned by Redis at XADD and only increase.
//
// Both fields come from the dispatch script rather than from bookkeeping in
// this process: the timestamp is what makes the outage a measurement, and the
// epoch is what makes the single-writer invariant checkable after the fact.
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
		last := msgs[len(msgs)-1].ID
		if len(msgs) < 5000 {
			break
		}
		start = "(" + last
	}
	return out, nil
}

// dispatchStamps pulls the timestamps out in ascending order. They are sorted
// because a gap analysis needs them ordered by time, whereas the epoch check
// below needs them in the order they were written.
func dispatchStamps(entries []dispatchEntry) []time.Time {
	out := make([]time.Time, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.At)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// epochInversions counts entries dispatched under a term older than one that
// had already written. This is the single-writer invariant stated as something
// a reader can check: the fence guarantees that once epoch N has written,
// nothing below N ever writes again, so replaying the stream in order must
// produce epochs that never decrease.
//
// Entries with no epoch are skipped rather than counted as zero. Those are
// worker-initiated retries, which are not dispatched by a leader and so are not
// governed by the fence; counting them would manufacture an inversion at every
// retry and make the check meaningless.
func epochInversions(entries []dispatchEntry) int {
	var high int64
	inversions := 0
	for _, e := range entries {
		if e.Epoch <= 0 {
			continue
		}
		if e.Epoch < high {
			inversions++
			continue
		}
		high = e.Epoch
	}
	return inversions
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

// outageAround measures the dispatch gap that brackets the kill: the last
// dispatch at or before it, and the first one after. It reports false when no
// dispatch ever followed, which is the single arm and is censored rather than
// counted as zero.
func outageAround(stamps []time.Time, kill time.Time) (float64, bool) {
	var before time.Time
	var after time.Time
	for _, s := range stamps {
		if !s.After(kill) {
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
		// Nothing had been dispatched yet; measure from the kill itself.
		before = kill
	}
	return after.Sub(before).Seconds() * 1000, true
}

// steadySpecs builds a constant-rate arrival plan. Failover is about time, not
// about the shape of the workload, so the arrivals are deliberately boring.
func steadySpecs(n int, rate float64, execMS int64) []workload.Spec {
	if rate <= 0 {
		rate = 1
	}
	gap := time.Duration(float64(time.Second) / rate)
	specs := make([]workload.Spec, 0, n)
	tenants := []string{"A", "B", "C"}
	for i := 0; i < n; i++ {
		specs = append(specs, workload.Spec{
			ID:                  fmt.Sprintf("f-%06d", i),
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

func writeCSV(path string, rows []trial) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{
		"arm", "rep", "policy", "replicas", "lease_ttl_ms", "lease_renew_ms", "tasks", "submitted",
		"dispatched", "completed", "duplicate_completions", "dead_lettered",
		"outage_ms", "max_gap_ms", "recovered", "leader_epochs", "leaders_before_kill",
		"wall_seconds", "suspect",
		"pause_ms", "leaders_after_resume", "fenced_operations", "epoch_inversions",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.Arm,
			strconv.Itoa(r.Rep),
			r.Policy,
			strconv.Itoa(r.Replicas),
			strconv.FormatInt(r.LeaseTTL.Milliseconds(), 10),
			strconv.FormatInt(r.LeaseRenew.Milliseconds(), 10),
			strconv.Itoa(r.Tasks),
			strconv.Itoa(r.Submitted),
			strconv.FormatInt(r.Dispatched, 10),
			strconv.FormatInt(r.Completed, 10),
			strconv.FormatInt(r.Duplicates, 10),
			strconv.FormatInt(r.DeadLetter, 10),
			strconv.FormatFloat(r.OutageMS, 'f', 1, 64),
			strconv.FormatFloat(r.MaxGapMS, 'f', 1, 64),
			strconv.FormatBool(r.Recovered),
			strconv.Itoa(r.Transitions),
			strconv.FormatFloat(r.LeadersBeforeKill, 'f', 0, 64),
			strconv.FormatFloat(r.WallSeconds, 'f', 1, 64),
			strconv.FormatBool(r.Suspect),
			strconv.FormatFloat(r.PauseMS, 'f', 1, 64),
			strconv.FormatFloat(r.LeadersAfterResume, 'f', 0, 64),
			strconv.FormatFloat(r.FencedOperations, 'f', 0, 64),
			strconv.Itoa(r.EpochInversions),
		}); err != nil {
			return err
		}
	}
	return nil
}

// selectArms resolves the --arms flag, preserving the declared order so that
// output is comparable between runs.
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

func summarise(w io.Writer, rows []trial) {
	fmt.Fprintf(w, "%-10s %5s %10s %10s %10s %10s %12s\n",
		"arm", "n", "median", "p95", "max", "maxgap", "recovered")
	for _, a := range arms {
		var outages []float64
		var gaps []float64
		recovered, total, suspect := 0, 0, 0
		fencedTrials, inversions := 0, 0
		var lostWork int64
		for _, r := range rows {
			if r.Arm != a.name {
				continue
			}
			total++
			inversions += r.EpochInversions
			if r.FencedOperations > 0 {
				fencedTrials++
			}
			if r.Suspect {
				suspect++
				continue
			}
			gaps = append(gaps, r.MaxGapMS)
			if r.Recovered {
				recovered++
				outages = append(outages, r.OutageMS)
			}
			lostWork += int64(r.Tasks) - r.Completed - r.DeadLetter
		}
		if total == 0 {
			continue
		}
		fmt.Fprintf(w, "%-10s %5d %10s %10s %10s %10.0f %8d/%-3d\n",
			a.name, total,
			fmtMS(quantile(outages, 0.5)), fmtMS(quantile(outages, 0.95)), fmtMS(quantile(outages, 1)),
			quantile(gaps, 0.5), recovered, total)
		if lostWork != 0 {
			fmt.Fprintf(w, "%-10s %s\n", "", fmt.Sprintf("tasks not in a terminal state: %d", lostWork))
		}
		if suspect != 0 {
			fmt.Fprintf(w, "%-10s %s\n", "", fmt.Sprintf("EXCLUDED %d suspect trial(s): outage far beyond the lease ttl", suspect))
		}
		if a.resume {
			// Both outcomes are correct, and the split between them is the
			// result: the remainder are trials in which the resumed leader
			// stood down on its own before it managed to attempt a write.
			fmt.Fprintf(w, "%-10s %s\n", "",
				fmt.Sprintf("the resumed leader was refused by the fence in %d/%d trials", fencedTrials, total))
		}
		fmt.Fprintf(w, "%-10s %s\n", "",
			fmt.Sprintf("dispatches under a superseded epoch: %d", inversions))
	}
}

func fmtMS(v float64) string {
	if v < 0 {
		return "-"
	}
	return fmt.Sprintf("%.0f ms", v)
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
	idx := int(q * float64(len(s)-1))
	return s[idx]
}
