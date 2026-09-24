package main

// A throwaway Redis Sentinel cluster, run as real local processes.
//
// Nothing here is containerised. The experiment needs to stop a Redis process
// mid-write and crash it without letting it shut down cleanly, and doing that
// to real processes on known ports is both simpler and more obviously faithful
// than arranging it through a container runtime.
//
// One detail matters more than it looks. Redis 7 and later wait for replicas to
// catch up during a graceful SHUTDOWN, so a politely stopped master loses
// nothing and the interesting case never happens. Every kill here is SIGKILL
// for that reason; see the note on kill().

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/netfault"
	"github.com/redis/go-redis/v9"
)

// node is one redis-server or redis-sentinel process.
type node struct {
	port int
	cmd  *exec.Cmd
	dir  string
	role string // "master", "replica" or "sentinel"
}

func (n *node) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }

// kill stops a node the way a crash does.
//
// Not SHUTDOWN, and not SIGTERM. Redis treats both as a request to leave
// cleanly and will wait for replicas to catch up before exiting, which is
// exactly the behaviour that makes acknowledged writes survive. Using either
// one here would produce a run in which nothing is ever lost and the experiment
// would report a safety property it had not tested.
func (n *node) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = n.cmd.Process.Wait()
	}
}

// cluster is one master, some replicas and a quorum of sentinels.
//
// Each replica reaches the master through its own netfault proxy rather than
// connecting directly. That indirection is the whole reason the lossy case can
// be produced at all: severing the proxy stops replication in a way that
// stopping the replica process cannot, because a stopped process still has a
// kernel acknowledging and buffering the stream on its behalf.
type cluster struct {
	master    *node
	replicas  []*node
	sentinels []*node
	masterSet string
	baseDir   string
	// replLinks[i] carries replication to replicas[i].
	replLinks []*netfault.Proxy
}

type clusterOptions struct {
	baseDir      string
	redisPort    int // first data port; replicas follow
	sentinelPort int // first sentinel port
	replicas     int
	masterName   string
	// minReplicas configures min-replicas-to-write on the master. This is the
	// server-side half of the mitigation: it makes the master refuse writes
	// when too few replicas are connected, rather than accepting writes it may
	// not be able to hand on.
	minReplicas int
	downAfter   time.Duration
	failoverIn  time.Duration
}

func startCluster(ctx context.Context, opts clusterOptions) (*cluster, error) {
	if err := os.MkdirAll(opts.baseDir, 0o755); err != nil {
		return nil, err
	}
	c := &cluster{masterSet: opts.masterName, baseDir: opts.baseDir}

	master, err := startRedis(ctx, opts.baseDir, opts.redisPort, "master", nil, opts.minReplicas)
	if err != nil {
		return nil, err
	}
	c.master = master

	for i := 1; i <= opts.replicas; i++ {
		link, err := netfault.New("127.0.0.1:0", master.addr())
		if err != nil {
			c.stopAll()
			return nil, err
		}
		go link.Serve(ctx)
		c.replLinks = append(c.replLinks, link)

		r, err := startRedisVia(ctx, opts.baseDir, opts.redisPort+i, link.Addr())
		if err != nil {
			c.stopAll()
			return nil, err
		}
		c.replicas = append(c.replicas, r)
	}

	// Sentinels last, so they observe a cluster that already exists.
	for i := 0; i < 3; i++ {
		s, err := startSentinel(ctx, opts, opts.sentinelPort+i, master.port)
		if err != nil {
			c.stopAll()
			return nil, err
		}
		c.sentinels = append(c.sentinels, s)
	}

	if err := c.waitReplicasOnline(ctx, opts.replicas, 20*time.Second); err != nil {
		c.stopAll()
		return nil, err
	}
	if err := c.waitSentinelsAgree(ctx, master.addr(), 20*time.Second); err != nil {
		c.stopAll()
		return nil, err
	}
	return c, nil
}

// startRedisVia starts a replica whose master address is a proxy rather than
// the master itself.
//
// The replica still announces its own listening port to the master, so Sentinel
// discovers it at its real address and can promote it normally; only the
// direction the replication stream travels is diverted.
func startRedisVia(ctx context.Context, baseDir string, port int, viaAddr string) (*node, error) {
	host, portStr, err := net.SplitHostPort(viaAddr)
	if err != nil {
		return nil, fmt.Errorf("proxy address %q: %w", viaAddr, err)
	}
	viaPort, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("proxy port %q: %w", portStr, err)
	}
	return startRedisAt(ctx, baseDir, port, "replica", host, viaPort, 0)
}

func startRedis(ctx context.Context, baseDir string, port int, role string, master *node, minReplicas int) (*node, error) {
	if master == nil {
		return startRedisAt(ctx, baseDir, port, role, "", 0, minReplicas)
	}
	return startRedisAt(ctx, baseDir, port, role, "127.0.0.1", master.port, minReplicas)
}

func startRedisAt(ctx context.Context, baseDir string, port int, role, masterHost string, masterPort, minReplicas int) (*node, error) {
	dir := filepath.Join(baseDir, fmt.Sprintf("redis-%d", port))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	args := []string{
		"--port", strconv.Itoa(port),
		"--dir", dir,
		"--logfile", "redis.log",
		// No persistence. This experiment is about replication losing writes,
		// and leaving an RDB or AOF on disk would let a restarted node recover
		// state that replication never carried, which is a different question.
		"--save", "",
		"--appendonly", "no",
	}
	if masterHost != "" {
		args = append(args, "--replicaof", masterHost, strconv.Itoa(masterPort))
	}
	if minReplicas > 0 {
		args = append(args,
			"--min-replicas-to-write", strconv.Itoa(minReplicas),
			"--min-replicas-max-lag", "10")
	}
	cmd := exec.CommandContext(ctx, "redis-server", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start redis on %d: %w", port, err)
	}
	n := &node{port: port, cmd: cmd, dir: dir, role: role}
	if err := waitPing(ctx, n.addr(), 15*time.Second); err != nil {
		n.kill()
		return nil, err
	}
	return n, nil
}

func startSentinel(ctx context.Context, opts clusterOptions, port, masterPort int) (*node, error) {
	dir := filepath.Join(opts.baseDir, fmt.Sprintf("sentinel-%d", port))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	conf := filepath.Join(dir, "sentinel.conf")
	body := fmt.Sprintf(`port %d
dir %s
sentinel monitor %s 127.0.0.1 %d 2
sentinel down-after-milliseconds %s %d
sentinel failover-timeout %s %d
sentinel parallel-syncs %s 1
`, port, dir, opts.masterName, masterPort,
		opts.masterName, opts.downAfter.Milliseconds(),
		opts.masterName, opts.failoverIn.Milliseconds(),
		opts.masterName)
	if err := os.WriteFile(conf, []byte(body), 0o644); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "redis-sentinel", conf, "--logfile", "sentinel.log")
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start sentinel on %d: %w", port, err)
	}
	n := &node{port: port, cmd: cmd, dir: dir, role: "sentinel"}
	if err := waitPing(ctx, n.addr(), 15*time.Second); err != nil {
		n.kill()
		return nil, err
	}
	return n, nil
}

func (c *cluster) sentinelAddrs() []string {
	out := make([]string, 0, len(c.sentinels))
	for _, s := range c.sentinels {
		out = append(out, s.addr())
	}
	return out
}

// currentMaster asks Sentinel who the master is, which is the only answer that
// matters: it is what a client using Sentinel discovery will be told.
func (c *cluster) currentMaster(ctx context.Context) (string, error) {
	var lastErr error
	for _, s := range c.sentinels {
		cl := redis.NewClient(&redis.Options{Addr: s.addr(), DialTimeout: 2 * time.Second})
		res, err := cl.Do(ctx, "SENTINEL", "get-master-addr-by-name", c.masterSet).StringSlice()
		cl.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if len(res) == 2 {
			return net.JoinHostPort(res[0], res[1]), nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no sentinel knows master set %q", c.masterSet)
	}
	return "", lastErr
}

// nodeByAddr resolves an address back to the process that owns it.
func (c *cluster) nodeByAddr(addr string) *node {
	if c.master != nil && c.master.addr() == addr {
		return c.master
	}
	for _, r := range c.replicas {
		if r.addr() == addr {
			return r
		}
	}
	return nil
}

func (c *cluster) waitReplicasOnline(ctx context.Context, want int, limit time.Duration) error {
	if want == 0 {
		return nil
	}
	cl := redis.NewClient(&redis.Options{Addr: c.master.addr(), DialTimeout: 2 * time.Second})
	defer cl.Close()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		info, err := cl.Info(ctx, "replication").Result()
		if err == nil && strings.Contains(info, fmt.Sprintf("connected_slaves:%d", want)) &&
			strings.Count(info, "state=online") == want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%d replicas did not come online within %s", want, limit)
}

func (c *cluster) waitSentinelsAgree(ctx context.Context, want string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		got, err := c.currentMaster(ctx)
		if err == nil && got == want {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("sentinels did not agree on master %s within %s", want, limit)
}

// waitForFailover blocks until Sentinel names a master other than the one given.
func (c *cluster) waitForFailover(ctx context.Context, was string, limit time.Duration) (string, time.Duration, error) {
	start := time.Now()
	deadline := start.Add(limit)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return "", 0, ctx.Err()
		}
		got, err := c.currentMaster(ctx)
		if err == nil && got != "" && got != was {
			return got, time.Since(start), nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", time.Since(start), fmt.Errorf("no failover away from %s within %s", was, limit)
}

// cutReplication severs the replication path to every replica. The master goes
// on accepting and acknowledging writes that now reach nobody else.
func (c *cluster) cutReplication() {
	for _, l := range c.replLinks {
		l.Cut()
	}
}

// waitReplicationDown blocks until the master reports that no replica is
// receiving the stream any more.
//
// Cutting a proxy is a request, not a result. Redis reports a replica as online
// until it notices the link is gone, and an experiment that starts writing
// before then is writing to a master that is still replicating normally — which
// is how an earlier version of this arm produced a trial with no data loss and
// no explanation for it. The fault has to be confirmed before it is used.
func (c *cluster) waitReplicationDown(ctx context.Context, limit time.Duration) error {
	cl := redis.NewClient(&redis.Options{
		Addr: c.master.addr(), DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
	})
	defer cl.Close()
	deadline := time.Now().Add(limit)
	var last string
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		info, err := cl.Info(ctx, "replication").Result()
		if err == nil {
			last = replicationSummary(info)
			// No replica in the online state is the condition that makes the
			// next write unreplicated.
			if strings.Count(info, "state=online") == 0 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("replication was still live %s after the link was cut (%s)", limit, last)
}

// replicationLive reports whether any replica is currently receiving the
// stream, which is how a healed fault is detected before it can contaminate a
// result.
func (c *cluster) replicationLive(ctx context.Context) (bool, error) {
	cl := redis.NewClient(&redis.Options{
		Addr: c.master.addr(), DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
	})
	defer cl.Close()
	info, err := cl.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Count(info, "state=online") > 0, nil
}

// replicationSummary pulls the interesting lines out of INFO replication for an
// error message.
func replicationSummary(info string) string {
	var keep []string
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "connected_slaves") || strings.HasPrefix(line, "slave") {
			keep = append(keep, line)
		}
	}
	return strings.Join(keep, "; ")
}

// healReplication restores the paths. The replicas reconnect on their own.
func (c *cluster) healReplication() {
	for _, l := range c.replLinks {
		l.Heal()
	}
}

func (c *cluster) stopAll() {
	for _, l := range c.replLinks {
		_ = l.Close()
	}
	for _, s := range c.sentinels {
		s.kill()
	}
	for _, r := range c.replicas {
		r.kill()
	}
	if c.master != nil {
		c.master.kill()
	}
}

func waitPing(ctx context.Context, addr string, limit time.Duration) error {
	cl := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: time.Second})
	defer cl.Close()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := cl.Ping(ctx).Err(); err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("%s did not answer PING within %s", addr, limit)
}
