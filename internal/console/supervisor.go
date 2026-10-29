package console

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/netfault"
)

// The supervisor runs a small, disposable cluster for the live console's demo
// mode: scheduler replicas and workers started from prebuilt binaries, each
// scheduler reaching Redis through its own fault-injection proxy.
//
// Every fault it can inject acts on a process it started itself, named in a
// fixed table, through a fixed operation. Nothing is built from request input:
// no shell, no paths, no arguments. That is the whole of its safety model, and
// the HTTP layer adds authentication and origin checks on top.

// Fault actions. The table below says which apply to which role.
const (
	ActionKill      = "kill"      // SIGKILL: a crash; nothing is released
	ActionTerminate = "terminate" // SIGTERM: a rollout; the lease is released
	ActionFreeze    = "freeze"    // SIGSTOP, then SIGCONT after the duration
	ActionPartition = "partition" // sever the process's path to Redis, then heal
	ActionRestart   = "restart"   // start a process that has exited
)

var allowed = map[string]map[string]bool{
	"scheduler": {ActionKill: true, ActionTerminate: true, ActionFreeze: true, ActionPartition: true, ActionRestart: true},
	"worker":    {ActionKill: true, ActionFreeze: true, ActionRestart: true},
}

// Bounds on timed faults. A freeze or partition always ends by itself, so a
// demo abandoned mid-fault does not leave a process stopped indefinitely.
const (
	MinFaultDuration = time.Second
	MaxFaultDuration = 30 * time.Second
)

// Process states as the console reports them.
const (
	StateRunning = "running"
	StateStopped = "stopped" // SIGSTOP, verified from the OS process table
	StateExited  = "exited"
)

// FaultRequest is the only input the supervisor accepts.
type FaultRequest struct {
	Action     string `json:"action"`
	Target     string `json:"target"` // a process name, or "leader"
	DurationMS int    `json:"duration_ms,omitempty"`
}

// ActiveFault is a timed fault in progress.
type ActiveFault struct {
	Action  string    `json:"action"`
	Started time.Time `json:"started_at"`
	Until   time.Time `json:"until"`
}

// ProcessState is what the console reports about a supervised process.
type ProcessState struct {
	PID      int          `json:"pid,omitempty"`
	State    string       `json:"state"`
	ExitCode *int         `json:"exit_code,omitempty"`
	Link     string       `json:"link,omitempty"` // "connected" or "partitioned"; schedulers only
	Fault    *ActiveFault `json:"fault,omitempty"`
	Restarts int          `json:"restarts"`
}

// ErrRejected marks a request that is well formed but not allowed now; the
// HTTP layer maps it to 409 rather than 400.
var ErrRejected = errors.New("rejected")

// SupervisorOptions configures the demo cluster.
type SupervisorOptions struct {
	BinDir      string
	RedisAddr   string
	Namespace   string
	Policy      string
	Schedulers  int
	Workers     int
	Concurrency int
	LeaseTTL    time.Duration
	LeaseRenew  time.Duration
	// MetricsPort is the first metrics port; schedulers take the next ones,
	// then workers.
	MetricsPort int
	Log         *slog.Logger
	// OnEvent receives process lifecycle events and leadership events parsed
	// from the schedulers' own structured logs.
	OnEvent func(Event)
}

type proc struct {
	name   string
	role   string
	bin    string
	args   []string
	addr   string
	link   *netfault.Proxy
	mu     sync.Mutex
	cmd    *exec.Cmd
	state  string
	exit   *int
	fault  *ActiveFault
	timer  *time.Timer
	undo   func()
	starts int
}

// Supervisor owns the demo cluster's processes.
type Supervisor struct {
	opt   SupervisorOptions
	procs []*proc
	byKey map[string]*proc
	ctx   context.Context
	stop  context.CancelFunc
	wg    sync.WaitGroup
	now   func() time.Time
}

// NewSupervisor validates the options and resolves the binaries. It starts
// nothing.
func NewSupervisor(opt SupervisorOptions) (*Supervisor, error) {
	if opt.Schedulers < 1 || opt.Schedulers > 3 {
		return nil, fmt.Errorf("schedulers must be 1 to 3, got %d", opt.Schedulers)
	}
	if opt.Workers < 1 || opt.Workers > 4 {
		return nil, fmt.Errorf("workers must be 1 to 4, got %d", opt.Workers)
	}
	if opt.Concurrency < 1 || opt.Concurrency > 16 {
		return nil, fmt.Errorf("concurrency must be 1 to 16, got %d", opt.Concurrency)
	}
	if opt.LeaseRenew <= 0 || opt.LeaseRenew >= opt.LeaseTTL {
		return nil, fmt.Errorf("lease renew %s must be positive and shorter than the ttl %s", opt.LeaseRenew, opt.LeaseTTL)
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opt.OnEvent == nil {
		opt.OnEvent = func(Event) {}
	}
	bins := map[string]string{}
	for _, b := range []string{"scheduler", "worker"} {
		p, err := filepath.Abs(filepath.Join(opt.BinDir, b))
		if err != nil {
			return nil, err
		}
		if st, err := os.Stat(p); err != nil || st.IsDir() || st.Mode()&0o111 == 0 {
			return nil, fmt.Errorf("%s is not an executable binary; run `make build` first", p)
		}
		bins[b] = p
	}

	s := &Supervisor{opt: opt, byKey: map[string]*proc{}, now: time.Now}
	port := opt.MetricsPort
	for i := 0; i < opt.Schedulers; i++ {
		name := fmt.Sprintf("sched-%d", i)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		port++
		p := &proc{name: name, role: "scheduler", bin: bins["scheduler"], addr: addr, state: StateExited}
		p.args = []string{
			"--namespace", opt.Namespace,
			"--policy", opt.Policy,
			"--consumer", name,
			"--metrics-addr", addr,
			"--leader-election", "true",
			"--lease-ttl", opt.LeaseTTL.String(),
			"--lease-renew", opt.LeaseRenew.String(),
			"--log-level", "info",
			"--log-format", "json",
		}
		s.add(p)
	}
	for i := 0; i < opt.Workers; i++ {
		name := fmt.Sprintf("worker-%d", i)
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		port++
		p := &proc{name: name, role: "worker", bin: bins["worker"], addr: addr, state: StateExited}
		p.args = []string{
			"--redis-addr", opt.RedisAddr,
			"--namespace", opt.Namespace,
			"--name", name,
			"--concurrency", strconv.Itoa(opt.Concurrency),
			"--metrics-addr", addr,
			"--log-level", "warn",
			"--log-format", "json",
		}
		s.add(p)
	}
	return s, nil
}

func (s *Supervisor) add(p *proc) {
	s.procs = append(s.procs, p)
	s.byKey[p.name] = p
}

// Slots is the total worker execution slots in the cluster.
func (s *Supervisor) Slots() int { return s.opt.Workers * s.opt.Concurrency }

// Targets lists every supervised process as a scrape target.
func (s *Supervisor) Targets() []Target {
	out := make([]Target, 0, len(s.procs))
	for _, p := range s.procs {
		out = append(out, Target{Name: p.name, Role: p.role, URL: "http://" + p.addr})
	}
	return out
}

// Start brings up the proxies and every process.
func (s *Supervisor) Start(ctx context.Context) error {
	s.ctx, s.stop = context.WithCancel(ctx)
	for _, p := range s.procs {
		if p.role == "scheduler" {
			link, err := netfault.New("127.0.0.1:0", s.opt.RedisAddr)
			if err != nil {
				s.Stop()
				return fmt.Errorf("proxy for %s: %w", p.name, err)
			}
			p.link = link
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				link.Serve(s.ctx)
			}()
		}
		if err := s.launch(p); err != nil {
			s.Stop()
			return err
		}
	}
	return nil
}

func (s *Supervisor) launch(p *proc) error {
	args := p.args
	if p.link != nil {
		// Through the proxy, which is what makes a per-replica partition
		// possible while Redis stays up for everyone else.
		args = append([]string{"--redis-addr", p.link.Addr()}, args...)
	}
	cmd := exec.Command(p.bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = io.Discard
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", p.name, err)
	}
	p.mu.Lock()
	p.cmd, p.state, p.exit = cmd, StateRunning, nil
	p.starts++
	p.mu.Unlock()
	s.opt.OnEvent(Event{At: s.now(), Source: SourceControl, Kind: "process_started", Subject: p.name,
		Message: fmt.Sprintf("%s started (pid %d)", p.name, cmd.Process.Pid)})

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.readLogs(p.name, stderr)
	}()
	go func() {
		defer s.wg.Done()
		err := cmd.Wait()
		code := cmd.ProcessState.ExitCode()
		p.mu.Lock()
		if p.cmd == cmd {
			p.state, p.exit = StateExited, &code
			p.clearFaultLocked()
		}
		p.mu.Unlock()
		msg := fmt.Sprintf("%s exited (code %d)", p.name, code)
		if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			msg = fmt.Sprintf("%s exited on %s", p.name, ws.Signal())
		} else if err != nil && code == -1 {
			msg = fmt.Sprintf("%s exited: %v", p.name, err)
		}
		s.opt.OnEvent(Event{At: s.now(), Source: SourceObserved, Kind: "process_exited", Subject: p.name, Message: msg})
	}()
	return nil
}

// leadershipLogs are the scheduler log lines that mark a leadership change.
// They are recorded by the process itself, with its own timestamp, which is
// why the console reports them separately from what it infers by polling.
var leadershipLogs = map[string]string{
	"became scheduler leader":                                    "lease_acquired",
	"lost the scheduler lease":                                   "lease_lost",
	"fenced out by a newer scheduler epoch, standing down":       "fenced",
	"lease renewal failed, will retry":                           "renewal_failed",
	"reclaimed ingress entries stranded by a previous scheduler": "ingress_reclaimed",
	"lease release failed, standby will wait out the ttl":        "release_failed",
	"campaigning for the scheduler lease":                        "campaigning",
}

func (s *Supervisor) readLogs(name string, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if ev, ok := parseLogLine(name, sc.Bytes()); ok {
			s.opt.OnEvent(ev)
		}
	}
}

func parseLogLine(name string, line []byte) (Event, bool) {
	var rec map[string]any
	if json.Unmarshal(line, &rec) != nil {
		return Event{}, false
	}
	msg, _ := rec["msg"].(string)
	kind, ok := leadershipLogs[msg]
	if !ok {
		return Event{}, false
	}
	at := time.Now()
	if ts, ok := rec["time"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			at = t
		}
	}
	detail := msg
	if e, ok := rec["epoch"]; ok {
		detail = fmt.Sprintf("%s (epoch %v)", msg, e)
	}
	return Event{At: at, Source: SourceLog, Kind: kind, Subject: name, Message: detail}, true
}

// Resolve maps a request target to a process. "leader" means whichever
// process the lease currently names; owner is that name, read from Redis by
// the caller.
func (s *Supervisor) resolve(target, owner string) (*proc, error) {
	if target == "leader" {
		if owner == "" {
			return nil, fmt.Errorf("%w: no scheduler holds the lease right now", ErrRejected)
		}
		target = owner
	}
	p, ok := s.byKey[target]
	if !ok {
		return nil, fmt.Errorf("unknown target %q", target)
	}
	return p, nil
}

// Validate checks a request against the table of allowed operations without
// acting on it.
func Validate(req FaultRequest) (time.Duration, error) {
	known := false
	for _, ops := range allowed {
		if ops[req.Action] {
			known = true
		}
	}
	if !known {
		return 0, fmt.Errorf("unknown action %q", req.Action)
	}
	if req.Target == "" {
		return 0, errors.New("target is required")
	}
	d := time.Duration(req.DurationMS) * time.Millisecond
	timed := req.Action == ActionFreeze || req.Action == ActionPartition
	if timed && (d < MinFaultDuration || d > MaxFaultDuration) {
		return 0, fmt.Errorf("%s needs duration_ms between %d and %d", req.Action, MinFaultDuration.Milliseconds(), MaxFaultDuration.Milliseconds())
	}
	if !timed && req.DurationMS != 0 {
		return 0, fmt.Errorf("%s takes no duration", req.Action)
	}
	return d, nil
}

// Apply performs one fault. owner is the current lease holder, for "leader".
func (s *Supervisor) Apply(req FaultRequest, owner string) (string, error) {
	d, err := Validate(req)
	if err != nil {
		return "", err
	}
	p, err := s.resolve(req.Target, owner)
	if err != nil {
		return "", err
	}
	if !allowed[p.role][req.Action] {
		return "", fmt.Errorf("%s is not available for a %s", req.Action, p.role)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if req.Action == ActionRestart {
		if p.state != StateExited {
			return "", fmt.Errorf("%w: %s is %s, not exited", ErrRejected, p.name, p.state)
		}
		p.mu.Unlock()
		err := s.launch(p)
		p.mu.Lock()
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("restarted %s", p.name), nil
	}
	if p.state != StateRunning || p.cmd == nil {
		return "", fmt.Errorf("%w: %s is %s", ErrRejected, p.name, p.state)
	}
	if p.fault != nil {
		return "", fmt.Errorf("%w: %s already has a %s in progress", ErrRejected, p.name, p.fault.Action)
	}
	pid := p.cmd.Process.Pid
	switch req.Action {
	case ActionKill:
		if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
			return "", err
		}
		return fmt.Sprintf("sent SIGKILL to %s (pid %d)", p.name, pid), nil
	case ActionTerminate:
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return "", err
		}
		return fmt.Sprintf("sent SIGTERM to %s (pid %d)", p.name, pid), nil
	case ActionFreeze:
		if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
			return "", err
		}
		// A signal call returning nil is not evidence the process stopped;
		// the OS process table is.
		verified := "stop not verified: ps unavailable"
		if st, err := processState(pid); err == nil {
			if strings.HasPrefix(st, "T") {
				p.state = StateStopped
				verified = "verified stopped (ps state " + st + ")"
			} else {
				verified = "ps reports state " + st + ", not stopped"
			}
		}
		s.armLocked(p, ActionFreeze, d, func() {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			if p.state == StateStopped {
				p.state = StateRunning
			}
		})
		return fmt.Sprintf("sent SIGSTOP to %s (pid %d), %s; SIGCONT in %s", p.name, pid, verified, d), nil
	case ActionPartition:
		if p.link == nil {
			return "", fmt.Errorf("%s has no proxy to cut", p.name)
		}
		p.link.Cut()
		s.armLocked(p, ActionPartition, d, func() { p.link.Heal() })
		return fmt.Sprintf("cut %s off from Redis; heals in %s", p.name, d), nil
	}
	return "", fmt.Errorf("unhandled action %q", req.Action)
}

// armLocked records a timed fault and schedules its end. Called with p.mu held.
func (s *Supervisor) armLocked(p *proc, action string, d time.Duration, undo func()) {
	now := s.now()
	f := &ActiveFault{Action: action, Started: now, Until: now.Add(d)}
	p.fault = f
	p.timer = time.AfterFunc(d, func() {
		p.mu.Lock()
		if p.fault != f {
			p.mu.Unlock()
			return
		}
		undo()
		p.fault, p.timer = nil, nil
		p.mu.Unlock()
		s.opt.OnEvent(Event{At: s.now(), Source: SourceControl, Kind: action + "_ended", Subject: p.name,
			Message: fmt.Sprintf("%s on %s ended after %s", action, p.name, d)})
	})
	p.undo = undo
}

// clearFaultLocked ends a timed fault now. Called with p.mu held.
func (p *proc) clearFaultLocked() string {
	if p.fault == nil {
		return ""
	}
	action := p.fault.Action
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.undo != nil {
		p.undo()
	}
	p.fault, p.timer, p.undo = nil, nil, nil
	return action
}

// ClearAll ends every timed fault immediately: resumes frozen processes and
// heals partitions. It does not restart exited processes.
func (s *Supervisor) ClearAll() []string {
	var out []string
	for _, p := range s.procs {
		p.mu.Lock()
		if a := p.clearFaultLocked(); a != "" {
			out = append(out, fmt.Sprintf("ended %s on %s", a, p.name))
		}
		p.mu.Unlock()
	}
	return out
}

// States reports every process.
func (s *Supervisor) States() map[string]ProcessState {
	out := make(map[string]ProcessState, len(s.procs))
	for _, p := range s.procs {
		p.mu.Lock()
		st := ProcessState{State: p.state, ExitCode: p.exit, Restarts: max(p.starts-1, 0)}
		if p.cmd != nil && p.cmd.Process != nil && p.state != StateExited {
			st.PID = p.cmd.Process.Pid
		}
		if p.link != nil {
			st.Link = "connected"
			if p.link.IsCut() {
				st.Link = "partitioned"
			}
		}
		if p.fault != nil {
			f := *p.fault
			st.Fault = &f
		}
		p.mu.Unlock()
		out[p.name] = st
	}
	return out
}

// Stop ends faults, terminates every process (SIGKILL after a grace period)
// and closes the proxies.
func (s *Supervisor) Stop() {
	s.ClearAll()
	for _, p := range s.procs {
		p.mu.Lock()
		if p.cmd != nil && p.state != StateExited {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
		}
		p.mu.Unlock()
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, p := range s.procs {
		for {
			p.mu.Lock()
			done := p.state == StateExited
			cmd := p.cmd
			p.mu.Unlock()
			if done || cmd == nil {
				break
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Signal(syscall.SIGKILL)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if s.stop != nil {
		s.stop()
	}
	for _, p := range s.procs {
		if p.link != nil {
			_ = p.link.Close()
		}
	}
	s.wg.Wait()
}

// processState reads one process's state letter from the OS process table.
// pid is an integer the supervisor owns; nothing here comes from a request.
func processState(pid int) (string, error) {
	out, err := exec.Command("ps", "-o", "state=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}
