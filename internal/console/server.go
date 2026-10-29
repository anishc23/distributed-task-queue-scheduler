package console

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
)

// The console's HTTP surface. Its security rests on four checks, each of
// which stops a different attack on a loopback service:
//
//  1. It listens on loopback only (enforced in cmd/console), so nothing off
//     the machine can connect.
//  2. Every request's Host must be the address it is bound to. A page on
//     another site cannot rebind its own DNS name to 127.0.0.1 and talk to it.
//  3. Every state-changing request needs the per-run token, which is printed
//     to the terminal that started the console and never served by it.
//  4. Every state-changing request must be same-origin JSON. A cross-site form
//     cannot set the header or the content type, and the console sends no CORS
//     headers, so a cross-origin script's preflight fails.
//
// Fault injection is also off unless the console was started with it on, and
// can only touch processes the console started itself (see Supervisor).

// ServerOptions configures the HTTP layer.
type ServerOptions struct {
	Console       *Console
	Broker        *broker.Broker
	Supervisor    *Supervisor
	Submitter     *Submitter
	Mode          string // "demo" or "monitor"
	FaultsEnabled bool
	Token         string
	// Hosts are the Host header values accepted, e.g. "127.0.0.1:8088".
	Hosts []string
	// Origins are the Origin header values accepted on writes. The console's
	// own origin is always derived from Hosts; extra ones are for a dev server.
	Origins []string
	Static  string // directory holding the built frontend; empty serves none
	Poll    time.Duration
	Log     *slog.Logger
}

type server struct {
	opt     ServerOptions
	hosts   map[string]bool
	origins map[string]bool // every Origin a write may carry
	extra   map[string]bool // the configured extras only (a dev server)
	started time.Time
}

// Handler returns the console's complete HTTP handler.
func Handler(opt ServerOptions) http.Handler {
	s := &server{opt: opt, hosts: map[string]bool{}, origins: map[string]bool{}, extra: map[string]bool{}, started: time.Now()}
	for _, h := range opt.Hosts {
		s.hosts[strings.ToLower(h)] = true
		s.origins["http://"+strings.ToLower(h)] = true
	}
	for _, o := range opt.Origins {
		o = strings.ToLower(strings.TrimRight(o, "/"))
		s.origins[o], s.extra[o] = true, true
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/status", s.status)
	mux.HandleFunc("GET /api/v1/history", s.history)
	mux.HandleFunc("GET /api/v1/events", s.events)
	mux.HandleFunc("GET /api/v1/dispatches", s.dispatches)
	mux.HandleFunc("GET /api/v1/dlq", s.dlq)
	mux.HandleFunc("GET /api/v1/controls", s.controls)
	mux.HandleFunc("POST /api/v1/faults", s.write(s.fault))
	mux.HandleFunc("POST /api/v1/faults/clear", s.write(s.clear))
	mux.HandleFunc("POST /api/v1/workload", s.write(s.workload))
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "no such endpoint")
	})
	if opt.Static != "" {
		mux.Handle("/", spa(opt.Static))
	}
	return s.guard(mux)
}

// guard applies the Host check to everything and fixed security headers.
func (s *server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hosts[strings.ToLower(r.Host)] {
			writeErr(w, http.StatusMisdirectedRequest, "unexpected Host header; the console answers only to the address it is bound to")
			return
		}
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-store")
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// write wraps a state-changing handler in the token and origin checks.
func (s *server) write(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.opt.Token == "" {
			writeErr(w, http.StatusForbidden, "this console accepts no control requests (monitor mode)")
			return
		}
		if o := r.Header.Get("Origin"); o != "" && !s.origins[strings.ToLower(o)] {
			writeErr(w, http.StatusForbidden, "cross-origin request refused")
			return
		}
		if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" && !s.devOrigin(r) {
			writeErr(w, http.StatusForbidden, "cross-site request refused")
			return
		}
		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			writeErr(w, http.StatusUnsupportedMediaType, "requests must be application/json")
			return
		}
		got := r.Header.Get("X-Console-Token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.opt.Token)) != 1 {
			writeErr(w, http.StatusUnauthorized, "missing or wrong console token; use the URL the console printed when it started")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		h(w, r)
	}
}

// devOrigin allows an explicitly configured extra origin (a local dev server
// proxying to the console) through the Sec-Fetch-Site check, which it would
// otherwise fail as "same-site". The console's own origin gets no such pass: a
// genuine same-origin request never arrives marked cross-site.
func (s *server) devOrigin(r *http.Request) bool {
	o := strings.ToLower(r.Header.Get("Origin"))
	return o != "" && s.extra[o]
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("malformed request: %w", err)
	}
	if dec.More() {
		return errors.New("malformed request: trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// ConsoleInfo describes the console itself, so the interface can say what it
// is connected to.
type ConsoleInfo struct {
	Mode          string    `json:"mode"`
	StartedAt     time.Time `json:"started_at"`
	ServerTime    time.Time `json:"server_time"`
	PollMS        int64     `json:"poll_ms"`
	FaultsEnabled bool      `json:"faults_enabled"`
}

func (s *server) info() ConsoleInfo {
	return ConsoleInfo{Mode: s.opt.Mode, StartedAt: s.started, ServerTime: time.Now(), PollMS: s.opt.Poll.Milliseconds(), FaultsEnabled: s.opt.FaultsEnabled}
}

func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	snap := s.opt.Console.Latest()
	writeJSON(w, http.StatusOK, map[string]any{"console": s.info(), "snapshot": snap})
}

func (s *server) history(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"console": s.info(), "samples": s.opt.Console.History()})
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	writeJSON(w, http.StatusOK, map[string]any{"events": s.opt.Console.Events().Since(since)})
}

// dispatchCap bounds one read of the dispatch log. At the demo's rates a
// minute is well under it; a truncated read is reported as such.
const dispatchCap = 60000

func (s *server) dispatches(w http.ResponseWriter, r *http.Request) {
	window := time.Duration(clampInt(r.URL.Query().Get("window_s"), 60, 10, 300)) * time.Second
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	now := time.Now()
	from := now.Add(-window)
	msgs, err := s.opt.Broker.DispatchesSince(ctx, from, dispatchCap)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not read the dispatch log from Redis: "+err.Error())
		return
	}
	out := SummariseDispatches(msgs, from, window, 250*time.Millisecond, dispatchCap)
	out.ReadAt = now
	writeJSON(w, http.StatusOK, out)
}

// DeadLetterEntry is a dead-lettered task's metadata. The task payload is not
// exposed: the console has no reason to show it and every reason not to.
type DeadLetterEntry struct {
	TaskID   string    `json:"task_id"`
	Tenant   string    `json:"tenant"`
	Attempts int       `json:"attempts"`
	Reason   string    `json:"reason"`
	FailedAt time.Time `json:"failed_at"`
}

func (s *server) dlq(w http.ResponseWriter, r *http.Request) {
	n := clampInt(r.URL.Query().Get("limit"), 50, 1, 200)
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	entries, err := s.opt.Broker.ReadDeadLetters(ctx, int64(n))
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not read the dead-letter stream: "+err.Error())
		return
	}
	out := make([]DeadLetterEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, DeadLetterEntry{TaskID: e.Task.ID, Tenant: e.Task.TenantID, Attempts: e.Attempts, Reason: e.Reason, FailedAt: e.FailedAt})
	}
	writeJSON(w, http.StatusOK, map[string]any{"read_at": time.Now(), "entries": out})
}

// ControlsInfo tells the interface what it may offer. It contains no secret.
type ControlsInfo struct {
	FaultsEnabled bool                    `json:"faults_enabled"`
	Reason        string                  `json:"reason,omitempty"`
	Actions       map[string][]string     `json:"actions,omitempty"`
	Processes     map[string]ProcessState `json:"processes,omitempty"`
	MinDuration   int64                   `json:"min_duration_ms"`
	MaxDuration   int64                   `json:"max_duration_ms"`
	Presets       []Preset                `json:"presets,omitempty"`
	Submission    *SubmissionStatus       `json:"submission,omitempty"`
}

func (s *server) controls(w http.ResponseWriter, _ *http.Request) {
	info := ControlsInfo{FaultsEnabled: s.opt.FaultsEnabled, MinDuration: MinFaultDuration.Milliseconds(), MaxDuration: MaxFaultDuration.Milliseconds()}
	switch {
	case s.opt.Supervisor == nil:
		info.FaultsEnabled = false
		info.Reason = "monitor mode: the console did not start these processes, so it will not signal them"
	case !s.opt.FaultsEnabled:
		info.Reason = "fault injection is off; restart the console with --enable-faults to turn it on"
	}
	if s.opt.Supervisor != nil {
		info.Processes = s.opt.Supervisor.States()
		info.Actions = map[string][]string{}
		for role, ops := range allowed {
			for _, a := range []string{ActionKill, ActionTerminate, ActionFreeze, ActionPartition, ActionRestart} {
				if ops[a] {
					info.Actions[role] = append(info.Actions[role], a)
				}
			}
		}
	}
	if s.opt.Submitter != nil {
		info.Presets = Presets
		info.Submission = s.opt.Submitter.Status()
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *server) fault(w http.ResponseWriter, r *http.Request) {
	if s.opt.Supervisor == nil || !s.opt.FaultsEnabled {
		writeErr(w, http.StatusForbidden, "fault injection is not enabled on this console")
		return
	}
	var req FaultRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	msg, err := s.opt.Supervisor.Apply(req, s.opt.Console.LeaseOwner())
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, ErrRejected) {
			code = http.StatusConflict
		}
		writeErr(w, code, err.Error())
		return
	}
	s.opt.Console.Events().Add(Event{At: time.Now(), Source: SourceControl, Kind: "fault_" + req.Action, Subject: req.Target, Message: msg})
	writeJSON(w, http.StatusOK, map[string]string{"result": msg})
}

func (s *server) clear(w http.ResponseWriter, r *http.Request) {
	if s.opt.Supervisor == nil || !s.opt.FaultsEnabled {
		writeErr(w, http.StatusForbidden, "fault injection is not enabled on this console")
		return
	}
	var req struct{}
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	done := s.opt.Supervisor.ClearAll()
	msg := "no timed faults were in progress"
	if len(done) > 0 {
		msg = strings.Join(done, "; ")
	}
	s.opt.Console.Events().Add(Event{At: time.Now(), Source: SourceControl, Kind: "faults_cleared", Message: msg})
	writeJSON(w, http.StatusOK, map[string]any{"result": msg})
}

func (s *server) workload(w http.ResponseWriter, r *http.Request) {
	if s.opt.Submitter == nil {
		writeErr(w, http.StatusForbidden, "workload submission is only available in demo mode")
		return
	}
	var req struct {
		Preset string `json:"preset"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Detached from the request: the submission outlives it, and is bounded
	// by the preset's size, not by a timeout.
	st, err := s.opt.Submitter.Start(context.WithoutCancel(r.Context()), req.Preset)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, ErrRejected) {
			code = http.StatusConflict
		}
		writeErr(w, code, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, st)
}

func clampInt(s string, def, lo, hi int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return min(max(n, lo), hi)
}

// spa serves the built frontend, falling back to index.html for unknown
// paths, and never outside the directory.
func spa(dir string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := filepath.Clean("/" + r.URL.Path)
		if st, err := os.Stat(filepath.Join(dir, clean)); err == nil && !st.IsDir() {
			files.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, "index.html"))
	})
}

// LoopbackHosts returns the Host values a console bound to addr answers to.
func LoopbackHosts(addr string) ([]string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("%s is not a loopback address", host)
	}
	return []string{"127.0.0.1:" + port, "localhost:" + port, "[::1]:" + port}, nil
}
