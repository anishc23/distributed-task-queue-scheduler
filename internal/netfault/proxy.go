// Package netfault provides a TCP proxy that can be broken on purpose.
//
// It exists because of a failed experiment. The store-failure experiment needs
// a Redis replica that is genuinely missing writes the master had already
// acknowledged, and the obvious way to arrange that — SIGSTOP the replica
// process, write to the master, then kill the master — does not work. A stopped
// process still has a kernel that accepts and acknowledges TCP segments on its
// behalf, so the replication stream piles up in the socket buffer and is
// applied in full the moment the process is continued. The replica ends up with
// everything, the experiment reports that nothing was lost, and the conclusion
// is wrong.
//
// Losing data in transit requires cutting the path, not pausing the reader.
// This proxy sits in the path and can be cut.
//
// It is deliberately small. It is not a general-purpose fault injector and does
// not simulate packet loss, reordering or bandwidth limits; it closes
// connections and refuses new ones, which is what a partition looks like to an
// application, and it can delay bytes, which is what a slow link looks like.
package netfault

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy forwards TCP connections from a local address to a target, and can sever
// that path on demand. The zero value is not usable; call New.
type Proxy struct {
	target   string
	listener net.Listener

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	cut     atomic.Bool
	delay   atomic.Int64 // nanoseconds added to each forwarded chunk
	accepts atomic.Int64
	cuts    atomic.Int64

	closeOnce sync.Once
	done      chan struct{}
}

// New binds a listener. Passing "127.0.0.1:0" picks a free port, which Addr
// then reports.
func New(listen, target string) (*Proxy, error) {
	if target == "" {
		return nil, errors.New("netfault: target address is required")
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("netfault: listen on %s: %w", listen, err)
	}
	return &Proxy{
		target:   target,
		listener: ln,
		conns:    make(map[net.Conn]struct{}),
		done:     make(chan struct{}),
	}, nil
}

// Addr is the address clients should dial.
func (p *Proxy) Addr() string { return p.listener.Addr().String() }

// Serve accepts connections until the context is cancelled or Close is called.
// It is meant to be run in its own goroutine.
func (p *Proxy) Serve(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			p.Close()
		case <-p.done:
		}
	}()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return // listener closed
		}
		if p.cut.Load() {
			// A partition does not politely queue connections.
			_ = client.Close()
			continue
		}
		p.accepts.Add(1)
		go p.handle(client)
	}
}

func (p *Proxy) handle(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client)
	p.track(upstream)
	defer func() {
		p.forget(client)
		p.forget(upstream)
		_ = client.Close()
		_ = upstream.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pipe(upstream, client) }()
	go func() { defer wg.Done(); p.pipe(client, upstream) }()
	wg.Wait()
}

// pipe copies one direction, honouring the configured delay and stopping
// immediately when the proxy is cut.
func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		if p.cut.Load() {
			return
		}
		// A read deadline keeps a quiet connection from sitting in Read
		// forever and missing a cut.
		_ = src.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := src.Read(buf)
		if n > 0 {
			if d := time.Duration(p.delay.Load()); d > 0 {
				time.Sleep(d)
			}
			if p.cut.Load() {
				// Bytes read but not forwarded. This is the case the
				// experiment depends on: the sender believes it sent them.
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if !errors.Is(err, io.EOF) {
				return
			}
			return
		}
	}
}

func (p *Proxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *Proxy) forget(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// Cut severs the path: every open connection is closed and new ones are
// refused until Heal is called.
//
// Closing rather than blackholing is deliberate. A silent blackhole makes the
// endpoints wait out their own timeouts, which takes long enough to blur an
// experiment measured in seconds; a close is the same loss of data in transit,
// delivered immediately.
func (p *Proxy) Cut() {
	if p.cut.Swap(true) {
		return
	}
	p.cuts.Add(1)
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
}

// Heal restores forwarding. Connections closed by Cut are not restored; the
// endpoints have to reconnect, which is what they would do after a partition.
func (p *Proxy) Heal() { p.cut.Store(false) }

// IsCut reports whether the path is currently severed.
func (p *Proxy) IsCut() bool { return p.cut.Load() }

// SetDelay adds latency to every forwarded chunk in both directions. Zero
// removes it.
func (p *Proxy) SetDelay(d time.Duration) {
	if d < 0 {
		d = 0
	}
	p.delay.Store(int64(d))
}

// Stats reports how many connections were accepted and how many times the path
// was cut, so a test can assert the fault it asked for actually happened.
func (p *Proxy) Stats() (accepted, cuts int64) {
	return p.accepts.Load(), p.cuts.Load()
}

// Close stops accepting and drops every connection.
func (p *Proxy) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.listener.Close()
		p.mu.Lock()
		for c := range p.conns {
			_ = c.Close()
		}
		p.conns = make(map[net.Conn]struct{})
		p.mu.Unlock()
	})
	return err
}
