package netfault_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/netfault"
)

// echoServer stands in for whatever is on the far side of the proxy.
func echoServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

func newProxy(t *testing.T, target string) *netfault.Proxy {
	t.Helper()
	p, err := netfault.New("127.0.0.1:0", target)
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go p.Serve(ctx)
	t.Cleanup(func() { cancel(); _ = p.Close() })
	return p
}

func roundTrip(t *testing.T, addr, msg string) (string, error) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// The ordinary case: the proxy is a pipe and the far side sees what was sent.
func TestProxyForwardsWhileHealthy(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())

	got, err := roundTrip(t, p.Addr(), "hello")
	if err != nil {
		t.Fatalf("round trip through a healthy proxy: %v", err)
	}
	if got != "hello" {
		t.Errorf("got %q through the proxy, want %q", got, "hello")
	}
}

// A cut has to stop traffic that is already flowing, not merely refuse the
// next connection. This is the property the store-failure experiment depends
// on: a replica with an established replication link must stop receiving.
func TestCutBreaksAnEstablishedConnection(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())

	c, err := net.DialTimeout("tcp", p.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("first")); err != nil {
		t.Fatalf("write before the cut: %v", err)
	}
	buf := make([]byte, 5)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read before the cut: %v", err)
	}

	p.Cut()

	// The established connection must die rather than quietly keep working.
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = c.Write([]byte("second"))
	if _, err := io.ReadFull(c, buf); err == nil {
		t.Fatal("the connection still carried data after the path was cut; " +
			"an experiment relying on this would report no data loss and be wrong")
	}
}

// And new connections must be refused while cut, or the endpoints simply
// reconnect and carry on.
func TestCutRefusesNewConnections(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())
	p.Cut()

	if _, err := roundTrip(t, p.Addr(), "hello"); err == nil {
		t.Fatal("a new connection succeeded through a cut proxy")
	}
	if !p.IsCut() {
		t.Error("IsCut reported false after Cut")
	}
}

// Healing restores service. The endpoints have to reconnect, which is what
// they would do after a real partition.
func TestHealRestoresService(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())

	p.Cut()
	if _, err := roundTrip(t, p.Addr(), "x"); err == nil {
		t.Fatal("expected the cut to refuse a connection")
	}

	p.Heal()
	var lastErr error
	for i := 0; i < 20; i++ {
		got, err := roundTrip(t, p.Addr(), "again")
		if err == nil {
			if got != "again" {
				t.Fatalf("got %q after healing, want %q", got, "again")
			}
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("service did not come back after Heal: %v", lastErr)
}

// Cut is idempotent and counted, so a harness can assert the fault it asked
// for actually happened.
func TestStatsRecordCutsAndAccepts(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())

	if _, err := roundTrip(t, p.Addr(), "a"); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	p.Cut()
	p.Cut() // idempotent: a second cut is not a second fault

	accepted, cuts := p.Stats()
	if accepted < 1 {
		t.Errorf("accepted = %d, want at least 1", accepted)
	}
	if cuts != 1 {
		t.Errorf("cuts = %d, want 1: Cut must be idempotent", cuts)
	}
}

// A target that cannot be reached must not take the proxy down with it; the
// client simply gets a closed connection.
func TestUnreachableTargetClosesTheClient(t *testing.T) {
	// Bind and immediately release a port to get one nothing is listening on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()

	p := newProxy(t, dead)
	if _, err := roundTrip(t, p.Addr(), "hello"); err == nil {
		t.Fatal("expected a failure when the target is unreachable")
	} else if errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error kind: %v", err)
	}
}

// Delay is applied to forwarded traffic. Kept loose on purpose: this asserts
// that the knob does something, not that the scheduler is precise.
func TestDelaySlowsForwarding(t *testing.T) {
	ln := echoServer(t)
	p := newProxy(t, ln.Addr().String())
	p.SetDelay(150 * time.Millisecond)

	start := time.Now()
	if _, err := roundTrip(t, p.Addr(), "slow"); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	// Two hops, each delayed, so at least one delay must be visible.
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Errorf("round trip took %s with a 150ms delay configured", elapsed)
	}
}
