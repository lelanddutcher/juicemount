//go:build redisoptionsacceptance

package cache

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// qualificationDelayProxy forwards opaque bytes only. One 250 ms delay is
// applied to each received TCP burst in each direction; commands, credentials,
// and response payloads are never inspected or logged.
type qualificationDelayProxy struct {
	listener   net.Listener
	upstream   string
	mu         sync.Mutex
	conns      []net.Conn
	closed     bool
	wg         sync.WaitGroup
	burstsUp   atomic.Int64
	burstsDown atomic.Int64
}

func newQualificationDelayProxy(t *testing.T, upstream string) *qualificationDelayProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &qualificationDelayProxy{listener: ln, upstream: upstream}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.wg.Add(1)
			go p.serve(client)
		}
	}()
	return p
}

func (p *qualificationDelayProxy) serve(client net.Conn) {
	defer p.wg.Done()
	server, err := net.DialTimeout("tcp", p.upstream, 2*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		_ = server.Close()
		return
	}
	p.conns = append(p.conns, client, server)
	p.mu.Unlock()
	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); p.pump(server, client, &p.burstsUp) }()
	go func() { defer pumps.Done(); p.pump(client, server, &p.burstsDown) }()
	// A completed direction retires its peer rather than leaving an owned pump
	// blocked indefinitely on a half-closed connection.
	pumps.Wait()
	_ = client.Close()
	_ = server.Close()
}

func (p *qualificationDelayProxy) pump(dst, src net.Conn, bursts *atomic.Int64) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			bursts.Add(1)
			time.Sleep(250 * time.Millisecond)
			written := 0
			for written < n {
				m, writeErr := dst.Write(buf[written:n])
				written += m
				if writeErr != nil || m == 0 {
					_ = src.Close()
					_ = dst.Close()
					return
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = src.Close()
			}
			_ = dst.Close()
			return
		}
	}
}

func (p *qualificationDelayProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

type qualificationTimeline struct {
	start     time.Time
	connected atomic.Int64
	evalStart atomic.Int64
	evalEnd   atomic.Int64
}

func (h *qualificationTimeline) mark(v *atomic.Int64) {
	v.CompareAndSwap(0, time.Since(h.start).Nanoseconds())
}
func (h *qualificationTimeline) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *qualificationTimeline) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *qualificationTimeline) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" {
			h.mark(&h.evalStart)
		}
		err := next(ctx, cmd)
		if cmd.Name() == "eval" {
			h.mark(&h.evalEnd)
		}
		return err
	}
}

// Cold connection setup must not consume a short per-probe total deadline.
// A finite qualification budget admits the selected binding through delayed
// connections while keeping admission closed until validation completes.
func TestVolumeSessionColdQualificationThroughBurstDelay(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, direct, binding := volumeFixture(t) // owned authenticated Redis
	proxy := newQualificationDelayProxy(t, direct.Options().Addr)
	defer proxy.close()
	// The direct control establishes that the seeded format and selected
	// binding are valid before any delayed transport is exercised.
	directSession, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw,
		Options: direct.Options(), StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer directSession.Stop()
	directSession.SetOnline(true)
	controlCtx, controlCancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer controlCancel()
	if err := directSession.Qualify(controlCtx); err != nil {
		t.Fatalf("direct selected volume refused: %v", err)
	}
	controlCancel()

	opts := *direct.Options()
	opts.Addr = proxy.listener.Addr().String()
	opts.Dialer = nil // production NewSession also deliberately clears Dialer
	// A separately preconnected control isolates the bounded Lua EVAL from
	// authentication/selection bootstrap. Its initial handshake uses a longer
	// transport budget; the subsequent ReadVolumeBinding retains its 2s limit.
	warmOpts := opts
	warmOpts.DialTimeout, warmOpts.ReadTimeout, warmOpts.WriteTimeout = 10*time.Second, 10*time.Second, 10*time.Second
	warm := redis.NewClient(&warmOpts)
	defer warm.Close()
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer warmCancel()
	if err := warm.Ping(warmCtx).Err(); err != nil {
		t.Fatalf("delayed proxy bootstrap control: %v", err)
	}
	warmCancel()
	warmEvalCtx, warmEvalCancel := context.WithTimeout(context.Background(), 3*time.Second)
	warmBinding, warmErr := ReadVolumeBinding(warmEvalCtx, warm, raw)
	warmEvalCancel()
	_ = warm.Close()
	if warmErr != nil || warmBinding != binding {
		t.Fatalf("preconnected delayed EVAL control: %v, binding_equal=%t", warmErr, warmBinding == binding)
	}
	clock := &qualificationTimeline{start: time.Now()}
	startUp, startDown := proxy.burstsUp.Load(), proxy.burstsDown.Load()
	opts.OnConnect = func(context.Context, *redis.Conn) error { clock.mark(&clock.connected); return nil }
	var published atomic.Bool
	store := newVolumeTestStore()
	s, err := NewSession(SessionConfig{Store: store, LogicalURL: raw, Options: &opts,
		StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }, OnQualified: published.Store})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.probe.AddHook(clock)
	if published.Load() || s.Qualified() {
		t.Fatal("cold session published before qualification")
	}
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	err = s.Qualify(ctx)
	t.Logf("secret-free cold qualification: connected_ns=%d eval_hook_start_ns=%d eval_hook_end_ns=%d cold_bursts_up=%d cold_bursts_down=%d error_class=%T",
		clock.connected.Load(), clock.evalStart.Load(), clock.evalEnd.Load(), proxy.burstsUp.Load()-startUp, proxy.burstsDown.Load()-startDown, err)
	if err != nil {
		if published.Load() || s.Qualified() {
			t.Fatal("failed qualification published admission")
		}
		t.Fatalf("finite parent budget did not qualify delayed selected volume: %v", err)
	}
	if !published.Load() || !s.Qualified() {
		t.Fatal("successful qualification did not publish")
	}
	if got, ok := s.SnapshotBinding(); !ok || got != binding {
		t.Fatal("wrong selected binding published")
	}
}

func TestVolumeSessionBurstDelayRespectsShortParentDeadline(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	raw, direct, _ := volumeFixture(t)
	proxy := newQualificationDelayProxy(t, direct.Options().Addr)
	defer proxy.close()
	opts := *direct.Options()
	opts.Addr = proxy.listener.Addr().String()
	opts.Dialer = nil
	var published atomic.Bool
	s, err := NewSession(SessionConfig{Store: newVolumeTestStore(), LogicalURL: raw, Options: &opts,
		StateDir: t.TempDir(), VerifyReader: func(*Reader) error { return nil }, OnQualified: published.Store})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = s.Qualify(ctx)
	if err == nil || time.Since(start) >= 3*time.Second {
		t.Fatalf("short parent deadline failed closed in %v: %v", time.Since(start), err)
	}
	if published.Load() || s.Qualified() {
		t.Fatal("timed-out cold qualification published admission")
	}
	if _, exists, err := LoadVolumeBinding(s.cfg.Store, raw); err != nil || exists {
		t.Fatalf("timed-out cold qualification saved a binding: exists=%t err=%v", exists, err)
	}
}
