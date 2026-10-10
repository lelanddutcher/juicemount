package respproxy

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func closeCompletionWait(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func closeCompletionStillOpen(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s completed before singleton release", name)
	default:
	}
}

func TestCloseDoneAfterSingletonRelease(t *testing.T) {
	hookEntered := make(chan struct{})
	hookGate := make(chan struct{})
	releaseEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	var hookOnce, releaseOnce sync.Once
	var releases, hooks atomic.Int32
	p := singletonScripted(t, 5*time.Second, time.Hour, func(a [][]byte) string {
		switch string(a[0]) {
		case "SET":
			return "+OK\r\n"
		case "PING":
			return "+PONG\r\n"
		case "EVAL":
			if string(a[1]) != releaseLua {
				return ":1\r\n"
			}
			if releases.Add(1) == 1 {
				close(releaseEntered)
			}
			<-releaseGate
			return ":1\r\n"
		default:
			return ":1\r\n"
		}
	})
	// This cleanup runs before singletonScripted's own Close/join cleanup.
	t.Cleanup(func() {
		hookOnce.Do(func() { close(hookGate) })
		releaseOnce.Do(func() { close(releaseGate) })
	})
	hookResult := make(chan error, 1)
	p.cfg.BeforeSideClose = func() {
		hooks.Add(1)
		v, err := p.Side().Do(0, strArgs("PING")...)
		if err == nil && (v.Type != '+' || string(v.Str) != "PONG") {
			err = errors.New("side PING did not return PONG")
		}
		hookResult <- err
		close(hookEntered)
		<-hookGate
	}
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	originalClose := make(chan error, 1)
	go func() { originalClose <- p.Close() }()
	closeCompletionWait(t, "BeforeSideClose", hookEntered)
	if err := <-hookResult; err != nil {
		t.Fatalf("side unavailable before release: %v", err)
	}
	if !p.Closing() || hooks.Load() != 1 {
		t.Fatalf("close admission state=%t hooks=%d", p.Closing(), hooks.Load())
	}
	closeCompletionStillOpen(t, "Done at hook", p.Done())
	start := time.Now()
	if err := p.Close(); err != nil || time.Since(start) > time.Second {
		t.Fatalf("losing Close blocked or failed: err=%v elapsed=%s", err, time.Since(start))
	}
	closeCompletionStillOpen(t, "Done after losing Close", p.Done())
	hookOnce.Do(func() { close(hookGate) })
	closeCompletionWait(t, "singleton release request", releaseEntered)
	closeCompletionStillOpen(t, "Done during release", p.Done())
	releaseOnce.Do(func() { close(releaseGate) })
	closeCompletionWait(t, "Close Done", p.Done())
	select {
	case err := <-originalClose:
		if err != nil {
			t.Fatalf("original Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original Close result did not join")
	}
	p.side.mu.Lock()
	sideClosed := p.side.closed
	p.side.mu.Unlock()
	if !sideClosed || releases.Load() != 1 || hooks.Load() != 1 {
		t.Fatalf("close tail: sideClosed=%t releases=%d hooks=%d", sideClosed, releases.Load(), hooks.Load())
	}
}

func TestTrackAfterCloseRejectsLateConnection(t *testing.T) {
	p, err := New(Config{Upstream: "unused", Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	closeCompletionWait(t, "non-singleton Close Done", p.Done())
	client, peer := net.Pipe()
	defer peer.Close()
	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	p.track(client, true)
	var b [1]byte
	if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("late connection stayed open: %v", err)
	}
	p.mu.Lock()
	retained := len(p.conns)
	p.mu.Unlock()
	if retained != 0 {
		t.Fatalf("late connection retained in map: %d", retained)
	}
}
