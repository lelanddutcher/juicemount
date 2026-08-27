package nfs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

type blockingPresenceStore struct {
	calls atomic.Int64
}

func (s *blockingPresenceStore) block(ctx context.Context) error {
	s.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}
func (s *blockingPresenceStore) hset(ctx context.Context, _, _, _ string) error {
	return s.block(ctx)
}
func (s *blockingPresenceStore) hdel(ctx context.Context, _ string, _ ...string) error {
	return s.block(ctx)
}
func (s *blockingPresenceStore) expire(ctx context.Context, _ string, _ time.Duration) error {
	return s.block(ctx)
}
func (s *blockingPresenceStore) scanKeys(context.Context, string) ([]string, error) {
	return nil, nil
}
func (s *blockingPresenceStore) hgetall(context.Context, string) (map[string]string, error) {
	return nil, nil
}

// TestPresenceUpdatesNeverBlockWritePath protects the contract that optional
// collaboration telemetry cannot add its Redis deadline to an NFS WRITE close.
func TestPresenceUpdatesNeverBlockWritePath(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(false)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	store := &blockingPresenceStore{}
	p := newPresenceTracker("test-host", store)
	t.Cleanup(p.Stop)

	start := time.Now()
	p.Open("project.prproj")
	p.Close("project.prproj")
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("presence Open+Close blocked write path for %s", elapsed)
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for store.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.calls.Load() == 0 {
		t.Fatal("background presence worker did not receive the queued update")
	}
}

// TestPresenceSkipsRedisOffline proves a known-dead remote path creates no
// telemetry attempt at all; local presence state still updates correctly.
func TestPresenceSkipsRedisOffline(t *testing.T) {
	wasOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	store := &blockingPresenceStore{}
	p := newPresenceTracker("test-host", store)
	t.Cleanup(p.Stop)
	p.Open("offline.mov")
	if got := len(p.LocalSnapshot()); got != 1 {
		t.Fatalf("local snapshot entries=%d, want 1", got)
	}
	p.Close("offline.mov")
	if got := len(p.LocalSnapshot()); got != 0 {
		t.Fatalf("local snapshot entries after close=%d, want 0", got)
	}
	time.Sleep(25 * time.Millisecond)
	if got := store.calls.Load(); got != 0 {
		t.Fatalf("offline presence attempted %d Redis operations, want 0", got)
	}
}
