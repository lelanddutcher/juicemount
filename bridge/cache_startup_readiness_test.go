//go:build redisoptionsacceptance

package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func startupReadinessOwner(t *testing.T) *bridgeCacheOwner {
	t.Helper()
	userWas, autoWas := pin.IsUserOffline(), pin.IsAutoOffline()
	raw := redistestfixture.Start(t)
	const volumeUUID = "22222222-2222-4222-8222-222222222222"
	cacheRoot := filepath.Join(t.TempDir(), volumeUUID)
	if err := os.MkdirAll(filepath.Join(cacheRoot, "raw", "chunks"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_CACHE_DIR", cacheRoot)
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"`+volumeUUID+`","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	o, err := newBridgeCacheOwner(store, raw, []string{"", "", ""}, opts, t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		o.stop()
		pin.ResetFUSEIdentityForTest()
		pin.SetOffline(userWas)
		pin.SetAutoOffline(autoWas, "startup readiness test cleanup")
	})
	return o
}

func qualifyStartupReadinessOwner(t *testing.T, o *bridgeCacheOwner) {
	t.Helper()
	o.session.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !o.session.Qualified() || !o.ready.Load() {
		t.Fatal("actual cache qualification did not publish")
	}
}

func TestCacheStartupRecoveryBeforeListenerPublication(t *testing.T) {
	o := startupReadinessOwner(t)
	qualifyStartupReadinessOwner(t, o)
	pin.SetAutoOffline(true, "startup qualification fence")
	var listenerReady atomic.Bool
	entered, release, finished, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1), make(chan struct{})
	var once sync.Once
	releaseHeld := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseHeld()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("original startup recovery goroutine did not join")
		}
	})
	go func() {
		defer close(done)
		finished <- o.startBeforeListener(func() bool {
			close(entered)
			<-release
			return true
		}, "", &listenerReady)
	}()
	select {
	case <-entered:
	case recovered := <-finished:
		if listenerReady.Load() && pin.IsAutoOffline() {
			t.Fatal("listener advertised while healthy backend remained locally auto-offline")
		}
		t.Fatalf("startup returned without the initial recovery observation: recovered=%v", recovered)
	case <-time.After(3 * time.Second):
		t.Fatal("startup recovery did not reach reachability fence")
	}
	// The remount callback uses this same flag before it may advertise NFS.
	if listenerReady.Load() || !pin.IsAutoOffline() {
		t.Fatal("listener became publishable before the initial recovery decision")
	}
	releaseHeld()
	select {
	case recovered := <-finished:
		if !recovered || !listenerReady.Load() || pin.IsAutoOffline() {
			t.Fatal("qualified boot advertised NFS while still locally auto-offline")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial recovery did not complete")
	}
}

func TestCacheStartupRecoveryPreservesRefusals(t *testing.T) {
	for _, tc := range []struct {
		name            string
		qualify         bool
		reachable       bool
		fuseRefused     bool
		userOffline     bool
		loseDuringCheck bool
		wantRecovery    bool
	}{
		{name: "unqualified"},
		{name: "unreachable", qualify: true},
		{name: "fuse identity refused", qualify: true, reachable: true, fuseRefused: true},
		{name: "explicit user offline", qualify: true, reachable: true, userOffline: true, wantRecovery: true},
		{name: "qualification lost before clear", qualify: true, reachable: true, loseDuringCheck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := startupReadinessOwner(t)
			if tc.qualify {
				qualifyStartupReadinessOwner(t, o)
			}
			pin.SetOffline(tc.userOffline)
			pin.SetAutoOffline(true, "startup qualification fence")
			fusePath := ""
			if tc.fuseRefused {
				fusePath = t.TempDir()
				pin.SetFUSEIdentityPath(fusePath) // ordinary directory is not a FUSE generation
			}
			var listenerReady atomic.Bool
			got := o.startBeforeListener(func() bool {
				if tc.loseDuringCheck {
					o.session.SetOnline(false)
				}
				return tc.reachable
			}, fusePath, &listenerReady)
			if got != tc.wantRecovery || !listenerReady.Load() {
				t.Fatalf("recovered=%v listenerReady=%v, want recovery=%v", got, listenerReady.Load(), tc.wantRecovery)
			}
			if pin.IsAutoOffline() == tc.wantRecovery {
				t.Fatalf("automatic offline=%v after recovery=%v", pin.IsAutoOffline(), got)
			}
			if tc.userOffline && !pin.IsOffline() {
				t.Fatal("automatic recovery cleared explicit user-offline intent")
			}
		})
	}
}
