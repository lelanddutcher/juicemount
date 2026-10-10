//go:build redisoptionsacceptance

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func bridgeCacheIntegrationFixture(t *testing.T) (string, *redis.Client, *metadata.Store, *metadata.RedisClient) {
	t.Helper()
	// Start verifies the root-supplied private TMPDIR and pinned Redis binary
	// before creating anything. Every later temporary path is below that root.
	raw := redistestfixture.Start(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	wasOffline := pin.IsAutoOffline()
	t.Cleanup(func() { pin.SetAutoOffline(wasOffline, "acceptance cleanup") })
	opts, err := redisurl.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Set(ctx, "setting", `{"UUID":"22222222-2222-4222-8222-222222222222","BlockSize":4096}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	rc, err := metadata.NewRedisClientDeferred(raw, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rc.Stop)
	return raw, rdb, store, rc
}

func bridgeCacheIntegrationOwner(t *testing.T, raw string, rdb *redis.Client, store *metadata.Store, rc *metadata.RedisClient, binary string) *bridgeCacheOwner {
	t.Helper()
	o, err := newBridgeCacheOwner(store, raw, []string{"", "", ""}, rdb.Options(), t.TempDir(), binary)
	if err != nil {
		t.Fatal(err)
	}
	rc.SetBackendQualified(false)
	o.rc.Store(rc) // same pre-qualification wiring as NFSServerStart
	t.Cleanup(o.stop)
	return o
}

func bridgeCacheIntegrationQualify(t *testing.T, o *bridgeCacheOwner) {
	t.Helper()
	o.session.SetOnline(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := o.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	if !o.session.Qualified() || !o.ready.Load() {
		t.Fatal("actual qualification did not reach bridge callback")
	}
}

func TestBridgeCacheSessionIntegrationRecoveryLoss(t *testing.T) {
	raw, rdb, store, rc := bridgeCacheIntegrationFixture(t)
	o := bridgeCacheIntegrationOwner(t, raw, rdb, store, rc, "")
	bridgeCacheIntegrationQualify(t, o)
	if !o.recoverIfSafe(func() bool { return true }, "") || pin.IsAutoOffline() {
		t.Fatal("fresh qualification did not enable recovery")
	}
	entered, release, settled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan bool, 1)
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	go func() {
		defer close(settled)
		result <- o.recoverIfSafe(func() bool {
			close(entered) // real helper has already captured epoch and checked Session
			<-release
			return true
		}, "")
	}()
	t.Cleanup(func() { releaseHeld(); <-settled })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not reach the held reachability boundary")
	}
	lossDone := make(chan struct{})
	go func() { o.session.SetOnline(false); close(lossDone) }()
	t.Cleanup(func() { releaseHeld(); <-lossDone })
	select {
	case <-lossDone:
	case <-time.After(3 * time.Second):
		t.Fatal("actual Session loss could not complete while recovery was held")
	}
	if o.session.Qualified() || !pin.IsAutoOffline() {
		t.Fatal("completed Session loss failed to close bridge admission")
	}
	if err := rc.SyncOnce(); !errors.Is(err, metadata.ErrBackendUnqualified) {
		t.Fatalf("metadata after loss = %v", err)
	}
	releaseHeld()
	select {
	case cleared := <-result:
		if cleared || !pin.IsAutoOffline() {
			t.Fatal("stale real recovery cleared completed Session loss")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("released recovery did not complete")
	}
	bridgeCacheIntegrationQualify(t, o)
	if !o.recoverIfSafe(func() bool { return true }, "") || pin.IsAutoOffline() {
		t.Fatal("fresh requalification failed to recover")
	}
	if err := rc.SyncOnce(); err != nil {
		t.Fatalf("qualified metadata sync refused: %v", err)
	}
}

func TestBridgeCacheSessionIntegrationVerifierProvenance(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		wantReader    bool
	}{
		{"unknown", "", false},
		{"selected_supported", "1.3.1", true},
		{"selected_unsupported", "9.0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, rdb, store, rc := bridgeCacheIntegrationFixture(t)
			binDir := t.TempDir()
			// A supported PATH binary must not fill unknown provenance or
			// override an explicitly selected unsupported executable.
			if err := os.WriteFile(filepath.Join(binDir, "juicefs"), []byte("#!/bin/sh\nprintf 'juicefs version 1.3.1\\n'\n"), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", binDir)
			binary := ""
			if tc.version != "" {
				binary = filepath.Join(binDir, "selected-juicefs")
				if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf 'juicefs version "+tc.version+"\\n'\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			o := bridgeCacheIntegrationOwner(t, raw, rdb, store, rc, binary)
			if err := rc.SyncOnce(); !errors.Is(err, metadata.ErrBackendUnqualified) {
				t.Fatalf("initial metadata admission = %v", err)
			}
			bridgeCacheIntegrationQualify(t, o)
			if got := o.session.Reader() != nil; got != tc.wantReader {
				t.Fatalf("reader attached = %v, want %v", got, tc.wantReader)
			}
			if !tc.wantReader && !errors.Is(o.session.ReaderError(), cache.ErrReaderUnavailable) {
				t.Fatalf("detached reader reason = %v", o.session.ReaderError())
			}
			if err := rc.SyncOnce(); err != nil {
				t.Fatalf("detached/attached reader blocked qualified metadata: %v", err)
			}
			if !o.recoverIfSafe(func() bool { return true }, "") {
				t.Fatal("qualified backend could not recover without direct acceleration")
			}
			// Repeat actual constructor on the saved binding. NewSession has
			// already attempted saved-reader verification before it returns.
			o.stop()
			saved := bridgeCacheIntegrationOwner(t, raw, rdb, store, rc, binary)
			if got := saved.session.Reader() != nil; got != tc.wantReader {
				t.Fatalf("saved reader attached = %v, want %v", got, tc.wantReader)
			}
			if saved.session.Qualified() || saved.ready.Load() {
				t.Fatal("saved constructor unexpectedly granted remote admission")
			}
		})
	}
}
