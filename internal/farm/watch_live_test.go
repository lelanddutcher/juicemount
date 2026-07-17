package farm

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ===========================================================================
// LIVE-REDIS tests for the keyspace-event PUSH path (skip cleanly when no
// Redis is reachable). The farm's whole auto-discovery model rests on Redis
// actually EMITTING __keyspace@N__:d<inode> events and the Watcher.Run() loop
// engaging on them — a path the pure-unit watch_test.go bypasses (it calls
// Note/Tick directly). The known production fragility is a Redis whose
// notify-keyspace-events got unset (e.g. after a rebuild), silently killing
// push discovery. These tests exercise the real subscription end-to-end and
// assert the DISABLED-config negative too, so a regression is caught here
// instead of as "the farm mysteriously stopped auto-queuing dirs".
//
// Point JM_TEST_REDIS at a THROWAWAY instance (the test mutates the global
// notify-keyspace-events config and flushes its db); it defaults to a local
// db 15. CONFIG is saved and restored on cleanup.
// ===========================================================================

func farmTestRedisURL() string {
	if u := os.Getenv("JM_TEST_REDIS"); u != "" {
		return u
	}
	return "redis://127.0.0.1:6379/15" // db 15: throwaway test db
}

// farmLiveRedisOrSkip returns a client to the test db (flushed) plus its db
// index, or skips the test when no Redis is reachable. It also saves and
// restores notify-keyspace-events so a shared instance is left as it was found.
func farmLiveRedisOrSkip(t *testing.T) (*redis.Client, int) {
	t.Helper()
	opt, err := redis.ParseURL(farmTestRedisURL())
	if err != nil {
		t.Fatalf("bad JM_TEST_REDIS: %v", err)
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("local Redis not reachable, skipping live keyspace test: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("cannot flush test db, refusing to touch a shared db: %v", err)
	}
	// Save the original notify config to restore on cleanup (it is a GLOBAL,
	// not per-db, setting — be a good citizen on a shared instance).
	var orig string
	if m, err := rdb.ConfigGet(ctx, "notify-keyspace-events").Result(); err == nil {
		orig = m["notify-keyspace-events"]
	}
	t.Cleanup(func() {
		ctx2, c2 := context.WithTimeout(context.Background(), time.Second)
		defer c2()
		rdb.ConfigSet(ctx2, "notify-keyspace-events", orig)
		rdb.FlushDB(ctx2)
		rdb.Close()
	})
	return rdb, opt.DB
}

func setKeyspaceEvents(t *testing.T, rdb *redis.Client, flags string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.ConfigSet(ctx, "notify-keyspace-events", flags).Err(); err != nil {
		t.Skipf("cannot CONFIG SET notify-keyspace-events (managed Redis?): %v", err)
	}
}

// TestKeyspaceEventDeliveryLive proves the two halves of the fragility:
//   - with notify-keyspace-events ENABLED, a write to a d<inode> key actually
//     produces a __keyspace@N__:d<inode> message (Redis IS receiving/emitting);
//   - with it DISABLED, the same write produces NOTHING — so this test would
//     go red if the production config ever silently reverts.
func TestKeyspaceEventDeliveryLive(t *testing.T) {
	rdb, db := farmLiveRedisOrSkip(t)
	ctx := context.Background()
	pattern := fmt.Sprintf("__keyspace@%d__:d*", db)

	// --- ENABLED: the event must arrive ---
	setKeyspaceEvents(t, rdb, "KEA")
	sub := rdb.PSubscribe(ctx, pattern)
	defer sub.Close()
	// Wait for the subscription to be confirmed before writing (go-redis docs).
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe confirm: %v", err)
	}
	ch := sub.Channel()

	if err := rdb.HSet(ctx, "d777", "child.mov", "x").Err(); err != nil {
		t.Fatalf("HSet d777: %v", err)
	}
	select {
	case m := <-ch:
		if m == nil {
			t.Fatal("nil keyspace message")
		}
		// Channel is __keyspace@<db>__:d777 — confirm it's our key.
		if wantSuffix := ":d777"; len(m.Channel) < len(wantSuffix) || m.Channel[len(m.Channel)-len(wantSuffix):] != wantSuffix {
			t.Fatalf("event on unexpected channel %q", m.Channel)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ENABLED: no keyspace event within 3s — Redis is not emitting d-key events")
	}

	// --- DISABLED: the event must NOT arrive (the fragility guard) ---
	setKeyspaceEvents(t, rdb, "")
	// Drain anything already buffered so the silence check is clean.
	drain(ch)
	if err := rdb.HSet(ctx, "d778", "child.mov", "x").Err(); err != nil {
		t.Fatalf("HSet d778: %v", err)
	}
	select {
	case m := <-ch:
		if m != nil {
			t.Fatalf("DISABLED: got a keyspace event %q — notify config change did not take effect", m.Channel)
		}
	case <-time.After(750 * time.Millisecond):
		// Expected: silence.
	}
}

// TestWatcherEngagesOnKeyspaceEventLive proves the full farm path: a real
// d<inode> write flows through Watcher.Run()'s PSUBSCRIBE, gets Noted, settles,
// and is enqueued as its resolved directory. This is the end-to-end "the
// watcher DOES engage after" assertion.
func TestWatcherEngagesOnKeyspaceEventLive(t *testing.T) {
	rdb, _ := farmLiveRedisOrSkip(t)
	setKeyspaceEvents(t, rdb, "KEA")
	ctx := context.Background()

	var (
		mu       sync.Mutex
		enqueued []string
	)
	w := NewWatcher(WatchConfig{
		MetaURL: farmTestRedisURL(),
		Resolve: func(_ context.Context, ino uint64) (string, bool) {
			if ino == 42 {
				return "clips/reel1", true
			}
			return "", false
		},
		Enqueue: func(_ context.Context, rel string) error {
			mu.Lock()
			enqueued = append(enqueued, rel)
			mu.Unlock()
			return nil
		},
		Settle:     50 * time.Millisecond,
		Tick:       20 * time.Millisecond,
		MaxPerTick: 10,
		Logf:       t.Logf,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(runCtx) }()

	// Give Run() time to establish its PSUBSCRIBE before the trigger write
	// (keyspace events are fire-and-forget — a write before the subscription is
	// active would be missed).
	time.Sleep(400 * time.Millisecond)
	if err := rdb.HSet(ctx, "d42", "clip.mov", "x").Err(); err != nil {
		t.Fatalf("HSet d42: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := append([]string(nil), enqueued...)
		mu.Unlock()
		for _, r := range got {
			if r == "clips/reel1" {
				cancel()
				<-done
				return // success: the watcher engaged on the real event
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatalf("watcher did not enqueue the resolved dir within 3s (enqueued=%v)", enqueued)
}

func drain(ch <-chan *redis.Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
