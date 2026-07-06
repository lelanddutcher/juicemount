package metadata

import (
	"strconv"
	"testing"
	"time"
)

// newMetaTestStore opens an in-memory store WITHOUT requiring Redis — enough
// for the store_meta round-trip and ShouldSkipBootSync truth-table tests.
func newMetaTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestStoreMetaRoundTrip covers the C1 durable key/value side-table: an absent
// key reads back (ok=false, no error), a set value reads back verbatim, and an
// upsert overwrites in place.
func TestStoreMetaRoundTrip(t *testing.T) {
	store := newMetaTestStore(t)

	// Absent key.
	if _, ok, err := store.GetMeta("nope"); err != nil {
		t.Fatalf("GetMeta absent: unexpected err: %v", err)
	} else if ok {
		t.Fatalf("GetMeta absent: ok=true, want false")
	}

	// Set + get.
	if err := store.SetMeta("k1", "v1"); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	got, ok, err := store.GetMeta("k1")
	if err != nil {
		t.Fatalf("GetMeta after set: %v", err)
	}
	if !ok || got != "v1" {
		t.Fatalf("GetMeta after set: got=%q ok=%v, want %q true", got, ok, "v1")
	}

	// Upsert overwrites.
	if err := store.SetMeta("k1", "v2"); err != nil {
		t.Fatalf("SetMeta overwrite: %v", err)
	}
	got, ok, err = store.GetMeta("k1")
	if err != nil {
		t.Fatalf("GetMeta after overwrite: %v", err)
	}
	if !ok || got != "v2" {
		t.Fatalf("GetMeta after overwrite: got=%q ok=%v, want %q true", got, ok, "v2")
	}
}

// setLastSync persists a last_sync_time meta value at the given wall clock.
func setLastSync(t *testing.T, store *Store, at time.Time) {
	t.Helper()
	if err := store.SetMeta(metaKeyLastSyncTime, strconv.FormatInt(at.Unix(), 10)); err != nil {
		t.Fatalf("seed last_sync_time: %v", err)
	}
}

// TestShouldSkipBootSync exercises the C1 truth table: push on+fresh → skip;
// push off → no-skip; stale → no-skip; env override (max-age + kill switch);
// first-run/no-lastSyncTime → no-skip; corrupt/future timestamps → no-skip.
func TestShouldSkipBootSync(t *testing.T) {
	const pushEnv = "JM_METADATA_KEYSPACE_PUSH"
	const ageEnv = "JM_BOOT_SYNC_MAX_AGE_SEC"
	const killEnv = "JM_BOOT_SYNC_SKIP"

	t.Run("push on + fresh -> skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Hour))
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		if !rc.ShouldSkipBootSync() {
			t.Error("fresh mirror + push on should skip the boot SCAN")
		}
	})

	t.Run("push off -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Hour))
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "0")
		if rc.ShouldSkipBootSync() {
			t.Error("push off must always run the boot SCAN")
		}
	})

	t.Run("push unset -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Hour))
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "")
		if rc.ShouldSkipBootSync() {
			t.Error("push unset must always run the boot SCAN")
		}
	})

	t.Run("stale (older than window) -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-25*time.Hour)) // > 24h default
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		if rc.ShouldSkipBootSync() {
			t.Error("stale mirror (>24h) must run the boot SCAN")
		}
	})

	t.Run("first run / no last_sync_time -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		if rc.ShouldSkipBootSync() {
			t.Error("no persisted last_sync_time must run the boot SCAN")
		}
	})

	t.Run("env override shrinks window -> stale -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-2*time.Hour)) // fresh vs 24h default
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		t.Setenv(ageEnv, "3600") // 1h window → the 2h-old sync is now stale
		if rc.ShouldSkipBootSync() {
			t.Error("with a 1h override window, a 2h-old sync must run the SCAN")
		}
	})

	t.Run("env override widens window -> fresh -> skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-30*time.Hour)) // stale vs 24h default
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		t.Setenv(ageEnv, strconv.Itoa(60*60*48)) // 48h window → 30h-old is fresh
		if !rc.ShouldSkipBootSync() {
			t.Error("with a 48h override window, a 30h-old sync should skip")
		}
	})

	t.Run("env override 0 -> never skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Minute)) // very fresh
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		t.Setenv(ageEnv, "0")
		if rc.ShouldSkipBootSync() {
			t.Error("JM_BOOT_SYNC_MAX_AGE_SEC=0 must never skip")
		}
	})

	t.Run("malformed env override -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Minute))
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		t.Setenv(ageEnv, "not-a-number")
		if rc.ShouldSkipBootSync() {
			t.Error("malformed JM_BOOT_SYNC_MAX_AGE_SEC must fail safe (no skip)")
		}
	})

	t.Run("kill switch JM_BOOT_SYNC_SKIP=0 -> no skip even when fresh", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(-1*time.Minute))
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		t.Setenv(killEnv, "0")
		if rc.ShouldSkipBootSync() {
			t.Error("JM_BOOT_SYNC_SKIP=0 must force the always-sync behavior")
		}
	})

	t.Run("corrupt persisted timestamp -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		if err := store.SetMeta(metaKeyLastSyncTime, "garbage"); err != nil {
			t.Fatalf("seed corrupt value: %v", err)
		}
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		if rc.ShouldSkipBootSync() {
			t.Error("corrupt last_sync_time must fail safe (no skip)")
		}
	})

	t.Run("future timestamp (clock skew) -> no skip", func(t *testing.T) {
		store := newMetaTestStore(t)
		setLastSync(t, store, time.Now().Add(2*time.Hour)) // future
		rc := &RedisClient{store: store}
		t.Setenv(pushEnv, "1")
		if rc.ShouldSkipBootSync() {
			t.Error("a future last_sync_time (clock skew) must fail safe (no skip)")
		}
	})
}
