package metadata

import (
	"strconv"
	"testing"
	"time"
)

// TestShouldSkipBootGapFill pins the #6/C1 boot gap-fill skip: fresh push
// heartbeat → skip; stale/absent → SCAN; kill-switch → SCAN; and strictly
// boot-once (a second evaluation — a reconnect — never skips).
func TestShouldSkipBootGapFill(t *testing.T) {
	stamp := func(s *Store, age time.Duration) {
		v := strconv.FormatInt(time.Now().Add(-age).Unix(), 10)
		if err := s.SetMeta(metaKeyPushLastAlive, v); err != nil {
			t.Fatalf("SetMeta: %v", err)
		}
	}
	reset := func() { bootGapFillEvaluated.Store(false) }

	t.Run("fresh heartbeat skips", func(t *testing.T) {
		reset()
		s := openTestStore(t)
		stamp(s, 2*time.Minute)
		rc := &RedisClient{store: s}
		if !rc.shouldSkipBootGapFill() {
			t.Fatal("2m-old heartbeat inside the 15m window should skip")
		}
	})

	t.Run("stale heartbeat scans", func(t *testing.T) {
		reset()
		s := openTestStore(t)
		stamp(s, 2*time.Hour)
		rc := &RedisClient{store: s}
		if rc.shouldSkipBootGapFill() {
			t.Fatal("2h-old heartbeat must NOT skip")
		}
	})

	t.Run("no heartbeat scans", func(t *testing.T) {
		reset()
		s := openTestStore(t)
		rc := &RedisClient{store: s}
		if rc.shouldSkipBootGapFill() {
			t.Fatal("absent heartbeat must NOT skip (first run / wiped mirror)")
		}
	})

	t.Run("kill switch scans", func(t *testing.T) {
		reset()
		t.Setenv("JM_BOOT_SCAN_FRESH_SKIP", "0")
		s := openTestStore(t)
		stamp(s, time.Minute)
		rc := &RedisClient{store: s}
		if rc.shouldSkipBootGapFill() {
			t.Fatal("JM_BOOT_SCAN_FRESH_SKIP=0 must force the SCAN")
		}
	})

	t.Run("boot-once: reconnect never skips", func(t *testing.T) {
		reset()
		s := openTestStore(t)
		stamp(s, time.Minute)
		rc := &RedisClient{store: s}
		if !rc.shouldSkipBootGapFill() {
			t.Fatal("first (boot) evaluation should skip")
		}
		stamp(s, time.Second) // even fresher
		if rc.shouldSkipBootGapFill() {
			t.Fatal("second evaluation (a reconnect) must ALWAYS gap-fill")
		}
	})

	t.Run("window override", func(t *testing.T) {
		reset()
		t.Setenv("JM_BOOT_SCAN_FRESH_WINDOW_SEC", "60")
		s := openTestStore(t)
		stamp(s, 5*time.Minute) // fresh vs 15m default, stale vs 60s override
		rc := &RedisClient{store: s}
		if rc.shouldSkipBootGapFill() {
			t.Fatal("5m-old heartbeat must NOT skip under a 60s window override")
		}
	})
}
