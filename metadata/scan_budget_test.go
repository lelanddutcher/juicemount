package metadata

// G7 (task #80): class-gated SCAN budget + deferred-not-failed slow-link
// syncs. Live problem 2026-07-01: a fixed 120s SCAN budget on a cellular
// relay (full SCAN needs ~200-300s) produced 31 consecutive "redis SCAN
// batch: context deadline exceeded" failures over 3h, each fast-retried by
// the failure backoff (saturating the metered link) while /activity rendered
// an eternal "Rebuilding index…" — with the keyspace push engaged and
// carrying deltas the whole time.
//
// Pure-struct + local-listener tests, no live Redis (bare &RedisClient{} per
// the established harness style in this package).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// 1. scanContextTimeout: class table + env override.
// ---------------------------------------------------------------------------

func TestScanContextTimeoutClassTable(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "")
	t.Setenv("JM_SCAN_TIMEOUT_SEC", "")
	t.Cleanup(func() { SetClassSignals(nil, nil) })

	cases := []struct {
		iface string
		want  time.Duration
	}{
		{"en21", 120 * time.Second},       // LAN — byte-identical to the old fixed budget
		{"eth0", 120 * time.Second},       // LAN
		{"en0", 120 * time.Second},        // WiFi — byte-identical
		{"utun4", 300 * time.Second},      // tunnel/cellular
		{"tailscale0", 300 * time.Second}, // tunnel
	}
	for _, c := range cases {
		SetClassSignals(func() string { return c.iface }, nil)
		if got := scanContextTimeout(); got != c.want {
			t.Errorf("scanContextTimeout(iface=%q) = %v, want %v", c.iface, got, c.want)
		}
	}

	// JM_WAN_MODE=1 forces the tunnel band regardless of interface.
	SetClassSignals(func() string { return "en21" }, nil)
	t.Setenv("JM_WAN_MODE", "1")
	if got := scanContextTimeout(); got != 300*time.Second {
		t.Errorf("JM_WAN_MODE=1 with en21 = %v, want 300s", got)
	}

	// No interface signal wired → WiFi default → 120s.
	t.Setenv("JM_WAN_MODE", "")
	SetClassSignals(nil, nil)
	if got := scanContextTimeout(); got != 120*time.Second {
		t.Errorf("no signal = %v, want 120s (WiFi default)", got)
	}
}

func TestScanContextTimeoutEnvOverride(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "1") // tunnel: class default would be 300s
	t.Cleanup(func() { SetClassSignals(nil, nil) })

	t.Setenv("JM_SCAN_TIMEOUT_SEC", "45")
	if got := scanContextTimeout(); got != 45*time.Second {
		t.Errorf("JM_SCAN_TIMEOUT_SEC=45 = %v, want 45s", got)
	}
	// 0 / negative / garbage keep the default (class) logic.
	for _, v := range []string{"0", "-5", "garbage"} {
		t.Setenv("JM_SCAN_TIMEOUT_SEC", v)
		if got := scanContextTimeout(); got != 300*time.Second {
			t.Errorf("JM_SCAN_TIMEOUT_SEC=%q = %v, want class default 300s", v, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 2. Deferred-not-failed classification.
// ---------------------------------------------------------------------------

func TestIsDeferredSyncErrClassification(t *testing.T) {
	t.Setenv("JM_SYNC_DEFERRAL", "")
	deadlineErr := fmt.Errorf("redis SCAN batch: %w", context.DeadlineExceeded)
	otherErr := errors.New("dial tcp: connection refused")

	rc := &RedisClient{}
	// backstop unset → DefaultReconcileInterval → push NOT engaged.
	if rc.isDeferredSyncErr(deadlineErr) {
		t.Error("deadline error with push NOT engaged must be a FAILURE, not deferred")
	}

	rc.backstopNanos.Store(int64(15 * time.Minute)) // push ENGAGED
	if !rc.isDeferredSyncErr(deadlineErr) {
		t.Error("deadline error with push engaged must be DEFERRED")
	}
	if rc.isDeferredSyncErr(otherErr) {
		t.Error("non-deadline error must never be deferred (real outage → fail-fast backoff)")
	}
	if rc.isDeferredSyncErr(nil) {
		t.Error("nil error must not be deferred")
	}

	// Kill switch restores pre-G7 behavior exactly.
	t.Setenv("JM_SYNC_DEFERRAL", "0")
	if rc.isDeferredSyncErr(deadlineErr) {
		t.Error("JM_SYNC_DEFERRAL=0 must disable the deferred classification")
	}
}

// hungRedisClient returns a RedisClient whose rdb points at a local TCP
// listener that accepts and reads but NEVER replies — every command blocks
// until the caller's context deadline fires, reproducing the slow-link SCAN
// budget expiry without a live Redis.
func hungRedisClient(t *testing.T) *RedisClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(io.Discard, c) // swallow writes, never respond
			}(c)
		}
	}()
	rc := &RedisClient{}
	rc.rdb.Store(redis.NewClient(&redis.Options{Addr: ln.Addr().String()}))
	t.Cleanup(func() { rc.redisDB().Close() })
	return rc
}

// TestDoReconcileDeferredSkipsBackoff drives the REAL doReconcile→syncMetadata
// path into a budget expiry (1s via env) with the push engaged, and asserts
// the G7 contract: no consecutiveFailures bump, no connected=false flip,
// IsSyncing false, deferred reason set.
func TestDoReconcileDeferredSkipsBackoff(t *testing.T) {
	t.Setenv("JM_SCAN_TIMEOUT_SEC", "1")
	t.Setenv("JM_SYNC_DEFERRAL", "")
	t.Setenv("JM_WAN_MODE", "")

	rc := hungRedisClient(t)
	rc.backstopNanos.Store(int64(15 * time.Minute)) // push ENGAGED
	rc.connected = true

	consecutive := 0
	backoff := 15 * time.Minute
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	rc.doReconcile(&consecutive, &backoff, 5*time.Minute, ticker)

	if consecutive != 0 {
		t.Errorf("consecutiveFailures = %d, want 0 (deferred must not drive the failure backoff)", consecutive)
	}
	if backoff != 15*time.Minute {
		t.Errorf("backoff = %v, want the backstop 15m (next attempt at the normal backstop tick)", backoff)
	}
	if rc.IsSyncing() {
		t.Error("IsSyncing() = true after a deferred attempt; /activity would render an eternal Rebuilding index…")
	}
	if rc.SyncDeferredReason() == "" {
		t.Error("SyncDeferredReason() empty after a deferred attempt")
	}
	rc.mu.RLock()
	connected := rc.connected
	rc.mu.RUnlock()
	if !connected {
		t.Error("deferred attempt flipped connected=false; push-healthy means the backend IS reachable")
	}
}

// TestDoReconcileFailurePathUnchanged: same budget expiry but with the push
// NOT engaged (30s backstop) — the classic failure backoff must be
// byte-identical to before (counter bump, connected=false, ticker shortened).
func TestDoReconcileFailurePathUnchanged(t *testing.T) {
	t.Setenv("JM_SCAN_TIMEOUT_SEC", "1")
	t.Setenv("JM_SYNC_DEFERRAL", "")
	t.Setenv("JM_WAN_MODE", "")

	rc := hungRedisClient(t)
	rc.backstopNanos.Store(int64(DefaultReconcileInterval)) // push NOT engaged
	rc.connected = true

	consecutive := 0
	backoff := DefaultReconcileInterval
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	rc.doReconcile(&consecutive, &backoff, 5*time.Minute, ticker)

	if consecutive != 1 {
		t.Errorf("consecutiveFailures = %d, want 1 (push not engaged → real failure path)", consecutive)
	}
	if backoff != DefaultReconcileInterval*2 {
		t.Errorf("backoff = %v, want %v (first-failure backoff)", backoff, DefaultReconcileInterval*2)
	}
	if rc.SyncDeferredReason() != "" {
		t.Errorf("SyncDeferredReason() = %q, want empty on a genuine failure", rc.SyncDeferredReason())
	}
	if rc.IsSyncing() {
		t.Error("IsSyncing() = true after a failed attempt")
	}
	rc.mu.RLock()
	connected := rc.connected
	rc.mu.RUnlock()
	if connected {
		t.Error("genuine failure must still flip connected=false (offline detection unchanged)")
	}
}

// ---------------------------------------------------------------------------
// 3. IsSyncing truthfulness across outcomes.
// ---------------------------------------------------------------------------

func TestIsSyncingFalseAfterDeferredOrFailed(t *testing.T) {
	t.Setenv("JM_SYNC_DEFERRAL", "")
	rc := &RedisClient{}
	rc.backstopNanos.Store(int64(15 * time.Minute)) // push ENGAGED
	deadlineErr := fmt.Errorf("redis SCAN batch: %w", context.DeadlineExceeded)

	// In-flight sync → syncing.
	rc.mu.Lock()
	rc.lastSyncStartedAt = time.Now()
	rc.mu.Unlock()
	if !rc.IsSyncing() {
		t.Fatal("IsSyncing() = false during an in-flight sync")
	}

	// Deferred outcome → not syncing, reason set, and — flap-debounce
	// contract — lastSyncStartedAt is NOT zeroed (reconcileLoop reads it to
	// suppress flap-triggered SCANs right after the link was saturated).
	rc.noteSyncOutcome(deadlineErr)
	if rc.IsSyncing() {
		t.Error("IsSyncing() = true after a DEFERRED attempt")
	}
	if rc.SyncDeferredReason() == "" {
		t.Error("SyncDeferredReason() empty after a deferred attempt")
	}
	rc.mu.RLock()
	startedZeroed := rc.lastSyncStartedAt.IsZero()
	rc.mu.RUnlock()
	if startedZeroed {
		t.Error("lastSyncStartedAt was zeroed — breaks the reconcileLoop flap-debounce semantics")
	}

	// A NEW attempt flips syncing back on.
	rc.mu.Lock()
	rc.lastSyncStartedAt = time.Now()
	rc.mu.Unlock()
	if !rc.IsSyncing() {
		t.Fatal("IsSyncing() = false for a fresh attempt after a deferral")
	}

	// Genuine failure (non-deferred) also ends "syncing" and clears the reason.
	rc.noteSyncOutcome(errors.New("dial tcp: connection refused"))
	if rc.IsSyncing() {
		t.Error("IsSyncing() = true after a FAILED attempt")
	}
	if rc.SyncDeferredReason() != "" {
		t.Errorf("SyncDeferredReason() = %q after a genuine failure, want empty", rc.SyncDeferredReason())
	}
}

// ---------------------------------------------------------------------------
// 4. Streak counter: once-per-streak logging input, reset on success.
// ---------------------------------------------------------------------------

func TestDeferredStreakCounterResetsOnSuccess(t *testing.T) {
	t.Setenv("JM_SYNC_DEFERRAL", "")
	rc := &RedisClient{}
	rc.backstopNanos.Store(int64(15 * time.Minute)) // push ENGAGED
	deadlineErr := fmt.Errorf("redis SCAN batch: %w", context.DeadlineExceeded)

	// Three consecutive deferrals: the streak counts each attempt, but only
	// the 0→1 transition emits the Warn (noteSyncOutcome logs iff prevStreak
	// was 0 — asserted here via the counter the log decision keys on).
	for i := 1; i <= 3; i++ {
		rc.noteSyncOutcome(deadlineErr)
		rc.mu.RLock()
		streak := rc.syncDeferredStreak
		rc.mu.RUnlock()
		if streak != i {
			t.Fatalf("after %d deferrals: syncDeferredStreak = %d, want %d", i, streak, i)
		}
	}

	// Success resets the streak and clears the reason, re-arming the
	// once-per-streak Warn for the next deferral run.
	rc.noteSyncOutcome(nil)
	rc.mu.RLock()
	streak := rc.syncDeferredStreak
	rc.mu.RUnlock()
	if streak != 0 {
		t.Errorf("after success: syncDeferredStreak = %d, want 0", streak)
	}
	if rc.SyncDeferredReason() != "" {
		t.Errorf("after success: SyncDeferredReason() = %q, want empty", rc.SyncDeferredReason())
	}

	// The next deferral starts a NEW streak at 1 (Warn fires again).
	rc.noteSyncOutcome(deadlineErr)
	rc.mu.RLock()
	streak = rc.syncDeferredStreak
	rc.mu.RUnlock()
	if streak != 1 {
		t.Errorf("new streak after recovery: syncDeferredStreak = %d, want 1", streak)
	}
}
