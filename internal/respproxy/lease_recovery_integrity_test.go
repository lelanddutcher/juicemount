package respproxy

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func recoveryEngine(t *testing.T) (*LeaseEngine, *redis.Client) {
	t.Helper()
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	p, err := New(Config{Upstream: addr, VolumeDB: testDB, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return NewLeaseEngine(LeaseConfig{Side: p.Side(), VolumeDB: testDB, Logf: t.Logf}), rdb
}

func putRecoveryLease(t *testing.T, rdb *redis.Client, proj uint64, counter, hashEpoch string, holder string) {
	t.Helper()
	ctx := context.Background()
	if counter != "" {
		if err := rdb.Set(ctx, epochKey(proj), counter, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if hashEpoch != "" {
		if err := rdb.HSet(ctx, leaseKey(proj), map[string]any{
			"holder": holder, "kind": string(KindMac), "host": "mac1-host",
			"epoch": hashEpoch, "mode": LeaseAuto, "granted_ms": "1",
			"last_write_ms": "1", "last_hb_ms": "1",
			"checkout_until_ms": "0", "pending": "0", "label": "A",
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLeaseRecoveryCoherentHeldAndFree(t *testing.T) {
	e, rdb := recoveryEngine(t)
	putRecoveryLease(t, rdb, 3, "1", "1", "mac1")
	putRecoveryLease(t, rdb, 4, "2", "2", "")
	start := time.Now().UnixMilli()
	if err := e.loadLeases(start); err != nil {
		t.Fatal(err)
	}
	if got := e.leases[3]; got == nil || got.holder != "mac1" || got.epoch != 1 || got.lastHbMs != start {
		t.Fatalf("held lease not restored with existing orphan grace: %+v", got)
	}
	if got := e.leases[4]; got == nil || got.held() || got.epoch != 2 {
		t.Fatalf("free lease not restored: %+v", got)
	}
	if got := rdb.Get(context.Background(), epochKey(3)).Val(); got != "1" {
		t.Fatalf("startup changed counter: %q", got)
	}
}

func TestLeaseRecoveryPreservesExactLargeEpoch(t *testing.T) {
	e, rdb := recoveryEngine(t)
	const epoch = "9007199254740993" // Above Lua's exact-number range.
	putRecoveryLease(t, rdb, 3, epoch, epoch, "mac1")
	if err := e.loadLeases(time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got := e.leases[3].epoch; got != 9007199254740993 {
		t.Fatalf("loaded epoch %d, want %s", got, epoch)
	}
}

func TestLeaseRecoveryRejectsBadEpochWithoutPartialPublish(t *testing.T) {
	cases := []struct{ name, counter, hashEpoch string }{
		{"missing_counter", "", "1"},
		{"zero_counter", "0", "1"},
		{"malformed_counter", "bad", "1"},
		{"noncanonical_counter", "01", "1"},
		{"too_large_counter", "9223372036854775808", "1"},
		{"missing_hash_epoch", "1", ""},
		{"zero_hash_epoch", "1", "0"},
		{"malformed_hash_epoch", "1", "bad"},
		{"noncanonical_hash_epoch", "1", "01"},
		{"too_large_hash_epoch", "1", "9223372036854775808"},
		{"precision_alias", "9007199254740993", "9007199254740992"},
		{"advanced_counter", "2", "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, rdb := recoveryEngine(t)
			putRecoveryLease(t, rdb, 3, "1", "1", "mac1")
			putRecoveryLease(t, rdb, 4, tc.counter, tc.hashEpoch, "mac1")
			if tc.hashEpoch == "" {
				if err := rdb.HSet(context.Background(), leaseKey(4), "holder", "mac1").Err(); err != nil {
					t.Fatal(err)
				}
			}
			sentinel := &lease{proj: 99, holder: "untouched"}
			e.leases[99] = sentinel
			if err := e.loadLeases(time.Now().UnixMilli()); err == nil {
				t.Fatal("accepted corrupt persisted lease")
			}
			if len(e.leases) != 1 || e.leases[99] != sentinel {
				t.Fatalf("published partial startup candidates: %+v", e.leases)
			}
			if tc.counter != "" {
				if got := rdb.Get(context.Background(), epochKey(4)).Val(); got != tc.counter {
					t.Fatalf("startup rewrote counter %q to %q", tc.counter, got)
				}
			}
		})
	}
}

func TestLeaseRecoveryAllowsOrphanCounterWithoutHash(t *testing.T) {
	e, rdb := recoveryEngine(t)
	if err := rdb.Set(context.Background(), epochKey(3), "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := e.loadLeases(time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if len(e.leases) != 0 || rdb.Get(context.Background(), epochKey(3)).Val() != "1" {
		t.Fatalf("orphan counter should remain untouched: %+v", e.leases)
	}
}

func TestLeaseRecoveryRejectsMalformedHashKey(t *testing.T) {
	e, rdb := recoveryEngine(t)
	if err := rdb.HSet(context.Background(), LeaseKeyPrefix+"bad", "epoch", "1").Err(); err != nil {
		t.Fatal(err)
	}
	if err := e.loadLeases(time.Now().UnixMilli()); err == nil {
		t.Fatal("accepted malformed lease key")
	}
	if len(e.leases) != 0 {
		t.Fatalf("published lease from malformed key: %+v", e.leases)
	}
}

func TestLeaseRecoveryRejectsTornEndAfterRestart(t *testing.T) {
	e, rdb := recoveryEngine(t)
	ctx := context.Background()
	putRecoveryLease(t, rdb, 3, "1", "1", "mac1")
	if err := e.loadLeases(time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	// Historical state from the old INCR-then-HSET end path: the end
	// advanced the counter, but a failed HSET left the old holder hash.
	if err := rdb.Set(ctx, epochKey(3), "2", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := rdb.Get(ctx, epochKey(3)).Val(); got != "2" {
		t.Fatalf("counter = %q, want 2", got)
	}
	if got := rdb.HGet(ctx, leaseKey(3), "epoch").Val(); got != "1" {
		t.Fatalf("persisted hash epoch = %q, want old 1", got)
	}
	e2 := NewLeaseEngine(LeaseConfig{Side: e.cfg.Side, VolumeDB: testDB, Logf: t.Logf})
	if err := e2.loadLeases(time.Now().UnixMilli()); err == nil {
		t.Fatal("restart revived old holder after torn end")
	}
	if len(e2.leases) != 0 {
		t.Fatalf("restart published torn lease: %+v", e2.leases)
	}
	if got := rdb.Get(ctx, epochKey(3)).Val(); got != "2" {
		t.Fatalf("restart changed counter to %q", got)
	}
}
