package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Encoding helpers mirroring the JuiceFS d/i byte layout, used to seed Redis
// (live tests) and to exercise decodeDirChild/decodeInodeAttr directly.
// These are the INVERSE of the decoders in keyspace.go; if a future schema
// change breaks one, the parity test breaks loudly.
// ---------------------------------------------------------------------------

// encodeDirChild builds the 9-byte directory-entry HASH value: byte[0]=ft,
// bytes[5:9]=big-endian low-32 inode. Bytes 1..4 are the high word (always 0
// in the live tree), matching what gi() ignores.
func encodeDirChild(childInode uint64, ft byte) []byte {
	v := make([]byte, 9)
	v[0] = ft
	v[5] = byte(childInode >> 24)
	v[6] = byte(childInode >> 16)
	v[7] = byte(childInode >> 8)
	v[8] = byte(childInode)
	return v
}

// encodeInodeAttr builds a JuiceFS i-key attr blob long enough (>=59 bytes)
// to carry mtime at 0-based offset 23 and size at 0-based offset 51, both
// big-endian u64 — matching the Lua u64(attr,24)/u64(attr,52).
func encodeInodeAttr(mtime, size int64) []byte {
	b := make([]byte, 64)
	putBE64(b, 23, uint64(mtime))
	putBE64(b, 51, uint64(size))
	return b
}

func putBE64(b []byte, off int, v uint64) {
	b[off+0] = byte(v >> 56)
	b[off+1] = byte(v >> 48)
	b[off+2] = byte(v >> 40)
	b[off+3] = byte(v >> 32)
	b[off+4] = byte(v >> 24)
	b[off+5] = byte(v >> 16)
	b[off+6] = byte(v >> 8)
	b[off+7] = byte(v)
}

// ---------------------------------------------------------------------------
// decodeDirChild / decodeInodeAttr round-trip.
// ---------------------------------------------------------------------------

func TestDecodeDirChild(t *testing.T) {
	cases := []struct {
		inode uint64
		ft    byte
	}{
		{732093, 1}, // file (matches the live d732093 sample)
		{10466, 2},  // dir
		{1, 2},
		{748363, 1}, // near the live max inode
	}
	for _, c := range cases {
		val := encodeDirChild(c.inode, c.ft)
		gotInode, gotFt, ok := decodeDirChild(val)
		if !ok {
			t.Fatalf("decodeDirChild(%d,%d): ok=false", c.inode, c.ft)
		}
		if gotInode != c.inode || gotFt != c.ft {
			t.Errorf("decodeDirChild = (%d,%d), want (%d,%d)", gotInode, gotFt, c.inode, c.ft)
		}
	}

	// Wrong length must be rejected (mirrors gi()'s #v~=9 guard).
	if _, _, ok := decodeDirChild(make([]byte, 8)); ok {
		t.Error("decodeDirChild on 8-byte value should be ok=false")
	}
	if _, _, ok := decodeDirChild(make([]byte, 10)); ok {
		t.Error("decodeDirChild on 10-byte value should be ok=false")
	}
}

func TestDecodeInodeAttr(t *testing.T) {
	mt := int64(1_700_000_000)
	sz := int64(53_477_376)
	attr := encodeInodeAttr(mt, sz)
	gotMt, gotSz, ok := decodeInodeAttr(attr)
	if !ok {
		t.Fatal("decodeInodeAttr: ok=false on full attr")
	}
	if gotMt != mt || gotSz != sz {
		t.Errorf("decodeInodeAttr = (%d,%d), want (%d,%d)", gotMt, gotSz, mt, sz)
	}

	// Short attr (< 59 bytes) must be rejected, leaving caller to default 0/0.
	if _, _, ok := decodeInodeAttr(make([]byte, 58)); ok {
		t.Error("decodeInodeAttr on 58-byte blob should be ok=false")
	}
}

// ---------------------------------------------------------------------------
// Channel parsing (db-index aware) — the wrong-db / verb-agnostic guard.
// ---------------------------------------------------------------------------

func TestParseDirInodeFromChannel(t *testing.T) {
	prefix := "__keyspace@1__:"
	cases := []struct {
		channel   string
		wantInode uint64
		wantOK    bool
	}{
		{"__keyspace@1__:d732093", 732093, true},
		{"__keyspace@1__:d1", 1, true},
		{"__keyspace@1__:delfiles", 0, false},  // d + non-numeric -> ignored
		{"__keyspace@1__:delSlices", 0, false}, // d + non-numeric -> ignored
		{"__keyspace@1__:i732093", 0, false},   // i-key, not a d-key
		{"__keyspace@0__:d732093", 0, false},   // WRONG DB: prefix mismatch -> rejected
		{"__keyspace@1__:d", 0, false},         // bare d
		{"garbage", 0, false},
	}
	for _, c := range cases {
		got, ok := parseDirInodeFromChannel(c.channel, prefix)
		if ok != c.wantOK || (ok && got != c.wantInode) {
			t.Errorf("parseDirInodeFromChannel(%q) = (%d,%v), want (%d,%v)",
				c.channel, got, ok, c.wantInode, c.wantOK)
		}
	}
}

// ---------------------------------------------------------------------------
// notify-keyspace-events sufficiency decision (the FALLBACK gate).
// ---------------------------------------------------------------------------

func TestKeyspaceNotifySufficient(t *testing.T) {
	cases := []struct {
		flags string
		want  bool
	}{
		{"", false},     // live NAS today: empty -> DISABLED
		{"Kghx", true},  // recommended value
		{"KEghx", true}, // with keyevent observability
		{"KA", true},    // K + all-classes
		{"AKE", true},   // order-independent
		{"Kg", false},   // missing hash class
		{"Kh", false},   // missing generic class
		{"ghx", false},  // missing keyspace family (K)
		{"Egh", false},  // keyevent only, no keyspace (K) -> our channel learns nothing
		{"K", false},    // K alone insufficient
	}
	for _, c := range cases {
		if got := keyspaceNotifySufficient(c.flags); got != c.want {
			t.Errorf("keyspaceNotifySufficient(%q) = %v, want %v", c.flags, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// keyspacePushEnabled kill switch.
// ---------------------------------------------------------------------------

func TestKeyspacePushEnabledKillSwitch(t *testing.T) {
	rc := &RedisClient{}
	t.Setenv("JM_METADATA_KEYSPACE_PUSH", "")
	if rc.keyspacePushEnabled() {
		t.Error("unset env should be DISABLED")
	}
	t.Setenv("JM_METADATA_KEYSPACE_PUSH", "0")
	if rc.keyspacePushEnabled() {
		t.Error("=0 should be DISABLED")
	}
	t.Setenv("JM_METADATA_KEYSPACE_PUSH", "1")
	if !rc.keyspacePushEnabled() {
		t.Error("=1 should be ENABLED")
	}
}

// ---------------------------------------------------------------------------
// Class-gating: interface name band -> backstop interval + coalescer tuning.
// Regression guard for the ticker.Reset-on-running-loop behavior is in
// TestBackstopController below.
// ---------------------------------------------------------------------------

func TestCurrentLinkClassBands(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "")
	cases := []struct {
		iface string
		want  linkClass
	}{
		{"en21", classLAN},
		{"eth0", classLAN},
		{"en0", classWiFi},
		{"utun4", classTunnel},
		{"tailscale0", classTunnel},
		{"weird9", classWiFi}, // unknown -> conservative WiFi
	}
	for _, c := range cases {
		SetClassSignals(func() string { return c.iface }, nil)
		if got := currentLinkClass(); got != c.want {
			t.Errorf("currentLinkClass(iface=%q) = %v, want %v", c.iface, got, c.want)
		}
	}
	// JM_WAN_MODE forces tunnel regardless of interface.
	SetClassSignals(func() string { return "en21" }, nil)
	t.Setenv("JM_WAN_MODE", "1")
	if got := currentLinkClass(); got != classTunnel {
		t.Errorf("JM_WAN_MODE=1 with en21 = %v, want classTunnel", got)
	}
	// No interface signal wired -> WiFi default (still tunnel under WAN mode).
	t.Setenv("JM_WAN_MODE", "")
	SetClassSignals(nil, nil)
	if got := currentLinkClass(); got != classWiFi {
		t.Errorf("nil iface signal = %v, want classWiFi", got)
	}
}

func TestBackstopAndTuningOrdering(t *testing.T) {
	// Tunnel/cellular MUST get the longest backstop and loosest coalescing —
	// that is the motivation (rare cellular SCAN) — but the cellular SCAN is
	// CAPPED LOW, not maximally rare, to bound staleness.
	// Every class demotes the SCAN well below the 30s DISABLED cadence:
	for _, c := range []linkClass{classLAN, classWiFi, classTunnel} {
		if backstopForClass(c) <= DefaultReconcileInterval {
			t.Errorf("class %v backstop %v must exceed the 30s DISABLED cadence", c, backstopForClass(c))
		}
	}
	// Tunnel/cellular is CAPPED LOW (<= lan/wifi), NOT the longest. The cap bounds
	// the backstop-only staleness windows (foreign in-place attr edits + a delete
	// missed during a reconnect gap) on the flap-prone metered link to minutes,
	// not hours, while still making the expensive cellular SCAN rare vs the old
	// constant 30s cadence.
	if backstopForClass(classTunnel) > backstopForClass(classLAN) ||
		backstopForClass(classTunnel) > backstopForClass(classWiFi) {
		t.Errorf("tunnel backstop must be capped <= lan/wifi: lan=%v wifi=%v tunnel=%v",
			backstopForClass(classLAN), backstopForClass(classWiFi), backstopForClass(classTunnel))
	}
	if backstopForClass(classTunnel) != 5*time.Minute {
		t.Errorf("tunnel/cellular backstop must be the 5m staleness cap, got %v", backstopForClass(classTunnel))
	}
	// Coalescing is still LOOSER on the metered link (fewer, larger batches).
	if !(tuningForClass(classLAN).debounce < tuningForClass(classTunnel).debounce) {
		t.Error("tunnel debounce should be looser than LAN")
	}
	// DISABLED/DEGRADED always 30s regardless of class.
	rc := &RedisClient{}
	rc.backstopNanos.Store(int64(DefaultReconcileInterval))
	SetClassSignals(func() string { return "en21" }, func() bool { return true })
	rc.setEngagement(keyspaceDegraded)
	if rc.currentBackstop() != DefaultReconcileInterval {
		t.Errorf("DEGRADED backstop = %v, want 30s", rc.currentBackstop())
	}
	rc.setEngagement(keyspaceDisabled)
	if rc.currentBackstop() != DefaultReconcileInterval {
		t.Errorf("DISABLED backstop = %v, want 30s", rc.currentBackstop())
	}
	// ENABLED + reachable -> long (LAN here).
	rc.setEngagement(keyspaceEnabled)
	if rc.currentBackstop() != backstopForClass(classLAN) {
		t.Errorf("ENABLED(LAN) backstop = %v, want %v", rc.currentBackstop(), backstopForClass(classLAN))
	}
	// ENABLED but UNREACHABLE -> falls back to 30s (push delivery is down).
	SetClassSignals(func() string { return "en21" }, func() bool { return false })
	rc.setEngagement(keyspaceEnabled)
	if rc.currentBackstop() != DefaultReconcileInterval {
		t.Errorf("ENABLED+unreachable backstop = %v, want 30s", rc.currentBackstop())
	}
	SetClassSignals(nil, nil) // reset global hooks for other tests
}

// TestNetworkChangeDeferralPredicate locks the exact gate the reconcileLoop
// syncNowCh handler uses to DEFER a network-change full SCAN to the keyspace
// push: currentBackstop() > DefaultReconcileInterval. That is true IFF the push
// is ENABLED + reachable (PSUBSCRIBE up, deltas flowing) — the one state in
// which a per-flap full 200k SCAN is redundant, because the keyspace loop owns
// reconnect gap-fill and the rare backstop is the safety net. In every other
// state (DISABLED / DEGRADED / ENABLED-but-unreachable) the gate is false and
// the classic flap-debounce + SCAN fires as the authoritative fallback —
// byte-identical to pre-push behavior. If a future setEngagement change set a
// long backstop in a non-ENABLED state, this test fails before the live
// regression (a flaky link silently losing its convergence SCAN) ever ships.
func TestNetworkChangeDeferralPredicate(t *testing.T) {
	// Mirrors the reconcileLoop syncNowCh gate exactly.
	deferred := func(rc *RedisClient) bool {
		return rc.currentBackstop() > DefaultReconcileInterval
	}
	rc := &RedisClient{}
	rc.backstopNanos.Store(int64(DefaultReconcileInterval))

	// ENABLED + reachable -> DEFER (push carries deltas; per-flap SCAN redundant).
	SetClassSignals(func() string { return "en0" }, func() bool { return true })
	rc.setEngagement(keyspaceEnabled)
	if !deferred(rc) {
		t.Errorf("ENABLED+reachable must DEFER the network-change SCAN (backstop=%v)", rc.currentBackstop())
	}
	// DEGRADED -> do NOT defer (PSUBSCRIBE dropped; the SCAN is the fallback).
	rc.setEngagement(keyspaceDegraded)
	if deferred(rc) {
		t.Errorf("DEGRADED must NOT defer; classic SCAN must fire (backstop=%v)", rc.currentBackstop())
	}
	// DISABLED -> do NOT defer (no push; byte-identical classic 30s behavior).
	rc.setEngagement(keyspaceDisabled)
	if deferred(rc) {
		t.Errorf("DISABLED must NOT defer (backstop=%v)", rc.currentBackstop())
	}
	// ENABLED but UNREACHABLE -> do NOT defer (push delivery is down).
	SetClassSignals(func() string { return "en0" }, func() bool { return false })
	rc.setEngagement(keyspaceEnabled)
	if deferred(rc) {
		t.Errorf("ENABLED+unreachable must NOT defer (backstop=%v)", rc.currentBackstop())
	}
	SetClassSignals(nil, nil)
}

// ---------------------------------------------------------------------------
// Coalescer: collapse a burst on one dir to ONE reconcile; promote a wide
// burst to ONE full SCAN.
// ---------------------------------------------------------------------------

// countingRC stubs reconcileDir/TriggerSync by recording calls. We can't stub
// methods directly, so we use a real RedisClient with a counting hook via a
// dedicated test seam: the coalescer calls rc.reconcileDir and rc.TriggerSync.
// To observe those without Redis, we drive the coalescer's flush logic through
// a fake by overriding behavior with channels — done by spinning a real rc
// whose store has the dirs, but here we only need to count promotion vs
// per-dir. We use the exported-for-test seam below.

func TestCoalescerCollapseAndPromote(t *testing.T) {
	t.Setenv("JM_WAN_MODE", "")
	SetClassSignals(func() string { return "en21" }, func() bool { return true }) // LAN: debounce 200ms

	var (
		mu          sync.Mutex
		reconciled  []uint64
		triggerSync int
	)
	rc := &RedisClient{}
	rc.testReconcileDir = func(inode uint64) error {
		mu.Lock()
		reconciled = append(reconciled, inode)
		mu.Unlock()
		return nil
	}
	rc.testTriggerSync = func() {
		mu.Lock()
		triggerSync++
		mu.Unlock()
	}

	// (i) 1000 events on ONE dir -> exactly one reconcileDir, zero promotions.
	co := newInodeCoalescer(rc)
	for i := 0; i < 1000; i++ {
		co.add(42)
	}
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reconciled) == 1
	})
	mu.Lock()
	if len(reconciled) != 1 || reconciled[0] != 42 || triggerSync != 0 {
		mu.Unlock()
		t.Fatalf("single-dir burst: reconciled=%v triggerSync=%d, want [42] / 0", reconciled, triggerSync)
	}
	reconciled = nil
	mu.Unlock()
	co.stop()

	// (ii) 300 distinct dirs (> burst ceiling 200) -> ONE TriggerSync, no
	// per-dir reconciles.
	co2 := newInodeCoalescer(rc)
	for i := uint64(1000); i < 1300; i++ {
		co2.add(i)
	}
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return triggerSync == 1
	})
	mu.Lock()
	if triggerSync != 1 || len(reconciled) != 0 {
		mu.Unlock()
		t.Fatalf("wide burst: triggerSync=%d reconciled=%d, want 1 / 0", triggerSync, len(reconciled))
	}
	mu.Unlock()
	co2.stop()
	SetClassSignals(nil, nil)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}

// ---------------------------------------------------------------------------
// Backstop controller: a RUNNING reconcileLoop must pick up a live
// backstopNanos change (regression guard for the baseInterval captured-once
// bug). We don't run the full loop here (it would hit Redis); instead we
// assert the read path currentBackstop reflects atomic writes, which is what
// reconcileLoop consults each turn.
// ---------------------------------------------------------------------------

func TestCurrentBackstopReflectsLiveWrites(t *testing.T) {
	rc := &RedisClient{}
	rc.backstopNanos.Store(int64(DefaultReconcileInterval))
	if rc.currentBackstop() != DefaultReconcileInterval {
		t.Fatalf("initial = %v", rc.currentBackstop())
	}
	rc.backstopNanos.Store(int64(45 * time.Minute))
	if rc.currentBackstop() != 45*time.Minute {
		t.Fatalf("after live write = %v, want 45m", rc.currentBackstop())
	}
	// Zero/garbage -> safe default, never a 0-duration ticker.
	rc.backstopNanos.Store(0)
	if rc.currentBackstop() != DefaultReconcileInterval {
		t.Fatalf("zero backstop = %v, want default", rc.currentBackstop())
	}
}

// ---------------------------------------------------------------------------
// EDGE Bug A regression: a foreign delete of a TOP-LEVEL entry (under root)
// must be pruned by the push path. The store indexes root's children under
// childrenIdx["."] (each child's ParentPath = path.Dir("movies") == "."), so
// scopedPrune MUST be driven with the "." key, not "". Before the fix
// reconcileDir passed parentPath="" to scopedPrune, making ListChildren("")
// empty and the root-level scoped prune DEAD CODE. This test exercises
// scopedPrune directly against a real Store (no Redis needed) and would fail
// if scopedPrune were called with "" for the root.
// ---------------------------------------------------------------------------

func TestScopedPruneRootKey(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	defer store.Close()

	mt := time.Unix(1700000000, 0)
	// Two top-level entries; both are indexed under childrenIdx["."].
	for _, e := range []*Entry{
		MakeEntry("movies", true, 0, mt, 10),
		MakeEntry("notes.txt", false, 4444, mt, 30),
	} {
		store.InsertToCache(e)
		if err := store.Insert(e); err != nil {
			t.Fatalf("Insert %q: %v", e.Path, err)
		}
	}

	rc := &RedisClient{store: store} // no fuseRoot, no pin checker

	// Sanity: the root children live under "." not "".
	if c, _ := store.ListChildren("."); len(c) != 2 {
		t.Fatalf("precondition: ListChildren(\".\") = %d, want 2", len(c))
	}
	if c, _ := store.ListChildren(""); len(c) != 0 {
		t.Fatalf("precondition: ListChildren(\"\") = %d, want 0 (root keyed under \".\")", len(c))
	}

	// Foreign delete of /notes.txt: fresh Redis set for root now has only
	// "movies". Drive the prune with the ROOT store key ("."), exactly as the
	// fixed reconcileDir(1) does.
	fresh := map[string]struct{}{"movies": {}}
	rc.scopedPrune(".", fresh)

	if store.LookupByPath("notes.txt") != nil {
		t.Error("EDGE Bug A: top-level notes.txt was NOT pruned (scopedPrune root-key mismatch)")
	}
	if store.LookupByPath("movies") == nil {
		t.Error("movies (still present in Redis) must survive the prune")
	}

	// Guard: driving the prune with the WRONG key ("") must be a no-op — this
	// is the pre-fix behavior, asserted here so a regression that reverts to
	// "" is caught.
	store.InsertToCache(MakeEntry("notes.txt", false, 4444, mt, 30))
	_ = store.Insert(MakeEntry("notes.txt", false, 4444, mt, 30))
	rc.scopedPrune("", map[string]struct{}{"movies": {}})
	if store.LookupByPath("notes.txt") == nil {
		t.Error("scopedPrune(\"\") should be a no-op for root children (sanity for the regression guard)")
	}
}

// ===========================================================================
// LIVE-REDIS tests (skip cleanly when no local Redis is reachable). These
// exercise reconcileDir end-to-end and the PARITY gate: incremental store
// state MUST equal a fresh full-SCAN store state over the same Redis data.
// ===========================================================================

const liveTestRedisURL = "redis://127.0.0.1:6379/15" // db 15: throwaway test db

func liveRedisOrSkip(t *testing.T) *redis.Client {
	t.Helper()
	addr, db, _ := ParseRedisURL(liveTestRedisURL)
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db, DialTimeout: 1 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("local Redis not reachable, skipping live test: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		rdb.Close()
		t.Skipf("cannot flush test db15, refusing to touch a shared db: %v", err)
	}
	t.Cleanup(func() {
		ctx2, c2 := context.WithTimeout(context.Background(), 1*time.Second)
		defer c2()
		rdb.FlushDB(ctx2)
		rdb.Close()
	})
	return rdb
}

// seedJuiceFSTree writes a small d/i key tree mirroring the live JuiceFS
// schema. Layout:
//
//	/                     (inode 1, root — implicit)
//	/movies               (inode 10, dir)
//	/movies/a.mov         (inode 11, file)
//	/movies/b.mov         (inode 12, file)
//	/movies/sub           (inode 20, dir)
//	/movies/sub/c.mov     (inode 21, file)
//	/notes.txt            (inode 30, file)
func seedJuiceFSTree(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx := context.Background()
	hset := func(dirInode uint64, name string, childInode uint64, ft byte) {
		if err := rdb.HSet(ctx, fmt.Sprintf("d%d", dirInode), name, string(encodeDirChild(childInode, ft))).Err(); err != nil {
			t.Fatalf("HSet d%d %s: %v", dirInode, name, err)
		}
	}
	iset := func(inode uint64, mtime, size int64) {
		if err := rdb.Set(ctx, fmt.Sprintf("i%d", inode), string(encodeInodeAttr(mtime, size)), 0).Err(); err != nil {
			t.Fatalf("Set i%d: %v", inode, err)
		}
	}
	hset(1, "movies", 10, 2)
	hset(1, "notes.txt", 30, 1)
	hset(10, "a.mov", 11, 1)
	hset(10, "b.mov", 12, 1)
	hset(10, "sub", 20, 2)
	hset(20, "c.mov", 21, 1)
	iset(10, 1700000010, 0)
	iset(11, 1700000011, 1111)
	iset(12, 1700000012, 2222)
	iset(20, 1700000020, 0)
	iset(21, 1700000021, 3333)
	iset(30, 1700000030, 4444)
}

type storeSnapshot map[string]struct {
	inode uint64
	size  int64
	mtime int64
	isDir bool
}

func snapshotStore(s *Store) storeSnapshot {
	snap := make(storeSnapshot)
	paths, _ := s.AllPaths()
	for p := range paths {
		e := s.LookupByPath(p)
		if e == nil {
			continue
		}
		snap[p] = struct {
			inode uint64
			size  int64
			mtime int64
			isDir bool
		}{e.Inode, e.Size, e.Mtime.Unix(), e.IsDir}
	}
	return snap
}

// newLiveRC builds a RedisClient bound to the live test db with a fresh store.
//
// IMPORTANT: uses a unique temp-FILE SQLite DB, NOT ":memory:". The in-memory
// store uses cache=shared, so every ":memory:" Store in this process shares
// ONE underlying database — which would make the "incremental store" and the
// "fresh full-SCAN store" the same store and invalidate the parity comparison.
func newLiveRC(t *testing.T, rdb *redis.Client) *RedisClient {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	rc, err := NewRedisClient(liveTestRedisURL, store)
	if err != nil {
		t.Skipf("Redis not reachable: %v", err)
	}
	t.Cleanup(func() { rc.Stop() })
	return rc
}

// TestReconcileDirLive walks the seeded tree top-down via reconcileDir and
// verifies the incremental result matches a fresh full SCAN — the PARITY gate.
func TestReconcileDirLive(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	seedJuiceFSTree(t, rdb)

	// Incremental build: establish root's children, then descend. reconcileDir
	// on a dir whose inode isn't yet in the store would TriggerSync; we descend
	// in inode order so each parent is established before its children.
	inc := newLiveRC(t, rdb)
	for _, inode := range []uint64{1, 10, 20} {
		if err := inc.reconcileDir(inode); err != nil {
			t.Fatalf("reconcileDir(%d): %v", inode, err)
		}
	}
	incSnap := snapshotStore(inc.store)

	// Authoritative full SCAN into a fresh store.
	full := newLiveRC(t, rdb)
	if err := full.SyncOnce(); err != nil {
		t.Fatalf("full SyncOnce: %v", err)
	}
	fullSnap := snapshotStore(full.store)

	assertSnapshotsEqual(t, "reconcileDir vs full SCAN", incSnap, fullSnap)

	// Sanity: the expected 6 entries are present.
	for _, p := range []string{"movies", "notes.txt", "movies/a.mov", "movies/b.mov", "movies/sub", "movies/sub/c.mov"} {
		if _, ok := incSnap[p]; !ok {
			t.Errorf("incremental snapshot missing %q", p)
		}
	}
}

// TestParityAfterMutationsLive applies foreign ops (create/delete/rename/move/
// rmdir) to Redis, drives the changed dirs through reconcileDir, and asserts
// the result equals a fresh full SCAN over the SAME final Redis state.
func TestParityAfterMutationsLive(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	seedJuiceFSTree(t, rdb)
	ctx := context.Background()

	inc := newLiveRC(t, rdb)
	// Baseline establish.
	for _, inode := range []uint64{1, 10, 20} {
		if err := inc.reconcileDir(inode); err != nil {
			t.Fatalf("baseline reconcileDir(%d): %v", inode, err)
		}
	}

	// --- Foreign mutations directly in Redis ---
	// 1. CREATE /movies/new.mov (inode 13)
	rdb.HSet(ctx, "d10", "new.mov", string(encodeDirChild(13, 1)))
	rdb.Set(ctx, "i13", string(encodeInodeAttr(1700000013, 5555)), 0)
	// 2. DELETE /movies/a.mov
	rdb.HDel(ctx, "d10", "a.mov")
	rdb.Del(ctx, "i11")
	// 3. RENAME in place /movies/b.mov -> /movies/b2.mov (inode 12 preserved)
	rdb.HDel(ctx, "d10", "b.mov")
	rdb.HSet(ctx, "d10", "b2.mov", string(encodeDirChild(12, 1)))
	// 4. RMDIR subtree /movies/sub (+ its child c.mov)
	rdb.HDel(ctx, "d10", "sub")
	rdb.Del(ctx, "d20", "i20", "i21")

	// Drive the changed dirs (the coalescer would deliver exactly these).
	if err := inc.reconcileDir(10); err != nil {
		t.Fatalf("reconcileDir(10) post-mutation: %v", err)
	}

	incSnap := snapshotStore(inc.store)

	full := newLiveRC(t, rdb)
	if err := full.SyncOnce(); err != nil {
		t.Fatalf("full SyncOnce post-mutation: %v", err)
	}
	fullSnap := snapshotStore(full.store)

	assertSnapshotsEqual(t, "post-mutation parity", incSnap, fullSnap)

	// Explicit expectations on the incremental store.
	if _, ok := incSnap["movies/new.mov"]; !ok {
		t.Error("create not reflected: movies/new.mov missing")
	}
	if _, ok := incSnap["movies/a.mov"]; ok {
		t.Error("delete not reflected: movies/a.mov still present")
	}
	if _, ok := incSnap["movies/b.mov"]; ok {
		t.Error("rename old path still present: movies/b.mov")
	}
	if _, ok := incSnap["movies/b2.mov"]; !ok {
		t.Error("rename new path missing: movies/b2.mov")
	}
	if _, ok := incSnap["movies/sub"]; ok {
		t.Error("rmdir subtree-root still present: movies/sub")
	}
	if _, ok := incSnap["movies/sub/c.mov"]; ok {
		t.Error("rmdir descendant not prefix-pruned: movies/sub/c.mov")
	}
}

// TestScopedPruneQA30PinSafetyLive: a foreign delete of a sibling must prune
// it, but a PINNED file must NEVER be pruned even when absent from Redis.
func TestScopedPruneQA30PinSafetyLive(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	seedJuiceFSTree(t, rdb)
	ctx := context.Background()

	rc := newLiveRC(t, rdb)
	for _, inode := range []uint64{1, 10, 20} {
		if err := rc.reconcileDir(inode); err != nil {
			t.Fatalf("reconcileDir(%d): %v", inode, err)
		}
	}

	// Pin /movies/a.mov (internal path). The pin store keys on mountpoint-
	// prefixed paths; with mountPoint unset, internalFromMounted is identity,
	// so we pin the internal path directly.
	rc.store.SetPinChecker(&fakePinChecker{paths: map[string]struct{}{
		"movies/a.mov": {},
	}})

	// Foreign delete BOTH a.mov (pinned) and b.mov (not pinned) from Redis.
	rdb.HDel(ctx, "d10", "a.mov", "b.mov")
	rdb.Del(ctx, "i11", "i12")

	if err := rc.reconcileDir(10); err != nil {
		t.Fatalf("reconcileDir(10): %v", err)
	}

	if rc.store.LookupByPath("movies/a.mov") == nil {
		t.Error("QA-30 violation: pinned movies/a.mov was pruned")
	}
	if rc.store.LookupByPath("movies/b.mov") != nil {
		t.Error("non-pinned movies/b.mov should have been pruned")
	}
}

// TestUnknownInodeDefersToSyncLive: reconcileDir on an inode not yet in the
// store must NOT walk Redis ancestors — it defers to a full SCAN via
// TriggerSync and returns nil without mutating the store.
func TestUnknownInodeDefersToSyncLive(t *testing.T) {
	rdb := liveRedisOrSkip(t)
	seedJuiceFSTree(t, rdb)

	rc := newLiveRC(t, rdb)
	var triggered int
	rc.testTriggerSync = func() { triggered++ }

	// inode 20 (movies/sub) is unknown — never established. reconcileDir must
	// defer.
	if err := rc.reconcileDir(20); err != nil {
		t.Fatalf("reconcileDir(20): %v", err)
	}
	if triggered != 1 {
		t.Errorf("unknown inode should TriggerSync exactly once, got %d", triggered)
	}
	if n := len(snapshotStore(rc.store)); n != 0 {
		t.Errorf("unknown-inode reconcile mutated store (%d entries), want 0", n)
	}
}

func assertSnapshotsEqual(t *testing.T, label string, a, b storeSnapshot) {
	t.Helper()
	if len(a) != len(b) {
		t.Errorf("%s: path-set size differs: incremental=%d full=%d", label, len(a), len(b))
	}
	keys := map[string]struct{}{}
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, p := range sorted {
		av, aok := a[p]
		bv, bok := b[p]
		if aok != bok {
			t.Errorf("%s: %q present incremental=%v full=%v", label, p, aok, bok)
			continue
		}
		if av != bv {
			t.Errorf("%s: %q differs incremental=%+v full=%+v", label, p, av, bv)
		}
	}
}
