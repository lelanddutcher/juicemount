package respproxy

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// A side connection can lose its reply after Redis committed the script.
// The engine must not infer that the old RAM holder is still authoritative.
type lostTransitionReply struct {
	inner sideDoer
	mu    sync.Mutex
	op    string
}

func (s *lostTransitionReply) Do(db int, args ...[]byte) (Value, error) {
	s.mu.Lock()
	lose := len(args) > 5 && string(args[0]) == "EVAL" && string(args[1]) == leaseTransitionLua && string(args[5]) == s.op
	if lose {
		s.op = ""
	}
	s.mu.Unlock()
	v, err := s.inner.Do(db, args...)
	if lose && err == nil && v.Err() == nil {
		return Value{}, errors.New("injected lost reply after committed lease transition")
	}
	return v, err
}

func directLeaseView(t *testing.T, r *leaseRig) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: r.tp.Proxy.cfg.Upstream, DB: testDB, Protocol: 3})
	t.Cleanup(func() { c.Close() })
	return c
}

func leaseFlags(e *LeaseEngine, proj uint64) (exists, held, unresolved bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.leases[proj]
	if l == nil {
		return false, false, false
	}
	return true, l.held(), l.unresolved
}

func TestLeaseAtomicTransitionCoherentGrantRenewEnd(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	mac := r.conn(t, "mac1")
	db := directLeaseView(t, r)
	ctx := context.Background()
	grant := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
	if grant.Type != '*' || string(grant.Elems[0].Str) != "GRANTED" || grant.Elems[1].Int != 1 {
		t.Fatalf("grant: %+v", grant)
	}
	renew := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "checkout", "3600")
	if renew.Type != '*' || string(renew.Elems[0].Str) != "RENEWED" || renew.Elems[1].Int != 1 {
		t.Fatalf("renew: %+v", renew)
	}
	if got := db.HGet(ctx, leaseKey(3), "mode").Val(); got != LeaseCheckout {
		t.Fatalf("renew mode = %q", got)
	}
	ended := mac.do("JM.LEASE", "RELEASE", "ino:3", "1")
	if ended.Type != '*' || string(ended.Elems[0].Str) != "RELEASED" || ended.Elems[1].Int != 2 {
		t.Fatalf("end: %+v", ended)
	}
	if c, h := db.Get(ctx, epochKey(3)).Val(), db.HGet(ctx, leaseKey(3), "epoch").Val(); c != "2" || h != c {
		t.Fatalf("counter/hash after end = %q/%q", c, h)
	}
	_, ramHeld, _ := leaseFlags(r.eng, 3)
	if holder := db.HGet(ctx, leaseKey(3), "holder").Val(); holder != "" || ramHeld {
		t.Fatalf("ended holder redis=%q RAM held=%v", holder, ramHeld)
	}
}

func TestLeaseAtomicTransitionGrantsAfterOrphanCounter(t *testing.T) {
	r := startLeaseRig(t, ModeEnforce)
	db := directLeaseView(t, r)
	ctx := context.Background()
	if err := db.Set(ctx, epochKey(3), "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	v := r.conn(t, "mac1").do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
	if v.Type != '*' || v.Elems[1].Int != 2 {
		t.Fatalf("grant after orphan counter: %+v", v)
	}
	if c, h := db.Get(ctx, epochKey(3)).Val(), db.HGet(ctx, leaseKey(3), "epoch").Val(); c != "2" || h != c {
		t.Fatalf("counter/hash = %q/%q", c, h)
	}
}

func TestLeaseAtomicTransitionExactEpochAboveLuaPrecision(t *testing.T) {
	e, db := recoveryEngine(t)
	const before = "9007199254740992"
	putRecoveryLease(t, db, 3, before, before, "")
	if err := e.loadLeases(time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	mac := Principal{Name: "mac1", Kind: KindMac}
	f := e.fence(3)
	f.Lock()
	grant, _, err := e.grantLocked(mac, 3, LeaseAuto, 0, "large epoch test")
	f.Unlock()
	if err != nil || grant.Epoch != 9007199254740993 {
		t.Fatalf("large grant: %+v %v", grant, err)
	}
	if c, h := db.Get(context.Background(), epochKey(3)).Val(), db.HGet(context.Background(), leaseKey(3), "epoch").Val(); c != "9007199254740993" || h != c {
		t.Fatalf("large counter/hash = %q/%q", c, h)
	}
	f.Lock()
	_, ep, err := e.endLocked(3, "release", "mac1", "large epoch test")
	f.Unlock()
	if err != nil || ep != 9007199254740994 {
		t.Fatalf("large end epoch=%d err=%v", ep, err)
	}
}

func TestLeaseAtomicTransitionLostReplyQuarantinesProject(t *testing.T) {
	for _, op := range []string{"grant", "renew", "end"} {
		t.Run(op, func(t *testing.T) {
			lost := &lostTransitionReply{}
			r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
				lost.inner = c.Side
				c.Side = lost
			})
			mac := r.conn(t, "mac1")
			if op != "grant" {
				v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
				if v.Type != '*' || v.Elems[1].Int != 1 {
					t.Fatalf("initial grant: %+v", v)
				}
			}
			lost.mu.Lock()
			lost.op = op
			lost.mu.Unlock()
			var v Value
			switch op {
			case "end":
				v = mac.do("JM.LEASE", "RELEASE", "ino:3", "1")
			default:
				v = mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
			}
			if v.Type != '-' {
				t.Fatalf("lost reply was accepted: %+v", v)
			}
			db := directLeaseView(t, r)
			ctx := context.Background()
			counter := db.Get(ctx, epochKey(3)).Val()
			if hashEpoch := db.HGet(ctx, leaseKey(3), "epoch").Val(); counter == "" || hashEpoch != counter {
				t.Fatalf("committed keys incoherent: %q/%q", counter, hashEpoch)
			}
			if exists, _, unresolved := leaseFlags(r.eng, 3); !exists || !unresolved {
				t.Fatalf("project not quarantined: exists=%v unresolved=%v", exists, unresolved)
			}
			mustDeny(t, mac.do("SET", "i10", mkAttr(typeFile, 3)), ReasonLeaseHeld, "same-project write after lost reply")
			mustDeny(t, mac.do("HINCRBY", "dirUsedSpace", "3", "1"), ReasonLeaseHeld, "Mac dir-stat after lost reply")
			mustDeny(t, r.conn(t, "admin1").do("HINCRBY", "dirUsedSpace", "3", "1"), ReasonLeaseHeld, "admin dir-stat after lost reply")
			mustDeny(t, r.conn(t, "bg1").do("HINCRBY", "dirUsedSpace", "3", "1"), ReasonLeaseHeld, "BG dir-stat after lost reply")
			if got := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); got.Type != '-' {
				t.Fatalf("ACQUIRE after lost reply: %+v", got)
			}
			if got := mac.do("JM.LEASE", "STATUS", "ino:3"); got.Type != '-' {
				t.Fatalf("STATUS exposed unresolved holder: %+v", got)
			}
			if got := mac.do("JM.LEASE", "HEARTBEAT", "ino:3"); got.Type != '*' || len(got.Elems) != 1 || string(got.Elems[0].Elems[2].Str) != "lost" {
				t.Fatalf("HEARTBEAT exposed unresolved holder: %+v", got)
			}
			other := r.conn(t, "mac2").do("JM.LEASE", "ACQUIRE", "ino:4", "auto")
			if other.Type != '*' || string(other.Elems[0].Str) != "GRANTED" {
				t.Fatalf("unrelated project blocked: %+v", other)
			}
			recovered := NewLeaseEngine(LeaseConfig{Side: r.tp.Side(), VolumeDB: testDB, Logf: t.Logf})
			if err := recovered.loadLeases(time.Now().UnixMilli()); err != nil {
				t.Fatalf("coherent committed state did not recover: %v", err)
			}
			if got := recovered.leases[3]; got == nil || got.unresolved || got.epoch == 0 {
				t.Fatalf("bad recovered state: %+v", got)
			}
		})
	}
}

func TestLeaseAtomicTransitionRejectsPrewriteInvalidState(t *testing.T) {
	for _, tc := range []struct{ name, key, value string }{
		{"bad_counter", "counter", "bad"},
		{"overflow_counter", "counter", "9223372036854775807"},
		{"wrong_hash_type", "hash", "wrong"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := startLeaseRig(t, ModeEnforce)
			db := directLeaseView(t, r)
			ctx := context.Background()
			key := epochKey(3)
			if tc.key == "hash" {
				key = leaseKey(3)
			}
			if err := db.Set(ctx, key, tc.value, 0).Err(); err != nil {
				t.Fatal(err)
			}
			v := r.conn(t, "mac1").do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
			if v.Type != '-' {
				t.Fatalf("invalid state granted: %+v", v)
			}
			if got := db.Get(ctx, key).Val(); got != tc.value {
				t.Fatalf("prewrite rejection changed key: %q", got)
			}
			if exists, held, unresolved := leaseFlags(r.eng, 3); !exists || !unresolved || held {
				t.Fatalf("prewrite error left usable RAM: exists=%v held=%v unresolved=%v", exists, held, unresolved)
			}
		})
	}
}

func TestLeaseAtomicTransitionRejectsStaleExpectedState(t *testing.T) {
	t.Run("unexpected_free_hash", func(t *testing.T) {
		r := startLeaseRig(t, ModeEnforce)
		db := directLeaseView(t, r)
		putRecoveryLease(t, db, 3, "1", "1", "")
		v := r.conn(t, "mac1").do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
		if v.Type != '-' || db.Get(context.Background(), epochKey(3)).Val() != "1" {
			t.Fatalf("stale free expectation changed Redis: %+v", v)
		}
	})
	t.Run("changed_holder", func(t *testing.T) {
		r := startLeaseRig(t, ModeEnforce)
		mac := r.conn(t, "mac1")
		if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' {
			t.Fatalf("grant: %+v", v)
		}
		db := directLeaseView(t, r)
		ctx := context.Background()
		if err := db.HSet(ctx, leaseKey(3), "holder", "mac2").Err(); err != nil {
			t.Fatal(err)
		}
		v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto")
		if v.Type != '-' || db.HGet(ctx, leaseKey(3), "holder").Val() != "mac2" || db.Get(ctx, epochKey(3)).Val() != "1" {
			t.Fatalf("stale holder expectation changed Redis: %+v", v)
		}
	})
}

func TestLeaseAtomicTransitionUnknownPlacementKeepsKnownBypass(t *testing.T) {
	for _, principal := range []string{"admin1", "bg1"} {
		t.Run(principal, func(t *testing.T) {
			r := startLeaseRig(t, ModeEnforce)
			rc := r.conn(t, principal)
			mustDeny(t, rc.do("HSET", "d999", "unknown", mkEntry(typeFile, 10)), ReasonUnknownKey,
				principal+" unknown directory")
			mustOK(t, rc.do("HINCRBY", "dirUsedSpace", "3", "0"), principal+" known project dir-stat")
			mustOK(t, rc.do("HSET", "d"+inoStr(TrashInode), "2026100818", mkEntry(typeDir, TrashInode+1)),
				principal+" known trash directory")
		})
	}
}

type failAbsentInodeRead struct{ inner sideDoer }

func (s failAbsentInodeRead) Do(db int, args ...[]byte) (Value, error) {
	if len(args) == 2 && string(args[0]) == "GET" && string(args[1]) == "i99" {
		return Value{}, errors.New("injected placement read failure")
	}
	return s.inner.Do(db, args...)
}

func TestLeaseAtomicTransitionReplacedInodeReadFailure(t *testing.T) {
	for _, principal := range []string{"admin1", "bg1"} {
		t.Run(principal, func(t *testing.T) {
			r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
				c.Side = failAbsentInodeRead{inner: c.Side}
			})
			db := directLeaseView(t, r)
			ctx := context.Background()
			oldEntry := mkEntry(typeFile, 99)
			if err := db.Set(ctx, "i99", mkAttr(typeFile, 3), 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := db.HSet(ctx, "d3", "target", oldEntry).Err(); err != nil {
				t.Fatal(err)
			}
			mustDeny(t, r.conn(t, principal).do("HSET", "d3", "target", mkEntry(typeFile, 10)),
				ReasonUnknownKey, "failed replaced-inode read is not confirmed absence")
			if got, err := db.HGet(ctx, "d3", "target").Result(); err != nil || got != oldEntry {
				t.Fatalf("failed replaced-inode read changed destination: %q err=%v", got, err)
			}
		})
	}
}

func TestLeaseAtomicTransitionAbsentChunkCleanupBoundary(t *testing.T) {
	seed := func(t *testing.T, r *leaseRig) *redis.Client {
		t.Helper()
		db := directLeaseView(t, r)
		ctx := context.Background()
		if err := db.RPush(ctx, "c99_0", mkSlice(777, 4096)).Err(); err != nil {
			t.Fatal(err)
		}
		if err := db.HSet(ctx, "sliceRef", "k777_4096", "1").Err(); err != nil {
			t.Fatal(err)
		}
		if err := db.ZAdd(ctx, "delfiles", redis.Z{Score: 0, Member: "99:4096"}).Err(); err != nil {
			t.Fatal(err)
		}
		return db
	}
	gcTxn := func(t *testing.T, rc *rawConn) Value {
		t.Helper()
		if v := rc.do("WATCH", "c99_0"); string(v.Str) != "OK" {
			t.Fatalf("WATCH: %+v", v)
		}
		return txn(rc, []string{"DEL", "c99_0"}, []string{"HINCRBY", "sliceRef", "k777_4096", "-1"})
	}
	for _, principal := range []string{"admin1", "bg1"} {
		t.Run("confirmed_absent_cleanup_"+principal, func(t *testing.T) {
			r := startLeaseRig(t, ModeEnforce)
			db := seed(t, r)
			v := gcTxn(t, r.conn(t, principal))
			if v.Type != '*' || len(v.Elems) != 2 {
				t.Fatalf("real GC-shape delete refused: %+v", v)
			}
			ctx := context.Background()
			if db.Exists(ctx, "c99_0").Val() != 0 || db.HGet(ctx, "sliceRef", "k777_4096").Val() != "0" {
				t.Fatal("GC delete/decrement not committed")
			}
			mustOK(t, r.conn(t, principal).do("ZREM", "delfiles", "99:4096"),
				principal+" removes completed GC tracking")
			if db.ZScore(ctx, "delfiles", "99:4096").Err() != redis.Nil {
				t.Fatal("completed GC tracking record remains")
			}
			if v := r.conn(t, principal).do("LPUSH", "c99_0", "bad"); v.Type != '-' {
				t.Fatalf("created a chunk at an absent inode: %+v", v)
			}
		})
	}
	t.Run("failed_placement_read", func(t *testing.T) {
		r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
			c.Side = failAbsentInodeRead{inner: c.Side}
		})
		db := seed(t, r)
		mustDeny(t, gcTxn(t, r.conn(t, "bg1")), ReasonUnknownKey,
			"BG cannot treat a failed read as confirmed absence")
		if db.Exists(context.Background(), "c99_0").Val() != 1 || db.HGet(context.Background(), "sliceRef", "k777_4096").Val() != "1" ||
			db.ZScore(context.Background(), "delfiles", "99:4096").Err() != nil {
			t.Fatal("failed placement read mutated GC source")
		}
	})
}

// A detached flush used to HSET last_* after a successor grant. The project
// fence now orders the flush before release and the next grant.
type pausedLeaseFlush struct {
	inner   sideDoer
	entered chan struct{}
	resume  chan struct{}
	mu      sync.Mutex
	active  bool
	once    sync.Once
}

type lostLeaseFlushReply struct {
	inner sideDoer
	mu    sync.Mutex
	armed bool
}

func (s *lostLeaseFlushReply) Do(db int, args ...[]byte) (Value, error) {
	s.mu.Lock()
	lose := s.armed && len(args) >= 2 && string(args[0]) == "EVAL" && string(args[1]) == leaseFlushLua
	if lose {
		s.armed = false
	}
	s.mu.Unlock()
	v, err := s.inner.Do(db, args...)
	if lose && err == nil && v.Err() == nil {
		return Value{}, errors.New("injected lost flush reply after commit")
	}
	return v, err
}

func TestLeaseAtomicTransitionLostFlushReplyQuarantines(t *testing.T) {
	lost := &lostLeaseFlushReply{}
	r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
		lost.inner = c.Side
		c.Side = lost
		c.FlushEvery = time.Hour
	})
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' {
		t.Fatalf("grant: %+v", v)
	}
	r.eng.mu.Lock()
	r.eng.leases[3].dirty = true
	r.eng.mu.Unlock()
	lost.mu.Lock()
	lost.armed = true
	lost.mu.Unlock()
	r.eng.flushDirty()
	r.eng.mu.Lock()
	unresolved := r.eng.leases[3].unresolved
	r.eng.mu.Unlock()
	if !unresolved {
		t.Fatal("ambiguous flush left old holder usable")
	}
	mustDeny(t, mac.do("SET", "i10", mkAttr(typeFile, 3)), ReasonLeaseHeld, "project write after lost flush reply")
	if v := mac.do("JM.LEASE", "HEARTBEAT", "ino:3"); v.Type != '*' || len(v.Elems) != 1 || string(v.Elems[0].Elems[2].Str) != "lost" {
		t.Fatalf("heartbeat after lost flush reply: %+v", v)
	}
	restarted := NewLeaseEngine(LeaseConfig{Side: r.tp.Side(), VolumeDB: testDB, Logf: t.Logf})
	if err := restarted.loadLeases(time.Now().UnixMilli()); err != nil || restarted.leases[3] == nil || restarted.leases[3].unresolved {
		t.Fatalf("coherent flush did not recover: %+v err=%v", restarted.leases[3], err)
	}
}

func (s *pausedLeaseFlush) Do(db int, args ...[]byte) (Value, error) {
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if active && len(args) >= 2 && string(args[0]) == "EVAL" && string(args[1]) == leaseFlushLua {
		s.once.Do(func() {
			close(s.entered)
			<-s.resume
		})
	}
	return s.inner.Do(db, args...)
}

func TestLeaseAtomicTransitionFlushOrdersBeforeNewTenure(t *testing.T) {
	pause := &pausedLeaseFlush{entered: make(chan struct{}), resume: make(chan struct{})}
	var unblock sync.Once
	r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
		pause.inner = c.Side
		c.Side = pause
		c.FlushEvery = time.Hour
	})
	t.Cleanup(func() { unblock.Do(func() { close(pause.resume) }) })
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' {
		t.Fatalf("grant: %+v", v)
	}
	r.eng.mu.Lock()
	r.eng.leases[3].lastHbMs = time.Now().UnixMilli()
	r.eng.leases[3].pending = true
	r.eng.leases[3].dirty = true
	r.eng.mu.Unlock()
	pause.mu.Lock()
	pause.active = true
	pause.mu.Unlock()
	flushed := make(chan struct{})
	go func() { r.eng.flushDirty(); close(flushed) }()
	select {
	case <-pause.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush never entered side call")
	}
	ended := make(chan error, 1)
	go func() {
		f := r.eng.fence(3)
		f.Lock()
		_, _, err := r.eng.endLocked(3, "release", "mac1", "test")
		f.Unlock()
		ended <- err
	}()
	select {
	case err := <-ended:
		t.Fatalf("end passed an in-flight flush: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock.Do(func() { close(pause.resume) })
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("flush stuck")
	}
	select {
	case err := <-ended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("end stuck")
	}
	r.clk.Add(time.Second)
	nextHB := r.clk.Now().UnixMilli()
	if v := r.conn(t, "mac2").do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' || string(v.Elems[0].Str) != "GRANTED" {
		t.Fatalf("next tenure: %+v", v)
	}
	db := directLeaseView(t, r)
	ctx := context.Background()
	if c, h := db.Get(ctx, epochKey(3)).Val(), db.HGet(ctx, leaseKey(3), "epoch").Val(); c != "3" || h != c {
		t.Fatalf("successor counter/hash = %q/%q", c, h)
	}
	if holder := db.HGet(ctx, leaseKey(3), "holder").Val(); !strings.EqualFold(holder, "mac2") {
		t.Fatalf("stale flush damaged successor holder: %q", holder)
	}
	if hb, pending := db.HGet(ctx, leaseKey(3), "last_hb_ms").Val(), db.HGet(ctx, leaseKey(3), "pending").Val(); hb != strconv.FormatInt(nextHB, 10) || pending != "0" {
		t.Fatalf("stale flush damaged successor liveness: hb=%q pending=%q", hb, pending)
	}
}

func TestLeaseAtomicTransitionFlushRetainsConcurrentHeartbeat(t *testing.T) {
	pause := &pausedLeaseFlush{entered: make(chan struct{}), resume: make(chan struct{})}
	var unblock sync.Once
	r := startLeaseRig(t, ModeEnforce, func(c *LeaseConfig) {
		pause.inner = c.Side
		c.Side = pause
		c.FlushEvery = time.Hour
	})
	t.Cleanup(func() { unblock.Do(func() { close(pause.resume) }) })
	mac := r.conn(t, "mac1")
	if v := mac.do("JM.LEASE", "ACQUIRE", "ino:3", "auto"); v.Type != '*' {
		t.Fatalf("grant: %+v", v)
	}
	r.eng.mu.Lock()
	r.eng.leases[3].dirty = true
	r.eng.mu.Unlock()
	pause.mu.Lock()
	pause.active = true
	pause.mu.Unlock()
	done := make(chan struct{})
	go func() { r.eng.flushDirty(); close(done) }()
	select {
	case <-pause.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach Redis")
	}
	r.clk.Add(time.Second)
	newHB := r.clk.Now().UnixMilli()
	if v := mac.do("JM.LEASE", "HEARTBEAT", "ino:3"); v.Type != '*' || len(v.Elems) != 1 || string(v.Elems[0].Elems[2].Str) != "held" {
		t.Fatalf("heartbeat during flush: %+v", v)
	}
	unblock.Do(func() { close(pause.resume) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flush stuck")
	}
	r.eng.mu.Lock()
	dirty := r.eng.leases[3].dirty
	r.eng.mu.Unlock()
	if !dirty {
		t.Fatal("flush lost a concurrent heartbeat")
	}
	r.eng.flushDirty()
	db := directLeaseView(t, r)
	if got := db.HGet(context.Background(), leaseKey(3), "last_hb_ms").Val(); got != strconv.FormatInt(newHB, 10) {
		t.Fatalf("later flush persisted heartbeat %q, want %d", got, newHB)
	}
}
