package metadata

import (
	"testing"
	"time"
)

// The other attr push tests publish changes back to back and expect each
// one: they run with the gate off. The gate's own tests turn it on.
func init() { peerRefreshIntervalFn = func() time.Duration { return 0 } }

// gateLab is a spanLab with the gate on, a fake clock and captured timers.
type gateLab struct {
	*spanLab
	now    time.Time
	timers []gateTimer
}

type gateTimer struct {
	wait time.Duration
	fire func()
}

func newGateLab(t *testing.T, interval time.Duration) *gateLab {
	t.Helper()
	g := &gateLab{spanLab: newSpanLab(t), now: time.Unix(1700005000, 0)}
	prevI, prevN, prevA := peerRefreshIntervalFn, peerGateNow, peerGateAfter
	peerRefreshIntervalFn = func() time.Duration { return interval }
	peerGateNow = func() time.Time { return g.now }
	peerGateAfter = func(d time.Duration, f func()) { g.timers = append(g.timers, gateTimer{d, f}) }
	t.Cleanup(func() { peerRefreshIntervalFn, peerGateNow, peerGateAfter = prevI, prevN, prevA })
	return g
}

func (g *gateLab) advance(d time.Duration) { g.now = g.now.Add(d) }

// queued reports whether inode waits in the client's deferral queue.
func (g *gateLab) queued(inode uint64) bool {
	q := &g.rc.ownedDeferrals
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.files[inode]
	return ok
}

// A teammate's save that commits a part every 2 s: the first part shows at
// once, the parts inside the interval are held (no hook, no row change, no
// new reported mtime), and one re-read at the interval's end publishes all
// of them as one change, with an mtime the Mac has never seen.
func TestPeerSaveChangesAreCoalescedPerInterval(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 8192, 2)
	g.push()
	if g.hooks() != 1 || !g.reported().Equal(m1) {
		t.Fatalf("first part: hooks %d, reported %v (want at once)", g.hooks(), g.reported())
	}
	for k := 1; k <= 4; k++ { // parts at +2, +4, +6, +8 s
		g.advance(2 * time.Second)
		mk := m1.Add(time.Duration(k) * 2 * time.Second)
		g.set(mk, mk, 8192+uint64(k)*4096, int64(2+k))
		g.push()
		if g.hooks() != 1 || !g.reported().Equal(m1) || g.store.LookupByPath("media.mov").Size != 8192 {
			t.Fatalf("part %d inside the interval: hooks %d, reported %v, size %d (want held)",
				k, g.hooks(), g.reported(), g.store.LookupByPath("media.mov").Size)
		}
	}
	// One re-read armed, not one per part, due at the interval's end.
	if len(g.timers) != 1 {
		t.Fatalf("%d re-reads armed, want 1", len(g.timers))
	}
	if want := 8*time.Second - attrRetryBase; g.timers[0].wait != want {
		t.Fatalf("re-read armed after %v, want %v (10 s after the first part, less the queue's own delay)", g.timers[0].wait, want)
	}
	g.timers[0].fire()
	if !g.queued(42) {
		t.Fatal("the held change was not queued for its re-read")
	}
	// The re-read at the interval's end publishes the latest state once.
	g.advance(2 * time.Second)
	last := m1.Add(8 * time.Second)
	g.push()
	if g.hooks() != 2 || !g.reported().Equal(last) || g.store.LookupByPath("media.mov").Size != 8192+4*4096 {
		t.Fatalf("interval end: hooks %d, reported %v (want %v), size %d", g.hooks(), g.reported(), last,
			g.store.LookupByPath("media.mov").Size)
	}
	// Quiet: another event with nothing new changes nothing.
	g.advance(20 * time.Second)
	g.push()
	if g.hooks() != 2 {
		t.Fatalf("quiet re-read ran the hooks again: %d", g.hooks())
	}
	// The next save long after the interval shows at once.
	next := time.Unix(1700000300, 0)
	g.set(next, next, 8192, 9)
	g.push()
	if g.hooks() != 3 || !g.reported().Equal(next) {
		t.Fatalf("next save: hooks %d reported %v", g.hooks(), g.reported())
	}
}

// A save dated before its close (the mtime-span case): every part after the
// first commits under the final mtime and changes only the ctime. The held
// late commits are still late commits at the re-read (the observation the
// mirror last published is unchanged), so one refresh moves the reported
// mtime past every value reported before: the Mac drops the pages it read
// under the final mtime (P2 stays 0, the final bytes show).
func TestHeldLateCommitsPublishAsOneMove(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	final := time.Unix(1700000100, 250)
	g.set(final, final.Add(10*time.Millisecond), 8192, 2)
	g.push()
	if g.hooks() != 1 || !g.reported().Equal(final) {
		t.Fatalf("first commit: hooks %d reported %v", g.hooks(), g.reported())
	}
	for k := 1; k <= 3; k++ {
		g.advance(time.Second)
		g.set(final, final.Add(time.Duration(k)*time.Second), 8192, int64(2+k))
		g.push()
	}
	if g.hooks() != 1 || !g.reported().Equal(final) {
		t.Fatalf("late commits inside the interval: hooks %d reported %v (want held)", g.hooks(), g.reported())
	}
	g.advance(7 * time.Second)
	g.push()
	got := g.reported()
	if g.hooks() != 2 || !got.After(final) || !g.trueMtime().Equal(final) {
		t.Fatalf("interval end: hooks %d, reported %v (want above %v), true %v", g.hooks(), got, final, g.trueMtime())
	}
	if late := attrPushLateCommits.Load(); late == 0 {
		t.Fatal("the held commits were not counted as a late commit")
	}
	// No more commits: the reported value stays.
	g.advance(30 * time.Second)
	g.push()
	if g.hooks() != 2 || !g.reported().Equal(got) {
		t.Fatalf("settled: hooks %d reported %v (want %v)", g.hooks(), g.reported(), got)
	}
}

// A re-read that a shared deferral timer fires before the interval ends is
// held again and armed again: the held change is never dropped.
func TestEarlyReReadIsHeldAndArmedAgain(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 8192, 2)
	g.push()
	g.advance(time.Second)
	m2 := m1.Add(time.Second)
	g.set(m2, m2, 8192, 3)
	g.push()
	if len(g.timers) != 1 {
		t.Fatalf("%d re-reads armed, want 1", len(g.timers))
	}
	g.timers[0].fire()
	g.advance(5 * time.Second) // the queue fired early
	g.push()
	if g.hooks() != 1 || len(g.timers) != 2 {
		t.Fatalf("early re-read: hooks %d, re-reads armed %d (want held and armed again)", g.hooks(), len(g.timers))
	}
	if want := 4*time.Second - attrRetryBase; g.timers[1].wait != want {
		t.Fatalf("second re-read after %v, want %v", g.timers[1].wait, want)
	}
	g.timers[1].fire()
	g.advance(4 * time.Second)
	g.push()
	if g.hooks() != 2 || !g.reported().Equal(m2) {
		t.Fatalf("interval end: hooks %d reported %v (want %v)", g.hooks(), g.reported(), m2)
	}
	// Within the slack of the interval's end the change goes through.
	g.advance(10*time.Second - peerRefreshSlack)
	m3 := m2.Add(time.Minute)
	g.set(m3, m3, 8192, 4)
	g.push()
	if g.hooks() != 3 {
		t.Fatalf("a re-read inside the slack was held: hooks %d", g.hooks())
	}
}

// A teammate's change that shortens the file is never held: a row with the
// longer size would send READs past the new end (premature-EOF EIO).
func TestShorterFileIsNotHeld(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 16384, 2)
	g.push()
	g.advance(time.Second)
	m2 := m1.Add(time.Second)
	g.set(m2, m2, 4096, 3)
	g.push()
	if g.hooks() != 2 || g.store.LookupByPath("media.mov").Size != 4096 || len(g.timers) != 0 {
		t.Fatalf("shrink: hooks %d size %d timers %d (want published at once)", g.hooks(),
			g.store.LookupByPath("media.mov").Size, len(g.timers))
	}
	// A longer file inside the interval is still held.
	g.advance(time.Second)
	m3 := m2.Add(time.Second)
	g.set(m3, m3, 8192, 4)
	g.push()
	if g.hooks() != 2 || len(g.timers) != 1 {
		t.Fatalf("growth inside the interval: hooks %d timers %d (want held)", g.hooks(), len(g.timers))
	}
}

// With the interval 0 (JM_PEER_REFRESH_INTERVAL=0) every change publishes.
func TestPeerGateOff(t *testing.T) {
	g := newGateLab(t, 0)
	m := time.Unix(1700000100, 0)
	for k := 0; k < 3; k++ {
		mk := m.Add(time.Duration(k) * time.Second)
		g.set(mk, mk, 8192, int64(2+k))
		g.push()
		g.advance(100 * time.Millisecond)
	}
	if g.hooks() != 3 || len(g.timers) != 0 {
		t.Fatalf("gate off: hooks %d timers %d", g.hooks(), len(g.timers))
	}
}

func TestPeerRefreshIntervalEnv(t *testing.T) {
	for _, c := range []struct {
		env  string
		want time.Duration
	}{{"0", 0}, {"4s", 4 * time.Second}, {"5m", peerRefreshIntervalMax}} {
		t.Setenv("JM_PEER_REFRESH_INTERVAL", c.env)
		if got := peerRefreshInterval(); got != c.want {
			t.Fatalf("JM_PEER_REFRESH_INTERVAL=%s: %v, want %v", c.env, got, c.want)
		}
	}
	t.Setenv("JM_PEER_REFRESH_INTERVAL", "junk")
	if got := peerRefreshInterval(); got <= 0 || got > peerRefreshIntervalMax {
		t.Fatalf("an unparsable value must fall back to the class default, got %v", got)
	}
}

// The gate's map stays bounded.
func TestPeerGatePrunes(t *testing.T) {
	var g peerRefreshGate
	t0 := time.Unix(1700000000, 0)
	for i := uint64(1); i <= peerGatePruneAt; i++ {
		g.published(i, t0)
	}
	g.published(1<<40, t0.Add(peerRefreshIntervalMax+time.Second))
	if n := len(g.last); n != 1 {
		t.Fatalf("%d inodes kept, want 1", n)
	}
}
