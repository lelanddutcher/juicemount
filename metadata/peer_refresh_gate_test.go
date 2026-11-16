package metadata

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// gateLab is a spanLab whose test drives the gate's clock (spanLab.advance)
// and fires the armed re-reads (spanLab.timers) itself.
type gateLab struct {
	*spanLab
}

func newGateLab(t *testing.T, interval time.Duration) *gateLab {
	t.Helper()
	g := &gateLab{spanLab: newSpanLab(t)}
	g.gateClock = true
	g.rc.testPeerGateInterval = func() time.Duration { return interval }
	return g
}

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

// Review M1 of #102: the interactions with #99 and #100 run with the gate
// holding (10 s), not with it off.

// The Mac's own bytes are never held: one second after a teammate's change
// was published, the echo of the Mac's own in-place WRITE publishes at once
// with its true mtime, and no re-read is armed.
func TestOwnWriteIsNotHeld(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 8192, 2)
	g.push()
	g.advance(time.Second)
	mown := time.Unix(1700000101, 300000000)
	g.store.NoteOwnBytes("media.mov", 9000, mown.Add(-time.Millisecond), mown.Add(time.Second))
	g.set(mown, mown.Add(time.Second), 9000, 3)
	g.push()
	if row := g.store.LookupByPath("media.mov"); row.Size != 9000 || !g.reported().Equal(mown) || len(g.timers) != 0 {
		t.Fatalf("own WRITE echo: size %d, reported %v (want %v), re-reads armed %d (want published at once)",
			row.Size, g.reported(), mown, len(g.timers))
	}
}

// A folder reconcile while a late commit is held publishes nothing new (the
// same mtime and size) and must not consume the late commit: the re-read at
// the interval's end still publishes it and moves the reported mtime.
func TestFolderReconcileDuringHeldLateCommit(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	final := time.Unix(1700000100, 250)
	g.set(final, final.Add(10*time.Millisecond), 8192, 2)
	g.push()
	g.advance(time.Second)
	g.set(final, final.Add(time.Second), 8192, 3)
	g.push()
	if g.hooks() != 1 || len(g.timers) != 1 {
		t.Fatalf("late commit: hooks %d, re-reads armed %d (want held)", g.hooks(), len(g.timers))
	}
	g.advance(time.Second)
	g.reconcile()
	if !g.reported().Equal(final) {
		t.Fatalf("the folder reconcile moved the reported mtime to %v", g.reported())
	}
	g.timers[0].fire()
	g.advance(8 * time.Second)
	g.push()
	if g.hooks() < 2 || !g.reported().After(final) {
		t.Fatalf("re-read after the reconcile: hooks %d, reported %v (want above %v)", g.hooks(), g.reported(), final)
	}
}

// A growth is held, then the teammate shortens the file below the row's
// size: the shrink publishes at once, and the armed re-read later finds
// nothing new.
func TestGrowThenShrinkPublishesTheShrink(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 8192, 2)
	g.push()
	g.advance(time.Second)
	m2 := m1.Add(time.Second)
	g.set(m2, m2, 16384, 3)
	g.push()
	if g.store.LookupByPath("media.mov").Size != 8192 || len(g.timers) != 1 {
		t.Fatalf("growth: size %d, re-reads %d (want held)", g.store.LookupByPath("media.mov").Size, len(g.timers))
	}
	g.advance(time.Second)
	m3 := m2.Add(time.Second)
	g.set(m3, m3, 4096, 4)
	g.push()
	if g.hooks() != 2 || g.store.LookupByPath("media.mov").Size != 4096 || !g.reported().Equal(m3) {
		t.Fatalf("shrink: hooks %d, size %d, reported %v (want published at once)", g.hooks(),
			g.store.LookupByPath("media.mov").Size, g.reported())
	}
	g.timers[0].fire()
	g.advance(10 * time.Second)
	g.push()
	if g.hooks() != 2 {
		t.Fatalf("the re-read after the shrink ran the hooks again: %d", g.hooks())
	}
}

// A restart while a late commit is held loses the gate and the observations
// (known gap, review M2 of #102: without another event nothing publishes
// it). The next event after the restart publishes it as a late commit and
// moves the reported mtime.
func TestRestartWhileHeld(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	final := time.Unix(1700000100, 250)
	g.set(final, final.Add(10*time.Millisecond), 8192, 2)
	g.push()
	g.advance(time.Second)
	g.set(final, final.Add(time.Second), 8192, 3)
	g.push()
	if g.hooks() != 1 || len(g.timers) != 1 {
		t.Fatalf("late commit: hooks %d, re-reads armed %d (want held)", g.hooks(), len(g.timers))
	}
	g.store.Close()
	g.open() // the Manager restarts; the armed re-read died with the old client
	g.rc.testPeerGateInterval = func() time.Duration { return 10 * time.Second }
	g.advance(2 * time.Second)
	g.push() // one more i-key event (a later part, a chmod, the backstop)
	if g.hooks() != 1 || !g.reported().After(final) {
		t.Fatalf("after the restart: hooks %d, reported %v (want a move above %v)", g.hooks(), g.reported(), final)
	}
}

// The Mac starts writing the file (a foreground owner, #100) just as the
// held change's re-read fires: the re-read is deferred, nothing is lost, and
// the next re-read after the release publishes the teammate's change.
func TestReReadWhileTheMacWritesIsDeferred(t *testing.T) {
	g := newGateLab(t, 10*time.Second)
	var owned bool
	g.store.SetForegroundOwner(func(p string) bool { return p == "media.mov" && owned })
	m1 := time.Unix(1700000100, 0)
	g.set(m1, m1, 8192, 2)
	g.push()
	g.advance(time.Second)
	m2 := m1.Add(time.Second)
	g.set(m2, m2, 12288, 3)
	g.push()
	if len(g.timers) != 1 {
		t.Fatalf("re-reads armed %d, want 1", len(g.timers))
	}
	g.timers[0].fire()
	g.advance(9 * time.Second)
	owned = true
	if d := g.pushOwned(); len(d) != 1 || g.hooks() != 1 {
		t.Fatalf("re-read while the Mac writes: deferred %v, hooks %d (want deferred, nothing published)", d, g.hooks())
	}
	owned = false
	g.advance(time.Second)
	if d := g.pushOwned(); len(d) != 0 || g.hooks() != 2 || !g.reported().Equal(m2) ||
		g.store.LookupByPath("media.mov").Size != 12288 {
		t.Fatalf("after the release: deferred %v, hooks %d, reported %v (want %v), size %d", d, g.hooks(),
			g.reported(), m2, g.store.LookupByPath("media.mov").Size)
	}
}

// Review M3 of #102: the interval follows the measured round trip, so a Mac
// at home over Tailscale (class capped at slow by the tunnel) gets 1 s.
func TestPeerIntervalFollowsRTT(t *testing.T) {
	ms := time.Millisecond
	for _, c := range []struct {
		name   string
		env    string
		snap   netprofile.Snapshot
		want   time.Duration
		source string
	}{
		{"home over tailscale", "", netprofile.Snapshot{HaveRTT: true, RTT: 4 * ms, Class: netprofile.ClassSlow}, time.Second, "rtt"},
		{"30 ms", "", netprofile.Snapshot{HaveRTT: true, RTT: 30 * ms, Class: netprofile.ClassSlow}, time.Second, "rtt"},
		{"regional", "", netprofile.Snapshot{HaveRTT: true, RTT: 60 * ms, Class: netprofile.ClassMedium}, 3 * time.Second, "rtt"},
		{"abroad", "", netprofile.Snapshot{HaveRTT: true, RTT: 200 * ms, Class: netprofile.ClassFast}, 10 * time.Second, "rtt"},
		{"no sample, slow", "", netprofile.Snapshot{Class: netprofile.ClassSlow}, 10 * time.Second, "class"},
		{"no sample, metered", "", netprofile.Snapshot{Class: netprofile.ClassMetered}, 10 * time.Second, "class"},
		{"no sample, medium", "", netprofile.Snapshot{Class: netprofile.ClassMedium}, 3 * time.Second, "class"},
		{"no sample, fast", "", netprofile.Snapshot{Class: netprofile.ClassFast}, time.Second, "class"},
		{"env", "4s", netprofile.Snapshot{HaveRTT: true, RTT: 200 * ms}, 4 * time.Second, "env"},
		{"env off", "0", netprofile.Snapshot{HaveRTT: true, RTT: 200 * ms}, 0, "env"},
		{"env capped", "5m", netprofile.Snapshot{}, peerRefreshIntervalMax, "env"},
		{"env junk", "junk", netprofile.Snapshot{HaveRTT: true, RTT: 4 * ms}, time.Second, "rtt"},
	} {
		in := peerIntervalFor(c.env, c.snap)
		if in.interval != c.want || in.source != c.source {
			t.Errorf("%s: %v from %s, want %v from %s", c.name, in.interval, in.source, c.want, c.source)
		}
	}
}

// Review L3 of #102: past peerGatePruneAt recent inodes, the map is walked at
// most once per second.
func TestPeerGatePrunesAtMostOncePerSecond(t *testing.T) {
	var g peerRefreshGate
	t0 := time.Unix(1700000000, 0)
	for i := uint64(1); i <= peerGatePruneAt; i++ {
		g.published(i, t0)
	}
	late := t0.Add(peerRefreshIntervalMax + time.Second)
	g.published(1<<40, late) // walks and prunes
	for i := uint64(1); i <= peerGatePruneAt; i++ {
		g.published(i, t0) // stale stamps again, within the same second
	}
	g.published(1<<41, late.Add(500*time.Millisecond))
	if n := len(g.last); n != peerGatePruneAt+2 {
		t.Fatalf("%d inodes after a second burst within 1 s, want %d (no walk)", n, peerGatePruneAt+2)
	}
	g.published(1<<42, late.Add(time.Second))
	if n := len(g.last); n != 3 {
		t.Fatalf("%d inodes after the next walk, want 3", n)
	}
}
