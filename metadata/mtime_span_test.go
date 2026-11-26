package metadata

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

// jfsAttr is a juicefs i-key value, made by juicefs itself.
func jfsAttr(mtime, ctime time.Time, size uint64) string {
	return string((&meta.Attr{
		Typ: meta.TypeFile, Mode: 0o644, Nlink: 1, Length: size, Parent: 1,
		Mtime: mtime.Unix(), Mtimensec: uint32(mtime.Nanosecond()),
		Ctime: ctime.Unix(), Ctimensec: uint32(ctime.Nanosecond()),
	}).Marshal())
}

func TestDecodeInodeAttrCtimeMatchesJuiceFS(t *testing.T) {
	m, c := time.Unix(1700000000, 123), time.Unix(1700000007, 456789)
	raw := []byte(jfsAttr(m, c, 4096))
	gotM, size, ok := decodeInodeAttrTime(raw)
	gotC, cok := decodeInodeAttrCtime(raw)
	if !ok || !cok || !gotM.Equal(m) || !gotC.Equal(c) || size != 4096 {
		t.Fatalf("decoded mtime %v ctime %v size %d (ok %v %v)", gotM, gotC, size, ok, cok)
	}
}

// spanLab is a mirrored file "media.mov" (inode 42) and the attr push over a
// Redis whose i42 value and slice count the test sets.
type spanLab struct {
	t     *testing.T
	dir   string
	rc    *RedisClient
	store *Store
	fired *[]string
	gen   uint64
	mu    sync.Mutex
	attr  string
	sig   sliceSig
	// The refresh gate (peer_refresh_gate.go) is on, at labGateInterval.
	// Unless gateClock is set, every push first moves the clock past the
	// interval, so these tests see each teammate change published; the gate
	// tests drive the clock themselves and collect the armed re-reads.
	clock     time.Time
	gateClock bool
	timers    []gateTimer
}

const labGateInterval = 10 * time.Second

type gateTimer struct {
	wait time.Duration
	fire func()
}

func newSpanLab(t *testing.T) *spanLab {
	t.Helper()
	l := &spanLab{t: t, dir: t.TempDir(), clock: time.Unix(1700005000, 0), attr: jfsAttr(time.Unix(1700000000, 500), time.Unix(1700000000, 500), 4096), sig: sliceSig{1, 1}}
	l.open()
	if err := l.rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	*l.fired = nil
	return l
}

// open (re)opens the mirror and a fresh push, as a Manager restart does.
func (l *spanLab) open() {
	t := l.t
	t.Helper()
	store, err := Open(filepath.Join(l.dir, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	value := make([]byte, 9)
	value[0] = jfsTypeFile
	binary.BigEndian.PutUint32(value[5:], 42)
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			return map[string]string{"media.mov": string(value)}, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			l.mu.Lock()
			defer l.mu.Unlock()
			out := make([]interface{}, len(keys))
			for i := range keys {
				out[i] = l.attr
			}
			return out, nil
		},
		testReadSlices: func(uint64, int64) sliceSig {
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.sig
		},
	}
	t.Cleanup(func() { close(rc.stopCh) })
	rc.testPeerGateInterval = func() time.Duration { return labGateInterval }
	rc.testPeerGateNow = func() time.Time {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.clock
	}
	rc.testPeerGateAfter = func(d time.Duration, f func()) {
		l.mu.Lock()
		l.timers = append(l.timers, gateTimer{d, f})
		l.mu.Unlock()
	}
	fired := []string{}
	store.SetOnScanContentInvalidated(func(p string, _ uint64) { fired = append(fired, p) })
	l.rc, l.store, l.fired = rc, store, &fired
	l.gen, _, _ = rc.backendAdmission()
}

// set is what Redis holds now: the i-key and the slice count (each count
// stands for one tail slice).
func (l *spanLab) set(mtime, ctime time.Time, size uint64, slices int64) {
	l.setSig(mtime, ctime, size, sliceSig{slices, uint64(slices)})
}

func (l *spanLab) setSig(mtime, ctime time.Time, size uint64, sig sliceSig) {
	l.mu.Lock()
	l.attr, l.sig = jfsAttr(mtime, ctime, size), sig
	l.mu.Unlock()
}

func (l *spanLab) push() {
	l.t.Helper()
	if !l.gateClock {
		l.advance(labGateInterval + time.Second)
	}
	if retry, err := l.rc.reconcileFileAttrs([]uint64{42}, l.gen); err != nil || retry != nil {
		l.t.Fatalf("push: retry %v err %v", retry, err)
	}
}

// advance moves the gate's clock.
func (l *spanLab) advance(d time.Duration) {
	l.mu.Lock()
	l.clock = l.clock.Add(d)
	l.mu.Unlock()
}

func (l *spanLab) reconcile() {
	l.t.Helper()
	if err := l.rc.reconcileDir(1); err != nil {
		l.t.Fatal(err)
	}
}

func (l *spanLab) hooks() int { return len(*l.fired) }

func (l *spanLab) reported() time.Time {
	l.t.Helper()
	e := l.store.LookupByPath("media.mov")
	if e == nil {
		l.t.Fatal("no row")
	}
	return e.FileInfo().ModTime()
}

func (l *spanLab) trueMtime() time.Time {
	l.t.Helper()
	return l.store.LookupByPath("media.mov").Mtime
}

// A teammate's save stamps its final mtime at its first commit and commits
// the other parts later; those change only the ctime (and add slices). The
// Mac keeps pages until it sees another mtime, so after every late commit the
// NFS mtime must move to a value not reported before, and the content hooks
// must run. The true mtime stays in the row, and the shown second too.
func TestAttrPushLateCommitMovesTheNFSMtime(t *testing.T) {
	l := newSpanLab(t)
	final := time.Unix(1700000100, 250)
	l.set(final, final.Add(10*time.Millisecond), 8192, 2)
	l.push()
	if got := l.reported(); !got.Equal(final) || l.hooks() != 1 {
		t.Fatalf("first commit: NFS mtime %v (want %v), hooks %d", got, final, l.hooks())
	}
	var seen []time.Time
	for k := 1; k <= 3; k++ {
		l.set(final, final.Add(time.Duration(k)*time.Second), 8192, int64(2+k))
		l.push()
		got := l.reported()
		if !got.Equal(final.Add(time.Duration(k))) || l.hooks() != 1+k {
			t.Fatalf("late commit %d: NFS mtime %v, hooks %d", k, got, l.hooks())
		}
		if !l.trueMtime().Equal(final) {
			t.Fatalf("late commit %d: row mtime %v; the true mtime must stay", k, l.trueMtime())
		}
		if got.Unix() != final.Unix() {
			t.Fatalf("the shown second moved: %v", got)
		}
		seen = append(seen, got)
	}
	// The same observation again (another event, no commit): nothing moves.
	l.push()
	if got := l.reported(); !got.Equal(seen[2]) || l.hooks() != 4 {
		t.Fatalf("repeat: NFS mtime %v hooks %d", got, l.hooks())
	}
	// A folder reconcile republishes the row: the reported mtime stays.
	l.reconcile()
	if got := l.reported(); !got.Equal(seen[2]) {
		t.Fatalf("after a folder reconcile the NFS mtime went back to %v", got)
	}
	// The next save: a new mtime above everything reported, as it is.
	next := time.Unix(1700000200, 0)
	l.set(next, next, 8192, 6)
	l.push()
	if got := l.reported(); !got.Equal(next) {
		t.Fatalf("next save: NFS mtime %v, want %v", got, next)
	}
}

// Review H1: the folder reconcile publishes the save's first commit (a cp -p
// into a folder changes the folder too), so the push's first look at the
// mtime is already a late commit. It must still move the mtime. The same for
// a SCAN, which reads no ctime at all.
func TestLateCommitAfterAFolderReconcilePublishedTheMtime(t *testing.T) {
	for _, by := range []string{"folder reconcile", "scan"} {
		t.Run(by, func(t *testing.T) {
			l := newSpanLab(t)
			m := time.Unix(1700000100, 25)
			l.set(m, m.Add(time.Second), 8192, 2) // first commit (M, C1)
			if by == "scan" {
				epoch, err := l.store.CaptureStoreEpoch()
				if err != nil {
					t.Fatal(err)
				}
				e := MakeEntry("media.mov", false, 8192, m, 42)
				e.MtimeNsecKnown, e.Mode = true, l.store.LookupByPath("media.mov").Mode
				if _, err := l.store.ScanUpsertBatch([]*Entry{e}, epoch, []string{"media.mov"}); err != nil {
					t.Fatal(err)
				}
			} else {
				l.reconcile()
			}
			if got := l.reported(); !got.Equal(m) {
				t.Fatalf("%s published %v, want %v", by, got, m)
			}
			before := l.hooks()
			l.set(m, m.Add(2*time.Second), 8192, 3) // another part commits (M, C2)
			l.push()
			if got := l.reported(); got.Equal(m) || l.hooks() == before {
				t.Fatalf("late commit after a %s: NFS mtime %v (true %v), hooks %d -> %d", by, got, m, before, l.hooks())
			}
		})
	}
}

// Review H2: past the observation cap, the active file's observation stays
// (eviction is by age, never all at once), so its next late commit moves the
// mtime.
func TestLateCommitSurvivesObservationOverflow(t *testing.T) {
	l := newSpanLab(t)
	clock := time.Unix(1700001000, 0)
	l.store.attrObs.now = func() time.Time { return clock }
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	// A big copy touches more files than the cap; the save goes on.
	for i := uint64(0); i < attrObsMax+10; i++ {
		clock = clock.Add(time.Millisecond)
		if i%5000 == 0 { // the save's own events keep its observation fresh
			l.push()
		}
		l.store.attrObs.put(1000+i, attrObs{mtimeNS: int64(i), sig: sliceSig{1, 1}}, false)
	}
	if n := len(l.store.attrObs.obs); n > attrObsMax {
		t.Fatalf("%d observations, cap %d", n, attrObsMax)
	}
	before := l.hooks()
	l.set(m, m.Add(2*time.Second), 8192, 3)
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("after the overflow: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// Review M1: a ctime-only change is not a late commit. A teammate's chmod,
// rename or link (the same slices) and the Mac's own chmod leave the mtime
// and run no hooks; a commit (one more slice) still counts.
func TestCtimeOnlyChangeIsNotALateCommit(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	before := l.hooks()
	l.set(m, m.Add(2*time.Second), 8192, 2) // a teammate's chmod
	l.push()
	if got := l.reported(); !got.Equal(m) || l.hooks() != before {
		t.Fatalf("teammate chmod: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
	l.set(m, m.Add(3*time.Second), 8192, 3) // a late commit
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("late commit: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// Second review H1 (probe P1): the Mac's own chmod mark must not hide a
// teammate's commit that follows it within the window. The mark is used only
// when the slice signature cannot decide, and only once.
func TestOwnChmodMarkDoesNotHideALateCommit(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	before := l.hooks()
	l.store.NoteOwnAttrChange("media.mov")
	l.set(m, m.Add(2*time.Second), 8192, 2) // the chmod's echo
	l.push()
	if got := l.reported(); !got.Equal(m) || l.hooks() != before {
		t.Fatalf("own chmod: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
	l.set(m, m.Add(3*time.Second), 8192, 3) // 5 s later, the teammate's last part
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("commit inside the own-chmod window: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}

	// No signature (a huge file): the mark covers its one echo only.
	l.store.Close() // its values are process-wide (inode 42)
	l2 := newSpanLab(t)
	l2.setSig(m, m.Add(time.Second), 8192, unknownSig)
	l2.push()
	before = l2.hooks()
	l2.store.NoteOwnAttrChange("media.mov")
	l2.setSig(m, m.Add(2*time.Second), 8192, unknownSig)
	l2.push()
	if got := l2.reported(); !got.Equal(m) || l2.hooks() != before {
		t.Fatalf("own chmod, no signature: NFS mtime %v hooks %d -> %d", got, before, l2.hooks())
	}
	l2.setSig(m, m.Add(3*time.Second), 8192, unknownSig)
	l2.push()
	if got := l2.reported(); got.Equal(m) || l2.hooks() != before+1 {
		t.Fatalf("the mark was not consumed: NFS mtime %v hooks %d -> %d", got, before, l2.hooks())
	}
}

// Third review N1: an app's save through a temp file renamed over the
// original publishes the stage's inode with no observation. The drainer marks
// the stage inode before the rename, so the echo (same mtime and size, the
// rename's ctime) keeps the mtime the Mac wrote and runs no hooks.
func TestOwnReplacementEchoKeepsTheMtime(t *testing.T) {
	l := newSpanLab(t)
	s := time.Unix(1700000300, 123456789)
	row := l.store.LookupByPath("media.mov")
	e := MakeEntry("media.mov", false, 9000, s, 42)
	e.Mode, e.MtimeNsecKnown = row.Mode, true
	l.store.NoteOwnInode(42)                  // replacement_publish.go, before backend.Rename
	if err := l.store.Insert(e); err != nil { // the settlement
		t.Fatal(err)
	}
	l.store.attrObs.mu.Lock()
	delete(l.store.attrObs.obs, 42) // the stage inode was never observed
	l.store.attrObs.mu.Unlock()
	before := l.hooks()
	l.setSig(s, s.Add(time.Second), 9000, unknownSig)
	l.push()
	if got := l.reported(); !got.Equal(s) || l.hooks() != before {
		t.Fatalf("own replacement echo: NFS mtime %v (want %v), hooks %d -> %d", got, s, before, l.hooks())
	}
}

// Third review N2 (probe Q2): after a restart nothing is recorded, so the
// Mac's own rename (marked before the FUSE rename) must not move the mtime.
func TestOwnRenameAfterARestartKeepsTheMtime(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	if err := l.store.Close(); err != nil {
		t.Fatal(err)
	}
	close(l.rc.stopCh)
	l.rc.stopCh = make(chan struct{})
	l.open()
	l.store.NoteOwnWrite("media.mov") // nfs rename, before os.Rename
	l.set(m, m.Add(2*time.Second), 8192, 2)
	l.push()
	if got := l.reported(); !got.Equal(m) || l.hooks() != 0 {
		t.Fatalf("own rename after a restart: NFS mtime %v hooks %d", got, l.hooks())
	}
}

// Third review N4 (probe Q1): an own mark whose echo the signature already
// decided is consumed, so a teammate's commit read without a signature
// within the window still counts.
func TestUsedOwnMarkDoesNotAbsorbALaterCommit(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	before := l.hooks()
	l.store.NoteOwnAttrChange("media.mov")
	l.set(m, m.Add(2*time.Second), 8192, 2) // the chmod's echo: same slices
	l.push()
	l.setSig(m, m.Add(3*time.Second), 8192, unknownSig) // a commit, no signature read
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("commit after a used mark: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// Third review N3: an ordinary change (above everything reported) writes no
// history row; a moved value does.
func TestOrdinaryChangeWritesNoHistory(t *testing.T) {
	l := newSpanLab(t)
	rows := func() int {
		var n int
		if err := l.store.db.QueryRow(`SELECT COUNT(*) FROM nfs_mtime_reported`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	l.set(m.Add(time.Second), m.Add(2*time.Second), 8192, 3)
	l.push()
	if n := rows(); n != 0 {
		t.Fatalf("ordinary changes wrote %d history rows", n)
	}
	l.set(m.Add(time.Second), m.Add(3*time.Second), 8192, 4) // a late commit
	l.push()
	if n := rows(); n == 0 {
		t.Fatal("a moved value wrote no history row")
	}
}

// Second review H2 (probe P2): a commit and a compaction can leave the slice
// count unchanged. The last slice of the committed chunk changed, so it is a
// late commit.
func TestCommitPlusCompactionIsALateCommit(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.setSig(m, m.Add(time.Second), 8192, sliceSig{6, 0xaaaa})
	l.push()
	before := l.hooks()
	l.setSig(m, m.Add(2*time.Second), 8192, sliceSig{6, 0xbbbb}) // +1 commit, -1 by compaction
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("commit + compaction: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// The signature hashes each chunk list's length and last slice.
func TestSliceSignatureSeesTheTail(t *testing.T) {
	mk := func(lens []int64, tails []string) sliceSig {
		var c sigCmds
		for i := range lens {
			l := redis.NewIntCmd(context.Background())
			l.SetVal(lens[i])
			tl := redis.NewStringCmd(context.Background())
			tl.SetVal(tails[i])
			c.lens, c.tails = append(c.lens, l), append(c.tails, tl)
		}
		return c.result()
	}
	a := mk([]int64{3, 3}, []string{"s1", "s2"})
	if a.count != 6 || !a.sameContent(mk([]int64{3, 3}, []string{"s1", "s2"})) {
		t.Fatalf("same lists differ: %+v", a)
	}
	if a.sameContent(mk([]int64{3, 3}, []string{"s1", "s9"})) {
		t.Fatal("another last slice, same counts: must differ")
	}
	if a.sameContent(mk([]int64{2, 4}, []string{"s1", "s2"})) {
		t.Fatal("other per-chunk lengths: must differ")
	}
	if (sigCmds{}).result().known() {
		t.Fatal("no reads must be unknown")
	}
}

// Second review M1 (probe P3): a row Insert publishes that this client did
// not write (the FUSE stat fill on a cache miss, a metadata event) is not
// taken for an own write: the next late commit still counts.
func TestStatFillIsNotAnOwnWrite(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	row := l.store.LookupByPath("media.mov")
	e := MakeEntry("media.mov", false, 8192, m, 42)
	e.Mode, e.MtimeNsecKnown = row.Mode, true
	if err := l.store.Insert(e); err != nil { // juiceFS.Stat fallback
		t.Fatal(err)
	}
	before := l.hooks()
	l.set(m, m.Add(2*time.Second), 8192, 3)
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("after a stat fill: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// Second review M3: ctimes come from different hosts' clocks. A commit whose
// ctime is below the recorded one (a slower clock) is still a late commit.
func TestCtimeFromASlowerClockStillCounts(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(10*time.Second), 8192, 2) // stamped by a clock 2 s ahead
	l.push()
	before := l.hooks()
	l.set(m, m.Add(9*time.Second), 8192, 3) // the teammate's commit, 1 s later
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("slower clock: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
}

// This client's own save comes back with an mtime the push has not seen: it
// is recorded only. The drain's chmod after it (a ctime-only change) is not a
// late commit either.
func TestAttrPushOwnSaveEchoKeepsTheMtime(t *testing.T) {
	l := newSpanLab(t)
	l.push()
	*l.fired = nil
	own := time.Unix(1700000300, 0) // the spool file's mtime, which the drain restores
	row := l.store.LookupByPath("media.mov")
	e := MakeEntry("media.mov", false, 9000, own, 42)
	e.Mode, e.MtimeNsecKnown = row.Mode, true
	if err := l.store.Insert(e); err != nil { // the drain publishes the row itself
		t.Fatal(err)
	}
	l.store.NoteOwnContent("media.mov", 9000, own) // as the in-place drain marks it
	l.set(own, own.Add(40*time.Second), 9000, 1)   // its echo (Chtimes)
	l.push()
	l.set(own, own.Add(41*time.Second), 9000, 1) // the drain's chmod
	l.push()
	if got := l.reported(); !got.Equal(own) || len(*l.fired) != 0 {
		t.Fatalf("own echo: NFS mtime %v (want %v), hooks %d", got, own, len(*l.fired))
	}
}

// Review M2: the reported mtime never returns to a value reported before.
// (a) juicefs steps the mtime back (M, M', M); (b) the Manager restarts; (c)
// the row leaves the RAM cache and is read back from SQL.
func TestReportedMtimeNeverRepeats(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	mPrime := m.Add(400 * time.Millisecond) // a slice written after the touch
	seen := map[int64]bool{}
	step := func(name string, mt time.Time, ctime time.Duration, slices int64) {
		t.Helper()
		l.set(mt, m.Add(ctime), 8192, slices)
		l.push()
		r := l.reported().UnixNano()
		if seen[r] {
			t.Fatalf("%s: reported %d again", name, r)
		}
		seen[r] = true
		if !l.trueMtime().Equal(mt) {
			t.Fatalf("%s: true mtime %v, want %v", name, l.trueMtime(), mt)
		}
	}
	step("first commit M", m, time.Second, 2)
	step("M' commits first", mPrime, 2*time.Second, 3)
	step("back to M", m, 3*time.Second, 4)
	step("late commit", m, 4*time.Second, 5)
	last := l.reported()

	// (b) restart: a new Store on the same file, a new push.
	if err := l.store.Close(); err != nil {
		t.Fatal(err)
	}
	close(l.rc.stopCh)
	l.rc.stopCh = make(chan struct{})
	l.open()
	if got := l.reported(); !got.Equal(last) {
		t.Fatalf("after a restart the NFS mtime is %v, was %v", got, last)
	}
	step("late commit after restart", m, 5*time.Second, 6)
	last = l.reported()

	// (c) the row as SQL builds it (an evicted row, JM_SERVE_FROM_SQLITE=1).
	fromSQL := l.store.lookupByPathSQLite("media.mov")
	if fromSQL == nil || fromSQL == l.store.LookupByPath("media.mov") {
		t.Fatalf("sql row: %v", fromSQL)
	}
	if got := fromSQL.FileInfo().ModTime(); !got.Equal(last) {
		t.Fatalf("the SQL row reports %v, the cache %v", got, last)
	}
}

// Second review M2 (probe P4): an old date X coming back after N (X more
// than an hour below) is reported as it is only the first time. The same for
// the date the file was first loaded with (no history kept for it then).
func TestOldDateComingBackIsNotReportedAgain(t *testing.T) {
	x, n := time.Unix(1690000000, 0), time.Unix(1700000200, 0)
	load := time.Unix(1700000000, 500) // the lab's first row
	for name, c := range map[string]struct {
		steps  []time.Time
		asItIs int // the first steps reported as they are
	}{
		"X, N, X":       {[]time.Time{x, n, x}, 2},
		"load, N, load": {[]time.Time{load.Add(2 * time.Hour), load}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			l := newSpanLab(t)
			seen := map[int64]bool{l.reported().UnixNano(): true}
			for k, mt := range c.steps {
				l.set(mt, time.Unix(1700020000+int64(k), 0), 8192, int64(2+k))
				l.push()
				r := l.reported().UnixNano()
				if k < c.asItIs && r != mt.UnixNano() {
					t.Fatalf("step %d: reported %d, want the true %d", k, r, mt.UnixNano())
				}
				if seen[r] {
					t.Fatalf("step %d: reported %d again", k, r)
				}
				seen[r] = true
			}
		})
	}
}

// Second review M4: values of inodes no longer mirrored are deleted at open;
// those of live inodes stay.
func TestReportedValuesOfDeletedInodesArePruned(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	l.set(m, m.Add(2*time.Second), 8192, 3)
	l.push()
	want := l.reported()
	if _, err := l.store.db.Exec(`INSERT INTO nfs_mtime_reported (inode, true_ns, reported_ns) VALUES (999, 1, 2)`); err != nil {
		t.Fatal(err)
	}
	if err := l.store.Close(); err != nil {
		t.Fatal(err)
	}
	close(l.rc.stopCh)
	l.rc.stopCh = make(chan struct{})
	l.open()
	var n int
	if err := l.store.db.QueryRow(`SELECT COUNT(*) FROM nfs_mtime_reported WHERE inode=999`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rows of a deleted inode: %d (%v)", n, err)
	}
	if got := l.reported(); !got.Equal(want) {
		t.Fatalf("live inode after reopen: %v, want %v", got, want)
	}
	if got := ReportedMtime(999, time.Unix(0, 1)); !got.Equal(time.Unix(0, 1)) {
		t.Fatalf("deleted inode still reports %v", got)
	}
}

// Review L1: the sidecar snapshot's attributes use the reported mtime.
func TestReportedMtimeForAnInodeAndTrueMtime(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	l.set(m, m.Add(2*time.Second), 8192, 3)
	l.push()
	if got := ReportedMtime(42, m); !got.Equal(l.reported()) || got.Equal(m) {
		t.Fatalf("ReportedMtime %v, row %v", got, l.reported())
	}
	if got := ReportedMtime(42, m.Add(time.Second)); !got.Equal(m.Add(time.Second)) {
		t.Fatalf("another true mtime: %v", got)
	}
}

// The push's one round trip against a real Redis (JM_TEST_REDIS_ADDR, e.g.
// a throwaway container): attrs, chunk lengths and last slices; a missing
// chunk list reads as empty.
func TestReadAttrsAndSigsAgainstRedis(t *testing.T) {
	addr := os.Getenv("JM_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("JM_TEST_REDIS_ADDR not set")
	}
	db := redis.NewClient(&redis.Options{Addr: addr})
	defer db.Close()
	ctx := context.Background()
	if err := db.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{}
	rc.rdb.Store(db)
	m := time.Unix(1700000100, 25)
	db.Set(ctx, "i42", jfsAttr(m, m, 70<<20), 0)
	db.RPush(ctx, "c42_0", "s1", "s2")
	db.RPush(ctx, "c42_1", "s3")
	read := func() ([]interface{}, sliceSig) {
		t.Helper()
		vals, sigs, err := rc.readAttrsAndSigs(ctx, []string{"i42", "i43"}, []sliceQuery{{42, 70 << 20}, {43, 0}})
		if err != nil || len(vals) != 2 || len(sigs) != 2 {
			t.Fatalf("read: %v %v %v", vals, sigs, err)
		}
		if vals[1] != nil || sigs[1] != (sliceSig{0, sigs[1].tail}) {
			t.Fatalf("missing file: %v %+v", vals[1], sigs[1])
		}
		return vals, sigs[0]
	}
	vals, a := read()
	if s, _ := vals[0].(string); s != jfsAttr(m, m, 70<<20) || a.count != 3 {
		t.Fatalf("attr %q sig %+v", vals[0], a)
	}
	// A compaction of chunk 0 (2 -> 1) plus a commit to chunk 1 (1 -> 2):
	// the same count, another signature.
	db.Del(ctx, "c42_0")
	db.RPush(ctx, "c42_0", "s12")
	db.RPush(ctx, "c42_1", "s4")
	_, b := read()
	if b.count != 3 || b.sameContent(a) {
		t.Fatalf("commit + compaction: %+v vs %+v", b, a)
	}
}

// heldLab holds media.mov for a foreground operation while held is set. With
// raceOnce set to 1, the next check (the push's pre-check) passes and the
// one after it (the Store's recheck under its writer lock) refuses: the owner
// began between the read and the write.
func heldLab(t *testing.T) (*spanLab, *atomic.Bool, *atomic.Int32) {
	l := newSpanLab(t)
	var held atomic.Bool
	var raceOnce atomic.Int32
	l.store.SetForegroundOwner(func(p string) bool {
		if p != "media.mov" {
			return false
		}
		switch raceOnce.Load() {
		case 1:
			raceOnce.Store(2)
			return false
		case 2:
			raceOnce.Store(0)
			return true
		}
		return held.Load()
	})
	return l, &held, &raceOnce
}

func (l *spanLab) pushOwned() (deferred []uint64) {
	l.t.Helper()
	if !l.gateClock {
		l.advance(labGateInterval + time.Second)
	}
	retry, deferred, err := l.rc.reconcileFileAttrsOwned([]uint64{42}, l.gen)
	if err != nil || retry != nil {
		l.t.Fatalf("push: retry %v err %v", retry, err)
	}
	return deferred
}

// #100 review M4 and review b M1: the Mac's in-place save abroad holds the
// file while its WRITEs sync. The attr push meets the held file (at its
// pre-check, or at the Store's recheck when the owner begins in between) and
// reads it again after the release. The re-read must take the echo for the
// Mac's own bytes: the true mtime, no late commit, no +1 ns. A later commit
// of the same bytes under that mtime is the Mac's own too.
func TestHeldOwnInPlaceSaveKeepsItsMtime(t *testing.T) {
	for _, race := range []bool{false, true} {
		name := "held at the pre-check"
		if race {
			name = "owner begins between the read and the write"
		}
		t.Run(name, func(t *testing.T) {
			l, held, raceOnce := heldLab(t)
			m0 := time.Unix(1700000100, 25)
			l.set(m0, m0.Add(time.Second), 8192, 2) // a teammate's file
			l.push()
			// The Mac's in-place save publishes its own row (seconds only,
			// as CompleteOnlineMutation does) while the operation holds it.
			held.Store(true)
			ownSec := time.Unix(1700000300, 0)
			if err := l.store.SetSizeExact("media.mov", 9000, ownSec); err != nil {
				t.Fatal(err)
			}
			// Its echo: JuiceFS's own mtime, from the previous second (the
			// sync took a while abroad). Without the own mark this would be
			// reported as ownSec + 1 ns.
			mown := time.Unix(1700000299, 700000000)
			// The WRITE's own content mark: JuiceFS stamped the mtime inside
			// the WRITE syscall, which began just before and synced 2 s
			// later (nfs/write_binding.go).
			l.store.NoteOwnBytes("media.mov", 9000, mown.Add(-300*time.Microsecond), mown.Add(2*time.Second))
			l.set(mown, mown.Add(2*time.Second), 9000, 3)
			if !race {
				if d := l.pushOwned(); len(d) != 1 {
					t.Fatalf("held: deferred %v", d)
				}
			}
			held.Store(false)
			if race {
				raceOnce.Store(1)
				if d := l.pushOwned(); len(d) != 1 {
					t.Fatalf("raced: deferred %v", d)
				}
			}
			lateBefore := attrPushLateCommits.Load()
			if d := l.pushOwned(); len(d) != 0 { // the re-read
				t.Fatalf("re-read deferred %v", d)
			}
			if got := l.reported(); !got.Equal(mown) {
				t.Fatalf("own save after the re-read: NFS mtime %v, want %v (no +1 ns)", got, mown)
			}
			l.set(mown, mown.Add(3*time.Second), 9000, 4) // a later part of the same save
			l.push()
			if got := l.reported(); !got.Equal(mown) || attrPushLateCommits.Load() != lateBefore {
				t.Fatalf("own late part: NFS mtime %v, late commits +%d", got, attrPushLateCommits.Load()-lateBefore)
			}
		})
	}
}

// A teammate's commit that lands while the Mac holds the file still counts on
// the re-read, with the Mac's own content mark fresh.
func TestTeammateCommitDuringHeldWindowCounts(t *testing.T) {
	l, held, _ := heldLab(t)
	ownSec := time.Unix(1700000300, 0)
	if err := l.store.SetSizeExact("media.mov", 9000, ownSec); err != nil {
		t.Fatal(err)
	}
	mown := time.Unix(1700000299, 700000000)
	l.store.NoteOwnContent("media.mov", 9000, mown)
	l.set(mown, mown.Add(time.Second), 9000, 3)
	l.push() // the Mac's own echo
	mt := time.Unix(1700000400, 250)
	l.set(mt, mt.Add(time.Second), 9500, 5) // the teammate's save, first commit
	l.push()
	if got := l.reported(); !got.Equal(mt) {
		t.Fatalf("teammate's save: NFS mtime %v, want %v", got, mt)
	}
	before, lateBefore := l.hooks(), attrPushLateCommits.Load()
	held.Store(true)
	l.set(mt, mt.Add(2*time.Second), 9500, 6) // its late part, while held
	if d := l.pushOwned(); len(d) != 1 || l.hooks() != before {
		t.Fatalf("held: deferred %v hooks %d -> %d", d, before, l.hooks())
	}
	held.Store(false)
	l.pushOwned()
	if got := l.reported(); got.Equal(mt) || l.hooks() != before+1 || attrPushLateCommits.Load() != lateBefore+1 {
		t.Fatalf("re-read: NFS mtime %v hooks %d -> %d late +%d", got, before, l.hooks(), attrPushLateCommits.Load()-lateBefore)
	}
}

// Review 99d H1 (probe shape): a teammate's same-size in-place save 20 s
// after the Mac's own save is not the Mac's own (no exact match), so its late
// part counts.
func TestTeammateSameSizeSaveIsNotOwn(t *testing.T) {
	l := newSpanLab(t)
	if err := l.store.SetSizeExact("media.mov", 9000, time.Unix(1700000300, 0)); err != nil {
		t.Fatal(err)
	}
	mown := time.Unix(1700000299, 700000000)
	// The in-place WRITE's bracket (2 s from its syscall to its sync).
	l.store.NoteOwnBytes("media.mov", 9000, mown.Add(-300*time.Microsecond), mown.Add(2*time.Second))
	l.set(mown, mown.Add(time.Second), 9000, 3)
	l.push() // the Mac's own echo
	if got := l.reported(); !got.Equal(mown) {
		t.Fatalf("own echo: NFS mtime %v, want %v", got, mown)
	}
	mt := time.Unix(1700000320, 250) // the teammate, same size, 20 s later
	l.set(mt, mt.Add(time.Second), 9000, 5)
	l.push()
	before, lateBefore := l.hooks(), attrPushLateCommits.Load()
	l.set(mt, mt.Add(3*time.Second), 9000, 6) // its late part
	l.push()
	if got := l.reported(); got.Equal(mt) || l.hooks() != before+1 || attrPushLateCommits.Load() != lateBefore+1 {
		t.Fatalf("teammate late part: NFS mtime %v (true %v) hooks %d -> %d late +%d",
			got, mt, before, l.hooks(), attrPushLateCommits.Load()-lateBefore)
	}
}

// Review 99d M1 (probe shape): the folder read publishes the Mac's own save
// first; a later part of the same save (exactly the Mac's size and mtime)
// is still the Mac's own.
func TestOwnSavePublishedByTheFolderReadKeepsItsMtime(t *testing.T) {
	l := newSpanLab(t)
	if err := l.store.SetSizeExact("media.mov", 9000, time.Unix(1700000300, 0)); err != nil {
		t.Fatal(err)
	}
	mown := time.Unix(1700000299, 700000000)
	l.store.NoteOwnContent("media.mov", 9000, mown)
	l.set(mown, mown.Add(time.Second), 9000, 3)
	l.reconcile()
	if got := l.reported(); !got.Equal(mown) {
		t.Fatalf("folder read: NFS mtime %v, want %v", got, mown)
	}
	before, lateBefore := l.hooks(), attrPushLateCommits.Load()
	l.set(mown, mown.Add(3*time.Second), 9000, 4) // a later part of the same save
	l.push()
	if got := l.reported(); !got.Equal(mown) || l.hooks() != before || attrPushLateCommits.Load() != lateBefore {
		t.Fatalf("own late part: NFS mtime %v hooks %d -> %d late +%d", got, before, l.hooks(), attrPushLateCommits.Load()-lateBefore)
	}
}

// Review 99d H2: 50,000 marks (a bulk copy) keep the map bounded and each
// call cheap: no call scans the whole map.
func TestOwnMarksStayBounded(t *testing.T) {
	var o ownMarks
	o.init() // Store open does this
	clock := time.Unix(1700000000, 0)
	o.now = func() time.Time { return clock }
	const n = 50000
	start := time.Now()
	calls := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		clock = clock.Add(time.Millisecond) // 50 s in all: none expires
		s := time.Now()
		o.noteContent(uint64(i+1), 100, clock, clock)
		calls[i] = time.Since(s)
	}
	total := time.Since(start)
	first := calls[0]
	rest := append([]time.Duration(nil), calls[1:]...)
	sort.Slice(rest, func(i, j int) bool { return rest[i] < rest[j] })
	t.Logf("%d marks: total %v first call %v p99 %v p99.9 %v worst after the first %v map %d ring %d",
		n, total, first, rest[len(rest)*99/100], rest[len(rest)*999/1000], rest[len(rest)-1], o.len(), o.size)
	if o.len() > ownMarksMax || o.size > ownRingSize {
		t.Fatalf("map %d ring %d past the caps", o.len(), o.size)
	}
	if !o.exact(n, 100, clock) || o.exact(1, 100, time.Unix(1700000000, 0).Add(time.Millisecond)) {
		t.Fatal("the newest mark must stay and the oldest go")
	}
	// Expiry: past the content window every mark goes.
	clock = clock.Add(ownContentWindow + time.Second)
	o.noteEcho(999999)
	if o.len() != 1 {
		t.Fatalf("after expiry: %d marks", o.len())
	}
}

func BenchmarkOwnMarksBulk(b *testing.B) {
	var o ownMarks
	clock := time.Unix(1700000000, 0)
	o.now = func() time.Time { return clock }
	for i := 0; i < b.N; i++ {
		clock = clock.Add(time.Millisecond)
		o.noteContent(uint64(i%50000+1), 100, clock, clock)
	}
}

// Review 99d L1: an echo mark set while a judged row is being written (the
// Mac's chmod during a teammate's late-commit write) survives that row's
// settle; the mark the row was judged with is used up.
func TestSettleUsesOnlyTheJudgedEchoMark(t *testing.T) {
	var o ownMarks
	o.noteEcho(7)
	o.takeEchoIf(7, o.echoGen(7))
	if o.echoGen(7) != 0 {
		t.Fatal("the judged mark must be used up")
	}
	judged := o.echoGen(7) // none at judge time
	o.noteEcho(7)          // the Mac's chmod during the write
	o.takeEchoIf(7, judged)
	if o.echoGen(7) == 0 {
		t.Fatal("a mark set after the row was judged must survive")
	}
}

// Review 99d L2: an echo mark whose path is held longer than its window is
// renewed by the deferral, so it still covers the echo after the release.
func TestHeldEchoMarkRunsFromTheRelease(t *testing.T) {
	var o ownMarks
	clock := time.Unix(1700000000, 0)
	o.now = func() time.Time { return clock }
	o.noteEcho(7)
	for i := 0; i < 5; i++ { // held for 100 s, deferred every 20 s
		clock = clock.Add(20 * time.Second)
		o.renewEcho(7)
	}
	clock = clock.Add(5 * time.Second) // the re-read after the release
	if !o.takeEcho(7) {
		t.Fatal("the echo mark expired while its path was held")
	}
}

// Review 99e M1: the push judges a changed row before the Mac's own WRITE
// marks it; the WRITE marks, publishes and releases the path before the
// Store write. The Store write judges again under its lock: the Mac's own
// save keeps its true mtime.
func TestOwnWriteMarkedAfterThePushJudgedKeepsItsMtime(t *testing.T) {
	l := newSpanLab(t)
	if err := l.store.SetSizeExact("media.mov", 9000, time.Unix(1700000290, 0)); err != nil {
		t.Fatal(err)
	}
	l.set(time.Unix(1700000290, 0), time.Unix(1700000290, 0), 9000, 2)
	l.push()
	mown := time.Unix(1700000299, 700000000) // JuiceFS's stamp of the Mac's WRITE
	l.set(mown, mown.Add(time.Second), 9000, 3)
	e := l.store.LookupByInode(42)
	own := l.store.ownBytes(42, 9000, mown) // the push's judgment: no mark yet
	if own {
		t.Fatal("setup: marked before the judgment")
	}
	up := &Entry{Path: e.Path, Name: e.Name, ParentPath: e.ParentPath, Size: 9000, Mtime: mown,
		MtimeNsecKnown: true, Inode: 42, Mode: e.Mode, span: &attrSpan{sig: sliceSig{3, 3}, own: own, ctime: mown.Add(time.Second)}}
	// The WRITE finishes: mark, publish, release.
	l.store.NoteOwnBytes("media.mov", 9000, mown.Add(-300*time.Microsecond), mown.Add(2*time.Second))
	if err := l.store.SetSizeExact("media.mov", 9000, mown.Add(2100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	token := l.rc.scanToken()
	if _, err := l.rc.applyReconcileUpsertsOwned([]*Entry{up}, &token); err != nil {
		t.Fatal(err)
	}
	if got := l.reported(); !got.Equal(mown) {
		t.Errorf("the Mac's own save reported %d, want its true mtime %d", got.UnixNano(), mown.UnixNano())
	}
}

// Review 99e M1, the late-commit shape: a ctime-only part judged a late
// commit before the mark that makes it the Mac's own. The Store write
// judges it own: nothing moves, no content hook fires, and the caller does
// not count it as late.
func TestOwnLateCommitMarkedAfterThePushJudgedIsItsEcho(t *testing.T) {
	l := newSpanLab(t)
	clock := time.Unix(1700000300, 0)
	l.store.own.now = func() time.Time { return clock }
	l.set(time.Unix(1700000290, 0), time.Unix(1700000290, 0), 9000, 2)
	l.push()
	mown := time.Unix(1700000299, 700000000)
	from, to := mown.Add(-300*time.Microsecond), mown.Add(2*time.Second)
	l.store.NoteOwnBytes("media.mov", 9000, from, to)
	l.set(mown, mown.Add(time.Second), 9000, 3)
	l.push()
	if got := l.reported(); !got.Equal(mown) {
		t.Fatalf("setup: own save reported %d, want %d", got.UnixNano(), mown.UnixNano())
	}
	clock = clock.Add(3 * time.Minute) // the mark expired: the push judges a late commit
	e := l.store.LookupByInode(42)
	late := &Entry{Path: e.Path, Name: e.Name, ParentPath: e.ParentPath, Size: 9000, Mtime: mown,
		MtimeNsecKnown: true, Inode: 42, Mode: e.Mode,
		span: &attrSpan{ctime: mown.Add(3 * time.Second), sig: unknownSig, lateCommit: true}}
	l.store.NoteOwnBytes("media.mov", 9000, from, to) // the Mac's own operation marks it
	hooks := l.hooks()
	token := l.rc.scanToken()
	if _, err := l.rc.applyReconcileUpsertsOwned([]*Entry{late}, &token); err != nil {
		t.Fatal(err)
	}
	if got := l.reported(); !got.Equal(mown) {
		t.Errorf("own late part moved the mtime to %d, want %d", got.UnixNano(), mown.UnixNano())
	}
	if l.hooks() != hooks {
		t.Errorf("own late part fired %d content hooks", l.hooks()-hooks)
	}
	if late.span == nil || late.span.lateCommit || !late.span.own {
		t.Errorf("the caller still sees a late commit: %+v", late.span)
	}
}

// Lab 2026-11-16 (z-os-ab3, after review 99e): the push reports the Mac's
// own value m; the WRITE's publish then sets the row to its own time (whole
// seconds, above m); a later push sees m again. m was reported as m for
// these same own bytes, so it is reported as before, not moved.
func TestOwnValueSeenAgainAfterThePublishIsReportedAsBefore(t *testing.T) {
	l := newSpanLab(t)
	l.set(time.Unix(1700000290, 0), time.Unix(1700000290, 0), 9000, 2)
	l.push()
	mown := time.Unix(1700000308, 709593334)
	l.store.NoteOwnBytes("media.mov", 9000, mown.Add(-300*time.Microsecond), mown.Add(2*time.Second))
	l.set(mown, mown.Add(time.Second), 9000, 3)
	l.push()
	if got := l.reported(); !got.Equal(mown) {
		t.Fatalf("setup: reported %d, want %d", got.UnixNano(), mown.UnixNano())
	}
	// CompleteOnlineMutation(time.Now()): whole seconds, above m.
	if err := l.store.SetSizeExact("media.mov", 9000, time.Unix(1700000311, 0)); err != nil {
		t.Fatal(err)
	}
	l.set(mown, mown.Add(2*time.Second), 9000, 3)
	l.push()
	if got := l.reported(); !got.Equal(mown) {
		t.Errorf("the Mac's own value seen again reported %d, want %d as before", got.UnixNano(), mown.UnixNano())
	}
	// A teammate's value seen again is still moved (never repeat).
	mt := time.Unix(1700000320, 0)
	l.set(mt, mt, 9000, 5)
	l.push()
	if err := l.store.SetSizeExact("media.mov", 9000, time.Unix(1700000330, 0)); err != nil {
		t.Fatal(err)
	}
	l.set(mt, mt.Add(time.Second), 9000, 5)
	l.push()
	if got := l.reported(); got.Equal(mt) {
		t.Errorf("a teammate's value seen again was repeated: %d", got.UnixNano())
	}
}
