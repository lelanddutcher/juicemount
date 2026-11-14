package metadata

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
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
	t      *testing.T
	dir    string
	rc     *RedisClient
	store  *Store
	fired  *[]string
	gen    uint64
	mu     sync.Mutex
	attr   string
	slices int64
}

func newSpanLab(t *testing.T) *spanLab {
	t.Helper()
	l := &spanLab{t: t, dir: t.TempDir(), attr: jfsAttr(time.Unix(1700000000, 500), time.Unix(1700000000, 500), 4096), slices: 1}
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
		testReadSlices: func(uint64, int64) int64 {
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.slices
		},
	}
	t.Cleanup(func() { close(rc.stopCh) })
	fired := []string{}
	store.SetOnScanContentInvalidated(func(p string, _ uint64) { fired = append(fired, p) })
	l.rc, l.store, l.fired = rc, store, &fired
	l.gen, _, _ = rc.backendAdmission()
}

// set is what Redis holds now: the i-key and the slice count.
func (l *spanLab) set(mtime, ctime time.Time, size uint64, slices int64) {
	l.mu.Lock()
	l.attr, l.slices = jfsAttr(mtime, ctime, size), slices
	l.mu.Unlock()
}

func (l *spanLab) push() {
	l.t.Helper()
	if retry, err := l.rc.reconcileFileAttrs([]uint64{42}, l.gen); err != nil || retry != nil {
		l.t.Fatalf("push: retry %v err %v", retry, err)
	}
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
		l.store.attrObs.put(1000+i, attrObs{mtimeNS: int64(i), slices: 1}, false)
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

// Review M1: a ctime-only change is not a late commit. The Mac's own chmod
// (marked by the NFS layer) and a teammate's chmod, rename or link (the slice
// count stays) leave the mtime and run no hooks; a commit (one more slice)
// still counts.
func TestCtimeOnlyChangeIsNotALateCommit(t *testing.T) {
	l := newSpanLab(t)
	m := time.Unix(1700000100, 25)
	l.set(m, m.Add(time.Second), 8192, 2)
	l.push()
	before := l.hooks()
	// The Mac's own chmod. The slice count is unknown to the push (a SCAN
	// published the row), so only the mark tells.
	l.store.attrObs.put(42, attrObs{mtimeNS: m.UnixNano(), slices: -1, ctimeUnknown: true}, false)
	l.store.NoteOwnAttrChange("media.mov")
	l.set(m, m.Add(2*time.Second), 8192, 2)
	l.push()
	if got := l.reported(); !got.Equal(m) || l.hooks() != before {
		t.Fatalf("own chmod: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
	// A teammate's chmod: same slices.
	l.store.attrObs.own = nil
	l.set(m, m.Add(3*time.Second), 8192, 2)
	l.push()
	if got := l.reported(); !got.Equal(m) || l.hooks() != before {
		t.Fatalf("teammate chmod: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
	}
	// A late commit: one more slice.
	l.set(m, m.Add(4*time.Second), 8192, 3)
	l.push()
	if got := l.reported(); got.Equal(m) || l.hooks() != before+1 {
		t.Fatalf("late commit: NFS mtime %v hooks %d -> %d", got, before, l.hooks())
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
	l.set(own, own.Add(40*time.Second), 9000, 1) // its echo (Chtimes)
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
