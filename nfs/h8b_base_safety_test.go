package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
	"golang.org/x/sys/unix"
)

// H-8B base-identity safety tests. Default build: no tags, no Redis.

func writeBackendFile(t *testing.T, d *Drainer, name string, body []byte) string {
	t.Helper()
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return dest
}

func withOffline(t *testing.T, fn func()) {
	t.Helper()
	was := pin.IsOffline()
	pin.SetOffline(true)
	defer pin.SetOffline(was)
	fn()
}

func conflictCopyOf(d *Drainer, name string) string {
	dir, base := filepath.Split(filepath.Join(d.fuseRoot, name))
	ext := filepath.Ext(base)
	return filepath.Join(dir, base[:len(base)-len(ext)]+" (conflict mac2 2026-10-06 140309)"+ext)
}

func rowOf(t *testing.T, spool *SpoolStore, e *SpoolEntry) *metadata.SpoolRow {
	t.Helper()
	r, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A slow base probe must not hold the OpenWrite shard lock: an unrelated open
// that hashes to the same shard proceeds while the probe runs.
func TestH8BSlowBaseProbeDoesNotBlockSameShardOpen(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	const slowPath = "Film/ProjA/edit.prproj"
	other := ""
	for i := 0; i < 1_000_000 && other == ""; i++ {
		p := "Film/ProjB/f" + strconv.Itoa(i)
		if spool.pathShard(p) == spool.pathShard(slowPath) {
			other = p
		}
	}
	if other == "" {
		t.Fatal("no same-shard path found")
	}
	// An active entry for `other` exists before the slow probe starts; its next
	// open only needs the shard lock.
	e2, err := spool.OpenWrite(other)
	if err != nil {
		t.Fatal(err)
	}
	const probeDelay = 1500 * time.Millisecond
	entered := make(chan struct{})
	slow := spoolBaseProbe(func(p string) (metadata.SpoolBase, uint64) {
		if p == slowPath {
			close(entered)
			time.Sleep(probeDelay)
		}
		return metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, 0
	})
	spool.baseProbe.Store(&slow)
	done := make(chan error, 1)
	go func() {
		e, err := spool.OpenWrite(slowPath)
		if err == nil {
			e.ReleaseHandle()
		}
		done <- err
	}()
	<-entered
	start := time.Now()
	e3, err := spool.OpenWrite(other)
	if err != nil {
		t.Fatal(err)
	}
	blocked := time.Since(start)
	e3.ReleaseHandle()
	e2.ReleaseHandle()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if blocked > probeDelay/3 {
		t.Fatalf("H8B: a same-shard open waited %v behind another path's base probe", blocked)
	}
}

// A row parked in waiting_project must never strand when no lease client is
// wired: it is dispatched, and Start clears the wait.
func TestH8BHeldRowNotStrandedWithoutLeaseClient(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/card/A001.mov"
	payload := []byte("held by a previous run that had a lease client")
	e := writeSpoolEntry(t, spool, name, payload)
	if ok, err := spool.Meta().MarkDraining(e.ID()); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if held, err := spool.Meta().HoldForProject(e.ID(), "Film/ProjA", "waiting_project: test"); err != nil || !held {
		t.Fatal(held, err)
	}
	d.releaseHoldsWithoutLeases()
	if w, _ := spool.Meta().WaitProject(e.ID()); w != "" {
		t.Fatalf("H8B: start without a lease client left the row waiting on %q", w)
	}
	// Even if a wait reappears (a client was removed at runtime), dispatch
	// must not skip it.
	if ok, err := spool.Meta().MarkDraining(e.ID()); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if held, _ := spool.Meta().HoldForProject(e.ID(), "Film/ProjA", "again"); !held {
		t.Fatal("re-hold failed")
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(filepath.Join(d.fuseRoot, name)); !bytes.Equal(got, payload) {
		t.Fatalf("H8B: waiting row stranded without a lease client (%d bytes drained)", len(got))
	}
}

// An exact (present) base that no longer matches: another writer rewrote the
// file in place between the save and the drain. Conflict copy, original kept.
func TestH8BPresentBaseMismatchBecomesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/notes.txt"
	dest := writeBackendFile(t, d, name, []byte("v1"))
	mirrorSeesBackend(t, spool, name, dest)
	ours := []byte("v2 our save")
	e := writeSpoolEntry(t, spool, name, ours)
	if b, _ := d.rowDrainBase(rowOf(t, spool, e)); b.Kind != metadata.SpoolBasePresent {
		t.Fatalf("base %v, want present", b.Kind)
	}
	time.Sleep(5 * time.Millisecond)
	theirs := []byte("v2 other writer, edited in place")
	if err := os.WriteFile(dest, theirs, 0o644); err != nil {
		t.Fatal(err)
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("H8B: changed present base overwritten in place: %q", got)
	}
	if got, err := os.ReadFile(conflictCopyOf(d, name)); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("H8B: no conflict copy: %q %v", got, err)
	}
}

// An offline save records a weak base (mirror length + mtime). If the backend
// file changed meanwhile, the drain publishes a conflict copy.
func TestH8BWeakBaseMismatchBecomesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/offline.txt"
	dest := writeBackendFile(t, d, name, []byte("v1 seen before going offline"))
	mirrorSeesBackend(t, spool, name, dest)
	ours := []byte("v2 offline save")
	var e *SpoolEntry
	withOffline(t, func() { e = writeSpoolEntry(t, spool, name, ours) })
	if b, _ := d.rowDrainBase(rowOf(t, spool, e)); b.Kind != metadata.SpoolBaseWeak {
		t.Fatalf("base %v, want weak", b.Kind)
	}
	theirs := []byte("v2 another Mac, different length")
	if err := os.WriteFile(dest, theirs, 0o644); err != nil {
		t.Fatal(err)
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("H8B: changed weak base overwritten in place: %q", got)
	}
	if _, err := os.Lstat(conflictCopyOf(d, name)); err != nil {
		t.Fatalf("H8B: no conflict copy: %v", err)
	}
}

// A drain that partially rewrote its destination and failed must, on retry,
// recognise its own file (the claimed inode) and rewrite it in place, not
// conflict-copy it.
func TestH8BRetryAfterPartialRewriteStaysInPlace(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/export.mov"
	dest := writeBackendFile(t, d, name, []byte("version one of the export"))
	mirrorSeesBackend(t, spool, name, dest)
	before := reviewIno(t, dest)
	ours := patternedPayload(4096, 21)
	e := writeSpoolEntry(t, spool, name, ours)
	growSpoolFile(t, e.SpoolFilePath()) // first attempt: size mismatch after writing dest
	drainOnce(t, d)
	if r := rowOf(t, spool, e); r.DrainState != metadata.DrainReady {
		t.Fatalf("first attempt should be retryable: %+v", r)
	}
	if err := os.Truncate(e.SpoolFilePath(), int64(len(ours))); err != nil {
		t.Fatal(err)
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, ours) || reviewIno(t, dest) != before {
		t.Fatalf("H8B: retry after a partial in-place rewrite did not land in place (%d bytes)", len(got))
	}
	if _, err := os.Lstat(conflictCopyOf(d, name)); err == nil {
		t.Fatal("H8B: retry conflict-copied its own partially rewritten file")
	}
}

// A file swapped in between the base decision and the open is neither
// truncated nor written.
func TestH8BInPlaceOpenRefusesSwappedInode(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/cut.prproj"
	dest := writeBackendFile(t, d, name, []byte("v1"))
	mirrorSeesBackend(t, spool, name, dest)
	e := writeSpoolEntry(t, spool, name, []byte("v2 ours"))
	swapped := []byte("swapped in after the check")
	drainCheckedHook = func(p string) {
		tmp := p + ".other"
		if err := os.WriteFile(tmp, swapped, 0o644); err != nil {
			t.Error(err)
		}
		if err := os.Rename(tmp, p); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainCheckedHook = nil }()
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, swapped) {
		t.Fatalf("H8B: swapped-in file modified: %q", got)
	}
	if r := rowOf(t, spool, e); r.DrainState == metadata.DrainDone {
		t.Fatalf("H8B: drain completed onto a swapped inode: %+v", r)
	}
}

// countBaseLstats counts the base probe's backend Lstats.
func countBaseLstats(t *testing.T) *int {
	t.Helper()
	n := new(int)
	orig := drainBaseLstat
	drainBaseLstat = func(p string, timeout time.Duration) (unix.Stat_t, error, bool) {
		*n++
		return orig(p, timeout)
	}
	t.Cleanup(func() { drainBaseLstat = orig })
	return n
}

// A new-name CREATE (no mirror entry) records "absent" with ZERO backend
// calls: a card copy of thousands of files stays at local-disk speed and
// never queues on nfsLstatGate. A save over a file the mirror lists makes
// exactly one.
func TestH8BNewNameCreateMakesNoBackendCall(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	calls := countBaseLstats(t)
	for i := 0; i < 50; i++ {
		e := writeSpoolEntry(t, spool, "Card/A001/C"+strconv.Itoa(i)+".MP4", []byte("x"))
		if b, _ := d.rowDrainBase(rowOf(t, spool, e)); b.Kind != metadata.SpoolBaseAbsent {
			t.Fatalf("new name recorded base %v, want absent", b.Kind)
		}
	}
	if *calls != 0 {
		t.Fatalf("H8B: %d backend Lstats for 50 new-name creates, want 0", *calls)
	}
	const name = "Film/ProjA/edit.prproj"
	dest := writeBackendFile(t, d, name, []byte("v1"))
	mirrorSeesBackend(t, spool, name, dest)
	writeSpoolEntry(t, spool, name, []byte("v2"))
	if *calls != 1 {
		t.Fatalf("save over a mirrored file made %d backend Lstats, want 1", *calls)
	}
}

// A mirror miss over an existing backend file (keyspace lag, eviction) costs
// a conflict copy, never an overwrite: the backend file is untouched and our
// bytes are published beside it.
func TestH8BMirrorMissSaveBecomesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/notes.txt"
	theirs := []byte("v1 not in this Mac's mirror")
	dest := writeBackendFile(t, d, name, theirs)
	ours := []byte("v2 my save")
	writeSpoolEntry(t, spool, name, ours)
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("H8B: mirror-miss save overwrote the backend file: %q", got)
	}
	if got, err := os.ReadFile(conflictCopyOf(d, name)); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("H8B: mirror-miss save lost (no conflict copy): %v", err)
	}
}

// A mirrored file whose backend Lstat times out records the mirror's weak
// base, never unknown: a changed file is not overwritten.
func TestH8BProbeTimeoutFallsBackToWeakBase(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/slow.txt"
	dest := writeBackendFile(t, d, name, []byte("v1"))
	mirrorSeesBackend(t, spool, name, dest)
	orig := drainBaseLstat
	drainBaseLstat = func(string, time.Duration) (unix.Stat_t, error, bool) { return unix.Stat_t{}, nil, false }
	defer func() { drainBaseLstat = orig }()
	e := writeSpoolEntry(t, spool, name, []byte("v2"))
	if b, _ := d.rowDrainBase(rowOf(t, spool, e)); b.Kind != metadata.SpoolBaseWeak {
		t.Fatalf("H8B: probe timeout recorded base %v, want weak", b.Kind)
	}
}

// Derivative paths are never mirrored and are governed by the derivative
// clobber guard, not base identity: even an offline write (where a mirror
// miss means "new name" for user paths) rewrites in place.
func TestH8BDerivativePathIgnoresBaseIdentity(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = ".juicemount/state/notes.json"
	dest := writeBackendFile(t, d, name, []byte(`{"v":1}`))
	ours := []byte(`{"v":2}`)
	var e *SpoolEntry
	withOffline(t, func() { e = writeSpoolEntry(t, spool, name, ours) })
	if b, _ := d.rowDrainBase(rowOf(t, spool, e)); b.Kind != metadata.SpoolBaseUnknown {
		t.Fatalf("derivative path recorded base %v, want unknown", b.Kind)
	}
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, ours) {
		t.Fatalf("H8B: derivative-path write did not land in place: %q", got)
	}
}

// A file created by another Mac after this Mac opened its write offline (the
// name was new to this Mac) is never overwritten.
func TestH8BOfflineNewNameTakenElsewhereBecomesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "Film/ProjA/y.mov"
	ours := []byte("mac A offline create")
	var e *SpoolEntry
	withOffline(t, func() { e = writeSpoolEntry(t, spool, name, ours) })
	macB := []byte("MAC B")
	dest := writeBackendFile(t, d, name, macB)
	drainOnce(t, d)
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, macB) {
		t.Fatalf("H8B: other writer's file overwritten: %q", got)
	}
	if got, err := os.ReadFile(conflictCopyOf(d, name)); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("H8B: no conflict copy: %v", err)
	}
	// The conflict-copied row is finished: RetryFailed must not requeue it
	// and publish a duplicate copy.
	if n, err := spool.RetryFailed(); err != nil || n != 0 {
		t.Fatalf("H8B: RetryFailed requeued a conflict-copied row (n=%d err=%v)", n, err)
	}
	if r := rowOf(t, spool, e); r.DrainState != metadata.DrainFailed {
		t.Fatalf("conflict row state %s", r.DrainState)
	}
}

// A pending save renamed together with its backend inode keeps mtime and
// length in its base: an in-place edit of the moved file before the drain
// becomes a conflict copy; an untouched moved file is rewritten in place.
func TestH8BRenameKeepsMtimeAndLengthInBase(t *testing.T) {
	for _, edited := range []bool{true, false} {
		t.Run(map[bool]string{true: "edited", false: "untouched"}[edited], func(t *testing.T) {
			fixConflictClock(t)
			spool, d := newTestDrainer(t, DrainerConfig{})
			const from, to = "Film/ProjA/a.prproj", "Film/ProjA/b.prproj"
			src := writeBackendFile(t, d, from, []byte("v1"))
			mirrorSeesBackend(t, spool, from, src)
			ours := []byte("v2 our save, renamed before it drained")
			writeSpoolEntry(t, spool, from, ours)
			if _, _, err := spool.MigrateForRename(from, to); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(d.fuseRoot, to)
			if err := os.Rename(src, dest); err != nil { // the handler's FUSE rename
				t.Fatal(err)
			}
			theirs := []byte("v1 edited in place by another writer after the rename")
			if edited {
				time.Sleep(5 * time.Millisecond)
				if err := os.WriteFile(dest, theirs, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			drainOnce(t, d)
			got, _ := os.ReadFile(dest)
			if edited && !bytes.Equal(got, theirs) {
				t.Fatalf("H8B: edit of a renamed base overwritten: %q", got)
			}
			if !edited && !bytes.Equal(got, ours) {
				t.Fatalf("H8B: untouched renamed base not rewritten in place: %q", got)
			}
		})
	}
}

// DrainOnceForTest returns 0 when the only row waits on a project and a lease
// client exists (the wait is honoured).
func TestH8BWaitHonouredWithLeaseClient(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	f := newFakeLeaseServer(t)
	d.SetProjectLeases(newTestLeaseClient(t, f, spool, nil))
	e := writeSpoolEntry(t, spool, "Film/ProjA/x.mov", []byte("x"))
	spool.Meta().MarkDraining(e.ID())
	spool.Meta().HoldForProject(e.ID(), "Film/ProjA", "busy")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 0 {
		t.Fatalf("waiting row dispatched while a lease client exists: %d", n)
	}
}
