package nfs

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// H-8 drainer no-clobber oracles. Each one fails if the drainer opens,
// truncates, removes or overwrites a destination file it did not create.

func drainOnce(t *testing.T, d *Drainer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if n := d.DrainOnceForTest(ctx); n != 1 {
		t.Fatalf("processed=%d, want 1", n)
	}
}

// mirrorSeesBackend publishes a mirror entry for an existing backend file, as
// LOOKUP/readdir would after this Mac saw it. A save of an existing file only
// rewrites in place when this Mac saw the file it replaces (H-8B base
// identity); without the entry the name was new to this Mac and the drain
// publishes a conflict copy.
func mirrorSeesBackend(t *testing.T, spool *SpoolStore, name, dest string) {
	t.Helper()
	fi, err := os.Lstat(dest)
	if err != nil {
		t.Fatal(err)
	}
	store := spool.Meta().Owner()
	if store == nil {
		t.Fatal("spool store not wired to a metadata store")
	}
	if err := store.Insert(metadata.MakeEntry(strings.TrimPrefix(name, "/"), false, fi.Size(), fi.ModTime(), 4242)); err != nil {
		t.Fatal(err)
	}
}

func assertNoDrainStages(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dir, err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), streamTempPrefix) {
			t.Errorf("H8: drain left stage litter %q", e.Name())
		}
	}
}

// A REMOVE + replacement that lands while the copy is in flight cancels the
// row. The canceled drain must not install over the replacement.
func TestDrainCancelBeforeInstallPreservesReplacement(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/reel.mov"
	dest := filepath.Join(d.fuseRoot, name)
	writeSpoolEntry(t, spool, name, patternedPayload(4096, 37))
	replacement := patternedPayload(128, 109)
	drainPreInstallHook = func(row *metadata.SpoolRow) {
		spool.CancelForDelete(name)
		if err := os.WriteFile(dest, replacement, 0o600); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("H8: canceled drain removed replacement: %v", err)
	}
	if !bytes.Equal(got, replacement) {
		t.Fatalf("H8: canceled drain overwrote replacement (got %d bytes, want %d)", len(got), len(replacement))
	}
	// A canceled drain publishes nothing: no stage and no conflict copy.
	ents, _ := os.ReadDir(filepath.Dir(dest))
	if len(ents) != 1 {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("H8: canceled drain published extra files: %q", names)
	}
}

// A drain that fails after an in-place rewrite must not remove the existing
// destination: the pathname, and its inode, stay. The retry rewrites it.
func TestDrainFailureNeverRemovesExistingDestination(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/edit.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("version one"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := reviewIno(t, dest)
	mirrorSeesBackend(t, spool, name, dest)
	e := writeSpoolEntry(t, spool, name, []byte("our spooled bytes"))
	growSpoolFile(t, e.SpoolFilePath()) // copy ends in a size mismatch

	drainOnce(t, d)

	if _, err := os.Lstat(dest); err != nil {
		t.Fatalf("H8: failed drain removed the destination: %v", err)
	}
	if after := reviewIno(t, dest); after != before {
		t.Fatalf("H8: failed drain replaced the destination inode %d -> %d", before, after)
	}
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState == metadata.DrainDone {
		t.Fatalf("failed drain must not complete: %+v", row)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

func growSpoolFile(t *testing.T, p string) {
	t.Helper()
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("-extra")); err != nil {
		t.Fatal(err)
	}
}

// retryCreateAfterPermission removed a read-only destination and recreated it.
// It is deleted: a read-only file owned by ANOTHER uid fails the attempt and
// survives untouched.
func TestDrainPermissionCaseNeverRemovesReadOnlyDestination(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	drainGeteuid = func() int { return os.Geteuid() + 1 } // the file is not ours
	defer func() { drainGeteuid = os.Geteuid }()
	const name = "/proj/._locked"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	other := []byte("read-only file another writer owns")
	if err := os.WriteFile(dest, other, 0o444); err != nil {
		t.Fatal(err)
	}
	before := reviewIno(t, dest)
	mirrorSeesBackend(t, spool, name, dest)
	e := writeSpoolEntry(t, spool, name, []byte("our sidecar"))

	drainOnce(t, d)

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("H8: permission path removed the read-only destination: %v", err)
	}
	if !bytes.Equal(got, other) || reviewIno(t, dest) != before {
		t.Fatalf("H8: permission path replaced the read-only destination: %q", got)
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState == metadata.DrainDone {
		t.Fatalf("drain must not complete over a read-only destination: %+v", row)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

// A successful new-file drain leaves exactly the destination: no stage litter.
func TestDrainSuccessLeavesNoStage(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/new.mov"
	payload := patternedPayload(8192, 5)
	writeSpoolEntry(t, spool, name, payload)
	drainOnce(t, d)
	dest := filepath.Join(d.fuseRoot, name)
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("dest=%d bytes err=%v", len(got), err)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

// A 255-byte user name must still drain: the stage name is capped.
func TestDrainStagePathFitsComponentLimit(t *testing.T) {
	long := strings.Repeat("é", 127) + "x" // 255 bytes
	p, err := drainStagePath(filepath.Join("/root", long), testInstance, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(p)
	if len(base) > 255 || !drainStageName.MatchString(base) || !metadata.StreamPartialName(base) {
		t.Fatalf("stage name %d bytes %q", len(base), base)
	}
}

// discard must remove only the inode this drain created.
func TestDrainStageDiscardRemovesOnlyItsOwnInode(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "clip.mov")
	st, err := createDrainStage(dest, testInstance, 7)
	if err != nil {
		t.Fatal(err)
	}
	st.f.Close()
	foreign := filepath.Join(dir, "foreign")
	if err := os.WriteFile(foreign, []byte("not ours"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(foreign, st.path); err != nil {
		t.Fatal(err)
	}
	st.discard()
	if got, err := os.ReadFile(st.path); err != nil || string(got) != "not ours" {
		t.Fatalf("H8: discard removed a file it did not create: %q err=%v", got, err)
	}
	st2, err := createDrainStage(dest, testInstance, 8)
	if err != nil {
		t.Fatal(err)
	}
	st2.f.Close()
	st2.discard()
	if _, err := os.Lstat(st2.path); !os.IsNotExist(err) {
		t.Fatalf("own stage not removed: %v", err)
	}
}

func fixConflictClock(t *testing.T) {
	drainConflictHost = func() string { return "mac2" }
	drainConflictNow = func() time.Time { return time.Date(2026, 10, 6, 14, 3, 9, 0, time.UTC) }
	t.Cleanup(func() { drainConflictHost, drainConflictNow = defaultConflictHost, time.Now })
}

// A new name that another writer creates while this drain copies is never
// overwritten: the drain publishes a conflict copy and keeps its spool source.
func TestDrainNewNameTakenPublishesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/reel.mov"
	dest := filepath.Join(d.fuseRoot, name)
	ours := patternedPayload(4096, 37)
	e := writeSpoolEntry(t, spool, name, ours)
	theirs := []byte("another writer created this name first")
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if err := os.WriteFile(dest, theirs, 0o644); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("H8: new-name drain overwrote another writer's file (%d bytes)", len(got))
	}
	conflict := filepath.Join(filepath.Dir(dest), "reel (conflict mac2 2026-10-06 140309).mov")
	if got, err := os.ReadFile(conflict); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("conflict copy: %d bytes err=%v", len(got), err)
	}
	row, _ := spool.Meta().Get(e.ID())
	if row == nil || row.DrainState == metadata.DrainDone {
		t.Fatalf("conflict-published row must not complete at the original name: %+v", row)
	}
	if _, err := os.Stat(e.SpoolFilePath()); err != nil {
		t.Fatalf("spool source must be retained: %v", err)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

// If an OLDER row of ours for the same path created the name, the newer row
// retries (and then rewrites in place) instead of becoming a conflict copy.
func seedLegacyUnfinishedPredecessor(t *testing.T, spool *SpoolStore, name string) int64 {
	t.Helper()
	id, err := spool.Meta().Insert(name, filepath.Join(t.TempDir(), "older-spool"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Meta().MarkFailed(id, "legacy unfinished predecessor"); err != nil {
		t.Fatal(err)
	}
	return id
}

func restoreLegacyUnfinishedPredecessor(t *testing.T, spool *SpoolStore, id int64) {
	t.Helper()
	// Existing databases can contain the older writing row beside a newer
	// ready save; normal admission no longer creates this ambiguous shape.
	if _, err := spool.Meta().Owner().DB().Exec(`UPDATE spool_entries SET drain_state='writing' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
}

func TestDrainNewNameTakenByOwnOlderRowRetries(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/save.mov"
	dest := filepath.Join(d.fuseRoot, name)
	olderID := seedLegacyUnfinishedPredecessor(t, spool, name)
	e := writeSpoolEntry(t, spool, name, []byte("newer save"))
	restoreLegacyUnfinishedPredecessor(t, spool, olderID)
	drainPreInstallHook = func(*metadata.SpoolRow) {
		// The unfinished older row installs its file (same process).
		if err := os.WriteFile(dest, []byte("older save"), 0o644); err != nil {
			t.Error(err)
		}
		fi, _ := os.Lstat(dest)
		st := fi.Sys().(*syscall.Stat_t)
		d.recordInstalledIdentity(olderID, uint64(st.Dev), uint64(st.Ino))
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	conflict := filepath.Join(filepath.Dir(dest), "save (conflict mac2 2026-10-06 140309).mov")
	if _, err := os.Lstat(conflict); err == nil {
		t.Fatal("H8: own predecessor's file turned the newer save into a conflict copy")
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState == metadata.DrainFailed {
		t.Fatalf("newer save must stay retryable: %+v", row)
	}
	assertNoDrainStages(t, filepath.Dir(dest))
}

// The boot sweep removes only exact-format stages of this spool's rows that
// predate the cutoff. Everything else in the directory survives.
func TestSweepDrainStagesRemovesOnlyOwnOrphans(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/clip.mov"
	e := writeSpoolEntry(t, spool, name, []byte("pending"))
	dir := filepath.Join(d.fuseRoot, "proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	inst, persisted := d.instanceID()
	if !persisted {
		t.Fatal("instance ID not persisted")
	}
	cutoff := time.Now()
	old := cutoff.Add(-time.Hour).UnixNano()
	newer := cutoff.Add(time.Hour).UnixNano()
	mk := func(n string) string {
		p := filepath.Join(dir, n)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	orphan := mk(fmt.Sprintf("%s%s-%d-%x-1-clip.mov", drainStagePrefix, inst, e.ID(), old))
	other := "fedcba9876543210"
	if other == inst {
		other = "0123456789abcdef"
	}
	keep := []string{
		mk(fmt.Sprintf("%s%s-%d-%x-2-clip.mov", drainStagePrefix, inst, e.ID(), newer)),  // after cutoff
		mk(fmt.Sprintf("%s%s-%d-%x-3-clip.mov", drainStagePrefix, inst, e.ID()+99, old)), // not our row
		mk(fmt.Sprintf("%s%s-%d-%x-4-clip.mov", drainStagePrefix, other, e.ID(), old)),   // another Mac, same row ID
		mk(fmt.Sprintf("%s%d-clip.mov", streamTempPrefix, e.ID())),                       // streamer format
		mk(fmt.Sprintf("%sx-%x-1-clip.mov", drainStagePrefix, old)),                      // malformed
		mk("clip.mov"), // user file
	}
	n, err := d.SweepDrainStages(cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("removed %d, want 1", n)
	}
	if _, err := os.Lstat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan stage survived: %v", err)
	}
	for _, p := range keep {
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("H8: sweep removed a file it does not own: %s", filepath.Base(p))
		}
	}
}

// Start runs the boot sweep for stages a crash left behind.
func TestDrainerStartSweepsOrphanStages(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	e := writeSpoolEntry(t, spool, "/proj/crash.mov", []byte("pending"))
	dir := filepath.Join(d.fuseRoot, "proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	inst, _ := d.instanceID()
	orphan := filepath.Join(dir, fmt.Sprintf("%s%s-%d-%x-1-crash.mov", drainStagePrefix, inst, e.ID(), time.Now().Add(-time.Hour).UnixNano()))
	if err := os.WriteFile(orphan, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	d.Start()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(orphan); os.IsNotExist(err) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !d.Stop(5 * time.Second) {
		t.Fatal("drainer did not stop")
	}
	if _, err := os.Lstat(orphan); !os.IsNotExist(err) {
		t.Fatalf("H8: Start did not sweep the orphan stage: %v", err)
	}
}

// A new-file drain that fails after copying removes its own stage and nothing
// else.
func TestDrainNewFileFailureLeavesNoStage(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/fresh.mov"
	e := writeSpoolEntry(t, spool, name, []byte("new bytes"))
	growSpoolFile(t, e.SpoolFilePath())
	drainOnce(t, d)
	dir := filepath.Join(d.fuseRoot, "proj")
	if _, err := os.Lstat(filepath.Join(dir, "fresh.mov")); err == nil {
		t.Fatal("failed new-file drain published the name")
	}
	assertNoDrainStages(t, dir)
}

const testInstance = "0123456789abcdef"

// The drain effects chmod our published sidecar to its final mode (0444).
// Finder's next rewrite must land in place on that same inode (2d8fc2b).
func TestDrainRewritesOwnReadOnlyFileInPlace(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/perms/._readonly.dat"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("older-sidecar"), 0o444); err != nil {
		t.Fatal(err)
	}
	before := reviewIno(t, dest)
	mirrorSeesBackend(t, spool, name, dest)
	payload := []byte("new-authoritative-appledouble")
	e := writeSpoolEntry(t, spool, name, payload)

	drainOnce(t, d)

	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("H8: own read-only rewrite did not land: %q err=%v", got, err)
	}
	if after := reviewIno(t, dest); after != before {
		t.Fatalf("H8: own read-only rewrite changed inode %d -> %d", before, after)
	}
	if fi, _ := os.Lstat(dest); fi.Mode().Perm() != 0o444 {
		t.Fatalf("original mode not restored: %v", fi.Mode().Perm())
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("row not done: %+v", row)
	}
}

// A canceled row must not truncate the replacement at an existing name: the
// claim recheck precedes the in-place O_TRUNC open.
func TestDrainCancelBeforeInPlaceOpenPreservesReplacement(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/cut.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	mirrorSeesBackend(t, spool, name, dest)
	writeSpoolEntry(t, spool, name, []byte("stale v2 from a canceled row"))
	replacement := []byte("replacement written after REMOVE+CREATE")
	drainPreOpenHook = func(*metadata.SpoolRow) {
		spool.CancelForDelete(name)
		if err := os.WriteFile(dest, replacement, 0o644); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreOpenHook = nil }()

	drainOnce(t, d)

	if got, _ := os.ReadFile(dest); !bytes.Equal(got, replacement) {
		t.Fatalf("H8: canceled row truncated the replacement: %q", got)
	}
}

// Reviewer probe P2: an older DONE row (retained ~10 min) must not license
// overwriting a file another Mac created at the name.
func TestDrainOlderDoneRowDoesNotLicenseOverwrite(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/x.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	old, err := spool.Meta().Insert(name, filepath.Join(t.TempDir(), "old"))
	if err != nil {
		t.Fatal(err)
	}
	spool.Meta().MarkReady(old, 0, nil)
	spool.Meta().MarkDraining(old)
	if ok, err := spool.Meta().MarkDone(old); !ok || err != nil {
		t.Fatalf("markdone %v %v", ok, err)
	}
	writeSpoolEntry(t, spool, name, []byte("mac A new"))
	macB := []byte("MAC B's file created while A's drain staged")
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if _, err := os.Lstat(dest); err != nil {
			if err := os.WriteFile(dest, macB, 0o644); err != nil {
				t.Error(err)
			}
			// Even an identity recorded for the finished row must not count.
			fi, _ := os.Lstat(dest)
			st := fi.Sys().(*syscall.Stat_t)
			d.recordInstalledIdentity(old, uint64(st.Dev), uint64(st.Ino))
		}
	}
	defer func() { drainPreInstallHook = nil }()

	for i := 0; i < 5; i++ {
		drainOnce(t, d)
		if rows, _ := spool.Meta().ListReady(1); len(rows) == 0 {
			break
		}
	}
	if got, _ := os.ReadFile(dest); !bytes.Equal(got, macB) {
		t.Fatalf("H8: another Mac's file overwritten via a finished older row: %q", got)
	}
	conflict := filepath.Join(filepath.Dir(dest), "x (conflict mac2 2026-10-06 140309).mov")
	if _, err := os.Lstat(conflict); err != nil {
		t.Fatalf("expected conflict copy: %v", err)
	}
}

// The spool instance ID persists across drainers on the same spool root.
func TestDrainInstanceIDPersists(t *testing.T) {
	root := t.TempDir()
	a, ok := loadDrainInstanceID(root)
	if !ok || !drainInstanceRe.MatchString(a) {
		t.Fatalf("first id %q ok=%v", a, ok)
	}
	b, ok := loadDrainInstanceID(root)
	if !ok || a != b {
		t.Fatalf("id not persisted: %q then %q", a, b)
	}
}

// If the path is swapped between the chmod and the reopen, the swapped-in
// file is neither truncated nor written, and the attempt fails.
func TestDrainOwnReadOnlyRewriteRefusesSwappedFile(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/perms/._swap.dat"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest, []byte("ours-0444"), 0o444); err != nil {
		t.Fatal(err)
	}
	mirrorSeesBackend(t, spool, name, dest)
	e := writeSpoolEntry(t, spool, name, []byte("rewrite"))
	swapped := []byte("someone else's file swapped in")
	drainOwnReadOnlyHook = func(p string) {
		other := filepath.Join(filepath.Dir(p), "other")
		if err := os.WriteFile(other, swapped, 0o644); err != nil {
			t.Error(err)
		}
		if err := os.Rename(other, p); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainOwnReadOnlyHook = nil }()

	drainOnce(t, d)

	if got, _ := os.ReadFile(dest); !bytes.Equal(got, swapped) {
		t.Fatalf("H8: swapped-in file was modified: %q", got)
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState == metadata.DrainDone {
		t.Fatalf("attempt must not complete onto a swapped file: %+v", row)
	}
}

// An UNFINISHED older row exists, but the file at the name is not the inode it
// wrote (another writer's). That is a conflict copy, not an in-place retry.
func TestDrainUnfinishedOlderRowWithForeignFileMakesConflictCopy(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{})
	const name = "/proj/z.mov"
	dest := filepath.Join(d.fuseRoot, name)
	olderID := seedLegacyUnfinishedPredecessor(t, spool, name)
	d.recordInstalledIdentity(olderID, 1, 999999999) // the older row wrote some other inode
	writeSpoolEntry(t, spool, name, []byte("ours"))
	restoreLegacyUnfinishedPredecessor(t, spool, olderID)
	theirs := []byte("another writer")
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if err := os.WriteFile(dest, theirs, 0o644); err != nil {
			t.Error(err)
		}
	}
	defer func() { drainPreInstallHook = nil }()

	drainOnce(t, d)

	if got, _ := os.ReadFile(dest); !bytes.Equal(got, theirs) {
		t.Fatalf("H8: foreign file changed: %q", got)
	}
	conflict := filepath.Join(filepath.Dir(dest), "z (conflict mac2 2026-10-06 140309).mov")
	if _, err := os.Lstat(conflict); err != nil {
		t.Fatalf("H8: expected a conflict copy, not a retry: %v", err)
	}
}
