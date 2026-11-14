package nfs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A new file's install rename can land on the server and still report an
// error: the link drops with the reply in flight, or juicefs retries a
// transaction that had already committed and answers EEXIST. The name then
// holds this row's own complete stage. Remote lab (roam-lab.sh drop30,
// 2026-11-13): an 8 MiB file drained across a 30 s link drop left
// "w00009 (conflict ...).bin" beside its own w00009.bin.

func lostReplyFixture(t *testing.T) (*SpoolStore, *Drainer, *SpoolEntry, string, []byte) {
	t.Helper()
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	name := "/proj/shot/clip.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("roam"), 2<<20/4) // 2 MiB: the stage path
	return spool, d, writeSpoolEntry(t, spool, name, payload), dest, payload
}

// lieAfterRename makes the first install rename to dest land and then
// report err. It returns a pointer to the count of such renames.
func lieAfterRename(t *testing.T, dest string, err error) *int {
	t.Helper()
	lied := 0
	orig := drainRenameNoReplace
	drainRenameNoReplace = func(a, b string) error {
		rerr := orig(a, b)
		if rerr == nil && b == dest && lied == 0 {
			lied++
			return err
		}
		return rerr
	}
	t.Cleanup(func() { drainRenameNoReplace = orig })
	return &lied
}

func assertOnlyOwnFile(t *testing.T, spool *SpoolStore, e *SpoolEntry, dest string, payload []byte) {
	t.Helper()
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if row.DrainState != metadata.DrainDone {
		t.Fatalf("row state %s (%q), want done", row.DrainState, row.LastError)
	}
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("destination bytes wrong (%d bytes, %v)", len(got), err)
	}
	ents, err := os.ReadDir(filepath.Dir(dest))
	if err != nil {
		t.Fatal(err)
	}
	for _, ent := range ents {
		if ent.Name() != filepath.Base(dest) {
			t.Fatalf("left %q beside the destination (a conflict copy of its own file?)", ent.Name())
		}
	}
}

// The process sees its own stage at the name right after the failed rename.
func TestDrainInstallRenameLandedButReportedError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"reply lost (EIO)", syscall.EIO},
		{"juicefs retry after commit (EEXIST)", os.ErrExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spool, d, e, dest, payload := lostReplyFixture(t)
			lied := lieAfterRename(t, dest, tc.err)
			probeDrainLoop(d)
			if *lied != 1 {
				t.Fatalf("the install rename ran %d lying times, want 1", *lied)
			}
			assertOnlyOwnFile(t, spool, e, dest, payload)
		})
	}
}

// The app restarts between the landed rename and its reply (the process died
// before it could look): the retry runs in a new drainer with no memory of
// the stage. The persisted staged claim makes it adopt the file: no conflict
// copy, and no truncate and upload again (no in-place open at all).
func TestDrainInstallLostReplyAdoptedAfterRestart(t *testing.T) {
	spool, d, e, dest, payload := lostReplyFixture(t)
	lied := lieAfterRename(t, dest, syscall.EIO)
	drainStageAtName = func(*drainStage, string) bool { return false }
	t.Cleanup(func() { drainStageAtName = stageAtName })

	// First process: the rename lands, the reply is lost, the row goes back
	// to the queue.
	ctx, cancel := context.WithTimeout(context.Background(), drainPassDeadline)
	d.DrainOnceForTest(ctx)
	cancel()
	if *lied != 1 {
		t.Fatalf("the install rename ran %d lying times, want 1", *lied)
	}
	var landed unix.Stat_t
	if err := unix.Lstat(dest, &landed); err != nil {
		t.Fatalf("the landed rename left no file: %v", err)
	}
	base, _, err := spool.Meta().DrainBase(e.ID())
	if err != nil || base.Kind != metadata.SpoolBaseClaimed || !base.Staged || base.Ino != uint64(landed.Ino) {
		t.Fatalf("base after the lost reply = %+v (%v), want a staged claim of inode %d", base, err, landed.Ino)
	}

	// Second process: a new drainer over the same spool (in-memory state gone).
	d2, err := NewDrainer(spool, DrainerConfig{FuseRoot: d.fuseRoot, MaxAttempts: 5, BackoffBase: time.Millisecond,
		PollFallback: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	d2.SetOnDrainCommit(d.onDrainCommit)
	drainCheckedHook = func(string) { t.Errorf("the retry opened its own installed file to rewrite it") }
	t.Cleanup(func() { drainCheckedHook = nil })
	probeDrainLoop(d2)
	assertOnlyOwnFile(t, spool, e, dest, payload)
	var after unix.Stat_t
	if err := unix.Lstat(dest, &after); err != nil || after.Ino != landed.Ino {
		t.Fatalf("the adopted file was replaced: inode %d -> %d (%v)", landed.Ino, after.Ino, err)
	}
}

// The decision itself. The name holds the inode this row staged and claimed.
//   - untainted: adopt, complete without writing;
//   - another Mac held the project since the row was accepted (and may have
//     edited that inode in place): a conflict copy, never an overwrite;
//   - a plain (in-place) claim keeps the old rule: rewrite its own file.
func TestDrainStagedClaimDecision(t *testing.T) {
	for _, tc := range []struct {
		name      string
		staged    bool
		tainted   bool
		wantAdopt bool
	}{
		{"staged untainted adopts", true, false, true},
		{"staged tainted conflicts", true, true, false},
		{"plain claim rewrites in place", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spool, d := newTestDrainer(t, DrainerConfig{})
			name := filepath.Join("Film", "Project", "clip.mov")
			e := writeSpoolEntry(t, spool, name, []byte("our-complete-body"))
			if ok, err := spool.Meta().MarkDraining(e.ID()); err != nil || !ok {
				t.Fatalf("claim row: %v, %v", ok, err)
			}
			// The installed file; with taint, another Mac rewrote it in place
			// (same inode, same length) during the outage.
			body := []byte("our-complete-body")
			switch {
			case tc.tainted:
				body = []byte("THEIR-edit-body!!")
			case !tc.staged:
				body = []byte("our-comp") // an in-place rewrite that stopped part way
			}
			dest := writeBackendFile(t, d, name, body)
			var st unix.Stat_t
			if err := unix.Lstat(dest, &st); err != nil {
				t.Fatal(err)
			}
			claim := spool.Meta().ClaimDrainBase
			if tc.staged {
				claim = spool.Meta().ClaimDrainBaseStaged
			}
			if err := claim(e.ID(), uint64(st.Ino)); err != nil {
				t.Fatal(err)
			}
			row := rowOf(t, spool, e)
			_, accepted := d.rowDrainBase(row)
			lease := leaseDecision{}
			if tc.tainted {
				lease.foreignBefore = accepted + 1
			}
			target, ok := d.openDrainTarget(row, dest, lease, time.Time{})
			if !ok || target == nil {
				t.Fatalf("decision disposed of the row: %v", ok)
			}
			if target.f != nil {
				defer target.f.Close()
			}
			if target.stage != nil {
				defer target.stage.discard()
			}
			switch {
			case tc.wantAdopt:
				if target.adoptIno != uint64(st.Ino) || target.f != nil || target.stage != nil {
					t.Fatalf("want adoption of inode %d without writing, got %+v", st.Ino, target)
				}
			case tc.tainted:
				if target.stage == nil || target.conflict == "" {
					t.Fatalf("tainted: want a conflict copy, got %+v", target)
				}
			default:
				if target.f == nil || target.stage != nil || target.adoptIno != 0 {
					t.Fatalf("plain claim: want an in-place rewrite, got %+v", target)
				}
			}
			if !tc.wantAdopt && tc.tainted {
				if got, _ := os.ReadFile(dest); !bytes.Equal(got, body) {
					t.Fatalf("the other Mac's edit was overwritten: %q", got)
				}
			}
		})
	}
}

// Control: a file another writer put at the free name while this row
// drained still gets a conflict copy (it has another inode).
func TestDrainInstallForeignNameStillConflicts(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	name := "/proj/shot/other.mov"
	dest := filepath.Join(d.fuseRoot, name)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	e := writeSpoolEntry(t, spool, name, bytes.Repeat([]byte("mine"), 2<<20/4))
	orig := drainRenameNoReplace
	drainRenameNoReplace = func(a, b string) error {
		if b == dest {
			if _, err := os.Lstat(dest); os.IsNotExist(err) {
				if werr := os.WriteFile(dest, []byte("someone else's"), 0o644); werr != nil {
					t.Error(werr)
				}
			}
		}
		return orig(a, b)
	}
	t.Cleanup(func() { drainRenameNoReplace = orig })
	probeDrainLoop(d)
	if got, _ := os.ReadFile(dest); string(got) != "someone else's" {
		t.Fatalf("the foreign file was replaced: %q", got)
	}
	row, _ := spool.Meta().Get(e.ID())
	if !strings.Contains(row.LastError, conflictCopyMarker) {
		t.Fatalf("no conflict copy for a foreign file: state %s err %q", row.DrainState, row.LastError)
	}
}

// An older row's file at the name is rewritten in place only while no other
// Mac held the project: with taint it may hold that Mac's edit.
func TestOlderRowInstalledDestRespectsTaint(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	name := filepath.Join("Film", "Project", "older.mov")
	olderID := seedLegacyUnfinishedPredecessor(t, spool, name)
	newer := writeSpoolEntry(t, spool, name, []byte("second save"))
	restoreLegacyUnfinishedPredecessor(t, spool, olderID)
	dest := writeBackendFile(t, d, name, []byte("first save"))
	var st unix.Stat_t
	if err := unix.Lstat(dest, &st); err != nil {
		t.Fatal(err)
	}
	d.recordInstalledIdentity(olderID, uint64(st.Dev), uint64(st.Ino)) //nolint:unconvert // int32 on Darwin
	row := rowOf(t, spool, newer)
	if !d.olderRowInstalledDest(row, dest, false) {
		t.Fatal("untainted: the older row's own file was not recognised")
	}
	if d.olderRowInstalledDest(row, dest, true) {
		t.Fatal("tainted: an older row's inode was trusted although another Mac held the project")
	}
}

// failRenameUnlanded makes every install rename to dest fail with err
// without moving anything (the request never reached the server). It
// returns a pointer to the count.
func failRenameUnlanded(t *testing.T, dest string, err error, times int) *int {
	t.Helper()
	n := 0
	orig := drainRenameNoReplace
	drainRenameNoReplace = func(a, b string) error {
		if b == dest && n < times {
			n++
			return err
		}
		return orig(a, b)
	}
	t.Cleanup(func() { drainRenameNoReplace = orig })
	return &n
}

// Re-review MEDIUM-1 (1): an unfinished older row of ours takes the name
// while this row stages; the EEXIST sends us to retry in place. The staged
// claim written before the rename must be rolled back, or the retry refuses
// the in-place rewrite and the newer save ends as a conflict copy.
func TestDrainNewNameTakenByOwnOlderRowEndsDone(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	const name = "/proj/save.mov"
	dest := filepath.Join(d.fuseRoot, name)
	olderID := seedLegacyUnfinishedPredecessor(t, spool, name)
	e := writeSpoolEntry(t, spool, name, []byte("newer save"))
	restoreLegacyUnfinishedPredecessor(t, spool, olderID)
	drainPreInstallHook = func(*metadata.SpoolRow) {
		if _, err := os.Lstat(dest); err == nil {
			return
		}
		if err := os.WriteFile(dest, []byte("older save"), 0o644); err != nil {
			t.Error(err)
		}
		fi, _ := os.Lstat(dest)
		st := fi.Sys().(*syscall.Stat_t)
		d.recordInstalledIdentity(olderID, uint64(st.Dev), uint64(st.Ino))
	}
	t.Cleanup(func() { drainPreInstallHook = nil })

	drainOnce(t, d)
	if base, _, err := spool.Meta().DrainBase(e.ID()); err != nil || base.Kind != metadata.SpoolBaseAbsent {
		t.Fatalf("base after the EEXIST = %+v (%v), want absent (staged claim rolled back)", base, err)
	}
	probeDrainLoop(d)
	assertOnlyOwnFile(t, spool, e, dest, []byte("newer save"))
}

// Re-review MEDIUM-1 (2): the rename fails with EIO and never landed (the
// roaming case). The claim is rolled back, so the retry is not paused as
// "server copy incomplete", and no dead inode stays recorded (LOW-2:
// directInode would reuse it).
func TestDrainInstallUnlandedRenameRestoresBase(t *testing.T) {
	spool, d, e, dest, payload := lostReplyFixture(t)
	n := failRenameUnlanded(t, dest, syscall.EIO, 1)
	ctx, cancel := context.WithTimeout(context.Background(), drainPassDeadline)
	d.DrainOnceForTest(ctx)
	cancel()
	if *n != 1 {
		t.Fatalf("failed renames = %d, want 1", *n)
	}
	base, _, err := spool.Meta().DrainBase(e.ID())
	if err != nil || base.Kind != metadata.SpoolBaseAbsent {
		t.Fatalf("base after an unlanded rename = %+v (%v), want absent", base, err)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(row.LastError, serverIncompleteMarker) || strings.Contains(row.LastError, serverPreviousMarker) || !row.PauseSince.IsZero() {
		t.Fatalf("an unlanded rename paused the row: %q (paused since %v)", row.LastError, row.PauseSince)
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		t.Fatalf("name exists after an unlanded rename: %v", err)
	}
	probeDrainLoop(d)
	assertOnlyOwnFile(t, spool, e, dest, payload)
}

// Re-review MEDIUM-1 (3): a temp file's install fails without landing, then
// the app renames the temp over an existing file (atomic save). The rename
// must record a replace base, not keep a dead claim that later becomes a
// conflict copy.
func TestDrainAtomicSaveAfterFailedInstallReplaces(t *testing.T) {
	fixConflictClock(t)
	spool, d := newTestDrainer(t, DrainerConfig{MaxAttempts: 5, BackoffBase: time.Millisecond})
	const name = "proj/edit.prproj"
	tmpName := name + ".tmp"
	dest := writeBackendFile(t, d, name, []byte("version 1 on the server"))
	mirrorSeesBackend(t, spool, name, dest)
	payload := bytes.Repeat([]byte("v2"), 2<<20/2) // 2 MiB: the stage path
	e := writeSpoolEntry(t, spool, tmpName, payload)
	tmpDest := filepath.Join(d.fuseRoot, tmpName)
	failRenameUnlanded(t, tmpDest, syscall.EIO, 1)
	ctx, cancel := context.WithTimeout(context.Background(), drainPassDeadline)
	d.DrainOnceForTest(ctx)
	cancel()

	if _, _, err := spool.MigrateForRename(tmpName, name); err != nil {
		t.Fatal(err)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if base, _ := d.rowDrainBase(row); base.Kind != metadata.SpoolBaseReplace {
		t.Fatalf("atomic save after a failed install recorded base %v, want replace", base.Kind)
	}
	probeDrainLoop(d)
	assertOnlyOwnFile(t, spool, e, dest, payload)
}
