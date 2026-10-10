package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// Epochs from separate project counters are not comparable. Exercise the
// durable migration for each state, including a draining row's replacement ID.
func TestRenameProjectEpochScope(t *testing.T) {
	present := metadata.SpoolBase{Kind: metadata.SpoolBasePresent, Ino: 42, Len: 3, MtimeNs: 123}
	for _, tc := range []struct {
		name, from, to, suffix string
		base                   metadata.SpoolBase
		wantEpoch              uint64
	}{
		{"same project temp keeps history", "Film/A/tmp", "Film/A/edit", "", metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, 100},
		{"cross project temp binds destination", "Film/A/tmp", "Film/B/edit", "", metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, 3},
		{"cross project existing file", "Film/A/file", "Film/B/file", "", present, 0},
		{"cross project weak file", "Film/A/file", "Film/B/file", "", metadata.SpoolBase{Kind: metadata.SpoolBaseWeak}, 0},
		{"cross project claimed file", "Film/A/file", "Film/B/file", "", metadata.SpoolBase{Kind: metadata.SpoolBaseClaimed, Ino: 42}, 0},
		{"cross project prior replacement", "Film/A/file", "Film/B/file", "", metadata.SpoolBase{Kind: metadata.SpoolBaseReplace, Ino: 42}, 0},
		{"cross project directory child", "Film/A/dir", "Film/B/dir", "/file", present, 0},
		{"project root child", "Film/A", "Film/B", "/dir/file", present, 0},
		{"same project directory child", "Film/A/dir", "Film/A/new", "/file", present, 100},
		{"unscoped temp enters project", "tmp", "Film/B/edit", "", metadata.SpoolBase{Kind: metadata.SpoolBaseAbsent}, 3},
		{"existing file leaves project", "Film/A/file", "file", "", present, 0},
	} {
		for _, state := range []metadata.DrainState{metadata.DrainWriting, metadata.DrainReady, metadata.DrainDraining} {
			t.Run(tc.name+"/"+string(state), func(t *testing.T) {
				spool, _ := newTestDrainer(t, DrainerConfig{})
				probes := 0
				probe := spoolBaseProbe(func(name string) (metadata.SpoolBase, uint64) {
					probes++
					if name == tc.from+tc.suffix {
						return tc.base, 100
					}
					return present, 3
				})
				spool.baseProbe.Store(&probe)
				e, err := spool.OpenWrite(tc.from + tc.suffix)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := e.WriteAt([]byte("new"), 0); err != nil {
					t.Fatal(err)
				}
				e.ReleaseHandle()
				oldID := e.ID()
				if state != metadata.DrainWriting {
					if err := e.Finalize(); err != nil {
						t.Fatal(err)
					}
				}
				if state == metadata.DrainDraining {
					if ok, err := spool.Meta().MarkDraining(oldID); err != nil || !ok {
						t.Fatal(ok, err)
					}
				}
				if before := rowOf(t, spool, e); before.DrainState != state {
					t.Fatalf("fixture state=%s want=%s", before.DrainState, state)
				}
				probes = 0
				if n, requeued, err := spool.MigrateForRename(tc.from, tc.to); err != nil || n != 1 || requeued != (state == metadata.DrainDraining) {
					t.Fatalf("migrate: n=%d requeued=%v err=%v", n, requeued, err)
				}
				wantProbes := 0
				if tc.suffix == "" {
					wantProbes = 1
				}
				if probes != wantProbes {
					t.Fatalf("rename probes=%d want=%d", probes, wantProbes)
				}
				row, err := spool.Meta().LookupByPath(tc.to + tc.suffix)
				if err != nil {
					t.Fatal(err)
				}
				if (row.ID != oldID) != (state == metadata.DrainDraining) {
					t.Fatalf("old/new IDs %d/%d in state %s", oldID, row.ID, state)
				}
				_, epoch, err := spool.Meta().DrainBase(row.ID)
				if err != nil || epoch != tc.wantEpoch {
					t.Fatalf("persisted epoch=%d want=%d err=%v", epoch, tc.wantEpoch, err)
				}
			})
		}
	}
}

func renameLeaseFixture(t *testing.T, spool *SpoolStore, d *Drainer) (*fakeLeaseServer, *ProjectLeaseClient, *time.Time) {
	t.Helper()
	f := newFakeLeaseServer(t)
	f.mu.Lock()
	f.project("Film/A").epoch = 99
	f.mu.Unlock()
	now := time.Unix(1_800_000_000, 0)
	c := newTestLeaseClient(t, f, spool, func() time.Time { return now })
	d.SetProjectLeases(c)
	for project, epoch := range map[string]uint64{"Film/A": 100, "Film/B": 1} {
		if dec, err := c.EnsureForDrain(project); err != nil || !dec.held || dec.epoch != epoch {
			t.Fatalf("initial %s lease %+v: %v", project, dec, err)
		}
	}
	return f, c, &now
}

func foreignRewriteRestoringMtime(t *testing.T, f *fakeLeaseServer, now *time.Time, dest, project string, body []byte) {
	t.Helper()
	before := backendIdentity(t, dest)
	f.setHolder(project, "peer@Other-Mac")
	if err := os.WriteFile(dest, body, 0o644); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(0, before.MtimeNs)
	if err := os.Chtimes(dest, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	after := backendIdentity(t, dest)
	if after.Ino != before.Ino || after.Len != before.Len || after.MtimeNs != before.MtimeNs {
		t.Fatalf("control did not restore the vulnerable tuple: before=%+v after=%+v", before, after)
	}
	f.free(project)
	*now = now.Add(time.Hour)
}

func assertRenameConflictBytes(t *testing.T, spool *SpoolStore, d *Drainer, name, dest string, ours, theirs []byte, kind metadata.SpoolBaseKind) {
	t.Helper()
	row, err := spool.Meta().LookupByPath(name)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := d.rowDrainBase(row)
	current := backendIdentity(t, dest)
	if base.Kind != kind || base.Len != current.Len ||
		(kind == metadata.SpoolBaseWeak && base.MtimeNs/1e9 != current.MtimeNs/1e9) ||
		(kind != metadata.SpoolBaseWeak && (base.Ino != current.Ino || base.MtimeNs != current.MtimeNs)) {
		t.Fatalf("fixture must match the vulnerable base tuple: base=%+v current=%+v", base, current)
	}
	if dec, err := d.ProjectLeases().EnsureForDrain(projectOf(name)); err != nil || !dec.held || dec.foreignBefore != 3 {
		t.Fatalf("fixture must establish foreign boundary3: %+v err=%v", dec, err)
	}
	drainOnce(t, d)
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, theirs) {
		t.Fatalf("foreign destination changed: %q err=%v", got, err)
	}
	if got, err := os.ReadFile(conflictCopyOf(d, name)); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("accepted save not preserved in conflict copy: %q err=%v", got, err)
	}
	rows, err := spool.Meta().ListAll()
	if err != nil || len(rows) != 1 || rows[0].DrainState != metadata.DrainFailed || !isConflictCopyFailure(rows[0].LastError) {
		t.Fatalf("conflict disposition: %+v err=%v", rows, err)
	}
}

// A source counter of 100 must not bypass foreignBefore=3 in the destination.
// Both the offline weak tuple and the online moved tuple can match peer edits.
func TestRenameCrossProjectPreservesForeignBytes(t *testing.T) {
	for _, offline := range []bool{true, false} {
		t.Run(map[bool]string{true: "offline weak", false: "online moved"}[offline], func(t *testing.T) {
			fixConflictClock(t)
			spool, d := newTestDrainer(t, DrainerConfig{})
			f, _, now := renameLeaseFixture(t, spool, d)
			const from, to = "Film/A/dir", "Film/B/dir"
			const suffix = "/edit.txt"
			src := writeBackendFile(t, d, from+suffix, []byte("oldbytes"))
			mirrorSeesBackend(t, spool, from+suffix, src)
			ours, theirs := []byte("our new save"), []byte("peerdata")
			if offline {
				withOffline(t, func() { writeSpoolEntry(t, spool, from+suffix, ours) })
			} else {
				writeSpoolEntry(t, spool, from+suffix, ours)
			}
			if _, _, err := spool.MigrateForRename(from, to); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(d.fuseRoot, "Film/B"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(filepath.Dir(src), filepath.Join(d.fuseRoot, to)); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(d.fuseRoot, to+suffix)
			foreignRewriteRestoringMtime(t, f, now, dest, "Film/B", theirs)
			kind := metadata.SpoolBaseMoved
			if offline {
				kind = metadata.SpoolBaseWeak
			}
			assertRenameConflictBytes(t, spool, d, to+suffix, dest, ours, theirs, kind)
		})
	}
}

func TestAtomicRenameForeignEpochRestoredAttributes(t *testing.T) {
	for _, sameProject := range []bool{false, true} {
		t.Run(map[bool]string{false: "cross project", true: "same project"}[sameProject], func(t *testing.T) {
			fixConflictClock(t)
			spool, d := newTestDrainer(t, DrainerConfig{})
			f, c, now := renameLeaseFixture(t, spool, d)
			from, to := "Film/A/edit.tmp", "Film/B/edit.txt"
			if sameProject {
				from = "Film/B/edit.tmp"
			}
			dest := writeBackendFile(t, d, to, []byte("oldbytes"))
			mirrorSeesBackend(t, spool, to, dest)
			ours, theirs := []byte("our atomic save"), []byte("peerdata")
			writeSpoolEntry(t, spool, from, ours)
			if sameProject {
				// Renew before rename: migration must not erase the row's
				// earlier tenure just because the current lease is newer.
				foreignRewriteRestoringMtime(t, f, now, dest, "Film/B", theirs)
				if dec, err := c.EnsureForDrain("Film/B"); err != nil || !dec.held || dec.foreignBefore != 3 {
					t.Fatalf("foreign reacquire %+v: %v", dec, err)
				}
			}
			if _, _, err := spool.MigrateForRename(from, to); err != nil {
				t.Fatal(err)
			}
			if !sameProject {
				foreignRewriteRestoringMtime(t, f, now, dest, "Film/B", theirs)
			}
			assertRenameConflictBytes(t, spool, d, to, dest, ours, theirs, metadata.SpoolBaseReplace)
		})
	}
}

// The destination grant remains usable when no foreign holder intervenes.
func TestAtomicRenameCrossProjectContinuousDestinationLease(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	renameLeaseFixture(t, spool, d)
	const from, to = "Film/A/edit.tmp", "Film/B/edit.txt"
	dest := writeBackendFile(t, d, to, []byte("oldbytes"))
	mirrorSeesBackend(t, spool, to, dest)
	ours := []byte("our atomic save")
	e := writeSpoolEntry(t, spool, from, ours)
	if _, _, err := spool.MigrateForRename(from, to); err != nil {
		t.Fatal(err)
	}
	drainOnce(t, d)
	if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, ours) {
		t.Fatalf("ordinary atomic save did not publish: %q err=%v", got, err)
	}
	row, err := spool.Meta().Get(e.ID())
	if err != nil || row.DrainState != metadata.DrainDone {
		t.Fatalf("ordinary atomic save did not complete: %+v err=%v", row, err)
	}
}
