package nfs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// These exercise the handler's CREATE/WRITE/RENAME and READ shapes over local
// fixture directories. They do not claim live Finder or mount acceptance.
func reopenRecoveryFixture(t *testing.T, old *juiceFS, spool *SpoolStore, fuseRoot string) (*juiceFS, *SpoolStore) {
	t.Helper()
	var seq int
	var name, dbPath string
	if err := old.handler.store.DB().QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &dbPath); err != nil || dbPath == "" {
		t.Fatalf("find persisted metadata DB: %q %v", dbPath, err)
	}
	if !old.handler.StopHandler() {
		t.Fatal("old handler did not stop before metadata reopen")
	}
	if err := old.handler.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	meta := metadata.NewSpoolStore(store.DB())
	store.SetSpoolStore(meta) // same owner ordering as the production bridge
	recovered, err := NewSpoolStore(spool.root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, fuseRoot)
	t.Cleanup(func() { h.StopHandler() })
	h.SetSpool(recovered, nil)
	return &juiceFS{handler: h}, recovered
}

func TestAtomicSaveRecoveredReadUsesSavedBytes(t *testing.T) {
	jfs, spool, _, dest, _, saved := atomicSaveFixture(t, 64<<10)
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d rows", n)
	}
	jfs, recovered := reopenRecoveryFixture(t, jfs, spool, filepath.Dir(dest))
	mirror := jfs.handler.store.LookupByPath("edit.prproj")
	entry, ok := recovered.LookupActive("edit.prproj")
	if mirror == nil || !ok || entry.Inode() != mirror.Inode || entry.Inode() == 0 {
		t.Fatalf("recovered inode lost mirror authority: mirror=%+v entry=%v", mirror, entry)
	}
	f, err := jfs.OpenFile("edit.prproj", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, err := io.ReadAll(io.LimitReader(f, int64(len(saved)+1)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, saved) {
		t.Fatalf("recovered read used earlier server bytes: got=%d saved=%d", len(got), len(saved))
	}
}

func TestValidEmptySaveRecoversAsEmpty(t *testing.T) {
	jfs, spool, _, fuseRoot := newSpoolWiredHandlerNoDrain(t)
	f, err := jfs.Create("empty.prproj")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	spool.sweepOnce(0)
	jfs, recovered := reopenRecoveryFixture(t, jfs, spool, fuseRoot)
	r, err := recovered.Meta().LookupByPath("empty.prproj")
	if err != nil || r.Size != 0 || len(r.SHA256) != 32 {
		t.Fatalf("empty saved row: %+v %v", r, err)
	}
	read, err := jfs.OpenFile("empty.prproj", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	got, err := io.ReadAll(read)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty recovered read: %d %v", len(got), err)
	}
}

func TestMissingDigestCannotPublishAfterRecoveryOrRetry(t *testing.T) {
	jfs, spool, d, dest, _, saved := atomicSaveFixture(t, 64<<10)
	old, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if n := spool.sweepOnce(0); n != 1 {
		t.Fatalf("finalized %d", n)
	}
	r, err := spool.Meta().LookupByPath("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	unknown := bytes.Repeat([]byte{0x5a}, len(saved))
	if err := os.WriteFile(r.SpoolFile, unknown, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := jfs.handler.store.DB().Exec(`UPDATE spool_entries SET sha256=NULL WHERE id=?`, r.ID); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewSpoolStore(spool.root, 0, spool.Meta())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err = recovered.Meta().Get(r.ID)
	if err != nil || r.QualificationHold == "" || r.DrainState != metadata.DrainFailed || !recovered.ReadFault(r.NFSPath) {
		t.Fatalf("unqualified source not durably held: %+v %v", r, err)
	}
	if n, err := recovered.RetryFailed(); err != nil || n != 0 {
		t.Fatalf("explicit retry released unproved source: %d %v", n, err)
	}
	if n, err := recovered.RetryFailedAutomatic(); err != nil || n != 0 {
		t.Fatalf("automatic retry released unproved source: %d %v", n, err)
	}
	dr, err := NewDrainer(recovered, DrainerConfig{FuseRoot: d.fuseRoot})
	if err != nil {
		t.Fatal(err)
	}
	jfs.handler.SetSpool(recovered, dr)
	drainAllForTest(t, dr)
	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatalf("unqualified bytes reached server: %v", err)
	}
	if rows, err := recovered.Meta().ListReady(1); err != nil || len(rows) != 0 {
		t.Fatalf("unqualified row dispatchable: %d %v", len(rows), err)
	}
}

func TestStaleQuarantineCannotMoveRenameSuccessorSource(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	const oldPath, newPath = "/proj/old.mov", "/proj/new.mov"
	e := writeSpoolEntry(t, spool, oldPath, bytes.Repeat([]byte{0x39}, 4096))
	r, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := spool.Meta().MarkDraining(r.ID); err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	if n, _, err := spool.MigrateForRename(oldPath, newPath); err != nil || n != 1 {
		t.Fatalf("rename migration: %d %v", n, err)
	}
	if err := spool.QuarantineDrain(r.ID, oldPath, r.SpoolFile, r.Size, "stale hash mismatch"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.SpoolFile); err != nil {
		t.Fatalf("successor source moved: %v", err)
	}
	successor, err := spool.Meta().LookupByPath(newPath)
	if err != nil || successor.ID == r.ID || successor.SpoolFile != r.SpoolFile {
		t.Fatalf("successor lost source custody: %+v %v", successor, err)
	}
}

func TestClaimedFailureCleanupDoesNotReverseEntryAndMetadataLocks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		quarantine bool
	}{{"permanent", false}, {"quarantine", true}} {
		t.Run(tc.name, func(t *testing.T) {
			spool, drainer := newTestDrainer(t, DrainerConfig{})
			const path = "proj/held-entry.mov"
			e := writeSpoolEntry(t, spool, path, []byte("saved local bytes"))
			row, err := spool.Meta().Get(e.ID())
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
				t.Fatalf("claim: %v %v", ok, err)
			}

			// A continuation writer can own this lock before asking metadata
			// for authority. The failure callback must use only the atomic ID.
			e.mu.Lock()
			failureDone := make(chan error, 1)
			go func() {
				if tc.quarantine {
					failureDone <- spool.QuarantineDrain(row.ID, path, row.SpoolFile, row.Size, "integrity mismatch")
				} else {
					drainer.failPermanent(row, "terminal failure")
					failureDone <- nil
				}
			}()
			inspectDone := make(chan error, 1)
			startedInspect := false
			failureFinished, inspectFinished := false, false
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			stopAt := time.Now().Add(time.Second)
			var waitErr string
			for !failureFinished || !inspectFinished {
				if !startedInspect {
					current, err := spool.Meta().Get(row.ID) // read does not take spool.writeMu
					if err != nil {
						waitErr = err.Error()
						break
					}
					if current.DrainState == metadata.DrainFailed {
						startedInspect = true
						go func() {
							_, err := spool.Meta().InspectSpoolContinuation(row.ID, path, row.SpoolFile)
							inspectDone <- err
						}()
					} else {
						if time.Now().After(stopAt) {
							waitErr = "claimed failure did not commit within one second"
							break
						}
						time.Sleep(time.Millisecond)
						continue
					}
				}
				select {
				case err := <-failureDone:
					failureFinished = true
					if err != nil {
						waitErr = err.Error()
					}
				case err := <-inspectDone:
					inspectFinished = true
					if err != nil {
						waitErr = err.Error()
					}
				case <-deadline.C:
					waitErr = "failure cleanup or continuation inspection waited for entry.mu"
				}
				if waitErr != "" {
					break
				}
			}
			// Always release the fixture lock and join both workers, including on
			// the defective implementation's timeout path.
			e.mu.Unlock()
			if !failureFinished {
				select {
				case <-failureDone:
				case <-time.After(time.Second):
					t.Fatal("failure worker did not stop")
				}
			}
			if startedInspect && !inspectFinished {
				select {
				case <-inspectDone:
				case <-time.After(time.Second):
					t.Fatal("inspection worker did not stop")
				}
			}
			if waitErr != "" {
				t.Fatal(waitErr)
			}
		})
	}
}

func reopenBareSpoolForAuthorityTest(t *testing.T, old *SpoolStore) *SpoolStore {
	t.Helper()
	owner := old.Meta().Owner()
	var seq int
	var name, dbPath string
	if err := owner.DB().QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &dbPath); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	store.SetSpoolStore(meta)
	reopened, err := NewSpoolStore(old.root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.RecoverOnBoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	return reopened
}

func TestDeletedWinnerCannotReviveRetiredFailureAfterRestart(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	const path = "proj/replaced.bin"
	old := writeSpoolEntry(t, spool, path, []byte("older saved image"))
	if changed, err := spool.Meta().MarkFailed(old.ID(), "prior failure"); err != nil || !changed {
		t.Fatalf("fail old: %v %v", changed, err)
	}
	spool.EvictIndex(path, old.ID())
	spool.blockRead(path, old.ID())
	newer := writeSpoolEntry(t, spool, path, []byte("newer saved image"))
	if changed, err := spool.cancelForDelete(path); err != nil || !changed {
		t.Fatalf("cancel winner: %v %v", changed, err)
	}
	if spool.ReadFault(path) {
		t.Fatal("retired older fault still blocks namespace")
	}
	r, err := spool.Meta().Get(old.ID())
	if err != nil || r.RetiredBy != newer.ID() {
		t.Fatalf("retirement before reboot: %+v %v", r, err)
	}
	reopened := reopenBareSpoolForAuthorityTest(t, spool)
	r, err = reopened.Meta().Get(old.ID())
	if err != nil || r.RetiredBy != newer.ID() || r.DrainState != metadata.DrainFailed {
		t.Fatalf("retirement after reboot: %+v %v", r, err)
	}
	if n, err := reopened.RetryFailedAutomatic(); err != nil || n != 0 || reopened.ReadFault(path) {
		t.Fatalf("retired history regained authority: %d %v", n, err)
	}
}

func TestFailedQuarantineMoveKeepsIntegrityHoldAfterRestart(t *testing.T) {
	spool, _ := newTestDrainer(t, DrainerConfig{})
	const path = "proj/corrupt.bin"
	e := writeSpoolEntry(t, spool, path, []byte("valid original bytes"))
	r, err := spool.Meta().Get(e.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.SpoolFile, []byte("known corrupt bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(spool.root, "quarantine"), []byte("blocks directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := spool.Meta().MarkDraining(r.ID); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := spool.QuarantineDrain(r.ID, path, r.SpoolFile, r.Size, "SHA mismatch"); err == nil {
		t.Fatal("quarantine move unexpectedly succeeded")
	}
	reopened := reopenBareSpoolForAuthorityTest(t, spool)
	r, err = reopened.Meta().Get(r.ID)
	if err != nil || r.QualificationHold == "" || r.DrainState != metadata.DrainFailed {
		t.Fatalf("integrity hold lost: %+v %v", r, err)
	}
	for _, retry := range []func() (int, error){reopened.RetryFailed, reopened.RetryFailedAutomatic} {
		if n, err := retry(); err != nil || n != 0 {
			t.Fatalf("known corrupt source retried: %d %v", n, err)
		}
	}
}

func TestTerminalAtomicSaveRefusesOldServerRead(t *testing.T) {
	jfs, spool, d, dest, _, saved := atomicSaveFixture(t, 64<<10)
	spool.sweepOnce(0)
	r, err := spool.Meta().LookupByPath("edit.prproj")
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := spool.Meta().MarkDraining(r.ID); err != nil || !claimed {
		t.Fatalf("claim before terminal worker failure: %v %v", claimed, err)
	}
	d.failPermanent(r, "publication cap reached; saved bytes retained")
	if _, err := jfs.OpenFile("edit.prproj", os.O_RDONLY, 0); err != syscall.EIO {
		t.Fatalf("terminal save read = %v; want explicit EIO, not previous server version", err)
	}
	if got, err := os.ReadFile(r.SpoolFile); err != nil || !bytes.Equal(got, saved) {
		t.Fatalf("local saved bytes lost: %v", err)
	}
	if got, err := os.ReadFile(dest); err != nil || bytes.Equal(got, saved) {
		t.Fatalf("previous server copy unexpectedly replaced: %v", err)
	}
	if _, err := jfs.OpenFile(filepath.Base(dest), os.O_RDONLY, 0); err != syscall.EIO {
		t.Fatalf("second read = %v; want stable EIO", err)
	}
}

func TestPausedCopyRetainsAuthorityThroughProjectHold(t *testing.T) {
	setDrainPauseForTest(t, nil)
	spool, d, id, _, saved, restore := inPlaceFailureFixture(t, DrainerConfig{MaxAttempts: 3})
	for i := 0; i < 4; i++ {
		drainAllForTest(t, d)
	}
	r, err := spool.Meta().Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if r.DrainAttempts < 3 || r.PauseSince.IsZero() || r.DrainState != metadata.DrainReady {
		t.Fatalf("precondition: %+v", r)
	}
	if wait := time.Until(r.RetryAfter); wait > 0 {
		time.Sleep(wait + time.Millisecond)
	}
	if ok, err := spool.Meta().MarkDraining(id); err != nil || !ok {
		t.Fatalf("claim paused: %v %v", ok, err)
	}
	if ok, err := spool.Meta().HoldForProject(id, "proj/F2", "waiting_project: lease held"); err != nil || !ok {
		t.Fatalf("hold: %v %v", ok, err)
	}
	if n, err := spool.Meta().ReleaseProjectHold("proj/F2"); err != nil || n != 1 {
		t.Fatalf("release: %d %v", n, err)
	}
	if after, err := spool.Meta().Get(id); err != nil || !after.PauseSince.Equal(r.PauseSince) {
		t.Fatalf("hold erased pause authority: %+v %v", after, err)
	}
	restore()
	drainAllForTest(t, d)
	r, err = spool.Meta().Get(id)
	if err != nil || r.DrainState != metadata.DrainDone {
		t.Fatalf("saved copy failed after hold: %+v %v", r, err)
	}
	got, err := os.ReadFile(d.fuseRoot + "/proj/F2/edit.bin")
	if err != nil || !bytes.Equal(got, saved) {
		t.Fatalf("published wrong bytes: %v", err)
	}
}
