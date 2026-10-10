package nfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// A valid completion releases the spool hold before its path callback runs.
// A new writer can therefore own the same path by the time the callback runs.
func TestCompletedDrainCallbackPreservesSuccessor(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline", true: "batch"}[batch], func(t *testing.T) {
			var jfs *juiceFS
			var spool *SpoolStore
			var drainer *Drainer
			var fuseRoot string
			if batch {
				jfs, spool, drainer, fuseRoot = newBatchWiredHandlerNoDispatch(t, 1)
			} else {
				jfs, spool, drainer, fuseRoot = newSpoolWiredHandlerNoDrain(t)
			}
			const name = "clip.mov"
			oldBody := bytes.Repeat([]byte("old"), 4096)
			newBody := []byte("successor bytes")
			foreignPath := t.TempDir() + "/foreign.mov"
			if err := os.WriteFile(foreignPath, oldBody, 0600); err != nil {
				t.Fatal(err)
			}
			simulateNFSCreateThenWrite(t, jfs, name, oldBody, len(oldBody))
			spool.sweepOnce(0)

			called := false
			var capturedItem metadata.DrainCommitItem
			var capturedResult metadata.DrainCommitResult
			realEffects := drainer.onDrainEffects
			drainer.SetOnDrainEffects(func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
				capturedItem, capturedResult = item, receipt
				called = true
				// The FULL hold is live while effects run; even direct Store
				// callers cannot advance this generation's mode.
				if err := jfs.handler.store.UpdateMode(name, 0444); err == nil {
					t.Error("mode changed during held completion effects")
				}
				foreign, err := os.OpenFile(foreignPath, os.O_RDWR, 0)
				if err != nil {
					t.Error(err)
					return err
				}
				foreignErr := jfs.handler.applyLegacyDrainEffects(item, receipt, foreign, body)
				if closeErr := foreign.Close(); closeErr != nil {
					t.Error(closeErr)
				}
				if foreignErr == nil {
					t.Error("live receipt chmoded foreign descriptor")
				}
				if fi, statErr := os.Stat(foreignPath); statErr != nil || fi.Mode().Perm() != 0600 {
					t.Errorf("foreign mode changed before refusal: info=%v err=%v", fi, statErr)
				}
				return realEffects(item, receipt, fd, body)
			})
			drainAllForTest(t, drainer)
			if !called {
				t.Fatal("valid completion effects did not run")
			}
			if err := jfs.Remove(name); err != nil {
				t.Fatalf("remove completed predecessor: %v", err)
			}
			simulateNFSCreateThenWrite(t, jfs, name, newBody, len(newBody))
			if err := os.WriteFile(fuseRoot+"/"+name, newBody, 0600); err != nil {
				t.Fatal(err)
			}
			successor := jfs.handler.store.LookupByPath(name)
			active, ok := spool.LookupActive(name)
			if successor == nil || !ok {
				t.Fatal("successor was not established")
			}
			successorID := active.ID()
			row, err := spool.Meta().Get(successorID)
			if err != nil {
				t.Fatal(err)
			}
			successorState := row.DrainState
			successorCapacity, _ := spool.Capacity()
			backingBefore, backingBeforeErr := os.ReadFile(fuseRoot + "/" + name)
			fi, err := os.Stat(fuseRoot + "/" + name)
			if err != nil {
				t.Fatal(err)
			}
			backingBeforeMode := fi.Mode().Perm()
			replayFD, err := os.OpenFile(fuseRoot+"/"+name, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			replayErr := jfs.handler.applyLegacyDrainEffects(capturedItem, capturedResult, replayFD, nil)
			if err := replayFD.Close(); err != nil {
				t.Fatal(err)
			}
			if replayErr == nil {
				t.Fatal("retired predecessor receipt replayed effects")
			}
			entry := jfs.handler.store.LookupByPath(name)
			if entry == nil || entry.Size != successor.Size || entry.Version != successor.Version || entry.LocalOnly != successor.LocalOnly || !entry.Mtime.Equal(successor.Mtime) {
				t.Errorf("successor mirror changed by predecessor callback: before=%+v after=%+v", successor, entry)
			}
			active, ok = spool.LookupActive(name)
			if !ok || active.ID() != successorID || active.WrittenEnd() != int64(len(newBody)) {
				t.Errorf("successor spool index changed: entry=%+v present=%t", active, ok)
			} else if got, err := os.ReadFile(active.SpoolFilePath()); err != nil || !bytes.Equal(got, newBody) {
				t.Errorf("successor spool bytes=%q err=%v", got, err)
			}
			if row, err := spool.Meta().Get(successorID); err != nil || row.DrainState != successorState {
				t.Errorf("successor queue row=%+v err=%v, wanted state %s", row, err, successorState)
			}
			if used, _ := spool.Capacity(); used != successorCapacity {
				t.Errorf("successor reservation=%d want %d", used, successorCapacity)
			}
			backingAfter, backingAfterErr := os.ReadFile(fuseRoot + "/" + name)
			if !bytes.Equal(backingBefore, backingAfter) || errors.Is(backingBeforeErr, os.ErrNotExist) != errors.Is(backingAfterErr, os.ErrNotExist) {
				t.Errorf("successor backing changed: before=%q (%v), after=%q (%v)", backingBefore, backingBeforeErr, backingAfter, backingAfterErr)
			}
			if fi, err := os.Stat(fuseRoot + "/" + name); err != nil {
				t.Errorf("successor backing stat: %v", err)
			} else if fi.Mode().Perm() != backingBeforeMode {
				t.Errorf("successor backing mode changed: before=%#o after=%#o", backingBeforeMode, fi.Mode().Perm())
			}
		})
	}
}

// A coalesced flush larger than namespace admission capacity must progress in
// cohorts, without self-refusing and re-copying the remaining rows.
func TestExactDrainBatchCohortsBeyondNamespaceCapacity(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newBatchWiredHandlerNoDispatch(t, 0)
	const count = 25
	body := []byte("cohort bytes")
	wantIDs := make(map[string]int64, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("cohort-%02d.mov", i)
		if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, 0, time.Now(), uint64(1000+i))); err != nil {
			t.Fatal(err)
		}
		writeReadySpool(t, spool, name, body)
		active, ok := spool.LookupActive(name)
		if !ok {
			t.Fatalf("missing private source for %s", name)
		}
		wantIDs[name] = active.ID()
	}
	// Direct SpoolEntry.Close has already finalized each source. ListReady is
	// the durable pre-claim oracle; sweepOnce counts only newly finalized rows.
	rows, err := spool.Meta().ListReady(count + 1)
	if err != nil || len(rows) != count {
		t.Fatalf("ready rows=%d err=%v", len(rows), err)
	}
	seenPaths := make(map[string]bool, count)
	seenIDs := make(map[int64]bool, count)
	for _, row := range rows {
		wantID, expected := wantIDs[row.NFSPath]
		if !expected || seenPaths[row.NFSPath] || seenIDs[row.ID] || row.ID != wantID {
			t.Fatalf("unexpected or duplicate ready tuple: path=%q id=%d wantID=%d", row.NFSPath, row.ID, wantID)
		}
		seenPaths[row.NFSPath], seenIDs[row.ID] = true, true
		if row.DrainState != metadata.DrainReady || row.Size != int64(len(body)) || row.PunchedEnd != 0 {
			t.Fatalf("ready tuple %d state=%s size=%d punched=%d", row.ID, row.DrainState, row.Size, row.PunchedEnd)
		}
		active, ok := spool.LookupActive(row.NFSPath)
		if !ok || active.ID() != row.ID || active.SpoolFilePath() != row.SpoolFile {
			t.Fatalf("ready tuple %d has no matching private index", row.ID)
		}
		if source, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(source, body) {
			t.Fatalf("ready tuple %d source=%q err=%v", row.ID, source, err)
		}
		mirror := jfs.handler.store.LookupByPath(row.NFSPath)
		captured, err := jfs.handler.store.CaptureMirror(row.NFSPath)
		if err != nil {
			t.Fatalf("ready tuple %d capture mirror: %v", row.ID, err)
		}
		var sqlSize, sqlRevision int64
		var sqlIncarnation string
		if err := jfs.handler.store.DB().QueryRow(`SELECT size,incarnation,revision FROM entries WHERE path=?`, row.NFSPath).
			Scan(&sqlSize, &sqlIncarnation, &sqlRevision); err != nil {
			t.Fatalf("ready tuple %d SQL mirror: %v", row.ID, err)
		}
		if mirror == nil || mirror.Version.IsZero() || mirror.Version != captured || mirror.Size != sqlSize || sqlSize != 0 ||
			mirror.Version.Incarnation != sqlIncarnation || mirror.Version.Revision != sqlRevision {
			t.Fatalf("ready tuple %d has unaligned versioned mirror: RAM=%+v SQL size=%d incarnation=%q revision=%d",
				row.ID, mirror, sqlSize, sqlIncarnation, sqlRevision)
		}
	}
	pending := make([]pendingDrain, 0, count)
	for _, row := range rows {
		if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
			t.Fatalf("claim %d: %t %v", row.ID, ok, err)
		}
		dest := filepath.Join(fuseRoot, row.NFSPath)
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(body); err != nil {
			t.Fatal(err)
		}
		fi, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatal("destination lacks stat identity")
		}
		pending = append(pending, pendingDrain{row: row, dest: dest, n: row.Size,
			mtime: time.Unix(1800000000, 0), fd: f,
			backingIdentity: fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino)})
	}
	drainer.batch.commitAndDispose(pending)
	if got := drainer.Metrics().DrainsSucceeded.Load(); got != count {
		t.Fatalf("successful drains=%d want %d", got, count)
	}
	for _, row := range rows {
		if got, err := spool.Meta().Get(row.ID); err != nil || got.DrainState != metadata.DrainDone {
			t.Errorf("row %d state=%+v err=%v", row.ID, got, err)
		}
		if _, ok := spool.LookupActive(row.NFSPath); ok {
			t.Errorf("row %d still active", row.ID)
		}
		if got := jfs.handler.store.LookupByPath(row.NFSPath); got == nil || got.Size != int64(len(body)) {
			t.Errorf("row %d EOF=%+v", row.ID, got)
		}
	}
}

// This barrier blocks the deterministic effects hook, not a kernel chmod.
// Stop must retain the active drain until that hook and its final FD close
// finish; unrelated metadata must remain responsive throughout.
func TestStartedDrainHeldEffectsKeepLeaseAndStopOwnership(t *testing.T) {
	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	const name = "held-clip.mov"
	if err := jfs.handler.store.Insert(metadata.MakeEntry("unrelated.mov", false, 0, time.Now(), 9001)); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	realEffects := drainer.onDrainEffects
	drainer.SetOnDrainEffects(func(item metadata.DrainCommitItem, r metadata.DrainCommitResult, fd *os.File, body []byte) error {
		close(entered)
		<-release
		return realEffects(item, r, fd, body)
	})
	drainer.Start()
	defer drainer.Stop(5 * time.Second)
	simulateNFSCreateThenWrite(t, jfs, name, []byte("held body"), 1024)
	spool.sweepOnce(0)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("started drainer never reached effects")
	}
	if err := (&juiceChange{handler: jfs.handler}).Chmod(name, 0400); !errors.Is(err, errFUSETimeout) {
		t.Errorf("held mode RPC refusal=%v, want retryable", err)
	}
	responsive := make(chan error, 1)
	go func() { responsive <- jfs.handler.store.UpdateMode("unrelated.mov", 0400) }()
	select {
	case err := <-responsive:
		if err != nil {
			t.Errorf("unrelated metadata update: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("unrelated metadata blocked behind held effects")
	}
	if drainer.Stop(10 * time.Millisecond) {
		t.Error("Stop returned while effects and final FD remain held")
	}
	unblock()
	if !drainer.Stop(5 * time.Second) {
		t.Fatal("Stop did not join released effects")
	}
	if got := drainer.Metrics().DrainsSucceeded.Load(); got != 1 {
		t.Errorf("successful drains=%d want 1", got)
	}
	if _, ok := spool.LookupActive(name); ok {
		t.Error("successful drain retained active source")
	}
}

func TestDrainFinalRetainedFDCloseFailureKeepsCustody(t *testing.T) {
	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	const name = "final-close.mov"
	body := []byte("only private source")
	simulateNFSCreateThenWrite(t, jfs, name, body, 1024)
	active, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("missing spool source")
	}
	id := active.ID()
	usedBefore, _ := spool.Capacity()
	realEffects := drainer.onDrainEffects
	drainer.SetOnDrainEffects(func(item metadata.DrainCommitItem, r metadata.DrainCommitResult, fd *os.File, data []byte) error {
		if err := realEffects(item, r, fd, data); err != nil {
			return err
		}
		// Force the retained description's final Close to fail after valid
		// effects. The original destination Close already qualified earlier.
		return fd.Close()
	})
	spool.sweepOnce(0)
	drainAllForTest(t, drainer)
	if got := drainer.Metrics().DrainsSucceeded.Load(); got != 0 {
		t.Errorf("reported success after final close error: %d", got)
	}
	if _, ok := spool.LookupActive(name); !ok {
		t.Error("final close error evicted source index")
	}
	if used, _ := spool.Capacity(); used != usedBefore {
		t.Errorf("capacity released: before=%d after=%d", usedBefore, used)
	}
	row, err := spool.Meta().Get(id)
	if err != nil || row.DrainState != metadata.DrainDone {
		t.Errorf("row=%+v err=%v, want held done", row, err)
	}
	if _, err := os.Stat(active.SpoolFilePath()); err != nil {
		t.Errorf("private source lost: %v", err)
	}
	var holds int
	if err := jfs.handler.store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, id).Scan(&holds); err != nil || holds != 1 {
		t.Errorf("FULL hold count=%d err=%v", holds, err)
	}
}
