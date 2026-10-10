package nfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// The effects hook is a deterministic scheduling barrier after the FULL
// completion transaction and before any effects or private-source cleanup.
// An authoritative parent rescan may discover the real backend inode here;
// that observation must not retire the still-owned completion generation.
func TestDrainCompletionObservationPreservesHeldGeneration(t *testing.T) {
	jfs, spool, drainer, _ := newSpoolWiredHandlerNoDrain(t)
	store := jfs.handler.store
	const name = "._observation.mov"
	const sibling = "unrelated.mov"
	body := patternedPayload(4096, 37)
	if err := store.Insert(metadata.MakeEntry(sibling, false, 7, time.Unix(1700000000, 0), 700)); err != nil {
		t.Fatal(err)
	}
	simulateNFSCreateThenWrite(t, jfs, name, body, len(body))
	active, ok := spool.LookupActive(name)
	if !ok {
		t.Fatal("missing original private source")
	}
	id, source := active.ID(), active.SpoolFilePath()
	usedBefore, _ := spool.Capacity()
	var observed *metadata.Entry
	var effectsErr error
	called, preserved := false, false
	realEffects := drainer.onDrainEffects
	drainer.SetOnDrainEffects(func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, data []byte) error {
		called = true
		var holds int
		if err := store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=? AND completion_id=?`, id, receipt.CompletionID).Scan(&holds); err != nil || holds != 1 {
			return fmt.Errorf("effects entered without exact FULL hold: count=%d err=%v", holds, err)
		}
		fi, err := fd.Stat()
		if err != nil {
			return err
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || st.Ino == receipt.Inode {
			return fmt.Errorf("fixture did not expose distinct backend inode")
		}
		observed = metadata.MakeEntry(name, false, item.Size, receipt.Mtime, st.Ino)
		observed.Mode = receipt.Mode
		epoch, err := store.CaptureStoreEpoch()
		if err != nil {
			return err
		}
		other := metadata.MakeEntry(sibling, false, 19, time.Unix(1700000001, 0), 700)
		if _, err := store.ReconcileUpsertBatch([]*metadata.Entry{observed, other}, epoch); err != nil {
			return fmt.Errorf("held-path filtering must still admit unrelated sibling: %w", err)
		}
		if got := store.LookupByPath(sibling); got == nil || got.Size != 19 {
			return fmt.Errorf("unrelated sibling observation did not advance")
		}
		got := store.LookupByPath(name)
		preserved = got != nil && got.Version == receipt.Published && got.Inode == receipt.Inode && got.Size == receipt.Size
		// Always run the real effects, even on a broken generation: the RED
		// must demonstrate custody and poisoning rather than stop at a test assertion.
		effectsErr = realEffects(item, receipt, fd, data)
		return effectsErr
	})
	spool.sweepOnce(0)
	drainAllForTest(t, drainer)
	if !called {
		t.Fatal("drain did not reach committed effects barrier")
	}
	var holds int
	if err := store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, id).Scan(&holds); err != nil {
		t.Fatal(err)
	}
	if !preserved || effectsErr != nil {
		if holds != 1 {
			t.Errorf("failed effects lost FULL hold: %d", holds)
		}
		if got, err := os.ReadFile(source); err != nil || !bytes.Equal(got, body) {
			t.Errorf("failed effects lost exact private image: err=%v", err)
		}
		if used, _ := spool.Capacity(); used != usedBefore {
			t.Errorf("failed effects released capacity: before=%d after=%d", usedBefore, used)
		}
		_, statErr := jfs.Stat(".")
		if !errors.Is(statErr, syscall.EIO) {
			t.Errorf("failed completion root error=%v, want EIO", statErr)
		}
		t.Fatalf("reconcile changed held completion generation: preserved=%t effects=%v", preserved, effectsErr)
	}
	if holds != 0 || drainer.Metrics().DrainsSucceeded.Load() != 1 {
		t.Fatalf("completion did not finish: holds=%d succeeded=%d", holds, drainer.Metrics().DrainsSucceeded.Load())
	}
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful completion retained private source: %v", err)
	}
	if _, err := jfs.Stat("."); err != nil {
		t.Fatalf("successful completion poisoned root: %v", err)
	}
	if used, _ := spool.Capacity(); used != usedBefore-int64(len(body)) {
		t.Fatalf("completion refund: before=%d after=%d", usedBefore, used)
	}
	// Releasing the hold must restore ordinary backend convergence; retaining
	// the synthetic inode forever is not a valid repair.
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileUpsertBatch([]*metadata.Entry{observed}, epoch); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath(name); got == nil || got.Inode != observed.Inode || got.Size != int64(len(body)) {
		t.Fatalf("post-completion backend observation did not converge: %+v", got)
	}
	if !jfs.handler.StopHandler() {
		t.Fatal("completed handler failed orderly shutdown")
	}
}
