package nfs

import (
	"bytes"
	"os"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestPartialTypedDrainWiringRetainsCustody(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, missing := range []string{"admission", "effects", "commit", "batch_commit"} {
			if !batch && missing == "batch_commit" {
				continue
			}
			mode := "inline"
			if batch {
				mode = "batch"
			}
			t.Run(mode+"/"+missing, func(t *testing.T) {
				var jfs *juiceFS
				var spool *SpoolStore
				var d *Drainer
				if batch {
					jfs, spool, d, _ = newBatchWiredHandlerNoDispatch(t, 1)
				} else {
					jfs, spool, d, _ = newSpoolWiredHandlerNoDrain(t)
				}
				const name = "partial.mov"
				body := []byte("private bytes survive incomplete completion wiring")
				simulateNFSCreateThenWrite(t, jfs, name, body, len(body))
				spool.sweepOnce(0)
				active, ok := spool.LookupActive(name)
				if !ok {
					t.Fatal("missing source")
				}
				before := jfs.handler.store.LookupByPath(name)
				capacity, _ := spool.Capacity()
				commits := 0
				inlineCommit := d.onDrainCommit
				d.onDrainCommit = func(it metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
					commits++
					return inlineCommit(it)
				}
				batchCommit := d.onBatchDrainCompleteExact
				d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
					commits++
					return batchCommit(items)
				}
				switch missing {
				case "admission":
					d.onDrainAdmission = nil
				case "effects":
					d.onDrainEffects = nil
				case "commit":
					d.onDrainCommit = nil
				case "batch_commit":
					d.onBatchDrainCompleteExact = nil
				}
				drainAllForTest(t, d)
				if commits != 0 || d.Metrics().DrainsSucceeded.Load() != 0 || !d.LastDrainSuccess().IsZero() {
					t.Fatalf("incomplete wiring committed/reported success: commits=%d", commits)
				}
				if got := jfs.handler.store.LookupByPath(name); got == nil || got.Version != before.Version || got.Size != before.Size || got.Mode != before.Mode {
					t.Fatalf("incomplete wiring changed mirror: before=%+v after=%+v", before, got)
				}
				if got, ok := spool.LookupActive(name); !ok || got.ID() != active.ID() {
					t.Fatal("incomplete wiring evicted private source")
				}
				if got, err := os.ReadFile(active.SpoolFilePath()); err != nil || !bytes.Equal(got, body) {
					t.Fatalf("private source changed: %q err=%v", got, err)
				}
				if used, _ := spool.Capacity(); used != capacity {
					t.Fatalf("reservation changed: got=%d want=%d", used, capacity)
				}
				row, err := spool.Meta().Get(active.ID())
				if err != nil || row.DrainState != metadata.DrainDraining {
					t.Fatalf("unexpected retained row: %+v err=%v", row, err)
				}
				var holds int
				if err := jfs.handler.store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE spool_id=?`, active.ID()).Scan(&holds); err != nil || holds != 1 {
					t.Fatalf("missing custody hold: count=%d err=%v", holds, err)
				}
			})
		}
	}
}
