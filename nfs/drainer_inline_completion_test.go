package nfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// ownedPendingDrainForTest transfers one exact destination description to the
// completion helper. A test that omits this FD could pass by failing before
// the generation-bound effects it intended to exercise.
func ownedPendingDrainForTest(t *testing.T, row *metadata.SpoolRow, dest string, mtime time.Time) pendingDrain {
	t.Helper()
	fd, err := os.OpenFile(dest, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := fd.Stat()
	if err != nil || !fi.Mode().IsRegular() || fi.Size() != row.Size {
		_ = fd.Close()
		t.Fatalf("destination does not match row: info=%v row=%+v err=%v", fi, row, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		_ = fd.Close()
		t.Fatal("destination has no dev:ino identity")
	}
	return pendingDrain{row: row, dest: dest, n: row.Size, mtime: mtime, fd: fd,
		backingIdentity: fmt.Sprintf("%d:%d", uint64(st.Dev), st.Ino)}
}

func TestInlineDrainUncertainCompletionRetainsCustody(t *testing.T) {
	for _, scenario := range []string{
		"rejected", "sql_error", "missing_result", "unwired",
		"fallback_rejected", "fallback_error", "fallback_unwired", "fallback_missing_provider",
	} {
		t.Run(scenario, func(t *testing.T) {
			jfs, spool, d, root := newSpoolWiredHandlerNoDrain(t)
			t.Cleanup(func() { d.Stop(time.Second); d.inFlight.Wait() })
			// Every intended batch rejection/SQL-fault case has its exact
			// provider. Missing-provider cases remove one hook deliberately.
			d.SetOnBatchDrainCompleteExact(jfs.handler.store.BatchDrainCompleteExact)
			const name = "reel.mov"
			payload := []byte("retained private spool bytes")
			writeReadySpool(t, spool, name, payload)
			spool.sweepOnce(0)
			rows, err := spool.Meta().ListReady(1)
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%+v err=%v", rows, err)
			}
			row := rows[0]
			if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
				t.Fatalf("claim=%v err=%v", ok, err)
			}
			stamp := time.Unix(1700000000, 0)
			if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, 128, stamp, 902)); err != nil {
				t.Fatal(err)
			}
			beforeVersion := jfs.handler.store.LookupByPath(name).Version
			dest := filepath.Join(root, name)
			replacement := bytes.Repeat([]byte("R"), len(payload))
			if err := os.WriteFile(dest, replacement, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(dest)
			if err != nil {
				t.Fatal(err)
			}
			usedBefore, _ := spool.Capacity()
			legacyCallbacks, effectCalls := 0, 0
			record := func(string, int64, time.Time) { legacyCallbacks++ }
			d.onSizeReady, d.onDrainMetadataReady, d.onDrainComplete = record, record, record
			realEffects := d.onDrainEffects
			d.onDrainEffects = func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
				effectCalls++
				return realEffects(item, receipt, fd, body)
			}
			providerCalls := 0
			triggerReached := false
			rejectedResult := false
			const triggerMarker = "p58_inline_done_fault"
			realInline := d.onDrainCommit
			d.onDrainCommit = func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
				providerCalls++
				if item.SpoolID != row.ID || item.BackingIdentity == "" {
					t.Errorf("inline provider got wrong tuple: %+v", item)
				}
				r, err := realInline(item)
				rejectedResult = !r.Done && err == nil
				triggerReached = err != nil && strings.Contains(err.Error(), triggerMarker)
				return r, err
			}
			realBatch := d.onBatchDrainCompleteExact
			d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
				providerCalls++
				if len(items) != 1 || items[0].SpoolID != row.ID || items[0].BackingIdentity == "" {
					t.Errorf("batch provider got wrong tuple: %+v", items)
				}
				rs, err := realBatch(items)
				rejectedResult = err == nil && len(rs) == 1 && !rs[0].Done
				triggerReached = err != nil && strings.Contains(err.Error(), triggerMarker)
				return rs, err
			}
			switch scenario {
			case "rejected", "fallback_rejected":
				// SQL cancellation is separate from the still-owned private source.
				if _, err := spool.Meta().DeleteActiveByPath(name); err != nil {
					t.Fatal(err)
				}
			case "sql_error", "fallback_error":
				if _, err := jfs.handler.store.DB().Exec(`CREATE TRIGGER reject_done BEFORE UPDATE ON spool_entries WHEN NEW.drain_state='done' BEGIN SELECT RAISE(ABORT, 'p58_inline_done_fault'); END`); err != nil {
					t.Fatal(err)
				}
			case "missing_result":
				d.onDrainCommit = func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
					providerCalls++
					return metadata.DrainCommitResult{}, nil
				}
			case "unwired", "fallback_unwired":
				d.onDrainCommit = nil
			case "fallback_missing_provider":
				d.onBatchDrainCompleteExact = nil
			}
			pending := ownedPendingDrainForTest(t, row, dest, time.Now())
			if strings.HasPrefix(scenario, "fallback_") {
				(&drainBatcher{d: d}).commitAndDispose([]pendingDrain{pending})
			} else {
				d.completeInline(pending)
			}
			wantProvider := 1
			if scenario == "unwired" || scenario == "fallback_unwired" || scenario == "fallback_missing_provider" {
				wantProvider = 0
			}
			if providerCalls != wantProvider {
				t.Errorf("provider calls=%d want %d", providerCalls, wantProvider)
			}
			if scenario == "sql_error" || scenario == "fallback_error" {
				if !triggerReached {
					t.Error("real provider did not reach distinctive SQL done trigger")
				}
			}
			if scenario == "rejected" || scenario == "fallback_rejected" {
				if !rejectedResult {
					t.Error("provider did not return Done=false for canceled tuple")
				}
			}
			got, err := os.ReadFile(dest)
			if err != nil || !bytes.Equal(got, replacement) {
				t.Fatalf("replacement changed: %q err=%v", got, err)
			}
			after, err := os.Stat(dest)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("replacement identity changed: %v", err)
			}
			if got, err := os.ReadFile(row.SpoolFile); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("private source lost: %q err=%v", got, err)
			}
			if e, ok := spool.LookupActive(name); !ok || e.ID() != row.ID {
				t.Fatal("completion evicted source index")
			}
			if used, _ := spool.Capacity(); used != usedBefore {
				t.Fatalf("capacity changed: %d want %d", used, usedBefore)
			}
			if legacyCallbacks != 0 || effectCalls != 0 || d.Metrics().DrainsSucceeded.Load() != 0 || !d.LastDrainSuccess().IsZero() {
				t.Fatal("uncertain completion reported effects or success")
			}
			mirror := jfs.handler.store.LookupByPath(name)
			if mirror == nil || mirror.Version != beforeVersion || mirror.Size != 128 || !mirror.Mtime.Equal(stamp) {
				t.Fatalf("uncertain completion changed mirror: %+v", mirror)
			}
			if scenario != "rejected" && scenario != "fallback_rejected" {
				current, err := spool.Meta().Get(row.ID)
				if err != nil || current == nil || current.DrainState == metadata.DrainDone {
					t.Fatalf("uncertain row completed: %+v err=%v", current, err)
				}
			}
		})
	}
}

func TestInlineDrainPublishesBeforeShadowEviction(t *testing.T) {
	for _, size := range []int{0, 128} {
		for _, fallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("size_%d_batch_%t", size, fallback), func(t *testing.T) {
				const name = "shrink.mov"
				jfs, spool, d, root := newSpoolWiredHandlerNoDrain(t)
				if fallback {
					d.SetOnBatchDrainCompleteExact(jfs.handler.store.BatchDrainCompleteExact)
				}
				if err := jfs.handler.store.Insert(metadata.MakeEntry(name, false, 4096, time.Now(), 42)); err != nil {
					t.Fatal(err)
				}
				writeReadySpool(t, spool, name, make([]byte, size))
				spool.sweepOnce(0)
				rows, err := spool.Meta().ListReady(1)
				if err != nil || len(rows) != 1 {
					t.Fatalf("rows=%+v err=%v", rows, err)
				}
				row := rows[0]
				if ok, err := spool.Meta().MarkDraining(row.ID); err != nil || !ok {
					t.Fatalf("claim=%v err=%v", ok, err)
				}
				providerCalls, effectsCalls := 0, 0
				realInline := d.onDrainCommit
				d.onDrainCommit = func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
					providerCalls++
					return realInline(item)
				}
				if fallback {
					realBatch := d.onBatchDrainCompleteExact
					d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
						providerCalls++
						return realBatch(items)
					}
				}
				realEffects := d.onDrainEffects
				d.onDrainEffects = func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
					effectsCalls++
					if item.SpoolID != row.ID || receipt.SpoolID != row.ID || receipt.Published.IsZero() || receipt.BackingIdentity != item.BackingIdentity {
						t.Errorf("effects got inexact receipt: item=%+v receipt=%+v", item, receipt)
					}
					got := jfs.handler.store.LookupByPath(name)
					if got == nil || got.Size != int64(size) || got.Version != receipt.Published {
						t.Errorf("effects before exact RAM publication: %+v receipt=%+v", got, receipt)
					}
					var sqlSize, revision int64
					var incarnation string
					if err := jfs.handler.store.DB().QueryRow(`SELECT size,incarnation,revision FROM entries WHERE path=?`, name).Scan(&sqlSize, &incarnation, &revision); err != nil || sqlSize != int64(size) || incarnation != receipt.Published.Incarnation || revision != receipt.Published.Revision {
						t.Errorf("effects before exact SQL publication: size=%d incarnation=%q revision=%d err=%v", sqlSize, incarnation, revision, err)
					}
					var holds int
					if err := jfs.handler.store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds WHERE completion_id=? AND spool_id=?`, receipt.CompletionID, row.ID).Scan(&holds); err != nil || holds != 1 {
						t.Errorf("FULL hold missing before effects: count=%d err=%v", holds, err)
					}
					if e, ok := spool.LookupActive(name); !ok || e.ID() != row.ID {
						t.Error("shadow evicted before typed effects")
					}
					if _, err := os.Stat(row.SpoolFile); err != nil {
						t.Errorf("private source removed before typed effects: %v", err)
					}
					return realEffects(item, receipt, fd, body)
				}
				dest := filepath.Join(root, name)
				if err := os.WriteFile(dest, make([]byte, size), 0600); err != nil {
					t.Fatal(err)
				}
				p := ownedPendingDrainForTest(t, row, dest, time.Now())
				if fallback {
					(&drainBatcher{d: d}).commitAndDispose([]pendingDrain{p})
				} else {
					d.completeInline(p)
				}
				if providerCalls != 1 || effectsCalls != 1 || d.Metrics().DrainsSucceeded.Load() != 1 {
					t.Fatalf("size=%d batch=%t provider=%d effects=%d successes=%d", size, fallback, providerCalls, effectsCalls, d.Metrics().DrainsSucceeded.Load())
				}
				if got := jfs.handler.store.LookupByPath(name); got == nil || got.Size != int64(size) {
					t.Errorf("final RAM EOF=%+v", got)
				}
				if fi, err := os.Stat(dest); err != nil {
					t.Errorf("final destination stat: %v", err)
				} else if e := jfs.handler.store.LookupByPath(name); e == nil || fi.Mode().Perm() != e.Mode.Perm() {
					t.Errorf("owned FD mode effect missing: backing=%#o mirror=%+v", fi.Mode().Perm(), e)
				}
				var durableSize int64
				if err := jfs.handler.store.DB().QueryRow(`SELECT size FROM entries WHERE path=?`, name).Scan(&durableSize); err != nil || durableSize != int64(size) {
					t.Errorf("final SQL EOF=%d err=%v", durableSize, err)
				}
				if _, ok := spool.LookupActive(name); ok {
					t.Fatal("committed shadow retained")
				}
				if _, err := os.Stat(row.SpoolFile); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("committed source remains: %v", err)
				}
			})
		}
	}
}
