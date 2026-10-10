package nfs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestDrainBatchUncertainCompletionPreservesBacking(t *testing.T) {
	for _, scenario := range []string{
		"rejected", "sql_error", "provider_error", "missing_result", "fallback_rejected", "fallback_error", "missing_provider",
	} {
		t.Run(scenario, func(t *testing.T) {
			jfs, spool, d, root := newBatchWiredHandlerNoDispatch(t, 1)
			t.Cleanup(func() {
				d.Stop(time.Second)
				// This fixture has no dispatcher; explicitly join any retry timer.
				d.inFlight.Wait()
			})
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
			// Exact production completion requires a versioned, RAM/SQL-aligned
			// mirror. Without it the preimage guard masks the SQL trigger oracle.
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
			d.onSizeReady = nil
			d.onDrainMetadataReady = nil
			d.onDrainComplete = func(string, int64, time.Time) { legacyCallbacks++ }
			realEffects := d.onDrainEffects
			d.onDrainEffects = func(item metadata.DrainCommitItem, receipt metadata.DrainCommitResult, fd *os.File, body []byte) error {
				effectCalls++
				return realEffects(item, receipt, fd, body)
			}
			providerCalls := 0
			triggerReached := false
			rejectedResult := false
			var injectedReturned error
			injectedProviderErr := errors.New("p58_injected_provider_error")
			const triggerMarker = "p58_batch_done_fault"
			realBatch := d.onBatchDrainCompleteExact
			d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
				providerCalls++
				if len(items) != 1 || items[0].SpoolID != row.ID || items[0].BackingIdentity == "" {
					t.Errorf("exact batch provider got wrong tuple: %+v", items)
				}
				rs, err := realBatch(items)
				rejectedResult = err == nil && len(rs) == 1 && !rs[0].Done
				triggerReached = err != nil && strings.Contains(err.Error(), triggerMarker)
				return rs, err
			}
			realInline := d.onDrainCommit
			d.onDrainCommit = func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
				providerCalls++
				if item.SpoolID != row.ID || item.BackingIdentity == "" {
					t.Errorf("inline provider got wrong tuple: %+v", item)
				}
				r, err := realInline(item)
				rejectedResult = err == nil && !r.Done
				triggerReached = err != nil && strings.Contains(err.Error(), triggerMarker)
				return r, err
			}
			switch scenario {
			case "rejected", "fallback_rejected":
				// Cancel SQL before either provider. The private source/index is
				// deliberately retained so this test owns its custody oracle.
				if _, err := spool.Meta().DeleteActiveByPath(row.NFSPath); err != nil {
					t.Fatal(err)
				}
			case "sql_error", "fallback_error":
				if _, err := jfs.handler.store.DB().Exec(`CREATE TRIGGER reject_done BEFORE UPDATE ON spool_entries WHEN NEW.drain_state='done' BEGIN SELECT RAISE(ABORT, 'p58_batch_done_fault'); END`); err != nil {
					t.Fatal(err)
				}
			case "missing_result":
				d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
					providerCalls++
					return nil, nil
				}
			case "provider_error":
				d.onBatchDrainCompleteExact = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
					providerCalls++
					if len(items) != 1 || items[0].SpoolID != row.ID || items[0].BackingIdentity == "" {
						t.Errorf("injected provider got wrong tuple: %+v", items)
					}
					injectedReturned = injectedProviderErr
					return nil, injectedProviderErr
				}
			case "missing_provider":
				// This case tests partial typed wiring itself, separately from
				// cancellation and the real SQL-trigger paths above.
				d.onBatchDrainCompleteExact = nil
			}
			pending := ownedPendingDrainForTest(t, row, dest, time.Now())
			if strings.HasPrefix(scenario, "fallback_") {
				// Explicit one-item inline fallback: keep the exact batch hook
				// wired, rather than disguising partial wiring as a SQL fault.
				d.completeInline(pending)
			} else {
				d.batch.commitAndDispose([]pendingDrain{pending})
			}
			wantProvider := 1
			if scenario == "missing_provider" {
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
			if scenario == "provider_error" && !errors.Is(injectedReturned, injectedProviderErr) {
				t.Error("injected provider error marker was not returned")
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
				t.Fatalf("private spool lost: %q err=%v", got, err)
			}
			if _, ok := spool.LookupActive(row.NFSPath); !ok {
				t.Fatal("completion evicted private spool index")
			}
			if used, _ := spool.Capacity(); used != usedBefore {
				t.Fatalf("completion released capacity: got=%d want=%d", used, usedBefore)
			}
			if legacyCallbacks != 0 || effectCalls != 0 || d.Metrics().DrainsSucceeded.Load() != 0 || !d.LastDrainSuccess().IsZero() {
				t.Fatal("rejected completion reported effects or success")
			}
			mirror := jfs.handler.store.LookupByPath(name)
			if mirror == nil || mirror.Version != beforeVersion || mirror.Size != 128 || !mirror.Mtime.Equal(stamp) {
				t.Fatalf("uncertain completion changed mirror: %+v", mirror)
			}
			var sqlSize int64
			if err := jfs.handler.store.DB().QueryRow(`SELECT size FROM entries WHERE path=?`, name).Scan(&sqlSize); err != nil || sqlSize != 128 {
				t.Errorf("uncertain SQL mirror size=%d err=%v", sqlSize, err)
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
