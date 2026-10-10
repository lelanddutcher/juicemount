package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

type completionCustodyFixture struct {
	store        *metadata.Store
	spool        *SpoolStore
	drainer      *Drainer
	row          *metadata.SpoolRow
	dbPath, root string
	body         []byte
}

func newCompletionCustodyFixture(t *testing.T) *completionCustodyFixture {
	t.Helper()
	f := &completionCustodyFixture{dbPath: filepath.Join(t.TempDir(), "metadata.db"), root: t.TempDir(), body: []byte("the only retained local source")}
	var err error
	f.store, err = metadata.Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.InitSpoolSchema(f.store.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(f.store.DB())
	f.store.SetSpoolStore(meta)
	f.spool, err = NewSpoolStore(f.root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	f.drainer, err = NewDrainer(f.spool, DrainerConfig{FuseRoot: t.TempDir(), MaxAttempts: 2, BackoffBase: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	f.drainer.SetOnDrainCommit(f.store.DrainComplete)
	t.Cleanup(func() { f.drainer.Stop(time.Second); f.drainer.inFlight.Wait(); f.spool.Stop(); _ = f.store.Close() })
	const name = "clip.mov"
	if err := f.store.Insert(metadata.MakeEntry(name, false, 0, time.Now(), 42)); err != nil {
		t.Fatal(err)
	}
	writeReadySpool(t, f.spool, name, f.body)
	f.spool.sweepOnce(0)
	rows, err := meta.ListReady(1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if ok, err := meta.MarkDraining(rows[0].ID); err != nil || !ok {
		t.Fatalf("claim=%v err=%v", ok, err)
	}
	f.row, err = meta.Get(rows[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *completionCustodyFixture) pending() pendingDrain {
	return pendingDrain{row: f.row, dest: filepath.Join(f.drainer.fuseRoot, f.row.NFSPath), n: f.row.Size, mtime: time.Now()}
}

func (f *completionCustodyFixture) assertLocalCustody(t *testing.T) {
	t.Helper()
	if b, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(b, f.body) {
		t.Errorf("source changed: bytes=%q err=%v", b, err)
	}
	if e, ok := f.spool.LookupActive(f.row.NFSPath); !ok || e.ID() != f.row.ID {
		t.Errorf("source index lost or replaced: present=%t", ok)
	}
	if used, _ := f.spool.Capacity(); used != int64(len(f.body)) {
		t.Errorf("source reservation=%d want%d", used, len(f.body))
	}
	if f.drainer.Metrics().DrainsSucceeded.Load() != 0 {
		t.Error("uncertain completion reported success")
	}
}

func TestCompletionUnwiredRetainsCustody(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	f.drainer.onDrainCommit = nil
	// More invocations than the file's entire retry budget. A configuration
	// error must never transform valid source bytes into a failed/evicted row.
	for i := 0; i < 5; i++ {
		f.drainer.completeInline(f.pending())
		row, err := f.spool.Meta().Get(f.row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.DrainState == metadata.DrainReady {
			if ok, err := f.spool.Meta().MarkDraining(row.ID); err != nil || !ok {
				t.Fatalf("claim=%v err=%v", ok, err)
			}
			row, err = f.spool.Meta().Get(row.ID)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.row = row
	}
	f.assertLocalCustody(t)
	if f.row.DrainAttempts != 0 || f.row.DrainState == metadata.DrainFailed {
		t.Errorf("configuration failure spent source budget: %+v", f.row)
	}
}

func TestCompletionMalformedTokenSuppressesCallbacks(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline", true: "batch"}[batch], func(t *testing.T) {
			f := newCompletionCustodyFixture(t)
			calls := 0
			record := func(string, int64, time.Time) { calls++ }
			f.drainer.onSizeReady, f.drainer.onDrainMetadataReady, f.drainer.onDrainComplete = record, record, record
			f.drainer.onDrainCommit = func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
				result, err := f.store.DrainComplete(item)
				result.CompletionID = "wrong-nonempty-token"
				return result, err
			}
			f.drainer.onBatchDrainComplete = func(items []metadata.DrainCommitItem) ([]metadata.DrainCommitResult, error) {
				results, err := f.store.BatchDrainComplete(items)
				for i := range results {
					results[i].CompletionID = "wrong-nonempty-token"
				}
				return results, err
			}
			if batch {
				(&drainBatcher{d: f.drainer}).commitAndDispose([]pendingDrain{f.pending()})
			} else {
				f.drainer.completeInline(f.pending())
			}
			if calls != 0 {
				t.Errorf("malformed completion ran %d callbacks", calls)
			}
			f.assertLocalCustody(t)
		})
	}
}

func TestCompletionHoldRequiresCurrentTuple(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size + 1}
	if err := f.spool.Meta().HoldDrainCompletion(item); !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("stale tuple enrollment error=%v", err)
	}
	var count int
	if err := f.store.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("holds=%d err=%v", count, err)
	}
	f.assertLocalCustody(t)
}

func TestCompletionFailedTransitionPreservesCustody(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size}
	if err := f.spool.Meta().HoldDrainCompletion(item); err != nil {
		t.Fatal(err)
	}
	f.drainer.failPermanent(f.row, "cannot override an uncertain completion")
	f.assertLocalCustody(t)
	if got := f.drainer.Metrics().DrainsFailed.Load(); got != 0 {
		t.Errorf("refused failure counted=%d", got)
	}
}

func TestCompletionLostReplyDoesNotReplay(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "error_after_commit", true: "missing_result"}[missing], func(t *testing.T) {
			f := newCompletionCustodyFixture(t)
			f.drainer.SetOnDrainCommit(func(item metadata.DrainCommitItem) (metadata.DrainCommitResult, error) {
				result, err := f.store.DrainComplete(item)
				if err != nil || !result.Done {
					t.Fatalf("real commit=%+v err=%v", result, err)
				}
				if missing {
					return metadata.DrainCommitResult{}, nil
				}
				return metadata.DrainCommitResult{}, errors.New("lost completion response after commit")
			})
			f.drainer.completeInline(f.pending())
			row, err := f.spool.Meta().Get(f.row.ID)
			if err != nil {
				t.Fatal(err)
			}
			if row.DrainState != metadata.DrainDone || row.DrainAttempts != 0 {
				t.Errorf("ambiguous completion replayed committed row: %+v", row)
			}
			f.assertLocalCustody(t)
		})
	}
}

func TestCompletionCrashAfterCommitRetainsSource(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	// Crash boundary: the real transaction commits, but the drainer never
	// receives its return value and cannot acknowledge or dispose the source.
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size, Mtime: time.Now()}
	result, err := f.store.DrainComplete(item)
	if err != nil || !result.Done {
		t.Fatalf("real commit=%+v err=%v", result, err)
	}
	f.spool.Stop()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	meta := metadata.NewSpoolStore(reopened.DB())
	recovered, err := NewSpoolStore(f.root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Stop()
	report, recoveryErr := recovered.RecoverOnBoot(context.Background())
	if recoveryErr == nil {
		t.Errorf("restart accepted unresolved completion: %+v", report)
	}
	if b, err := os.ReadFile(f.row.SpoolFile); err != nil || !bytes.Equal(b, f.body) {
		t.Errorf("restart destroyed unresolved local source: bytes=%q err=%v", b, err)
	}
	row, err := meta.Get(f.row.ID)
	if err != nil || row.DrainState != metadata.DrainDone {
		t.Errorf("restart replayed completion: row=%+v err=%v", row, err)
	}
}

func TestCompletionHoldRefusesMutations(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "draining", true: "done"}[committed], func(t *testing.T) {
			f := newCompletionCustodyFixture(t)
			item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size, Mtime: time.Now()}
			if committed {
				if r, err := f.store.DrainComplete(item); err != nil || !r.Done {
					t.Fatalf("result=%+v err=%v", r, err)
				}
			} else if err := f.spool.Meta().HoldDrainCompletion(item); err != nil {
				t.Fatal(err)
			}
			checks := []struct {
				name string
				call func() error
			}{
				{"cancel", func() error {
					changed, err := f.spool.cancelForDelete(f.row.NFSPath)
					if changed {
						t.Error("held cancellation claimed an effect")
					}
					return err
				}},
				{"rename", func() error { _, _, err := f.spool.MigrateForRename(f.row.NFSPath, "replacement.mov"); return err }},
				{"reset", func() error { return f.spool.Meta().ResetToReady(f.row.ID) }},
				{"failed", func() error { _, err := f.spool.Meta().MarkFailed(f.row.ID, "not a data failure"); return err }},
				{"done", func() error { _, err := f.spool.Meta().MarkDone(f.row.ID); return err }},
				{"delete", func() error { return f.spool.Meta().Delete(f.row.ID) }},
				{"reopen", func() error { _, err := f.spool.Meta().ReopenReady(f.row.ID); return err }},
				{"punch", func() error { return f.spool.Meta().SetPunchedEnd(f.row.ID, 1) }},
				{"attempt", func() error { return f.spool.Meta().IncrementAttempts(f.row.ID, "retry") }},
				{"new_writer", func() error {
					_, err := f.spool.Meta().Insert(f.row.NFSPath, filepath.Join(f.root, SpoolFilesSubdir, "new"))
					return err
				}},
			}
			for _, check := range checks {
				var held *metadata.PendingDrainCompletion
				if err := check.call(); !errors.As(err, &held) {
					t.Errorf("%s=%v want held refusal", check.name, err)
				}
			}
			if n, err := f.spool.Meta().DeleteDone(time.Now().Add(time.Hour)); err != nil || n != 0 {
				t.Errorf("GC deleted hold n=%d err=%v", n, err)
			}
			f.assertLocalCustody(t)
			row, err := f.spool.Meta().Get(f.row.ID)
			want := metadata.DrainDraining
			if committed {
				want = metadata.DrainDone
			}
			if err != nil || row.DrainState != want || row.DrainAttempts != 0 || row.PunchedEnd != 0 {
				t.Errorf("held row changed: %+v err=%v", row, err)
			}
		})
	}
}

func TestCompletionAcknowledgedCleanupIsOnceOnly(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size, Mtime: time.Now()}
	result, err := f.store.DrainComplete(item)
	if err != nil || !result.Done {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := f.spool.CompleteAcknowledgedDrain(item, result); err != nil {
		t.Fatal(err)
	}
	if used, _ := f.spool.Capacity(); used != 0 {
		t.Fatalf("cleanup reservation=%d", used)
	}
	if _, ok := f.spool.LookupActive(f.row.NFSPath); ok {
		t.Fatal("completed index retained")
	}
	if _, err := os.Stat(f.row.SpoolFile); !os.IsNotExist(err) {
		t.Fatalf("source not cleaned: %v", err)
	}
	if err := f.store.CheckSpoolRecovery(); err != nil {
		t.Fatalf("acknowledged clean completion blocked restart: %v", err)
	}
	newer := []byte("new writer reservation must remain")
	writeReadySpool(t, f.spool, f.row.NFSPath, newer)
	if err := f.spool.CompleteAcknowledgedDrain(item, result); err == nil {
		t.Error("duplicate receipt accepted")
	}
	if used, _ := f.spool.Capacity(); used != int64(len(newer)) {
		t.Fatalf("duplicate cleanup released new writer budget: %d", used)
	}
	e, ok := f.spool.LookupActive(f.row.NFSPath)
	if !ok || e.ID() == f.row.ID {
		t.Fatal("duplicate cleanup changed new writer index")
	}
	b, err := os.ReadFile(e.SpoolFilePath())
	if err != nil || !bytes.Equal(b, newer) {
		t.Fatalf("new writer changed: %q err=%v", b, err)
	}
}

func TestCompletionCleanupFailureRetainsHold(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size, Mtime: time.Now()}
	result, err := f.store.DrainComplete(item)
	if err != nil || !result.Done {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	parent := filepath.Dir(f.row.SpoolFile)
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0700) })
	if err := f.spool.CompleteAcknowledgedDrain(item, result); err == nil {
		t.Fatal("cleanup unexpectedly accepted read-only directory")
	}
	f.assertLocalCustody(t)
	var held *metadata.PendingDrainCompletion
	if err := f.store.CheckSpoolRecovery(); !errors.As(err, &held) {
		t.Fatalf("failed cleanup lost hold: %v", err)
	}
}

func TestCompletionBootRefusesBeforeAnyRecoveryWork(t *testing.T) {
	f := newCompletionCustodyFixture(t)
	item := metadata.DrainCommitItem{SpoolID: f.row.ID, NFSPath: f.row.NFSPath, SpoolFile: f.row.SpoolFile, Size: f.row.Size, Mtime: time.Now()}
	if err := f.spool.Meta().HoldDrainCompletion(item); err != nil {
		t.Fatal(err)
	}
	f.spool.used.Store(12345)
	var held *metadata.PendingDrainCompletion
	if report, err := f.spool.RecoverOnBoot(context.Background()); !errors.As(err, &held) || report != (RecoveryReport{}) {
		t.Fatalf("boot=%+v err=%v", report, err)
	}
	if used, _ := f.spool.Capacity(); used != 12345 {
		t.Fatalf("boot changed capacity before refusal: %d", used)
	}
	server := NewServer(Config{ListenAddr: "127.0.0.1:0", FUSEPath: f.drainer.fuseRoot}, f.store)
	t.Cleanup(func() { server.handler.StopHandler() })
	if err := server.Start(); !errors.As(err, &held) {
		t.Fatalf("listener admission=%v", err)
	}
	if server.IsRunning() || server.listener != nil || server.protocol != nil {
		t.Fatal("listener published while completion held")
	}
}
