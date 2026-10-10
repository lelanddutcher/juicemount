package metadata

import (
	"errors"
	"testing"
	"time"
)

func TestScanPreservesAcceptedReplacementMirrorAndAncestry(t *testing.T) {
	f, _, _ := acceptedPublicationFixture(t)
	destination := f.w.Destination.Path
	before := f.store.LookupByPath(destination)
	if before == nil || !before.LocalOnly || before.Size != f.w.Image.Size {
		t.Fatalf("accepted destination=%+v", before)
	}
	parentPath := f.w.Destination.Chain[0].Path
	parent := f.store.LookupByPath(parentPath)
	localParentPath := f.w.SourceParents[0].Token.Path
	localParent := f.store.LookupByPath(localParentPath)
	if parent == nil || localParent == nil || !localParent.LocalOnly {
		t.Fatalf("accepted parents=%+v %+v", parent, localParent)
	}

	stale := before.snapshot()
	stale.Version = MirrorToken{}
	stale.Inode = f.w.Destination.Chain[len(f.w.Destination.Chain)-1].Inode
	stale.Size = f.w.Destination.Chain[len(f.w.Destination.Chain)-1].Size
	stale.LocalOnly = false
	stale.Mtime = stale.Mtime.Add(time.Second)
	remoteParent := parent.snapshot()
	remoteParent.Version = MirrorToken{}
	remoteParent.Mtime = remoteParent.Mtime.Add(time.Second)
	ordinary := MakeEntry("unrelated.mov", false, 17, time.Unix(1700000400, 0), 701)

	epoch, err := f.store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	next, err := f.store.ScanUpsertBatch([]*Entry{stale, remoteParent, ordinary}, epoch,
		[]string{destination, parentPath})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.store.LookupByPath(destination); !sameReplacementEntry(got, before) {
		t.Fatalf("SCAN clobbered accepted destination: got=%+v want=%+v", got, before)
	}
	if got := f.store.LookupByPath(parentPath); !sameReplacementEntry(got, parent) {
		t.Fatalf("SCAN clobbered captured ancestor: got=%+v want=%+v", got, parent)
	}
	if got := f.store.LookupByPath(ordinary.Path); got == nil || got.Size != ordinary.Size {
		t.Fatalf("ordinary remote observation was suppressed: %+v", got)
	}
	next, err = f.store.ScanClearLocalOnlyBatch([]string{destination, localParentPath}, next)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.store.LookupByPath(destination); !sameReplacementEntry(got, before) {
		t.Fatalf("SCAN cleared accepted local destination: %+v", got)
	}
	if got := f.store.LookupByPath(localParentPath); !sameReplacementEntry(got, localParent) {
		t.Fatalf("SCAN cleared captured source ancestor: %+v", got)
	}
	if _, err := f.store.ScanUpsertBatch([]*Entry{MakeEntry("another.mov", false, 9, time.Unix(1700000500, 0), 702)}, next, nil); err != nil {
		t.Fatalf("ordinary later SCAN chunk: %v", err)
	}
}

func TestKeyspaceReconcilePreservesAcceptedReplacement(t *testing.T) {
	f, _, _ := acceptedPublicationFixture(t)
	before := f.store.LookupByPath(f.w.Destination.Path)
	stale := before.snapshot()
	stale.Version = MirrorToken{}
	stale.Inode = f.w.Destination.Chain[len(f.w.Destination.Chain)-1].Inode
	stale.Size = f.w.Destination.Chain[len(f.w.Destination.Chain)-1].Size
	stale.LocalOnly = false
	parent := f.store.LookupByPath(f.w.Destination.Chain[0].Path)
	remoteParent := parent.snapshot()
	remoteParent.Version = MirrorToken{}
	remoteParent.Mtime = remoteParent.Mtime.Add(time.Second)
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	var invalidated []string
	f.store.SetOnScanContentInvalidated(func(path string, _ uint64) { invalidated = append(invalidated, path) })
	f.store.SetOnContentInvalidated(func(path string, isDir bool) {
		if !isDir {
			t.Fatalf("file used reentrant keyspace callback: %s", path)
		}
		invalidated = append(invalidated, path)
	})
	if err := rc.applyReconcileUpserts([]*Entry{stale, remoteParent}); err != nil {
		t.Fatal(err)
	}
	if got := f.store.LookupByPath(before.Path); !sameReplacementEntry(got, before) {
		t.Fatalf("keyspace clobbered accepted destination: got=%+v want=%+v", got, before)
	}
	if len(invalidated) != 0 {
		t.Fatalf("protected content invalidated: %v", invalidated)
	}
	ordinary := MakeEntry("ordinary.mov", false, 10, time.Unix(1700000600, 0), 703)
	if err := f.store.Insert(ordinary); err != nil {
		t.Fatal(err)
	}
	changed := ordinary.snapshot()
	changed.Version = MirrorToken{}
	changed.Size++
	if err := rc.applyReconcileUpserts([]*Entry{changed}); err != nil {
		t.Fatal(err)
	}
	if len(invalidated) != 1 || invalidated[0] != ordinary.Path {
		t.Fatalf("ordinary content invalidations: %v", invalidated)
	}
	ordinaryDir := MakeEntry("ordinary-dir", true, 0, time.Unix(1700000600, 0), 704)
	if err := f.store.Insert(ordinaryDir); err != nil {
		t.Fatal(err)
	}
	changedDir := ordinaryDir.snapshot()
	changedDir.Version = MirrorToken{}
	changedDir.Mtime = changedDir.Mtime.Add(time.Second)
	changedFile := changed.snapshot()
	changedFile.Size++
	if err := rc.applyReconcileUpserts([]*Entry{changedFile, changedDir}); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("directory reconcile with accepted byte cache=%v", err)
	}
	if len(invalidated) != 1 {
		t.Fatalf("refused batch invalidated content: %v", invalidated)
	}
	if got := f.store.LookupByPath(changed.Path); got == nil || got.Size != changed.Size {
		t.Fatalf("refused batch changed file: %+v", got)
	}
	if got := f.store.LookupByPath(ordinaryDir.Path); got == nil || !got.Mtime.Equal(ordinaryDir.Mtime) {
		t.Fatalf("refused batch changed directory: %+v", got)
	}
}

func TestScanAcceptedReplacementJournalFailClosedAndCleanedRelease(t *testing.T) {
	for _, fault := range []string{"missing", "phase-mismatch"} {
		t.Run(fault, func(t *testing.T) {
			f, row, _ := acceptedPublicationFixture(t)
			before := f.store.LookupByPath(f.w.Destination.Path)
			stmt := `DELETE FROM spool_replacement_publications WHERE spool_id=?`
			if fault == "phase-mismatch" {
				stmt = `UPDATE spool_replacement_publications SET phase='cleaned' WHERE spool_id=?`
			}
			if _, err := f.store.DB().Exec(stmt, row.ID); err != nil {
				t.Fatal(err)
			}
			epoch, err := f.store.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			stale := before.snapshot()
			stale.Version = MirrorToken{}
			stale.Size--
			if _, err := f.store.ScanUpsertBatch([]*Entry{stale}, epoch, nil); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("malformed accepted journal SCAN=%v", err)
			}
			if got := f.store.LookupByPath(before.Path); !sameReplacementEntry(got, before) {
				t.Fatalf("malformed journal clobbered mirror: %+v", got)
			}
		})
	}

	f, row, p := settledCleanupMetadataFixture(t)
	cleaning, err := f.store.BeginReplacementCleanup(p, "1:2", "1:3", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	cleaned, err := f.store.FinishReplacementCleanup(*cleaning, func() error { return nil })
	if err != nil || cleaned == nil || cleaned.Phase != ReplacementPublicationCleaned {
		t.Fatalf("cleanup=%+v err=%v", cleaned, err)
	}
	if got, _, _, err := readReplacementPublication(f.store.DB(), row.ID); err != nil || got == nil {
		t.Fatalf("cleaned receipt=%+v err=%v", got, err)
	}
	before := f.store.LookupByPath(f.w.Destination.Path)
	stale := before.snapshot()
	stale.Version = MirrorToken{}
	stale.Size++
	epoch, err := f.store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ScanUpsertBatch([]*Entry{stale}, epoch, nil); err != nil {
		t.Fatalf("valid cleaned journal did not release SCAN: %v", err)
	}
	if got := f.store.LookupByPath(stale.Path); got == nil || got.Size != stale.Size {
		t.Fatalf("cleaned SCAN result=%+v", got)
	}
	ordinaryDir := MakeEntry("ordinary-dir", true, 0, time.Unix(1700000600, 0), 704)
	if err := f.store.Insert(ordinaryDir); err != nil {
		t.Fatal(err)
	}
	var invalidated []string
	f.store.SetOnContentInvalidated(func(path string, isDir bool) {
		if !isDir {
			t.Fatalf("file used reentrant directory callback: %s", path)
		}
		invalidated = append(invalidated, path)
	})
	changedDir := ordinaryDir.snapshot()
	changedDir.Version = MirrorToken{}
	changedDir.Mtime = changedDir.Mtime.Add(time.Second)
	rc := &RedisClient{store: f.store, stopCh: make(chan struct{})}
	if err := rc.applyReconcileUpserts([]*Entry{changedDir}); err != nil {
		t.Fatalf("cleaned journal did not release directory reconcile: %v", err)
	}
	if len(invalidated) != 1 || invalidated[0] != ordinaryDir.Path {
		t.Fatalf("cleaned directory invalidation=%v", invalidated)
	}
}
