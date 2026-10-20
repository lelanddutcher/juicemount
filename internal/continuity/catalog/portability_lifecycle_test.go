package catalog

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPortabilityIncrementalRowsRejectBeforeReadingFooter(t *testing.T) {
	_, stream := loadPortabilityVector(t, "legacy")
	frames := streamFrames(t, stream)
	for _, tc := range []struct {
		name                               string
		frameCount                         int
		rowLimit                           int64
		assets, revisions, parents, events int
	}{
		{"first event", 2, 2, 0, 0, 0, 0}, {"parent edges", 3, 4, 1, 1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := bytes.NewReader(joinFrames(frames[:tc.frameCount]))
			readsBeyondEvent := 0
			unexpectedRead := errors.New("reader reached beyond the over-budget event")
			reader := portReadFunc(func(p []byte) (int, error) {
				if prefix.Len() == 0 {
					readsBeyondEvent++
					return 0, unexpectedRead
				}
				return prefix.Read(p)
			})
			path := filepath.Join(t.TempDir(), "restore.sqlite")
			limits := portabilityLimits()
			limits.MaxProjectionRows = tc.rowLimit
			if err := Restore(context.Background(), path, testLibrary, reader, limits); !errors.Is(err, ErrLimit) {
				t.Fatalf("incremental row budget: %v", err)
			}
			if readsBeyondEvent != 0 {
				t.Fatal("restore consumed input after an over-budget event")
			}
			a, r, p, e := queryIncompleteCounts(t, path)
			if a != tc.assets || r != tc.revisions || p != tc.parents || e != tc.events {
				t.Fatalf("over-budget event committed: %d/%d/%d/%d", a, r, p, e)
			}
		})
	}
	_, relational := loadPortabilityVector(t, "relational")
	path := filepath.Join(t.TempDir(), "tombstones.sqlite")
	limits := portabilityLimits()
	limits.MaxProjectionRows = 7
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(relational), limits); err != nil {
		t.Fatal("repeated revision/removal/reactivation invented rows:", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := projectionTotal(context.Background(), s.db)
	if closeErr := s.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || rows != 7 {
		t.Fatalf("retained tombstone row count: %d, %v", rows, err)
	}
	path = filepath.Join(t.TempDir(), "rollback-pair.sqlite")
	limits.MaxProjectionRows = 6
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(relational), limits); !errors.Is(err, ErrLimit) {
		t.Fatalf("pair row budget: %v", err)
	}
	db, err := openDB(path, true)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = projectionTotal(context.Background(), db)
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || rows != 5 {
		t.Fatalf("part of over-budget relationship/location batch survived: %d, %v", rows, err)
	}
}

type snapshotGateWriter struct {
	ctx     context.Context
	reached chan struct{}
	release chan struct{}
	once    sync.Once
	buffer  bytes.Buffer
}

func (w *snapshotGateWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.reached) })
	select {
	case <-w.release:
		return w.buffer.Write(p)
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
}

func TestPortabilitySnapshotAllowsWriterAndCloseDrainsExport(t *testing.T) {
	s, _, expected := vectorSource(t, "legacy")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	writer := &snapshotGateWriter{ctx: ctx, reached: make(chan struct{}), release: make(chan struct{})}
	exported := make(chan error, 1)
	go func() { exported <- s.Export(ctx, testLibrary, writer, portabilityLimits()) }()
	select {
	case <-writer.reached:
	case <-ctx.Done():
		close(writer.release)
		<-exported
		t.Fatal("export did not reach its admitted snapshot")
	}
	// Magic is written only after snapshot schema/count selection. A real write
	// must complete while that exact read snapshot and export remain blocked.
	_, writeErr := s.Apply(ctx, testOperation("during-export", Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "Scene <A> & café\u2028next", Label: "after snapshot"}}))
	closeAttempted := make(chan struct{})
	closed := make(chan error, 1)
	go func() { close(closeAttempted); closed <- s.Close() }()
	<-closeAttempted
	// Observe the pending writer in RWMutex, rather than assuming the Close
	// goroutine ran because a start notification or interval elapsed.
	pending, early := false, false
	var closeErr error
	for !pending && !early && ctx.Err() == nil {
		if !s.mu.TryRLock() {
			pending = true
			break
		}
		s.mu.RUnlock()
		select {
		case closeErr = <-closed:
			early = true
		default:
			runtime.Gosched()
		}
	}
	close(writer.release)
	exportErr := <-exported
	if !early {
		closeErr = <-closed
	}
	if !pending || early || writeErr != nil || exportErr != nil || closeErr != nil {
		t.Fatalf("snapshot/Close: pending=%v early=%v writer=%v export=%v close=%v", pending, early, writeErr, exportErr, closeErr)
	}
	if !bytes.Equal(writer.buffer.Bytes(), expected) {
		t.Fatal("export mixed pre/post-snapshot history or projections")
	}
	fresh, err := Open(s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	a, err := fresh.GetAsset(context.Background(), testLibrary, testAsset)
	if err != nil || a.Label != "after snapshot" || a.UpdatedSequence != 3 {
		t.Fatalf("concurrent writer did not actually commit: %+v, %v", a, err)
	}
	if _, err := s.LibraryID(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close did not finish: %v", err)
	}
}

func TestPortabilityCancellationRetainsIncompleteReplay(t *testing.T) {
	_, stream := loadPortabilityVector(t, "legacy")
	frames := streamFrames(t, stream)
	prefix := bytes.NewReader(joinFrames(frames[:2]))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := portReadFunc(func(p []byte) (int, error) {
		if prefix.Len() == 0 {
			cancel()
			return 0, context.Canceled
		}
		return prefix.Read(p)
	})
	path := filepath.Join(t.TempDir(), "restore.sqlite")
	if err := Restore(ctx, path, testLibrary, reader, portabilityLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore cancellation: %v", err)
	}
	a, r, p, e := queryIncompleteCounts(t, path)
	if a != 1 || r != 1 || p != 0 || e != 1 {
		t.Fatalf("cancellation lost prior incomplete batches: %d/%d/%d/%d", a, r, p, e)
	}
	s, _, _ := vectorSource(t, "empty")
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	writer := portWriteFunc(func(p []byte) (int, error) { cancel2(); return len(p), nil })
	if err := s.Export(ctx2, testLibrary, writer, portabilityLimits()); !errors.Is(err, context.Canceled) {
		t.Fatalf("export cancellation: %v", err)
	}
	if s.db.Stats().InUse != 0 {
		t.Fatal("canceled export held the Store write pool")
	}
}

func TestPortabilityFinalBarrierErrorHasUncertainCompleteTarget(t *testing.T) {
	_, stream := loadPortabilityVector(t, "relational")
	path := filepath.Join(t.TempDir(), "restore.sqlite")
	syncErr := errors.New("injected final directory barrier failure")
	calls := 0
	err := restoreCatalog(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits(), func(string) error { calls++; return syncErr })
	if !errors.Is(err, syncErr) || calls != 1 {
		t.Fatalf("barrier outcome: %v, calls %d", err, calls)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal("committed target not inspectable after uncertain barrier:", err)
	}
	before := legacyRecords(t, s.db)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream), portabilityLimits()); !errors.Is(err, ErrConflict) {
		t.Fatalf("restore adopted prior target: %v", err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(before, legacyRecords(t, s.db)) {
		t.Fatal("restore retry replaced uncertain completed target")
	}
}

type ownedPortReader struct {
	*bytes.Reader
	closed bool
}

func (r *ownedPortReader) Close() error { r.closed = true; return nil }

type ownedPortWriter struct {
	bytes.Buffer
	closed bool
}

func (w *ownedPortWriter) Close() error { w.closed = true; return nil }

func TestPortabilityCallerKeepsIOOwnership(t *testing.T) {
	s, _, stream := vectorSource(t, "empty")
	writer := &ownedPortWriter{}
	if err := s.Export(context.Background(), testLibrary, writer, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	reader := &ownedPortReader{Reader: bytes.NewReader(stream)}
	path := filepath.Join(t.TempDir(), "restore.sqlite")
	if err := Restore(context.Background(), path, testLibrary, reader, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	if writer.closed || reader.closed {
		t.Fatal("API closed caller-owned I/O")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
