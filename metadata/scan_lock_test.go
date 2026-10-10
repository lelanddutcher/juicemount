package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestCaptureStoreEpochReleasesCacheLockDuringSQLAndOrdersPublication(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("known.mov", false, 12, stamp, 12)); err != nil {
		t.Fatal(err)
	}
	before, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	release, baseline := holdPublicationSQLPool(t, s)
	type captureResult struct {
		epoch StoreEpoch
		err   error
	}
	result := make(chan captureResult, 1)
	settled := false
	successorDone := make(chan struct{}, 1)
	successorStarted := false
	successorSettled := false
	go func() {
		epoch, err := s.CaptureStoreEpoch()
		result <- captureResult{epoch, err}
	}()
	defer func() {
		release()
		if !settled {
			select {
			case <-result:
			case <-time.After(5 * time.Second):
				t.Error("epoch capture did not settle")
			}
		}
		if successorStarted && !successorSettled {
			select {
			case <-successorDone:
			case <-time.After(5 * time.Second):
				t.Error("cache-only publisher did not settle")
			}
		}
	}()
	waitForPublicationSQLWait(t, s, baseline)
	if !s.mu.TryLock() {
		release()
		t.Fatal("CaptureStoreEpoch held cache lock across SQL wait")
	}
	s.mu.Unlock()
	readDone := make(chan *Entry, 1)
	go func() { readDone <- s.lookupByPathRAM("known.mov") }()
	select {
	case e := <-readDone:
		if e == nil || e.Inode != 12 || e.Size != 12 {
			t.Fatalf("cached read during epoch capture = %+v", e)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		select {
		case <-readDone:
		case <-time.After(5 * time.Second):
			t.Error("cached reader did not settle after SQL release")
		}
		t.Fatal("cached read waited behind epoch SQL")
	}
	successorStartedSignal := make(chan struct{})
	go func() {
		close(successorStartedSignal)
		s.InsertToCache(MakeEntry("new.mov", false, 13, stamp, 13))
		successorDone <- struct{}{}
	}()
	successorStarted = true
	<-successorStartedSignal
	select {
	case <-successorDone:
		successorSettled = true
		t.Fatal("cache-only publication overtook epoch capture")
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case captured := <-result:
		settled = true
		if captured.err != nil || captured.epoch != before {
			t.Fatalf("capture = %+v, %v; want %+v", captured.epoch, captured.err, before)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("epoch capture did not complete")
	}
	select {
	case <-successorDone:
		successorSettled = true
	case <-time.After(5 * time.Second):
		t.Fatal("queued cache-only publisher did not complete")
	}
	after, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if after.StoreID != before.StoreID || after.Epoch != before.Epoch || after.CacheGeneration != before.CacheGeneration+1 {
		t.Fatalf("post-successor epoch %+v, want durable epoch unchanged and cache generation +1 from %+v", after, before)
	}
}

func TestScanClearDuplicateRollsBackAllRowsAndCache(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	a := MakeEntry("a.mov", false, 10, stamp, 10)
	b := MakeEntry("b.mov", false, 11, stamp, 11)
	a.LocalOnly, b.LocalOnly = true, true
	if err := s.Insert(a); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(b); err != nil {
		t.Fatal(err)
	}
	before := s.lookupByPathRAM("a.mov")
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScanClearLocalOnlyBatch([]string{"a.mov", "b.mov", "a.mov"}, epoch); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("duplicate clear = %v, want mirror conflict", err)
	}
	for _, path := range []string{"a.mov", "b.mov"} {
		e := s.lookupByPathRAM(path)
		if e == nil || !e.LocalOnly {
			t.Fatalf("rollback cache %s = %+v", path, e)
		}
		var localOnly int
		if err := s.db.QueryRow(`SELECT local_only FROM entries WHERE path=?`, path).Scan(&localOnly); err != nil || localOnly != 1 {
			t.Fatalf("rollback durable %s local_only=%d err=%v", path, localOnly, err)
		}
	}
	if got := s.lookupByPathRAM("a.mov"); got.Version != before.Version {
		t.Fatalf("rollback changed cached token: before=%+v after=%+v", before.Version, got.Version)
	}
	after, err := s.CaptureStoreEpoch()
	if err != nil || after != epoch {
		t.Fatalf("rollback epoch = %+v, %v; want %+v", after, err, epoch)
	}
}

func TestScanBatchesRejectPreAdmissionCacheOnlySuccessor(t *testing.T) {
	for _, operation := range []string{"clear", "delete"} {
		t.Run(operation, func(t *testing.T) {
			s := newTestStore(t)
			stamp := time.Unix(1700000000, 0)
			original := MakeEntry("media.mov", false, 10, stamp, 10)
			original.LocalOnly = true
			if err := s.Insert(original); err != nil {
				t.Fatal(err)
			}
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			s.InsertToCache(MakeEntry("media.mov", false, 99, stamp.Add(time.Second), 99))
			callbacks := 0
			s.SetOnScanContentInvalidated(func(string, uint64) { callbacks++ })
			if operation == "clear" {
				_, err = s.ScanClearLocalOnlyBatch([]string{"media.mov"}, epoch)
			} else {
				_, err = s.ScanDeletePathsBatch([]string{"media.mov"}, epoch)
			}
			if !errors.Is(err, ErrMirrorConflict) || callbacks != 0 {
				t.Fatalf("%s after cache successor = %v, callbacks=%d", operation, err, callbacks)
			}
			if got := s.lookupByPathRAM("media.mov"); got == nil || got.Inode != 99 || got.Size != 99 {
				t.Fatalf("%s lost cache-only winner: %+v", operation, got)
			}
			var inode int64
			var localOnly int
			if err := s.db.QueryRow(`SELECT inode, local_only FROM entries WHERE path='media.mov'`).Scan(&inode, &localOnly); err != nil || inode != 10 || localOnly != 1 {
				t.Fatalf("%s changed durable row inode=%d local_only=%d err=%v", operation, inode, localOnly, err)
			}
		})
	}
}

func TestScanBatchesQueueCacheOnlySuccessorThroughHeldSQL(t *testing.T) {
	for _, operation := range []string{"clear", "delete"} {
		t.Run(operation, func(t *testing.T) {
			t.Setenv("JM_FTS_DEFER", "0")
			t.Setenv("JM_SERVE_FROM_SQLITE", "0")
			s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			stamp := time.Unix(1700000000, 0)
			original := MakeEntry("media.mov", false, 10, stamp, 10)
			original.LocalOnly = true
			if err := s.Insert(original); err != nil {
				t.Fatal(err)
			}
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			release, baseline := holdPublicationSQLPool(t, s)
			type batchResult struct {
				epoch StoreEpoch
				err   error
			}
			batchDone := make(chan batchResult, 1)
			settled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() {
				var next StoreEpoch
				var err error
				if operation == "clear" {
					next, err = s.ScanClearLocalOnlyBatch([]string{"media.mov"}, epoch)
				} else {
					next, err = s.ScanDeletePathsBatch([]string{"media.mov"}, epoch)
				}
				batchDone <- batchResult{next, err}
			}()
			defer func() {
				release()
				if !settled {
					select {
					case <-batchDone:
					case <-time.After(5 * time.Second):
						t.Error("scan batch did not settle")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("queued cache-only successor did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				release()
				t.Fatal("scan batch did not retain publication gate during SQL wait")
			}
			successorStartedSignal := make(chan struct{})
			go func() {
				close(successorStartedSignal)
				s.InsertToCache(MakeEntry("media.mov", false, 99, stamp.Add(time.Second), 99))
				successorDone <- struct{}{}
			}()
			successorStarted = true
			<-successorStartedSignal
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor overtook scan SQL")
			case <-time.After(25 * time.Millisecond):
			}
			release()
			var next StoreEpoch
			select {
			case result := <-batchDone:
				settled = true
				if result.err != nil {
					t.Fatal(result.err)
				}
				next = result.epoch
			case <-time.After(5 * time.Second):
				t.Fatal("scan batch did not finish after SQL release")
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not follow scan")
			}
			if got := s.lookupByPathRAM("media.mov"); got == nil || got.Inode != 99 || got.Size != 99 {
				t.Fatalf("%s lost queued successor: %+v", operation, got)
			}
			var durableInode int64
			var localOnly int
			err = s.db.QueryRow(`SELECT inode, local_only FROM entries WHERE path='media.mov'`).Scan(&durableInode, &localOnly)
			if operation == "clear" {
				if err != nil || durableInode != 10 || localOnly != 0 {
					t.Fatalf("clear durable row inode=%d local_only=%d err=%v", durableInode, localOnly, err)
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("delete durable row error=%v, want no rows", err)
			}
			after, err := s.CaptureStoreEpoch()
			if err != nil || after.StoreID != next.StoreID || after.Epoch != next.Epoch || after.CacheGeneration != next.CacheGeneration+1 {
				t.Fatalf("post-successor epoch %+v, %v; scan epoch %+v", after, err, next)
			}
		})
	}
}

func TestScanDeleteAbsentCachedParentStillRejectsCacheOnlyChild(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("dir", true, 0, stamp, 10)); err != nil {
		t.Fatal(err)
	}
	s.DeleteFromCache("dir")
	s.InsertToCache(MakeEntry("dir/child.mov", false, 12, stamp, 11))
	if got := s.lookupByPathRAM("dir"); got != nil {
		t.Fatalf("test setup retained parent in RAM: %+v", got)
	}
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	s.SetOnScanContentInvalidated(func(string, uint64) { callbacks++ })
	if _, err := s.ScanDeletePathsBatch([]string{"dir"}, epoch); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("prune with cache-only child = %v, want mirror conflict", err)
	}
	if callbacks != 0 || s.lookupByPathRAM("dir/child.mov") == nil {
		t.Fatalf("conflicted prune changed child or invoked %d callbacks", callbacks)
	}
	var inode int64
	if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='dir'`).Scan(&inode); err != nil || inode != 10 {
		t.Fatalf("conflicted prune durable parent inode=%d err=%v", inode, err)
	}
}

func TestScanDeleteCallbackReadsRAMAndInvalidatesDifferingPreimages(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("media.mov", false, 10, stamp, 42)); err != nil {
		t.Fatal(err)
	}
	s.InsertToCache(MakeEntry("media.mov", false, 20, stamp.Add(time.Second), 44))
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	var callbackErr error
	s.SetOnScanContentInvalidated(func(path string, oldInode uint64) {
		if !s.mu.TryRLock() {
			callbackErr = errors.New("prune callback ran under cache writer lock")
			return
		}
		s.mu.RUnlock()
		if e := s.lookupByPathRAM(path); e == nil || e.Inode != 44 {
			callbackErr = fmt.Errorf("callback cache preimage = %+v, want inode 44", e)
			return
		}
		observed = append(observed, oldInode)
	})
	next, err := s.ScanDeletePathsBatch([]string{"media.mov"}, epoch)
	if err != nil || callbackErr != nil {
		t.Fatalf("prune = %v, callback = %v", err, callbackErr)
	}
	sort.Slice(observed, func(i, j int) bool { return observed[i] < observed[j] })
	if len(observed) != 2 || observed[0] != 42 || observed[1] != 44 {
		t.Fatalf("callback preimages %v, want [42 44]", observed)
	}
	if next.CacheGeneration != epoch.CacheGeneration || next.Epoch <= epoch.Epoch {
		t.Fatalf("next epoch %+v after %+v", next, epoch)
	}
	if got := s.lookupByPathRAM("media.mov"); got != nil {
		t.Fatalf("prune retained cache row: %+v", got)
	}
	var durable int
	if err := s.db.QueryRow(`SELECT 1 FROM entries WHERE path='media.mov'`).Scan(&durable); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("prune durable row error=%v value=%d, want no rows", err, durable)
	}
}
