package metadata

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// holdPublicationSQLPool first joins background SQL workers, then occupies the
// sole connection. Each caller must launch only its target operation before
// waiting for WaitCount; the global counter cannot identify competing waiters.
func holdPublicationSQLPool(t *testing.T, s *Store) (release func(), baseline int64) {
	t.Helper()
	if s.ftsStop != nil {
		t.Fatal("held SQL pool requires JM_FTS_DEFER=0")
	}
	if s.pruneLadderGCStop != nil {
		s.pruneLadderGCStopOnce.Do(func() { close(s.pruneLadderGCStop) })
		select {
		case <-s.pruneLadderGCDone:
		case <-time.After(5 * time.Second):
			t.Fatal("prune ladder GC did not stop before SQL pool hold")
		}
	}
	s.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = conn.Close() }) }
	t.Cleanup(release)
	return release, s.db.Stats().WaitCount
}

func waitForPublicationSQLWait(t *testing.T, s *Store, baseline int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.db.Stats().WaitCount == baseline && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.db.Stats().WaitCount == baseline {
		t.Fatal("batch never reached held SQL pool")
	}
}

func TestBulkSQLWaitAllowsCachedReadersAndOrdersCacheOnlySuccessor(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	if err := s.BulkInsert([]*Entry{
		MakeEntry("known", true, 0, stamp, 10),
		MakeEntry("known/clip.mov", false, 4096, stamp, 11),
	}, 500); err != nil {
		t.Fatal(err)
	}
	release, baseline := holdPublicationSQLPool(t, s)
	bulkDone := make(chan error, 1)
	bulkSettled := false
	go func() {
		bulkDone <- s.BulkInsert([]*Entry{MakeEntry("future.mov", false, 42, stamp, 100)}, 500)
	}()
	defer func() {
		release()
		if !bulkSettled {
			select {
			case <-bulkDone:
			case <-time.After(5 * time.Second):
				t.Error("bulk writer did not settle")
			}
		}
	}()
	waitForPublicationSQLWait(t, s, baseline)

	readDone := make(chan error, 1)
	go func() {
		path := s.lookupByPathRAM("known/clip.mov")
		inode := s.LookupByInode(11)
		children, err := s.listChildrenRAM("known")
		if err == nil && (path == nil || path.Size != 4096 || inode == nil || inode.Path != "known/clip.mov" || len(children) != 1 || children[0].Path != "known/clip.mov") {
			err = errors.New("cached path/inode/children payload changed")
		}
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		release()
		<-readDone
		t.Fatal("RAM cached readers waited behind unrelated SQL")
	}

	successorDone := make(chan struct{}, 1)
	go func() {
		s.InsertToCache(MakeEntry("future.mov", false, 77, stamp.Add(time.Second), 101))
		successorDone <- struct{}{}
	}()
	select {
	case <-successorDone:
		t.Fatal("cache-only successor overtook admitted bulk SQL")
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case err := <-bulkDone:
		bulkSettled = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk writer did not complete after SQL release")
	}
	select {
	case <-successorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("cache-only successor did not complete")
	}
	if got := s.lookupByPathRAM("future.mov"); got == nil || got.Inode != 101 || got.Size != 77 {
		t.Fatalf("cache-only successor lost after bulk commit: %+v", got)
	}
	var inode int64
	if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='future.mov'`).Scan(&inode); err != nil || inode != 100 {
		t.Fatalf("bulk durable row = inode %d, err %v; want 100", inode, err)
	}
}

func TestFailedBulkReleasesPublicationGateWithoutCacheOrSQLWrite(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	s, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("media.mov", false, 10, stamp, 10)); err != nil {
		t.Fatal(err)
	}
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	s.SetOnScanContentInvalidated(func(string, uint64) { callbacks++ })
	bad := MakeEntry("bad.mov", false, 1, stamp, 30)
	bad.Version = MirrorToken{Path: "bad.mov", StoreID: "impossible", Epoch: 1}
	release, baseline := holdPublicationSQLPool(t, s)
	result := make(chan error, 1)
	resultSettled := false
	go func() {
		_, err := s.ScanUpsertBatch([]*Entry{MakeEntry("media.mov", false, 20, stamp, 20), bad}, epoch, []string{"media.mov"})
		result <- err
	}()
	defer func() {
		release()
		if !resultSettled {
			select {
			case <-result:
			case <-time.After(5 * time.Second):
				t.Error("failed batch did not settle")
			}
		}
	}()
	waitForPublicationSQLWait(t, s, baseline)
	successorDone := make(chan struct{}, 1)
	go func() {
		s.InsertToCache(MakeEntry("media.mov", false, 30, stamp.Add(time.Second), 31))
		successorDone <- struct{}{}
	}()
	select {
	case <-successorDone:
		t.Fatal("cache-only successor passed blocked failed batch")
	case <-time.After(25 * time.Millisecond):
	}
	release()
	select {
	case err := <-result:
		resultSettled = true
		if !errors.Is(err, ErrMirrorConflict) {
			t.Fatalf("failed batch = %v, want mirror conflict", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed batch did not return")
	}
	select {
	case <-successorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("cache-only successor remained behind failed batch")
	}
	if callbacks != 0 {
		t.Fatalf("conflicted batch invalidated %d old contents", callbacks)
	}
	if got := s.lookupByPathRAM("media.mov"); got == nil || got.Inode != 31 || got.Size != 30 {
		t.Fatalf("post-failure cache-only successor = %+v", got)
	}
	var durableSize int64
	if err := s.db.QueryRow(`SELECT size FROM entries WHERE path='media.mov'`).Scan(&durableSize); err != nil || durableSize != 10 {
		t.Fatalf("rolled-back durable size = %d, err %v; want 10", durableSize, err)
	}
	var badCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM entries WHERE path='bad.mov'`).Scan(&badCount); err != nil || badCount != 0 {
		t.Fatalf("rolled-back bad row count = %d, err %v", badCount, err)
	}
}

func TestRecoverShadowInvalidatesOlderScanEpoch(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("recover.mov", false, 10, stamp, 10)); err != nil {
		t.Fatal(err)
	}
	s.DeleteFromCache("recover.mov")
	rec, ok := s.LookupRecentlyEvicted(10)
	if !ok {
		t.Fatal("evicted shadow missing")
	}
	old, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if recovered := s.RecoverShadow(rec, 10); recovered == nil || recovered.Inode != 10 {
		t.Fatalf("recovered shadow = %+v", recovered)
	}
	current, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if current.CacheGeneration != old.CacheGeneration+1 {
		t.Fatalf("recovery generation %d, want %d", current.CacheGeneration, old.CacheGeneration+1)
	}
	callbackCount := 0
	s.SetOnScanContentInvalidated(func(string, uint64) { callbackCount++ })
	_, err = s.ScanUpsertBatch([]*Entry{MakeEntry("recover.mov", false, 20, stamp, 20)}, old, []string{"recover.mov"})
	if !errors.Is(err, ErrMirrorConflict) || callbackCount != 0 {
		t.Fatalf("stale epoch result=%v callbacks=%d", err, callbackCount)
	}
	if got := s.lookupByPathRAM("recover.mov"); got == nil || got.Inode != 10 || got.Size != 10 {
		t.Fatalf("stale scan changed recovered cache: %+v", got)
	}
}

func TestAdmittedBulkCallbackSeesExactOldPreimageWithoutCacheLock(t *testing.T) {
	s := newTestStore(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("media.mov", false, 10, stamp, 10)); err != nil {
		t.Fatal(err)
	}
	old, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	var observed []uint64
	s.SetOnScanContentInvalidated(func(path string, inode uint64) {
		if path != "media.mov" {
			return
		}
		if current := s.lookupByPathRAM(path); current == nil || current.Inode != 10 {
			observed = append(observed, 0)
		} else {
			observed = append(observed, inode)
		}
	})
	next, err := s.ScanUpsertBatch([]*Entry{MakeEntry("media.mov", false, 20, stamp.Add(time.Second), 20)}, old, []string{"media.mov"})
	if err != nil {
		t.Fatal(err)
	}
	if len(observed) != 1 || observed[0] != 10 {
		t.Fatalf("callback old preimages %v, want [10]", observed)
	}
	if next.CacheGeneration != old.CacheGeneration || next.Epoch <= old.Epoch {
		t.Fatalf("returned scan epoch %+v after %+v", next, old)
	}
	if got := s.lookupByPathRAM("media.mov"); got == nil || got.Inode != 20 || got.Size != 20 {
		t.Fatalf("committed cache entry = %+v", got)
	}
	var durableInode int64
	if err := s.db.QueryRow(`SELECT inode FROM entries WHERE path='media.mov'`).Scan(&durableInode); err != nil {
		t.Fatal(err)
	}
	if durableInode != 20 {
		t.Fatalf("committed durable inode = %d, want 20", durableInode)
	}
}
