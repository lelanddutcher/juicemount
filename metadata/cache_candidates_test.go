package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the real reconcileDir publication seam. A parsed Redis child is
// only a candidate after its fenced mirror transaction and final token check.
func TestCacheCandidateReconcilePublicationFence(t *testing.T) {
	newFixture := func(t *testing.T) (*Store, *RedisClient, *atomic.Int64) {
		t.Helper()
		s := openTestStore(t)
		var raw [9]byte
		raw[0] = jfsTypeFile
		binary.BigEndian.PutUint32(raw[5:], 42)
		rc := &RedisClient{
			store: s, stopCh: make(chan struct{}),
			testReadDirRaw: func(uint64) (map[string]string, error) {
				return map[string]string{"._clip.mov": string(raw[:])}, nil
			},
			testReadAttrs: func([]string) ([]interface{}, error) {
				return []interface{}{string(encodeTestAttr(1700000000, 4096))}, nil
			},
		}
		rc.SetBackendQualified(true)
		count := &atomic.Int64{}
		rc.SetCacheCandidateSink(func(CacheCandidate) { count.Add(1) })
		t.Cleanup(func() { close(rc.stopCh); rc.attrRetryWG.Wait() })
		return s, rc, count
	}

	t.Run("committed", func(t *testing.T) {
		s, rc, count := newFixture(t)
		if err := rc.reconcileDir(1); err != nil {
			t.Fatal(err)
		}
		if count.Load() != 1 || s.LookupByPath("._clip.mov") == nil {
			t.Fatalf("committed candidate count=%d row=%+v", count.Load(), s.LookupByPath("._clip.mov"))
		}
	})

	t.Run("failed_sql", func(t *testing.T) {
		s, rc, count := newFixture(t)
		if _, err := s.DB().Exec(`CREATE TRIGGER fail_candidate BEFORE INSERT ON entries
			WHEN NEW.path='._clip.mov' BEGIN SELECT RAISE(FAIL,'fault'); END`); err != nil {
			t.Fatal(err)
		}
		if err := rc.reconcileDir(1); err == nil {
			t.Fatal("faulted publication acknowledged")
		}
		if count.Load() != 0 || s.LookupByPath("._clip.mov") != nil {
			t.Fatalf("failed publication emitted hint=%d or row=%+v", count.Load(), s.LookupByPath("._clip.mov"))
		}
	})

	t.Run("backend_generation", func(t *testing.T) {
		s, rc, count := newFixture(t)
		entered, release := make(chan struct{}), make(chan struct{})
		rc.testReadDirRaw = func(uint64) (map[string]string, error) {
			close(entered)
			<-release
			var raw [9]byte
			raw[0] = jfsTypeFile
			binary.BigEndian.PutUint32(raw[5:], 42)
			return map[string]string{"._clip.mov": string(raw[:])}, nil
		}
		done := make(chan error, 1)
		go func() { done <- rc.reconcileDir(1) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("reconcile did not enter held Redis read")
		}
		rc.SetBackendQualified(false)
		rc.SetBackendQualified(true)
		close(release)
		select {
		case err := <-done:
			if !errors.Is(err, errScanPublicationInvalidated) {
				t.Fatalf("stale reconcile = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("stale reconcile did not settle")
		}
		if count.Load() != 0 || s.LookupByPath("._clip.mov") != nil {
			t.Fatalf("old backend emitted hint=%d or row=%+v", count.Load(), s.LookupByPath("._clip.mov"))
		}
	})

	t.Run("successor_origin", func(t *testing.T) {
		s, rc, count := newFixture(t)
		// The first fenced upsert succeeds, then a newer local incarnation wins
		// before post-commit hint emission. Never offer the old incarnation.
		rc.testAfterScanUpsertChunk = func() {
			if err := s.Insert(MakeEntry("._clip.mov", false, 8192, time.Unix(1700000100, 0), 43)); err != nil {
				t.Error(err)
			}
		}
		rc.testScanBatch = func(_ context.Context, _, _ string) ([]string, error) {
			return []string{"0", "v2:1:1:1700000000:0:4096:42:1:._clip.mov"}, nil
		}
		if err := rc.SyncOnce(); err != nil && !errors.Is(err, errScanPublicationInvalidated) {
			t.Fatal(err)
		}
		if count.Load() != 0 {
			t.Fatalf("old origin emitted %d hints", count.Load())
		}
		if row := s.LookupByPath("._clip.mov"); row == nil || row.Inode != 43 {
			t.Fatalf("successor row = %+v", row)
		}
	})

	t.Run("scan_committed_control", func(t *testing.T) {
		s, rc, count := newFixture(t)
		rc.testScanBatch = func(_ context.Context, _, _ string) ([]string, error) {
			return []string{"0", "v2:1:1:1700000000:0:4096:42:1:._clip.mov"}, nil
		}
		if err := rc.SyncOnce(); err != nil {
			t.Fatal(err)
		}
		if count.Load() != 1 || s.LookupByPath("._clip.mov") == nil {
			t.Fatalf("committed SCAN candidate count=%d row=%+v", count.Load(), s.LookupByPath("._clip.mov"))
		}
	})
}
