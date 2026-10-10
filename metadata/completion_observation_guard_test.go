package metadata

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func completionObservationFixture(t *testing.T, variant string) (*Store, *SpoolStore, DrainCommitItem, DrainCommitResult) {
	t.Helper()
	s, sp := openStoreWithSpool(t)
	if err := s.Insert(MakeEntry("folder", true, 0, time.Unix(1700000000, 0), 81)); err != nil {
		t.Fatal(err)
	}
	name := "folder/zero.mov" // The fixture inode has its high bit set.
	var item DrainCommitItem
	var result DrainCommitResult
	{
		id := seedReadyRow(t, s, sp, name, 19)
		item = DrainCommitItem{SpoolID: id, NFSPath: name, SpoolFile: "/spool/files/" + filepath.Base(name), Size: 19, Mtime: time.Unix(1800000000, 0), BackingIdentity: "41:42"}
		if variant == "fallback" {
			if err := sp.HoldDrainCompletion(item); err != nil {
				t.Fatal(err)
			}
		} else {
			if variant == "legacy-incomplete" {
				if _, err := s.DB().Exec(`CREATE TRIGGER stop_done BEFORE UPDATE ON entries BEGIN SELECT RAISE(ABORT,'lost completion'); END`); err != nil {
					t.Fatal(err)
				}
			}
			rs, err := s.BatchDrainCompleteExact([]DrainCommitItem{item})
			if variant == "legacy-incomplete" {
				if err == nil {
					t.Fatal("injected completion succeeded")
				}
				if _, err := s.DB().Exec(`DROP TRIGGER stop_done`); err != nil {
					t.Fatal(err)
				}
			} else {
				if err != nil || len(rs) != 1 || !rs[0].Done {
					t.Fatalf("completion: %+v %v", rs, err)
				}
				result = rs[0]
			}
		}
	}
	return s, sp, item, result
}

func TestCompletionObservationsPreserveCustody(t *testing.T) {
	for _, variant := range []string{"legacy", "legacy-incomplete", "fallback"} {
		t.Run(variant, func(t *testing.T) {
			s, _, item, _ := completionObservationFixture(t, variant)
			before := s.LookupByPath(item.NFSPath).snapshot()
			parent := s.LookupByPath("folder").snapshot()
			rc := &RedisClient{store: s, stopCh: make(chan struct{})}
			var callbacks []string
			s.SetOnContentInvalidated(func(p string, _ bool) { callbacks = append(callbacks, p) })
			s.SetOnPathInvalidated(func(p string, _ bool) { callbacks = append(callbacks, p) })
			s.SetOnScanContentInvalidated(func(p string, _ uint64) { callbacks = append(callbacks, p) })
			for _, evt := range []MetadataEvent{
				{Op: "update", Path: item.NFSPath, Inode: 999, Size: 2},
				{Op: "delete", Path: item.NFSPath},
				{Op: "rename", OldPath: item.NFSPath, Path: "moved.mov", Inode: 999},
				{Op: "rename", OldPath: "other.mov", Path: item.NFSPath, Inode: 999},
				{Op: "update", Path: "folder", Inode: 81, IsDir: true},
				{Op: "create", Path: "alias.mov", Inode: before.Inode},
				{Op: "create", Path: "unrelated-dir", Inode: 998, IsDir: true},
			} {
				rc.applyEvent(evt)
			}
			if len(callbacks) != 0 || !sameReplacementEntry(s.LookupByPath(item.NFSPath), before) || !sameReplacementEntry(s.LookupByPath("folder"), parent) {
				t.Fatalf("subscription changed held image/callbacks=%v", callbacks)
			}
			stale := MakeEntry(item.NFSPath, false, 2, time.Unix(1900000000, 0), 999)
			alias := MakeEntry("alias.mov", false, 3, time.Unix(1900000000, 0), before.Inode)
			sibling := MakeEntry("folder/sibling.mov", false, 7, time.Unix(1900000000, 0), 900)
			for _, ingress := range []string{"scan", "keyspace"} {
				epoch, err := s.CaptureStoreEpoch()
				if err != nil {
					t.Fatal(err)
				}
				if ingress == "scan" {
					_, err = s.ScanUpsertBatch([]*Entry{stale, alias, sibling}, epoch, []string{item.NFSPath})
				} else {
					err = rc.applyReconcileUpserts([]*Entry{stale, alias, sibling})
				}
				if err != nil {
					t.Fatalf("%s: %v", ingress, err)
				}
				if !sameReplacementEntry(s.LookupByPath(item.NFSPath), before) || s.LookupByPath("alias.mov") != nil {
					t.Fatalf("%s changed held image/alias", ingress)
				}
				if got := s.LookupByPath(sibling.Path); got == nil || got.Size != 7 {
					t.Fatalf("%s suppressed sibling", ingress)
				}
				for _, p := range callbacks {
					if p != sibling.Path {
						t.Fatalf("%s invalidated held content %s", ingress, p)
					}
				}
			}
			callbacks = nil
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			next, err := s.ScanClearLocalOnlyBatch([]string{item.NFSPath}, epoch)
			if err != nil {
				t.Fatal(err)
			}
			if !sameReplacementEntry(s.LookupByPath(item.NFSPath), before) {
				t.Fatal("SCAN clear changed hold")
			}
			if _, err := s.ScanDeletePathsBatch([]string{item.NFSPath}, next); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("SCAN prune=%v", err)
			}
			if err := s.DeletePathsConditional([]*Entry{before}); !errors.Is(err, ErrMirrorConflict) {
				t.Fatalf("conditional prune=%v", err)
			}
			if len(callbacks) != 0 {
				t.Fatalf("prune invalidated held content: %v", callbacks)
			}
			rc.applyEvent(MetadataEvent{Op: "update", Path: sibling.Path, Inode: 900, Size: 11})
			if got := s.LookupByPath(sibling.Path); got == nil || got.Size != 11 {
				t.Fatal("subscription sibling suppressed")
			}
		})
	}
}

func TestCompletionObservationMalformedHoldFailsClosed(t *testing.T) {
	for _, mutation := range []string{"missing-row", "path", "size", "kind"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, item, _ := completionObservationFixture(t, "legacy")
			query := map[string]string{
				"missing-row": `DELETE FROM spool_entries WHERE id=?`,
				"path":        `UPDATE spool_completion_holds SET nfs_path='../bad' WHERE spool_id=?`,
				"size":        `UPDATE spool_completion_holds SET size=size+1 WHERE spool_id=?`,
				"kind":        `UPDATE spool_completion_holds SET kind='unknown' WHERE spool_id=?`,
			}[mutation]
			if _, err := s.DB().Exec(query, item.SpoolID); err != nil {
				t.Fatal(err)
			}
			rc := &RedisClient{store: s, stopCh: make(chan struct{})}
			s.SetOnContentInvalidated(func(string, bool) { t.Error("malformed custody admitted callback") })
			rc.applyEvent(MetadataEvent{Op: "create", Path: "other.mov", Inode: 909, Size: 11})
			if s.LookupByPath("other.mov") != nil {
				t.Fatal("malformed custody admitted event")
			}
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.ScanUpsertBatch([]*Entry{MakeEntry("other.mov", false, 11, time.Now(), 909)}, epoch, nil); err == nil {
				t.Fatal("malformed custody admitted SCAN")
			}
		})
	}
}

func TestCompletionAdmissionWaitsForSubscription(t *testing.T) {
	for _, variant := range []string{"legacy", "fallback"} {
		t.Run(variant, func(t *testing.T) {
			s, sp := openStoreWithSpool(t)
			var admit func() error
			{
				id := seedReadyRow(t, s, sp, "held.mov", 19)
				item := DrainCommitItem{SpoolID: id, NFSPath: "held.mov", SpoolFile: "/spool/files/held.mov", Size: 19, Mtime: time.Unix(1800000000, 0), BackingIdentity: "41:42"}
				admit = func() error {
					if variant == "fallback" {
						return sp.HoldDrainCompletion(item)
					}
					_, err := s.BatchDrainCompleteExact([]DrainCommitItem{item})
					return err
				}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			s.SetOnContentInvalidated(func(string, bool) {
				assertCompletionObservationGate(t, s)
				close(entered)
				<-release
			})
			rc := &RedisClient{store: s, stopCh: make(chan struct{})}
			eventDone := make(chan struct{})
			go func() {
				rc.applyEvent(MetadataEvent{Op: "create", Path: "sibling.mov", Inode: 901, Size: 11})
				close(eventDone)
			}()
			<-entered
			done := make(chan error, 1)
			go func() { done <- admit() }()
			select {
			case err := <-done:
				t.Fatalf("admission crossed active callback: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			var count int
			if err := s.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("hold committed under callback: %d %v", count, err)
			}
			once.Do(func() { close(release) })
			<-eventDone
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := s.DB().QueryRow(`SELECT count(*) FROM spool_completion_holds`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("missing admitted hold: %d %v", count, err)
			}
		})
	}
}

func TestCompletionFallbackWaitsForScanCallback(t *testing.T) {
	s, sp := openStoreWithSpool(t)
	id := seedReadyRow(t, s, sp, "held.mov", 19)
	item := DrainCommitItem{SpoolID: id, NFSPath: "held.mov", SpoolFile: "/spool/files/held.mov", Size: 19}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	s.SetOnScanContentInvalidated(func(string, uint64) {
		assertCompletionWriterGate(t, s)
		close(entered)
		<-release
	})
	epoch, err := s.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	scanDone := make(chan error, 1)
	go func() {
		_, err := s.ReconcileUpsertBatch([]*Entry{MakeEntry("held.mov", false, 19, time.Now(), 902)}, epoch)
		scanDone <- err
	}()
	<-entered
	done := make(chan error, 1)
	go func() { done <- sp.HoldDrainCompletion(item) }()
	select {
	case err := <-done:
		t.Fatalf("fallback crossed SCAN callback: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	if err := <-scanDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCompletionCleanupReleasesObservation(t *testing.T) {
	s, sp, item, result := completionObservationFixture(t, "legacy")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	cleanup := make(chan error, 1)
	go func() {
		cleanup <- sp.CleanupDrainCompletion(item, result, func() error {
			assertCompletionObservationGate(t, s)
			assertCompletionWriterGate(t, s)
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	rc := &RedisClient{store: s, stopCh: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		rc.applyEvent(MetadataEvent{Op: "update", Path: item.NFSPath, Inode: 903, Size: 23})
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("event crossed in-progress cleanup")
	case <-time.After(25 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	if err := <-cleanup; err != nil {
		t.Fatal(err)
	}
	<-done
	if got := s.LookupByPath(item.NFSPath); got == nil || got.Inode != 903 || got.Size != 23 {
		t.Fatalf("cleanup did not release event: %+v", got)
	}
}

func TestCompletionUnwiredMutationRefused(t *testing.T) {
	s, _, item, result := completionObservationFixture(t, "legacy")
	standalone := NewSpoolStore(s.DB())
	if err := standalone.CleanupDrainCompletion(item, result, func() error { t.Fatal("unwired disposal"); return nil }); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("unwired cleanup=%v", err)
	}
	if err := standalone.HoldDrainCompletion(DrainCommitItem{SpoolID: 999, NFSPath: "other.mov", SpoolFile: filepath.Join(t.TempDir(), "private")}); !errors.Is(err, ErrMirrorConflict) {
		t.Fatalf("unwired admission=%v", err)
	}
}

func assertCompletionObservationGate(t *testing.T, s *Store) {
	t.Helper()
	if s.replacementObservationMu.TryLock() {
		s.replacementObservationMu.Unlock()
		t.Error("callback ran without the observation gate")
	}
}

func assertCompletionWriterGate(t *testing.T, s *Store) {
	t.Helper()
	if s.writeMu.TryLock() {
		s.writeMu.Unlock()
		t.Error("callback ran without the Store writer gate")
	}
}

func TestCompletionHighBitAliasAfterRAMEviction(t *testing.T) {
	for _, variant := range []string{"legacy", "fallback"} {
		t.Run(variant, func(t *testing.T) {
			s, _, item, _ := completionObservationFixture(t, variant)
			before := s.lookupByPathSQLite(item.NFSPath).snapshot()
			if before.Inode&(uint64(1)<<63) == 0 {
				t.Fatal("fixture lacks high-bit inode")
			}
			s.DeleteFromCache(item.NFSPath)
			if s.lookupByPathRAM(item.NFSPath) != nil {
				t.Fatal("fixture not evicted")
			}
			var callbacks int
			s.SetOnContentInvalidated(func(string, bool) { callbacks++ })
			s.SetOnScanContentInvalidated(func(string, uint64) { callbacks++ })
			rc := &RedisClient{store: s, stopCh: make(chan struct{})}
			rc.applyEvent(MetadataEvent{Op: "create", Path: "alias.mov", Inode: before.Inode, Size: 1})
			epoch, err := s.CaptureStoreEpoch()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.ScanUpsertBatch([]*Entry{MakeEntry("scan-alias.mov", false, 1, time.Now(), before.Inode)}, epoch, nil); err != nil {
				t.Fatal(err)
			}
			if callbacks != 0 || s.lookupByPathRAM("alias.mov") != nil || s.lookupByPathSQLite("alias.mov") != nil ||
				s.lookupByPathRAM("scan-alias.mov") != nil || s.lookupByPathSQLite("scan-alias.mov") != nil {
				t.Fatal("evicted high-bit alias crossed ownership guard")
			}
			if got := s.lookupByPathSQLite(item.NFSPath); !sameReplacementEntry(got, before) {
				t.Fatalf("durable held image changed: %+v", got)
			}
		})
	}
}
