package metadata

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func conditionalPruneLockFixture(t *testing.T) (*Store, string, []*Entry) {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_SERVE_FROM_SQLITE", "0")
	file := filepath.Join(t.TempDir(), "mirror.db")
	s, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	stamp := time.Unix(1700000000, 0)
	if err := s.BulkInsert([]*Entry{
		MakeEntry("pruned", true, 0, stamp, 20),
		MakeEntry("pruned/old.mov", false, 42, stamp, 21),
	}, 500); err != nil {
		t.Fatal(err)
	}
	return s, file, []*Entry{s.lookupByPathRAM("pruned"), s.lookupByPathRAM("pruned/old.mov")}
}

func TestConditionalPrunePreAdmissionCacheWinnerRejectsCommit(t *testing.T) {
	for _, caseName := range []string{"replacement", "new-child-with-parent-evicted"} {
		t.Run(caseName, func(t *testing.T) {
			s, _, selected := conditionalPruneLockFixture(t)
			stamp := time.Unix(1700000000, 0)
			if caseName == "new-child-with-parent-evicted" {
				s.DeleteFromCache("pruned")
				selected[0] = mirrorDurableEntry(t, s, "pruned")
			}
			calls := 0
			s.SetOnScanContentInvalidated(func(string, uint64) { calls++ })
			release, baseline := holdPublicationSQLPool(t, s)
			done := make(chan error, 1)
			settled := false
			go func() { done <- s.DeletePathsConditional(selected) }()
			defer func() {
				release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("pre-admission prune did not settle")
					}
				}
			}()
			waitForPublicationSQLWait(t, s, baseline)
			// The gate must still be free while SQL is waiting. The cache-only
			// winner publishes before final membership/version admission.
			winnerDone := make(chan struct{}, 1)
			go func() {
				name := "pruned/old.mov"
				if caseName == "new-child-with-parent-evicted" {
					name = "pruned/new.mov"
				}
				s.InsertToCache(MakeEntry(name, false, 77, stamp.Add(time.Second), 77))
				winnerDone <- struct{}{}
			}()
			select {
			case <-winnerDone:
			case <-time.After(250 * time.Millisecond):
				release()
				select {
				case <-winnerDone:
				case <-time.After(5 * time.Second):
					t.Fatal("cache-only winner did not settle after SQL release")
				}
				t.Fatal("cache-only winner blocked before final admission")
			}
			release()
			select {
			case err := <-done:
				settled = true
				if !errors.Is(err, ErrMirrorConflict) {
					t.Fatalf("pre-admission cache winner produced %v, want conflict", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("pre-admission prune did not finish")
			}
			if calls != 0 {
				t.Fatalf("rejected prune invalidated %d files", calls)
			}
			for _, e := range selected {
				if durable := mirrorDurableEntry(t, s, e.Path); durable == nil || durable.Version != e.Version || durable.Inode != e.Inode {
					t.Fatalf("rejected prune changed durable %q: %+v", e.Path, durable)
				}
			}
			winnerName := "pruned/old.mov"
			if caseName == "new-child-with-parent-evicted" {
				winnerName = "pruned/new.mov"
			}
			if winner := s.lookupByPathRAM(winnerName); winner == nil || winner.Inode != 77 || winner.Size != 77 || !winner.Version.IsZero() {
				t.Fatalf("cache-only winner lost after conflict: %+v", winner)
			}
		})
	}
}

func TestConditionalPruneCommitGateOrdersSuccessorAndReentrantPostHook(t *testing.T) {
	for _, successor := range []string{"replace", "unlink"} {
		t.Run(successor, func(t *testing.T) {
			s, file, selected := conditionalPruneLockFixture(t)
			stamp := time.Unix(1700000000, 0)
			var invalidated []struct {
				path  string
				inode uint64
			}
			s.SetOnScanContentInvalidated(func(name string, inode uint64) {
				invalidated = append(invalidated, struct {
					path  string
					inode uint64
				}{name, inode})
			})
			hookDone := make(chan struct{}, 1)
			s.deletePathsAfterCommitHook = func() {
				if s.lookupByPathRAM("pruned") != nil {
					t.Error("post-commit hook saw uncleaned parent")
				}
				s.InsertToCache(MakeEntry("hook-created.mov", false, 7, stamp, 99))
				s.NoteDirectoryChanged("hook")
				hookDone <- struct{}{}
			}
			gate := installPruneCommitGate(t, s, file, nil)
			done := make(chan error, 1)
			settled := false
			successorDone := make(chan struct{}, 1)
			successorStarted := false
			successorSettled := false
			go func() { done <- s.DeletePathsConditional(selected) }()
			defer func() {
				gate.release()
				if !settled {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("Commit-gated prune did not settle")
					}
				}
				if successorStarted && !successorSettled {
					select {
					case <-successorDone:
					case <-time.After(5 * time.Second):
						t.Error("queued successor did not settle")
					}
				}
			}()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("prune did not reach real driver Commit")
			}
			if !s.mu.TryLock() {
				t.Error("Commit retained cache lock")
			} else {
				s.mu.Unlock()
			}
			if s.cachePublicationMu.TryLock() {
				s.cachePublicationMu.Unlock()
				t.Fatal("Commit did not retain publication gate")
			}
			if len(invalidated) != 1 || invalidated[0].path != "pruned/old.mov" || invalidated[0].inode != 21 {
				t.Fatalf("pre-Commit invalidation = %+v", invalidated)
			}
			for _, e := range selected {
				if cached := s.lookupByPathRAM(e.Path); cached == nil || cached.Version != e.Version || cached.Inode != e.Inode {
					t.Fatalf("cache cleanup occurred before Commit: %+v", cached)
				}
			}
			go func() {
				if successor == "replace" {
					s.InsertToCache(MakeEntry("pruned/old.mov", false, 77, stamp.Add(time.Second), 77))
				} else {
					s.DeleteFromCache("pruned/old.mov")
				}
				successorDone <- struct{}{}
			}()
			successorStarted = true
			select {
			case <-successorDone:
				successorSettled = true
				t.Fatal("cache-only successor crossed held Commit")
			case <-time.After(25 * time.Millisecond):
			}
			gate.release()
			select {
			case err := <-done:
				settled = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("prune did not finish after Commit release")
			}
			select {
			case <-successorDone:
				successorSettled = true
			case <-time.After(5 * time.Second):
				t.Fatal("cache-only successor did not finish")
			}
			select {
			case <-hookDone:
			default:
				t.Fatal("post-commit hook did not run")
			}
			for _, e := range selected {
				if durable := mirrorDurableEntry(t, s, e.Path); durable != nil {
					t.Fatalf("committed prune retained durable %q: %+v", e.Path, durable)
				}
			}
			if s.lookupByPathRAM("pruned") != nil || s.lookupByInodeRAM(21) != nil {
				t.Fatal("pruned parent or old file handle survived")
			}
			if hookEntry := s.lookupByPathRAM("hook-created.mov"); hookEntry == nil || hookEntry.Inode != 99 || hookEntry.Size != 7 {
				t.Fatalf("post-hook cache publication lost: %+v", hookEntry)
			}
			if visible := s.DirectoryVisibilityMtime("hook", time.Time{}); visible.IsZero() {
				t.Fatal("post-hook visibility publication lost")
			}
			winner := s.lookupByPathRAM("pruned/old.mov")
			if successor == "replace" {
				if winner == nil || winner.Inode != 77 || winner.Size != 77 || !winner.Version.IsZero() {
					t.Fatalf("queued replacement lost: %+v", winner)
				}
			} else if winner != nil {
				t.Fatalf("queued unlink left path: %+v", winner)
			}
		})
	}
}

func TestConditionalPruneFailedCommitRollsBackAndAllowsRetry(t *testing.T) {
	s, file, selected := conditionalPruneLockFixture(t)
	s.NoteDirectoryChanged("pruned")
	beforeVisibility := s.DirectoryVisibilityMtime("pruned", time.Time{})
	var invalidated []uint64
	s.SetOnScanContentInvalidated(func(name string, inode uint64) {
		if name != "pruned/old.mov" {
			t.Errorf("unexpected invalidation path %q", name)
		}
		invalidated = append(invalidated, inode)
	})
	hooks := 0
	s.deletePathsAfterCommitHook = func() { hooks++ }
	gate := installPruneCommitGate(t, s, file, errors.New("injected pre-commit rollback"))
	done := make(chan error, 1)
	settled := false
	go func() { done <- s.DeletePathsConditional(selected) }()
	defer func() {
		gate.release()
		if !settled {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("failed-Commit prune did not settle")
			}
		}
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not reach real driver Commit")
	}
	if !s.mu.TryLock() {
		t.Error("failed Commit retained cache lock")
	} else {
		s.mu.Unlock()
	}
	gate.release()
	select {
	case err := <-done:
		settled = true
		if err == nil {
			t.Fatal("injected Commit failure was acknowledged")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed Commit did not return")
	}
	if hooks != 0 || len(invalidated) != 1 || invalidated[0] != 21 {
		t.Fatalf("failed Commit hook=%d invalidations=%v", hooks, invalidated)
	}
	for _, e := range selected {
		if cached := s.lookupByPathRAM(e.Path); cached == nil || cached.Version != e.Version || cached.Inode != e.Inode {
			t.Fatalf("failed Commit changed cache %q: %+v", e.Path, cached)
		}
		if durable := mirrorDurableEntry(t, s, e.Path); durable == nil || durable.Version != e.Version || durable.Inode != e.Inode {
			t.Fatalf("failed Commit changed durable %q: %+v", e.Path, durable)
		}
	}
	if got := s.DirectoryVisibilityMtime("pruned", time.Time{}); !got.Equal(beforeVisibility) {
		t.Fatalf("failed Commit changed visibility: %v -> %v", beforeVisibility, got)
	}
	for _, lock := range []struct {
		name   string
		try    func() bool
		unlock func()
	}{
		{"writer", s.writeMu.TryLock, s.writeMu.Unlock},
		{"publication", s.cachePublicationMu.TryLock, s.cachePublicationMu.Unlock},
		{"cache", s.mu.TryLock, s.mu.Unlock},
	} {
		if !lock.try() {
			t.Fatalf("%s lock retained after failed Commit", lock.name)
		}
		lock.unlock()
	}
	if err := s.DeletePathsConditional(selected); err != nil {
		t.Fatalf("same observation could not retry after real rollback: %v", err)
	}
	if hooks != 1 || len(invalidated) != 2 || invalidated[1] != 21 {
		t.Fatalf("retry hook=%d invalidations=%v", hooks, invalidated)
	}
	for _, e := range selected {
		if s.lookupByPathRAM(e.Path) != nil || mirrorDurableEntry(t, s, e.Path) != nil {
			t.Fatalf("retried prune retained %q", e.Path)
		}
	}
}

func TestConditionalPruneCallbacksPreserveSelectedFileOrder(t *testing.T) {
	s, _, selected := conditionalPruneLockFixture(t)
	stamp := time.Unix(1700000000, 0)
	if err := s.Insert(MakeEntry("pruned/second.mov", false, 9, stamp, 22)); err != nil {
		t.Fatal(err)
	}
	second := s.lookupByPathRAM("pruned/second.mov")
	selected = []*Entry{second, selected[0], selected[1]}
	var calls []struct {
		path  string
		inode uint64
	}
	s.SetOnScanContentInvalidated(func(name string, inode uint64) {
		if !s.mu.TryLock() {
			t.Error("invalidation callback ran under cache lock")
		} else {
			s.mu.Unlock()
		}
		if s.cachePublicationMu.TryLock() {
			s.cachePublicationMu.Unlock()
			t.Error("invalidation callback ran without publication gate")
		}
		calls = append(calls, struct {
			path  string
			inode uint64
		}{name, inode})
	})
	if err := s.DeletePathsConditional(selected); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].path != "pruned/second.mov" || calls[0].inode != 22 || calls[1].path != "pruned/old.mov" || calls[1].inode != 21 {
		t.Fatalf("selected-file callback order/identity = %+v", calls)
	}
}
