package metadata

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newBoundedEvictionFixture(t *testing.T, count, budget int, durable bool) *Store {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_DURABLE_SERVE_FALLBACK", "1")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	s, err := OpenWithMaxCacheSize(filepath.Join(t.TempDir(), "mirror.db"), count+10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	base := time.Unix(1700000000, 0)
	entries := []*Entry{MakeEntry("known", true, 0, base, 10)}
	for i := 0; i < count; i++ {
		entries = append(entries, MakeEntry(fmt.Sprintf("known/file-%04d", i), false, int64(i+1), base.Add(time.Duration(i)*time.Second), uint64(100+i)))
	}
	if durable {
		if err := s.BulkInsert(entries, 500); err != nil {
			t.Fatal(err)
		}
	} else {
		for _, e := range entries {
			s.InsertToCache(e)
		}
	}
	s.maxCacheSize = budget
	return s
}

// Pauses after the real selection, with its actual caller lock state intact.
// The mutation must finish before selection is allowed to resume.
func duringEvictionSelection(t *testing.T, s *Store, mutate func()) {
	t.Helper()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	s.evictionAfterSelectHook = func() { close(entered); <-release }
	go func() { s.evictWithPinnedSet(); close(done) }()
	defer func() {
		unblock()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("eviction failed to join")
		}
		s.evictionAfterSelectHook = nil
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("selection did not finish")
	}
	changed := make(chan struct{})
	go func() { mutate(); close(changed) }()
	select {
	case <-changed:
	case <-time.After(100 * time.Millisecond):
		unblock()
		select {
		case <-changed:
		case <-time.After(3 * time.Second):
			t.Fatal("mutation failed to join")
		}
		t.Error("selection held up mutation or recovery for100ms")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("eviction did not finish")
	}
}

func TestBoundedEvictionRejectsChangedCandidates(t *testing.T) {
	for _, kind := range []string{"replacement", "size", "mode", "same-pointer"} {
		t.Run(kind, func(t *testing.T) {
			s := newBoundedEvictionFixture(t, 16, 8, kind != "same-pointer")
			original := s.pathCache["known/file-0000"]
			oldIdentity := original.cacheIdentity
			var mutationErr error
			duringEvictionSelection(t, s, func() {
				switch kind {
				case "replacement":
					mutationErr = s.Insert(MakeEntry(original.Path, false, 4096, time.Now(), 9999))
				case "size":
					mutationErr = s.UpdateSize(original.Path, 4096, time.Now())
				case "mode":
					mutationErr = s.UpdateMode(original.Path, 0600)
				case "same-pointer":
					s.DeleteFromCache(original.Path)
					s.InsertToCache(original)
				}
			})
			if mutationErr != nil {
				t.Fatal(mutationErr)
			}
			got := s.lookupByPathRAM(original.Path)
			if got == nil {
				t.Fatal("newer publication was evicted")
			}
			switch kind {
			case "replacement":
				if got.Inode != 9999 || got.Size != 4096 {
					t.Fatal("replacement changed")
				}
			case "size":
				if got.Size != 4096 {
					t.Fatal("size update changed")
				}
			case "mode":
				if got.Mode.Perm() != 0600 {
					t.Fatal("mode update changed")
				}
			case "same-pointer":
				if got.cacheIdentity == oldIdentity {
					t.Fatal("same pointer reused its publication identity")
				}
			}
		})
	}
}

func TestBoundedEvictionAllowsShadowRecoveryDuringSelection(t *testing.T) {
	s := newBoundedEvictionFixture(t, 64, 32, false)
	var recovered *Entry
	duringEvictionSelection(t, s, func() {
		recovered = s.RecoverShadow(evictedShadow{Path: "pending.mov", Name: "pending.mov", ParentPath: ".", Size: 4096, Mtime: time.Now()}, 8000)
	})
	if recovered == nil || recovered.Size != 4096 || s.lookupByInodeRAM(8000) == nil {
		t.Fatal("newly recovered handle was lost")
	}
}

func TestBoundedEvictionRetiresOnlyObservedUnchangedAliases(t *testing.T) {
	s := newBoundedEvictionFixture(t, 32, 16, false)
	old := time.Unix(1600000000, 0)
	recent := time.Now()
	for _, row := range []struct {
		path  string
		stamp time.Time
		a, b  uint64
	}{{"retained", recent, 2001, 2002}, {"victim", old, 3001, 3002}} {
		s.InsertToCache(MakeEntry(row.path, false, 7, row.stamp, row.a))
		s.InsertToCache(MakeEntry(row.path, false, 7, row.stamp, row.b))
	}
	duringEvictionSelection(t, s, func() {
		s.InsertToCache(MakeEntry("new-alias", false, 9, recent, 4001))
		s.InsertToCache(MakeEntry("new-alias", false, 9, recent, 4002))
	})
	for _, inode := range []uint64{2001, 3001, 3002} {
		if s.lookupByInodeRAM(inode) != nil {
			t.Errorf("old alias/victim%d retained", inode)
		}
	}
	for _, inode := range []uint64{2002, 4001, 4002} {
		if s.lookupByInodeRAM(inode) == nil {
			t.Errorf("retained/new alias%d lost", inode)
		}
	}
}

func TestBoundedEvictionChunksPreserveCompleteDirectoriesAndTotals(t *testing.T) {
	s := newBoundedEvictionFixture(t, 1024, 128, true)
	chunks := 0
	s.evictionAfterApplyChunkHook = func() {
		chunks++
		children, err := s.ListChildrenForReadDir("known")
		if err != nil || len(children) != 1024 {
			t.Errorf("partial directory after chunk%d: count%d err%v", chunks, len(children), err)
		}
		raw, err := s.listChildrenRAM("known")
		if err != nil {
			t.Error(err)
		}
		var sum int64
		for _, e := range raw {
			sum += e.Size
		}
		bytes, files, ok := s.SubtreeSize("known")
		if !ok || bytes != sum || files != int64(len(raw)) {
			t.Errorf("incoherent totals after chunk%d", chunks)
		}
	}
	s.evictWithPinnedSet()
	s.evictionAfterApplyChunkHook = nil
	if chunks < 4 {
		t.Fatalf("expectedmultipleapplychunks,got%d", chunks)
	}
	if count, _ := s.CacheStats(); count != 128 {
		t.Fatalf("residency%d", count)
	}
	if s.lookupByPathRAM("known/file-1023") == nil || s.lookupByPathRAM("known/file-0000") != nil {
		t.Fatal("oldest ordering changed")
	}
}

func TestReadDirCompletenessRechecksEvictionAfterInitialFlag(t *testing.T) {
	s := newBoundedEvictionFixture(t, 8, 3, true)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s.readDirAfterCacheCheckHook = func() { close(entered); <-release }
	type result struct {
		entries []*Entry
		err     error
	}
	done := make(chan result, 1)
	go func() { rows, err := s.ListChildrenForReadDir("known"); done <- result{rows, err} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reader missed false-load seam")
	}
	s.evictWithPinnedSet()
	unblock()
	select {
	case got := <-done:
		if got.err != nil || len(got.entries) != 8 {
			t.Fatalf("partial listing count%d err%v", len(got.entries), got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not join")
	}
	s.readDirAfterCacheCheckHook = nil
}

type changingEvictionPins struct {
	calls       int
	failAt      int
	pinAtSecond bool
}

func (p *changingEvictionPins) PinnedPaths() (map[string]struct{}, error) {
	p.calls++
	if p.calls == p.failAt {
		return nil, errors.New("owned pin failure")
	}
	pins := map[string]struct{}{}
	if p.pinAtSecond && p.calls >= 2 {
		pins["known/file-0000"] = struct{}{}
	}
	return pins, nil
}
func TestBoundedEvictionPinRefreshAndErrors(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			s := newBoundedEvictionFixture(t, 32, 16, false)
			pins := &changingEvictionPins{failAt: failAt, pinAtSecond: true}
			s.SetPinChecker(pins)
			s.evictWithPinnedSet()
			if s.lookupByPathRAM("known") == nil || s.lookupByPathRAM("known/file-0000") == nil {
				t.Fatal("directory/newpinlost")
			}
			if failAt != 0 {
				if count, _ := s.CacheStats(); count != 33 || len(s.recentlyEvicted) != 0 {
					t.Fatal("pin failure changed caches")
				}
			}
		})
	}
}

func TestBoundedEvictionCursorWithConcurrentDirectWriters(t *testing.T) {
	s := newBoundedEvictionFixture(t, 4096, 2048, false)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			if err := s.Insert(MakeEntry(fmt.Sprintf("new/%d", i), false, 77, time.Now(), uint64(10000+i))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	s.evictWithPinnedSet()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("directwriterdidnotjoin")
	}
	for i := 0; i < 100; i++ {
		e := s.lookupByPathRAM(fmt.Sprintf("new/%d", i))
		if e == nil || e.Size != 77 {
			t.Fatal("newdirectpublicationlost")
		}
	}
}

func TestBoundedShadowExpiryRefreshAndClose(t *testing.T) {
	s := newBoundedEvictionFixture(t, 1, 8, false)
	now := time.Now()
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i := 0; i < 4096; i++ {
			s.parkEvictedShadowLocked(MakeEntry(fmt.Sprint(i), false, 1, now, uint64(i+10000)), now.Add(-2*ShadowTTL))
		}
		s.shadowSweepAfter = time.Time{}
		before := len(s.recentlyEvicted)
		s.shadowEvictedAtLocked(MakeEntry("trigger", false, 1, now, 9000), now)
		if before+1-len(s.recentlyEvicted) > evictionChunk || s.shadowSweep == nil {
			t.Fatal("shadowproducerdidnotbounditswork")
		}
		var retained uint64
		for inode := range s.recentlyEvicted {
			if inode >= 10000 {
				retained = inode
				break
			}
		}
		s.parkEvictedShadowLocked(MakeEntry("replacement", false, 4096, now, retained), now)
		rec := s.recentlyEvicted[retained]
		rec.Deleted = true
		s.recentlyEvicted[retained] = rec
		finishShadowExpiryForTest(t, s, now.Add(2*time.Second))
		rec, ok := s.recentlyEvicted[retained]
		if !ok || !rec.Deleted || rec.Path != "replacement" || rec.Size != 4096 {
			t.Fatal("expiryremovedrefreshedgeneration")
		}
		// Leave another cursor unfinished; Close must stop its suspended iterator.
		for i := 0; i < 4096; i++ {
			s.parkEvictedShadowLocked(MakeEntry("old", false, 1, now, uint64(i+20000)), now.Add(-2*ShadowTTL))
		}
		s.shadowSweepAfter = time.Time{}
		s.sweepShadowChunkLocked(now)
	}()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.shadowSweep != nil {
		t.Fatal("closestillretainssweepiterator")
	}
}
