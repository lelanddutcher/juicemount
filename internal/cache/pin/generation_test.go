package pin

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Generation tests use an explicitly injected producer. The production
// prefetcher never treats FUSE bytes alone as an authority witness.
type testOwnedPinProvider struct {
	store *Store
	root  string
}

type testOwnedPinJob struct {
	provider *testOwnedPinProvider
	want     ExpectedFile
}

func (p *testOwnedPinProvider) Capture(_ context.Context, e Entry) (OwnedAuthorityJob, error) {
	info, err := os.Stat(filepath.Join(p.root, filepath.Base(e.Path)))
	if err != nil {
		return nil, err
	}
	return &testOwnedPinJob{provider: p, want: ExpectedFile{Inode: info.Sys().(*syscall.Stat_t).Ino, Size: info.Size(), MtimeNS: info.ModTime().UnixNano()}}, nil
}
func (p *testOwnedPinProvider) ValidateReady(_ context.Context, e Entry) bool {
	return len(e.Authority) != 0
}
func (p *testOwnedPinProvider) Current(e Entry) bool { return len(e.Authority) != 0 }
func (p *testOwnedPinProvider) DiscardReady(Entry)   {}
func (j *testOwnedPinJob) Release()                  {}
func (j *testOwnedPinJob) Expected() ExpectedFile    { return j.want }
func (j *testOwnedPinJob) SourceCurrent() bool       { return true }
func (j *testOwnedPinJob) Complete(_ context.Context, digest [sha256.Size]byte) ([]byte, error) {
	return append([]byte("unit-owned:"), digest[:]...), nil
}
func (j *testOwnedPinJob) Finalize(_ context.Context, e Entry) (bool, error) {
	return j.provider.store.AdmitReady(e)
}

func pinnedFixture(t *testing.T, name string, data []byte) (*Store, *Prefetcher, string) {
	t.Helper()
	dir := t.TempDir()
	path := "/Volumes/zpool/" + name
	if data != nil {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := Open(filepath.Join(t.TempDir(), "pin.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.Pin(path, int64(len(data)), "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	// This fixture supplies a real local FUSE source but no FUSEManager. Bind
	// an explicit test cache policy to this test's volume before the worker
	// reaches the ordinary download admission path.
	cacheRoot := t.TempDir()
	free, device, known := volumeFreeBytesAtKnown(cacheRoot)
	if !known || free < 4<<20 {
		t.Fatal("test cache volume has no measurable working window")
	}
	SetCachePolicy(free/2, free/4, cacheRoot, device, true)
	t.Cleanup(func() { SetCachePolicy(0, 0, "", 0, false) })
	if v := ComputeCapacity(store, cacheRoot); !v.PolicyKnown {
		t.Fatal("test cache policy was not admitted")
	}
	provider := &testOwnedPinProvider{store: store, root: dir}
	if err := store.SetReadyValidator(provider.Current); err != nil {
		t.Fatal(err)
	}
	p := NewPrefetcher(store, dir, "/Volumes/zpool", 1, provider)
	t.Cleanup(func() {
		if !p.StopWithin(5 * time.Second) {
			t.Error("prefetch worker did not join")
		}
	})
	return store, p, path
}

func pinEntry(t *testing.T, s *Store, path string) Entry {
	t.Helper()
	e, ok := s.Get(path)
	if !ok || e.Generation == "" {
		t.Fatalf("missing pin generation: %+v", e)
	}
	return *e
}

func awaitPinResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch result unresolved")
		return nil
	}
}

func TestPrefetchReadyCannotPublishAfterRepin(t *testing.T) {
	s, p, path := pinnedFixture(t, "clip.mov", []byte("resident bytes"))
	old := pinEntry(t, s, path)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var senders sync.WaitGroup
	defer func() {
		once.Do(func() { close(release) })
		senders.Wait()
	}()
	p.testBeforeReady = func() { close(entered); <-release }
	result := make(chan error, 1)
	senders.Add(1)
	go func() {
		defer senders.Done()
		result <- p.EnqueueWait(context.Background(), old)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old worker did not reach Ready")
	}
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(path, old.Size, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	successor := pinEntry(t, s, path)
	if successor.Generation == old.Generation || successor.Status != StatusPending {
		t.Fatalf("repin did not reset generation: %+v", successor)
	}
	once.Do(func() { close(release) })
	if err := awaitPinResult(t, result); !errors.Is(err, ErrPinSuperseded) {
		t.Fatalf("old completion: %v", err)
	}
	after := pinEntry(t, s, path)
	if after.Generation != successor.Generation || after.Status != StatusPending || after.BytesCached != 0 {
		t.Fatalf("stale Ready changed successor: %+v", after)
	}
}

func TestPrefetchFailureCannotPublishAfterRepin(t *testing.T) {
	s, p, path := pinnedFixture(t, "missing.mov", nil)
	old := pinEntry(t, s, path)
	claimed, ok, err := s.ClaimPrefetch(old)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if err := s.UnpinPath(path); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(path, 9, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	successor := pinEntry(t, s, path)
	if err := p.prefetch(claimed); err == nil {
		t.Fatal("missing original unexpectedly read")
	}
	after := pinEntry(t, s, path)
	if after.Generation != successor.Generation || after.Status != StatusPending || after.LastError != "" {
		t.Fatalf("stale Failed changed successor: %+v", after)
	}
}

func TestPrefetchReadyAndDurableAttemptGeneration(t *testing.T) {
	s, p, path := pinnedFixture(t, "clip.mov", []byte("verified source"))
	initial := pinEntry(t, s, path)
	if err := p.EnqueueWait(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	ready := pinEntry(t, s, path)
	if ready.Status != StatusReady || ready.BytesCached != initial.Size || ready.Generation == initial.Generation {
		t.Fatalf("ready attempt: %+v", ready)
	}
	if changed, err := s.UpdateStatusForEntry(initial, StatusFailed, 0, "stale"); err != nil || changed {
		t.Fatalf("old attempt changed ready: %v %v", changed, err)
	}
}

func TestPinGenerationSurvivesRestartAndLegacyRowsReset(t *testing.T) {
	dbpath := filepath.Join(t.TempDir(), "pin.db")
	db, err := sql.Open("sqlite", dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE pinned_files (path TEXT PRIMARY KEY,size INTEGER NOT NULL,status INTEGER NOT NULL,bytes_cached INTEGER NOT NULL DEFAULT 0,last_prefetched INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',pinned_at INTEGER NOT NULL,pin_root TEXT NOT NULL DEFAULT ''); INSERT INTO pinned_files(path,size,status,bytes_cached,last_prefetched,last_error,pinned_at,pin_root) VALUES('/Volumes/zpool/old.mov',10,3,10,5,'',1,'/Volumes/zpool');`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	s, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	old := pinEntry(t, s, "/Volumes/zpool/old.mov")
	if old.Status != StatusPending || old.BytesCached != 0 || old.LastPrefetched.UnixMilli() != 0 {
		t.Fatalf("legacy row was not reset: %+v", old)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	restored := pinEntry(t, s, old.Path)
	if restored.Generation != old.Generation || restored.Status != StatusPending {
		t.Fatalf("migration did not persist: %+v", restored)
	}
	if err := s.Pin(old.Path, 10, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	repin := pinEntry(t, s, old.Path)
	if repin.Generation == old.Generation {
		t.Fatal("repin reused generation")
	}
	if changed, err := s.UpdateStatusForEntry(old, StatusReady, 10, ""); err == nil || changed {
		t.Fatalf("unwitnessed Ready accepted: %v %v", changed, err)
	}
	if changed, err := s.CompleteReady(old, []byte("stale-owned-witness")); err != nil || changed {
		t.Fatalf("old restarted row completed repin: %v %v", changed, err)
	}
}

func TestQueuedPrefetchCannotPublishAfterRepin(t *testing.T) {
	s, p, first := pinnedFixture(t, "a.mov", []byte("a"))
	second := "/Volumes/zpool/b.mov"
	if err := os.WriteFile(filepath.Join(p.fusePath, "b.mov"), []byte("b"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(second, 1, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var senders sync.WaitGroup
	defer func() {
		once.Do(func() { close(release) })
		senders.Wait()
	}()
	queued := make(chan struct{})
	p.testAfterEnqueue = func(e Entry) {
		if e.Path == second {
			close(queued)
		}
	}
	p.testBeforeReady = func() {
		if current := p.currentFile.Load(); current != nil && *current == filepath.Join(p.fusePath, "a.mov") {
			close(entered)
			<-release
		}
	}
	firstResult := make(chan error, 1)
	firstEntry := pinEntry(t, s, first)
	senders.Add(1)
	go func() {
		defer senders.Done()
		firstResult <- p.EnqueueWait(context.Background(), firstEntry)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first worker did not hold")
	}
	oldSecond := pinEntry(t, s, second)
	secondResult := make(chan error, 1)
	senders.Add(1)
	go func() {
		defer senders.Done()
		secondResult <- p.EnqueueWait(context.Background(), oldSecond)
	}()
	select {
	case <-queued:
	case <-time.After(5 * time.Second):
		t.Fatal("second job not queued")
	}
	if err := s.UnpinPath(second); err != nil {
		t.Fatal(err)
	}
	if err := s.Pin(second, 1, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	successor := pinEntry(t, s, second)
	once.Do(func() { close(release) })
	if err := awaitPinResult(t, firstResult); err != nil {
		t.Fatal(err)
	}
	if err := awaitPinResult(t, secondResult); !errors.Is(err, ErrPinSuperseded) {
		t.Fatalf("old queued job: %v", err)
	}
	after := pinEntry(t, s, second)
	if after.Generation != successor.Generation || after.Status != StatusPending {
		t.Fatalf("queued job changed repin: %+v", after)
	}
}

func TestBlockedClaimDoesNotBlockStopAdmission(t *testing.T) {
	dbpath := filepath.Join(t.TempDir(), "pin.db")
	s, err := Open(dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := "/Volumes/zpool/unavailable.mov"
	if err := s.Pin(path, 1, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	old := pinEntry(t, s, path)
	locker, err := sql.Open("sqlite", dbpath)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	tx, err := locker.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE pinned_files SET last_error=last_error WHERE path=?`, path); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	released := false
	var p *Prefetcher
	defer func() {
		if !released {
			tx.Rollback()
		}
		if p != nil && !p.StopWithin(5*time.Second) {
			t.Error("original worker did not join after SQLite release")
		}
	}()
	p = NewPrefetcher(s, t.TempDir(), "/Volumes/zpool", 1)
	entered := make(chan struct{})
	p.testBeforeClaim = func(Entry) { close(entered) }
	if !p.Enqueue(old) {
		t.Fatal("enqueue blocked by SQLite claim")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach claim")
	}
	stopStart := time.Now()
	if p.StopWithin(20 * time.Millisecond) {
		t.Fatal("blocked original claim falsely joined")
	}
	if time.Since(stopStart) > time.Second {
		t.Fatal("StopWithin blocked on SQLite claim")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	released = true
	if !p.StopWithin(5 * time.Second) {
		t.Fatal("original worker did not join after SQLite release")
	}
	after := pinEntry(t, s, path)
	if after.Status != StatusPending || after.LastError != "" {
		t.Fatalf("stopped worker opened missing FUSE content: %+v", after)
	}
}

func TestPinManyMintedGenerationAndRepairRevokesAttempt(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "pin.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := Entry{Path: "/Volumes/zpool/reel.mov", Size: 2, PinRoot: "/Volumes/zpool"}
	if err := s.PinMany([]Entry{e}); err != nil {
		t.Fatal(err)
	}
	first := pinEntry(t, s, e.Path)
	claimed, ok, err := s.ClaimPrefetch(first)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if changed, err := s.RequeueForRepair(claimed); err != nil || !changed {
		t.Fatalf("repair reset: %v %v", changed, err)
	}
	pending := pinEntry(t, s, e.Path)
	if pending.Generation == claimed.Generation || pending.Status != StatusPending {
		t.Fatalf("repair did not revoke old attempt: %+v", pending)
	}
	if changed, err := s.UpdateStatusForEntry(claimed, StatusReady, 2, ""); err == nil || changed {
		t.Fatalf("unwitnessed Ready accepted: %v %v", changed, err)
	}
	if changed, err := s.CompleteReady(claimed, []byte("stale-owned-witness")); err != nil || changed {
		t.Fatalf("old attempt completed repair: %v %v", changed, err)
	}
	if err := s.PinMany([]Entry{e}); err != nil {
		t.Fatal(err)
	}
	repin := pinEntry(t, s, e.Path)
	if repin.Generation == pending.Generation || repin.BytesCached != 0 || repin.LastError != "" {
		t.Fatalf("PinMany repin reused progress: %+v", repin)
	}
}
