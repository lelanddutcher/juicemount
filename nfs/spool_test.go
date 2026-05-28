package nfs

import (
	"crypto/sha256"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
	_ "modernc.org/sqlite"
)

func newTestSpoolStore(t *testing.T, capacity int64) *SpoolStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;`); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)
	store, err := NewSpoolStore(t.TempDir(), capacity, meta)
	if err != nil {
		t.Fatalf("new spool store: %v", err)
	}
	return store
}

func TestSpoolOpenWriteAndClose(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, err := s.OpenWrite("/Movies/clip.mov")
	if err != nil {
		t.Fatalf("open write: %v", err)
	}
	if e.NFSPath() != "/Movies/clip.mov" {
		t.Errorf("nfs path: got %q", e.NFSPath())
	}

	data := []byte("hello juicemount")
	n, err := e.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write at: %v", err)
	}
	if n != len(data) {
		t.Errorf("short write: %d vs %d", n, len(data))
	}
	if got := e.WrittenEnd(); got != int64(len(data)) {
		t.Errorf("writtenEnd=%d, want %d", got, len(data))
	}

	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// SHA-256 should be the hash of "hello juicemount".
	expect := sha256.Sum256(data)
	if got := e.SHA256(); string(got) != string(expect[:]) {
		t.Errorf("sha256 mismatch")
	}

	// SQL row should be in ready state with correct size + sha.
	row, err := s.Meta().Get(e.ID())
	if err != nil {
		t.Fatalf("get row: %v", err)
	}
	if row.DrainState != metadata.DrainReady {
		t.Errorf("state=%q, want ready", row.DrainState)
	}
	if row.Size != int64(len(data)) {
		t.Errorf("row size=%d", row.Size)
	}
	if string(row.SHA256) != string(expect[:]) {
		t.Errorf("row sha mismatch")
	}
}

func TestSpoolSameFilePathReturnsSameEntry(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e1, err := s.OpenWrite("/dup.mov")
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	e2, err := s.OpenWrite("/dup.mov")
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	if e1 != e2 {
		t.Fatalf("second OpenWrite on same path returned a different entry")
	}

	// First Close should NOT actually finalize (refcount > 0 after open).
	if err := e1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}
	row, _ := s.Meta().Get(e1.ID())
	if row.DrainState != metadata.DrainWriting {
		t.Errorf("after first close, state=%q, want writing", row.DrainState)
	}

	// Second Close finalizes.
	if err := e2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}
	row, _ = s.Meta().Get(e1.ID())
	if row.DrainState != metadata.DrainReady {
		t.Errorf("after second close, state=%q, want ready", row.DrainState)
	}
}

func TestSpoolWriteIsFsynced(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, _ := s.OpenWrite("/sync.bin")
	if _, err := e.WriteAt([]byte("durable"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// File should exist on disk and contain the data after Close (which
	// fsyncs). We can't unit-test that fsync was actually called against
	// the kernel, but we can verify the file is readable through OpenForRead.
	r, err := e.OpenForRead()
	if err != nil {
		t.Fatalf("open read: %v", err)
	}
	defer r.Close()
	buf := make([]byte, 32)
	n, _ := r.ReadAt(buf, 0)
	if string(buf[:n]) != "durable" {
		t.Errorf("got %q, want %q", buf[:n], "durable")
	}
}

func TestSpoolCapacityEnforced(t *testing.T) {
	s := newTestSpoolStore(t, 100) // 100-byte cap

	e1, _ := s.OpenWrite("/a")
	if _, err := e1.WriteAt(make([]byte, 60), 0); err != nil {
		t.Fatalf("first write under cap: %v", err)
	}
	// Used is now 60.
	used, total := s.Capacity()
	if used != 60 || total != 100 {
		t.Errorf("capacity: used=%d, total=%d", used, total)
	}

	// Second write within budget.
	if _, err := e1.WriteAt(make([]byte, 30), 60); err != nil {
		t.Fatalf("second write under cap: %v", err)
	}
	used, _ = s.Capacity()
	if used != 90 {
		t.Errorf("used after 2nd write: %d, want 90", used)
	}

	// Third write would exceed.
	if _, err := e1.WriteAt(make([]byte, 30), 90); err != ErrSpoolFull {
		t.Fatalf("expected ErrSpoolFull, got %v", err)
	}
}

func TestSpoolCapacityUnlimited(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/big")
	for i := 0; i < 4; i++ {
		if _, err := e.WriteAt(make([]byte, 1<<20), int64(i)*(1<<20)); err != nil {
			t.Fatalf("write iter %d: %v", i, err)
		}
	}
	used, total := s.Capacity()
	if total != 0 {
		t.Errorf("total=%d, want 0 (unlimited)", total)
	}
	if used != 4<<20 {
		t.Errorf("used=%d, want %d", used, 4<<20)
	}
}

func TestSpoolLookupActive(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	if _, ok := s.LookupActive("/missing"); ok {
		t.Fatalf("expected miss on empty store")
	}

	e, _ := s.OpenWrite("/a.mov")
	got, ok := s.LookupActive("/a.mov")
	if !ok {
		t.Fatalf("expected hit")
	}
	if got != e {
		t.Fatalf("wrong entry")
	}

	// After full close, the index removal happens via the drainer (slice
	// B) in production. For slice A unit-test purposes the entry remains
	// indexed until explicit removal — this exercises ONLY the writer side.
}

func TestSpoolWakeDrainerOnReady(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	var wakes int
	var wakeMu sync.Mutex
	s.SetDrainerWake(func() {
		wakeMu.Lock()
		wakes++
		wakeMu.Unlock()
	})

	e, _ := s.OpenWrite("/wake.mov")
	if _, err := e.WriteAt([]byte("hi"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	wakeMu.Lock()
	n := wakes
	wakeMu.Unlock()
	if n != 1 {
		t.Errorf("wakes=%d, want 1", n)
	}
}

func TestSpoolConcurrentOpenSamePath(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	const N = 16
	entries := make([]*SpoolEntry, N)
	errs := make([]error, N)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			e, err := s.OpenWrite("/concurrent.bin")
			entries[i] = e
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if entries[i] != entries[0] {
			t.Fatalf("goroutine %d got a different entry — same-path-dedupe broken", i)
		}
	}

	// Close N times — only the last should finalize.
	for _, e := range entries {
		if err := e.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	row, _ := s.Meta().Get(entries[0].ID())
	if row.DrainState != metadata.DrainReady {
		t.Errorf("after %d closes, state=%q", N, row.DrainState)
	}
}

func TestSpoolWriteAfterCloseRejected(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/x")
	_ = e.Close()
	if _, err := e.WriteAt([]byte("late"), 0); err == nil {
		t.Fatalf("expected write-after-close error, got nil")
	}
}

func TestSpoolCloseAndDeleteCleansUp(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, _ := s.OpenWrite("/abandon.bin")
	if _, err := e.WriteAt([]byte("trash"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	usedBefore, _ := s.Capacity()
	if usedBefore != 5 {
		t.Fatalf("used before=%d, want 5", usedBefore)
	}

	if err := e.CloseAndDelete("user cancelled"); err != nil {
		t.Fatalf("close+delete: %v", err)
	}

	usedAfter, _ := s.Capacity()
	if usedAfter != 0 {
		t.Errorf("used after delete=%d, want 0", usedAfter)
	}
	if _, ok := s.LookupActive("/abandon.bin"); ok {
		t.Errorf("entry should be removed from index")
	}
	row, _ := s.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainFailed {
		t.Errorf("SQL row state=%q, want failed", row.DrainState)
	}
}

// TestSpoolCloseAndDeleteAfterCloseIsNoop covers reviewer HIGH-1: once
// Close() has finalized the entry (drain_state=ready), a subsequent
// CloseAndDelete() must NOT downgrade the row to failed or delete the
// file out from under the drainer.
func TestSpoolCloseAndDeleteAfterCloseIsNoop(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, _ := s.OpenWrite("/raced.bin")
	if _, err := e.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// SQL row is now `ready`. CloseAndDelete must be a no-op for state.
	if err := e.CloseAndDelete("scrubber thought it was orphaned"); err != nil {
		t.Fatalf("CloseAndDelete after Close should not error, got %v", err)
	}
	row, _ := s.Meta().Get(e.ID())
	if row.DrainState != metadata.DrainReady {
		t.Errorf("state changed by no-op CloseAndDelete: got %q, want ready", row.DrainState)
	}

	// Spool file must still exist for the drainer.
	if _, err := e.OpenForRead(); err != nil {
		t.Errorf("spool file destroyed by no-op CloseAndDelete: %v", err)
	}
}

// TestSpoolCloseAndDeleteIdentityCheckedIndex covers reviewer HIGH-1's
// other half: a CloseAndDelete on entry A must NOT evict a newer entry
// B that was inserted at the same nfs path.
func TestSpoolCloseAndDeleteIdentityCheckedIndex(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	eA, _ := s.OpenWrite("/replaced.bin")
	_, _ = eA.WriteAt([]byte("first"), 0)
	if err := eA.Close(); err != nil {
		t.Fatalf("close A: %v", err)
	}
	// Manually evict A from the index so we can simulate B taking the slot.
	// (In production this happens when the drainer finishes copying A.)
	s.Index().Delete("/replaced.bin")

	eB, _ := s.OpenWrite("/replaced.bin")
	if eB == eA {
		t.Fatal("expected a new entry for the second OpenWrite")
	}

	// Now a late scrubber calls CloseAndDelete on the stale eA. It must
	// NOT evict eB from the index.
	_ = eA.CloseAndDelete("late scrubber")

	got, ok := s.LookupActive("/replaced.bin")
	if !ok || got != eB {
		t.Fatalf("CloseAndDelete on stale entry evicted the live entry: ok=%v got=%v", ok, got)
	}
}

// TestSpoolCapacityRaceAcrossEntries covers reviewer HIGH-2: two
// concurrent writers on DIFFERENT entries must not be able to both pass
// the cap check + both commit and over-fill. CAS reservation makes the
// cap a hard ceiling.
func TestSpoolCapacityRaceAcrossEntries(t *testing.T) {
	s := newTestSpoolStore(t, 1000)

	const writers = 32
	const payload = 64

	entries := make([]*SpoolEntry, writers)
	for i := range entries {
		e, err := s.OpenWrite("/file" + strconv.Itoa(i))
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		entries[i] = e
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var success atomic.Int64
	var refused atomic.Int64
	for i, e := range entries {
		wg.Add(1)
		go func(i int, e *SpoolEntry) {
			defer wg.Done()
			<-start
			_, err := e.WriteAt(make([]byte, payload), 0)
			switch err {
			case nil:
				success.Add(1)
			case ErrSpoolFull:
				refused.Add(1)
			default:
				t.Errorf("goroutine %d unexpected err: %v", i, err)
			}
		}(i, e)
	}
	close(start)
	wg.Wait()

	used, total := s.Capacity()
	if used > total {
		t.Fatalf("CAPACITY OVER-FILL: used=%d > total=%d (CAS reservation broken)", used, total)
	}
	// At cap 1000, 64-byte writes can fit at most 15 successes (15*64=960).
	maxAllowed := total / payload
	if success.Load() > maxAllowed {
		t.Errorf("too many succeeded: %d, max %d (over-fill)", success.Load(), maxAllowed)
	}
}

// TestSpoolOutOfOrderInvalidatesStreamingHash covers reviewer HIGH-4:
// any WriteAt at an offset less than the current writtenEnd must mark
// the streaming hash invalid so Close stores nil sha.
func TestSpoolOutOfOrderInvalidatesStreamingHash(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	e, _ := s.OpenWrite("/sparse.bin")
	// Sequential writes — hash should still be valid.
	_, _ = e.WriteAt([]byte("hello"), 0)
	_, _ = e.WriteAt([]byte(" world"), 5)
	if !e.StreamingHashValid() {
		t.Errorf("hash should be valid after two sequential writes")
	}
	// Out-of-order write at offset 0 — invalidates streaming hash.
	_, _ = e.WriteAt([]byte("REWRITE"), 0)
	if e.StreamingHashValid() {
		t.Errorf("hash should be invalid after out-of-order write")
	}

	_ = e.Close()
	if e.SHA256() != nil {
		t.Errorf("SHA256() should be nil after out-of-order write invalidates streaming hash")
	}
	// SQL row's sha column should also be nil → drainer knows to re-hash.
	row, _ := s.Meta().Get(e.ID())
	if row.SHA256 != nil {
		t.Errorf("SQL row should have nil sha after streaming hash invalidated")
	}
}

// TestSpoolSetDrainerWakeConcurrentlyWithSignal covers reviewer HIGH-3:
// SetDrainerWake racing with signalReady must not data-race the func
// pointer. Run under -race; -race build will fail this test if the
// pointer access is unsynchronized.
func TestSpoolSetDrainerWakeConcurrentlyWithSignal(t *testing.T) {
	s := newTestSpoolStore(t, 0)

	// Pre-create an entry whose Close fires signalReady.
	e, _ := s.OpenWrite("/wake.bin")
	_, _ = e.WriteAt([]byte("x"), 0)

	done := make(chan struct{})
	go func() {
		// Hammer SetDrainerWake in a loop.
		for i := 0; i < 1000; i++ {
			s.SetDrainerWake(func() {})
		}
		close(done)
	}()
	// Now Close while the setter is racing.
	_ = e.Close()
	<-done
	// No assertion — pass means -race didn't fire.
}

// TestSpoolOpenFileFailureDeletesRow covers reviewer HIGH-5: if the
// underlying os.OpenFile fails after Insert, the SQL row must be DELETED
// (not MarkFailed-ed) so a retry on the same path doesn't grow
// spool_entries unboundedly under persistent disk failure.
// TestSpoolStopRefusesOpenWrite covers the closed-store error path —
// after Stop, OpenWrite must return an error rather than silently
// accepting writes that the drainer will never process.
func TestSpoolStopRefusesOpenWrite(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	s.Stop()
	if _, err := s.OpenWrite("/post-stop.bin"); err == nil {
		t.Fatalf("expected OpenWrite to error after Stop()")
	}
}

func TestSpoolOpenFileFailureDeletesRow(t *testing.T) {
	// Construct a SpoolStore whose root is read-only, so OpenWrite's
	// os.OpenFile fails immediately.
	rootDir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "spool.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)
	s, err := NewSpoolStore(rootDir, 0, meta)
	if err != nil {
		t.Fatalf("new spool: %v", err)
	}
	// Make the files dir read-only so OpenFile EACCES.
	if err := os.Chmod(filepath.Join(rootDir, SpoolFilesSubdir), 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(rootDir, SpoolFilesSubdir), 0o755) })

	// First failed attempt.
	_, err = s.OpenWrite("/badpath.bin")
	if err == nil {
		t.Fatalf("expected OpenWrite to fail on read-only spool dir")
	}

	// Verify no rows accumulated.
	rows, _ := meta.ListAll()
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after failed OpenWrite, got %d", len(rows))
	}

	// Second failed attempt — same path, must also leave 0 rows.
	_, _ = s.OpenWrite("/badpath.bin")
	rows, _ = meta.ListAll()
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after second failed OpenWrite, got %d", len(rows))
	}
}
