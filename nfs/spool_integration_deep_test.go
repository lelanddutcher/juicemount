package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
	_ "modernc.org/sqlite"
)

// This file is the deep, hardware-free integration suite for the write spool
// (built 2026-05-29 alongside the Finding-1 fix). Everything here runs against
// a temp-dir "FUSE root" + temp SQLite — NO Redis/MinIO/JuiceFS/sudo/mount —
// because the drainer's target is just a directory. It exercises the failure
// modes (crash recovery, capacity exhaustion, SHA-mismatch quarantine, drain
// retry, reopen-while-draining, idle sweeper, Stat shadow + post-drain size
// sync) and the real NFS handle round-trip (ToHandle/FromHandle → spool).

// ---- helpers ----

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func openTestDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`); err != nil {
		tb.Fatalf("pragma: %v", err)
	}
	return db
}

// copyViaHandler drives the same billy calls the go-nfs fork's onCreate/onWrite
// make, returning an error instead of t.Fatal so it is safe to call from
// goroutines. CREATE: Create+Close; WRITE RPCs: OpenFile(O_RDWR)+WriteAt+Close.
func copyViaHandler(jfs *juiceFS, name string, payload []byte, chunk int) error {
	cf, err := jfs.Create(name)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	if err := cf.Close(); err != nil {
		return fmt.Errorf("create-close %s: %w", name, err)
	}
	for off := 0; off < len(payload); off += chunk {
		end := off + chunk
		if end > len(payload) {
			end = len(payload)
		}
		wf, err := jfs.OpenFile(name, os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("openfile %s @%d: %w", name, off, err)
		}
		wa, ok := wf.(io.WriterAt)
		if !ok {
			return fmt.Errorf("write handle %T lacks WriterAt", wf)
		}
		if _, err := wa.WriteAt(payload[off:end], int64(off)); err != nil {
			return fmt.Errorf("writeat %s @%d: %w", name, off, err)
		}
		if err := wf.Close(); err != nil {
			return fmt.Errorf("close %s @%d: %w", name, off, err)
		}
	}
	return nil
}

func deterministicPayload(seed, n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(seed*131 + i*7 + 3)
	}
	return p
}

// ---- #1: deep spool integration suite ----

// (a) Many files copied through the handler all drain correctly. Creates are
// sequential (NFS dispatch is sequential per connection — internal/nfs/conn.go),
// so the concurrency under test is the drainer's 4-worker pool draining all N
// ready entries in parallel after the sweep.
func TestSpoolMultiFileCopiesAllDrain(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)

	const n = 8
	payloads := make([][]byte, n)
	wantSHA := make([][32]byte, n)
	for i := 0; i < n; i++ {
		payloads[i] = deterministicPayload(i, 512*1024+i*7919) // varying sizes
		wantSHA[i] = sha256.Sum256(payloads[i])
	}

	for i := 0; i < n; i++ {
		if err := copyViaHandler(jfs, fmt.Sprintf("clip-%d.mov", i), payloads[i], 64*1024); err != nil {
			t.Fatalf("copy %d failed: %v", i, err)
		}
	}

	spool.sweepOnce(0) // finalize all quiescent entries
	waitFor(t, 10*time.Second, func() bool { return drainer.Metrics().DrainsSucceeded.Load() >= n })

	for i := 0; i < n; i++ {
		got, err := os.ReadFile(filepath.Join(fuseRoot, fmt.Sprintf("clip-%d.mov", i)))
		if err != nil {
			t.Fatalf("read drained file %d: %v", i, err)
		}
		if sha256.Sum256(got) != wantSHA[i] {
			t.Errorf("file %d content mismatch (len got=%d want=%d)", i, len(got), len(payloads[i]))
		}
	}
	if f := drainer.Metrics().DrainsFailed.Load(); f != 0 {
		t.Errorf("DrainsFailed=%d, want 0", f)
	}
	if used, _ := spool.Capacity(); used != 0 {
		t.Errorf("spool capacity not fully released: used=%d", used)
	}
}

// (b) Boot scrubber reconciles every drain_state correctly + is idempotent.
func TestSpoolRecoverOnBootDispositions(t *testing.T) {
	root := t.TempDir()
	db := openTestDB(t, filepath.Join(t.TempDir(), "m.db"))
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)

	s1, err := NewSpoolStore(root, 0, meta)
	if err != nil {
		t.Fatalf("s1: %v", err)
	}

	// (1) ready + file present
	eReady, _ := s1.OpenWrite("/ready.bin")
	_, _ = eReady.WriteAt([]byte("ready-data!!"), 0) // 12 bytes
	_ = eReady.Close()
	// (2) draining + file present
	eDrain, _ := s1.OpenWrite("/draining.bin")
	_, _ = eDrain.WriteAt([]byte("draining-data"), 0) // 13 bytes
	_ = eDrain.Close()
	if ok, err := meta.MarkDraining(eDrain.ID()); err != nil || !ok {
		t.Fatalf("mark draining: ok=%v err=%v", ok, err)
	}
	// (3) writing + partial file (never closed)
	eWriting, _ := s1.OpenWrite("/writing.bin")
	_, _ = eWriting.WriteAt([]byte("partial"), 0)
	// (4) ready row whose file has gone missing
	eMissing, _ := s1.OpenWrite("/missing.bin")
	_, _ = eMissing.WriteAt([]byte("gone"), 0)
	_ = eMissing.Close()
	if err := os.Remove(eMissing.SpoolFilePath()); err != nil {
		t.Fatalf("remove missing file: %v", err)
	}
	// (5) orphan file with no SQL row
	orphan := filepath.Join(root, SpoolFilesSubdir, "orphan-file")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	// Simulate a process restart: a fresh SpoolStore over the same root+db
	// (empty in-memory index, used=0), then run the boot scrubber.
	s2, err := NewSpoolStore(root, 0, meta)
	if err != nil {
		t.Fatalf("s2: %v", err)
	}
	rep, err := s2.RecoverOnBoot(context.Background())
	if err != nil {
		t.Fatalf("RecoverOnBoot: %v", err)
	}

	if rep.ReadyResumed != 1 {
		t.Errorf("ReadyResumed=%d, want 1", rep.ReadyResumed)
	}
	if rep.DrainingReset != 1 {
		t.Errorf("DrainingReset=%d, want 1", rep.DrainingReset)
	}
	if rep.WritingFailedRows != 1 {
		t.Errorf("WritingFailedRows=%d, want 1", rep.WritingFailedRows)
	}
	if rep.OrphanRowsFailed != 1 {
		t.Errorf("OrphanRowsFailed=%d, want 1", rep.OrphanRowsFailed)
	}
	if rep.OrphanFilesDeleted != 1 {
		t.Errorf("OrphanFilesDeleted=%d, want 1", rep.OrphanFilesDeleted)
	}

	// Capacity re-accounted from the surviving ready + reset-to-ready rows.
	const wantUsed = 12 + 13
	if used, _ := s2.Capacity(); used != wantUsed {
		t.Errorf("recovered used=%d, want %d", used, wantUsed)
	}
	// State assertions.
	if row, _ := meta.Get(eWriting.ID()); row == nil || row.DrainState != metadata.DrainFailed {
		t.Errorf("writing row not failed: %+v", row)
	}
	if row, _ := meta.Get(eMissing.ID()); row == nil || row.DrainState != metadata.DrainFailed {
		t.Errorf("missing-file row not failed: %+v", row)
	}
	if row, _ := meta.Get(eDrain.ID()); row == nil || row.DrainState != metadata.DrainReady {
		t.Errorf("draining row not reset to ready: %+v", row)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan file not deleted (err=%v)", err)
	}

	// Idempotency: a second scrub must produce the SAME used, not a doubling.
	if _, err := s2.RecoverOnBoot(context.Background()); err != nil {
		t.Fatalf("second RecoverOnBoot: %v", err)
	}
	if used, _ := s2.Capacity(); used != wantUsed {
		t.Errorf("after second recover used=%d, want %d (not idempotent)", used, wantUsed)
	}
}

// (c) Capacity cap is enforced; an overflowing WriteAt returns ErrSpoolFull and
// commits nothing.
func TestSpoolCapacityExhaustion(t *testing.T) {
	s := newTestSpoolStore(t, 1<<20) // 1 MiB cap
	e, err := s.OpenWrite("/big.bin")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 512<<10), 0); err != nil {
		t.Fatalf("first 512 KiB write: %v", err)
	}
	if _, err := e.WriteAt(make([]byte, 600<<10), 512<<10); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("overflow write: got err=%v, want ErrSpoolFull", err)
	}
	used, total := s.Capacity()
	if used != 512<<10 || total != 1<<20 {
		t.Errorf("capacity after overflow: used=%d total=%d, want used=%d total=%d",
			used, total, 512<<10, 1<<20)
	}
}

// (d) A write to a path whose prior entry is finalized-but-still-draining
// blocks until the drain evicts it, then starts a FRESH entry — no dup-drain,
// no write-to-closed error (Findings 2 & 4 on the live path).
func TestSpoolReopenWhileDrainingBlocksThenFresh(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e1, _ := s.OpenWrite("/reopen.bin")
	_, _ = e1.WriteAt([]byte("first"), 0)
	_ = e1.Close() // finalized → ready, still in the index (no drainer running)

	type res struct {
		e   *SpoolEntry
		err error
	}
	ch := make(chan res, 1)
	go func() {
		e, err := s.OpenWrite("/reopen.bin")
		ch <- res{e, err}
	}()

	select {
	case <-ch:
		t.Fatalf("OpenWrite returned immediately; expected it to block while the prior entry drains")
	case <-time.After(60 * time.Millisecond):
		// still blocked — correct
	}

	// Simulate the drainer completing: evicts the entry from the index.
	if err := s.MarkDrainComplete(e1.ID(), e1.NFSPath(), e1.SpoolFilePath(), e1.WrittenEnd()); err != nil {
		t.Fatalf("MarkDrainComplete: %v", err)
	}

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reopen returned error: %v", r.err)
		}
		if r.e.ID() == e1.ID() {
			t.Errorf("reopen returned the SAME (finalized) entry id=%d; expected a fresh one", e1.ID())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("blocked OpenWrite did not return after the prior entry drained")
	}
}

// (e) A bit flip on the spool file between write and drain is detected by SHA
// verification and quarantined — never silently drained, never deleted.
func TestSpoolDrainQuarantinesOnCorruption(t *testing.T) {
	spool, d := newTestDrainer(t, DrainerConfig{})
	payload := bytes.Repeat([]byte("X"), 8192)
	e := writeSpoolEntry(t, spool, "/corrupt.bin", payload) // ready, SHA recorded

	// Flip a byte on disk after the streaming SHA was finalized.
	f, err := os.OpenFile(e.SpoolFilePath(), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open spool file: %v", err)
	}
	if _, err := f.WriteAt([]byte("Z"), 0); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	_ = f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.DrainOnceForTest(ctx)

	if q := d.Metrics().Quarantined.Load(); q != 1 {
		t.Fatalf("Quarantined=%d, want 1", q)
	}
	if d.Metrics().DrainsSucceeded.Load() != 0 {
		t.Errorf("a corrupted drain must not count as succeeded")
	}
	if _, err := os.Stat(e.SpoolFilePath()); !os.IsNotExist(err) {
		t.Errorf("spool file should have moved out of files/ (err=%v)", err)
	}
	q := filepath.Join(spool.Root(), "quarantine", filepath.Base(e.SpoolFilePath()))
	if _, err := os.Stat(q); err != nil {
		t.Errorf("corrupted file should be preserved in quarantine/: %v", err)
	}
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainFailed {
		t.Errorf("row not marked failed: %+v", row)
	}
}

// (f) A persistently-unwritable FUSE dest causes retries, then a permanent
// failure after MaxAttempts — no infinite loop, no panic.
func TestSpoolDrainRetriesThenFailsPermanently(t *testing.T) {
	spool := newTestSpoolStore(t, 0)
	fuseRoot := t.TempDir()
	// Make the FUSE root unwritable so os.Create(dest) fails every attempt.
	if err := os.Chmod(fuseRoot, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(fuseRoot, 0o700) }) // let TempDir cleanup succeed

	d, err := NewDrainer(spool, DrainerConfig{
		FuseRoot:     fuseRoot,
		MaxAttempts:  2,
		BackoffBase:  5 * time.Millisecond,
		PollFallback: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("drainer: %v", err)
	}
	e := writeSpoolEntry(t, spool, "/willfail.bin", []byte("data"))
	d.Start()
	t.Cleanup(func() { d.Stop(2 * time.Second) })

	waitFor(t, 3*time.Second, func() bool { return d.Metrics().DrainsFailed.Load() >= 1 })
	if row, _ := spool.Meta().Get(e.ID()); row == nil || row.DrainState != metadata.DrainFailed {
		t.Errorf("row not failed after retry exhaustion: %+v", row)
	}
}

// (g) Stat reports the real (growing) size via the spool shadow during the
// write, and STILL reports the correct size after drain (onSpoolDrained syncs
// it into the metadata cache — otherwise Stat would read the Create-time 0).
func TestSpoolStatShadowAndPostDrainSize(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)

	const sz = 3 << 20
	payload := deterministicPayload(99, sz)
	if err := copyViaHandler(jfs, "f.mov", payload, 256<<10); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Pre-drain: spool shadow must surface the full written size.
	fi, err := jfs.Stat("f.mov")
	if err != nil {
		t.Fatalf("pre-drain Stat: %v", err)
	}
	if fi.Size() != sz {
		t.Errorf("pre-drain Stat size=%d, want %d (spool shadow)", fi.Size(), sz)
	}

	spool.sweepOnce(0)
	// Wait until the drain — AND its onSpoolDrained UpdateSize — has fully
	// completed. InFlight returns to 0 only in the worker's deferred cleanup,
	// which runs AFTER onSpoolDrained. Reading Stat only once the drainer is
	// idle avoids racing the drainer's metadata UpdateSize: metadata.Store.
	// UpdateSize mutates the cached *Entry in place under s.mu, but juiceFS.Stat
	// reads it lock-free — a pre-existing race (legacy writeFile.Close hits it
	// too), benign in prod (aligned int64) but -race-flagged. Tracked in
	// docs/OPEN_BUGS.md; the test must not depend on it.
	waitFor(t, 5*time.Second, func() bool {
		m := drainer.Metrics()
		return m.DrainsSucceeded.Load() >= 1 && m.InFlight.Load() == 0
	})

	// Post-drain (drainer idle, no concurrent writer): metadata size synced.
	fi2, err := jfs.Stat("f.mov")
	if err != nil {
		t.Fatalf("post-drain Stat: %v", err)
	}
	if fi2.Size() != sz {
		t.Errorf("post-drain Stat size=%d, want %d (onSpoolDrained UpdateSize)", fi2.Size(), sz)
	}
	if st, err := os.Stat(filepath.Join(fuseRoot, "f.mov")); err != nil || st.Size() != sz {
		t.Errorf("FUSE file size wrong: %v size=%v", err, st)
	}
}

// (h) The idle sweeper finalizes a quiescent entry (no open handles, no recent
// write) on its own timer — the NFS-compatible replacement for finalize-on-Close.
func TestSpoolIdleSweeperFinalizesQuiescentEntry(t *testing.T) {
	s := newTestSpoolStore(t, 0)
	e, _ := s.OpenWrite("/idle.bin")
	_, _ = e.WriteAt([]byte("quiescent"), 0)
	e.ReleaseHandle() // per-RPC close: refcount 0, but NOT finalized

	if e.SHA256() != nil {
		t.Fatalf("entry should not be finalized before the idle sweep")
	}
	stop := s.StartSweeper(40*time.Millisecond, 10*time.Millisecond)
	defer stop()

	// Poll the authoritative SQL state (MarkReady commits as the last finalize
	// step, after the in-memory SHA is set — so the row reaching `ready` proves
	// the whole finalize ran, race-free).
	waitFor(t, 2*time.Second, func() bool {
		row, _ := s.Meta().Get(e.ID())
		return row != nil && row.DrainState == metadata.DrainReady
	})
	if e.SHA256() == nil {
		t.Errorf("SHA should be finalized once the entry is marked ready")
	}
}

// ---- #2: real NFS handle round-trip ----

// TestSpoolHandleRoundTripCreateWriteRead drives the exact handler calls the
// go-nfs fork's onCreate/onWrite/onRead make — CREATE→ToHandle, then per WRITE
// FromHandle(handle)→OpenFile(O_RDWR), then read-back via the resolved path —
// proving the spool is reached through real NFS *handle resolution*, not just
// by a path string (which the billy-level cert test uses). The only thing this
// omits vs a kernel mount is the generic XDR wire layer.
func TestSpoolHandleRoundTripCreateWriteRead(t *testing.T) {
	jfs, spool, drainer, fuseRoot := newSpoolWiredHandler(t)
	h := jfs.handler

	const sz = 2 << 20
	payload := deterministicPayload(7, sz)
	want := sha256.Sum256(payload)

	// CREATE RPC (onCreate): fs.Create(path); Close; ToHandle(fs, newFile).
	cf, err := jfs.Create("Movies/clip.mov")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_ = cf.Close()
	handle := h.ToHandle(jfs, []string{"Movies", "clip.mov"})
	if len(handle) != 8 {
		t.Fatalf("ToHandle returned bad handle len=%d", len(handle))
	}

	// WRITE RPCs (onWrite): FromHandle(handle) → OpenFile(join(path), O_RDWR).
	for off := 0; off < sz; off += 256 << 10 {
		end := off + 256<<10
		if end > sz {
			end = sz
		}
		fsx, parts, ferr := h.FromHandle(handle)
		if ferr != nil {
			t.Fatalf("FromHandle @%d: %v", off, ferr)
		}
		p := strings.Join(parts, "/")
		wf, oerr := fsx.OpenFile(p, os.O_RDWR, 0o644)
		if oerr != nil {
			t.Fatalf("OpenFile via handle @%d: %v", off, oerr)
		}
		if _, werr := wf.(io.WriterAt).WriteAt(payload[off:end], int64(off)); werr != nil {
			t.Fatalf("WriteAt via handle @%d: %v", off, werr)
		}
		_ = wf.Close()
	}

	// The handle-resolved writes MUST have routed through the spool.
	if _, active := spool.LookupActive("Movies/clip.mov"); !active {
		t.Fatalf("path not active in spool after handle-routed writes — handle resolution bypassed the spool")
	}

	spool.sweepOnce(0)
	waitFor(t, 5*time.Second, func() bool { return drainer.Metrics().DrainsSucceeded.Load() >= 1 })

	// Authoritative integrity check: the drained bytes in FUSE match.
	got, err := os.ReadFile(filepath.Join(fuseRoot, "Movies", "clip.mov"))
	if err != nil {
		t.Fatalf("read drained file: %v", err)
	}
	if sha256.Sum256(got) != want {
		t.Fatalf("drained content mismatch (len=%d)", len(got))
	}

	// Bonus: read back through the handle-resolved path (post-drain the handle
	// still resolves and the read falls through to FUSE).
	fsx, parts, ferr := h.FromHandle(handle)
	if ferr != nil {
		t.Errorf("FromHandle after drain: %v", ferr)
	} else {
		rf, oerr := fsx.OpenFile(strings.Join(parts, "/"), os.O_RDONLY, 0)
		if oerr != nil {
			t.Errorf("read-open via handle after drain: %v", oerr)
		} else {
			rb, _ := io.ReadAll(rf)
			_ = rf.Close()
			if sha256.Sum256(rb) != want {
				t.Errorf("handle-resolved read-back mismatch (len=%d)", len(rb))
			}
		}
	}
}
