package nfs

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func TestBoundReadyContinuationLateWritePreservesImage(t *testing.T) {
	f := newBoundContinuationFixture(t, true)
	e, err := f.spool.OpenWrite(f.receipt.Path)
	if err != nil || e != f.entry {
		t.Fatalf("reopen=(%p,%v), want %p", e, err, f.entry)
	}
	want := []byte(f.body)
	copy(want[2:6], []byte("late"))
	if n, err := e.WriteAt([]byte("late"), 2); err != nil || n != 4 {
		t.Fatalf("seek-back=(%d,%v)", n, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(f.receipt.SpoolFile)
	if err != nil || !bytes.Equal(got, want) || e.WrittenEnd() != int64(len(want)) {
		t.Fatalf("image=%q end=%d err=%v", got, e.WrittenEnd(), err)
	}
	row, err := f.meta.Get(f.receipt.SpoolID)
	wantHash := sha256.Sum256(want)
	if err != nil || row.DrainState != metadata.DrainReady || row.Size != int64(len(want)) || !bytes.Equal(row.SHA256, wantHash[:]) {
		t.Fatalf("final row=(%+v,%v)", row, err)
	}
}

func TestBoundReadyPendingRetainsPathAndFD(t *testing.T) {
	f := newBoundContinuationFixture(t, true)
	used, _ := f.spool.Capacity()
	f.spool.boundReadyTransition = func(c metadata.SpoolContinuationSnapshot, _ func()) (bool, error) {
		ok, err := f.meta.ReopenBoundReady(c, func() {})
		if err != nil || !ok {
			return ok, err
		}
		return false, &metadata.PendingSpoolContinuation{Cause: errors.New("lost adapter return after durable CAS")}
	}
	got, err := f.spool.OpenWrite(f.receipt.Path)
	var pending *metadata.PendingSpoolContinuation
	if got != nil || !errors.As(err, &pending) {
		t.Fatalf("uncertain reopen=(%p,%v)", got, err)
	}
	row, err := f.meta.Get(f.receipt.SpoolID)
	if err != nil || row.DrainState != metadata.DrainWriting || !f.entry.closed || f.entry.refcount != 0 {
		t.Fatalf("uncertain state=(%+v,%v) entry closed=%v refs=%d", row, err, f.entry.closed, f.entry.refcount)
	}
	custody, present := f.spool.pendingContinuations[f.receipt.SpoolID]
	if !present || custody.fd == nil || custody.path != f.receipt.Path || custody.file != f.receipt.SpoolFile {
		t.Fatal("pending custody lost its pinned FD")
	}
	if info, err := custody.fd.Stat(); err != nil || info.Size() != int64(len(f.body)) {
		t.Fatalf("pending FD=(%v,%v)", info, err)
	}
	if _, err := f.spool.OpenWrite(f.receipt.Path); !errors.As(err, &pending) {
		t.Fatalf("later OpenWrite admitted: %v", err)
	}
	if _, err := f.spool.OpenWriteSeeded(f.receipt.Path, []byte("new")); !errors.As(err, &pending) {
		t.Fatalf("later seeded OpenWrite admitted: %v", err)
	}
	if changed, err := f.spool.cancelForDelete(f.receipt.Path); changed || !errors.As(err, &pending) {
		t.Fatalf("cancel=(%v,%v)", changed, err)
	}
	if err := f.entry.CloseAndDelete("pending"); !errors.As(err, &pending) {
		t.Fatalf("CloseAndDelete=%v", err)
	}
	if f.spool.EvictIndex(f.receipt.Path, f.receipt.SpoolID) {
		t.Fatal("pending index evicted")
	}
	if n, _, err := f.spool.MigrateForRename(f.receipt.Path, "moved.mov"); n != 0 || !errors.As(err, &pending) {
		t.Fatalf("migration=(%d,%v)", n, err)
	}
	if err := f.spool.QuarantineDrain(f.receipt.SpoolID, f.receipt.Path, f.receipt.SpoolFile, int64(len(f.body)), "pending"); !errors.As(err, &pending) {
		t.Fatalf("quarantine=%v", err)
	}
	if done, err := f.spool.MarkDrainComplete(f.receipt.SpoolID, f.receipt.Path, f.receipt.SpoolFile, int64(len(f.body))); done || !errors.As(err, &pending) {
		t.Fatalf("drain complete=(%v,%v)", done, err)
	}
	if current, ok := f.spool.LookupActive(f.receipt.Path); !ok || current != f.entry {
		t.Fatalf("pending index=(%p,%v)", current, ok)
	}
	if now, _ := f.spool.Capacity(); now != used {
		t.Fatalf("capacity changed %d to %d", used, now)
	}
	if body, err := os.ReadFile(f.receipt.SpoolFile); err != nil || string(body) != f.body {
		t.Fatalf("pending image=%q err=%v", body, err)
	}
	// Simulate an index loss independent of the guarded eviction API. The
	// pending path remains authoritative even without an indexed entry.
	if !f.spool.index.DeleteIfMatches(f.receipt.Path, f.entry) {
		t.Fatal("fixture index loss failed")
	}
	if _, err := f.spool.OpenWrite(f.receipt.Path); !errors.As(err, &pending) {
		t.Fatalf("no-index OpenWrite admitted: %v", err)
	}
	if _, err := f.spool.OpenWriteSeeded(f.receipt.Path, []byte("new")); !errors.As(err, &pending) {
		t.Fatalf("no-index seeded OpenWrite admitted: %v", err)
	}
}

type boundContinuationFixture struct {
	store   *metadata.Store
	meta    *metadata.SpoolStore
	spool   *SpoolStore
	entry   *SpoolEntry
	receipt metadata.SpoolBirthReceipt
	body    string
}

func newBoundContinuationFixture(t *testing.T, ready bool) boundContinuationFixture {
	t.Helper()
	db, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := metadata.InitSpoolSchema(db.DB()); err != nil {
		t.Fatal(err)
	}
	m := metadata.NewSpoolStore(db.DB())
	db.SetSpoolStore(m)
	spool, err := NewSpoolStore(filepath.Join(t.TempDir(), "spool"), 0, m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spool.Stop() })
	path := "bound.mov"
	file := filepath.Join(spool.root, SpoolFilesSubdir, "private-bound")
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := db.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := metadata.NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	r, born, err := db.InsertBoundSpoolBirth(expected, metadata.MakeEntry(path, false, 0, time.Now(), 81), file, nonce)
	if err != nil {
		t.Fatal(err)
	}
	e := &SpoolEntry{id: r.SpoolID, nfsPath: path, spoolFile: file, file: fd, store: spool, refcount: 1}
	e.SetInode(born.Inode)
	e.lastWrite.Store(time.Now().UnixNano())
	spool.index.Insert(path, e)
	body := "original whole image"
	if n, err := e.WriteAt([]byte(body), 0); err != nil || n != len(body) {
		t.Fatalf("seed=(%d,%v)", n, err)
	}
	if ready {
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Cleanup(func() { e.Close() })
	}
	return boundContinuationFixture{db, m, spool, e, r, body}
}

func TestLegacyActiveStreamingContinuation(t *testing.T) {
	t.Setenv("JM_SPOOL_STREAM_DRAIN", "")
	origMin, origMargin, origHead := spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve
	spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = 1<<20, 64<<10, 64<<10
	t.Cleanup(func() {
		spoolStreamMinSize, spoolSealMargin, spoolSealHeadReserve = origMin, origMargin, origHead
	})
	spool, d := newTestDrainer(t, DrainerConfig{})
	d.streamer = &streamer{sessions: make(map[int64]*streamSession)}
	t.Cleanup(d.closeAllStreamSessions)
	e, err := spool.OpenWrite("DCIM/legacy-stream.mov")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if _, err := e.WriteAt(make([]byte, 2<<20), 0); err != nil {
		t.Fatal(err)
	}
	d.streamOnce()
	if e.PunchedEnd() <= 0 {
		t.Fatalf("production stream did not punch: eligibility=%q", streamIneligible(e))
	}
	got, err := spool.OpenWrite("DCIM/legacy-stream.mov")
	if err != nil || got != e {
		t.Fatalf("legacy active continuation=(%p,%v), want %p", got, err, e)
	}
	if err := got.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoundActiveContinuationUsesExactFD(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "same"
		if replace {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			f := newBoundContinuationFixture(t, false)
			oldRefs := f.entry.refcount
			oldUsed, _ := f.spool.Capacity()
			held := f.entry.spoolFile + ".held"
			if replace {
				if err := os.Rename(f.entry.spoolFile, held); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.entry.spoolFile, []byte("successor"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := f.spool.OpenWrite(f.entry.nfsPath)
			if replace {
				if got != nil || !errors.Is(err, metadata.ErrSpoolContinuationConflict) {
					t.Fatalf("replacement=(%v,%v)", got, err)
				}
				if f.entry.refcount != oldRefs {
					t.Fatalf("refcount changed=%d", f.entry.refcount)
				}
				if b, err := os.ReadFile(held); err != nil || string(b) != f.body {
					t.Fatalf("old body=(%q,%v)", b, err)
				}
				if b, err := os.ReadFile(f.entry.spoolFile); err != nil || string(b) != "successor" {
					t.Fatalf("successor body=(%q,%v)", b, err)
				}
			} else {
				if err != nil || got != f.entry {
					t.Fatalf("same=(%v,%v)", got, err)
				}
				if n, err := got.WriteAt([]byte("X"), 0); err != nil || n != 1 {
					t.Fatalf("same write=(%d,%v)", n, err)
				}
				got.ReleaseHandle()
			}
			if used, _ := f.spool.Capacity(); used != oldUsed {
				t.Fatalf("capacity changed=%d", used)
			}
			row, err := f.meta.Get(f.receipt.SpoolID)
			if err != nil || row.DrainState != metadata.DrainWriting {
				t.Fatalf("row=(%+v,%v)", row, err)
			}
		})
	}
}

func TestBoundReadyContinuationRefusesStaleCohort(t *testing.T) {
	for _, kind := range []string{"symlink", "root", "files", "mirror", "phase", "deleted", "claimed", "punched"} {
		t.Run(kind, func(t *testing.T) {
			f := newBoundContinuationFixture(t, true)
			oldUsed, _ := f.spool.Capacity()
			switch kind {
			case "symlink":
				held := f.entry.spoolFile + ".held"
				if err := os.Rename(f.entry.spoolFile, held); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(held, f.entry.spoolFile); err != nil {
					t.Fatal(err)
				}
			case "root":
				held := f.spool.root + ".held"
				if err := os.Rename(f.spool.root, held); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(f.spool.root, SpoolFilesSubdir), 0700); err != nil {
					t.Fatal(err)
				}
			case "files":
				dir := filepath.Join(f.spool.root, SpoolFilesSubdir)
				if err := os.Rename(dir, dir+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
			case "mirror":
				id, err := metadata.NewSpoolCustodyID()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.DB().Exec(`UPDATE entries SET incarnation=? WHERE path=?`, id, f.entry.nfsPath); err != nil {
					t.Fatal(err)
				}
			case "phase":
				if _, err := f.store.DB().Exec(`UPDATE spool_bindings SET phase='retired' WHERE spool_id=?`, f.receipt.SpoolID); err != nil {
					t.Fatal(err)
				}
			case "deleted":
				if _, err := f.store.DB().Exec(`DELETE FROM spool_entries WHERE id=?`, f.receipt.SpoolID); err != nil {
					t.Fatal(err)
				}
			case "claimed":
				if ok, err := f.meta.MarkDraining(f.receipt.SpoolID); err != nil || !ok {
					t.Fatalf("claim=(%v,%v)", ok, err)
				}
			case "punched":
				if err := f.meta.SetPunchedEnd(f.receipt.SpoolID, 1); err != nil {
					t.Fatal(err)
				}
			}
			got, err := f.spool.OpenWrite(f.entry.nfsPath)
			if got != nil || !errors.Is(err, metadata.ErrSpoolContinuationConflict) {
				t.Fatalf("stale %s=(%v,%v)", kind, got, err)
			}
			if !f.entry.closed || f.entry.refcount != 0 {
				t.Fatalf("writer exposed closed=%v refs=%d", f.entry.closed, f.entry.refcount)
			}
			if indexed, ok := f.spool.index.Lookup(f.entry.nfsPath); !ok || indexed != f.entry {
				t.Fatalf("index replaced=%v/%v", indexed, ok)
			}
			if used, _ := f.spool.Capacity(); used != oldUsed {
				t.Fatalf("capacity changed=%d", used)
			}
		})
	}
}
