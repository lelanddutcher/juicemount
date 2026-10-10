package nfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

type shutdownFixture struct {
	h                     *JuiceMountHandler
	spool                 *SpoolStore
	db                    *metadata.Store
	dbPath, root, backing string
}

func newShutdownFixture(t *testing.T) *shutdownFixture {
	t.Helper()
	f := &shutdownFixture{dbPath: filepath.Join(t.TempDir(), "mirror.db"), root: filepath.Join(t.TempDir(), "spool"), backing: t.TempDir()}
	var err error
	f.db, err = metadata.Open(f.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := metadata.InitSpoolSchema(f.db.DB()); err != nil {
		t.Fatal(err)
	}
	m := metadata.NewSpoolStore(f.db.DB())
	f.spool, err = NewSpoolStore(f.root, 0, m)
	if err != nil {
		t.Fatal(err)
	}
	f.h = NewHandler(f.db, f.backing)
	f.h.SetSpool(f.spool, nil)
	t.Cleanup(func() {
		// Join the original worker before closing any descriptor or database.
		if !f.h.StopHandler() {
			panic("shutdown test worker still owns dependencies")
		}
		for _, e := range f.spool.index.Snapshot() {
			e.cancelClose()
		}
		f.spool.Stop()
		_ = f.db.Close()
	})
	return f
}

func (f *shutdownFixture) create(t *testing.T, name string, body []byte) *SpoolEntry {
	t.Helper()
	simulateNFSCreateThenWrite(t, &juiceFS{handler: f.h}, name, body, 513)
	e, ok := f.spool.index.Lookup(name)
	if !ok {
		t.Fatal("missing live image")
	}
	return e
}

func shutdownOffline(t *testing.T) {
	t.Helper()
	old := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(old) })
}

// Uses real NFS CREATE/WRITE/COMMIT call shapes with per-RPC Close, then closes
// and reopens SQLite and the spool. No backing object is available to refetch.
func TestOfflineShutdownPreservesReceivedImages(t *testing.T) {
	shutdownOffline(t)
	for _, name := range []string{"document.bin", "._document.bin"} {
		t.Run(name, func(t *testing.T) {
			f := newShutdownFixture(t)
			body := bytes.Repeat([]byte("received image!"), 293)
			e := f.create(t, name, body)
			if err := (&juiceFS{handler: f.h}).CommitFile(name); err != nil {
				t.Fatal(err)
			}
			c, err := f.spool.meta.InspectQuiescentSpool(e.id, name, e.spoolFile, e.Inode())
			if err != nil || !c.Legacy || c.State != "writing" {
				t.Fatalf("initial custody=(%+v,%v)", c, err)
			}
			e.mu.RLock()
			refs, contiguous, end := e.refcount, e.contiguousEnd, e.writtenEnd
			e.mu.RUnlock()
			if refs != 0 || contiguous != int64(len(body)) || end != contiguous {
				t.Fatalf("refs=%d coverage=%d end=%d", refs, contiguous, end)
			}
			if n := f.spool.sweepOnce(time.Nanosecond); n != 0 {
				t.Fatalf("offline sweep finalized %d", n)
			}
			used := f.spool.used.Load()
			if !f.h.stopHandlerWithDeadline(time.Second) {
				t.Fatal("shutdown incomplete")
			}
			row, err := f.spool.meta.Get(e.id)
			if err != nil {
				t.Fatal(err)
			}
			want := sha256.Sum256(body)
			if row.DrainState != metadata.DrainReady || row.Size != int64(len(body)) || !bytes.Equal(row.SHA256, want[:]) {
				t.Fatalf("received image not qualified: %+v", row)
			}
			if f.spool.used.Load() != used {
				t.Fatal("shutdown refunded saved bytes")
			}
			if err := f.db.Close(); err != nil {
				t.Fatal(err)
			}
			f.db, err = metadata.Open(f.dbPath)
			if err != nil {
				t.Fatal(err)
			}
			m := metadata.NewSpoolStore(f.db.DB())
			f.db.SetSpoolStore(m)
			f.spool, err = NewSpoolStore(f.root, 0, m)
			if err != nil {
				t.Fatal(err)
			}
			report, err := f.spool.RecoverOnBoot(context.Background())
			if err != nil || report.WritingFailedRows != 0 || f.spool.used.Load() != used {
				t.Fatalf("restart=(%+v,%v), used=%d", report, err, f.spool.used.Load())
			}
			f.h = NewHandler(f.db, f.backing)
			f.h.SetSpool(f.spool, nil)
			r, err := (&juiceFS{handler: f.h}).Open(name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil || closeErr != nil || !bytes.Equal(got, body) {
				t.Fatalf("restart read=(%d,%v,%v)", len(got), err, closeErr)
			}
			files, err := os.ReadDir(f.backing)
			if err != nil || len(files) != 0 {
				t.Fatalf("unexpected backing I/O: %v %v", files, err)
			}
		})
	}
}

func TestShutdownImageCoverageAndCustody(t *testing.T) {
	shutdownOffline(t)
	for _, kind := range []string{"coalesced", "empty", "gap", "preallocated", "failed_write", "live_handle", "replacement", "retired", "unlinked", "replaced", "hardlink", "superseded", "stream", "punched", "pending", "missing_initial", "mirror_replaced", "sync", "hash", "cas"} {
		t.Run(kind, func(t *testing.T) {
			f := newShutdownFixture(t)
			e := f.create(t, "image.bin", nil)
			wf, err := f.spool.OpenWrite("image.bin")
			if err != nil {
				t.Fatal(err)
			}
			write := func(p string, off int64) {
				t.Helper()
				if n, err := wf.WriteAt([]byte(p), off); err != nil || n != len(p) {
					t.Fatalf("write=(%d,%v)", n, err)
				}
			}
			if kind == "coalesced" || kind == "gap" {
				write("tail", 4)
				if kind == "coalesced" {
					write("head", 0)
				}
			} else if kind != "empty" {
				write("headtail", 0)
			}
			if kind == "preallocated" {
				if err := wf.Truncate(32); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "failed_write" {
				if _, err := wf.WriteAt([]byte("bad"), -1); err == nil {
					t.Fatal("expected failed write")
				}
			}
			if kind != "live_handle" {
				wf.ReleaseHandle()
			}
			switch kind {
			case "replacement":
				e.replacement = &metadata.SpoolReplacementWitness{}
			case "retired":
				e.retired.Store(true)
			case "unlinked":
				if err := os.Remove(e.spoolFile); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				if err := os.Rename(e.spoolFile, e.spoolFile+".held"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(e.spoolFile, []byte("occupant"), 0600); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(e.spoolFile, e.spoolFile+".link"); err != nil {
					t.Fatal(err)
				}
			case "superseded":
				f.spool.index.Insert("image.bin", &SpoolEntry{closed: true})
			case "stream":
				e.streamDest = "remote-stage"
			case "punched":
				e.punchedEnd = 1
			case "pending":
				f.spool.retainPendingBoundContinuation(e.id, e.nfsPath, e.spoolFile, e.file)
			case "missing_initial":
				e.initialIdentity = ""
			case "mirror_replaced":
				if _, err := f.db.DB().Exec(`UPDATE entries SET inode=inode+1 WHERE path='image.bin'`); err != nil {
					t.Fatal(err)
				}
			case "sync", "hash", "cas":
				f.spool.shutdownFault = func(stage string) error {
					if stage == kind {
						return errors.New("injected " + kind)
					}
					return nil
				}
			}
			// Preserve exact descriptor bytes, SQL and charge on every refusal.
			beforeRow, err := f.spool.meta.Get(e.id)
			if err != nil {
				t.Fatal(err)
			}
			before := make([]byte, e.writtenEnd)
			if len(before) > 0 {
				if _, err := e.file.ReadAt(before, 0); err != nil {
					t.Fatal(err)
				}
			}
			used := f.spool.used.Load()
			if !f.h.stopHandlerWithDeadline(time.Second) {
				t.Fatal("shutdown incomplete")
			}
			row, err := f.spool.meta.Get(e.id)
			if err != nil {
				t.Fatal(err)
			}
			pass := kind == "coalesced" || kind == "empty"
			if pass {
				want := sha256.Sum256(before)
				if row.DrainState != metadata.DrainReady || row.Size != int64(len(before)) || !bytes.Equal(row.SHA256, want[:]) {
					t.Fatalf("not qualified: %+v", row)
				}
			} else {
				if row.DrainState != beforeRow.DrainState || row.Size != beforeRow.Size || !bytes.Equal(row.SHA256, beforeRow.SHA256) {
					t.Fatalf("refusal changed row: %+v", row)
				}
				after := make([]byte, len(before))
				if len(after) > 0 {
					if _, err := e.file.ReadAt(after, 0); err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(after, before) {
					t.Fatal("refusal changed source bytes")
				}
			}
			if f.spool.used.Load() != used {
				t.Fatal("capacity changed")
			}
			e.cancelClose() // original worker is joined, including displaced entries
		})
	}
}

func TestShutdownFinalizationTimeoutRetainsSingleWorker(t *testing.T) {
	shutdownOffline(t)
	f := newShutdownFixture(t)
	e := f.create(t, "slow.bin", []byte("complete bytes"))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	f.spool.shutdownFault = func(stage string) error {
		if stage == "sync" {
			calls.Add(1)
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}
	t.Cleanup(func() {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		default:
			close(release)
		}
	})
	if f.h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("reported complete while worker owns FD")
	}
	<-entered
	original := f.h.spoolFinalizeDone
	if f.h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("retry reported complete")
	}
	if f.h.spoolFinalizeDone != original || calls.Load() != 1 {
		t.Fatal("duplicate worker")
	}
	if _, err := e.file.Stat(); err != nil {
		t.Fatal("closed worker descriptor", err)
	}
	if err := f.db.DB().Ping(); err != nil {
		t.Fatal("closed worker database", err)
	}
	if _, err := f.spool.OpenWrite("new.bin"); err == nil {
		t.Fatal("admission reopened")
	}
	if _, err := e.WriteAt([]byte("bad"), 0); err == nil {
		t.Fatal("sealed image writable")
	}
	close(release)
	if !f.h.stopHandlerWithDeadline(time.Second) {
		t.Fatal("retry failed to join original worker")
	}
	row, err := f.spool.meta.Get(e.id)
	if err != nil || row.DrainState != metadata.DrainWriting {
		t.Fatalf("canceled proof published: %+v %v", row, err)
	}
}

// The older idle finalizer makes sparse images readable. That cannot invent
// received-byte coverage for a later shutdown proof after READY reopening.
func TestShutdownRefusesCoverageLostBeforeReopen(t *testing.T) {
	shutdownOffline(t)
	for _, name := range []string{"gap.bin", "preallocated.bin", "._sidecar"} {
		t.Run(name, func(t *testing.T) {
			f := newShutdownFixture(t)
			e := f.create(t, name, nil)
			wf, err := f.spool.OpenWrite(name)
			if err != nil {
				t.Fatal(err)
			}
			if name == "preallocated.bin" {
				if err := wf.Truncate(8); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := wf.WriteAt([]byte("tail"), 4); err != nil {
				t.Fatal(err)
			}
			wf.ReleaseHandle()
			if e.contiguousEnd != 0 {
				t.Fatal("fixture has no gap")
			}
			if err := e.Finalize(); err != nil {
				t.Fatal(err)
			}
			reopened, err := f.spool.OpenWrite(name)
			if err != nil {
				t.Fatal(err)
			}
			if reopened != e {
				t.Fatal("not same live entry")
			}
			reopened.ReleaseHandle()
			before, err := os.ReadFile(e.spoolFile)
			if err != nil {
				t.Fatal(err)
			}
			used := f.spool.used.Load()
			if !f.h.stopHandlerWithDeadline(time.Second) {
				t.Fatal("shutdown incomplete")
			}
			row, err := f.spool.meta.Get(e.id)
			if err != nil {
				t.Fatal(err)
			}
			if row.DrainState != metadata.DrainWriting {
				t.Fatalf("fabricated coverage accepted after reopen: %s", row.DrainState)
			}
			after, err := os.ReadFile(e.spoolFile)
			if err != nil || !bytes.Equal(before, after) || f.spool.used.Load() != used {
				t.Fatal("refusal changed image or charge", err)
			}
		})
	}
}
