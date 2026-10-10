package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func TestOnlineSetattrShrinkPublishesExactSize(t *testing.T) {
	for _, populated := range []bool{false, true} {
		name := "absent_high_water"
		if populated {
			name = "populated_high_water"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			const path = "reel.mov"
			payload := patternedPayload(128<<10, 47)
			seedFile(t, jfs, store, fuseRoot, path, payload)
			if populated {
				jfs.handler.trackWriteSize(path, int64(len(payload)))
			}
			for _, size := range []uint64{64 << 10, 0} {
				attrs := &nfslib.SetFileAttributes{SetSize: &size}
				if err := attrs.Apply(jfs.handler.Change(jfs), jfs, path); err != nil {
					t.Fatalf("SETATTR size %d: %v", size, err)
				}
				if got := onDisk(t, fuseRoot, path); !bytes.Equal(got, payload[:size]) {
					t.Errorf("backing bytes after truncate to %d differ from preserved prefix (length %d)", size, len(got))
				}
				if got := store.LookupByPath(path).Size; got != int64(size) {
					t.Errorf("mirror size = %d, want %d immediately after SETATTR", got, size)
				}
				for label, stat := range map[string]func(string) (os.FileInfo, error){"Stat": jfs.Stat, "Lstat": jfs.Lstat} {
					fi, err := stat(path)
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
					if fi.Size() != int64(size) {
						t.Errorf("%s size = %d, want %d", label, fi.Size(), size)
					}
				}
				if size > 0 {
					if got := readVia(t, jfs, path, int(size)); !bytes.Equal(got, payload[:size]) {
						t.Errorf("NFS read lost valid prefix after truncate to %d", size)
					}
				}
				f, err := jfs.OpenFile(path, os.O_RDONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, 4096)
				n, readErr := f.ReadAt(buf, int64(size))
				_ = f.Close()
				if n != 0 || readErr != io.EOF {
					t.Errorf("read at new EOF %d = (%d, %v), want (0, EOF)", size, n, readErr)
				}
			}
		})
	}
}

func TestOnlineTruncateOrdersBackingMutationAndPublication(t *testing.T) {
	for _, first := range []string{"truncate", "write"} {
		t.Run(first+"_first", func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			const path = "reel.mov"
			payload := patternedPayload(128<<10, 61)
			seedFile(t, jfs, store, fuseRoot, path, payload)
			writer, err := jfs.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			truncater, err := jfs.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			paused, resume := make(chan struct{}), make(chan struct{})
			jfs.handler.testAfterInPlaceMutation = func(_ string, operation string) {
				if operation == first {
					close(paused)
					<-resume
				}
			}
			write := func() error {
				_, err := writer.(io.WriterAt).WriteAt(payload[64<<10:96<<10], 64<<10)
				return err
			}
			truncate := func() error { return truncater.Truncate(64 << 10) }
			firstOp, secondOp := truncate, write
			want := int64(96 << 10)
			if first == "write" {
				firstOp, secondOp = write, truncate
				want = 64 << 10
			}
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			go func() { firstDone <- firstOp() }()
			select {
			case <-paused:
			case <-time.After(2 * time.Second):
				t.Fatal("first backing mutation did not reach publication barrier")
			}
			go func() { secondDone <- secondOp() }()
			select {
			case err := <-secondDone:
				if !errors.Is(err, errFUSETimeout) {
					t.Errorf("second operation passed an unpublished %s: %v", first, err)
				}
				secondDone <- err
			case <-time.After(100 * time.Millisecond):
			}
			close(resume)
			retry := false
			for _, done := range []chan error{firstDone, secondDone} {
				select {
				case err := <-done:
					if errors.Is(err, errFUSETimeout) && done == secondDone {
						retry = true
					} else if err != nil {
						t.Error(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("mutation did not finish")
				}
			}
			if retry {
				if err := secondOp(); err != nil {
					t.Fatal(err)
				}
			}
			_ = writer.Close()
			_ = truncater.Close()
			if got := int64(len(onDisk(t, fuseRoot, path))); got != want {
				t.Errorf("backing size = %d, want %d", got, want)
			}
			if got := store.LookupByPath(path).Size; got != want {
				t.Errorf("mirror size = %d, want %d", got, want)
			}
		})
	}
}

func TestOnlineTruncateLateWriterClosePreservesSize(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const path = "reel.mov"
	payload := patternedPayload(128<<10, 53)
	seedFile(t, jfs, store, fuseRoot, path, payload)
	openWriter := func() *writeFile {
		t.Helper()
		f, err := jfs.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		wf, ok := f.(*writeFile)
		if !ok {
			_ = f.Close()
			t.Fatalf("write handle = %T, want *writeFile", f)
		}
		return wf
	}
	late, continuing := openWriter(), openWriter()
	if _, err := late.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := continuing.WriteAt(payload, 0); err != nil {
		t.Fatal(err)
	}
	size := uint64(64 << 10)
	attrs := &nfslib.SetFileAttributes{SetSize: &size}
	if err := attrs.Apply(jfs.handler.Change(jfs), jfs, path); err != nil {
		t.Fatal(err)
	}
	if err := late.Close(); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath(path).Size; got != int64(size) {
		t.Fatalf("late Close restored old size: got %d, want %d", got, size)
	}
	// An old handle can regrow the file without exceeding its pre-truncate
	// writtenEnd. Its new write must still raise the shared high-water.
	if _, err := continuing.WriteAt(payload[64<<10:96<<10], 64<<10); err != nil {
		t.Fatal(err)
	}
	if err := continuing.Close(); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath(path).Size; got != 96<<10 {
		t.Fatalf("existing writer failed to regrow mirror: got %d, want %d", got, 96<<10)
	}

	// Independent WRITE RPC handles close in the opposite offset order.
	low, high := openWriter(), openWriter()
	if _, err := low.WriteAt(payload[:4096], 0); err != nil {
		t.Fatal(err)
	}
	if _, err := high.WriteAt(payload[96<<10:128<<10], 96<<10); err != nil {
		t.Fatal(err)
	}
	if err := high.Close(); err != nil {
		t.Fatal(err)
	}
	if err := low.Close(); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath(path).Size; got != 128<<10 {
		t.Fatalf("out-of-order Close lost high-water: got %d, want %d", got, 128<<10)
	}
	if got := onDisk(t, fuseRoot, path); !bytes.Equal(got, payload) {
		t.Fatal("post-truncate WRITE did not preserve expected content")
	}
}

func TestOnlineTruncateFailurePreservesSize(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const path = "reel.mov"
	payload := patternedPayload(128<<10, 59)
	seedFile(t, jfs, store, fuseRoot, path, payload)
	jfs.handler.trackWriteSize(path, int64(len(payload)))
	f, err := jfs.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(-1); err == nil {
		t.Fatal("negative truncate succeeded")
	}
	if got := store.LookupByPath(path).Size; got != int64(len(payload)) {
		t.Errorf("failed truncate changed mirror size to %d", got)
	}
	jfs.handler.writeSizeMu.Lock()
	got := jfs.handler.writeSizes[path]
	jfs.handler.writeSizeMu.Unlock()
	if got != int64(len(payload)) {
		t.Errorf("failed truncate changed high-water to %d", got)
	}
	if got := onDisk(t, fuseRoot, path); !bytes.Equal(got, payload) {
		t.Fatal("failed truncate changed backing bytes")
	}
}

func TestOnlineTruncatePublicationFailurePoisonsServing(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(128<<10, 67))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_truncate_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'injected size publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	size := uint64(0)
	attrs := &nfslib.SetFileAttributes{SetSize: &size}
	if err := attrs.Apply(jfs.handler.Change(jfs), jfs, path); err == nil {
		t.Fatal("SETATTR acknowledged a failed durable size publication")
	}
	if got := len(onDisk(t, fuseRoot, path)); got != 0 {
		t.Errorf("backing size = %d, want 0", got)
	}
	if got := store.LookupByPath(path).Size; got != 128<<10 {
		t.Errorf("failed CAS published uncommitted RAM size %d", got)
	}
	for label, stat := range map[string]func(string) (os.FileInfo, error){"Stat": jfs.Stat, "Lstat": jfs.Lstat} {
		fi, err := stat(path)
		if !errors.Is(err, syscall.EIO) || fi != nil {
			t.Errorf("%s = (%v, %v), want poisoned serving refusal", label, fi, err)
		}
	}
	f, err := jfs.OpenFile(path, os.O_RDONLY, 0)
	if f != nil {
		_ = f.Close()
	}
	if !errors.Is(err, syscall.EIO) {
		t.Errorf("open after failed publication = %v, want EIO", err)
	}
	if err := jfs.CommitFile(path); !errors.Is(err, syscall.EIO) {
		t.Errorf("COMMIT after failed publication = %v, want EIO", err)
	}
	var durableSize int64
	if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, path).Scan(&durableSize); err != nil {
		t.Fatal(err)
	}
	if durableSize != 128<<10 {
		t.Fatalf("injected failure did not preserve old durable row: %d", durableSize)
	}
	if _, err := store.DB().Exec(`DROP TRIGGER fail_truncate_size`); err != nil {
		t.Fatal(err)
	}
	if err := attrs.Apply(jfs.handler.Change(jfs), jfs, path); err == nil {
		t.Fatal("removing SQL fault silently cleared unresolved backing mutation")
	}
}

func TestOnlineWriteReturnsSizePublicationError(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
	const path = "reel.mov"
	seedFile(t, jfs, store, fuseRoot, path, patternedPayload(4096, 71))
	if err := store.Insert(store.LookupByPath(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_write_size BEFORE UPDATE OF size ON entries BEGIN SELECT RAISE(FAIL, 'injected write size publication failure'); END`); err != nil {
		t.Fatal(err)
	}
	f, err := jfs.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.(io.WriterAt).WriteAt(patternedPayload(4096, 73), 4096); err == nil {
		t.Fatal("WRITE acknowledged a failed durable size publication")
	}
	if err := f.Close(); err == nil {
		t.Fatal("WRITE close acknowledged a failed durable size publication")
	}
	if got := len(onDisk(t, fuseRoot, path)); got != 8192 {
		t.Fatalf("backing size = %d, want 8192", got)
	}
	var size int64
	if err := store.DB().QueryRow(`SELECT size FROM entries WHERE path = ?`, path).Scan(&size); err != nil || size != 4096 {
		t.Fatalf("fault injection durable size = (%d, %v), want 4096", size, err)
	}
}
