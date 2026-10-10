package nfs

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

func TestOnlineReadAdmissionRejectsAlreadyAdmittedBytes(t *testing.T) {
	for _, failedSync := range []bool{false, true} {
		name := "successful_overlap"
		if failedSync {
			name = "failed_sync"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			if failedSync {
				t.Cleanup(func() { discardFailedNamespaceForTest(jfs.handler) })
			}
			seedFile(t, jfs, store, root, "reel.mov", patternedPayload(4096, 211))
			reader, err := jfs.OpenFile("reel.mov", os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			writer, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			paused, resume := make(chan struct{}), make(chan struct{})
			jfs.handler.testReadAfterServingCheck = func(string) {
				close(paused)
				<-resume
			}
			buf := bytes.Repeat([]byte{0xa5}, 4096)
			type result struct {
				n   int
				err error
			}
			done := make(chan result, 1)
			go func() {
				n, err := reader.ReadAt(buf, 0)
				done <- result{n, err}
			}()
			select {
			case <-paused:
			case <-time.After(2 * time.Second):
				close(resume)
				t.Fatal("read never reached post-admission barrier")
			}
			if failedSync {
				jfs.handler.testSyncOnlineMutation = func(*os.File) error { return errors.New("injected sync failure") }
			}
			_, writeErr := writer.(io.WriterAt).WriteAt(patternedPayload(4096, 223), 0)
			if failedSync && writeErr == nil || !failedSync && writeErr != nil {
				close(resume)
				t.Fatalf("write = %v, failedSync=%v", writeErr, failedSync)
			}
			close(resume)
			select {
			case got := <-done:
				want := error(errFUSETimeout)
				if failedSync {
					want = syscall.EIO
				}
				if got.n != 0 || !errors.Is(got.err, want) {
					t.Fatalf("already-admitted read = (%d,%v), want (0,%v)", got.n, got.err, want)
				}
				if !bytes.Equal(buf, make([]byte, len(buf))) {
					t.Fatal("invalidated read retained copied bytes in caller buffer")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("read did not leave post-admission barrier")
			}
			if failedSync {
				requirePathIntent(t, store, "write", 4096, 4096)
			}
		})
	}
}

func TestOnlineReadAdmissionUnrelatedPathAndPruning(t *testing.T) {
	var c namespaceCoordinator
	r, err := c.admitRead("scene/a.mov")
	if err != nil {
		t.Fatal(err)
	}
	op := c.begin([]string{"other/b.mov"})
	if op == nil {
		t.Fatal("unrelated mutation was refused")
	}
	op.finish(nil)
	if err := r.check(); err != nil {
		t.Fatalf("unrelated mutation invalidated read: %v", err)
	}
	r.release()
	if len(c.reads) != 0 {
		t.Fatalf("retired read epochs leaked: %+v", c.reads)
	}
	r, err = c.admitRead("scene/a.mov")
	if err != nil {
		t.Fatal(err)
	}
	op = c.begin([]string{"scene"})
	if op == nil {
		t.Fatal("ancestor mutation was refused")
	}
	op.finish(nil)
	if err := r.check(); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("ancestor mutation check = %v, want retry", err)
	}
	r.release()
}

func TestOnlineReadAdmissionZeroCountAndEOFResponse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset uint64
		count  uint32
	}{
		{"zero_count", 0, 0},
		{"past_eof", 8192, 64 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jfs, store, root := newIntegrityHarness(t)
			seedFile(t, jfs, store, root, "reel.mov", patternedPayload(4096, 227))
			handle := jfs.handler.ToHandle(jfs, []string{"reel.mov"})
			address := bindingRPCAddress(t, jfs.handler)
			paused, resume := make(chan struct{}), make(chan struct{})
			jfs.handler.testReadAfterAdmission = func(string) {
				close(paused)
				<-resume
			}
			type result struct {
				status uint32
				err    error
			}
			done := make(chan result, 1)
			go func() {
				status, err := bindingRPCStatus(address, nfslib.NFSProcedureRead, struct {
					Handle []byte
					Offset uint64
					Count  uint32
				}{handle, tc.offset, tc.count})
				done <- result{status, err}
			}()
			select {
			case <-paused:
			case <-time.After(2 * time.Second):
				close(resume)
				t.Fatal("READ never admitted")
			}
			writer, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
			if err != nil {
				close(resume)
				t.Fatal(err)
			}
			if _, err := writer.(io.WriterAt).WriteAt(patternedPayload(4096, 229), 0); err != nil {
				close(resume)
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				close(resume)
				t.Fatal(err)
			}
			close(resume)
			select {
			case got := <-done:
				if got.err != nil || got.status != uint32(nfslib.NFSStatusJukebox) {
					t.Fatalf("overlapped zero-byte READ = (%d,%v), want JUKEBOX", got.status, got.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("READ did not finish")
			}
			if pending, err := store.InspectOnlineMutations(); err != nil || len(pending) != 0 {
				t.Fatalf("successful write left intent: %+v, %v", pending, err)
			}
		})
	}
}

func TestOnlineReadAdmissionMultiSubreadPostAttrs(t *testing.T) {
	jfs, store, root := newIntegrityHarness(t)
	seedFile(t, jfs, store, root, "reel.mov", patternedPayload(1<<20, 233))
	handle := jfs.handler.ToHandle(jfs, []string{"reel.mov"})
	address := bindingRPCAddress(t, jfs.handler)
	paused, resume := make(chan struct{}), make(chan struct{})
	var infoCalls atomic.Int32
	jfs.handler.testReadCachedInfo = func(string) {
		if infoCalls.Add(1) == 2 {
			close(paused)
			<-resume
		}
	}
	type result struct {
		status uint32
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err := bindingRPCStatus(address, nfslib.NFSProcedureRead, struct {
			Handle []byte
			Offset uint64
			Count  uint32
		}{handle, 0, 512 << 10})
		done <- result{status, err}
	}()
	select {
	case <-paused:
	case <-time.After(2 * time.Second):
		close(resume)
		t.Fatal("READ did not reach post-subread attributes")
	}
	writer, err := jfs.OpenFile("reel.mov", os.O_RDWR, 0)
	if err != nil {
		close(resume)
		t.Fatal(err)
	}
	if _, err := writer.(io.WriterAt).WriteAt(patternedPayload(4096, 239), 0); err != nil {
		close(resume)
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		close(resume)
		t.Fatal(err)
	}
	close(resume)
	select {
	case got := <-done:
		if got.err != nil || got.status != uint32(nfslib.NFSStatusJukebox) {
			t.Fatalf("overlapped multi-subread READ = (%d,%v), want JUKEBOX", got.status, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("READ did not finish")
	}
	if infoCalls.Load() < 2 {
		t.Fatalf("CachedInfo calls = %d, want size and post-attrs", infoCalls.Load())
	}
	if pending, err := store.InspectOnlineMutations(); err != nil || len(pending) != 0 {
		t.Fatalf("successful write left intent: %+v, %v", pending, err)
	}
}
