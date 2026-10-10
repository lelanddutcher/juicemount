//go:build darwin

package nfs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func readBoundaryStage(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "stage"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// A stage may contain future copied bytes before those bytes are acknowledged.
// Neither those bytes nor a short stage EOF can substitute for the spool tail.
func TestSpoolReadAcrossAcknowledgedStage(t *testing.T) {
	for _, layout := range []string{"poisoned-tail", "prefix-only"} {
		for _, method := range []string{"ReadAt", "Read"} {
			t.Run(layout+"/"+method, func(t *testing.T) {
				e, src, _ := streamFixture(t, 256<<10)
				defer e.CloseAndDelete("fixture cleanup")
				stage := readBoundaryStage(t)
				const prefix = 64 << 10
				if layout == "poisoned-tail" {
					if _, err := stage.WriteAt(bytes.Repeat([]byte{0xEE}, 256<<10), 0); err != nil {
						t.Fatal(err)
					}
				}
				e.SetStreamDest(stage.Name())
				want := make([]byte, 64)
				if _, err := src.ReadAt(want, prefix-32); err != nil {
					t.Fatal(err)
				}
				if n, err := advanceStream(e, src, stage, nil, spoolMetaDurability{meta: e.store.meta}, destReadbackVerifier{f: stage}, prefix); err != nil || n != prefix {
					t.Fatalf("actual stream advance: %d %v", n, err)
				}
				r := &spoolReadFile{name: e.NFSPath(), entry: e}
				defer r.Close()
				got := make([]byte, len(want))
				var n int
				var err error
				if method == "ReadAt" {
					n, err = r.ReadAt(got, prefix-32)
				} else {
					if _, err = r.Seek(prefix-32, io.SeekStart); err != nil {
						t.Fatal(err)
					}
					n, err = r.Read(got)
				}
				if n != len(want) || err != nil || !bytes.Equal(got, want) {
					t.Errorf("read across acknowledged boundary: n=%d err=%v exact=%v; want both owned ranges", n, err, bytes.Equal(got, want))
				}
				if method == "Read" {
					if pos, _ := r.Seek(0, io.SeekCurrent); pos != prefix+32 {
						t.Errorf("cursor=%d want=%d", pos, prefix+32)
					}
				}
			})
		}
	}
}

type boundaryHeldRead struct {
	*os.File
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *boundaryHeldRead) ReadAt(p []byte, off int64) (int, error) {
	f.once.Do(func() { close(f.entered); <-f.release })
	return f.File.ReadAt(p, off)
}

type boundaryHeldDurability struct {
	streamDurability
	persisted chan struct{}
	publish   chan struct{}
}

func (d boundaryHeldDurability) persistPunchedEnd(id, end int64) error {
	if err := d.streamDurability.persistPunchedEnd(id, end); err != nil {
		return err
	}
	close(d.persisted)
	<-d.publish
	return nil
}

func boundaryWait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not reach barrier", what)
	}
}

// Stop immediately before the real pread, after the live routing decision.
// A published punch must not invalidate a local read already admitted there.
func TestSpoolReadAdmissionExcludesConcurrentPunch(t *testing.T) {
	e, src, _ := streamFixture(t, 256<<10)
	defer e.CloseAndDelete("fixture cleanup")
	stage := readBoundaryStage(t)
	e.SetStreamDest(stage.Name())
	fd, err := e.OpenForRead()
	if err != nil {
		t.Fatal(err)
	}
	held := &boundaryHeldRead{File: fd, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseRead sync.Once
	defer releaseRead.Do(func() { close(held.release) })
	r := &spoolReadFile{name: e.NFSPath(), entry: e, fd: held}
	// Join before Close: the reader owns the per-handle mutex at the barrier.
	readDone := make(chan struct{})
	got, want := make([]byte, 4096), make([]byte, 4096)
	if _, err := src.ReadAt(want, 0); err != nil {
		t.Fatal(err)
	}
	var readN int
	var readErr error
	go func() { readN, readErr = r.ReadAt(got, 0); close(readDone) }()
	defer func() {
		releaseRead.Do(func() { close(held.release) })
		boundaryWait(t, readDone, "reader cleanup")
		_ = r.Close()
	}()
	boundaryWait(t, held.entered, "local read")
	dur := boundaryHeldDurability{streamDurability: spoolMetaDurability{meta: e.store.meta}, persisted: make(chan struct{}), publish: make(chan struct{})}
	var releasePublish sync.Once
	advanceDone := make(chan struct{})
	var advanceN int64
	var advanceErr error
	go func() {
		advanceN, advanceErr = advanceStream(e, src, stage, nil, dur, destReadbackVerifier{f: stage}, 64<<10)
		close(advanceDone)
	}()
	defer func() {
		releaseRead.Do(func() { close(held.release) })
		releasePublish.Do(func() { close(dur.publish) })
		boundaryWait(t, advanceDone, "advance cleanup")
	}()
	boundaryWait(t, dur.persisted, "durable destination prefix")
	if e.mu.TryLock() {
		e.mu.Unlock()
		t.Error("routing snapshot released entry protection before local pread")
		// Expose the consequence on the baseline: actually punch before pread.
		releasePublish.Do(func() { close(dur.publish) })
		boundaryWait(t, advanceDone, "baseline punch")
		releaseRead.Do(func() { close(held.release) })
	} else {
		releaseRead.Do(func() { close(held.release) })
		boundaryWait(t, readDone, "protected local read")
		releasePublish.Do(func() { close(dur.publish) })
	}
	boundaryWait(t, readDone, "local read completion")
	boundaryWait(t, advanceDone, "stream advance completion")
	if readN != len(want) || readErr != nil || !bytes.Equal(got, want) {
		t.Errorf("concurrent read returned punched bytes: n=%d err=%v exact=%v", readN, readErr, bytes.Equal(got, want))
	}
	if advanceN != 64<<10 || advanceErr != nil {
		t.Fatalf("actual punch did not complete: %d %v", advanceN, advanceErr)
	}
	if _, err := src.ReadAt(got, 0); err != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatalf("fixture prefix was not physically punched: %v", err)
	}
	if n, err := r.ReadAt(got, 0); n != len(want) || err != nil || !bytes.Equal(got, want) {
		t.Errorf("later read did not route to stage: n=%d err=%v", n, err)
	}
}

func TestSpoolReadResnapshotsAfterDestinationIO(t *testing.T) {
	e, src, _ := streamFixture(t, 256<<10)
	defer e.CloseAndDelete("fixture cleanup")
	stage := readBoundaryStage(t)
	e.SetStreamDest(stage.Name())
	const prefix = 64 << 10
	want := make([]byte, 64)
	if _, err := src.ReadAt(want, prefix-32); err != nil {
		t.Fatal(err)
	}
	advance := func(end int64) {
		t.Helper()
		if n, err := advanceStream(e, src, stage, nil, spoolMetaDurability{meta: e.store.meta}, destReadbackVerifier{f: stage}, end); err != nil || n != prefix {
			t.Fatalf("actual advance to %d: %d %v", end, n, err)
		}
	}
	advance(prefix)
	fd, err := os.Open(stage.Name())
	if err != nil {
		t.Fatal(err)
	}
	held := &boundaryHeldRead{File: fd, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	r := &spoolReadFile{name: e.NFSPath(), entry: e, destFD: held}
	got := make([]byte, len(want))
	done := make(chan struct{})
	var n int
	go func() { n, err = r.ReadAt(got, prefix-32); close(done) }()
	defer func() {
		release.Do(func() { close(held.release) })
		boundaryWait(t, done, "destination reader cleanup")
		_ = r.Close()
	}()
	boundaryWait(t, held.entered, "destination read")
	if !e.mu.TryLock() {
		t.Fatal("entry lock held across destination I/O")
	}
	e.mu.Unlock()
	// While destination I/O is held, the old spool tail is actually punched.
	// The reader must resnapshot instead of using its original boundary.
	advance(2 * prefix)
	release.Do(func() { close(held.release) })
	boundaryWait(t, done, "destination read completion")
	if n != len(want) || err != nil || !bytes.Equal(got, want) {
		t.Errorf("read used stale tail routing: n=%d err=%v exact=%v", n, err, bytes.Equal(got, want))
	}
}
