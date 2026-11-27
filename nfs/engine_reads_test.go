package nfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine/wire"
	"github.com/lelanddutcher/juicemount/metadata"
)

type fakeEngineErr syscall.Errno

func (e fakeEngineErr) Error() string               { return fmt.Sprintf("engine: %v", syscall.Errno(e)) }
func (e fakeEngineErr) EngineStatus() syscall.Errno { return syscall.Errno(e) }

// fakeEngineClient serves files by inode from RAM: bytes that differ from
// the FUSE folder, so a test sees which door answered.
type fakeEngineClient struct {
	mu       sync.Mutex
	files    map[uint64][]byte
	next     uint64
	open     map[uint64]uint64 // handle -> inode
	opens    int
	reads    int
	releases int
	failRead error // returned once by Read
	openErr  error
}

func newFakeEngineClient() *fakeEngineClient {
	return &fakeEngineClient{files: map[uint64][]byte{}, open: map[uint64]uint64{}, next: 10}
}

func (c *fakeEngineClient) Open(ino uint64, _ time.Duration) (wire.OpenReply, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.openErr != nil {
		return wire.OpenReply{}, c.openErr
	}
	b, ok := c.files[ino]
	if !ok {
		return wire.OpenReply{}, fakeEngineErr(syscall.ENOENT)
	}
	c.next++
	c.open[c.next] = ino
	c.opens++
	return wire.OpenReply{Handle: c.next, Attr: wire.Attr{Inode: ino, Size: uint64(len(b))}}, nil
}

func (c *fakeEngineClient) Read(ino, fh uint64, p []byte, off int64, _ time.Duration) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.failRead; err != nil {
		c.failRead = nil
		return 0, err
	}
	if c.open[fh] != ino {
		return 0, fakeEngineErr(syscall.EBADF)
	}
	c.reads++
	b := c.files[ino]
	if off >= int64(len(b)) {
		return 0, nil
	}
	return copy(p, b[off:]), nil
}

func (c *fakeEngineClient) Release(ino, fh uint64, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.open[fh] != ino {
		return fakeEngineErr(syscall.EBADF)
	}
	delete(c.open, fh)
	c.releases++
	return nil
}

func (c *fakeEngineClient) counts() (opens, reads, releases, live int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens, c.reads, c.releases, len(c.open)
}

// engineSource is a switchable source: ready, epoch and client.
type engineSource struct {
	mu    sync.Mutex
	cl    *fakeEngineClient
	epoch uint64
	ready bool
}

func (s *engineSource) get() (EngineReadClient, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ready {
		return nil, 0, false
	}
	return s.cl, s.epoch, true
}

func engineHarness(t *testing.T) (*juiceFS, *metadata.Store, string, *engineSource) {
	t.Helper()
	jfs, store, root := newIntegrityHarness(t)
	src := &engineSource{cl: newFakeEngineClient(), epoch: 1, ready: true}
	jfs.handler.SetEngineReads(src.get)
	return jfs, store, root, src
}

func inodeOf(t *testing.T, store *metadata.Store, rel string) uint64 {
	t.Helper()
	e := store.LookupByPath(rel)
	if e == nil {
		t.Fatalf("no mirror entry for %s", rel)
	}
	return e.Inode
}

func TestEngineReadsServeThroughTheSocket(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "a/clip.mov", []byte("FUSE-BYTES-0123456789"))
	ino := inodeOf(t, store, "a/clip.mov")
	src.cl.files[ino] = []byte("ENGINE-BYTES-abcdefgh")

	if got := readVia(t, jfs, "a/clip.mov", 64); string(got) != "ENGINE-BYTES-abcdefgh" {
		t.Fatalf("read %q, want the engine's bytes", got)
	}
	if got := readVia(t, jfs, "a/clip.mov", 64); string(got) != "ENGINE-BYTES-abcdefgh" {
		t.Fatalf("second read %q", got)
	}
	opens, reads, _, live := src.cl.counts()
	if opens != 1 || reads != 2 || live != 1 {
		t.Fatalf("opens %d reads %d live %d; want one pooled handle reused", opens, reads, live)
	}
	// Turning the level off is today's path.
	jfs.handler.SetEngineReads(nil)
	if got := readVia(t, jfs, "a/clip.mov", 64); string(got) != "FUSE-BYTES-0123456789" {
		t.Fatalf("off: read %q, want the mount's bytes", got)
	}
	if _, _, releases, live := src.cl.counts(); releases != 1 || live != 0 {
		t.Fatalf("turning off left %d handles open (%d released)", live, releases)
	}
}

func TestEngineReadsFallBackToFUSE(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "clip.mov", []byte("FUSE"))
	src.cl.files[inodeOf(t, store, "clip.mov")] = []byte("ENGINE")

	src.mu.Lock()
	src.ready = false
	src.mu.Unlock()
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "FUSE" {
		t.Fatalf("socket not ready: read %q, want the mount", got)
	}
	src.mu.Lock()
	src.ready = true
	src.mu.Unlock()
	src.cl.openErr = fakeEngineErr(syscall.EACCES)
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "FUSE" {
		t.Fatalf("open refused: read %q, want the mount", got)
	}
	src.cl.openErr = nil
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "ENGINE" {
		t.Fatalf("socket back: read %q", got)
	}
}

func TestEngineReadsSkipSidecarsAndSyntheticInodes(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "._clip.mov", []byte("APPLEDOUBLE"))
	src.cl.files[inodeOf(t, store, "._clip.mov")] = []byte("WRONG")
	if got := readVia(t, jfs, "._clip.mov", 32); string(got) != "APPLEDOUBLE" {
		t.Fatalf("AppleDouble read %q, want the mount (its proofs are tied to a descriptor)", got)
	}
	// A synthetic inode (a spool-only file) never reaches the engine.
	full := filepath.Join(root, "local.mov")
	if err := os.WriteFile(full, []byte("LOCAL"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(full)
	syn := uint64(1<<63 | 77)
	if err := store.Insert(metadata.MakeEntry("local.mov", false, fi.Size(), fi.ModTime(), syn)); err != nil {
		t.Fatal(err)
	}
	src.cl.files[syn] = []byte("WRONG")
	if got := readVia(t, jfs, "local.mov", 32); string(got) != "LOCAL" {
		t.Fatalf("synthetic inode read %q", got)
	}
	if opens, _, _, _ := src.cl.counts(); opens != 0 {
		t.Fatalf("%d engine opens for files that must not use it", opens)
	}
}

// A file the mirror shows as changed since the handle opened is reopened
// (a fresh open reads fresh state); a READ that began before the handle
// opened reuses it.
func TestEngineHandleReopensWhenTheMirrorMoved(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "clip.mov", []byte("v1"))
	ino := inodeOf(t, store, "clip.mov")
	src.cl.files[ino] = []byte("v1")
	readVia(t, jfs, "clip.mov", 8)

	// A teammate's save: same inode, new size and time in the mirror.
	time.Sleep(10 * time.Millisecond)
	src.cl.mu.Lock()
	src.cl.files[ino] = []byte("v2-longer")
	src.cl.mu.Unlock()
	fi, _ := os.Stat(filepath.Join(root, "clip.mov"))
	if err := store.Insert(metadata.MakeEntry("clip.mov", false, 9, fi.ModTime().Add(time.Second), ino)); err != nil {
		t.Fatal(err)
	}
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "v2-longer" {
		t.Fatalf("after the edit: %q", got)
	}
	waitEngineCounts(t, src.cl, 2, 1, 1)
	readVia(t, jfs, "clip.mov", 16)
	if opens, _, _, _ := src.cl.counts(); opens != 2 {
		t.Fatalf("an unchanged mirror reopened (opens %d)", opens)
	}
}

func TestEngineHandleReopensOnANewEpoch(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "clip.mov", []byte("x"))
	ino := inodeOf(t, store, "clip.mov")
	src.cl.files[ino] = []byte("old-engine")
	readVia(t, jfs, "clip.mov", 16)

	// The mount restarted: a new socket, a new epoch, no old handles.
	fresh := newFakeEngineClient()
	fresh.files[ino] = []byte("new-engine")
	src.mu.Lock()
	src.cl, src.epoch = fresh, 2
	src.mu.Unlock()
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "new-engine" {
		t.Fatalf("after a restart: %q", got)
	}
	if opens, _, _, _ := fresh.counts(); opens != 1 {
		t.Fatalf("the new engine got %d opens, want 1 (reopen by inode)", opens)
	}
}

// A handle the engine no longer knows (EBADF) is dropped; the read is
// retried and the next READ reopens by inode.
func TestEngineBrokenHandleReopens(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "clip.mov", []byte("x"))
	ino := inodeOf(t, store, "clip.mov")
	src.cl.files[ino] = []byte("engine")
	readVia(t, jfs, "clip.mov", 16)
	src.cl.mu.Lock()
	src.cl.failRead = fakeEngineErr(syscall.EBADF)
	src.cl.mu.Unlock()
	f, err := jfs.OpenFile("clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := f.ReadAt(buf, 0)
	f.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		// The cold-read retry repeats an EIO on the same handle; the fake
		// fails once, so the retry already succeeds.
		t.Fatalf("read after EBADF: %v", err)
	}
	if string(buf[:n]) != "engine" {
		t.Fatalf("read after EBADF: %q", buf[:n])
	}
	if got := readVia(t, jfs, "clip.mov", 16); string(got) != "engine" {
		t.Fatalf("next READ: %q", got)
	}
	if opens, _, _, _ := src.cl.counts(); opens != 2 {
		t.Fatalf("opens %d, want a reopen after the broken handle", opens)
	}
}

func TestEngineReadAtSemantics(t *testing.T) {
	cl := newFakeEngineClient()
	cl.files[5] = bytes.Repeat([]byte{7}, 100)
	p := newEngineHandlePool(func() (EngineReadClient, uint64, bool) { return cl, 1, true })
	defer p.close()
	e, err := p.acquire(5, engineStamp{}, time.Now(), false, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if n, err := e.readAt(buf, 0, time.Second); n != 64 || err != nil {
		t.Fatalf("full read: %d %v", n, err)
	}
	if n, err := e.readAt(buf, 80, time.Second); n != 20 || err != io.EOF {
		t.Fatalf("read across the end: %d %v, want 20 and EOF", n, err)
	}
	if n, err := e.readAt(buf, 200, time.Second); n != 0 || err != io.EOF {
		t.Fatalf("read past the end: %d %v", n, err)
	}
	cl.failRead = fakeEngineErr(syscall.EIO)
	if _, err := e.readAt(buf, 0, time.Second); !errors.Is(err, syscall.EIO) {
		t.Fatalf("engine EIO: %v", err)
	}
	cl.failRead = os.ErrDeadlineExceeded
	if _, err := e.readAt(buf, 0, time.Second); !errors.Is(err, errFUSETimeout) {
		t.Fatalf("timeout: %v, want the JUKEBOX timeout", err)
	}
	p.mu.Lock()
	broken := e.broken
	p.mu.Unlock()
	if broken {
		t.Fatal("a timed-out call broke the handle; an offline READ would lose it")
	}
	p.release(e)
}

func TestEngineHandleSingleFlightAndIdleClose(t *testing.T) {
	old := engineHandleIdle
	engineHandleIdle = 40 * time.Millisecond
	t.Cleanup(func() { engineHandleIdle = old })
	cl := newFakeEngineClient()
	cl.files[5] = []byte("x")
	p := newEngineHandlePool(func() (EngineReadClient, uint64, bool) { return cl, 1, true })
	defer p.close()
	var wg sync.WaitGroup
	hs := make([]*engineHandle, 16)
	for i := range hs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := p.acquire(5, engineStamp{}, time.Now(), true, time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			hs[i] = h
		}(i)
	}
	wg.Wait()
	for _, h := range hs {
		if h != nil {
			p.release(h)
		}
	}
	if opens, _, _, _ := cl.counts(); opens != 1 {
		t.Fatalf("%d opens for 16 concurrent READs of one inode, want 1", opens)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, releases, live := cl.counts(); releases == 1 && live == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("an idle handle was not closed")
}

// Rule 5 (read a speculative block in pieces) is for macFUSE only: an
// engine source reads the block in one call.
func TestReadaheadEngineBlockIsOnePiece(t *testing.T) {
	cl := newFakeEngineClient()
	cl.files[5] = bytes.Repeat([]byte{1}, readaheadBlockSize)
	p := newEngineHandlePool(func() (EngineReadClient, uint64, bool) { return cl, 1, true })
	defer p.close()
	src, release := p.prefetchSource(5)
	if src == nil {
		t.Fatal("no engine prefetch source")
	}
	defer release()
	rm := &ReadaheadManager{demandFirst: func() bool { return true }}
	buf := make([]byte, readaheadBlockSize)
	n, _, err := rm.readBlock(src, buf, 0, nil, nil)
	if n != readaheadBlockSize || (err != nil && err != io.EOF) {
		t.Fatalf("block: %d %v", n, err)
	}
	if _, reads, _, _ := cl.counts(); reads != 1 {
		t.Fatalf("%d engine reads for one block, want 1 (no rule-5 pieces)", reads)
	}
	if fi, _ := src.Stat(); fi.Size() != readaheadBlockSize {
		t.Fatalf("stat size %d", fi.Size())
	}
}

func TestEngineHandlesReleasedOnStop(t *testing.T) {
	jfs, store, root, src := engineHarness(t)
	seedFile(t, jfs, store, root, "clip.mov", []byte("x"))
	src.cl.files[inodeOf(t, store, "clip.mov")] = []byte("engine")
	readVia(t, jfs, "clip.mov", 16)
	jfs.handler.StopHandler()
	if _, _, releases, live := src.cl.counts(); releases != 1 || live != 0 {
		t.Fatalf("after stop: %d live, %d released", live, releases)
	}
}

// waitEngineCounts waits for a retired handle's release, which runs on its
// own goroutine.
func waitEngineCounts(t *testing.T, cl *fakeEngineClient, opens, releases, live int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		o, _, r, l := cl.counts()
		if o == opens && r == releases && l == live {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("opens %d releases %d live %d; want %d %d %d", o, r, l, opens, releases, live)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
