package nfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Mac peer edits (laptop run 2026-11-14). These tests model the parts of
// the Mac that a Linux test cannot run:
//
//   - juicefs under macFUSE: a teammate's bytes reach this process's FUSE
//     reads only after the JMRefreshInode message (P1). The model: the FUSE
//     file keeps the old bytes until the test's refresh writes the new ones.
//   - the macOS NFS client: it keeps every page under the attributes of the
//     READ reply that brought it, and drops pages only when it sees a new
//     mtime or size (P2). The model: each READ is the pair (attributes the
//     reply carries, bytes it carries), exactly as onRead builds it: OpenFile
//     snapshots the attributes, ReadAt reads the bytes.

func TestJFSRefreshConstantsMatchJuiceFS(t *testing.T) {
	for _, c := range []struct {
		name     string
		app, jfs int
	}{
		{"message", jfsMsgRefreshInode, meta.JMRefreshInode},
		{"dropped", jfsRefreshKernelDropped, vfs.JMRefreshKernelDropped},
		{"none", jfsRefreshKernelNone, vfs.JMRefreshKernelNone},
		{"timeout", jfsRefreshKernelTimeout, vfs.JMRefreshKernelTimeout},
		{"failed", jfsRefreshKernelFailed, vfs.JMRefreshKernelFailed},
	} {
		if c.app != c.jfs {
			t.Errorf("%s: app %d, juicefs %d", c.name, c.app, c.jfs)
		}
	}
}

func TestJFSRefreshInodeExchange(t *testing.T) {
	var sent []byte
	reply := []byte{0, jfsRefreshKernelDropped}
	reply = binary.BigEndian.AppendUint64(reply, 49<<20)
	reply = binary.BigEndian.AppendUint64(reply, 1700000000123456789)
	c := &jfsControl{testExchange: func(msg []byte, _ int) ([]byte, error) { sent = msg; return reply, nil }}
	flags, length, mtime, err := c.refreshInode(98, time.Second)
	if err != nil || flags != jfsRefreshKernelDropped || length != 49<<20 || mtime != 1700000000123456789 {
		t.Fatalf("reply: %d %d %d %v", flags, length, mtime, err)
	}
	want := controlHeader(jfsMsgRefreshInode, 9)
	want = append(want, 1)
	want = binary.BigEndian.AppendUint64(want, 98)
	if !bytes.Equal(sent, want) {
		t.Fatalf("message % x, want % x", sent, want)
	}
	// A juicefs without patch 0031 answers one EINVAL byte.
	c.testExchange = func([]byte, int) ([]byte, error) { return []byte{byte(syscall.EINVAL)}, nil }
	if _, _, _, err := c.refreshInode(98, time.Second); !errors.Is(err, errControlUnsupported) {
		t.Fatalf("old juicefs: %v", err)
	}
	c.testExchange = func([]byte, int) ([]byte, error) { return []byte{byte(syscall.ENOENT)}, nil }
	if _, _, _, err := c.refreshInode(98, time.Second); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("deleted inode: %v", err)
	}
}

// staleFUSE is the macFUSE model: write stages a teammate's bytes, and they
// reach the FUSE file only when the refresh runs.
type staleFUSE struct {
	mu      sync.Mutex
	full    string
	staged  []byte
	delay   time.Duration
	calls   atomic.Int32
	inodes  []uint64
	refresh func(inode uint64, fusePath string) error
}

func (s *staleFUSE) stage(b []byte) { s.mu.Lock(); s.staged = b; s.mu.Unlock() }

func (s *staleFUSE) run(inode uint64, fusePath string) error {
	s.calls.Add(1)
	time.Sleep(s.delay) // the control exchange and juicefs's metadata round trip
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inodes = append(s.inodes, inode)
	if fusePath != s.full {
		return errors.New("refresh of the wrong path")
	}
	if s.staged != nil {
		if err := os.WriteFile(s.full, s.staged, 0o644); err != nil {
			return err
		}
		s.staged = nil
	}
	return nil
}

type readPair struct {
	mtime time.Time
	size  int64
	data  []byte
}

// nfsRead is one READ as onRead serves it: the reply's attributes come from
// OpenFile's snapshot, the bytes from ReadAt.
func nfsRead(t *testing.T, jfs *juiceFS, name string, off int64, n int) readPair {
	t.Helper()
	f, err := jfs.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()
	cp, ok := f.(nfslib.CachedInfoProvider)
	if !ok {
		t.Fatalf("%T has no READ attributes", f)
	}
	info := cp.CachedInfo()
	buf := make([]byte, n)
	got, err := f.ReadAt(buf, off)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	return readPair{mtime: info.ModTime(), size: info.Size(), data: buf[:got]}
}

// peerEdit is the attr push's mirror update: the same row and inode, a new
// mtime (and size), through the guarded reconcile path, which runs the
// content hooks before it changes the row.
func peerEdit(t *testing.T, store *metadata.Store, name string, size int64, mtime time.Time) {
	t.Helper()
	old := store.LookupByPath(name)
	if old == nil {
		t.Fatal("no mirror row")
	}
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(name, false, size, mtime, old.Inode)
	e.Mode = old.Mode
	if _, err := store.ReconcileUpsertBatch([]*metadata.Entry{e}, epoch); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath(name); got == nil || got.Mtime.UnixNano() != mtime.UnixNano() || got.Size != size {
		t.Fatalf("mirror not updated: %+v", got)
	}
}

func peerEditHarness(t *testing.T, gate bool) (*juiceFS, *metadata.Store, *staleFUSE, string) {
	t.Helper()
	jfs, store, fuseRoot := newIntegrityHarness(t)
	const name = "pf/clip.bin"
	seedFile(t, jfs, store, fuseRoot, name, bytes.Repeat([]byte{'A'}, 64<<10))
	s := &staleFUSE{full: filepath.Join(fuseRoot, name), delay: 150 * time.Millisecond}
	if gate {
		jfs.handler.contentRefresh = newContentRefresher(s.run)
	} else {
		jfs.handler.contentRefresh = nil
	}
	// The Mac has read the file: its descriptor is pooled, so juicefs holds it
	// open (and on macOS every later open shares that handle).
	if p := nfsRead(t, jfs, name, 0, 4096); !bytes.Equal(p.data, bytes.Repeat([]byte{'A'}, 4096)) {
		t.Fatal("first read")
	}
	return jfs, store, s, name
}

// P2. The macOS client keeps old bytes forever when a READ reply pairs them
// with the new mtime. No reply may do that: a reply whose attributes are new
// must carry the refreshed bytes. Readers run before, during and after the
// edit, as playback does.
func TestPeerEditNeverPairsOldBytesWithNewAttributes(t *testing.T) {
	for _, gate := range []bool{false, true} {
		name := "with the refresh gate"
		if !gate {
			name = "without it (the 8cb665f behaviour)"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, s, file := peerEditHarness(t, gate)
			oldM := store.LookupByPath(file).Mtime
			newM := oldM.Add(1500 * time.Millisecond)
			newBytes := bytes.Repeat([]byte{'B'}, 64<<10)

			var pairs []readPair
			var mu sync.Mutex
			stop := make(chan struct{})
			var wg sync.WaitGroup
			for r := 0; r < 4; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						p := nfsRead(t, jfs, file, 16<<10, 4096)
						mu.Lock()
						pairs = append(pairs, p)
						mu.Unlock()
						time.Sleep(2 * time.Millisecond)
					}
				}()
			}
			time.Sleep(30 * time.Millisecond)
			s.stage(newBytes)
			peerEdit(t, store, file, int64(len(newBytes)), newM)
			if !gate {
				// Without the message, juicefs sees the new mtime some other
				// way, later (0.1-2.2 s on the laptop).
				go func() { _ = s.run(0, s.full) }()
			}
			time.Sleep(400 * time.Millisecond)
			close(stop)
			wg.Wait()

			var bad, newOK int
			for _, p := range pairs {
				isNewAttr := p.mtime.Equal(newM)
				isNewBytes := bytes.Equal(p.data, newBytes[:4096])
				if isNewAttr && !isNewBytes {
					bad++
				}
				if isNewAttr && isNewBytes {
					newOK++
				}
			}
			t.Logf("%d READs: %d with old bytes under the new mtime, %d new/new", len(pairs), bad, newOK)
			if gate {
				if bad != 0 {
					t.Fatalf("%d READ replies paired old bytes with the new mtime: the Mac would keep them", bad)
				}
				if newOK == 0 {
					t.Fatal("no READ served the new version")
				}
				if s.calls.Load() == 0 {
					t.Fatal("the edit sent no refresh")
				}
			} else if bad == 0 {
				t.Fatal("the model does not reproduce P2 without the gate")
			}
		})
	}
}

// The GETATTR the Mac's attribute cache depends on: a teammate's in-place
// save changes mtime and ctime (macOS drops cached pages on a new mtime), an
// append also the size. Nanoseconds matter: two saves in one second differ.
func TestPeerEditChangesNFSAttributes(t *testing.T) {
	jfs, store, _, file := peerEditHarness(t, true)
	attr := func() *nfslib.FileAttribute {
		info, err := jfs.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		return nfslib.ToFileAttribute(info, file)
	}
	a0 := attr()
	m1 := store.LookupByPath(file).Mtime.Add(250 * time.Microsecond)
	peerEdit(t, store, file, int64(a0.Filesize), m1)
	a1 := attr()
	if a1.Mtime == a0.Mtime || a1.Ctime == a0.Ctime || a1.Filesize != a0.Filesize {
		t.Fatalf("in-place save: mtime %v -> %v, ctime %v -> %v", a0.Mtime, a1.Mtime, a0.Ctime, a1.Ctime)
	}
	peerEdit(t, store, file, int64(a0.Filesize)+1<<20, m1.Add(time.Millisecond))
	a2 := attr()
	if a2.Filesize != a0.Filesize+1<<20 || a2.Mtime == a1.Mtime {
		t.Fatalf("append: size %d, mtime %v", a2.Filesize, a2.Mtime)
	}
}

// P1 and P3 in the read path: a file the Mac holds open (pooled) gets the
// refresh, and the next READ, with the new attributes, returns the
// teammate's bytes, also past the old end (an append).
func TestPeerEditHeldFileRefreshedForNextRead(t *testing.T) {
	jfs, store, s, file := peerEditHarness(t, true)
	old := store.LookupByPath(file)
	grown := append(bytes.Repeat([]byte{'B'}, 64<<10), bytes.Repeat([]byte{'C'}, 4096)...)
	s.stage(grown)
	peerEdit(t, store, file, int64(len(grown)), old.Mtime.Add(time.Second))
	p := nfsRead(t, jfs, file, 64<<10, 4096)
	if p.size != int64(len(grown)) || !bytes.Equal(p.data, grown[64<<10:]) {
		t.Fatalf("appended bytes: size %d, %d bytes %q", p.size, len(p.data), p.data[:min(len(p.data), 4)])
	}
	if got := s.inodes; len(got) != 1 || got[0] != old.Inode {
		t.Fatalf("refreshed inodes %v, want [%d]", got, old.Inode)
	}
}

// A file this process never opened through the pool needs no refresh (its
// next open reaches juicefs); a farm busy writing other files costs nothing.
func TestPeerEditRefreshOnlyForFilesThePoolOpened(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	seedFile(t, jfs, store, fuseRoot, "farm/out.exr", []byte("frame"))
	var calls atomic.Int32
	jfs.handler.contentRefresh = newContentRefresher(func(uint64, string) error { calls.Add(1); return nil })
	peerEdit(t, store, "farm/out.exr", 5, store.LookupByPath("farm/out.exr").Mtime.Add(time.Second))
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("%d refreshes of a file never opened", calls.Load())
	}
}

// The update event path (juicemount:metadata) refreshes too, and it decides
// before #94's deferred close drops the descriptor (which would hide that
// juicefs still has the handle open).
func TestPeerEditUpdateEventRefreshesHeldFile(t *testing.T) {
	jfs, store, s, file := peerEditHarness(t, true)
	store.NotifyContentInvalidated(file, false)
	if !jfs.handler.contentRefresh.wait(file, 2*time.Second) || s.calls.Load() != 1 {
		t.Fatalf("update event: %d refreshes", s.calls.Load())
	}
}

// Many events while a refresh runs: one more run, and READs wait for the last.
func TestContentRefreshCoalescesAndBoundsTheWait(t *testing.T) {
	release, started := make(chan struct{}), make(chan struct{}, 4)
	var runs atomic.Int32
	c := newContentRefresher(func(uint64, string) error { runs.Add(1); started <- struct{}{}; <-release; return nil })
	c.begin("f", 7, "/x/f")
	<-started // events before the run starts fold into it
	for i := 0; i < 10; i++ {
		c.begin("f", 7, "/x/f")
	}
	if c.wait("f", 30*time.Millisecond) {
		t.Fatal("wait returned while the refresh was blocked")
	}
	if !c.wait("other", time.Millisecond) {
		t.Fatal("an unrelated path waited")
	}
	close(release)
	if !c.wait("f", 2*time.Second) {
		t.Fatal("wait did not end")
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("%d runs, want 2 (the first and one coalesced rerun)", got)
	}
	if c.waiting.Load() != 0 {
		t.Fatal("pending count leaked")
	}
	// An old juicefs: stop sending.
	c2 := newContentRefresher(func(uint64, string) error { return errControlUnsupported })
	c2.begin("f", 7, "/x/f")
	c2.wait("f", time.Second)
	c2.begin("f", 7, "/x/f")
	if !c2.unsupported.Load() || c2.waiting.Load() != 0 {
		t.Fatal("unsupported juicefs kept refreshing")
	}
}
