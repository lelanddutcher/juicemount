package meta

import (
	"syscall"
	"testing"
	"time"
)

// Patch 0028: an open reads the attr and the first chunk's slices in one
// round trip, the first read of chunk 0 needs none, and a GetAttr right after
// the open reuses the open's attr.

func jmOpenFile(t *testing.T, m *redisMeta, name string, slices ...Slice) Ino {
	t.Helper()
	ctx := Background()
	var ino Ino
	attr := &Attr{}
	if st := m.Mknod(ctx, RootInode, name, TypeFile, 0644, 022, 0, "", &ino, attr); st != 0 {
		t.Fatalf("mknod %s: %v", name, st)
	}
	var pos uint32
	for _, s := range slices {
		if st := m.Write(ctx, ino, 0, pos, s, time.Now()); st != 0 {
			t.Fatalf("write %s: %v", name, st)
		}
		pos += s.Len
	}
	return ino
}

func TestJMOpenRoundTrips(t *testing.T) {
	m, wc := countedMeta(t)
	ctx := Background()
	ino := jmOpenFile(t, m, "clip", Slice{Id: 11, Size: 1 << 20, Len: 1 << 20})
	// A fresh client state: nothing of this inode is cached.
	m.of.InvalidateChunk(ino, invalidateAllChunks)
	delete(m.of.files, ino)

	wc.n.Store(0)
	attr := &Attr{}
	if st := m.Open(ctx, ino, uint32(syscall.O_RDONLY), attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	if got := wc.n.Load(); got != 1 {
		t.Errorf("open took %d round trips, want 1", got)
	}
	if attr.Length != 1<<20 || attr.Typ != TypeFile {
		t.Fatalf("open attr: length %d type %d", attr.Length, attr.Typ)
	}
	wc.n.Store(0)
	var got []Slice
	if st := m.Read(ctx, ino, 0, &got); st != 0 {
		t.Fatalf("read: %v", st)
	}
	if n := wc.n.Load(); n != 0 {
		t.Errorf("first read of chunk 0 took %d round trips, want 0", n)
	}
	if len(got) != 1 || got[0].Id != 11 || got[0].Len != 1<<20 {
		t.Fatalf("read slices %+v", got)
	}
	wc.n.Store(0)
	var again Attr
	if st := m.GetAttr(ctx, ino, &again); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if n := wc.n.Load(); n != 0 {
		t.Errorf("getattr right after open took %d round trips, want 0", n)
	}
	if again.Length != attr.Length || again.Mtime != attr.Mtime {
		t.Fatalf("getattr after open: %+v, open saw %+v", again, *attr)
	}
	// Past the window it asks the engine again.
	m.of.Lock()
	m.of.files[ino].attrAt = time.Now().Add(-2 * openAttrReuse)
	m.of.Unlock()
	wc.n.Store(0)
	if st := m.GetAttr(ctx, ino, &again); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if n := wc.n.Load(); n != 1 {
		t.Errorf("getattr past the window took %d round trips, want 1", n)
	}
	m.Close(ctx, ino)
}

// A peer client changes the file between two opens: the second open sees
// the peer's attr and slices, never the first open's.
func TestJMOpenSeesPeerEdit(t *testing.T) {
	m, _ := countedMeta(t)
	ctx := Background()
	ino := jmOpenFile(t, m, "shared", Slice{Id: 21, Size: 4096, Len: 4096})
	attr := &Attr{}
	if st := m.Open(ctx, ino, uint32(syscall.O_RDONLY), attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	var got []Slice
	if st := m.Read(ctx, ino, 0, &got); st != 0 || len(got) != 1 || got[0].Id != 21 {
		t.Fatalf("first read: %v %+v", st, got)
	}
	m.Close(ctx, ino)

	peerStore, err := newRedisMeta("redis", m.addr, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	peer := peerStore.(*redisMeta)
	t.Cleanup(func() { _ = peer.Shutdown() })
	if _, err := peer.Load(true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // a distinct mtime
	if st := peer.Write(ctx, ino, 0, 4096, Slice{Id: 22, Size: 8192, Len: 8192}, time.Now()); st != 0 {
		t.Fatalf("peer write: %v", st)
	}

	attr = &Attr{}
	if st := m.Open(ctx, ino, uint32(syscall.O_RDONLY), attr); st != 0 {
		t.Fatalf("reopen: %v", st)
	}
	if attr.Length != 4096+8192 {
		t.Errorf("reopen length %d, want %d", attr.Length, 4096+8192)
	}
	got = nil
	if st := m.Read(ctx, ino, 0, &got); st != 0 {
		t.Fatalf("read after peer edit: %v", st)
	}
	var ids []uint64
	for _, s := range got {
		if s.Id != 0 {
			ids = append(ids, s.Id)
		}
	}
	if len(ids) != 2 || ids[0] != 21 || ids[1] != 22 {
		t.Errorf("read after peer edit saw slices %+v, want 21 then 22", got)
	}
	var ga Attr
	if st := m.GetAttr(ctx, ino, &ga); st != 0 || ga.Length != attr.Length {
		t.Errorf("getattr after reopen: %v length %d", st, ga.Length)
	}
	m.Close(ctx, ino)
}

// A local write after the open invalidates the reused attr and the cached
// slices: the next GetAttr and Read go to the engine.
func TestJMOpenLocalWriteInvalidates(t *testing.T) {
	m, wc := countedMeta(t)
	ctx := Background()
	ino := jmOpenFile(t, m, "local", Slice{Id: 31, Size: 4096, Len: 4096})
	attr := &Attr{}
	if st := m.Open(ctx, ino, uint32(syscall.O_RDWR), attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	if st := m.Write(ctx, ino, 0, 4096, Slice{Id: 32, Size: 4096, Len: 4096}, time.Now()); st != 0 {
		t.Fatalf("write: %v", st)
	}
	wc.n.Store(0)
	var ga Attr
	if st := m.GetAttr(ctx, ino, &ga); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if n := wc.n.Load(); n != 1 || ga.Length != 8192 {
		t.Errorf("getattr after a local write: %d round trips, length %d; want 1 and 8192", n, ga.Length)
	}
	var got []Slice
	if st := m.Read(ctx, ino, 0, &got); st != 0 {
		t.Fatalf("read: %v", st)
	}
	var ids []uint64
	for _, s := range got {
		if s.Id != 0 {
			ids = append(ids, s.Id)
		}
	}
	if len(ids) != 2 || ids[1] != 32 {
		t.Errorf("read after a local write saw %+v", got)
	}
	m.Close(ctx, ino)
}

// openfiles alone (no Redis): the prefetched slices are refused after any
// invalidation since the read, and when the file is open with another mtime.
func TestJMOpenPrefetchedRefusesChangedState(t *testing.T) {
	o := newOpenFiles(0, 0)
	first := []Slice{{Id: 1, Size: 10, Len: 10}}
	a := &Attr{Typ: TypeFile, Mtime: 100, Length: 10, Full: true}

	gen := o.chunkGen()
	if !o.OpenPrefetched(5, a, first, gen) {
		t.Fatal("clean open did not cache the prefetched slices")
	}
	if ss, ok := o.ReadChunk(5, 0); !ok || len(ss) != 1 {
		t.Fatalf("cached chunk 0: %v %+v", ok, ss)
	}

	// An invalidation between the read and the open.
	gen = o.chunkGen()
	o.InvalidateChunk(6, 0)
	if o.OpenPrefetched(6, a, first, gen) {
		t.Error("cached slices read before an invalidation")
	}

	// Another fd holds the file open with a different mtime.
	gen = o.chunkGen()
	o.Open(7, &Attr{Typ: TypeFile, Mtime: 200, Full: true})
	gen = o.chunkGen()
	if o.OpenPrefetched(7, a, first, gen) {
		t.Error("cached slices of another mtime")
	}
	if _, ok := o.ReadChunk(7, 0); ok {
		t.Error("chunk 0 cached after an mtime change")
	}

	// RecentOpenAttr: only for a file still open, within the window.
	var got Attr
	if !o.RecentOpenAttr(5, &got, time.Second) || got.Mtime != 100 {
		t.Errorf("recent attr of an open file: %+v", got)
	}
	o.Close(5)
	if o.RecentOpenAttr(5, &got, time.Second) {
		t.Error("recent attr served for a closed file")
	}
	gen = o.chunkGen()
	o.OpenPrefetched(5, a, nil, gen)
	o.InvalidateChunk(5, invalidateAttrOnly)
	if o.RecentOpenAttr(5, &got, time.Second) {
		t.Error("recent attr served after an attr invalidation")
	}
}
