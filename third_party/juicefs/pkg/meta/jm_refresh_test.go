package meta

import (
	"syscall"
	"testing"
	"time"
)

// Patch 0031: JMRefreshOpenFile makes an open file drop the slice list it
// cached before a peer's write, even when no open or GETATTR shows this
// client the new mtime (macOS: one FUSE handle for every descriptor).
func TestJMRefreshOpenFileDropsPeerStaleSlices(t *testing.T) {
	m, _ := countedMeta(t)
	ctx := Background()
	ino := jmOpenFile(t, m, "held", Slice{Id: 31, Size: 4096, Len: 4096})
	attr := &Attr{}
	if st := m.Open(ctx, ino, uint32(syscall.O_RDONLY), attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	defer m.Close(ctx, ino)
	var got []Slice
	if st := m.Read(ctx, ino, 0, &got); st != 0 || len(got) != 1 {
		t.Fatalf("first read: %v %+v", st, got)
	}

	peer := jmPeer(t, m)
	time.Sleep(10 * time.Millisecond) // a distinct mtime
	if st := peer.Write(ctx, ino, 0, 4096, Slice{Id: 32, Size: 8192, Len: 8192}, time.Now()); st != 0 {
		t.Fatalf("peer write: %v", st)
	}
	time.Sleep(openAttrReuse + 100*time.Millisecond) // past the open's attr window
	got = nil
	if st := m.Read(ctx, ino, 0, &got); st != 0 || len(got) != 1 {
		t.Fatalf("the model is off: the held file already sees the peer's slices: %v %+v", st, got)
	}

	var fresh Attr
	if st := m.JMRefreshOpenFile(ctx, ino, &fresh); st != 0 {
		t.Fatalf("refresh: %v", st)
	}
	if fresh.Length != 4096+8192 {
		t.Fatalf("refresh length %d, want %d", fresh.Length, 4096+8192)
	}
	got = nil
	if st := m.Read(ctx, ino, 0, &got); st != 0 {
		t.Fatalf("read after refresh: %v", st)
	}
	if ids := jmSliceIDs(got); len(ids) != 2 || ids[0] != 31 || ids[1] != 32 {
		t.Fatalf("read after refresh saw slices %+v, want 31 then 32", got)
	}
	// The kernel asks for attributes right after the refresh dropped its
	// cache: answered from the refresh's own read, no round trip.
	var ga Attr
	if !m.of.RecentOpenAttr(ino, &ga, openAttrReuse) || ga.Length != fresh.Length {
		t.Fatalf("the refreshed attributes are not reusable: %+v", ga)
	}
	if st := m.JMRefreshOpenFile(ctx, 987654, &fresh); st != syscall.ENOENT {
		t.Fatalf("unknown inode: %v, want ENOENT", st)
	}
}
