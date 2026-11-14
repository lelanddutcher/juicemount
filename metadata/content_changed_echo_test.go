package metadata

import (
	"testing"
	"time"
)

// The peer-edit refresh (nfs content_refresh.go) must not run for this
// client's own drains: juicemount:metadata echoes every write back with no
// origin field. The content-changed hook uses the attr push's echo test (a
// pending local write, or a row that already holds the event's attributes).
func TestContentChangedSkipsOwnEchoes(t *testing.T) {
	mtime := time.Unix(1700000000, 0)
	for _, tt := range []struct {
		name    string
		evt     MetadataEvent
		pending bool
		want    int
	}{
		{"drain echo", MetadataEvent{Op: "update", Path: "m/a.mov", Size: 100, Mtime: mtime.Unix(), Inode: 42}, false, 0},
		{"pending local write", MetadataEvent{Op: "update", Path: "m/a.mov", Size: 200, Mtime: mtime.Unix() + 5, Inode: 42}, true, 0},
		{"teammate save", MetadataEvent{Op: "update", Path: "m/a.mov", Size: 100, Mtime: mtime.Unix() + 5, Inode: 42}, false, 1},
		{"teammate append", MetadataEvent{Op: "update", Path: "m/a.mov", Size: 300, Mtime: mtime.Unix(), Inode: 42}, false, 1},
		{"teammate truncate", MetadataEvent{Op: "update", Path: "m/a.mov", Size: 10, Mtime: mtime.Unix() + 5, Inode: 42}, false, 1},
		{"new file", MetadataEvent{Op: "create", Path: "m/b.mov", Size: 1, Mtime: mtime.Unix(), Inode: 43}, false, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			rc := &RedisClient{store: s}
			if tt.pending {
				rc.SetSpoolGuard(func(p string) bool { return p == "m/a.mov" })
			}
			s.InsertToCache(MakeEntry("m/a.mov", false, 100, mtime, 42))
			var got []uint64
			var invalidatedFirst bool
			invalidated := false
			s.SetOnContentInvalidated(func(string, bool) { invalidated = true })
			s.SetOnContentChanged(func(p string, oldInode uint64) {
				got = append(got, oldInode)
				invalidatedFirst = invalidated
				if s.LookupByPath(p) == nil {
					t.Error("row gone before the content-changed hook")
				}
			})
			rc.applyEvent(tt.evt)
			if len(got) != tt.want || (tt.want == 1 && got[0] != 42) {
				t.Fatalf("content-changed calls %v, want %d for inode 42", got, tt.want)
			}
			if invalidatedFirst {
				t.Fatal("content-changed ran after the content hook dropped the descriptor")
			}
		})
	}
}
