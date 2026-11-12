package respproxy

// JuiceFS patch 0025 (JMCreateFile) through the real proxy in enforce mode:
// one transaction that creates a file and links its first slice is admitted
// in the writer's own project, and refused with nothing written in a project
// another Mac holds or with a slice id the writer was not given. The rig is
// cleanupScopeRig's: mac1 holds project A (3), mac2 holds project B (4).

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

type jmFileCreator interface {
	JMCreateFile(ctx meta.Context, parent meta.Ino, name string, mode, cumask uint16, mtime time.Time, slice meta.Slice, inode *meta.Ino, attr *meta.Attr) syscall.Errno
	JMCreateFilesIn(ctx meta.Context, parent meta.Ino, files []*meta.JMNewFile) syscall.Errno
}

func TestJMCreateFileThroughProxy(t *testing.T) {
	r, db := cleanupScopeRig(t, 0)
	ctx := context.Background()
	ma := scopeClient(t, r, "mac1", nil)
	mb := scopeClient(t, r, "mac2", nil)
	ca, ok := ma.(jmFileCreator)
	if !ok {
		t.Fatal("the vendored meta client has no JMCreateFile")
	}
	newSlice := func(m meta.Meta, n uint32) meta.Slice {
		var id uint64
		if st := m.NewSlice(meta.Background(), &id); st != 0 {
			t.Fatalf("new slice: %v", st)
		}
		return meta.Slice{Id: id, Size: n, Len: n}
	}
	mtime := time.Unix(1794490456, 5)
	used0 := db.Get(ctx, "usedSpace").Val()

	// Own project: admitted, complete.
	var ino meta.Ino
	var attr meta.Attr
	s := newSlice(ma, 20480)
	if st := ca.JMCreateFile(meta.Background(), 3, "notes.xmp", 0644, 0, mtime, s, &ino, &attr); st != 0 {
		t.Fatalf("create in own project: %v", st)
	}
	noRefusals(t, r, "mac1", "JMCreateFile in its own project")
	if db.HGet(ctx, "d3", "notes.xmp").Val() != mkEntry(typeFile, uint64(ino)) {
		t.Fatalf("entry missing: %v", db.HKeys(ctx, "d3").Val())
	}
	if n := db.LLen(ctx, "c"+inoStr(uint64(ino))+"_0").Val(); n != 1 {
		t.Fatalf("chunk has %d slices, want 1", n)
	}
	if st := ma.GetAttr(meta.Background(), ino, &attr); st != 0 || attr.Length != 20480 || attr.Mtime != mtime.Unix() {
		t.Fatalf("attr: %v %+v", st, attr)
	}
	// An empty file has no slice to link.
	if st := ca.JMCreateFile(meta.Background(), 3, "empty", 0644, 0, mtime, meta.Slice{}, &ino, &attr); st != 0 {
		t.Fatalf("empty file: %v", st)
	}
	noRefusals(t, r, "mac1", "empty JMCreateFile")
	// Several files of one folder in one transaction: several new inodes
	// from the writer's own batch, each in this one folder.
	batch := []*meta.JMNewFile{
		{Name: "b1.xmp", Mode: 0644, Mtime: mtime, Slice: newSlice(ma, 300)},
		{Name: "b2.xmp", Mode: 0644, Mtime: mtime, Slice: newSlice(ma, 400)},
		{Name: "b3.xmp", Mode: 0644, Mtime: mtime},
	}
	if st := ca.JMCreateFilesIn(meta.Background(), 3, batch); st != 0 {
		t.Fatalf("batch in own project: %v", st)
	}
	noRefusals(t, r, "mac1", "JMCreateFilesIn in its own project")
	for _, f := range batch {
		if f.Errno != 0 || db.HGet(ctx, "d3", f.Name).Val() != mkEntry(typeFile, uint64(f.Inode)) {
			t.Fatalf("%s: %v, entry %q", f.Name, f.Errno, db.HGet(ctx, "d3", f.Name).Val())
		}
	}
	used1 := db.Get(ctx, "usedSpace").Val()
	if used1 == used0 {
		t.Fatal("usedSpace did not move")
	}

	// Another Mac's project: refused (JMDENY, EROFS to the caller), nothing
	// written.
	ino = 0
	if st := ca.JMCreateFile(meta.Background(), 4, "intrude.xmp", 0644, 0, mtime, newSlice(ma, 100), &ino, &attr); st != syscall.EROFS {
		t.Fatalf("create in B: %v, want EROFS", st)
	}
	if db.HExists(ctx, "d4", "intrude.xmp").Val() || ino != 0 {
		t.Fatal("a refused create landed in B")
	}
	// A slice id the proxy gave another principal cannot be linked.
	if st := ca.JMCreateFile(meta.Background(), 3, "stolen.xmp", 0644, 0, mtime, newSlice(mb, 100), &ino, &attr); st == 0 {
		t.Fatal("linked a slice allocated to mac2")
	}
	if db.HExists(ctx, "d3", "stolen.xmp").Val() {
		t.Fatal("a refused create landed in A")
	}
	if db.Get(ctx, "usedSpace").Val() != used1 {
		t.Fatal("refused creates changed usedSpace")
	}
	projectBIntact(t, db)
}
