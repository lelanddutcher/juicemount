package respproxy

// JuiceFS patch 0025 (JMCreateFile) through the real proxy in enforce mode:
// one transaction that creates a file and links its first slice is admitted
// in the writer's own project, and refused with nothing written in a project
// another Mac holds or with a slice id the writer was not given. The rig is
// cleanupScopeRig's: mac1 holds project A (3), mac2 holds project B (4).

import (
	"context"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
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

// L-5: the drainer's real path. The JMReserveInodes and JMCreateFiles
// messages go through a VFS's .control file, as the drainer writes them,
// with the meta client behind the enforcing proxy: the reserved number is
// admitted and used, the file without one gets a number from the writer's
// own batch, and a file in the project another Mac holds is refused alone
// (EROFS) with nothing written there.
func TestJMCreateFilesMessageThroughProxy(t *testing.T) {
	r, db := cleanupScopeRig(t, 0)
	ctx := context.Background()
	ma := scopeClient(t, r, "mac1", nil)
	format := ma.GetFormat()
	chunkConf := chunk.Config{BlockSize: 4 << 20, Compress: "none", MaxUpload: 2, BufferSize: 30 << 20,
		CacheDir: "memory", CacheSize: 10 << 20, MaxRetries: 1}
	blob, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, chunkConf, registry)
	conf := &vfs.Config{Meta: meta.DefaultConf(), Format: format, Version: "l5", Chunk: &chunkConf, FuseOpts: &vfs.FuseOptions{}}
	v := vfs.NewVFS(conf, ma, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)

	vctx := vfs.NewLogContext(meta.NewContext(1, 0, []uint32{0}))
	ce, st := v.Lookup(vctx, meta.RootInode, ".control")
	if st != 0 {
		t.Fatalf("lookup .control: %v", st)
	}
	_, fh, st := v.Open(vctx, ce.Inode, uint32(syscall.O_RDWR))
	if st != 0 {
		t.Fatalf("open .control: %v", st)
	}
	defer v.Release(vctx, ce.Inode, fh)
	var off uint64
	exchange := func(cmd uint32, body []byte, replyLen int) []byte {
		t.Helper()
		msg := binary.BigEndian.AppendUint32(nil, cmd)
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
		msg = append(msg, body...)
		if st := v.Write(vctx, ce.Inode, msg, off, fh); st != 0 {
			t.Fatalf("write message %d: %v", cmd, st)
		}
		off += uint64(len(msg))
		buf := make([]byte, replyLen)
		got := 0
		deadline := time.Now().Add(30 * time.Second)
		for got < replyLen {
			n, st := v.Read(vctx, ce.Inode, buf[got:], off+uint64(got), fh)
			if st != 0 {
				t.Fatalf("read reply %d: %v", cmd, st)
			}
			got += n
			if got > 0 && cmd == meta.JMCreateFiles && buf[0] != 0xA5 {
				t.Fatalf("unmarked create-files reply %x", buf[:got])
			}
			if n == 0 {
				if time.Now().After(deadline) {
					t.Fatalf("no reply to message %d", cmd)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		off += uint64(replyLen)
		return buf
	}

	rep := exchange(meta.JMReserveInodes, binary.BigEndian.AppendUint32(nil, 1024), 13) // the proxy admits only whole 1024 batches
	if rep[0] != 0 {
		t.Fatalf("reserve inodes: errno %d", rep[0])
	}
	first, count := binary.BigEndian.Uint64(rep[1:9]), binary.BigEndian.Uint32(rep[9:13])
	if count == 0 {
		t.Fatal("reserved no numbers")
	}
	noRefusals(t, r, "mac1", "JMReserveInodes")

	type nf struct {
		parent, preIno uint64
		name           string
		data           []byte
	}
	mtime := time.Unix(1794490456, 7)
	files := []nf{
		{3, first, "reserved.xmp", []byte("bytes with a reserved number")},
		{3, 0, "picked.xmp", []byte("bytes without one")},
		{4, 0, "intrude.xmp", []byte("another Mac's project")},
	}
	body := binary.BigEndian.AppendUint16(nil, uint16(len(files)))
	for _, f := range files {
		body = binary.BigEndian.AppendUint64(body, f.parent)
		body = binary.BigEndian.AppendUint64(body, f.preIno)
		body = binary.BigEndian.AppendUint16(body, 0o644)
		body = binary.BigEndian.AppendUint16(body, 0)
		body = binary.BigEndian.AppendUint64(body, uint64(mtime.Unix()))
		body = binary.BigEndian.AppendUint32(body, uint32(mtime.Nanosecond()))
		body = append(body, byte(len(f.name)))
		body = append(body, f.name...)
		body = binary.BigEndian.AppendUint32(body, uint32(len(f.data)))
		body = append(body, f.data...)
	}
	rep = exchange(meta.JMCreateFiles, body, 4+9*len(files))
	if rep[1] != 0 || int(binary.BigEndian.Uint16(rep[2:4])) != len(files) {
		t.Fatalf("create-files reply header %x", rep[:4])
	}
	errOf := func(i int) syscall.Errno { return syscall.Errno(rep[4+9*i]) }
	inoOf := func(i int) uint64 { return binary.BigEndian.Uint64(rep[5+9*i:]) }
	for i := 0; i < 2; i++ {
		if errOf(i) != 0 || db.HGet(ctx, "d3", files[i].name).Val() != mkEntry(typeFile, inoOf(i)) {
			t.Fatalf("%s: errno %v, entry %q", files[i].name, errOf(i), db.HGet(ctx, "d3", files[i].name).Val())
		}
		var attr meta.Attr
		if st := ma.GetAttr(meta.Background(), meta.Ino(inoOf(i)), &attr); st != 0 || attr.Length != uint64(len(files[i].data)) {
			t.Fatalf("%s: attr %v %+v", files[i].name, st, attr)
		}
	}
	if inoOf(0) != first {
		t.Fatalf("reserved.xmp got inode %d, want the reserved %d", inoOf(0), first)
	}
	if errOf(2) != syscall.EROFS || db.HExists(ctx, "d4", "intrude.xmp").Val() {
		t.Fatalf("intrude.xmp: errno %v, landed %v; want EROFS and nothing written",
			errOf(2), db.HExists(ctx, "d4", "intrude.xmp").Val())
	}
	// Sending the reserved number again recognises the file: no second
	// file, same inode.
	again := binary.BigEndian.AppendUint16(nil, 1)
	again = append(again, body[2:2+8+8+2+2+8+4+1+len(files[0].name)+4+len(files[0].data)]...)
	rep = exchange(meta.JMCreateFiles, again, 4+9)
	if rep[1] != 0 || errOf(0) != 0 || inoOf(0) != first {
		t.Fatalf("resend of the reserved number: %x", rep)
	}
	projectBIntact(t, db)
}
