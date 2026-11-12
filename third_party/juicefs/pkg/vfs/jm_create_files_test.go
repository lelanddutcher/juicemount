/*
 * JuiceMount-owned test (patch 0025). Not part of upstream JuiceFS.
 *
 * The Redis tests need JM_WATCH_BATCH_TEST_REDIS_URL (a disposable database;
 * it is flushed), as pkg/meta/redis_watch_batch_test.go.
 */

package vfs

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

type jmTestFile struct {
	parent meta.Ino
	preIno meta.Ino
	name   string
	mode   uint16
	mtime  time.Time
	data   []byte
}

func jmCreateFilesBody(files []jmTestFile) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(files)))
	for _, f := range files {
		b = binary.BigEndian.AppendUint64(b, uint64(f.parent))
		b = binary.BigEndian.AppendUint64(b, uint64(f.preIno))
		b = binary.BigEndian.AppendUint16(b, f.mode)
		b = binary.BigEndian.AppendUint16(b, 0)
		b = binary.BigEndian.AppendUint64(b, uint64(f.mtime.Unix()))
		b = binary.BigEndian.AppendUint32(b, uint32(f.mtime.Nanosecond()))
		b = append(b, byte(len(f.name)))
		b = append(b, f.name...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(f.data)))
		b = append(b, f.data...)
	}
	return b
}

type jmReply struct {
	status syscall.Errno
	errs   []syscall.Errno
	inos   []meta.Ino
}

func jmSend(t *testing.T, v *VFS, body []byte) jmReply {
	t.Helper()
	var out bytes.Buffer
	v.jmHandleCreateFiles(meta.NewContext(1, 0, []uint32{0}), utils.ReadBuffer(body), &out)
	b := out.Bytes()
	if len(b) < 4 || b[0] != jmCreateFilesReplyMarker {
		t.Fatalf("short or unmarked reply %x", b)
	}
	b = b[1:]
	n := int(binary.BigEndian.Uint16(b[1:3]))
	if len(b) != 3+9*n {
		t.Fatalf("reply length %d for %d files", len(b), n)
	}
	r := jmReply{status: syscall.Errno(b[0])}
	for i := 0; i < n; i++ {
		r.errs = append(r.errs, syscall.Errno(b[3+9*i]))
		r.inos = append(r.inos, meta.Ino(binary.BigEndian.Uint64(b[4+9*i:])))
	}
	return r
}

func jmRedisVFS(t *testing.T) *VFS {
	t.Helper()
	v, _ := jmRedisVFSBlob(t)
	return v
}

func jmRedisVFSBlob(t *testing.T) (*VFS, object.ObjectStorage) {
	t.Helper()
	u := os.Getenv("JM_WATCH_BATCH_TEST_REDIS_URL")
	if u == "" {
		t.Skip("set JM_WATCH_BATCH_TEST_REDIS_URL to a disposable Redis database of its own (it is flushed)")
	}
	if !strings.HasPrefix(u, "redis://") {
		t.Fatal("JM_WATCH_BATCH_TEST_REDIS_URL must be a redis:// URL")
	}
	m := meta.NewClient(u, meta.DefaultConf())
	if err := m.Reset(); err != nil {
		t.Fatal(err)
	}
	return createTestVFS(nil, u)
}

func jmCountObjects(t *testing.T, blob object.ObjectStorage) int {
	t.Helper()
	objs, _, _, err := blob.List("", "", "", "", 1000, false)
	if err != nil {
		t.Fatal(err)
	}
	return len(objs)
}

func jmReadAll(t *testing.T, v *VFS, parent meta.Ino, name string) ([]byte, *meta.Attr) {
	t.Helper()
	ctx := NewLogContext(meta.Background())
	e, st := v.Lookup(ctx, parent, name)
	if st != 0 {
		t.Fatalf("lookup %s: %v", name, st)
	}
	_, fh, st := v.Open(ctx, e.Inode, syscall.O_RDONLY)
	if st != 0 {
		t.Fatalf("open %s: %v", name, st)
	}
	defer v.Release(ctx, e.Inode, fh)
	buf := make([]byte, e.Attr.Length+10)
	n, st := v.Read(ctx, e.Inode, buf, 0, fh)
	if st != 0 {
		t.Fatalf("read %s: %v", name, st)
	}
	return buf[:n], e.Attr
}

// A batch creates every file complete: data, length, mode and mtime. A name
// that exists is refused alone, untouched, and its upload is removed.
func TestJMCreateFilesBatch(t *testing.T) {
	v := jmRedisVFS(t)
	ctx := NewLogContext(meta.Background())
	dir, st := v.Mkdir(ctx, 1, "w", 0755, 0)
	if st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	old, ofh, st := v.Create(ctx, dir.Inode, "taken", 0644, 0, syscall.O_RDWR)
	if st != 0 {
		t.Fatalf("create taken: %v", st)
	}
	v.Release(ctx, old.Inode, ofh)
	mtime := time.Unix(1794490456, 987654321)
	var files []jmTestFile
	for i := 0; i < 5; i++ {
		files = append(files, jmTestFile{dir.Inode, 0, "small_" + string(rune('a'+i)), 0644, mtime, bytes.Repeat([]byte{byte(i + 1)}, 1000*(i+1))})
	}
	files = append(files, jmTestFile{dir.Inode, 0, "taken", 0644, mtime, []byte("must not land")})
	files = append(files, jmTestFile{dir.Inode, 0, "empty", 0600, mtime, nil})
	files = append(files, jmTestFile{dir.Inode, 0, "", 0644, mtime, []byte("no name")})

	r := jmSend(t, v, jmCreateFilesBody(files))
	if r.status != 0 {
		t.Fatalf("status %v", r.status)
	}
	for i, f := range files {
		want := syscall.Errno(0)
		switch f.name {
		case "taken":
			want = syscall.EEXIST
		case "":
			want = syscall.EINVAL // this file alone
		}
		if r.errs[i] != want {
			t.Fatalf("%s: %v, want %v", f.name, r.errs[i], want)
		}
		if want != 0 {
			if r.inos[i] != 0 {
				t.Fatalf("%s: refused but inode %d", f.name, r.inos[i])
			}
			continue
		}
		data, attr := jmReadAll(t, v, dir.Inode, f.name)
		if !bytes.Equal(data, f.data) {
			t.Fatalf("%s: read %d bytes, want %d", f.name, len(data), len(f.data))
		}
		if attr.Mode != f.mode || attr.Mtime != mtime.Unix() || attr.Mtimensec != uint32(mtime.Nanosecond()) {
			t.Fatalf("%s: attr %+v", f.name, attr)
		}
		if e, _ := v.Lookup(ctx, dir.Inode, f.name); e.Inode != r.inos[i] {
			t.Fatalf("%s: reply inode %d, lookup %d", f.name, r.inos[i], e.Inode)
		}
	}
	if data, _ := jmReadAll(t, v, dir.Inode, "taken"); len(data) != 0 {
		t.Fatalf("existing file changed: %q", data)
	}
}

// A malformed message is refused whole, and the reply still has one entry per
// announced file, so the caller never waits for bytes that do not come.
func TestJMCreateFilesMalformed(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	good := jmCreateFilesBody([]jmTestFile{{1, 0, "a", 0644, time.Now(), []byte("x")}, {1, 0, "b", 0644, time.Now(), nil}})
	for name, body := range map[string][]byte{
		"truncated": good[:len(good)-3],
		"trailing":  append(append([]byte(nil), good...), 0),
		"too large": jmCreateFilesBody([]jmTestFile{{1, 0, "a", 0644, time.Now(), make([]byte, jmCreateFilesMaxBytes+1)}}),
		"no files":  {0, 0},
	} {
		r := jmSend(t, v, body)
		if r.status != syscall.EINVAL {
			t.Errorf("%s: status %v, want EINVAL", name, r.status)
		}
		for i, e := range r.errs {
			if e != syscall.EINVAL || r.inos[i] != 0 {
				t.Errorf("%s: file %d: %v %d", name, i, e, r.inos[i])
			}
		}
	}
}

// An engine without JMCreateFile answers ENOTSUP and writes nothing: the
// caller falls back to the ordinary create path.
func TestJMCreateFilesUnsupportedEngine(t *testing.T) {
	v, blob := createTestVFS(nil, "")
	r := jmSend(t, v, jmCreateFilesBody([]jmTestFile{{1, 0, "a", 0644, time.Now(), []byte("data")}}))
	if r.status != syscall.ENOTSUP || r.errs[0] != syscall.ENOTSUP {
		t.Fatalf("reply %+v, want ENOTSUP", r)
	}
	if _, st := v.Lookup(NewLogContext(meta.Background()), 1, "a"); st != syscall.ENOENT {
		t.Fatalf("lookup: %v", st)
	}
	objs, _, _, err := blob.List("", "", "", "", 100, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if strings.Contains(o.Key(), "chunks/") {
			t.Fatalf("object uploaded for an unsupported engine: %s", o.Key())
		}
	}
}

// A FLUSH of the control file (a forked child's exec closing the inherited
// descriptor) cancels the handle's context. The batch must still finish.
func TestJMCreateFilesIgnoresHandleCancel(t *testing.T) {
	v := jmRedisVFS(t)
	hctx := meta.NewContext(1, 0, []uint32{0})
	hctx.Cancel()
	var out bytes.Buffer
	v.jmHandleCreateFiles(hctx, utils.ReadBuffer(jmCreateFilesBody([]jmTestFile{{1, 0, "after-flush", 0644, time.Now(), []byte("kept")}})), &out)
	b := out.Bytes()
	if len(b) != 4+9 || b[1] != 0 || b[4] != 0 {
		t.Fatalf("reply %x, want success", b)
	}
	if data, _ := jmReadAll(t, v, 1, "after-flush"); string(data) != "kept" {
		t.Fatalf("read %q", data)
	}
}

// A reserved number in the message: the file gets it, and a resend after a
// lost reply is recognised as the same file.
func TestJMCreateFilesReservedInode(t *testing.T) {
	v := jmRedisVFS(t)
	jm := v.Meta.(interface {
		JMReserveInodes(n uint32) (meta.Ino, uint32, syscall.Errno)
	})
	first, n, st := jm.JMReserveInodes(4)
	if st != 0 || n == 0 {
		t.Fatalf("reserve: %v %d", st, n)
	}
	body := jmCreateFilesBody([]jmTestFile{{1, first, "reserved.xmp", 0644, time.Now(), []byte("mine")}})
	for i := 0; i < 2; i++ {
		r := jmSend(t, v, body)
		if r.status != 0 || r.errs[0] != 0 || r.inos[0] != first {
			t.Fatalf("send %d: %+v, want inode %d", i, r, first)
		}
	}
	if data, attr := jmReadAll(t, v, 1, "reserved.xmp"); string(data) != "mine" || attr.Length != 4 {
		t.Fatalf("read %q", data)
	}
}

// Resends that write nothing leave no object behind: a recognised resend
// (its object is not the one the file holds), an unknown number (ESRCH) and
// a number another create holds (EAGAIN).
func TestJMCreateFilesResendLeavesNoObject(t *testing.T) {
	v, blob := jmRedisVFSBlob(t)
	jm := v.Meta.(interface {
		JMReserveInodes(n uint32) (meta.Ino, uint32, syscall.Errno)
	})
	first, n, st := jm.JMReserveInodes(4)
	if st != 0 || n < 2 {
		t.Fatalf("reserve: %v %d", st, n)
	}
	body := jmCreateFilesBody([]jmTestFile{{1, first, "mine.xmp", 0644, time.Now(), []byte("mine")}})
	if r := jmSend(t, v, body); r.status != 0 || r.errs[0] != 0 || r.inos[0] != first {
		t.Fatalf("first send: %+v", r)
	}
	base := jmCountObjects(t, blob)
	if r := jmSend(t, v, body); r.status != 0 || r.errs[0] != 0 || r.inos[0] != first {
		t.Fatalf("resend: %+v", r)
	}
	if r := jmSend(t, v, jmCreateFilesBody([]jmTestFile{{1, first + meta.Ino(n) + 100, "void.xmp", 0644, time.Now(), []byte("void")}})); r.errs[0] != syscall.ESRCH {
		t.Fatalf("unknown number: %+v, want ESRCH", r)
	}
	if got := jmCountObjects(t, blob); got != base {
		t.Fatalf("%d objects after resends, want %d", got, base)
	}
	if data, _ := jmReadAll(t, v, 1, "mine.xmp"); string(data) != "mine" {
		t.Fatalf("the file lost its data: %q", data)
	}
}

// A folder that refused with ENOTSUP is remembered: the next file for it is
// refused before its upload.
func TestJMCreateFilesRefusedParentSkipsUpload(t *testing.T) {
	v, blob := createTestVFS(nil, "")
	jmNoteParentRefused(77)
	t.Cleanup(func() { jmRefusedMu.Lock(); delete(jmRefused, 77); jmRefusedMu.Unlock() })
	f := &jmNewFile{parent: 77, name: "x", data: []byte("bytes")}
	v.jmUpload(meta.Background(), f)
	if f.eno != syscall.ENOTSUP || f.slice.Id != 0 {
		t.Fatalf("upload for a refused folder: %v slice %d", f.eno, f.slice.Id)
	}
	if objs, _, _, _ := blob.List("", "", "", "", 10, false); len(objs) != 0 {
		t.Fatalf("%d objects uploaded", len(objs))
	}
}
