package vfs

// JuiceMount patch 0030: a read error on an open file is retried after
// jmReadErrRetryAfter instead of being kept until the file is closed.

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
)

// flakyStore fails every Get while down is set (the network is gone).
type flakyStore struct {
	object.ObjectStorage
	down atomic.Bool
	gets atomic.Int64
}

func (s *flakyStore) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	s.gets.Add(1)
	if s.down.Load() {
		return nil, errors.New("dial tcp: connect: network is unreachable")
	}
	return s.ObjectStorage.Get(key, off, limit, getters...)
}

// recoverVFS writes data to /f through one VFS, then returns a second VFS on
// the same metadata whose store reads through flaky with an empty cache, and
// the file's inode.
func recoverVFS(t *testing.T, data []byte) (*VFS, *flakyStore, Ino) {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	metaConf.Retries = 2 // the read gives up after about 0.3 s
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{Name: "recover", UUID: uuid.New().String(), Storage: "mem", BlockSize: 1024, Compression: "none"}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	flaky := &flakyStore{ObjectStorage: blob}
	newVFS := func() *VFS {
		dir := t.TempDir()
		conf := &Config{
			Meta: metaConf, Format: *format, Version: "Juicefs", FuseOpts: &FuseOptions{},
			Chunk: &chunk.Config{BlockSize: 1 << 20, Compress: "none", MaxUpload: 2, BufferSize: 64 << 20,
				CacheSize: 64 << 20, CacheDir: dir, FreeSpace: 0.0001, CacheMode: 0600, AutoCreate: true,
				PutTimeout: time.Minute, GetTimeout: time.Minute, MaxRetries: 1},
		}
		registry := prometheus.NewRegistry()
		store := chunk.NewCachedStore(flaky, *conf.Chunk, registry)
		return NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
	}
	w := newVFS()
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	fe, fh, st := w.Create(ctx, 1, "f", 0644, 0, uint32(os.O_RDWR))
	if st != 0 {
		t.Fatalf("create: %s", st)
	}
	if st := w.Write(ctx, fe.Inode, data, 0, fh); st != 0 {
		t.Fatalf("write: %s", st)
	}
	if st := w.Flush(ctx, fe.Inode, fh, 0); st != 0 {
		t.Fatalf("flush: %s", st)
	}
	w.Release(ctx, fe.Inode, fh)
	return newVFS(), flaky, fe.Inode
}

func recoverRead(v *VFS, ino Ino, fh uint64, n int) ([]byte, syscall.Errno) {
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	buf := make([]byte, n)
	got, st := v.Read(ctx, ino, buf, 0, fh)
	return buf[:got], st
}

// The network goes away while the file is open: the read fails with EIO. A
// read right after still gets the stored EIO without asking the store. Once
// the network is back and jmReadErrRetryAfter has passed, the same handle
// reads the data.
func TestReadErrorRetriedAfterNetworkReturns(t *testing.T) {
	old := jmReadErrRetryAfter
	jmReadErrRetryAfter = 300 * time.Millisecond
	t.Cleanup(func() { jmReadErrRetryAfter = old })
	data := bytes.Repeat([]byte("juicemount"), 1<<16)
	v, flaky, ino := recoverVFS(t, data)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	_, fh, st := v.Open(ctx, ino, uint32(os.O_RDONLY))
	if st != 0 {
		t.Fatalf("open: %s", st)
	}
	defer v.Release(ctx, ino, fh)

	flaky.down.Store(true)
	if _, st := recoverRead(v, ino, fh, 4096); st != syscall.EIO {
		t.Fatalf("read while down: %s, want EIO", st)
	}
	gets := flaky.gets.Load()
	if _, st := recoverRead(v, ino, fh, 4096); st != syscall.EIO {
		t.Fatalf("read right after the error: %s, want the stored EIO", st)
	}
	if n := flaky.gets.Load() - gets; n != 0 {
		t.Fatalf("a read inside jmReadErrRetryAfter asked the store %d times", n)
	}

	flaky.down.Store(false)
	time.Sleep(jmReadErrRetryAfter + 50*time.Millisecond)
	got, st := recoverRead(v, ino, fh, 4096)
	if st != 0 || !bytes.Equal(got, data[:4096]) {
		t.Fatalf("read after the network returned: %d bytes, %s; want the data", len(got), st)
	}
}

// A block that is really gone still fails: the retry asks the store again and
// returns EIO again, and keeps doing so.
func TestReadErrorOfMissingBlockStaysEIO(t *testing.T) {
	old := jmReadErrRetryAfter
	jmReadErrRetryAfter = 200 * time.Millisecond
	t.Cleanup(func() { jmReadErrRetryAfter = old })
	data := bytes.Repeat([]byte("juicemount"), 1<<16)
	v, flaky, ino := recoverVFS(t, data)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	_, fh, st := v.Open(ctx, ino, uint32(os.O_RDONLY))
	if st != 0 {
		t.Fatalf("open: %s", st)
	}
	defer v.Release(ctx, ino, fh)
	keys, err := object.ListAll(flaky.ObjectStorage, "", "", true)
	if err != nil {
		t.Fatal(err)
	}
	removed := 0
	for o := range keys {
		if o != nil && !o.IsDir() && bytes.Contains([]byte(o.Key()), []byte("chunks/")) {
			if err := flaky.ObjectStorage.Delete(o.Key()); err != nil {
				t.Fatal(err)
			}
			removed++
		}
	}
	if removed == 0 {
		t.Fatal("no block object to remove")
	}
	for i := 0; i < 3; i++ {
		gets := flaky.gets.Load()
		if _, st := recoverRead(v, ino, fh, 4096); st != syscall.EIO {
			t.Fatalf("read %d of a missing block: %s, want EIO", i, st)
		}
		if i > 0 && flaky.gets.Load() == gets {
			t.Fatalf("read %d after jmReadErrRetryAfter did not ask the store again", i)
		}
		time.Sleep(jmReadErrRetryAfter + 50*time.Millisecond)
	}
}

// ENOENT (the file was deleted) is kept as upstream keeps it.
func TestReadErrorENOENTStays(t *testing.T) {
	old := jmReadErrRetryAfter
	jmReadErrRetryAfter = 0
	t.Cleanup(func() { jmReadErrRetryAfter = old })
	f := &fileReader{err: syscall.ENOENT, errAt: time.Now().Add(-time.Hour), tried: 5}
	f.jmRetryAfterError()
	if f.err != syscall.ENOENT || f.tried != 5 {
		t.Fatalf("ENOENT cleared: err %s, tried %d", f.err, f.tried)
	}
	f = &fileReader{err: syscall.EIO, errAt: time.Now().Add(-time.Hour), tried: 5, closing: true}
	f.jmRetryAfterError()
	if f.err != syscall.EIO {
		t.Fatal("a closing reader's error was cleared")
	}
}
