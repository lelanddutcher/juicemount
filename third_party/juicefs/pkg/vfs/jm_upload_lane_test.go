package vfs

// JuiceMount: the small-upload lane (patch 0024) through the real write,
// background flush and fsync paths.

import (
	"bytes"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
)

// tagStorage counts PUTs in flight by the first byte of the object (each
// test file is filled with its own letter; the volume is uncompressed) and
// holds the PUTs of one letter on a gate.
type tagStorage struct {
	object.ObjectStorage
	gateTag  byte
	gate     chan struct{}
	mu       sync.Mutex
	inFlight map[byte]int
	puts     map[byte]int
}

func (s *tagStorage) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	var tag byte
	if len(data) > 0 {
		tag = data[0]
	}
	s.mu.Lock()
	s.inFlight[tag]++
	s.puts[tag]++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight[tag]--
		s.mu.Unlock()
	}()
	if tag == s.gateTag {
		<-s.gate
	}
	return s.ObjectStorage.Put(key, bytes.NewReader(data), getters...)
}

func (s *tagStorage) count(tag byte) (inFlight, puts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inFlight[tag], s.puts[tag]
}

// laneVFS is createTestVFS with the app's slow-link upload slots: one shared
// slot here, so a held PUT fills it, and one lane slot.
func laneVFS(t *testing.T, blob object.ObjectStorage) *VFS {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{Name: "lane", UUID: uuid.New().String(), Storage: "mem", BlockSize: 4096, Compression: "none"}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	conf := &Config{
		Meta: metaConf, Format: *format, Version: "Juicefs", FuseOpts: &FuseOptions{},
		Chunk: &chunk.Config{BlockSize: 4 << 20, Compress: "none", MaxUpload: 1, SmallUploads: 1,
			BufferSize: 300 << 20, CacheSize: 10 << 20, CacheDir: "memory", PutTimeout: time.Minute, MaxRetries: 1},
	}
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, *conf.Chunk, registry)
	return NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
}

func fill(tag byte, n int) []byte { return bytes.Repeat([]byte{tag}, n) }

func waitUntil(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A file written slowly is flushed in small slices by the background timer
// (idle for 1 s): those slices wait for the shared slot and never take the
// lane. An fsync of a small file takes the lane and returns while the shared
// slot is held. An fsync of a file larger than 1 MiB does not take the lane,
// even when the slice it flushes is small.
func TestJMUploadLaneOnlyFsyncOfSmallFile(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	blob := &tagStorage{ObjectStorage: mem, gateTag: 'A', gate: make(chan struct{}),
		inFlight: map[byte]int{}, puts: map[byte]int{}}
	v := laneVFS(t, blob)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	create := func(name string) (Ino, uint64) {
		fe, fh, e := v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
		if e != 0 {
			t.Fatalf("create %s: %s", name, e)
		}
		return fe.Inode, fh
	}
	write := func(ino Ino, fh uint64, tag byte, off, n int) {
		if e := v.Write(ctx, ino, fill(tag, n), uint64(off), fh); e != 0 {
			t.Fatalf("write %c: %s", tag, e)
		}
	}
	released := false
	release := func() {
		if !released {
			released = true
			close(blob.gate)
		}
	}
	defer release()

	// A: a slow writer. Its first slice goes up by the idle timer and holds
	// the one shared slot (gated); its second slice waits for that slot.
	a, afh := create("a")
	write(a, afh, 'A', 0, 200<<10)
	waitUntil(t, "background flush of A", 5*time.Second, func() bool { n, _ := blob.count('A'); return n == 1 })
	write(a, afh, 'A', 200<<10, 200<<10)
	time.Sleep(1500 * time.Millisecond) // idle timer freezes the second slice
	if n, puts := blob.count('A'); n != 1 || puts != 1 {
		t.Fatalf("background slices of A: %d in flight, %d PUTs; want 1 and 1 (no lane)", n, puts)
	}

	// B: a small save. Its fsync takes the lane while A holds the shared slot.
	b, bfh := create("b")
	write(b, bfh, 'B', 0, 300<<10)
	done := make(chan syscall.Errno, 1)
	go func() { done <- v.Fsync(ctx, b, 0, bfh) }()
	select {
	case e := <-done:
		if e != 0 {
			t.Fatalf("fsync b: %s", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fsync of a small file waited behind the shared slot")
	}

	// C: a file over 1 MiB. Its first slice is frozen by the idle timer, then
	// a small slice is fsynced. Neither takes the lane.
	c, cfh := create("c")
	write(c, cfh, 'C', 0, 1536<<10)
	time.Sleep(1500 * time.Millisecond)
	write(c, cfh, 'C', 1536<<10, 100<<10)
	cdone := make(chan syscall.Errno, 1)
	go func() { cdone <- v.Fsync(ctx, c, 0, cfh) }()
	select {
	case <-cdone:
		t.Fatal("fsync of a large file must not take the lane")
	case <-time.After(time.Second):
	}
	if _, puts := blob.count('C'); puts != 0 {
		t.Fatalf("%d PUTs of C started while the shared slot is held: one took the lane", puts)
	}

	release()
	if e := <-cdone; e != 0 {
		t.Fatalf("fsync c: %s", e)
	}
	if e := v.Fsync(ctx, a, 0, afh); e != 0 {
		t.Fatalf("fsync a: %s", e)
	}
	for _, f := range []struct {
		ino  Ino
		fh   uint64
		tag  byte
		size int
	}{{a, afh, 'A', 400 << 10}, {b, bfh, 'B', 300 << 10}, {c, cfh, 'C', 1636 << 10}} {
		buf := make([]byte, f.size)
		n, e := v.Read(ctx, f.ino, buf, 0, f.fh)
		if e != 0 || n != f.size || !bytes.Equal(buf, fill(f.tag, f.size)) {
			t.Fatalf("read back %c: %d bytes, %s", f.tag, n, e)
		}
		v.Release(ctx, f.ino, f.fh)
	}
}
