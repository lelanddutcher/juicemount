package vfs

// JuiceMount patch 0035: the JMLinkPolicy message (version 2) sets the
// read-ahead of one sequential reader (jm_readahead.go).

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

func linkSet2(flags byte, getMS, putMS uint32, retries byte, raKiB uint32) []byte {
	b := linkSet(flags, getMS, putMS, retries)
	b[0] = jmLinkPolicyVersion2
	return binary.BigEndian.AppendUint32(b, raKiB)
}

// Version 2 sets and reports the read-ahead; version 1 leaves it alone and
// keeps its 11-byte reply.
func TestJMReadaheadMessage(t *testing.T) {
	v := pinVFS(t) // 1 MiB blocks, --max-readahead 0
	if got, want := linkMsg(v, []byte{jmLinkPolicyVersion2}), append([]byte{0}, linkSet2(0, 60000, 60000, 1, 0)[1:]...); !bytes.Equal(got, want) {
		t.Fatalf("query v2: % x, want % x", got, want)
	}
	fast := linkSet2(0, 60000, 60000, 10, 16<<10)
	if got := linkMsg(v, fast); !bytes.Equal(got, append([]byte{0}, fast[1:]...)) {
		t.Fatalf("set v2: % x", got)
	}
	slow := linkSet(jmLinkFlagDemandFirst, 120000, 180000, 3)
	if got := linkMsg(v, slow); !bytes.Equal(got, append([]byte{0}, slow[1:]...)) {
		t.Fatalf("set v1: % x (want the 11-byte layout)", got)
	}
	want := append([]byte{0}, linkSet2(jmLinkFlagDemandFirst, 120000, 180000, 3, 16<<10)[1:]...)
	if got := linkMsg(v, []byte{jmLinkPolicyVersion2}); !bytes.Equal(got, want) {
		t.Fatalf("v1 changed the read-ahead: % x, want % x", got, want)
	}
	// Below one block is off and reported as 0.
	if got := linkMsg(v, linkSet2(0, 60000, 60000, 10, 512)); !bytes.Equal(got, append([]byte{0}, linkSet2(0, 60000, 60000, 10, 0)[1:]...)) {
		t.Fatalf("half a block: % x", got)
	}
	dr := v.reader.(*dataReader)
	if dr.jmReadaheadMax() != 512<<10 || dr.jmClampReadahead(4<<20) != 0 || dr.jmMaxRequests() != 1 {
		t.Fatalf("half a block: max %d, clamp %d, requests %d", dr.jmReadaheadMax(), dr.jmClampReadahead(4<<20), dr.jmMaxRequests())
	}
}

func TestJMReadaheadMessageRefusals(t *testing.T) {
	v := pinVFS(t)
	before := linkMsg(v, []byte{jmLinkPolicyVersion2})
	for name, body := range map[string][]byte{
		"v2 short (v1 body)": linkSet(1, 60000, 60000, 3),
		"v2 long":            append(linkSet2(1, 60000, 60000, 3, 0), 0),
		"over 1 GiB":         linkSet2(1, 60000, 60000, 3, 1<<20+1),
		"bad timeout":        linkSet2(1, 0, 60000, 3, 16<<10),
	} {
		if body[0] != jmLinkPolicyVersion2 {
			body = append([]byte{jmLinkPolicyVersion2}, body[1:]...)
		}
		if got := linkMsg(v, body); !bytes.Equal(got, []byte{uint8(syscall.EINVAL)}) {
			t.Fatalf("%s: reply % x, want EINVAL", name, got)
		}
	}
	if after := linkMsg(v, []byte{jmLinkPolicyVersion2}); !bytes.Equal(after, before) {
		t.Fatalf("a refused message changed the policy: % x -> % x", before, after)
	}
	if got := linkMsg(v, linkSet2(1, 60000, 60000, 3, 1<<20)); got[0] != 0 {
		t.Fatalf("exactly 1 GiB refused: % x", got)
	}
}

// blockStore records the highest block index any GET asked for.
type blockStore struct {
	object.ObjectStorage
	mu  sync.Mutex
	max int
}

func (s *blockStore) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	parts := strings.Split(key, "_")
	if len(parts) >= 3 {
		if i, err := strconv.Atoi(parts[len(parts)-2]); err == nil {
			s.mu.Lock()
			s.max = max(s.max, i)
			s.mu.Unlock()
		}
	}
	return s.ObjectStorage.Get(key, off, limit, getters...)
}

func (s *blockStore) maxBlock() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

// raVFS writes size bytes to /f through one VFS and returns a reader VFS on
// the same metadata with an empty cache and the given --max-readahead, its
// store, and an open handle.
func raVFS(t *testing.T, size int, mountRA int) (*VFS, *blockStore, Ino, uint64) {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{Name: "ra", UUID: uuid.New().String(), Storage: "mem", BlockSize: 1024, Compression: "none"}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	bs := &blockStore{ObjectStorage: blob, max: -1}
	newVFS := func(ra int) *VFS {
		conf := &Config{
			Meta: metaConf, Format: *format, Version: "Juicefs", FuseOpts: &FuseOptions{},
			Chunk: &chunk.Config{BlockSize: 1 << 20, Compress: "none", MaxUpload: 2, BufferSize: 256 << 20,
				CacheSize: 256 << 20, CacheDir: t.TempDir(), FreeSpace: 0.0001, CacheMode: 0600, AutoCreate: true,
				PutTimeout: time.Minute, GetTimeout: time.Minute, MaxRetries: 1, Readahead: ra},
		}
		registry := prometheus.NewRegistry()
		store := chunk.NewCachedStore(bs, *conf.Chunk, registry)
		return NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
	}
	w := newVFS(0)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	fe, fh, st := w.Create(ctx, 1, "f", 0644, 0, uint32(os.O_RDWR))
	if st != 0 {
		t.Fatalf("create: %s", st)
	}
	data := bytes.Repeat([]byte{7}, size)
	if st := w.Write(ctx, fe.Inode, data, 0, fh); st != 0 {
		t.Fatalf("write: %s", st)
	}
	if st := w.Flush(ctx, fe.Inode, fh, 0); st != 0 {
		t.Fatalf("flush: %s", st)
	}
	w.Release(ctx, fe.Inode, fh)
	r := newVFS(mountRA)
	bs.mu.Lock()
	bs.max = -1
	bs.mu.Unlock()
	_, rfh, st := r.Open(ctx, fe.Inode, uint32(os.O_RDONLY))
	if st != 0 {
		t.Fatalf("open: %s", st)
	}
	return r, bs, fe.Inode, rfh
}

// readSeq reads [from, to) in 128 KiB reads, as macFUSE sends them.
func readSeq(t *testing.T, v *VFS, ino Ino, fh uint64, from, to int) {
	t.Helper()
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2}))
	buf := make([]byte, 128<<10)
	for off := from; off < to; off += len(buf) {
		if n, st := v.Read(ctx, ino, buf, uint64(off), fh); st != 0 || n != len(buf) {
			t.Fatalf("read at %d: %d bytes, %s", off, n, st)
		}
	}
}

// settle waits until the highest block asked for stops moving.
func settle(bs *blockStore) int {
	last := -2
	for i := 0; i < 40; i++ {
		time.Sleep(25 * time.Millisecond)
		m := bs.maxBlock()
		if m == last {
			return m
		}
		last = m
	}
	return last
}

// The app's mount (--max-readahead below one block) never reads ahead until
// a message sets it; then a reader that has read two blocks in a row gets
// read-ahead, and a shorter run (a preview or a scrub jump) does not.
func TestJMReadaheadFollowsMessage(t *testing.T) {
	const mib = 1 << 20
	v, bs, ino, fh := raVFS(t, 24*mib, 512<<10)
	readSeq(t, v, ino, fh, 0, 3*mib)
	if got := settle(bs); got != 2 {
		t.Fatalf("mount value (off): highest block %d after reading 3 MiB, want 2", got)
	}

	if got := linkMsg(v, linkSet2(0, 60000, 60000, 10, 8<<10)); got[0] != 0 {
		t.Fatalf("set: % x", got)
	}
	// A new run (a jump to 12 MiB) shorter than two blocks: no read-ahead.
	readSeq(t, v, ino, fh, 12*mib, 12*mib+mib+mib/2)
	if got := settle(bs); got != 13 {
		t.Fatalf("1.5 MiB run: highest block %d, want 13 (no read-ahead)", got)
	}
	// A second jump 0.75 MiB past it (a scrub): juicefs's session takes it
	// and counts the gap (3.25 MiB > two blocks), but the run restarts.
	readSeq(t, v, ino, fh, 14*mib+mib/4, 15*mib+mib/4)
	if got := settle(bs); got != 15 {
		t.Fatalf("nearby jump: highest block %d, want 15 (no read-ahead)", got)
	}
	// That run reaches two blocks: read-ahead starts past the reader.
	readSeq(t, v, ino, fh, 15*mib+mib/4, 16*mib+mib/2)
	if got := settle(bs); got < 18 || got > 16+8+1 {
		t.Fatalf("2.25 MiB run: highest block %d, want read-ahead past 16 and within 8 MiB", got)
	}

	// Off again: a running session stops reading ahead at its next read.
	if got := linkMsg(v, linkSet2(0, 60000, 60000, 10, 0)); got[0] != 0 {
		t.Fatalf("set off: % x", got)
	}
	before := settle(bs)
	readSeq(t, v, ino, fh, 16*mib+mib/2, before*mib+mib)
	if got := settle(bs); got != before {
		t.Fatalf("off: highest block %d after reading through block %d, want no block past it", got, before)
	}
}

// Without a message the mount's --max-readahead holds, upstream behaviour:
// read-ahead from the first read.
func TestJMReadaheadUnsetKeepsUpstream(t *testing.T) {
	const mib = 1 << 20
	v, bs, ino, fh := raVFS(t, 16*mib, 8*mib)
	readSeq(t, v, ino, fh, 0, mib/2)
	if got := settle(bs); got < 1 {
		t.Fatalf("upstream read-ahead: highest block %d after half a block, want read-ahead", got)
	}
	dr := v.reader.(*dataReader)
	if dr.jmReadaheadMinRun() != 0 || dr.jmMaxRequests() != dr.maxRequests || dr.JMReadahead() != 8*mib {
		t.Fatalf("unset: min run %d, requests %d/%d, readahead %d", dr.jmReadaheadMinRun(), dr.jmMaxRequests(), dr.maxRequests, dr.JMReadahead())
	}
}

// Patch 0030's check in waitForIO failed a read whose slice had finished:
// the slice turned READY, then a cleanup dropped it while this reader still
// held it (drop marks it INVALID), before the reader woke. The reader got
// EIO for bytes it had (lab 2026-11-27: 6-41 EIO reads in 30 s of one
// sequential read on main). It must copy them.
func TestJMWaitedSliceDroppedAfterReadyIsNotEIO(t *testing.T) {
	const n = 4096
	f := &fileReader{length: n, r: &dataReader{blockSize: 1 << 20}}
	f.last = &f.slices
	s := &sliceReader{file: f, state: BUSY, refs: 1, block: &frange{0, n}, page: chunk.NewOffPage(n)}
	s.cond = utils.NewCond(&f.Mutex)
	f.slices = s
	f.last = &s.next
	s.prev = &f.slices
	buf := make([]byte, n)
	type result struct {
		n   int
		eno syscall.Errno
	}
	done := make(chan result, 1)
	f.Lock()
	go func() {
		f.Lock()
		got, eno := f.waitForIO(meta.Background(), []*req{{frange{0, n}, s}}, buf)
		f.Unlock()
		done <- result{got, eno}
	}()
	f.Unlock()
	time.Sleep(50 * time.Millisecond) // the reader is waiting on s
	f.Lock()
	copy(s.page.Data, bytes.Repeat([]byte{9}, n))
	s.state, s.currentPos = READY, n
	s.cond.Broadcast()
	s.drop() // a cleanup before the reader wakes: READY and in use -> INVALID
	if s.state != INVALID {
		t.Fatalf("drop left state %s, want INVALID (the race)", s.state)
	}
	f.Unlock()
	select {
	case r := <-done:
		if r.eno != 0 || r.n != n || buf[0] != 9 || buf[n-1] != 9 {
			t.Fatalf("read %d bytes, errno %v, want %d bytes of the finished slice", r.n, r.eno, n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not return")
	}
}

// Review L1: looping a short section (a jump back more than half a block)
// starts a new run each pass, so repeats never add up to the minimum run.
func TestJMReadaheadLoopIsNotARun(t *testing.T) {
	const mib = 1 << 20
	v, bs, ino, fh := raVFS(t, 24*mib, 512<<10)
	if got := linkMsg(v, linkSet2(0, 60000, 60000, 10, 8<<10)); got[0] != 0 {
		t.Fatalf("set: % x", got)
	}
	for pass := 0; pass < 4; pass++ { // 4 x 1.5 MiB = 6 MiB read, three times the minimum run
		readSeq(t, v, ino, fh, 4*mib, 4*mib+mib+mib/2)
	}
	if got := settle(bs); got != 5 {
		t.Fatalf("looped 1.5 MiB section: highest block %d, want 5 (no read-ahead)", got)
	}
}

// Version 2 carries patch 0036's bit 2 like version 1: it sets and reports
// the read-compaction gate together with the read-ahead.
func TestJMReadaheadMessageReadCompactionBit(t *testing.T) {
	v := pinVFS(t) // 1 MiB blocks
	g := v.Meta.(meta.JMReadCompactionGate)
	off := linkSet2(jmLinkFlagDemandFirst|jmLinkFlagNoReadComp, 120000, 180000, 3, 16<<10)
	if got := linkMsg(v, off); !bytes.Equal(got, append([]byte{0}, off[1:]...)) {
		t.Fatalf("set off: % x", got)
	}
	if g.JMReadCompaction() {
		t.Fatal("version 2 bit 2 did not turn read compaction off")
	}
	if got := linkMsg(v, []byte{jmLinkPolicyVersion2}); !bytes.Equal(got, append([]byte{0}, off[1:]...)) {
		t.Fatalf("query after off: % x", got)
	}
	on := linkSet2(jmLinkFlagDemandFirst, 60000, 60000, 10, 16<<10)
	if got := linkMsg(v, on); !bytes.Equal(got, append([]byte{0}, on[1:]...)) {
		t.Fatalf("set on: % x", got)
	}
	if !g.JMReadCompaction() {
		t.Fatal("a version 2 message without bit 2 did not turn read compaction on")
	}
}
