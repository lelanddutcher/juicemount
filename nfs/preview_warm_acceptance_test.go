//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Upgrade the existing selected-volume fixture from its preseeded 64 KiB hit
// to a genuinely absent 12 MiB original. The regular local file represents
// the FUSE source; publication hook below models JuiceFS's cache write.
func p86ColdVideo(t *testing.T) (*boundCacheFixture, string) {
	t.Helper()
	t.Setenv("JM_NATIVE_PREVIEW_WARM", "1")
	t.Setenv("JM_JFS_MAX_READAHEAD", "1M")
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	h.prefetchMu.Lock()
	h.prefetched["media"] = time.Now()
	h.prefetchMu.Unlock()
	h.sidecar.enabled = false
	profile := netprofile.New()
	profile.ObserveRTT(5 * time.Millisecond)
	h.readahead.profile = profile
	x.want = append(append(bytes.Repeat([]byte{'P'}, 4<<20), bytes.Repeat([]byte{'Q'}, 4<<20)...), bytes.Repeat([]byte{'R'}, 4<<20)...)
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	if err := os.WriteFile(backing, x.want, 0600); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1700000000, 123)
	if err := os.Chtimes(backing, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(backing)
	if err != nil {
		t.Fatal(err)
	}
	inode := fi.Sys().(*syscall.Stat_t).Ino
	if fi.ModTime().UnixNano() != mtime.UnixNano() {
		t.Fatalf("source mtime: %v", fi.ModTime())
	}
	e := metadata.MakeEntry("media/clip.mov", false, int64(len(x.want)), mtime, inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	attr := (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(len(x.want)), Mtime: 1700000000, Mtimensec: 123}).Marshal()
	edge := make([]byte, 9)
	edge[0] = byte(meta.TypeFile)
	binary.BigEndian.PutUint64(edge[1:], inode)
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 77)
	binary.BigEndian.PutUint32(record[12:16], uint32(len(x.want)))
	binary.BigEndian.PutUint32(record[20:24], uint32(len(x.want)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "clip.mov", edge)
	pipe.Set(ctx, "i"+p86Uint(inode), attr, 0)
	pipe.Del(ctx, "c3_0")
	pipe.RPush(ctx, "c"+p86Uint(inode)+"_0", string(record[:]))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	chunks, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(chunks, "0/0/77_0_65536")); err != nil {
		t.Fatal(err)
	}
	return x, chunks
}

func p86Uint(v uint64) string { return strconv.FormatUint(v, 10) }

func p86PublishBlock(chunks string, data []byte, off int64) error {
	if len(data) != 4<<20 || (off != 0 && off != 4<<20) {
		return errors.New("unexpected preview source range")
	}
	encoded := append([]byte(nil), data...)
	table := crc32.MakeTable(crc32.Castagnoli)
	for start := 0; start < len(data); start += 32 << 10 {
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(data[start:start+(32<<10)], table))
		encoded = append(encoded, sum[:]...)
	}
	name := filepath.Join(chunks, "0", "0", "77_"+p86Uint(uint64(off/(4<<20)))+"_4194304")
	return os.WriteFile(name, encoded, 0600)
}

func p86ReadDirPrompt(t *testing.T, x *boundCacheFixture) []os.FileInfo {
	t.Helper()
	type result struct {
		infos []os.FileInfo
		err   error
	}
	finished := make(chan result, 1)
	go func() { infos, err := x.jfs.ReadDir("media"); finished <- result{infos, err} }()
	select {
	case r := <-finished:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.infos
	case <-time.After(time.Second):
		t.Fatal("ReadDir blocked on preview source")
		return nil
	}
}

func TestP86ReadDirPreparesBoundOfflineVideo(t *testing.T) {
	x, chunks := p86ColdVideo(t)
	h := x.jfs.handler
	first, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	type sourceStep struct {
		off   int64
		count int
	}
	steps := make(chan sourceStep, 3)
	failures := make(chan error, 3)
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		if off == 0 {
			close(first)
			<-release
		}
		steps <- sourceStep{off, len(p)}
		return fd.ReadAt(p, off)
	}
	h.previewAfterRead = func(_ previewJob, p []byte, off int64) {
		if err := p86PublishBlock(chunks, p, off); err != nil {
			failures <- err
		}
	}
	h.previewFinished = func(previewJob) { close(done) }
	infos := p86ReadDirPrompt(t, x)
	if len(infos) != 1 || infos[0].Size() != int64(len(x.want)) {
		t.Fatalf("ReadDir listing: %v", infos)
	}
	select {
	case <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("no actual source read")
	}
	// The directory RPC already returned while this actual source syscall is held.
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("preview worker did not finish")
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if len(steps) != 2 {
		t.Fatalf("source steps=%d, want 2", len(steps))
	}
	for i := 0; i < 2; i++ {
		s := <-steps
		if s.off != int64(i*(4<<20)) || s.count != 4<<20 {
			t.Fatalf("source step %d: %+v", i, s)
		}
	}
	binding, _ := x.session.Reader().VolumeBinding()
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || !x.paths.CachePathEligible(metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}, id) {
		t.Fatal("worker did not qualify actual selected path")
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "setting", "d1", "d2", "i1", "i2", "i"+p86Uint(id.Inode), "c"+p86Uint(id.Inode)+"_0").Err(); err != nil {
		t.Fatal(err)
	}
	h.testRefuseFUSEContentReads = true
	cf := openBoundCacheFile(t, x)
	for _, off := range []int64{0, 5 << 20} {
		got := make([]byte, 128<<10)
		n, err := cf.ReadAt(got, off)
		if err != nil || n != len(got) || !bytes.Equal(got, x.want[off:off+int64(len(got))]) || cf.fuseFD != nil {
			t.Fatalf("offline bound read off=%d n=%d err=%v fd=%v", off, n, err, cf.fuseFD)
		}
	}
	for _, off := range []int64{8 << 20, 8<<20 - (32 << 10)} {
		got := make([]byte, 64<<10)
		n, err := cf.ReadAt(got, off)
		if n != 0 || err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
			t.Fatalf("unavailable range off=%d n=%d err=%v", off, n, err)
		}
	}
	if err := os.Remove(filepath.Join(chunks, "0/0/77_1_4194304")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 64<<10)
	if n, err := cf.ReadAt(got, 5<<20); n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("evicted block n=%d err=%v", n, err)
	}
}

func TestP86DelayedPublicationStaysUnready(t *testing.T) {
	x, _ := p86ColdVideo(t)
	h := x.jfs.handler
	done := make(chan struct{})
	steps := make(chan int64, 3)
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) { steps <- off; return fd.ReadAt(p, off) }
	h.previewFinished = func(previewJob) { close(done) }
	if _, err := x.jfs.ReadDir("media"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not retire")
	}
	if len(steps) != 2 || <-steps != 0 || <-steps != 4<<20 {
		t.Fatal("independently budgeted second source range was not read after first CRC miss")
	}
	h.testRefuseFUSEContentReads = true
	x.session.SetOnline(false)
	pin.SetOffline(true)
	cf := openBoundCacheFile(t, x)
	got := make([]byte, 64<<10)
	if n, err := cf.ReadAt(got, 0); n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("false first-block readiness n=%d err=%v", n, err)
	}
	if n, err := cf.ReadAt(got, 5<<20); n != 0 || err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("false second-block readiness n=%d err=%v", n, err)
	}
}

func TestP90ReadDirDelayedFirstPublicationStillPreparesTwoBlocks(t *testing.T) {
	x, chunks := p86ColdVideo(t)
	h := x.jfs.handler
	for _, block := range []string{"77_0_4194304", "77_1_4194304"} {
		if _, err := os.Stat(filepath.Join(chunks, "0", "0", block)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source cache was not cold for %s: %v", block, err)
		}
	}
	type sourceStep struct {
		offset int64
		bytes  int
	}
	steps := make(chan sourceStep, 3)
	failures := make(chan error, 3)
	done := make(chan struct{})
	var first []byte
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		n, err := fd.ReadAt(p, off)
		if err != nil || n != len(p) {
			failures <- errors.New("real source ReadAt failed or returned a short block")
		}
		steps <- sourceStep{off, n}
		return n, err
	}
	h.previewAfterRead = func(_ previewJob, p []byte, off int64) {
		if off == 0 {
			first = append([]byte(nil), p...)
			return // real daemon-style publication is deliberately delayed
		}
		if off != 4<<20 || len(first) != 4<<20 {
			failures <- errors.New("second source range missing before publication")
			return
		}
		if err := p86PublishBlock(chunks, first, 0); err != nil {
			failures <- err
		}
		if err := p86PublishBlock(chunks, p, off); err != nil {
			failures <- err
		}
	}
	h.previewFinished = func(previewJob) { close(done) }
	infos := p86ReadDirPrompt(t, x)
	if len(infos) != 1 || infos[0].Size() != int64(len(x.want)) {
		t.Fatalf("actual ReadDir listing: %v", infos)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("actual ReadDir worker did not retire")
	}
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if len(steps) != 2 {
		t.Fatalf("source operations=%d, want both independent 4 MiB ranges", len(steps))
	}
	for i := 0; i < 2; i++ {
		step := <-steps
		if step.offset != int64(i*(4<<20)) || step.bytes != 4<<20 {
			t.Fatalf("source operation %d: %+v", i, step)
		}
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	h.testRefuseFUSEContentReads = true
	cf := openBoundCacheFile(t, x)
	for _, off := range []int64{0, 5 << 20} {
		buf := make([]byte, 128<<10)
		n, err := cf.ReadAt(buf, off)
		if err != nil || n != len(buf) || !bytes.Equal(buf, x.want[off:off+int64(n)]) || cf.fuseFD != nil {
			t.Fatalf("delayed-publication offline bound read off=%d n=%d err=%v fd=%v", off, n, err, cf.fuseFD)
		}
	}
}

func TestP86OfflineAfterFirstSourceStopsSecond(t *testing.T) {
	x, chunks := p86ColdVideo(t)
	h := x.jfs.handler
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	steps := make(chan int64, 3)
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		steps <- off
		if off == 0 {
			close(entered)
			<-release
		}
		n, err := fd.ReadAt(p, off)
		if err != nil || n != len(p) {
			t.Errorf("held source ReadAt off=%d n=%d want=%d err=%v", off, n, len(p), err)
			return n, err
		}
		if off == 0 {
			if publishErr := p86PublishBlock(chunks, p[:n], off); publishErr != nil {
				t.Error(publishErr)
			}
		}
		return n, err
	}
	h.previewFinished = func(previewJob) { close(done) }
	if _, err := x.jfs.ReadDir("media"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first source did not enter")
	}
	pin.SetOffline(true)
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not retire")
	}
	if len(steps) != 1 || <-steps != 0 {
		t.Fatal("worker issued another source operation after offline transition")
	}
}

func TestP86StopRetainsHeldSourceAndBoundsAdmission(t *testing.T) {
	x, chunks := p86ColdVideo(t)
	h := x.jfs.handler
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	steps := make(chan int64, 3)
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		steps <- off
		if off == 0 {
			close(entered)
			<-release
		}
		n, err := fd.ReadAt(p, off)
		if err != nil || n != len(p) {
			t.Errorf("held source ReadAt off=%d n=%d want=%d err=%v", off, n, len(p), err)
			return n, err
		}
		if off == 0 {
			if publishErr := p86PublishBlock(chunks, p[:n], off); publishErr != nil {
				t.Error(publishErr)
			}
		}
		return n, err
	}
	h.previewFinished = func(previewJob) { close(done) }
	if _, err := x.jfs.ReadDir("media"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("source did not enter")
	}
	for i := 0; i < 32; i++ {
		if _, err := x.jfs.ReadDir("media"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(h.readahead.previewQueue); got != 0 {
		t.Fatalf("duplicate listings queued %d jobs", got)
	}
	if got := len(h.readahead.previewSeen); got != 1 {
		t.Fatalf("dedup entries=%d", got)
	}
	if h.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("stop returned while real source held")
	}
	if _, ok := x.store.LookupCachedFileIdentity("media/clip.mov"); !ok {
		t.Fatal("store retired across held source")
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not retire")
	}
	if !h.StopHandler() {
		t.Fatal("stop failed after source completion")
	}
	if len(steps) != 1 || <-steps != 0 {
		t.Fatal("stop allowed a second source operation")
	}
}

func TestP86MutationInvalidatesHeldSource(t *testing.T) {
	x, chunks := p86ColdVideo(t)
	h := x.jfs.handler
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	steps := make(chan int64, 3)
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) {
		steps <- off
		if off == 0 {
			close(entered)
			<-release
		}
		n, err := fd.ReadAt(p, off)
		if err != nil || n != len(p) {
			t.Errorf("held source ReadAt off=%d n=%d want=%d err=%v", off, n, len(p), err)
			return n, err
		}
		if off == 0 {
			if publishErr := p86PublishBlock(chunks, p[:n], off); publishErr != nil {
				t.Error(publishErr)
			}
		}
		return n, err
	}
	h.previewFinished = func(previewJob) { close(done) }
	if _, err := x.jfs.ReadDir("media"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("source did not enter")
	}
	old, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok {
		t.Fatal("missing old identity")
	}
	replacement := metadata.MakeEntry(old.Path, false, old.Size, time.Unix(1700000001, 123), old.Inode)
	replacement.MtimeNsecKnown = true
	if err := x.store.Insert(replacement); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not retire")
	}
	if len(steps) != 1 || <-steps != 0 {
		t.Fatal("stale job crossed into successor source")
	}
	if next, ok := x.store.LookupCachedFileIdentity(old.Path); !ok || next == old {
		t.Fatal("mutation did not replace identity")
	}
}

func TestP86ReadDirRefusesUnqualifiedPathBeforeSource(t *testing.T) {
	x, _ := p86ColdVideo(t)
	h := x.jfs.handler
	b := h.cacheBinding.Load()
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		return metadata.RedisPathProof{}, errors.New("no selected ancestry proof")
	}
	steps := make(chan int64, 1)
	done := make(chan struct{})
	h.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) { steps <- off; return fd.ReadAt(p, off) }
	h.previewFinished = func(previewJob) { close(done) }
	p86ReadDirPrompt(t, x)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("refused worker did not finish")
	}
	if len(steps) != 0 {
		t.Fatal("unqualified path issued source read")
	}
}

func TestP86ReadDirAdmissionRequiresOptInAndHealth(t *testing.T) {
	for _, tc := range []struct {
		name, flag, rollback string
		unknown              bool
	}{
		{name: "default-off", flag: "", rollback: "1M"},
		{name: "rollback", flag: "1", rollback: "0"},
		{name: "unknown-health", flag: "1", rollback: "1M", unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, _ := p86ColdVideo(t)
			t.Setenv("JM_NATIVE_PREVIEW_WARM", tc.flag)
			t.Setenv("JM_JFS_MAX_READAHEAD", tc.rollback)
			if tc.unknown {
				x.jfs.handler.readahead.profile = netprofile.New()
			}
			steps := make(chan int64, 1)
			x.jfs.handler.previewReadAt = func(fd *os.File, p []byte, off int64) (int, error) { steps <- off; return fd.ReadAt(p, off) }
			p86ReadDirPrompt(t, x)
			rm := x.jfs.handler.readahead
			if len(rm.previewQueue) != 0 || len(rm.previewSeen) != 0 || len(steps) != 0 {
				t.Fatal("refused listing admitted preview work")
			}
		})
	}
}
