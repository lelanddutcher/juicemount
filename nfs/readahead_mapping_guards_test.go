//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/redis/go-redis/v9"
)

// An ordinary read establishes real path authority; clearing only the Reader's
// mapping makes the following preparation the sole possible map publisher.
func readaheadGuardJob(t *testing.T, x *boundCacheFixture) *readaheadMappingJob {
	t.Helper()
	f := openBoundCacheFile(t, x)
	var buf [4096]byte
	if n, err := f.ReadAt(buf[:], 0); n != len(buf) || err != nil || !bytes.Equal(buf[:], bytes.Repeat([]byte{'B'}, len(buf))) {
		t.Fatalf("ordinary qualification n=%d err=%v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	x.session.Reader().InvalidateSliceCache(3)
	j := x.jfs.handler.captureReadaheadMapping(3, "media/clip.mov")
	if j == nil {
		t.Fatal("unchanged resident source could not capture metadata authority")
	}
	t.Cleanup(j.release)
	j.record(0, 4096)
	return j
}

func TestReadaheadMappingRejectsChangedProducerAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *boundCacheFixture)
	}{
		{name: "unchanged"},
		{name: "writer ABA", mutate: func(_ *testing.T, x *boundCacheFixture) {
			x.jfs.handler.incActiveWriter("media/clip.mov")
			x.jfs.handler.decActiveWriter("media/clip.mov")
		}},
		{name: "namespace ABA", mutate: func(t *testing.T, x *boundCacheFixture) {
			op := x.jfs.handler.namespace.begin([]string{"media/clip.mov"})
			if op == nil {
				t.Fatal("namespace operation not admitted")
			}
			op.blockReads()
			op.finish(nil)
		}},
		{name: "ancestor replacement", mutate: func(t *testing.T, x *boundCacheFixture) {
			replaceBoundCacheEntry(t, x, "media")
		}},
		{name: "reader replacement", mutate: func(_ *testing.T, x *boundCacheFixture) {
			x.jfs.handler.SetCacheReader(nil)
		}},
		{name: "remote epoch", mutate: func(_ *testing.T, x *boundCacheFixture) {
			x.session.SetOnline(false)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			j := readaheadGuardJob(t, x)
			if tc.mutate != nil {
				tc.mutate(t, x)
			}
			j.prepare()
			buf := make([]byte, 4096)
			witness := cache.ResidentAttr{Size: j.identity.Size, MtimeNS: j.identity.MtimeNS, Valid: true}
			_, err := j.reader.ReadResidentRange(3, 0, buf, witness)
			if tc.mutate == nil {
				if err != nil || !bytes.Equal(buf, bytes.Repeat([]byte{'B'}, len(buf))) {
					t.Fatalf("unchanged producer failed to publish map: err=%v", err)
				}
			} else if !errors.Is(err, cache.ErrCacheMiss) {
				t.Fatalf("changed %s published a map: err=%v", tc.name, err)
			}
		})
	}
}

func TestReadaheadMappingPublishesOnlyActualPositiveReads(t *testing.T) {
	for _, tc := range []struct {
		name    string
		read    func(*os.File, []byte, int64) (int, error)
		wantMap bool
	}{
		{"zero", func(*os.File, []byte, int64) (int, error) { return 0, nil }, false},
		{"error", func(*os.File, []byte, int64) (int, error) { return 0, errors.New("source read failed") }, false},
		{"partial with error", func(fd *os.File, p []byte, off int64) (int, error) {
			n, err := fd.ReadAt(p[:2048], off)
			if err != nil {
				return n, err
			}
			return n, errors.New("partial source read")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			j := readaheadGuardJob(t, x)
			j.release() // The actual worker acquires its own captured authority.
			rm := x.jfs.handler.readahead
			rm.prefetchReadAt = tc.read
			rm.prefetchWithInode(3, "media/clip.mov", 0, 4096, 1)
			buf := make([]byte, 4096)
			witness := cache.ResidentAttr{Size: j.identity.Size, MtimeNS: j.identity.MtimeNS, Valid: true}
			_, err := j.reader.ReadResidentRange(3, 0, buf, witness)
			if tc.wantMap {
				// The 4 KiB CRC bytes were preseeded. A 2 KiB source read
				// establishes map preparation, not new residency of its remainder.
				if err != nil || !bytes.Equal(buf, bytes.Repeat([]byte{'B'}, len(buf))) {
					t.Fatalf("positive partial source read did not prepare map: err=%v", err)
				}
			} else if !errors.Is(err, cache.ErrCacheMiss) {
				t.Fatalf("nonpositive source read prepared map: err=%v", err)
			}
		})
	}
}

type readaheadHeldMappingHook struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*readaheadHeldMappingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*readaheadHeldMappingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *readaheadHeldMappingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && h.armed.Load() {
			for _, arg := range cmd.Args() {
				if fmt.Sprint(arg) == "c3_0" {
					select {
					case h.entered <- struct{}{}:
					default:
					}
					<-h.release // Retain the actual hook after caller cancellation.
					break
				}
			}
		}
		return next(ctx, cmd)
	}
}

func TestReadaheadMappingShutdownJoinsHeldPreparation(t *testing.T) {
	x := newBoundCacheFixture(t)
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing selected volume")
	}
	hook := &readaheadHeldMappingHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
	client := redis.NewClient(x.rdb.Options())
	client.AddHook(hook)
	r, err := cache.NewVolumeReader(binding, client)
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	x.jfs.handler.SetCacheReader(r)
	t.Cleanup(func() { x.jfs.handler.StopHandler(); r.Stop(); _ = client.Close() })
	f := openBoundCacheFile(t, x)
	assertPhaseRead(t, f, 'B', false)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r.InvalidateSliceCache(3)
	j := x.jfs.handler.captureReadaheadMapping(3, "media/clip.mov")
	if j == nil {
		t.Fatal("cannot capture mapping job")
	}
	j.record(0, 4096)
	hook.armed.Store(true)
	prepared := make(chan struct{})
	go func() { defer close(prepared); defer j.release(); j.prepare() }()
	// Release and join the original job on every failing assertion, before
	// fixture cleanup can retire the Reader, client, and metadata store.
	defer func() { hook.once.Do(func() { close(hook.release) }); <-prepared }()
	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("mapping request did not reach held Redis hook")
	}
	if x.jfs.handler.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("handler retired dependencies while mapping work was held")
	}
	hook.once.Do(func() { close(hook.release) })
	<-prepared
	if !x.jfs.handler.stopHandlerWithDeadline(3 * time.Second) {
		t.Fatal("handler did not retire after original mapping job joined")
	}
}

func TestReadaheadMappingHeldFetchRejectsChangedOwner(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*boundCacheFixture)
	}{
		{"reader replacement", func(x *boundCacheFixture) { x.jfs.handler.SetCacheReader(nil) }},
		{"remote epoch", func(x *boundCacheFixture) { x.session.SetOnline(false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			binding, bound := x.session.Reader().VolumeBinding()
			if !bound {
				t.Fatal("missing selected volume")
			}
			hook := &readaheadHeldMappingHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
			client := redis.NewClient(x.rdb.Options())
			client.AddHook(hook)
			r, err := cache.NewVolumeReader(binding, client)
			if err != nil {
				_ = client.Close()
				t.Fatal(err)
			}
			x.jfs.handler.SetCacheReader(r)
			t.Cleanup(func() { x.jfs.handler.StopHandler(); r.Stop(); _ = client.Close() })
			f := openBoundCacheFile(t, x)
			assertPhaseRead(t, f, 'B', false)
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			r.InvalidateSliceCache(3)
			j := x.jfs.handler.captureReadaheadMapping(3, "media/clip.mov")
			if j == nil {
				t.Fatal("cannot capture mapping job")
			}
			j.record(0, 4096)
			hook.armed.Store(true)
			done := make(chan struct{})
			go func() { defer close(done); defer j.release(); j.prepare() }()
			defer func() { hook.once.Do(func() { close(hook.release) }); <-done }()
			select {
			case <-hook.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("mapping fetch not held")
			}
			tc.mutate(x)
			if j.current() {
				t.Fatal("owner mutation did not revoke captured job")
			}
			hook.once.Do(func() { close(hook.release) })
			<-done
			if j.current() {
				t.Fatal("completed old job regained owner authority")
			}
		})
	}
}
