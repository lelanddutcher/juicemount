//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redistestfixture"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type boundCacheFixture struct {
	jfs                                  *juiceFS
	store                                *metadata.Store
	rc                                   *metadata.RedisClient
	rdb                                  *redis.Client
	session                              *cache.Session
	paths                                *metadata.CachePathEligibility
	want                                 []byte
	raw, db, state, fuseRoot, volumeUUID string
}

func newBoundCacheFixture(t *testing.T, handlerOpts ...HandlerOption) *boundCacheFixture {
	t.Helper()
	return newBoundCacheFixtureWithRedis(t, redistestfixture.Start(t), handlerOpts...)
}

// Every client in this fixture receives the same supplied logical endpoint.
// The response-hold experiment supplies an owned proxy before construction.
func newBoundCacheFixtureWithRedis(t *testing.T, rawURL string, handlerOpts ...HandlerOption) *boundCacheFixture {
	return newBoundCacheFixtureWithRedisVolume(t, rawURL, "selected-volume", handlerOpts...)
}

func newBoundCacheFixtureWithRedisVolume(t *testing.T, rawURL, uuid string, handlerOpts ...HandlerOption) *boundCacheFixture {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("JM_CACHE_DIR", "")
	x := &boundCacheFixture{raw: rawURL, volumeUUID: uuid, db: filepath.Join(t.TempDir(), "metadata.db"), state: t.TempDir(), fuseRoot: t.TempDir(), want: bytes.Repeat([]byte{'B'}, 65536)}
	return seedBoundCacheFixture(t, x, handlerOpts...)
}

// seedBoundCacheFixture accepts caller-owned persistent paths for process tests.
func seedBoundCacheFixture(t *testing.T, x *boundCacheFixture, handlerOpts ...HandlerOption) *boundCacheFixture {
	t.Helper()
	pin.SetOffline(false)
	pin.ResetFUSEIdentityForTest()
	t.Cleanup(func() { pin.SetOffline(false); pin.ResetFUSEIdentityForTest() })
	var err error
	x.store, err = metadata.Open(x.db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = x.store.Close() })
	x.rc, err = metadata.NewRedisClientDeferred(x.raw, x.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(x.rc.Stop)
	opts, err := redisurl.Parse(x.raw)
	if err != nil {
		t.Fatal(err)
	}
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	x.rdb = redis.NewClient(opts)
	t.Cleanup(func() { _ = x.rdb.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	setting, err := json.Marshal(struct {
		UUID      string
		BlockSize int
	}{UUID: x.volumeUUID, BlockSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	attr := func(typ uint8, parent, size uint64) []byte {
		return (&meta.Attr{Typ: typ, Parent: meta.Ino(parent), Length: size, Mtime: 1700000000, Mtimensec: 123}).Marshal()
	}
	edge := func(typ byte, inode uint64) []byte {
		v := make([]byte, 9)
		v[0] = typ
		binary.BigEndian.PutUint64(v[1:], inode)
		return v
	}
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 77)
	binary.BigEndian.PutUint32(record[12:16], uint32(len(x.want)))
	binary.BigEndian.PutUint32(record[20:24], uint32(len(x.want)))
	pipe := x.rdb.Pipeline()
	pipe.Set(ctx, "setting", setting, 0)
	pipe.Set(ctx, "i1", attr(meta.TypeDirectory, 1, 0), 0)
	pipe.HSet(ctx, "d1", "media", edge(meta.TypeDirectory, 2))
	pipe.Set(ctx, "i2", attr(meta.TypeDirectory, 1, 0), 0)
	pipe.HSet(ctx, "d2", "clip.mov", edge(meta.TypeFile, 3))
	pipe.Set(ctx, "i3", attr(meta.TypeFile, 2, uint64(len(x.want))), 0)
	pipe.RPush(ctx, "c3_0", string(record[:]))
	if _, err = pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for _, e := range []*metadata.Entry{metadata.MakeEntry("media", true, 0, time.Unix(1700000000, 123), 2), metadata.MakeEntry("media/clip.mov", false, int64(len(x.want)), time.Unix(1700000000, 123), 3)} {
		e.MtimeNsecKnown = true
		if err = x.store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	if err = os.MkdirAll(filepath.Dir(backing), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(backing, bytes.Repeat([]byte{'F'}, len(x.want)), 0600); err != nil {
		t.Fatal(err)
	}
	chunks, err := cache.VolumeCacheDir(x.volumeUUID)
	if err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(chunks, "0/0/77_0_65536")
	if err = os.MkdirAll(filepath.Dir(block), 0700); err != nil {
		t.Fatal(err)
	}
	encoded := append([]byte(nil), x.want...)
	table := crc32.MakeTable(crc32.Castagnoli)
	for start := 0; start < len(x.want); start += 32 << 10 {
		end := start + (32 << 10)
		if end > len(x.want) {
			end = len(x.want)
		}
		var sum [4]byte
		binary.BigEndian.PutUint32(sum[:], crc32.Checksum(x.want[start:end], table))
		encoded = append(encoded, sum[:]...)
	}
	if err = os.WriteFile(block, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	x.paths, err = metadata.OpenCachePathEligibility(x.store)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(x.store, x.fuseRoot, handlerOpts...)
	x.jfs = &juiceFS{handler: h}
	x.session, err = cache.NewSession(cache.SessionConfig{Store: x.store, LogicalURL: x.raw, Options: opts, StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil }, OnReader: h.SetCacheReader, OnQualified: x.rc.SetBackendQualified, OnStatus: h.SetCacheBackendStatus})
	if err != nil {
		t.Fatal(err)
	}
	h.SetBoundCacheSession(x.session, x.rc, x.paths)
	h.SetCacheReader(x.session.Reader())
	t.Cleanup(func() { h.StopHandler(); x.session.Stop() })
	x.session.SetOnline(true)
	if err = x.session.Qualify(ctx); err != nil {
		t.Fatal(err)
	}
	return x
}

func openBoundCacheFile(t *testing.T, x *boundCacheFixture) *cachedFile {
	t.Helper()
	f, err := x.jfs.OpenFile("media/clip.mov", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	cf, ok := f.(*cachedFile)
	if !ok {
		t.Fatalf("unexpected file %T", f)
	}
	t.Cleanup(func() { _ = cf.Close() })
	cf.memBuf = nil
	cf.readahead = nil
	return cf
}

func TestBoundCacheProvesLegacyPathThenReadsOffline(t *testing.T) {
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	got := make([]byte, len(x.want))
	if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, x.want) || cf.fuseFD != nil {
		t.Fatalf("qualified direct read = %d %v, FUSE=%v", n, err, cf.fuseFD != nil)
	}
	b, _ := x.session.Reader().VolumeBinding()
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || !x.paths.CachePathEligible(metadata.CachePathBinding{Digest: b.Backend, FormatUUID: b.UUID}, id) {
		t.Fatal("actual Redis ancestry proof did not qualify legacy row")
	}
	x.session.SetOnline(false)
	pin.SetOffline(true)
	if err := x.rdb.Del(context.Background(), "setting", "d1", "d2", "i1", "i2", "i3", "c3_0").Err(); err != nil {
		t.Fatal(err)
	}
	clear(got)
	if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, x.want) || cf.fuseFD != nil {
		t.Fatalf("offline qualified read = %d %v", n, err)
	}
}

func TestBoundCacheWrongLegacyAncestryDoesNotServe(t *testing.T) {
	x := newBoundCacheFixture(t)
	edge := make([]byte, 9)
	edge[0] = meta.TypeDirectory
	binary.BigEndian.PutUint64(edge[1:], 99)
	if err := x.rdb.HSet(context.Background(), "d1", "media", edge).Err(); err != nil {
		t.Fatal(err)
	}
	cf := openBoundCacheFile(t, x)
	got := make([]byte, len(x.want))
	n, err := cf.ReadAt(got, 0)
	if err != nil || n != len(got) || !bytes.Equal(got, bytes.Repeat([]byte{'F'}, len(got))) || cf.fuseFD == nil {
		t.Fatalf("unproven cache did not fall through coherently = %d %v", n, err)
	}
	b, _ := x.session.Reader().VolumeBinding()
	id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
	if x.paths.CachePathEligible(metadata.CachePathBinding{Digest: b.Backend, FormatUUID: b.UUID}, id) {
		t.Fatal("wrong ancestor became eligible")
	}
}

func TestReadCacheVolumeMismatchNeverFallsThrough(t *testing.T) {
	for _, offline := range []bool{false, true} {
		name := "online"
		if offline {
			name = "offline"
		}
		t.Run(name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			cf := openBoundCacheFile(t, x)
			got := make([]byte, len(x.want))
			if _, err := cf.ReadAt(got, 0); err != nil {
				t.Fatal(err)
			}
			x.session.Reader().InvalidateSliceCache(3)
			if err := x.rdb.Set(context.Background(), "setting", `{"UUID":"other-volume","BlockSize":4096}`, 0).Err(); err != nil {
				t.Fatal(err)
			}
			pin.SetOffline(offline)
			for i := 0; i < 2; i++ {
				clear(got)
				n, err := cf.ReadAt(got, 0)
				if n != 0 || !errors.Is(err, syscall.EIO) || !errors.Is(err, cache.ErrVolumeBinding) {
					t.Fatalf("identity refusal %d = %d %v", i, n, err)
				}
				if cf.fuseFD != nil || !bytes.Equal(got, make([]byte, len(got))) {
					t.Fatal("identity refusal exposed backing bytes")
				}
			}
		})
	}
}

func TestBoundCacheRetainedHandleRefusesReplacement(t *testing.T) {
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	got := make([]byte, len(x.want))
	if _, err := cf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry("media/clip.mov", false, int64(len(x.want)), time.Unix(1700000000, 123), 3)
	e.MtimeNsecKnown = true
	if err := x.store.Delete("media/clip.mov"); err != nil {
		t.Fatal(err)
	}
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	clear(got)
	n, err := cf.ReadAt(got, 0)
	if n != 0 || !errors.Is(err, syscall.EAGAIN) || cf.fuseFD != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatalf("retained handle crossed local replacement = %d %v", n, err)
	}
}

func replaceBoundCacheEntry(t *testing.T, x *boundCacheFixture, name string) {
	t.Helper()
	e := x.store.LookupByPath(name)
	if e == nil {
		t.Fatal("missing replacement source")
	}
	if err := x.store.Delete(name); err != nil {
		t.Fatal(err)
	}
	fresh := metadata.MakeEntry(e.Path, e.IsDir, e.Size, e.Mtime, e.Inode)
	fresh.Mode, fresh.LocalOnly, fresh.MtimeNsecKnown = e.Mode, e.LocalOnly, e.MtimeNsecKnown
	if err := x.store.Insert(fresh); err != nil {
		t.Fatal(err)
	}
}

func assertBoundCacheRefusal(t *testing.T, cf *cachedFile, want error) {
	t.Helper()
	got := make([]byte, 4096)
	n, err := cf.ReadAt(got, 0)
	if n != 0 || !errors.Is(err, want) || cf.fuseFD != nil || !bytes.Equal(got, make([]byte, len(got))) {
		t.Fatalf("refusal = %d %v, FUSE=%v", n, err, cf.fuseFD != nil)
	}
}

func TestBoundCacheProofFormatRefusalSurvivesSecondObservation(t *testing.T) {
	for _, after := range []string{"format-restored", "deadline-expired"} {
		t.Run(after, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			cf := openBoundCacheFile(t, x)
			if err := x.rdb.Set(context.Background(), "setting", `{"UUID":"other-volume","BlockSize":4096}`, 0).Err(); err != nil {
				t.Fatal(err)
			}
			b := x.jfs.handler.cacheBinding.Load()
			actual := b.readProof
			calls := 0
			b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
				calls++
				proof, err := actual(ctx, uuid, name, ids)
				if !errors.Is(err, metadata.ErrReadProofVolume) {
					t.Fatalf("atomic proof lost format refusal: %v", err)
				}
				if after == "format-restored" {
					if err := x.rdb.Set(context.Background(), "setting", `{"UUID":"selected-volume","BlockSize":4096}`, 0).Err(); err != nil {
						t.Fatal(err)
					}
				} else {
					<-ctx.Done()
				}
				return proof, err
			}
			if after == "deadline-expired" {
				got := make([]byte, 4096)
				n, err := cf.ReadAt(got, 0)
				if n != 0 || (!errors.Is(err, syscall.EAGAIN) && !errors.Is(err, cache.ErrVolumeBinding)) || cf.fuseFD != nil || !bytes.Equal(got, make([]byte, len(got))) {
					t.Fatalf("expired proof exposed bytes: %d %v", n, err)
				}
				select {
				case <-x.jfs.handler.beginBoundCacheStop():
				case <-time.After(time.Second):
					t.Fatal("proof owner failed to finish")
				}
			} else {
				assertBoundCacheRefusal(t, cf, cache.ErrVolumeBinding)
			}
			assertBoundCacheRefusal(t, cf, cache.ErrVolumeBinding)
			if calls != 1 || !b.identityRefused.Load() {
				t.Fatalf("refusal not retained: calls=%d", calls)
			}
		})
	}
}

func TestBoundCacheProofWindowReplacementNeverFallsThrough(t *testing.T) {
	for _, which := range []string{"leaf", "ancestor", "leaf-with-proof-error"} {
		t.Run(which, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			cf := openBoundCacheFile(t, x)
			b := x.jfs.handler.cacheBinding.Load()
			actual := b.readProof
			b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
				proof, err := actual(ctx, uuid, name, ids)
				if err != nil {
					t.Fatal(err)
				}
				changed := "media/clip.mov"
				if which == "ancestor" {
					changed = "media"
				}
				replaceBoundCacheEntry(t, x, changed)
				if which == "leaf-with-proof-error" {
					return metadata.RedisPathProof{}, context.DeadlineExceeded
				}
				return proof, err
			}
			assertBoundCacheRefusal(t, cf, syscall.EAGAIN)
		})
	}
}

func TestBoundCacheOpenSnapshotReplacementNeverFallsThrough(t *testing.T) {
	for _, which := range []string{"known", "unknown-precision", "local-only"} {
		t.Run(which, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			x.jfs.handler.testCacheBeforeIdentity = func(name string) {
				replaceBoundCacheEntry(t, x, name)
				if which != "known" {
					e := x.store.LookupByPath(name)
					if which == "unknown-precision" {
						e.MtimeNsecKnown = false
					} else {
						e.LocalOnly = true
					}
					if err := x.store.Insert(e); err != nil {
						t.Fatal(err)
					}
				}
			}
			cf := openBoundCacheFile(t, x)
			x.jfs.handler.testCacheBeforeIdentity = nil
			assertBoundCacheRefusal(t, cf, syscall.EAGAIN)
		})
	}
}

func TestBoundCacheRefusalCoversBackendReadRoutes(t *testing.T) {
	for _, route := range []string{"known-row-no-reader", "unknown-row", "retained-uncached"} {
		t.Run(route, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			h := x.jfs.handler
			unknown := "unknown.mov"
			if err := os.WriteFile(filepath.Join(x.fuseRoot, unknown), []byte("foreign"), 0600); err != nil {
				t.Fatal(err)
			}
			var retained *billyFile
			if route == "retained-uncached" {
				f, err := x.jfs.OpenFile(unknown, os.O_RDONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				var ok bool
				retained, ok = f.(*billyFile)
				if !ok {
					t.Fatalf("unexpected uncached type %T", f)
				}
				defer retained.Close()
			}
			h.SetCacheBackendStatus(false, cache.ErrVolumeBinding)
			h.SetCacheReader(nil)
			if retained != nil {
				for _, positioned := range []bool{false, true} {
					got := make([]byte, 7)
					var n int
					var err error
					if positioned {
						n, err = retained.ReadAt(got, 0)
					} else {
						n, err = retained.Read(got)
					}
					if n != 0 || !errors.Is(err, cache.ErrVolumeBinding) || !bytes.Equal(got, make([]byte, len(got))) {
						t.Fatalf("retained refusal = %d %v", n, err)
					}
				}
				return
			}
			name := unknown
			if route == "known-row-no-reader" {
				name = "media/clip.mov"
			}
			f, err := x.jfs.OpenFile(name, os.O_RDONLY, 0)
			if f != nil {
				_ = f.Close()
			}
			if f != nil || !errors.Is(err, cache.ErrVolumeBinding) {
				t.Fatalf("open after hard refusal = %T %v", f, err)
			}
		})
	}
}

func TestBoundCacheHardRefusalPreservesQualifiedLocalRead(t *testing.T) {
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	got := make([]byte, len(x.want))
	if _, err := cf.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	x.jfs.handler.cacheBinding.Load().refuseIdentity(cache.ErrVolumeBinding)
	clear(got)
	if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, x.want) || cf.fuseFD != nil {
		t.Fatalf("selected local hit refused: %d %v", n, err)
	}
}

func TestBoundCacheReaderMissAfterAncestorReplacementNeverFallsThrough(t *testing.T) {
	x := newBoundCacheFixture(t)
	cf := openBoundCacheFile(t, x)
	if _, err := cf.ReadAt(make([]byte, 4096), 0); err != nil {
		t.Fatal(err)
	}
	b := x.jfs.handler.cacheBinding.Load()
	b.beforeCacheRead = func() {
		replaceBoundCacheEntry(t, x, "media")
		chunks, err := cache.VolumeCacheDir("selected-volume")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(chunks, "0/0/77_0_65536")); err != nil {
			t.Fatal(err)
		}
	}
	assertBoundCacheRefusal(t, cf, syscall.EAGAIN)
}

func TestBoundCacheInvalidFormatTypeDoesNotBecomePathMiss(t *testing.T) {
	for _, key := range []string{"setting", "d1"} {
		t.Run(key, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			cf := openBoundCacheFile(t, x)
			if err := x.rdb.Del(context.Background(), key).Err(); err != nil {
				t.Fatal(err)
			}
			if key == "setting" {
				if err := x.rdb.HSet(context.Background(), key, "UUID", "selected-volume").Err(); err != nil {
					t.Fatal(err)
				}
				assertBoundCacheRefusal(t, cf, cache.ErrVolumeBinding)
			} else {
				if err := x.rdb.Set(context.Background(), key, "wrong-type", 0).Err(); err != nil {
					t.Fatal(err)
				}
				got := make([]byte, 4096)
				if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, bytes.Repeat([]byte{'F'}, len(got))) {
					t.Fatalf("path-only refusal = %d %v", n, err)
				}
				if x.jfs.handler.cacheBinding.Load().identityRefused.Load() {
					t.Fatal("path wrongtype latched format refusal")
				}
			}
		})
	}
}

func TestBoundCacheQualificationDeadlineRetainsActualWork(t *testing.T) {
	x := newBoundCacheFixture(t)
	b := x.jfs.handler.cacheBinding.Load()
	actual := b.readProof
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		proof, err := actual(ctx, uuid, name, ids)
		close(entered)
		<-release
		return proof, err
	}
	id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
	result := make(chan error, 1)
	go func() {
		_, err := x.jfs.handler.readCacheBlock(context.Background(), x.session.Reader(), id, id.Inode, 0, make([]byte, 4096), 50*time.Millisecond)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("proof did not enter")
	}
	select {
	case err := <-result:
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("deadline = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request waited for physical work")
	}
	b.workMu.Lock()
	active := b.workActive
	b.workMu.Unlock()
	if active != 1 {
		t.Fatalf("timed-out actual work lost ownership: %d", active)
	}
	if x.jfs.handler.stopHandlerWithDeadline(20 * time.Millisecond) {
		t.Fatal("shutdown crossed running qualification")
	}
	if _, ok := x.store.LookupCachedFileIdentity(id.Path); !ok {
		t.Fatal("store retired while job held")
	}
	releaseOnce.Do(func() { close(release) })
	if !x.jfs.handler.StopHandler() {
		t.Fatal("shutdown failed after real work finished")
	}
	b.workMu.Lock()
	active = b.workActive
	b.workMu.Unlock()
	if active != 0 {
		t.Fatalf("shutdown leaked work: %d", active)
	}
}
