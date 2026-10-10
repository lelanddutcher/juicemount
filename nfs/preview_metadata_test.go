//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

type denyPreparedRemote struct {
	armed        atomic.Bool
	calls        atomic.Int32
	mu           sync.Mutex
	preparedKeys []string
}

func (h *denyPreparedRemote) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *denyPreparedRemote) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *denyPreparedRemote) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "eval" && len(cmd.Args()) > 1 && strings.Contains(fmt.Sprint(cmd.Args()[1]), "JM_CACHE_PREPARE_LIMIT") {
			for _, arg := range cmd.Args() {
				key := fmt.Sprint(arg)
				if strings.HasPrefix(key, "c") && strings.Contains(key, "_") {
					h.mu.Lock()
					h.preparedKeys = append(h.preparedKeys, key)
					h.mu.Unlock()
					break
				}
			}
		}
		if h.armed.Load() && cmd.Name() == "eval" {
			h.calls.Add(1)
			return errors.New("test-owned remote mapping refusal")
		}
		return next(ctx, cmd)
	}
}

func slowMetadataFixture(t *testing.T, enabled bool) (*boundCacheFixture, <-chan previewJob) {
	t.Helper()
	t.Setenv("JM_JFS_MAX_READAHEAD", "1M")
	x := newBoundCacheFixture(t, WithNativePreviewWarm(&enabled))
	// Keep the original's real inode and mtime aligned with the mirrored entry.
	// Otherwise a content warmer can reject it before any source-read hook runs.
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	mtime := time.Unix(1700000000, 123)
	if err := os.Chtimes(backing, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(backing)
	if err != nil {
		t.Fatal(err)
	}
	inode := fi.Sys().(*syscall.Stat_t).Ino
	e := metadata.MakeEntry("media/clip.mov", false, int64(len(x.want)), mtime, inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	var edge [9]byte
	edge[0] = byte(meta.TypeFile)
	binary.BigEndian.PutUint64(edge[1:], inode)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "clip.mov", edge[:])
	pipe.Set(ctx, "i"+strconv.FormatUint(inode, 10), (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(len(x.want)), Mtime: 1700000000, Mtimensec: 123}).Marshal(), 0)
	pipe.Rename(ctx, "c3_0", "c"+strconv.FormatUint(inode, 10)+"_0")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	h := x.jfs.handler
	h.sidecar.enabled = false
	h.prefetchMu.Lock()
	h.prefetched["media"] = time.Now()
	h.prefetchMu.Unlock()
	profile := netprofile.New()
	profile.ObserveRTT(100 * time.Millisecond)
	h.readahead.profile = profile
	done := make(chan previewJob, 1)
	h.previewReadAt = func(*os.File, []byte, int64) (int, error) {
		t.Error("metadata-only preparation opened the original")
		return 0, errors.New("unexpected original read")
	}
	h.previewFinished = func(job previewJob) { done <- job }
	return x, done
}

func TestSlowPreviewMetadataPreparesResidentReadWithoutOriginal(t *testing.T) {
	x, done := slowMetadataFixture(t, true)
	h := x.jfs.handler
	reader := x.session.Reader()
	binding, _ := reader.VolumeBinding()
	remoteGuard := &denyPreparedRemote{}
	client := redis.NewClient(x.rdb.Options())
	client.AddHook(remoteGuard)
	reader, err := cache.NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	h.SetCacheReader(reader)
	t.Cleanup(func() { h.StopHandler(); reader.Stop(); _ = client.Close() })
	id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	if x.paths.CachePathEligible(selected, id) {
		t.Fatal("path unexpectedly prequalified")
	}
	if _, err := reader.ReadResidentRange(id.Inode, 0, make([]byte, 4096), cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); !errors.Is(err, cache.ErrCacheMiss) {
		t.Fatalf("mapping unexpectedly prewarmed: %v", err)
	}
	infos := p86ReadDirPrompt(t, x)
	if len(infos) != 1 || infos[0].Name() != "clip.mov" {
		t.Fatalf("mirror listing = %v", infos)
	}
	select {
	case job := <-done:
		if job.key.mode != previewMetadataOnly || job.key.identity != id {
			t.Fatalf("job=%+v", job)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("metadata worker did not join")
	}
	if !x.paths.CachePathEligible(selected, id) {
		t.Fatal("ancestry not qualified")
	}
	if _, err := reader.ReadResidentRange(id.Inode, 0, make([]byte, 4096), cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); err != nil {
		t.Fatalf("first chunk mapping not resident: %v", err)
	}
	remoteGuard.armed.Store(true)
	b := h.cacheBinding.Load()
	b.readProof = func(context.Context, string, string, []uint64) (metadata.RedisPathProof, error) {
		t.Error("foreground requested remote ancestry proof")
		return metadata.RedisPathProof{}, errors.New("test-owned proof refusal")
	}
	cf := openBoundCacheFile(t, x)
	got := make([]byte, 4096)
	if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, x.want[:len(got)]) || cf.fuseFD != nil {
		t.Fatalf("offline foreground resident read = %d %v fd=%v", n, err, cf.fuseFD)
	}
	if len(h.readahead.previewQueue) != 0 {
		t.Fatal("metadata-only job left queued source work")
	}
	if got := remoteGuard.calls.Load(); got != 0 {
		t.Fatalf("foreground requested %d Redis mappings", got)
	}
}

func TestSlowPreviewMetadataDoesNotFetchMissingContent(t *testing.T) {
	x, done := slowMetadataFixture(t, true)
	blockDir, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(blockDir, "0/0/77_0_65536")); err != nil {
		t.Fatal(err)
	}
	_ = p86ReadDirPrompt(t, x)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata worker did not join")
	}
	x.session.SetOnline(false) // hot proof/map remain; no new Redis authority
	cf := openBoundCacheFile(t, x)
	got := make([]byte, 4096)
	if n, err := cf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, bytes.Repeat([]byte{'F'}, len(got))) || cf.fuseFD == nil {
		t.Fatalf("missing-content negative control = %d %v fd=%v", n, err, cf.fuseFD)
	}
}

func TestSlowPreviewMetadataRequiresOptInAndMeasuredNonMeteredLink(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		rtt     time.Duration
		metered bool
	}{
		{"default-off", false, 100 * time.Millisecond, false},
		{"unknown", true, 0, false},
		{"metered", true, 100 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, done := slowMetadataFixture(t, tc.enabled)
			profile := netprofile.New()
			if tc.rtt != 0 {
				profile.ObserveRTT(tc.rtt)
			}
			if tc.metered {
				c := netprofile.ClassMetered
				profile.ForceClass(&c)
			}
			x.jfs.handler.readahead.profile = profile
			_ = p86ReadDirPrompt(t, x)
			select {
			case <-done:
				t.Fatal("ineligible metadata job ran")
			default:
			}
			if seen, queued := p93SchedulerState(x.jfs.handler.readahead); seen != 0 || queued != 0 {
				t.Fatalf("admitted ineligible work: %d %d", seen, queued)
			}
		})
	}
}

func TestSlowPreviewMetadataRejectsCapturedUnknownMode(t *testing.T) {
	x, _ := slowMetadataFixture(t, true)
	rm := x.jfs.handler.readahead
	if rm.admitPreview(previewJob{key: previewKey{mode: previewNone}}) {
		t.Fatal("unknown captured mode admitted after link reclassification")
	}
}

func TestSlowPreviewMetadataPreparesFirstAndTerminalChunk(t *testing.T) {
	x, done := slowMetadataFixture(t, true)
	h := x.jfs.handler
	binding, _ := x.session.Reader().VolumeBinding()
	remoteGuard := &denyPreparedRemote{}
	client := redis.NewClient(x.rdb.Options())
	client.AddHook(remoteGuard)
	reader, err := cache.NewVolumeReader(binding, client)
	if err != nil {
		t.Fatal(err)
	}
	h.SetCacheReader(reader)
	t.Cleanup(func() { h.StopHandler(); reader.Stop(); _ = client.Close() })
	old, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok {
		t.Fatal("missing original identity")
	}
	const largeSize = 3*cache.ChunkSize + 4096
	backing := filepath.Join(x.fuseRoot, "media/clip.mov")
	if err := os.Truncate(backing, largeSize); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1700000000, 123)
	if err := os.Chtimes(backing, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry("media/clip.mov", false, largeSize, mtime, old.Inode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	var record [24]byte
	binary.BigEndian.PutUint64(record[4:12], 88)
	binary.BigEndian.PutUint32(record[12:16], 4096)
	binary.BigEndian.PutUint32(record[20:24], 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe := x.rdb.Pipeline()
	pipe.Set(ctx, "i"+strconv.FormatUint(old.Inode, 10), (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(largeSize), Mtime: 1700000000, Mtimensec: 123}).Marshal(), 0)
	pipe.RPush(ctx, "c"+strconv.FormatUint(old.Inode, 10)+"_3", string(record[:]))
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	_ = p86ReadDirPrompt(t, x)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata worker did not join")
	}
	id, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok || id.Size != largeSize {
		t.Fatalf("large identity = %+v", id)
	}
	expected := cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}
	if _, err := reader.ReadResidentRange(id.Inode, 0, make([]byte, 4096), expected); err != nil {
		t.Fatalf("first mapping: %v", err)
	}
	if _, err := reader.ResidentEOF(id.Inode, expected); err != nil {
		t.Fatalf("terminal mapping: %v", err)
	}
	if len(h.readahead.previewQueue) != 0 {
		t.Fatal("extra mapping work queued")
	}
	remoteGuard.mu.Lock()
	keys := append([]string(nil), remoteGuard.preparedKeys...)
	remoteGuard.mu.Unlock()
	wantKeys := []string{"c" + strconv.FormatUint(id.Inode, 10) + "_0", "c" + strconv.FormatUint(id.Inode, 10) + "_3"}
	if len(keys) != 2 || keys[0] != wantKeys[0] || keys[1] != wantKeys[1] {
		t.Fatalf("bounded first/terminal requests = %v; want %v", keys, wantKeys)
	}
}

func TestSlowPreviewMetadataRechecksAuthorityAfterProof(t *testing.T) {
	for _, change := range []string{"writer", "session", "stop"} {
		t.Run(change, func(t *testing.T) {
			x, done := slowMetadataFixture(t, true)
			h := x.jfs.handler
			b := h.cacheBinding.Load()
			original := b.readProof
			called := make(chan struct{}, 1)
			b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
				proof, err := original(ctx, uuid, path, ids)
				if change == "writer" {
					h.activeWritersMu.Lock()
					h.activeWriters[path] = 1
					h.activeWritersMu.Unlock()
				} else if change == "session" {
					x.session.SetOnline(false)
				} else {
					h.readahead.beginPreviewStop()
				}
				called <- struct{}{}
				return proof, err
			}
			_ = p86ReadDirPrompt(t, x)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("metadata worker did not join")
			}
			select {
			case <-called:
			default:
				t.Fatal("proof mutation was not exercised")
			}
			id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
			if _, err := x.session.Reader().ResidentEOF(id.Inode, cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); !errors.Is(err, cache.ErrCacheMiss) {
				t.Fatalf("mapping published after %s change: %v", change, err)
			}
		})
	}
}

func TestSlowPreviewCapturedMetadataNeverUpgradesToContent(t *testing.T) {
	x, done := slowMetadataFixture(t, true)
	h := x.jfs.handler
	chunks, err := cache.VolumeCacheDir("selected-volume")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(chunks, "0/0/77_0_65536")); err != nil {
		t.Fatal(err)
	}
	original := h.readahead.previewRun
	entered, release := make(chan previewJob, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	h.readahead.previewRun = func(job previewJob) { entered <- job; <-release; original(job) }
	_ = p86ReadDirPrompt(t, x)
	select {
	case job := <-entered:
		if job.key.mode != previewMetadataOnly {
			t.Fatalf("captured mode = %v", job.key.mode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued job did not start")
	}
	fast := netprofile.ClassFast
	h.readahead.profile.ForceClass(&fast)
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata worker did not join")
	}
	id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
	if _, err := x.session.Reader().ResidentEOF(id.Inode, cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); err != nil {
		t.Fatalf("captured metadata job did not prepare mapping: %v", err)
	}
}

func TestSlowPreviewCapturedContentDemotesBeforeStartAndProof(t *testing.T) {
	for _, transition := range []string{"before-start", "during-proof"} {
		t.Run(transition, func(t *testing.T) {
			x, done := slowMetadataFixture(t, true)
			h := x.jfs.handler
			chunks, err := cache.VolumeCacheDir("selected-volume")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(chunks, "0/0/77_0_65536")); err != nil {
				t.Fatal(err)
			}
			fast, slow := netprofile.ClassFast, netprofile.ClassSlow
			h.readahead.profile.ForceClass(&fast)
			if transition == "before-start" {
				original := h.readahead.previewRun
				entered, release := make(chan previewJob, 1), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				h.readahead.previewRun = func(job previewJob) { entered <- job; <-release; original(job) }
				_ = p86ReadDirPrompt(t, x)
				select {
				case job := <-entered:
					if job.key.mode != previewContent {
						t.Fatalf("captured mode = %v", job.key.mode)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("queued job did not start")
				}
				h.readahead.profile.ForceClass(&slow)
				once.Do(func() { close(release) })
			} else {
				b := h.cacheBinding.Load()
				original := b.readProof
				proofDone := make(chan struct{}, 1)
				b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
					proof, err := original(ctx, uuid, path, ids)
					h.readahead.profile.ForceClass(&slow)
					proofDone <- struct{}{}
					return proof, err
				}
				_ = p86ReadDirPrompt(t, x)
				select {
				case <-proofDone:
				case <-time.After(5 * time.Second):
					t.Fatal("proof transition was not exercised")
				}
			}
			select {
			case job := <-done:
				if job.key.mode != previewContent {
					t.Fatalf("captured mode = %v", job.key.mode)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("content worker did not join")
			}
			id, _ := x.store.LookupCachedFileIdentity("media/clip.mov")
			if _, err := x.session.Reader().ResidentEOF(id.Inode, cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); err != nil {
				t.Fatalf("demoted content job did not prepare mapping: %v", err)
			}
		})
	}
}
