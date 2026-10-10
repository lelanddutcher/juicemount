//go:build redisoptionsacceptance

package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// The slow-link preview worker is serial. This establishes whether a target
// admitted behind an earlier video can still make its own foreground proof,
// and whether the completed jobs leave the target locally readable afterward.
func TestSlowPreviewQueueDoesNotBlockTargetForegroundProof(t *testing.T) {
	x, _ := slowMetadataFixture(t, true)
	h := x.jfs.handler
	b := h.cacheBinding.Load()
	target, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
	if !ok {
		t.Fatal("missing target identity")
	}
	mtime := time.Unix(1700000000, 123)
	earlierPath := filepath.Join(x.fuseRoot, "media/a.mov")
	if err := os.WriteFile(earlierPath, bytes.Repeat([]byte{'F'}, len(x.want)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(earlierPath, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(earlierPath)
	if err != nil {
		t.Fatal(err)
	}
	earlierInode := stat.Sys().(*syscall.Stat_t).Ino
	e := metadata.MakeEntry("media/a.mov", false, int64(len(x.want)), mtime, earlierInode)
	e.MtimeNsecKnown = true
	if err := x.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	// Both files point to the fixture's real CRC-backed slice. Their path
	// identities and mapping keys are distinct, while disk bytes are resident.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records, err := x.rdb.LRange(ctx, "c"+strconv.FormatUint(target.Inode, 10)+"_0", 0, -1).Result()
	if err != nil || len(records) != 1 {
		t.Fatalf("target fixture mapping count=%d err=%v", len(records), err)
	}
	var edge [9]byte
	edge[0] = meta.TypeFile
	binary.BigEndian.PutUint64(edge[1:], earlierInode)
	pipe := x.rdb.Pipeline()
	pipe.HSet(ctx, "d2", "a.mov", edge[:])
	pipe.Set(ctx, "i"+strconv.FormatUint(earlierInode, 10), (&meta.Attr{Typ: meta.TypeFile, Parent: 2, Length: uint64(len(x.want)), Mtime: 1700000000, Mtimensec: 123}).Marshal(), 0)
	pipe.RPush(ctx, "c"+strconv.FormatUint(earlierInode, 10)+"_0", records[0])
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	earlier, ok := x.store.LookupCachedFileIdentity("media/a.mov")
	if !ok || earlier.Inode != earlierInode {
		t.Fatal("earlier video identity not resident in the mirror")
	}
	binding, bound := x.session.Reader().VolumeBinding()
	if !bound {
		t.Fatal("missing selected binding")
	}
	selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
	if x.paths.CachePathEligible(selected, earlier) || x.paths.CachePathEligible(selected, target) {
		t.Fatal("video path unexpectedly qualified before ReadDir")
	}
	guard := &denyPreparedRemote{}
	client := redis.NewClient(x.rdb.Options())
	client.AddHook(guard)
	reader, err := cache.NewVolumeReader(binding, client)
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	h.SetCacheReader(reader)
	t.Cleanup(func() { h.StopHandler(); reader.Stop(); _ = client.Close() })
	for _, id := range []metadata.CachedFileIdentity{earlier, target} {
		if _, err := reader.ReadResidentRange(id.Inode, 0, make([]byte, 4096), cache.ResidentAttr{Size: id.Size, MtimeNS: id.MtimeNS, Valid: true}); !errors.Is(err, cache.ErrCacheMiss) {
			t.Fatalf("%s mapping unexpectedly resident before listing: %v", id.Path, err)
		}
	}
	jobs := make(chan previewJob, 2)
	h.previewFinished = func(job previewJob) { jobs <- job }
	enteredEarlier := make(chan struct{}, 1)
	enteredTarget := make(chan struct{}, 1)
	releaseEarlier := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseEarlier) })
	var earlierProofs, targetProofs, refusedProofs atomic.Int64
	var earlierActive, targetEnteredDuringEarlier atomic.Bool
	var refuseProof atomic.Bool
	originalProof := b.readProof
	b.readProof = func(ctx context.Context, uuid, path string, ids []uint64) (metadata.RedisPathProof, error) {
		if refuseProof.Load() {
			refusedProofs.Add(1)
			return metadata.RedisPathProof{}, errors.New("test-owned path proof refusal")
		}
		switch path {
		case "media/a.mov":
			earlierProofs.Add(1)
			earlierActive.Store(true)
			defer earlierActive.Store(false)
			select {
			case enteredEarlier <- struct{}{}:
			default:
			}
			select {
			case <-releaseEarlier:
			case <-ctx.Done():
				return metadata.RedisPathProof{}, ctx.Err()
			}
		case "media/clip.mov":
			targetProofs.Add(1)
			targetEnteredDuringEarlier.Store(earlierActive.Load())
			select {
			case enteredTarget <- struct{}{}:
			default:
			}
		}
		return originalProof(ctx, uuid, path, ids)
	}
	infos := p86ReadDirPrompt(t, x)
	if len(infos) != 2 || infos[0].Name() != "a.mov" || infos[1].Name() != "clip.mov" {
		t.Fatalf("name-sorted real listing = %v", infos)
	}
	select {
	case <-enteredEarlier:
	case <-time.After(2 * time.Second):
		t.Fatal("serial worker did not enter earlier video proof")
	}
	rm := h.readahead
	wantTargetKey := previewKey{identity: target, binding: binding, mode: previewMetadataOnly}
	rm.mu.Lock()
	_, targetSeen := rm.previewSeen[wantTargetKey]
	queued, seen := len(rm.previewQueue), len(rm.previewSeen)
	rm.mu.Unlock()
	if !targetSeen || queued != 1 || seen != 2 {
		t.Fatalf("target exact preview key was not queued behind earlier video: seen=%t queue=%d dedup=%d", targetSeen, queued, seen)
	}
	if x.paths.CachePathEligible(selected, target) {
		t.Fatal("target path already qualified before foreground read")
	}
	cf := openBoundCacheFile(t, x)
	got := make([]byte, 4096)
	if n, err := cf.ReadAt(got, 0); n != len(got) || err != nil || !bytes.Equal(got, x.want[:len(got)]) || cf.fuseFD != nil {
		t.Fatalf("target foreground read while earlier proof held: n=%d err=%v FUSE=%v", n, err, cf.fuseFD != nil)
	}
	select {
	case <-enteredTarget:
	default:
		t.Fatal("target foreground read did not enter its own distinct path proof")
	}
	if earlierProofs.Load() != 1 || targetProofs.Load() != 1 {
		t.Fatalf("overlap proof counts earlier=%d target=%d", earlierProofs.Load(), targetProofs.Load())
	}
	rm.mu.Lock()
	queueStillHeld := len(rm.previewQueue)
	rm.mu.Unlock()
	if !targetEnteredDuringEarlier.Load() || !earlierActive.Load() || queueStillHeld != 1 {
		t.Fatalf("foreground proof did not overlap the physically held earlier job: at_entry=%t active=%t target_queue=%d", targetEnteredDuringEarlier.Load(), earlierActive.Load(), queueStillHeld)
	}
	releaseOnce.Do(func() { close(releaseEarlier) })
	for i, wantPath := range []string{"media/a.mov", "media/clip.mov"} {
		select {
		case job := <-jobs:
			if job.key.identity.Path != wantPath || job.key.mode != previewMetadataOnly {
				t.Fatalf("completed job %d = %+v, want %s metadata", i, job, wantPath)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("preview job %s did not join", wantPath)
		}
	}
	if !x.paths.CachePathEligible(selected, target) {
		t.Fatal("target path lost its foreground qualification")
	}
	guard.armed.Store(true)
	refuseProof.Store(true)
	fresh := openBoundCacheFile(t, x)
	clear(got)
	if n, err := fresh.ReadAt(got, 0); n != len(got) || err != nil || !bytes.Equal(got, x.want[:len(got)]) || fresh.fuseFD != nil {
		t.Fatalf("completed target local read: n=%d err=%v FUSE=%v", n, err, fresh.fuseFD != nil)
	}
	if refusedProofs.Load() != 0 || guard.calls.Load() != 0 || targetProofs.Load() != 1 {
		t.Fatalf("completed target requested remote proof/mapping: proof_refusals=%d mapping_refusals=%d target_proofs=%d", refusedProofs.Load(), guard.calls.Load(), targetProofs.Load())
	}
}
