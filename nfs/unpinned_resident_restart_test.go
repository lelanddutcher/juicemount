//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

// This composes ordinary path qualification and slice discovery with a fresh
// process-local owner set. The saved mapping and path proof are produced only
// by the first ordinary ReadAt and normal Session/Store shutdown.
func TestUnpinnedResidentOrdinaryReadRestartsOfflineWire(t *testing.T) {
	if memBufServeEnabled {
		t.Fatal("requires disabled memory buffer")
	}
	for _, removeMapping := range []bool{false, true} {
		name := "saved-mapping"
		if removeMapping {
			name = "removed-mapping-refuses"
		}
		t.Run(name, func(t *testing.T) {
			x := newBoundCacheFixture(t)
			oldReader := x.session.Reader()
			if oldReader == nil {
				t.Fatal("initial bound reader missing")
			}
			warm := openBoundCacheFile(t, x)
			assertPhaseRead(t, warm, 'B', false)
			binding, bound := oldReader.VolumeBinding()
			id, resident := x.store.LookupCachedFileIdentity("media/clip.mov")
			selected := metadata.CachePathBinding{Digest: binding.Backend, FormatUUID: binding.UUID}
			if !bound || !resident || !x.paths.CachePathEligible(selected, id) {
				t.Fatal("ordinary read did not establish selected local path proof")
			}
			if err := warm.Close(); err != nil {
				t.Fatal(err)
			}
			snapshotPath := oldReader.MappingSnapshotPath(x.state)
			if !x.jfs.handler.StopHandler() {
				t.Fatal("original handler did not join")
			}
			x.session.Stop() // Its owned final writer persists the observed map.
			x.rc.Stop()
			if err := x.rdb.Close(); err != nil {
				t.Fatal(err)
			}
			if err := x.store.Close(); err != nil {
				t.Fatal(err)
			}
			if info, err := os.Stat(snapshotPath); err != nil || info.Size() == 0 {
				t.Fatalf("normal shutdown did not save mapping: size=%v err=%v", info, err)
			}
			if removeMapping {
				if err := os.Remove(snapshotPath); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(snapshotPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("negative input still has mapping snapshot: %v", err)
				}
			}

			// Both refusal mechanisms have independent, observed PING controls.
			// Baselines follow their deliberate controls and precede Session
			// restoration plus the first wire read.
			denyAddr, remoteAttempts := ownedPinRemoteRefusal(t)
			var newStore *metadata.Store
			var newHandler *JuiceMountHandler
			var newRC *metadata.RedisClient
			var newSession *cache.Session
			var emptyPins *pin.Store
			t.Cleanup(func() {
				if newHandler != nil && !newHandler.StopHandler() {
					t.Error("reopened handler did not join")
				}
				if newSession != nil {
					newSession.Stop()
				}
				if newRC != nil {
					newRC.Stop()
				}
				if emptyPins != nil {
					_ = emptyPins.Close()
				}
				if newStore != nil {
					_ = newStore.Close()
				}
			})
			newStore, err := metadata.Open(x.db)
			if err != nil {
				t.Fatal(err)
			}
			newPaths, err := metadata.OpenCachePathEligibility(newStore)
			if err != nil {
				t.Fatal(err)
			}
			newHandler = NewHandler(newStore, x.fuseRoot)
			newHandler.sidecar.enabled = false
			newRC, err = metadata.NewRedisClientDeferred(x.raw, newStore)
			if err != nil {
				t.Fatal(err)
			}
			metadataDeny := &ownedPinDenyRedis{}
			newRC.RawDB().AddHook(metadataDeny)
			controlCtx, controlCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer controlCancel()
			if err := newRC.RawDB().Ping(controlCtx).Err(); err == nil || metadataDeny.calls.Load() == 0 {
				t.Fatalf("metadata command refusal control: err=%v calls=%d", err, metadataDeny.calls.Load())
			}
			commandCalls := metadataDeny.calls.Load()
			if _, err := newRC.RawDB().Pipelined(controlCtx, func(pipe redis.Pipeliner) error { pipe.Ping(controlCtx); return nil }); err == nil || metadataDeny.calls.Load() <= commandCalls {
				t.Fatalf("metadata pipeline refusal control: err=%v calls=%d", err, metadataDeny.calls.Load())
			}
			metadataBaseline, dialBaseline := metadataDeny.calls.Load(), remoteAttempts.Load()
			fuseBaseline := fuseCellCounts(t)
			opts, err := redisurl.Parse(x.raw)
			if err != nil {
				t.Fatal(err)
			}
			opts.Addr = denyAddr // LogicalURL and saved volume identity stay original.
			opts.MaxRetries = -1
			opts.ContextTimeoutEnabled = true
			opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = 200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond
			pin.SetOffline(true)
			// The fixture bypasses platform cache-directory verification, as
			// newBoundCacheFixture does; binding and path authority stay real.
			newSession, err = cache.NewSession(cache.SessionConfig{Store: newStore, LogicalURL: x.raw, Options: opts, StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil }, OnReader: newHandler.SetCacheReader, OnQualified: newRC.SetBackendQualified, OnStatus: newHandler.SetCacheBackendStatus})
			if err != nil {
				t.Fatal(err)
			}
			newHandler.SetBoundCacheSession(newSession, newRC, newPaths)
			newHandler.SetCacheReader(newSession.Reader())
			if newSession.Reader() == nil || newSession.Reader() == oldReader || newSession.Qualified() {
				t.Fatal("reopened offline reader reused old owner or claimed live qualification")
			}
			newID, ok := newStore.LookupCachedFileIdentity("media/clip.mov")
			if !ok || newID.Path != id.Path || newID.Inode != id.Inode || newID.Size != id.Size || newID.MtimeNS != id.MtimeNS || newID.Version != id.Version || !newPaths.CachePathEligible(selected, newID) {
				t.Fatal("durable local ancestry was not restored independently of Redis")
			}
			emptyPins, err = pin.Open(filepath.Join(t.TempDir(), "empty-pins.db"))
			if err != nil {
				t.Fatal(err)
			}
			newHandler.SetPinStore(emptyPins, "/private-volume")
			canonical := "/private-volume/media/clip.mov"
			if entry, exists := emptyPins.Get(canonical); exists || entry != nil || emptyPins.IsPinnedReady(canonical) {
				t.Fatal("ordinary file inherited pin authority")
			}
			if remoteAttempts.Load() != dialBaseline || metadataDeny.calls.Load() != metadataBaseline {
				t.Fatal("reopening local dependencies attempted remote metadata")
			}
			// The first content access on the fresh handler is the real NFS READ.
			client := localSidecarLoopback(t, newHandler)
			ssdBefore := metrics.Default().Snapshot()
			if !removeMapping {
				residentWireRead(t, client, newHandler, 0, 4096, x.want[:4096], 0)
				if delta := metrics.Default().Snapshot().DirectSSDCacheHit - ssdBefore.DirectSSDCacheHit; delta != 1 {
					t.Fatalf("fresh ordinary READ verified SSD hits=%d, want one", delta)
				}
			} else {
				pending, err := client.send(100003, 6, latencyReadArgs{Handle: newHandler.ToHandle(nil, []string{"media", "clip.mov"}), Count: 4096})
				if err != nil {
					t.Fatal(err)
				}
				reply, err := pending.await(time.Now().Add(3 * time.Second))
				if err != nil || reply.err != nil || len(reply.body) < 4 {
					t.Fatalf("negative READ reply err=%v rpc=%v length=%d", err, reply.err, len(reply.body))
				}
				if status := binary.BigEndian.Uint32(reply.body[:4]); status != uint32(nfslib.NFSStatusNXIO) {
					t.Fatalf("missing local mapping NFS status=%d, want NXIO", status)
				}
				if delta := metrics.Default().Snapshot().DirectSSDCacheHit - ssdBefore.DirectSSDCacheHit; delta != 0 {
					t.Fatalf("missing mapping reported verified SSD hit=%d", delta)
				}
			}
			for key, after := range fuseCellCounts(t) {
				if strings.HasPrefix(key, "foreground/") && after.Calls != fuseBaseline[key].Calls {
					t.Fatalf("fresh offline READ used foreground FUSE %s: %d calls", key, after.Calls-fuseBaseline[key].Calls)
				}
			}
			if remoteAttempts.Load() != dialBaseline || metadataDeny.calls.Load() != metadataBaseline {
				t.Fatalf("fresh offline READ attempted remote metadata: dials=%d commands=%d", remoteAttempts.Load()-dialBaseline, metadataDeny.calls.Load()-metadataBaseline)
			}
		})
	}
}
