//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/cache"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
	"github.com/lelanddutcher/juicemount/metadata"
	"github.com/redis/go-redis/v9"
)

func ownedPinPreparedJob(t *testing.T, x *boundCacheFixture, pins *pin.Store, provider *OwnedPinProvider) (*ownedPinJob, pin.Entry) {
	t.Helper()
	const canonical = "/volume/media/clip.mov"
	e, ok := pins.Get(canonical)
	if !ok {
		t.Fatal("missing pin")
	}
	claimed, ok, err := pins.ClaimPrefetch(*e)
	if err != nil || !ok {
		t.Fatalf("claim=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	owned, err := provider.Capture(ctx, claimed)
	if err != nil {
		t.Fatal(err)
	}
	job := owned.(*ownedPinJob)
	t.Cleanup(job.Release)
	source, err := os.Open(filepath.Join(x.fuseRoot, "media/clip.mov"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	if n, err := io.Copy(digest, source); err != nil || n != claimed.Size {
		_ = source.Close()
		t.Fatalf("source digest n=%d err=%v", n, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(nil))
	authority, err := job.Complete(ctx, sum)
	if err != nil || len(authority) == 0 || !job.current() {
		t.Fatalf("unchanged producer did not prepare authority: %v", err)
	}
	claimed.Authority = authority
	return job, claimed
}

func TestOwnedPinPublicationFencesEachMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *boundCacheFixture, uint64)
	}{
		{"unchanged", nil},
		{"leaf", func(t *testing.T, x *boundCacheFixture, _ uint64) { replaceBoundCacheEntry(t, x, "media/clip.mov") }},
		{"ancestor", func(t *testing.T, x *boundCacheFixture, _ uint64) {
			before, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
			if !ok {
				t.Fatal("missing leaf identity before ancestor mutation")
			}
			replaceBoundCacheEntry(t, x, "media")
			after, ok := x.store.LookupCachedFileIdentity("media/clip.mov")
			if !ok || after != before {
				t.Fatalf("ancestor mutation also changed leaf: before=%+v after=%+v found=%v", before, after, ok)
			}
		}},
		{"namespace-ABA", func(t *testing.T, x *boundCacheFixture, _ uint64) {
			op := x.jfs.handler.namespace.begin([]string{"media/clip.mov"})
			if op == nil {
				t.Fatal("namespace mutation not admitted")
			}
			op.blockReads()
			op.finish(nil)
		}},
		{"writer-ABA", func(_ *testing.T, x *boundCacheFixture, _ uint64) {
			x.jfs.handler.incActiveWriter("media/clip.mov")
			x.jfs.handler.decActiveWriter("media/clip.mov")
		}},
		{"mapping", func(_ *testing.T, x *boundCacheFixture, inode uint64) { x.session.Reader().InvalidateSliceCache(inode) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, pins, _, provider, inode, _, _, _ := ownedPinThreeChunkFixture(t)
			job, claimed := ownedPinPreparedJob(t, x, pins, provider)
			if tc.mutate != nil {
				tc.mutate(t, x, inode)
			}
			changed, err := pins.CompleteReady(claimed, claimed.Authority)
			if err != nil || !changed {
				t.Fatalf("durable candidate=%v err=%v", changed, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			admitted, err := job.Finalize(ctx, claimed)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mutate == nil {
				if !admitted || !pins.IsPinnedReady(claimed.Path) {
					t.Fatal("unchanged producer failed positive admission")
				}
				f := openBoundCacheFile(t, x)
				if f.pinGeneration != claimed.Generation {
					t.Fatal("fresh positive handle lost generation")
				}
				var b [4096]byte
				if n, err := f.ReadAt(b[:], 0); n != len(b) || err != nil || f.fuseFD != nil {
					t.Fatalf("positive local read n=%d err=%v FUSE=%v", n, err, f.fuseFD != nil)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			} else if admitted || pins.IsPinnedReady(claimed.Path) {
				t.Fatalf("%s published effective Ready after mutation", tc.name)
			}
		})
	}
}

func TestOwnedPinStaleFinalizerCannotRevokeSuccessor(t *testing.T) {
	x, pins, _, provider, _, _, _, _ := ownedPinThreeChunkFixture(t)
	oldJob, old := ownedPinPreparedJob(t, x, pins, provider)
	if changed, err := pins.CompleteReady(old, old.Authority); err != nil || !changed {
		t.Fatalf("old candidate=%v err=%v", changed, err)
	}
	if err := pins.Pin(old.Path, old.Size, "/volume"); err != nil {
		t.Fatal(err)
	}
	newJob, successor := ownedPinPreparedJob(t, x, pins, provider)
	if successor.Generation == old.Generation {
		t.Fatal("repin reused old generation")
	}
	if changed, err := pins.CompleteReady(successor, successor.Authority); err != nil || !changed {
		t.Fatalf("successor candidate=%v err=%v", changed, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if admitted, err := newJob.Finalize(ctx, successor); err != nil || !admitted || !pins.IsPinnedReady(old.Path) {
		t.Fatalf("successor admission=%v err=%v", admitted, err)
	}
	f := openBoundCacheFile(t, x)
	if f.pinGeneration != successor.Generation {
		t.Fatal("successor handle retained wrong generation")
	}
	var data [4096]byte
	if n, err := f.ReadAt(data[:], 0); n != len(data) || err != nil {
		t.Fatalf("successor read n=%d err=%v", n, err)
	}
	if admitted, err := oldJob.Finalize(ctx, old); err != nil || admitted {
		t.Fatalf("stale finalizer admission=%v err=%v", admitted, err)
	}
	if !pins.IsPinnedReady(old.Path) || f.pinGeneration != successor.Generation {
		t.Fatal("stale finalizer revoked successor")
	}
	provider.mu.RLock()
	_, staleLive := provider.live[old.Path+"\x00"+old.Generation]
	provider.mu.RUnlock()
	if staleLive {
		t.Fatal("stale finalizer retained old Reader authority")
	}
	if n, err := f.ReadAt(data[:], 0); n != len(data) || err != nil {
		t.Fatalf("successor read after stale finalizer n=%d err=%v", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// A durable Ready row is never by itself sufficient after a dependency is
// lost. Each subtest starts with a complete admitted producer, retires every
// owner, removes one persisted dependency, then opens a separate owner set.
func TestOwnedPinRestoreRefusesMissingDurableDependency(t *testing.T) {
	const canonical = "/volume/media/clip.mov"
	for _, missing := range []string{"mapping", "proof", "witness"} {
		t.Run(missing, func(t *testing.T) {
			x, pins, pf, _, _, _, _, pinDBPath := ownedPinThreeChunkFixture(t)
			e, ok := pins.Get(canonical)
			if !ok {
				t.Fatal("missing pin")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			if err := pf.EnqueueWait(ctx, *e); err != nil || !pins.IsPinnedReady(canonical) {
				t.Fatalf("positive producer err=%v ready=%v", err, pins.IsPinnedReady(canonical))
			}
			old := openBoundCacheFile(t, x)
			var control [4096]byte
			if n, err := old.ReadAt(control[:], 0); n != len(control) || err != nil || old.fuseFD != nil {
				t.Fatalf("positive local control n=%d err=%v FUSE=%v", n, err, old.fuseFD != nil)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot := x.session.Reader().MappingSnapshotPath(x.state)
			if !pf.StopWithin(5 * time.Second) {
				t.Fatal("old prefetcher did not join")
			}
			if !x.jfs.handler.StopHandler() {
				t.Fatal("old handler did not join")
			}
			x.session.Stop()
			x.rc.Stop()
			if err := x.rdb.Close(); err != nil {
				t.Fatal(err)
			}
			if err := x.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := pins.Close(); err != nil {
				t.Fatal(err)
			}

			switch missing {
			case "mapping":
				if _, err := os.Stat(snapshot); err != nil {
					t.Fatalf("positive snapshot absent before removal: %v", err)
				}
				if err := os.Remove(snapshot); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
					t.Fatalf("snapshot still present: %v", err)
				}
			case "proof", "witness":
				dbPath := x.db
				if missing == "witness" {
					dbPath = pinDBPath
				}
				db, err := sql.Open("sqlite", dbPath)
				if err != nil {
					t.Fatal(err)
				}
				if missing == "proof" {
					result, err := db.Exec(`DELETE FROM cache_path_eligibility WHERE path=?`, "media/clip.mov")
					if err != nil {
						_ = db.Close()
						t.Fatal(err)
					}
					if n, _ := result.RowsAffected(); n != 1 {
						_ = db.Close()
						t.Fatalf("removed %d path proofs", n)
					}
				} else {
					result, err := db.Exec(`UPDATE pinned_files SET authority=X'' WHERE path=?`, canonical)
					if err != nil {
						_ = db.Close()
						t.Fatal(err)
					}
					if n, _ := result.RowsAffected(); n != 1 {
						_ = db.Close()
						t.Fatalf("removed %d witnesses", n)
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}

			denyAddr, attempts := ownedPinRemoteRefusal(t)
			remoteBefore := attempts.Load()
			fuseBefore := fuseCellCounts(t)
			store, err := metadata.Open(x.db)
			if err != nil {
				t.Fatal(err)
			}
			var h *JuiceMountHandler
			var rc *metadata.RedisClient
			var session *cache.Session
			var reopened *pin.Store
			var restore *pin.Prefetcher
			t.Cleanup(func() {
				if restore != nil && !restore.StopWithin(5*time.Second) {
					t.Error("restore worker did not join")
				}
				if h != nil && !h.StopHandler() {
					t.Error("new handler did not join")
				}
				if session != nil {
					session.Stop()
				}
				if rc != nil {
					rc.Stop()
				}
				if reopened != nil {
					_ = reopened.Close()
				}
				_ = store.Close()
			})
			paths, err := metadata.OpenCachePathEligibility(store)
			if err != nil {
				t.Fatal(err)
			}
			h = NewHandler(store, x.fuseRoot)
			h.sidecar.enabled = false
			rc, err = metadata.NewRedisClientDeferred(x.raw, store)
			if err != nil {
				t.Fatal(err)
			}
			deny := &ownedPinDenyRedis{}
			rc.RawDB().AddHook(deny)
			controlCtx, controlCancel := context.WithTimeout(context.Background(), time.Second)
			defer controlCancel()
			if err := rc.RawDB().Ping(controlCtx).Err(); err == nil || deny.calls.Load() != 1 {
				t.Fatalf("command refusal control err=%v calls=%d", err, deny.calls.Load())
			}
			if _, err := rc.RawDB().Pipelined(controlCtx, func(pipe redis.Pipeliner) error { pipe.Ping(controlCtx); return nil }); err == nil || deny.calls.Load() != 2 {
				t.Fatalf("pipeline refusal control err=%v calls=%d", err, deny.calls.Load())
			}
			metadataBefore := deny.calls.Load()
			opts, err := redisurl.Parse(x.raw)
			if err != nil {
				t.Fatal(err)
			}
			opts.Addr = denyAddr
			session, err = cache.NewSession(cache.SessionConfig{Store: store, LogicalURL: x.raw, Options: opts, StateDir: x.state, VerifyReader: func(*cache.Reader) error { return nil }, OnReader: h.SetCacheReader, OnQualified: rc.SetBackendQualified, OnStatus: h.SetCacheBackendStatus})
			if err != nil {
				t.Fatal(err)
			}
			h.SetBoundCacheSession(session, rc, paths)
			h.SetCacheReader(session.Reader())
			reopened, err = pin.Open(pinDBPath)
			if err != nil {
				t.Fatal(err)
			}
			provider := NewOwnedPinProvider(h, reopened)
			if err := reopened.SetReadyValidator(provider.Current); err != nil {
				t.Fatal(err)
			}
			if err := reopened.SetReadyRevoker(provider.Revoke); err != nil {
				t.Fatal(err)
			}
			h.SetPinStore(reopened, "/volume")
			if reopened.IsPinnedReady(canonical) {
				t.Fatal("raw Ready row admitted before restoration")
			}
			restore = pin.NewPrefetcher(reopened, x.fuseRoot, "/volume", 1, provider)
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer restoreCancel()
			count, err := restore.RestoreReady(restoreCtx)
			if err != nil || count != 0 || reopened.IsPinnedReady(canonical) {
				t.Fatalf("%s restore count=%d err=%v ready=%v", missing, count, err, reopened.IsPinnedReady(canonical))
			}
			if _, admitted := reopened.ReadyAdmission(canonical); admitted {
				t.Fatal("missing dependency admitted owned handle")
			}
			aggregate, err := reopened.AggregateStats()
			if err != nil || aggregate.ReadyFiles != 0 || aggregate.CachedBytes != 0 {
				t.Fatalf("%s refused restore advertised cached availability: %+v err=%v", missing, aggregate, err)
			}
			roots, err := reopened.PinRoots()
			if err != nil || len(roots) != 1 || roots[0].ReadyFiles != 0 || roots[0].CachedBytes != 0 {
				t.Fatalf("%s refused restore advertised ready root: %+v err=%v", missing, roots, err)
			}
			if attempts.Load() != remoteBefore || deny.calls.Load() != metadataBefore {
				t.Fatalf("restore tried remote: connections=%d metadata=%d", attempts.Load()-remoteBefore, deny.calls.Load()-metadataBefore)
			}
			for key, after := range fuseCellCounts(t) {
				if strings.HasPrefix(key, "foreground/") && after.Calls != fuseBefore[key].Calls {
					t.Fatalf("restore used FUSE %s: %d", key, after.Calls-fuseBefore[key].Calls)
				}
			}
		})
	}
}
