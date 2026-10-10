//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// ReconcileAncestors invokes the production root and media Redis reconciliation
// path. Its return alone is insufficient: it can log and skip an inner dir
// failure, so both visibility generations must advance in this test.
func reconcileOwnedPinAncestors(t *testing.T, x *boundCacheFixture) {
	t.Helper()
	rootBefore := x.store.DirectoryVisibilityMtime(".", time.Time{})
	mediaBefore := x.store.DirectoryVisibilityMtime("media", time.Time{})
	if err := x.rc.ReconcileAncestors("media/clip.mov"); err != nil {
		t.Fatal(err)
	}
	rootAfter := x.store.DirectoryVisibilityMtime(".", time.Time{})
	mediaAfter := x.store.DirectoryVisibilityMtime("media", time.Time{})
	if !rootAfter.After(rootBefore) || !mediaAfter.After(mediaBefore) {
		t.Fatalf("real root/media reconcile not observed: root %v -> %v, media %v -> %v",
			rootBefore, rootAfter, mediaBefore, mediaAfter)
	}
}

func TestOwnedPinAcquisitionDuringRealDirectoryRefresh(t *testing.T) {
	for _, changedAncestor := range []bool{false, true} {
		name := "unchanged-root-and-media"
		if changedAncestor {
			name = "changed-media-ancestor"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("JM_PIN_WARM_METADATA", "1")
			x, pins, _, provider, _, _, _, _ := ownedPinThreeChunkFixture(t)
			// Normalize the manually seeded fixture with the actual Redis path
			// before Capture, then exercise that same path during acquisition.
			reconcileOwnedPinAncestors(t, x)
			const leafPath = "media/clip.mov"
			leafBefore, ok := x.store.LookupCachedFileIdentity(leafPath)
			if !ok {
				t.Fatal("normalized leaf missing")
			}
			before, err := x.paths.CaptureCachePath(leafPath)
			if err != nil {
				t.Fatal(err)
			}
			entry, ok := pins.Get("/volume/" + leafPath)
			if !ok {
				t.Fatal("pin missing")
			}
			claimed, ok, err := pins.ClaimPrefetch(*entry)
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
			if !job.SourceCurrent() || !reflect.DeepEqual(job.observed.Chain, before.Chain) {
				t.Fatal("captured producer did not retain normalized full ancestry")
			}
			if changedAncestor {
				// Change only the authoritative media directory mtime. The
				// real root reconcile must publish that new ancestor identity.
				attr := (&meta.Attr{Typ: meta.TypeDirectory, Parent: 1,
					Mtime: 1700000000, Mtimensec: 124}).Marshal()
				if err := x.rdb.Set(ctx, "i2", attr, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			reconcileOwnedPinAncestors(t, x)
			leafAfter, ok := x.store.LookupCachedFileIdentity(leafPath)
			if !ok || leafAfter != leafBefore {
				t.Fatalf("reconcile changed leaf identity: before=%+v after=%+v found=%v",
					leafBefore, leafAfter, ok)
			}
			after, err := x.paths.CaptureCachePath(leafPath)
			if err != nil {
				t.Fatal(err)
			}
			if changedAncestor {
				if reflect.DeepEqual(after.Chain, before.Chain) ||
					!reflect.DeepEqual(after.Chain[len(after.Chain)-1], before.Chain[len(before.Chain)-1]) ||
					job.SourceCurrent() || x.paths.CachePathObservationCurrent(job.observed) {
					t.Fatal("actual ancestor change failed to revoke captured path authority")
				}
			} else if !reflect.DeepEqual(after.Chain, before.Chain) ||
				!job.SourceCurrent() || !x.paths.CachePathObservationCurrent(job.observed) {
				t.Fatal("same-identity real refresh revoked captured path authority")
			}

			source, err := os.Open(filepath.Join(x.fuseRoot, leafPath))
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.New()
			n, readErr := io.Copy(hash, source)
			closeErr := source.Close()
			if readErr != nil || closeErr != nil || n != claimed.Size {
				t.Fatalf("source read n=%d err=%v close=%v", n, readErr, closeErr)
			}
			var digest [sha256.Size]byte
			copy(digest[:], hash.Sum(nil))
			authority, err := job.Complete(ctx, digest)
			if changedAncestor {
				if err == nil || !errors.Is(err, pin.ErrAuthorityUnavailable) ||
					len(authority) != 0 || pins.IsPinnedReady(claimed.Path) {
					t.Fatalf("changed ancestor admitted Complete: authority=%d err=%v", len(authority), err)
				}
				return
			}
			if err != nil || len(authority) == 0 || !job.SourceCurrent() {
				t.Fatalf("unchanged refresh failed Complete: authority=%d err=%v", len(authority), err)
			}
			changed, err := pins.CompleteReady(claimed, authority)
			if err != nil || !changed {
				t.Fatalf("durable candidate=%v err=%v", changed, err)
			}
			claimed.Authority = authority
			admitted, err := job.Finalize(ctx, claimed)
			if err != nil || !admitted || !pins.IsPinnedReady(claimed.Path) {
				t.Fatalf("unchanged refresh Finalize=%v err=%v ready=%v", admitted, err, pins.IsPinnedReady(claimed.Path))
			}
		})
	}
}
