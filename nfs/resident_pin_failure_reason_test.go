//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// This wrapper changes an actual captured authority after the prefetcher's
// final source proof, just before Complete. It does not replace Complete or
// manufacture the error that is saved to the pin store.
type pinStageProvider struct {
	*OwnedPinProvider
	afterSource func()
}

func (p *pinStageProvider) Capture(ctx context.Context, e pin.Entry) (pin.OwnedAuthorityJob, error) {
	job, err := p.OwnedPinProvider.Capture(ctx, e)
	if err != nil {
		return nil, err
	}
	return &pinStageJob{OwnedAuthorityJob: job, afterSource: p.afterSource}, nil
}

type pinStageJob struct {
	pin.OwnedAuthorityJob
	afterSource func()
	once        sync.Once
}

func (j *pinStageJob) SourceCurrent() bool {
	current := j.OwnedAuthorityJob.SourceCurrent()
	if current {
		j.once.Do(j.afterSource)
	}
	return current
}

func TestOwnedPinCompleteReasonRetainedAtFirstRejection(t *testing.T) {
	for _, tc := range []struct {
		name, stage string
		mutate      func(*JuiceMountHandler)
	}{
		{"reader-binding", "reader_binding", func(h *JuiceMountHandler) { h.SetCacheReader(nil) }},
		{"writer-generation", "current_before_mapping", func(h *JuiceMountHandler) {
			h.incActiveWriter("media/clip.mov")
			h.decActiveWriter("media/clip.mov")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, pins, _, provider, _, _, _, _ := ownedPinThreeChunkFixture(t)
			wrapped := &pinStageProvider{OwnedPinProvider: provider,
				afterSource: func() { tc.mutate(x.jfs.handler) }}
			worker := pin.NewPrefetcher(pins, x.fuseRoot, "/volume", 1, wrapped)
			t.Cleanup(func() {
				if !worker.StopWithin(5 * time.Second) {
					t.Error("reason worker did not join")
				}
			})
			e, ok := pins.Get("/volume/media/clip.mov")
			if !ok {
				t.Fatal("pin absent")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			err := worker.EnqueueWait(ctx, *e)
			if !errors.Is(err, pin.ErrAuthorityUnavailable) {
				t.Fatalf("cause lost: %v", err)
			}
			got, ok := pins.Get(e.Path)
			if !ok || got.Status != pin.StatusFailed || len(got.Authority) != 0 || pins.IsPinnedReady(e.Path) {
				t.Fatalf("rejected Complete advertised Ready: %+v found=%v", got, ok)
			}
			if !strings.Contains(got.LastError, "Complete rejected: pin authority "+tc.stage+": ") {
				t.Fatalf("first stage not retained: %q", got.LastError)
			}
		})
	}
}

func TestOwnedPinCompleteResidentDigestReason(t *testing.T) {
	x, pins, _, provider, _, _, _, _ := ownedPinThreeChunkFixture(t)
	e, ok := pins.Get("/volume/media/clip.mov")
	if !ok {
		t.Fatal("pin absent")
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
	defer owned.Release()
	// The fixture's resident bytes are available; an intentionally wrong
	// expected digest must reach the local hash comparison and fail closed.
	wrong := [sha256.Size]byte{1}
	authority, err := owned.Complete(ctx, wrong)
	if len(authority) != 0 || !errors.Is(err, pin.ErrAuthorityUnavailable) ||
		!strings.Contains(err.Error(), "resident_bytes: pin authority bytes_digest:") {
		t.Fatalf("digest denial lost nested stage/cause: authority=%d err=%v", len(authority), err)
	}
	if pins.IsPinnedReady(e.Path) {
		t.Fatal("wrong digest admitted Ready")
	}
	_ = x // retain the fixture's owned cache and cleanup through the test
}
