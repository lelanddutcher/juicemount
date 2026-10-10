//go:build redisoptionsacceptance && latency_acceptance

package nfs

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// The real cachedFile declines a cold READ when its path proof misses its
// bounded deadline. The TCP reply must be a retryable READ3resfail, even while
// the original backend proof remains physically held by this test.
func TestReadQualificationEAGAINWireJukebox(t *testing.T) {
	x := newBoundCacheFixture(t)
	b := x.jfs.handler.cacheBinding.Load()
	actual := b.readProof
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	joined := make(chan struct{})
	var once sync.Once
	var joinedOnce sync.Once
	var proofCalls atomic.Int64
	defer func() {
		once.Do(func() { close(release) })
		select {
		case <-joined:
		case <-time.After(6 * time.Second):
			t.Error("original proof worker did not join")
		}
	}()
	b.readProof = func(ctx context.Context, uuid, name string, ids []uint64) (metadata.RedisPathProof, error) {
		proofCalls.Add(1)
		defer joinedOnce.Do(func() { close(joined) })
		proof, err := actual(ctx, uuid, name, ids)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return proof, err
	}
	client := localSidecarLoopback(t, x.jfs.handler)
	before := fuseCellCounts(t)
	pending, err := client.send(100003, 6, latencyReadArgs{
		Handle: x.jfs.handler.ToHandle(nil, []string{"media", "clip.mov"}), Count: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("proof did not enter")
	}
	reply, err := pending.await(time.Now().Add(6 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.body) != 8 || binary.BigEndian.Uint32(reply.body[:4]) != 10008 || binary.BigEndian.Uint32(reply.body[4:]) != 0 {
		t.Fatalf("READ3resfail=%x, want JUKEBOX with absent post-op attrs", reply.body)
	}
	select {
	case <-joined:
		t.Fatal("proof worker settled before release")
	default:
	}
	after := fuseCellCounts(t)
	for key, cell := range after {
		if strings.HasPrefix(key, "foreground/") && cell.Calls != before[key].Calls {
			t.Fatalf("refused READ used FUSE %s: before=%d after=%d", key, before[key].Calls, cell.Calls)
		}
	}
	once.Do(func() { close(release) })
	<-joined
	if got := proofCalls.Load(); got != 1 {
		t.Fatalf("proof calls=%d, want exactly one original worker", got)
	}
}
