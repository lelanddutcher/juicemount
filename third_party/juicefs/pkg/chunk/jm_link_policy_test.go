package chunk

// JuiceMount patch 0027: the link policy switches while the store is open.

import (
	"bytes"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// A store opened without --demand-first (a lan mount) reads a cold block
// whole; after the policy turns demand-first on it reads demand spans; after
// it turns off again it reads whole blocks once more. Every read returns the
// right bytes.
func TestJMLinkPolicySwitchesDemandFirstBothWays(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	data := writeBlocks(t, mem, 301, 3)
	d := &demandStorage{ObjectStorage: mem}
	store := NewCachedStore(d, demandConf(t, false), nil).(*cachedStore)
	r := store.NewReader(301, len(data))
	bs := store.conf.BlockSize
	const n = 128 << 10

	read := func(block int) {
		t.Helper()
		off := block*bs + bs/2
		if got := readAt(t, r, off, n); !bytes.Equal(got, data[off:off+n]) {
			t.Fatalf("block %d: wrong bytes", block)
		}
	}
	slow := JMLinkPolicy{DemandFirst: true, GetTimeout: 2 * time.Minute, PutTimeout: 3 * time.Minute, MaxRetries: 3}
	fast := JMLinkPolicy{DemandFirst: false, GetTimeout: time.Minute, PutTimeout: time.Minute, MaxRetries: 10}

	conf := store.conf
	if got, want := store.JMLinkPolicy(), (JMLinkPolicy{GetTimeout: conf.GetTimeout, PutTimeout: conf.PutTimeout, MaxRetries: conf.MaxRetries}); got != want {
		t.Fatalf("mount policy %+v, want the mount flags %+v", got, want)
	}
	if store.spansOn() {
		t.Fatal("spans on without --demand-first")
	}

	// Lan mount, upstream path: a read in the middle of a block is a
	// ranged GET that also queues the whole block for the prefetcher.
	read(0)
	waitCached(t, store, sliceForRead(301, len(data), store).key(0))
	full0, ranged0 := d.full.Load(), d.ranged.Load()
	if full0 != 1 {
		t.Fatalf("lan mount: whole-block GETs %d, want 1", full0)
	}

	// The link turns slow.
	if got, ok := store.JMSetLinkPolicy(slow); !ok || got != slow {
		t.Fatalf("set slow: %+v %v", got, ok)
	}
	if !store.spansOn() || store.getTimeout() != 2*time.Minute || store.putTimeout() != 3*time.Minute || store.maxRetries() != 3 {
		t.Fatalf("slow policy not in force: spans %v get %s put %s retries %d",
			store.spansOn(), store.getTimeout(), store.putTimeout(), store.maxRetries())
	}
	read(1)
	time.Sleep(100 * time.Millisecond) // a prefetch would have started by now
	if f := d.full.Load(); f != full0 {
		t.Fatalf("slow policy: whole-block GETs %d, want %d (spans queue no prefetch)", f, full0)
	}
	if rg := d.ranged.Load() - ranged0; rg < 2 {
		t.Fatalf("slow policy: span GETs %d, want at least the 256 KiB parts of a 1 MiB span", rg)
	}

	// And fast again.
	if got, ok := store.JMSetLinkPolicy(fast); !ok || got != fast {
		t.Fatalf("set fast: %+v %v", got, ok)
	}
	if store.spansOn() {
		t.Fatal("spans still on after the fast policy")
	}
	read(2)
	waitCached(t, store, sliceForRead(301, len(data), store).key(2))
	if f := d.full.Load(); f != full0+1 {
		t.Fatalf("fast again: whole-block GETs %d, want %d", f, full0+1)
	}
}

func TestJMLinkPolicyRefusesOutOfBounds(t *testing.T) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	store := NewCachedStore(mem, demandConf(t, true), nil).(*cachedStore)
	before := store.JMLinkPolicy()
	for _, p := range []JMLinkPolicy{
		{GetTimeout: 0, PutTimeout: time.Minute},
		{GetTimeout: time.Minute, PutTimeout: 31 * time.Minute},
		{GetTimeout: time.Minute, PutTimeout: time.Minute, MaxRetries: 51},
		{GetTimeout: time.Minute, PutTimeout: time.Minute, MaxRetries: -1},
	} {
		if got, ok := store.JMSetLinkPolicy(p); ok || got != before {
			t.Fatalf("%+v: accepted (%v) or changed the policy to %+v", p, ok, got)
		}
	}
	if !before.DemandFirst {
		t.Fatal("--demand-first mount reports demand-first off")
	}
}
