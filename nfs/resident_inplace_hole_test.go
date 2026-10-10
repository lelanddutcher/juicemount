//go:build redisoptionsacceptance

package nfs

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// B-9: a resident (SSD-verified) answer bypasses ReadAt entirely, so it must
// honour the in-place hole guard for the whole range it would return, not only
// its first byte. A request starting in the valid prefix whose resident answer
// would run through a tracked hole must fall back to ReadAt (which clamps at
// the hole); one starting in the hole must be held.
func TestResidentReadDoesNotAnswerAcrossAnInPlaceHole(t *testing.T) {
	x := newBoundCacheFixture(t)
	h := x.jfs.handler
	size := int64(len(x.want))
	// One qualified direct read proves the cache path, as in the other
	// bound-cache tests; resident answers require that proof.
	warm := openBoundCacheFile(t, x)
	if n, err := warm.ReadAt(make([]byte, size), 0); err != nil || int64(n) != size {
		t.Fatalf("warming read = %d %v", n, err)
	}
	cf := openBoundCacheFile(t, x)

	// Control: with no hole the cross-EOF request is answered residently, so
	// the refusals below are caused by the hole and not by the fixture.
	r, err := cf.PrepareResidentRead(2048, 1<<20)
	if err != nil || r == nil || int64(len(r.Data)) != size-2048 {
		t.Fatalf("control resident answer = (%v, %v), want %d bytes", r, err, size-2048)
	}

	// An out-of-order in-place write: valid prefix [0,4096), hole [4096,61440).
	h.inPlaceHoles.noteWrite("media/clip.mov", 4096, size-4096, size)
	t.Cleanup(func() { h.inPlaceHoles.forget("media/clip.mov") })

	r, err = cf.PrepareResidentRead(2048, 1<<20)
	if err != nil || r != nil {
		n := 0
		if r != nil {
			n = len(r.Data)
		}
		t.Fatalf("resident answer across the hole = (%d bytes, %v), want (nil, nil): "+
			"the answer covers the unwritten range [4096,%d)", n, err, size-4096)
	}
	r, err = cf.PrepareResidentRead(8192, 4096)
	if r != nil || !pin.IsSpoolIncomplete(err) {
		t.Fatalf("resident answer inside the hole = (%v, %v), want (nil, spool incomplete)", r, err)
	}
}
