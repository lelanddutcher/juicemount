package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// inplace_hole_range_test.go — B-9 follow-up: the in-place hole guard must
// classify the whole READ RANGE, not only its first byte.
//
// The original guard asked planReadAt about `off` alone. A read that STARTS in
// the valid prefix and runs into the hole was therefore classified "serve", and
// the FUSE pread returned the real prefix followed by the hole's zeros with
// err=nil — the exact #6 silent-zeros shape, reached by any read wider than the
// remaining prefix (onRead issues 256 KiB subreads). The spool path never had
// this gap: spoolReadFile clamps every read to contiguousEnd.

const (
	rangeSeedLen = 4096
	rangeTailOff = 1 << 20
	rangeReadLen = 64 << 10
)

// assertPrefixOnly fails unless the read returned exactly the valid prefix:
// a short read ending at the hole, never hole bytes as content.
func assertPrefixOnly(t *testing.T, what string, buf []byte, got int, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: read straddling the hole failed with %v; want a short read of the valid prefix", what, err)
	}
	if got > rangeSeedLen {
		zeros := bytes.Count(buf[rangeSeedLen:got], []byte{0})
		t.Fatalf("%s: SILENT ZEROS: read straddling the hole returned n=%d (valid prefix is %d); "+
			"%d of the %d bytes past the prefix are hole zeros served as file content",
			what, got, rangeSeedLen, zeros, got-rangeSeedLen)
	}
	if got != rangeSeedLen || !bytes.Equal(buf[:got], bytes.Repeat([]byte{'A'}, rangeSeedLen)) {
		t.Fatalf("%s: read = n=%d, want the %d-byte valid prefix", what, got, rangeSeedLen)
	}
}

func seedOutOfOrderHole(t *testing.T) (*juiceFS, string) {
	t.Helper()
	jfs, store, fuseRoot := newIntegrityHarness(t)
	seedFile(t, jfs, store, fuseRoot, "reel.mov", bytes.Repeat([]byte{'A'}, rangeSeedLen))
	writeVia(t, jfs, "reel.mov", rangeTailOff, bytes.Repeat([]byte{'B'}, rangeSeedLen))
	if plan, tracked := jfs.handler.inPlaceHoles.inPlaceReadPlan("reel.mov", rangeSeedLen); !tracked || plan != planHold {
		t.Fatalf("precondition: hole at %d not tracked as a hold (plan=%v tracked=%v)", rangeSeedLen, plan, tracked)
	}
	return jfs, fuseRoot
}

// cachedFile: mirror entry present.
func TestCachedFileReadStraddlingInPlaceHoleIsClamped(t *testing.T) {
	jfs, _ := seedOutOfOrderHole(t)
	buf, got, err, handle := readAtVia(t, jfs, "reel.mov", 0, rangeReadLen)
	if _, ok := handle.(*cachedFile); !ok {
		t.Fatalf("handle = %T, want *cachedFile", handle)
	}
	assertPrefixOnly(t, "cachedFile", buf, got, err)

	// The client's reissue at the clamp point must now be held, not served.
	_, got, err, _ = readAtVia(t, jfs, "reel.mov", rangeSeedLen, rangeReadLen)
	if got != 0 || !pin.IsSpoolIncomplete(err) {
		t.Fatalf("reissue at the hole = (n=%d, err=%v), want (0, spool incomplete)", got, err)
	}
}

// billyFile: mirror entry missing (startup, out-of-band create, post-purge).
func TestMirrorMissBillyFileReadStraddlingInPlaceHoleIsClamped(t *testing.T) {
	jfs, _ := seedOutOfOrderHole(t)
	jfs.handler.store.DeleteFromCache("reel.mov")
	if e := jfs.handler.store.LookupByPath("reel.mov"); e != nil {
		t.Fatalf("mirror entry remains after eviction: %+v", e)
	}
	buf, got, err, handle := readAtVia(t, jfs, "reel.mov", 0, rangeReadLen)
	if _, ok := handle.(*billyFile); !ok {
		t.Fatalf("handle = %T, want *billyFile", handle)
	}
	assertPrefixOnly(t, "billyFile", buf, got, err)
}

// The ._ AppleDouble exemption (#100) must survive the range clamp: a sidecar
// is served like a plain server, holes included, never shortened or held.
func TestAppleDoubleReadStraddlingInPlaceHoleIsNotClamped(t *testing.T) {
	jfs, store, fuseRoot := newIntegrityHarness(t)
	seedFile(t, jfs, store, fuseRoot, "._reel.mov", bytes.Repeat([]byte{'A'}, 128))
	writeVia(t, jfs, "._reel.mov", 8192, bytes.Repeat([]byte{'B'}, 128))
	_, got, err, _ := readAtVia(t, jfs, "._reel.mov", 0, 8192+128)
	if err != nil || got != 8192+128 {
		t.Fatalf("AppleDouble straddling read = (n=%d, err=%v), want the whole file (%d, nil)", got, err, 8192+128)
	}
}

// A hole that appears WHILE a read is in flight. The pre-read check saw no
// hole, then an out-of-order write grew the file before the pread, so the pread
// returns hole zeros. The post-read recheck must cut the result back to the
// valid prefix.
func TestHoleCreatedDuringReadIsClampedAfterThePread(t *testing.T) {
	for _, mirrorMiss := range []bool{false, true} {
		name := "cachedFile"
		if mirrorMiss {
			name = "billyFile"
		}
		t.Run(name, func(t *testing.T) {
			jfs, store, fuseRoot := newIntegrityHarness(t)
			seedFile(t, jfs, store, fuseRoot, "reel.mov", bytes.Repeat([]byte{'A'}, rangeSeedLen))
			if mirrorMiss {
				store.DeleteFromCache("reel.mov")
			}
			h := jfs.handler
			fired := false
			h.testAfterInPlaceReadBound = func(name string) {
				if fired || name != "reel.mov" {
					return
				}
				fired = true
				// The writer's bytes land on the backing file and the tracker
				// records them, exactly as writeFile.WriteAt does — but without
				// a namespace operation, which would wait for this admitted read.
				fd, err := os.OpenFile(filepath.Join(fuseRoot, "reel.mov"), os.O_WRONLY, 0)
				if err != nil {
					t.Errorf("open backing file: %v", err)
					return
				}
				if _, err := fd.WriteAt(bytes.Repeat([]byte{'B'}, rangeSeedLen), rangeTailOff); err != nil {
					t.Errorf("out-of-order write: %v", err)
				}
				_ = fd.Close()
				h.inPlaceHoles.noteWrite("reel.mov", rangeSeedLen, rangeTailOff, rangeTailOff+rangeSeedLen)
			}
			buf, got, err, _ := readAtVia(t, jfs, "reel.mov", 0, rangeReadLen)
			if !fired {
				t.Fatal("hook never fired: the read did not pass through the in-place bound")
			}
			assertPrefixOnly(t, name+" (hole created mid-read)", buf, got, err)
		})
	}
}

// The owned-pin resident route is selected inside PrepareResidentRead before
// any of its own checks run. A tracked hole must be refused at that choke
// point, ahead of every resident route, exactly as ReadAt's Priority 0 is.
func TestPrepareResidentReadRefusesInPlaceHoleBeforeEveryRoute(t *testing.T) {
	jfs, _ := seedOutOfOrderHole(t)
	f := &cachedFile{name: "reel.mov", handler: jfs.handler, pinGeneration: "owned-pin-generation"}
	r, err := f.PrepareResidentRead(rangeSeedLen, 4096)
	if r != nil || !pin.IsSpoolIncomplete(err) {
		t.Fatalf("PrepareResidentRead(hole) = (%v, %v), want (nil, spool incomplete) — the "+
			"owned-pin resident route was consulted before the in-place hole guard", r, err)
	}
}
