package nfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

// The streaming-drain punch hazard, on the NFS read side (B-9 audit).
//
// F_PUNCHHOLE keeps the spool file's logical size, so a half-punched spool file
// still reports its full length and its punched prefix reads as zeros with no
// error. The existing routing tests publish punchedEnd over INTACT spool bytes;
// this one really punches them, so a read routed at the spool returns zeros
// rather than a stale-but-plausible byte. The read STRADDLES punchedEnd, which
// is the case that must split between the destination and the spool.
func TestHalfPunchedSpoolReadNeverServesPunchedZeros(t *testing.T) {
	jfs, store, _ := newIntegrityHarness(t)
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	s, err := NewSpoolStore(t.TempDir(), 64<<20, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	s.capCeiling.Store(int64(1) << 50) // see newTestSpoolStoreTB
	s.diskAvailAt.Store(time.Now().UnixNano())
	jfs.handler.SetSpool(s, nil)

	const (
		size   = 256 << 10
		punchE = 128 << 10
	)
	e, err := s.OpenWrite("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.WriteAt(bytes.Repeat([]byte{0xC3}, size), 0); err != nil {
		t.Fatal(err)
	}
	destPath := filepath.Join(t.TempDir(), ".juicemount-streaming-1-clip.mov")
	if err := os.WriteFile(destPath, bytes.Repeat([]byte{0xD4}, punchE), 0o644); err != nil {
		t.Fatal(err)
	}
	e.SetStreamDest(destPath)

	// Production order: publish, then punch (spool_stream.go STEP 5/6).
	e.publishPunchedEnd(punchE)
	e.mu.RLock()
	spoolFD := e.file
	e.mu.RUnlock()
	punched, err := punchRange(spoolFD, 0, punchE)
	if err != nil {
		t.Fatalf("punch: %v", err)
	}
	if punched == 0 {
		t.Skip("filesystem cannot punch holes; the hazard does not exist here")
	}
	// Precondition: the spool really is half-punched — full logical size, zeros
	// in the prefix. Without this the test could pass on intact bytes.
	raw, err := os.ReadFile(e.spoolFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != size || raw[0] != 0 || raw[punchE] != 0xC3 {
		t.Fatalf("precondition: spool len=%d first=0x%02X atPunch=0x%02X, want %d / 0x00 / 0xC3",
			len(raw), raw[0], raw[punchE], size)
	}

	// Through the real OpenFile dispatch, which must pick the spool shadow.
	const off, n = punchE - 8192, 16384
	buf, got, err, handle := readAtVia(t, jfs, "clip.mov", off, n)
	if _, ok := handle.(*spoolReadFile); !ok {
		t.Fatalf("handle = %T, want *spoolReadFile", handle)
	}
	if err != nil || got != n {
		t.Fatalf("straddling read = (n=%d, err=%v), want (%d, nil)", got, err, n)
	}
	want := append(bytes.Repeat([]byte{0xD4}, 8192), bytes.Repeat([]byte{0xC3}, 8192)...)
	if !bytes.Equal(buf[:got], want) {
		t.Fatalf("straddling read returned %d zero bytes of %d — the punched prefix was served "+
			"from the spool as file content instead of from the destination",
			bytes.Count(buf[:got], []byte{0}), got)
	}
}
