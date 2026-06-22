package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestBillyFileOfflineGate locks in the read-time offline gate added to
// billyFile (the read path used when a file isn't yet in the SQLite
// metadata cache, e.g. a freshly browsed directory). Without the gate,
// a file opened during a brief online window before offline-toggle could
// keep streaming through FUSE → backend after the toggle, defeating the
// fail-fast guarantee on cellular.
//
// The refusal error is pin.ErrOfflineNotAvailable (the protocol layer maps it
// to NXIO, which — unlike a generic IO error — does not invalidate the kernel's
// file handle cache). For a PINNED file the gate no longer trusts the "Ready"
// flag blindly: it bounds the read so an evicted block refuses cleanly rather
// than tarpitting on a backend GET (#43). A genuinely-local block (this test's
// temp file) returns instantly and is served.
func TestBillyFileOfflineGate(t *testing.T) {
	// Create a real file we can wrap with billyFile.
	dir := t.TempDir()
	fp := filepath.Join(dir, "sample.bin")
	if err := os.WriteFile(fp, []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	osFile, err := os.Open(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer osFile.Close()

	// Restore offline state on exit so we don't poison other tests.
	wasOffline := pin.IsOffline()
	t.Cleanup(func() { pin.SetOffline(wasOffline) })

	// Online + un-pinned: reads succeed.
	pin.SetOffline(false)
	bf := &billyFile{File: osFile, name: "sample.bin", pinned: false}
	buf := make([]byte, 5)
	if n, err := bf.ReadAt(buf, 0); err != nil || n == 0 {
		t.Errorf("online un-pinned ReadAt: n=%d err=%v, want bytes", n, err)
	}

	// Offline + un-pinned: reads must refuse with ErrOfflineNotAvailable.
	pin.SetOffline(true)
	if _, err := bf.ReadAt(buf, 0); !errors.Is(err, pin.ErrOfflineNotAvailable) {
		t.Errorf("offline un-pinned ReadAt: err=%v, want ErrOfflineNotAvailable", err)
	}
	if _, err := bf.Read(buf); !errors.Is(err, pin.ErrOfflineNotAvailable) {
		t.Errorf("offline un-pinned Read: err=%v, want ErrOfflineNotAvailable", err)
	}

	// Offline + pinned: a locally-readable block is served (bounded read
	// completes well within the timeout). This is the no-regression case for
	// genuinely-resident pinned files.
	bf2 := &billyFile{File: osFile, name: "sample.bin", pinned: true}
	if n, err := bf2.ReadAt(buf, 0); err != nil || n == 0 {
		t.Errorf("offline pinned ReadAt (local hit): n=%d err=%v, want bytes", n, err)
	}
}
