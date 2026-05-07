package nfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
)

// TestBillyFileOfflineGate locks in the read-time offline gate added to
// billyFile (the read path used when a file isn't yet in the SQLite
// metadata cache, e.g. a freshly browsed directory). Without the gate,
// a file opened during a brief online window before offline-toggle could
// keep streaming through FUSE → backend after the toggle, defeating the
// fail-fast guarantee on cellular.
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

	// Offline + un-pinned: reads must EIO.
	pin.SetOffline(true)
	if _, err := bf.ReadAt(buf, 0); err != syscall.EIO {
		t.Errorf("offline un-pinned ReadAt: err=%v, want EIO", err)
	}
	if _, err := bf.Read(buf); err != syscall.EIO {
		t.Errorf("offline un-pinned Read: err=%v, want EIO", err)
	}

	// Offline + pinned: reads succeed (FUSE LRU is local, safe to use).
	bf2 := &billyFile{File: osFile, name: "sample.bin", pinned: true}
	if n, err := bf2.ReadAt(buf, 0); err != nil || n == 0 {
		t.Errorf("offline pinned ReadAt: n=%d err=%v, want bytes", n, err)
	}
}
