package pin

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// FUSEIdentityCheckDev refuses a device equal to the mountpoint's parent
// volume (a plain folder standing in for the mount) and accepts another.
func TestFUSEIdentityCheckDev(t *testing.T) {
	dir := t.TempDir()
	mp := filepath.Join(dir, "mnt")
	if err := os.Mkdir(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	SetFUSEIdentityPath(mp)
	t.Cleanup(ResetFUSEIdentityForTest)
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		t.Fatal(err)
	}
	if err := FUSEIdentityCheckDev(uint64(st.Dev)); err == nil { //nolint:unconvert // int32 on Darwin
		t.Fatal("accepted a descriptor on the mountpoint's parent volume")
	}
	if err := FUSEIdentityCheckDev(uint64(st.Dev) + 1); err != nil { //nolint:unconvert // int32 on Darwin
		t.Fatalf("refused another device: %v", err)
	}
	ResetFUSEIdentityForTest()
	if err := FUSEIdentityCheckDev(uint64(st.Dev)); err != nil { //nolint:unconvert // int32 on Darwin
		t.Fatalf("no mount configured: %v", err)
	}
}
