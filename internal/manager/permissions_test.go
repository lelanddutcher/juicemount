package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeFI is a minimal os.FileInfo whose only meaningful field is Mode — the
// single thing writableByClient reads.
type fakeFI struct{ mode os.FileMode }

func (f fakeFI) Name() string       { return "x" }
func (f fakeFI) Size() int64        { return 0 }
func (f fakeFI) Mode() os.FileMode  { return f.mode }
func (f fakeFI) ModTime() time.Time { return time.Time{} }
func (f fakeFI) IsDir() bool        { return f.mode.IsDir() }
func (f fakeFI) Sys() any           { return nil }

// TestWritableByClient pins the verdict — the diagnostic that tells the operator
// whether the mount client can write a path. The load-bearing case is the last:
// root:wheel 0755 (what a root-run migration/farm leaves) is NOT writable by a
// uid-501 client — exactly the failure we're repairing.
func TestWritableByClient(t *testing.T) {
	cases := []struct {
		name                         string
		mode                         os.FileMode
		uid, gid, ownerUID, ownerGID int
		want                         bool
	}{
		{"client is owner, owner has write", 0644, 501, 20, 501, 20, true},
		{"client in owning group, group has write", 0664, 0, 20, 501, 20, true},
		{"group arm disabled when ownerGID<0", 0664, 0, 20, 501, -1, false},
		{"world-writable", 0666, 0, 0, 501, 20, true},
		{"root:wheel 0755, client 501:20 → NOT writable (the bug)", 0755, 0, 0, 501, 20, false},
		{"owner match but no owner-write bit", 0444, 501, 20, 501, 20, false},
		{"ownerUID 0 (unset) → owner arm disabled, not writable", 0644, 0, 0, 0, 0, false},
	}
	for _, c := range cases {
		got, reason := writableByClient(fakeFI{c.mode}, c.uid, c.gid, c.ownerUID, c.ownerGID)
		if got != c.want {
			t.Errorf("%s: writableByClient(mode=%04o,%d:%d vs owner %d:%d) = %v (%q), want %v",
				c.name, c.mode, c.uid, c.gid, c.ownerUID, c.ownerGID, got, reason, c.want)
		}
	}
}

// TestMountOwnerPersistRoundTrip: SetMountOwner persists, survives a reload, and
// the unset (uid<=0) case writes no mount_owner key (so a bare spec never
// serializes a misleading {0,-1}).
func TestMountOwnerPersistRoundTrip(t *testing.T) {
	sf := filepath.Join(t.TempDir(), "state.json")

	m1 := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/x"})
	m1.SetStateFile(sf)
	m1.SetMountOwner(501, 20)
	if u, g := m1.MountOwner(); u != 501 || g != 20 {
		t.Fatalf("live owner after SetMountOwner = %d:%d, want 501:20", u, g)
	}

	m2 := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/x"})
	m2.SetStateFile(sf)
	if u, g := m2.MountOwner(); u != 501 || g != 20 {
		t.Fatalf("reloaded owner = %d:%d, want 501:20 (persistence broken)", u, g)
	}

	// Persisted value must WIN over the flag-seeded spec on the reloaded manager.
	m3 := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/x", OwnerUID: 999, OwnerGID: 999})
	m3.SetStateFile(sf)
	if u, g := m3.MountOwner(); u != 501 || g != 20 {
		t.Fatalf("reloaded owner = %d:%d, want 501:20 (persisted must override flag-seeded 999)", u, g)
	}

	// Unset owner → no mount_owner key in the file.
	sf2 := filepath.Join(t.TempDir(), "state2.json")
	m4 := NewJobManager("/dev/null", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/x"})
	m4.SetStateFile(sf2)
	m4.SaveState()
	if b, err := os.ReadFile(sf2); err == nil && strings.Contains(string(b), "mount_owner") {
		t.Errorf("unset owner still wrote a mount_owner key:\n%s", b)
	}
}
