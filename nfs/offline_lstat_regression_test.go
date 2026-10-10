package nfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

// These cases change process-global offline state and must not run in parallel.
// A real local directory stands in for FUSE; foreground/lstat is measured via
// the same production attribution helper as fusemetrics_test.go.
func p96LstatFixture(t *testing.T) (*juiceFS, *pin.Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	pins, err := pin.Open(filepath.Join(t.TempDir(), "pins.db"))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	h := NewHandler(store, root)
	h.SetPinStore(pins, "/private-volume")
	t.Cleanup(func() {
		if !h.StopHandler() {
			t.Error("StopHandler did not join")
		}
		if err := pins.Close(); err != nil {
			t.Error(err)
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return &juiceFS{handler: h}, pins, root
}

func p96OfflineState(t *testing.T) {
	t.Helper()
	prior := pin.State()
	pin.SetOffline(false)
	pin.SetAutoOffline(false, "")
	t.Cleanup(func() {
		pin.SetOffline(prior.UserOffline)
		pin.SetAutoOffline(prior.AutoOffline, prior.Reason)
	})
}

func p96ForegroundLstat(t *testing.T, before map[string]uint64) uint64 {
	t.Helper()
	cells := fuseCellCounts(t)
	return cells["foreground/lstat"].Calls - before["foreground/lstat"]
}

func p96LstatCounts(t *testing.T) map[string]uint64 {
	t.Helper()
	cells := fuseCellCounts(t)
	return map[string]uint64{"foreground/lstat": cells["foreground/lstat"].Calls}
}

func TestOfflineLstatUnpinnedMissRefusesBeforeFUSE(t *testing.T) {
	p96OfflineState(t)
	jfs, _, root := p96LstatFixture(t)
	if err := os.WriteFile(filepath.Join(root, "cold.mov"), []byte("body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("cold.mov", filepath.Join(root, "cold-link")); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		user, auto bool
	}{
		{"cold.mov", true, false}, {"cold-link", false, true},
		{"absent.mov", true, true},
	}
	for _, tc := range cases {
		pin.SetOffline(tc.user)
		pin.SetAutoOffline(tc.auto, "test")
		before := p96LstatCounts(t)
		fi, err := jfs.Lstat(tc.name)
		if fi != nil || !errors.Is(err, pin.ErrOfflineNotAvailable) {
			t.Errorf("offline Lstat(%q) = (%v,%v), want nil/offline sentinel", tc.name, fi, err)
		}
		if delta := p96ForegroundLstat(t, before); delta != 0 {
			t.Errorf("offline Lstat(%q) issued %d foreground FUSE Lstat calls", tc.name, delta)
		}
	}
}

func TestOfflineLstatPreservesResidentAndQualifiedFallbacks(t *testing.T) {
	p96OfflineState(t)
	jfs, pins, root := p96LstatFixture(t)
	e := metadata.MakeEntry("resident.mov", false, 7, time.Unix(123, 0), 301)
	if err := jfs.handler.store.Insert(e); err != nil {
		t.Fatal(err)
	}
	pin.SetOffline(true)
	before := p96LstatCounts(t)
	fi, err := jfs.Lstat("resident.mov")
	if err != nil || fi == nil || fi.Name() != "resident.mov" {
		t.Fatalf("resident Lstat = (%v,%v)", fi, err)
	}
	if delta := p96ForegroundLstat(t, before); delta != 0 {
		t.Fatalf("resident mirror hit issued %d FUSE Lstat calls", delta)
	}

	if err := os.Symlink("missing-target", filepath.Join(root, "pinned-link")); err != nil {
		t.Fatal(err)
	}
	if err := pins.Pin("/private-volume/pinned-link", 0, "/private-volume"); err != nil {
		t.Fatal(err)
	}
	if err := pins.UpdateStatus("/private-volume/pinned-link", pin.StatusReady, 0, ""); err == nil {
		t.Fatal("dangling symlink acquired Ready without regular-file authority")
	}
	if pins.IsPinnedReady("/private-volume/pinned-link") {
		t.Fatal("bytes-only pin became effective Ready")
	}
	before = p96LstatCounts(t)
	fi, err = jfs.Lstat("pinned-link")
	if fi != nil || !errors.Is(err, pin.ErrOfflineNotAvailable) {
		t.Fatalf("unqualified symlink Lstat = (%v,%v)", fi, err)
	}
	if delta := p96ForegroundLstat(t, before); delta != 0 {
		t.Errorf("unqualified symlink issued %d foreground Lstat calls", delta)
	}
}

func TestOfflineLstatPreservesOnlineAndNoPinStore(t *testing.T) {
	p96OfflineState(t)
	jfs, _, root := p96LstatFixture(t)
	if err := os.Symlink("missing-target", filepath.Join(root, "online-link")); err != nil {
		t.Fatal(err)
	}
	before := p96LstatCounts(t)
	fi, err := jfs.Lstat("online-link")
	if err != nil || fi == nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("online dangling symlink Lstat = (%v,%v)", fi, err)
	}
	if delta := p96ForegroundLstat(t, before); delta != 1 {
		t.Errorf("online cold symlink lstat calls=%d, want 1", delta)
	}

	// Nil pin registry retains the legacy fallback, even when user offline.
	jfs.handler.SetPinStore(nil, "/private-volume")
	pin.SetOffline(true)
	if err := os.Symlink("missing-target", filepath.Join(root, "unconfigured-link")); err != nil {
		t.Fatal(err)
	}
	before = p96LstatCounts(t)
	fi, err = jfs.Lstat("unconfigured-link")
	if err != nil || fi == nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("no-pin-store symlink Lstat = (%v,%v)", fi, err)
	}
	if delta := p96ForegroundLstat(t, before); delta != 1 {
		t.Errorf("no-pin-store fallback lstat calls=%d, want 1", delta)
	}
}
