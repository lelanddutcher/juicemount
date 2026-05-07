package pin

import (
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPinAndQuery(t *testing.T) {
	s := newTestStore(t)
	if err := s.Pin("/Volumes/zpool/a.mov", 1024, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.Pending(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("Pending = %d, want 1", len(pending))
	}
	if pending[0].Path != "/Volumes/zpool/a.mov" {
		t.Errorf("path = %q", pending[0].Path)
	}
	if pending[0].Size != 1024 {
		t.Errorf("size = %d", pending[0].Size)
	}
	if pending[0].Status != StatusPending {
		t.Errorf("status = %v", pending[0].Status)
	}
}

func TestPinIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/x.mov", 100, "/")
	s.Pin("/x.mov", 200, "/") // size change
	all, _ := s.All()
	if len(all) != 1 {
		t.Errorf("expected 1 entry after re-pin, got %d", len(all))
	}
	if all[0].Size != 200 {
		t.Errorf("size = %d, want 200 (latest)", all[0].Size)
	}
}

func TestPinManyBatched(t *testing.T) {
	s := newTestStore(t)
	entries := []Entry{
		{Path: "/a", Size: 1, PinRoot: "/"},
		{Path: "/b", Size: 2, PinRoot: "/"},
		{Path: "/c", Size: 3, PinRoot: "/"},
	}
	if err := s.PinMany(entries); err != nil {
		t.Fatal(err)
	}
	all, _ := s.All()
	if len(all) != 3 {
		t.Errorf("expected 3, got %d", len(all))
	}
}

func TestUnpinByRoot(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/proj/a", 1, "/proj")
	s.Pin("/proj/b", 2, "/proj")
	s.Pin("/other/c", 3, "/other")
	n, err := s.Unpin("/proj")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("Unpin returned %d, want 2", n)
	}
	all, _ := s.All()
	if len(all) != 1 {
		t.Errorf("after unpin /proj, %d entries remain (want 1)", len(all))
	}
	if all[0].Path != "/other/c" {
		t.Errorf("wrong entry survived: %q", all[0].Path)
	}
}

func TestUpdateStatus(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/x", 100, "/")
	if err := s.UpdateStatus("/x", StatusReady, 100, ""); err != nil {
		t.Fatal(err)
	}
	all, _ := s.All()
	if all[0].Status != StatusReady {
		t.Errorf("status = %v", all[0].Status)
	}
	if all[0].BytesCached != 100 {
		t.Errorf("bytesCached = %d", all[0].BytesCached)
	}
}

func TestStaleQuery(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/old", 1, "/")
	s.Pin("/new", 1, "/")
	// Mark both as ready, but old's last_prefetched is now-stale
	s.UpdateStatus("/old", StatusReady, 1, "")
	time.Sleep(50 * time.Millisecond)
	s.UpdateStatus("/new", StatusReady, 1, "")

	stale, err := s.Stale(25*time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 {
		t.Errorf("Stale returned %d, want 1", len(stale))
	}
	if len(stale) > 0 && stale[0].Path != "/old" {
		t.Errorf("stale entry = %q, want /old", stale[0].Path)
	}
}

func TestPinRoots(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/a/1", 100, "/a")
	s.Pin("/a/2", 200, "/a")
	s.Pin("/b/1", 300, "/b")
	s.UpdateStatus("/a/1", StatusReady, 100, "")
	roots, err := s.PinRoots()
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 2 {
		t.Fatalf("roots = %d, want 2", len(roots))
	}
	// Should be sorted alphabetically
	if roots[0].Root != "/a" {
		t.Errorf("first root = %q, want /a", roots[0].Root)
	}
	if roots[0].TotalFiles != 2 {
		t.Errorf("/a TotalFiles = %d, want 2", roots[0].TotalFiles)
	}
	if roots[0].ReadyFiles != 1 {
		t.Errorf("/a ReadyFiles = %d, want 1", roots[0].ReadyFiles)
	}
	if roots[0].PendingFiles != 1 {
		t.Errorf("/a PendingFiles = %d, want 1", roots[0].PendingFiles)
	}
	if roots[0].TotalBytes != 300 {
		t.Errorf("/a TotalBytes = %d, want 300", roots[0].TotalBytes)
	}
}

func TestAggregateStats(t *testing.T) {
	s := newTestStore(t)
	s.Pin("/a", 100, "/")
	s.Pin("/b", 200, "/")
	s.Pin("/c", 300, "/")
	s.UpdateStatus("/a", StatusReady, 100, "")
	s.UpdateStatus("/b", StatusFailed, 0, "boom")

	a, err := s.AggregateStats()
	if err != nil {
		t.Fatal(err)
	}
	if a.TotalFiles != 3 {
		t.Errorf("TotalFiles = %d", a.TotalFiles)
	}
	if a.ReadyFiles != 1 {
		t.Errorf("ReadyFiles = %d", a.ReadyFiles)
	}
	if a.FailedFiles != 1 {
		t.Errorf("FailedFiles = %d", a.FailedFiles)
	}
	if a.PendingFiles != 1 {
		t.Errorf("PendingFiles = %d", a.PendingFiles)
	}
	if a.TotalBytes != 600 {
		t.Errorf("TotalBytes = %d, want 600", a.TotalBytes)
	}
	if a.CachedBytes != 100 {
		t.Errorf("CachedBytes = %d, want 100", a.CachedBytes)
	}
}

func TestStatusString(t *testing.T) {
	if StatusReady.String() != "ready" {
		t.Error("StatusReady.String")
	}
	if StatusUnknown.String() != "unknown" {
		t.Error("StatusUnknown.String")
	}
}

// TestIsPinnedReadyWindow exercises the four states the offline-mode open
// gate has to handle:
//   - Pending: known-not-cached → must refuse (otherwise we re-introduce
//     the FUSE-to-backend stall the gate exists to prevent)
//   - Prefetching with bytes_cached < size: in flight → refuse
//   - Prefetching with bytes_cached >= size: late-Ready window → allow
//   - Ready: full hit → allow
func TestIsPinnedReadyWindow(t *testing.T) {
	s := newTestStore(t)

	if err := s.Pin("/Volumes/zpool/a.mov", 1000, "/Volumes/zpool"); err != nil {
		t.Fatal(err)
	}

	// Default state after Pin() is Pending — gate must refuse.
	if s.IsPinnedReady("/Volumes/zpool/a.mov") {
		t.Error("Pending file should not be considered ready")
	}

	// Mid-prefetch: 50% cached, status Prefetching → still refuse.
	_ = s.UpdateStatus("/Volumes/zpool/a.mov", StatusPrefetching, 500, "")
	if s.IsPinnedReady("/Volumes/zpool/a.mov") {
		t.Error("half-cached Prefetching should not be considered ready")
	}

	// Late-Ready: 100% cached but status row not yet updated.
	// This is the race the offline-toggle would otherwise hit.
	_ = s.UpdateStatus("/Volumes/zpool/a.mov", StatusPrefetching, 1000, "")
	if !s.IsPinnedReady("/Volumes/zpool/a.mov") {
		t.Error("fully-cached Prefetching should be allowed (late-Ready window)")
	}

	// Final state.
	_ = s.UpdateStatus("/Volumes/zpool/a.mov", StatusReady, 1000, "")
	if !s.IsPinnedReady("/Volumes/zpool/a.mov") {
		t.Error("Ready file should always be allowed")
	}

	// Failed → refuse.
	_ = s.UpdateStatus("/Volumes/zpool/a.mov", StatusFailed, 0, "backend error")
	if s.IsPinnedReady("/Volumes/zpool/a.mov") {
		t.Error("Failed file should not be allowed")
	}

	// Unknown path → refuse.
	if s.IsPinnedReady("/Volumes/zpool/never-pinned.mov") {
		t.Error("unknown path should not be allowed")
	}
}

func TestOfflineModeToggle(t *testing.T) {
	if IsOffline() {
		t.Error("default should be online")
	}
	SetOffline(true)
	if !IsOffline() {
		t.Error("after SetOffline(true)")
	}
	SetOffline(false)
	if IsOffline() {
		t.Error("after SetOffline(false)")
	}
}

func TestStripVolumePrefix(t *testing.T) {
	cases := map[string]string{
		"/Volumes/zpool/Foo/bar.mov": "Foo/bar.mov",
		"/Volumes/zpool/x.mov":       "x.mov",
		"/Volumes/zpool":             "",
		"/other/path":                "/other/path",
		"":                           "",
	}
	for in, want := range cases {
		if got := stripVolumePrefix(in); got != want {
			t.Errorf("stripVolumePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// Quick smoke that filepath.Join behaves the way prefetcher expects
func TestFilePathJoinSanity(t *testing.T) {
	got := filepath.Join("/home/me/.juicemount/fuse-internal", "Foo/bar.mov")
	want := "/home/me/.juicemount/fuse-internal/Foo/bar.mov"
	if got != want {
		t.Errorf("Join surprise: %q != %q", got, want)
	}
}
