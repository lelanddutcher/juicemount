package pin

import (
	"os"
	"path/filepath"
	"testing"
)

// V2.3 U3: user-intent offline must persist across launches via the marker
// file — toggle-on writes it, toggle-off removes it, boot reads it. The
// field complaint: "clicking the offline toggle doesn't just start it in
// offline mode" — persistence is what makes the toggle stick.
func TestOfflineIntentPersistence(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "offline-intent")
	SetOfflinePersistPath(marker)
	t.Cleanup(func() {
		SetOffline(false)
		SetOfflinePersistPath("")
	})

	if PersistedOfflineIntent() {
		t.Fatal("fresh marker path must report no persisted intent")
	}
	SetOffline(true)
	if !PersistedOfflineIntent() {
		t.Fatal("toggle-on must persist the intent marker")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("marker file missing after toggle-on: %v", err)
	}
	SetOffline(false)
	if PersistedOfflineIntent() {
		t.Fatal("toggle-off must remove the intent marker")
	}
}

// Unconfigured persistence must be fully inert — SetOffline still works
// (flag only), PersistedOfflineIntent is false, no files written.
func TestOfflineIntentUnconfiguredInert(t *testing.T) {
	SetOfflinePersistPath("")
	t.Cleanup(func() { SetOffline(false) })
	SetOffline(true)
	if !IsUserOffline() {
		t.Fatal("SetOffline must still set the flag when persistence is unconfigured")
	}
	if PersistedOfflineIntent() {
		t.Fatal("unconfigured persistence must never report intent")
	}
}

// Batch-3 adversarial review #10: a marker that outlives the metadata mirror
// DB (an app-data reset deletes Application Support but not ~/.juicemount)
// must be treated as stale — cleared, so the boot proceeds online and takes
// the empty-mirror blocking sync — instead of serving an empty volume.
func TestDropStaleOfflineIntent(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "offline-intent")
	SetOfflinePersistPath(marker)
	t.Cleanup(func() {
		SetOffline(false)
		SetOfflinePersistPath("")
	})
	dbPath := filepath.Join(dir, "metadata.db")

	// Servable DB → marker honored, nothing dropped.
	SetOffline(true)
	if err := os.WriteFile(dbPath, []byte("sqlite"), 0o644); err != nil {
		t.Fatal(err)
	}
	if DropStaleOfflineIntent(dbPath) {
		t.Fatal("marker dropped despite a servable mirror DB")
	}
	if !PersistedOfflineIntent() {
		t.Fatal("marker must survive a servable-DB check")
	}

	// Zero-length DB → stale: cleared and reported.
	if err := os.WriteFile(dbPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !DropStaleOfflineIntent(dbPath) {
		t.Fatal("zero-length mirror DB must mark the intent stale")
	}
	if PersistedOfflineIntent() {
		t.Fatal("stale marker must be removed (empty-DB case)")
	}

	// Missing DB → stale.
	SetOffline(true)
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}
	if !DropStaleOfflineIntent(dbPath) {
		t.Fatal("missing mirror DB must mark the intent stale")
	}
	if PersistedOfflineIntent() {
		t.Fatal("stale marker must be removed (missing-DB case)")
	}

	// No marker present → no-op false.
	if DropStaleOfflineIntent(dbPath) {
		t.Fatal("no marker: DropStaleOfflineIntent must be a no-op")
	}

	// Unconfigured persistence → fully inert.
	SetOfflinePersistPath("")
	if DropStaleOfflineIntent(dbPath) {
		t.Fatal("unconfigured persistence must be inert")
	}
}
