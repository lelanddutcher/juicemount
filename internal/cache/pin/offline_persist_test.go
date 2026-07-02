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
