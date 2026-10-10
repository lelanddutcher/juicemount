package pin

import (
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
)

func TestIsolatedOfflineIntentPrivateAndLinkRefusal(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(account.HomeDir, "Developer", ".state")
	if st, err := os.Stat(base); err != nil || !st.IsDir() {
		t.Skip("private fixture base unavailable")
	}
	root, err := os.MkdirTemp(base, "jm-offline-i-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Mkdir(filepath.Join(root, "tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, appisolation.Marker), []byte("JuiceMount isolated state v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_APP_ISOLATION", "1")
	t.Setenv("JM_APP_ISOLATION_ROOT", root)
	t.Setenv("TMPDIR", filepath.Join(root, "tmp"))
	marker := filepath.Join(root, ".juicemount", "offline-intent")
	outside := filepath.Join(root, "outside-sentinel")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	SetOfflinePersistPath(marker)
	t.Cleanup(func() { SetOffline(false); SetOfflinePersistPath("") })
	SetOffline(true)
	if !PersistedOfflineIntent() {
		t.Fatal("private offline marker not persisted")
	}
	if st, err := os.Lstat(marker); err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		t.Fatalf("private marker type/mode: %v, %v", st, err)
	}
	SetOffline(false)
	if _, err := os.Lstat(marker); !os.IsNotExist(err) {
		t.Fatalf("private marker survived toggle off: %v", err)
	}
	if err := os.Symlink(outside, marker); err != nil {
		t.Fatal(err)
	}
	SetOffline(true)
	if PersistedOfflineIntent() {
		t.Fatal("linked offline marker accepted")
	}
	if st, err := os.Lstat(marker); err != nil || st.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("linked marker was followed or replaced: %v, %v", st, err)
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "keep" {
		t.Fatalf("outside sentinel changed: %q, %v", got, err)
	}
}

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

func TestOfflineIntentCreatesMissingParentAndClears(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "new", ".juicemount")
	marker := filepath.Join(parent, "offline-intent")
	SetOfflinePersistPath(marker)
	t.Cleanup(func() {
		SetOffline(false)
		SetOfflinePersistPath("")
	})
	if _, err := os.Stat(parent); !os.IsNotExist(err) {
		t.Fatalf("parent should start absent, stat error = %v", err)
	}
	SetOffline(true)
	if !IsUserOffline() || !PersistedOfflineIntent() {
		t.Fatal("toggle-on did not engage and persist offline intent")
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != "user-offline\n" {
		t.Fatalf("persisted marker = %q, %v", got, err)
	}
	if info, err := os.Stat(parent); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("new marker parent mode = %v, %v", info, err)
	}
	SetOffline(false)
	if IsUserOffline() || PersistedOfflineIntent() {
		t.Fatal("toggle-off did not clear offline intent")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker should be removed, stat error = %v", err)
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
