package manager

// Review 5: the store's Manager ID wins over the ID file (X3), and an empty
// read of reload.ack keeps the time the ack line was first seen (N13).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// X3: Manager A's principal store copied onto host B, where B already
// started once (it minted its own ID file). The store's enrollments were made
// under A's ID, so B must serve A's ID, and its ID file must follow.
func TestH8BStoreCopiedOntoStartedManagerKeepsItsID(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	sa, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(a, "principals-store.json")})
	if err != nil {
		t.Fatal(err)
	}
	enrollTestMac(t, sa, 0)
	idA := sa.ManagerID()
	sb, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(b, "principals-store.json")})
	if err != nil {
		t.Fatal(err)
	}
	if sb.ManagerID() == idA {
		t.Fatal("setup: two Managers minted the same ID")
	}
	raw, err := os.ReadFile(filepath.Join(a, "principals-store.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "principals-store.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	sb2, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(b, "principals-store.json")})
	if err != nil {
		t.Fatal(err)
	}
	if sb2.ManagerID() != idA {
		t.Fatalf("X3: Manager B serves the copied store under its own ID %s, not the store's %s", sb2.ManagerID()[:6], idA[:6])
	}
	if got := strings.TrimSpace(readString(t, filepath.Join(b, "principals-store.manager-id"))); got != idA {
		t.Fatalf("X3: B's ID file was not synced to the store's ID: %q, want %q", got, idA)
	}
}

// N13: the entrypoint may truncate reload.ack before it writes it. An empty
// read in that window must keep the ack line already seen and the time it was
// first seen; otherwise the next read of the same line restarts
// proxyOutdatedGrace and an outdated image is reported late, or never while
// the reads keep alternating.
func TestH8BEmptyAckReadKeepsSeenTime(t *testing.T) {
	oldGrace := proxyOutdatedGrace
	proxyOutdatedGrace = 200 * time.Millisecond
	t.Cleanup(func() { proxyOutdatedGrace = oldGrace })
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const line = "0123abcd 1 2"
	ackPath := filepath.Join(dir, reloadAckName)
	if err := os.WriteFile(ackPath, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	state := func() proxyState {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.proxyStateLocked()
	}
	// A whole-line ack of our request and no heartbeat: the a861a46 image.
	s.mu.Lock()
	s.issued = append(s.issued, issuedRequest{line: line, at: time.Now().Add(-time.Minute)})
	s.mu.Unlock()
	if got := state(); got == proxyOutdated {
		t.Fatal("setup: outdated before the grace ran")
	}
	time.Sleep(proxyOutdatedGrace + 100*time.Millisecond)
	if got := state(); got != proxyOutdated {
		t.Fatalf("setup: state after the grace = %v, want outdated", got)
	}
	if err := os.WriteFile(ackPath, nil, 0o600); err != nil { // truncated
		t.Fatal(err)
	}
	_ = state()
	if err := os.WriteFile(ackPath, []byte(line+"\n"), 0o600); err != nil { // written again
		t.Fatal(err)
	}
	if got := state(); got != proxyOutdated {
		t.Fatalf("N13: after an empty read of reload.ack the same line is no longer outdated (state %v): the seen time was reset", got)
	}
}
