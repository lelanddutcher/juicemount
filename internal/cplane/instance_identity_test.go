package cplane

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

const (
	macA = "AAAAAAAA-0000-4000-8000-00000000000A"
	macB = "BBBBBBBB-0000-4000-8000-00000000000B"
)

func onMac(t *testing.T, uuid string) {
	t.Helper()
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return uuid, nil }))
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// A copied instance record (Migration Assistant, Time Machine onto another
// Mac, cloned disk) must not give the second Mac the first Mac's principal.
func TestInstanceIdentityRotatesOnDeviceMismatch(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "metadata.db")
	unrelated := filepath.Join(dir, "metadata.db")
	if err := os.WriteFile(unrelated, []byte("cache"), 0o644); err != nil {
		t.Fatal(err)
	}

	onMac(t, macA)
	a := LoadInstanceIdentity(db)
	if a.Rotated || a.Adopted || a.ID == "" {
		t.Fatalf("first mint on A = %+v", a)
	}

	onMac(t, macB) // the same files, now on another Mac
	b := LoadInstanceIdentity(db)
	if !b.Rotated {
		t.Fatalf("clone not detected: B kept %q (A had %q)", b.ID, a.ID)
	}
	if b.ID == a.ID || b.ID == "" {
		t.Fatalf("clone kept the copied instance ID %q", b.ID)
	}
	if got := readFile(t, filepath.Join(dir, instanceIDFile)); got != b.ID {
		t.Fatalf("rotated ID not persisted: %q", got)
	}
	if got := readFile(t, filepath.Join(dir, instanceDeviceFile)); got != deviceid.Digest(macB) {
		t.Fatalf("rotated ID not bound to B: %q", got)
	}
	n, ok := ReadCloneNotice(db)
	if !ok || n.PreviousInstanceID != a.ID || n.InstanceID != b.ID || n.Message != CloneMessage {
		t.Fatalf("clone notice = %+v, %v", n, ok)
	}
	if got := readFile(t, unrelated); got != "cache" {
		t.Fatalf("rotation touched unrelated data: %q", got)
	}

	// Stable afterwards: B keeps its new ID, and the notice stays until cleared.
	again := LoadInstanceIdentity(db)
	if again.ID != b.ID || again.Rotated {
		t.Fatalf("second load on B = %+v, want %q unrotated", again, b.ID)
	}
	if _, ok := ReadCloneNotice(db); !ok {
		t.Fatal("notice vanished before approval")
	}
	if err := ClearCloneNotice(db); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadCloneNotice(db); ok {
		t.Fatal("notice survived ClearCloneNotice")
	}
}

func TestInstanceIdentityKeptOnSameDevice(t *testing.T) {
	db := filepath.Join(t.TempDir(), "metadata.db")
	onMac(t, macA)
	first := LoadInstanceIdentity(db)
	for i := 0; i < 3; i++ {
		got := LoadInstanceIdentity(db)
		if got.ID != first.ID || got.Rotated || got.Adopted {
			t.Fatalf("load %d on the same Mac = %+v, want %q", i, got, first.ID)
		}
	}
	if _, ok := ReadCloneNotice(db); ok {
		t.Fatal("clone notice written on the same Mac")
	}
}

// An install from a build without the binding has instance-id and nothing
// else. It must keep its ID (no forced re-enrollment) and bind it once.
func TestInstanceIdentityUpgradeAdoptsOnce(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "metadata.db")
	const legacy = "11111111-2222-4333-8444-555555555555"
	if err := os.WriteFile(filepath.Join(dir, instanceIDFile), []byte(legacy+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	onMac(t, macA)
	got := LoadInstanceIdentity(db)
	if got.ID != legacy || got.Rotated || !got.Adopted {
		t.Fatalf("upgrade load = %+v, want %q adopted", got, legacy)
	}
	if b := readFile(t, filepath.Join(dir, instanceDeviceFile)); b != deviceid.Digest(macA) {
		t.Fatalf("upgrade did not bind: %q", b)
	}
	if next := LoadInstanceIdentity(db); next.ID != legacy || next.Adopted || next.Rotated {
		t.Fatalf("adoption not once: %+v", next)
	}
	if _, ok := ReadCloneNotice(db); ok {
		t.Fatal("upgrade wrote a clone notice")
	}
	// Once bound, a copy onto another Mac is a clone like any other.
	onMac(t, macB)
	if c := LoadInstanceIdentity(db); !c.Rotated || c.ID == legacy {
		t.Fatalf("adopted ID copied to B = %+v", c)
	}
}

// A crash after the notice and new ID were written but before the digest was
// bound finishes the same rotation instead of minting a third ID.
func TestInstanceIdentityCrashMidRotationResumes(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "metadata.db")
	onMac(t, macA)
	a := LoadInstanceIdentity(db)
	onMac(t, macB)
	b := LoadInstanceIdentity(db)
	// Simulate the crash: the digest is still A's.
	if err := writeAtomic(filepath.Join(dir, instanceDeviceFile), deviceid.Digest(macA)); err != nil {
		t.Fatal(err)
	}
	r := LoadInstanceIdentity(db)
	if r.ID != b.ID {
		t.Fatalf("resume minted %q, want %q", r.ID, b.ID)
	}
	if n, _ := ReadCloneNotice(db); n.PreviousInstanceID != a.ID {
		t.Fatalf("resume lost the original previous ID: %+v", n)
	}
}

func TestInstanceIdentityNoFingerprintKeepsID(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "metadata.db")
	onMac(t, macA)
	a := LoadInstanceIdentity(db)
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "", deviceid.ErrUnavailable }))
	if got := LoadInstanceIdentity(db); got.ID != a.ID || got.Rotated {
		t.Fatalf("unverifiable load = %+v, want %q", got, a.ID)
	}
}
