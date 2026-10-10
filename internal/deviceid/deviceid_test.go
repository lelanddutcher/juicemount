package deviceid

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"testing"
)

// The app's DeviceFingerprint checks the same file.
func TestDigestMatchesSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../credstore/testdata/device_fingerprint_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Vectors []struct {
			UUID   string `json:"platform_uuid"`
			Digest string `json:"digest"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &v); err != nil || len(v.Vectors) == 0 {
		t.Fatalf("vectors: %v", err)
	}
	for _, c := range v.Vectors {
		if got := Digest(c.UUID); got != c.Digest {
			t.Errorf("Digest(%q) = %q, want %q", c.UUID, got, c.Digest)
		}
	}
}

func TestFingerprintSourceInjectable(t *testing.T) {
	restore := SetSourceForTest(func() (string, error) { return "", ErrUnavailable })
	if _, err := Fingerprint(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unavailable source: %v", err)
	}
	restore()
	t.Cleanup(SetSourceForTest(func() (string, error) { return "  ", nil }))
	if _, err := Fingerprint(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("blank UUID accepted: %v", err)
	}
}

// Reads this Mac's IOPlatformUUID (read-only IOKit property; no Keychain).
func TestRealFingerprintIsStable(t *testing.T) {
	a, err := Fingerprint()
	if errors.Is(err, ErrUnavailable) {
		t.Skip("no hardware UUID on this platform")
	}
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Fingerprint()
	if a != b || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(a) {
		t.Fatalf("fingerprint %q / %q", a, b)
	}
}
