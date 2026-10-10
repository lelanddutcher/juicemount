// Package deviceid binds JuiceMount's per-install identity to the Mac it runs
// on.
//
// Under H-8 option B each Mac enrolls with the Manager as its own principal,
// named from the instance ID (internal/cplane), and keeps that principal's
// secret in the credential store (internal/credstore and the app's
// RedisCredentialStore.swift). Both live in files that Migration Assistant,
// a Time Machine restore onto another Mac, or a cloned disk copy verbatim, so
// without a hardware check two Macs would share one principal and one secret.
//
// The fingerprint is a digest of the IOPlatformUUID (IOKit,
// kIOPlatformUUIDKey): stable for one Mac across reinstalls and OS updates,
// different on every other Mac. Only the digest is stored, never the UUID.
// The app computes the same digest (DeviceFingerprint in
// RedisCredentialStore.swift); both sides are checked against
// internal/credstore/testdata/device_fingerprint_vectors.json.
//
// Threat model: this catches accidental copies of an install (Migration
// Assistant, a Time Machine restore, a cloned disk); it does not stop a
// process running as the same user from deliberately rewriting the stored
// digest files.
package deviceid

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
)

// ErrUnavailable means this process cannot read a hardware identifier (a
// non-darwin build, or IOKit refused). Callers then neither rotate nor adopt:
// with no fingerprint there is nothing to compare.
var ErrUnavailable = errors.New("device fingerprint unavailable")

var (
	mu     sync.RWMutex
	source = platformUUID
)

// Fingerprint returns this Mac's device digest.
func Fingerprint() (string, error) {
	mu.RLock()
	src := source
	mu.RUnlock()
	raw, err := src()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(raw) == "" {
		return "", ErrUnavailable
	}
	return Digest(raw), nil
}

// Digest is the stored form of a hardware UUID: the first 128 bits of
// SHA-256 over a versioned prefix and the upper-cased, trimmed UUID, as
// lower-case hex.
func Digest(platformUUID string) string {
	sum := sha256.Sum256([]byte("juicemount-device-v1\n" + strings.ToUpper(strings.TrimSpace(platformUUID))))
	return hex.EncodeToString(sum[:16])
}

// SetSourceForTest replaces the hardware read (the raw platform UUID, or an
// error) and returns a function restoring the real one. Tests only.
func SetSourceForTest(f func() (string, error)) (restore func()) {
	mu.Lock()
	prev := source
	source = f
	mu.Unlock()
	return func() {
		mu.Lock()
		source = prev
		mu.Unlock()
	}
}
