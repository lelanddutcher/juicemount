package nfs

import (
	"os"
	"testing"
	"time"
)

func TestVerifierEviction(t *testing.T) {
	h := &JuiceMountHandler{
		verifiers:    make(map[string]verifierData),
		verifierStop: make(chan struct{}),
	}

	// Create some verifier entries with lastAccess = now.
	now := time.Now()
	h.verifierMu.Lock()
	h.verifiers["/dir/a"] = verifierData{
		verifier:   1,
		entries:    []os.FileInfo{},
		lastAccess: now,
	}
	h.verifiers["/dir/b"] = verifierData{
		verifier:   2,
		entries:    []os.FileInfo{},
		lastAccess: now,
	}
	h.verifierMu.Unlock()

	// Verify they exist.
	h.verifierMu.Lock()
	if len(h.verifiers) != 2 {
		t.Fatalf("expected 2 verifiers, got %d", len(h.verifiers))
	}
	h.verifierMu.Unlock()

	// Simulate TTL expiration by setting lastAccess far in the past.
	h.verifierMu.Lock()
	old := h.verifiers["/dir/a"]
	old.lastAccess = now.Add(-10 * time.Minute)
	h.verifiers["/dir/a"] = old
	h.verifierMu.Unlock()

	// Trigger cleanup with a 5-minute TTL.
	h.evictStaleVerifiers(5 * time.Minute)

	// /dir/a should be evicted; /dir/b should remain.
	h.verifierMu.Lock()
	defer h.verifierMu.Unlock()

	if _, ok := h.verifiers["/dir/a"]; ok {
		t.Error("/dir/a should have been evicted (lastAccess > 5 min ago)")
	}
	if _, ok := h.verifiers["/dir/b"]; !ok {
		t.Error("/dir/b should still be present (lastAccess is recent)")
	}
}

func TestVerifierEvictionAll(t *testing.T) {
	h := &JuiceMountHandler{
		verifiers:    make(map[string]verifierData),
		verifierStop: make(chan struct{}),
	}

	past := time.Now().Add(-20 * time.Minute)
	h.verifierMu.Lock()
	for i := 0; i < 10; i++ {
		key := "/stale/" + string(rune('a'+i))
		h.verifiers[key] = verifierData{
			verifier:   uint64(i),
			entries:    []os.FileInfo{},
			lastAccess: past,
		}
	}
	h.verifierMu.Unlock()

	h.evictStaleVerifiers(5 * time.Minute)

	h.verifierMu.Lock()
	remaining := len(h.verifiers)
	h.verifierMu.Unlock()

	if remaining != 0 {
		t.Errorf("expected all verifiers evicted, got %d remaining", remaining)
	}
}

func TestVerifierDataForVerifierUpdatesLastAccess(t *testing.T) {
	h := &JuiceMountHandler{
		verifiers:    make(map[string]verifierData),
		verifierStop: make(chan struct{}),
	}

	past := time.Now().Add(-4 * time.Minute)
	h.verifierMu.Lock()
	h.verifiers["/active"] = verifierData{
		verifier:   42,
		entries:    []os.FileInfo{},
		lastAccess: past,
	}
	h.verifierMu.Unlock()

	// DataForVerifier should update lastAccess.
	result := h.DataForVerifier("/active", 42)
	if result == nil {
		t.Fatal("expected non-nil result from DataForVerifier")
	}

	// After access, eviction with 5-min TTL should NOT evict it.
	h.evictStaleVerifiers(5 * time.Minute)

	h.verifierMu.Lock()
	_, ok := h.verifiers["/active"]
	h.verifierMu.Unlock()
	if !ok {
		t.Error("/active should NOT have been evicted after DataForVerifier refreshed lastAccess")
	}
}
