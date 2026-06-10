package nfs

// LB-4 (Phase 3b): the membuf budget/threshold used to be hardcoded at
// handler construction while the app's preferences pretended otherwise.
// These tests pin the new construction contract: WithMemBufLimits flows
// into the MemoryBuffer, and zero values (old config JSON) keep the
// package defaults exactly.

import (
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

func newOptionsTestStore(t *testing.T) *metadata.Store {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("open metadata store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestNewHandlerDefaultMemBufLimits(t *testing.T) {
	store := newOptionsTestStore(t)
	h := NewHandler(store, t.TempDir())
	t.Cleanup(h.StopHandler)

	if h.memBuf.threshold != DefaultMemBufThreshold {
		t.Fatalf("threshold = %d, want default %d", h.memBuf.threshold, DefaultMemBufThreshold)
	}
	if h.memBuf.budget != DefaultMemBufBudget {
		t.Fatalf("budget = %d, want default %d", h.memBuf.budget, DefaultMemBufBudget)
	}
}

func TestNewHandlerWithMemBufLimits(t *testing.T) {
	store := newOptionsTestStore(t)
	wantThreshold := int64(256) << 20 // 256 MB
	wantBudget := int64(4096) << 20   // 4 GiB
	h := NewHandler(store, t.TempDir(), WithMemBufLimits(wantThreshold, wantBudget))
	t.Cleanup(h.StopHandler)

	if h.memBuf.threshold != wantThreshold {
		t.Fatalf("threshold = %d, want %d", h.memBuf.threshold, wantThreshold)
	}
	if h.memBuf.budget != wantBudget {
		t.Fatalf("budget = %d, want %d", h.memBuf.budget, wantBudget)
	}
}

func TestNewHandlerZeroMemBufLimitsKeepDefaults(t *testing.T) {
	// 0 is what an old config JSON (fields absent) produces after the
	// bridge's MB→bytes conversion — it must mean "previous behavior",
	// not "buffer nothing".
	store := newOptionsTestStore(t)
	h := NewHandler(store, t.TempDir(), WithMemBufLimits(0, 0))
	t.Cleanup(h.StopHandler)

	if h.memBuf.threshold != DefaultMemBufThreshold || h.memBuf.budget != DefaultMemBufBudget {
		t.Fatalf("zero limits resolved to (%d, %d), want defaults (%d, %d)",
			h.memBuf.threshold, h.memBuf.budget, DefaultMemBufThreshold, DefaultMemBufBudget)
	}
}
