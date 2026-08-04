package nfs

import (
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/metadata"
)

// The free-disk clamp must be able to RECOVER, not just tighten.
//
// Found live on 2026-08-04: the app started with 21 GiB free, the constructor
// baked that clamp into the configured capacity (1.49 GiB), and when disk
// recovered to 25.5 GiB the spool was STILL capped at 1.49 GiB — because
// effectiveCapacity only ever takes min(configured, ceiling) and the configured
// value was itself a startup snapshot. A 2 GiB copy would stall with ~5.4 GiB of
// real headroom available. Same startup-snapshot bug the live clamp was built to
// fix, in the opposite direction.
func TestConfiguredCapacityIsNotClampedAtConstruction(t *testing.T) {
	dir := t.TempDir()
	db := openTestDB(t, filepath.Join(t.TempDir(), "m.db"))
	if err := metadata.InitSpoolSchema(db); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	meta := metadata.NewSpoolStore(db)

	// A capacity far above any plausible free disk on the test machine, so the
	// constructor would certainly have clamped it.
	const huge = int64(1) << 50 // 1 PiB
	s, err := NewSpoolStore(dir, huge, meta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)

	if got := s.ConfiguredCapacity(); got != huge {
		t.Errorf("configured capacity = %d, want %d unchanged — clamping it at "+
			"construction makes the limit a one-way ratchet that cannot recover "+
			"when disk frees up", got, huge)
	}

	// Admission is still bounded: effectiveCapacity clamps live against real
	// free disk, which is what actually protects against ENOSPC.
	eff := s.effectiveCapacity()
	if eff <= 0 {
		t.Fatalf("effective capacity = %d, want a positive live bound", eff)
	}
	if eff >= huge {
		t.Errorf("effective capacity = %d is not clamped below the configured %d — "+
			"the live bound is what prevents a mid-copy ENOSPC", eff, huge)
	}
}
