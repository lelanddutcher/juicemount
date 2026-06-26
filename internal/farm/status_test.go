package farm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

func i64p(v int64) *int64 { return &v }

// writeProxyBlob materializes a proxy.mp4 of n bytes at the inode's Tier-A blob
// dir under mount, so ComputeProxyEconomics can os.Stat real bytes.
func writeProxyBlob(t *testing.T, mount string, inode uint64, n int) {
	t.Helper()
	dir := DerivBlobDir(mount, inode)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir blob dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "proxy.mp4"), make([]byte, n), 0o644); err != nil {
		t.Fatalf("write proxy blob: %v", err)
	}
}

func openStore(t *testing.T) *derivatives.Store {
	t.Helper()
	s, err := derivatives.Open(":memory:")
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestComputeProxyEconomics: a good proxy (big saving) + a bloat proxy (iPhone/
// h264 case, tiny saving) → measured=2, aggregate bytes, an avg saving %, and the
// bloat list carries ONLY the under-threshold offender.
func TestComputeProxyEconomics(t *testing.T) {
	mount := t.TempDir()
	s := openStore(t)

	// inode 1: 1,000,000 source → 200,000 proxy = 80% saving (healthy).
	if err := s.PutDeriv(1, derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, SourceSize: i64p(1_000_000)}); err != nil {
		t.Fatalf("PutDeriv 1: %v", err)
	}
	writeProxyBlob(t, mount, 1, 200_000)
	// inode 2: 1,000,000 source → 950,000 proxy = 5% saving (BLOAT, < 25%).
	if err := s.PutDeriv(2, derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1, SourceSize: i64p(1_000_000)}); err != nil {
		t.Fatalf("PutDeriv 2: %v", err)
	}
	writeProxyBlob(t, mount, 2, 950_000)

	econ, err := ComputeProxyEconomics(s, mount)
	if err != nil {
		t.Fatalf("ComputeProxyEconomics: %v", err)
	}
	if econ == nil {
		t.Fatal("econ = nil, want a rollup")
	}
	if econ.Measured != 2 {
		t.Errorf("Measured = %d, want 2", econ.Measured)
	}
	if econ.TotalSourceBytes != 2_000_000 || econ.TotalProxyBytes != 1_150_000 {
		t.Errorf("totals = src %d / proxy %d, want 2000000 / 1150000", econ.TotalSourceBytes, econ.TotalProxyBytes)
	}
	// avg = 1 - 1150000/2000000 = 42%.
	if econ.AvgSavingPct != 42 {
		t.Errorf("AvgSavingPct = %d, want 42", econ.AvgSavingPct)
	}
	if len(econ.Bloat) != 1 || econ.Bloat[0].Inode != 2 || econ.Bloat[0].SavingPct != 5 {
		t.Errorf("Bloat = %+v, want one row inode=2 saving=5", econ.Bloat)
	}
}

// TestComputeProxyEconomicsSkips: rows with no source_size, a missing blob, or a
// failed status are not measurable; an all-unmeasurable set returns nil so the
// field is omitted on the wire.
func TestComputeProxyEconomicsSkips(t *testing.T) {
	mount := t.TempDir()
	s := openStore(t)

	// No source_size → skipped (even with a blob present).
	if err := s.PutDeriv(1, derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "p", Version: 1}); err != nil {
		t.Fatalf("PutDeriv 1: %v", err)
	}
	writeProxyBlob(t, mount, 1, 100)
	// Has source_size but NO blob on disk → skipped.
	if err := s.PutDeriv(2, derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "p", Version: 1, SourceSize: i64p(500_000)}); err != nil {
		t.Fatalf("PutDeriv 2: %v", err)
	}
	// Failed proxy → never listed by ListProxyRows.
	if err := s.PutDeriv(3, derivatives.DerivRow{Kind: "proxy", Status: "failed", Producer: "p", Version: 1, SourceSize: i64p(500_000)}); err != nil {
		t.Fatalf("PutDeriv 3: %v", err)
	}
	econ, err := ComputeProxyEconomics(s, mount)
	if err != nil {
		t.Fatalf("ComputeProxyEconomics: %v", err)
	}
	if econ != nil {
		t.Errorf("econ = %+v, want nil (nothing measurable)", econ)
	}
	// "" mount short-circuits to nil too.
	if econ, _ := ComputeProxyEconomics(s, ""); econ != nil {
		t.Errorf("econ with empty mount = %+v, want nil", econ)
	}
}

// TestBloatCapAndOrder: more than maxBloatRows offenders → list capped, sorted
// worst (smallest saving) first.
func TestBloatCapAndOrder(t *testing.T) {
	mount := t.TempDir()
	s := openStore(t)
	// 25 bloat proxies with descending saving 0..24% (all < threshold).
	n := maxBloatRows + 5
	for i := 0; i < n; i++ {
		inode := uint64(100 + i)
		src := int64(1_000_000)
		proxy := int(src) - i*10_000 // i=0 → 0% saving, i=24 → 24% saving
		if err := s.PutDeriv(inode, derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "p", Version: 1, SourceSize: &src}); err != nil {
			t.Fatalf("PutDeriv %d: %v", inode, err)
		}
		writeProxyBlob(t, mount, inode, proxy)
	}
	econ, err := ComputeProxyEconomics(s, mount)
	if err != nil {
		t.Fatalf("ComputeProxyEconomics: %v", err)
	}
	if len(econ.Bloat) != maxBloatRows {
		t.Fatalf("Bloat len = %d, want capped at %d", len(econ.Bloat), maxBloatRows)
	}
	// Worst-first: first row is the 0%-saving offender, ascending thereafter.
	if econ.Bloat[0].SavingPct != 0 {
		t.Errorf("Bloat[0].SavingPct = %d, want 0 (worst first)", econ.Bloat[0].SavingPct)
	}
	for i := 1; i < len(econ.Bloat); i++ {
		if econ.Bloat[i].SavingPct < econ.Bloat[i-1].SavingPct {
			t.Errorf("Bloat not ascending at %d: %d < %d", i, econ.Bloat[i].SavingPct, econ.Bloat[i-1].SavingPct)
		}
	}
}
