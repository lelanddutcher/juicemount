package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// TestReconcileOneSidecar covers the on-miss lazy reconcile (the auto-reconcile
// bridge): a sidecar present on the (fake) volume for an inode the store doesn't
// know yet is ingested on demand; an absent sidecar is a clean (false, nil); and
// it's idempotent.
func TestReconcileOneSidecar(t *testing.T) {
	mount := t.TempDir()
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	const inode = uint64(424242)

	// 1. Absent sidecar → (false, nil), store stays empty.
	if found, err := ReconcileOneSidecar(store, mount, inode); err != nil || found {
		t.Fatalf("absent sidecar: want (false,nil), got (%v,%v)", found, err)
	}
	if known, _ := store.Known(inode); known {
		t.Fatal("store should not know an inode with no sidecar")
	}

	// 2. Write a sidecar exactly where the farm would.
	hash, blob := "abc123", "strip.jpg" // the farm writes strip.jpg; "filmstrip.jpg" was a wrong fixture
	sc := ManifestSidecar{
		Inode:      inode,
		SourceHash: strptr("a1b2c3d4e5f60718"),
		Derivatives: []derivatives.DerivRow{
			{Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1, Hash: &hash, BlobRelPath: &blob},
			{Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1},
		},
		WrittenAt: 1,
	}
	dir := filepath.Join(mount, derivatives.DerivDirRel(inode))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(sc, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	// 3. On-miss reconcile → found, store now serves the rows + source hash.
	found, err := ReconcileOneSidecar(store, mount, inode)
	if err != nil || !found {
		t.Fatalf("present sidecar: want (true,nil), got (%v,%v)", found, err)
	}
	known, srcHash := store.Known(inode)
	if !known || srcHash == nil || *srcHash != "a1b2c3d4e5f60718" {
		t.Fatalf("after reconcile: known=%v srcHash=%v", known, srcHash)
	}
	rows, err := store.Manifest(inode)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 derivative rows, got %d", len(rows))
	}

	// 4. Idempotent: a second call still (true, nil), no duplicate rows.
	if found, err := ReconcileOneSidecar(store, mount, inode); err != nil || !found {
		t.Fatalf("idempotent call: got (%v,%v)", found, err)
	}
	if rows2, _ := store.Manifest(inode); len(rows2) != 2 {
		t.Fatalf("idempotent: want 2 rows, got %d", len(rows2))
	}
}

// TestReconcileSidecarsUsesSharedCore verifies the refactor kept the full-walk
// behavior: a volume with two inode sidecars reconciles both.
func TestReconcileSidecarsUsesSharedCore(t *testing.T) {
	mount := t.TempDir()
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	for _, inode := range []uint64{111, 222} {
		sc := ManifestSidecar{Inode: inode, SourceHash: strptr("00112233445566aa"), Derivatives: []derivatives.DerivRow{{Kind: "tech", Status: "ready", Producer: "linux-farm", Version: 1}}, WrittenAt: 1}
		dir := filepath.Join(mount, derivatives.DerivDirRel(inode))
		_ = os.MkdirAll(dir, 0o755)
		b, _ := json.MarshalIndent(sc, "", "  ")
		_ = os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o644)
	}
	res, err := ReconcileSidecars(store, mount)
	if err != nil {
		t.Fatal(err)
	}
	if res.Assets != 2 || res.Sidecars != 2 {
		t.Fatalf("want 2 assets/sidecars, got %+v", res)
	}
}

// A manifest must never redirect its writes to another asset. The directory is
// the authority for which inode we are reconciling; the body's `inode` is
// untrusted input, authorable by any client with mount write access — which is
// the premise of Tier 1 contribute-back, not a hypothetical (2026-08-04 review).
func TestReconcileRefusesManifestWhoseInodeDisagreesWithItsDirectory(t *testing.T) {
	dir := t.TempDir()
	store, err := derivatives.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	const dirInode = uint64(123)
	const victimInode = uint64(456)

	blobDir := filepath.Join(dir, derivatives.DerivDirRel(dirInode))
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	hash := "deadbeefdeadbeef"
	sc := ManifestSidecar{
		Inode:      victimInode, // lies: this file lives under dirInode
		SourceHash: &hash,
		Derivatives: []derivatives.DerivRow{{
			Kind: "filmstrip", Status: "ready", Producer: "linux-farm", Version: 1,
		}},
	}
	b, mErr := json.Marshal(sc)
	if mErr != nil {
		t.Fatal(mErr)
	}
	if err := os.WriteFile(filepath.Join(blobDir, "manifest.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	res := reconcileOneSidecarInto(store, dir, dirInode)
	if res.Rows != 0 || res.Assets != 0 {
		t.Errorf("mismatched manifest was ingested: rows=%d assets=%d", res.Rows, res.Assets)
	}
	if res.Errs == 0 {
		t.Error("expected the mismatch to be counted as an error")
	}
	// The victim must have no rows written on its behalf.
	if rows, err := store.Manifest(victimInode); err == nil && len(rows) > 0 {
		t.Errorf("rows were written for inode %d from a manifest under %d: %+v", victimInode, dirInode, rows)
	}
}

// The reviewer's exact attack (2026-08-04, CRITICAL): manifest.json lives on a
// volume any consumer can write, and reconcile used to hand its rows straight to
// PutDeriv. A row declaring media_type "text/html" plus an HTML blob therefore
// made /blob serve attacker HTML with that Content-Type from the control-plane
// origin — bypassing every guard on POST /derivatives/register.
func TestSanitizeSidecarRow_RejectsPolicyFromTheVolume(t *testing.T) {
	str := func(s string) *string { return &s }

	t.Run("attacker media_type is overridden, never honoured", func(t *testing.T) {
		evil := "text/html"
		row := derivatives.DerivRow{
			Kind: "proxy", Status: "ready", Producer: "linux-farm",
			BlobRelPath: str("proxy.mp4"), MediaType: &evil,
		}
		got, ok := sanitizeSidecarRow(row)
		if !ok {
			t.Fatal("a well-formed proxy row should be ingestable")
		}
		if got.MediaType == nil || *got.MediaType != "video/mp4" {
			t.Errorf("media type = %v, want the server-pinned video/mp4", got.MediaType)
		}
	})

	t.Run("arbitrary blob filename is refused", func(t *testing.T) {
		row := derivatives.DerivRow{
			Kind: "proxy", Status: "ready", Producer: "linux-farm",
			BlobRelPath: str("pwn.html"), MediaType: str("text/html"),
		}
		if _, ok := sanitizeSidecarRow(row); ok {
			t.Error("a row naming a non-reserved blob file must be dropped")
		}
	})

	t.Run("unknown kind is refused", func(t *testing.T) {
		row := derivatives.DerivRow{Kind: "totally-made-up", Status: "ready", Producer: "linux-farm"}
		if _, ok := sanitizeSidecarRow(row); ok {
			t.Error("an unknown kind must be dropped")
		}
	})

	t.Run("no blob kind may declare a scriptable type", func(t *testing.T) {
		for kind := range blobMediaTypes {
			got, ok := sanitizeSidecarRow(derivatives.DerivRow{
				Kind: kind, Status: "ready", Producer: "linux-farm",
				MediaType: str("text/html"),
			})
			if !ok {
				continue
			}
			switch *got.MediaType {
			case "text/html", "application/javascript", "image/svg+xml":
				t.Errorf("kind %q sanitized to scriptable %q", kind, *got.MediaType)
			}
		}
	})

	// Legitimate shapes the farm actually writes must survive.
	t.Run("farm-written shapes survive", func(t *testing.T) {
		for _, row := range []derivatives.DerivRow{
			{Kind: "filmstrip", Status: "ready", Producer: "linux-farm", BlobRelPath: str("strip.jpg")},
			{Kind: "thumbnail", Status: "failed", Producer: "linux-farm"},
			{Kind: "tech", Status: "ready", Producer: "linux-farm"},
			{Kind: "ai", Status: "ready", Producer: "on-device", BlobRelPath: str(derivatives.AIBlobNameLegacy)},
		} {
			if _, ok := sanitizeSidecarRow(row); !ok {
				t.Errorf("legitimate row dropped: kind=%q status=%q", row.Kind, row.Status)
			}
		}
	})
}
