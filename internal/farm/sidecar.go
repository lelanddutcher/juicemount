package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// ManifestSidecar is the self-describing index the farm writes to the volume next
// to an asset's blobs (<inode>/manifest.json) — the JM-15 discovery mechanism. It
// carries the COMPLETE current manifest for the inode so the Mac control plane can
// reconcile server-generated derivatives into its own Tier-B (its local
// derivatives.db, which it serves /derivatives from) WITHOUT a second SQLite
// handle on the network FS. The Mac replays these rows through the same idempotent
// IngestTech/PutDeriv upsert the farm used, so a reconciled row is byte-identical
// to a farm-produced one. Re-written (idempotent) after every generation pass.
type ManifestSidecar struct {
	Inode       uint64                 `json:"inode"`
	SourceHash  *string                `json:"source_hash"`
	Derivatives []derivatives.DerivRow `json:"derivatives"`
	WrittenAt   int64                  `json:"written_at"` // unix seconds the farm last refreshed the sidecar
}

// ReconcileResult reports a JM-15 reconcile pass.
type ReconcileResult struct {
	Sidecars int // sidecars found
	Assets   int // assets ingested
	Rows     int // derivative rows ingested
	Errs     int // sidecars that failed to parse/ingest
}

// ReconcileSidecars is the JM-15 Mac-side reconcile: it walks the volume's
// per-inode manifest.json sidecars (written by the farm) and ingests them into
// the local store — PutSource(inode, source_hash) + PutDeriv(each row). The
// running app serves the SAME db (WAL concurrent reads), so reconciled rows
// appear in /derivatives immediately, with no app restart. Idempotent (upserts).
func ReconcileSidecars(store *derivatives.Store, mount string) (ReconcileResult, error) {
	var res ReconcileResult
	base := filepath.Join(mount, ".juicemount", "derivatives")
	entries, err := os.ReadDir(base)
	if err != nil {
		return res, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(base, e.Name(), "manifest.json"))
		if err != nil {
			continue // no sidecar for this inode (blob-only dir)
		}
		res.Sidecars++
		var sc ManifestSidecar
		if json.Unmarshal(raw, &sc) != nil || sc.Inode == 0 {
			res.Errs++
			continue
		}
		if err := store.PutSource(sc.Inode, sc.SourceHash); err != nil {
			res.Errs++
			continue
		}
		ok := true
		for _, row := range sc.Derivatives {
			if err := store.PutDeriv(sc.Inode, row); err != nil {
				ok = false
				break
			}
			res.Rows++
		}
		if ok {
			res.Assets++
		} else {
			res.Errs++
		}
	}
	return res, nil
}

// WriteManifestSidecar serializes the asset's current manifest (source hash + all
// derivative rows) to <mount>/.juicemount/derivatives/<inode>/manifest.json. Call
// it AFTER the store writes for a pass, so the sidecar reflects the full,
// committed state. Best-effort: the caller logs but does not fail generation on a
// sidecar error (the row is already in the local/server db; the sidecar is the
// cross-host mirror, recoverable on the next pass).
func WriteManifestSidecar(store *derivatives.Store, mount string, inode uint64) error {
	known, srcHash := store.Known(inode)
	if !known {
		return nil
	}
	rows, err := store.Manifest(inode)
	if err != nil {
		return err
	}
	if rows == nil {
		rows = []derivatives.DerivRow{}
	}
	sc := ManifestSidecar{
		Inode:       inode,
		SourceHash:  srcHash,
		Derivatives: rows,
		WrittenAt:   time.Now().Unix(),
	}
	b, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(DerivBlobDir(mount, inode), "manifest.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
