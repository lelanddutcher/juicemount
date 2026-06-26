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
	// Tech is the structured `tech` metadata payload (ffprobe JSON) so the
	// reconcile can repopulate /metadata?kind=tech — needed for the consumer's
	// D1 tech consume AND its AI size-guard fallback (serverSize ← tech.size_bytes
	// when the row's source_size isn't served by an older control plane).
	Tech      *TechSidecar `json:"tech,omitempty"`
	WrittenAt int64        `json:"written_at"` // unix seconds the farm last refreshed the sidecar
}

// TechSidecar mirrors the `tech` metadata row for the sidecar.
type TechSidecar struct {
	Producer string          `json:"producer"`
	Version  int             `json:"version"`
	Hash     *string         `json:"hash"`
	Payload  json.RawMessage `json:"payload"`
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
		scPath := filepath.Join(base, e.Name(), "manifest.json")
		raw, err := os.ReadFile(scPath)
		if err != nil {
			continue // no sidecar for this inode (blob-only dir)
		}
		res.Sidecars++
		// A sidecar freshly (re)written server-side can read back torn/partial on
		// the client mount (JuiceFS eventual consistency). Retry the read a few
		// times before giving up — a clean copy almost always lands within ms.
		var sc ManifestSidecar
		parsed := false
		for try := 0; try < 4; try++ {
			if json.Unmarshal(raw, &sc) == nil && sc.Inode != 0 {
				parsed = true
				break
			}
			time.Sleep(150 * time.Millisecond)
			if r, e := os.ReadFile(scPath); e == nil {
				raw = r
			}
		}
		if !parsed {
			res.Errs++
			continue
		}
		if err := store.PutSource(sc.Inode, sc.SourceHash); err != nil {
			res.Errs++
			continue
		}
		// Repopulate /metadata?kind=tech (D1 consume + AI size-guard fallback).
		if sc.Tech != nil && len(sc.Tech.Payload) > 0 {
			_ = store.PutMetadata(sc.Inode, "tech", sc.Tech.Producer, sc.Tech.Version, sc.Tech.Hash, sc.Tech.Payload)
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
	// Carry the tech metadata payload so the reconcile can repopulate /metadata.
	if tm, err := store.Metadata(inode, "tech"); err == nil && tm != nil {
		sc.Tech = &TechSidecar{Producer: tm.Producer, Version: tm.Version, Hash: tm.Hash, Payload: tm.Payload}
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
