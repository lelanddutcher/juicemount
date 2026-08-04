package farm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
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
		inode, perr := strconv.ParseUint(e.Name(), 10, 64)
		if perr != nil {
			continue // not an inode-named dir
		}
		r := reconcileOneSidecarInto(store, mount, inode)
		res.Sidecars += r.Sidecars
		res.Assets += r.Assets
		res.Rows += r.Rows
		res.Errs += r.Errs
	}
	return res, nil
}

// ReconcileOneSidecar lazily ingests a SINGLE inode's manifest sidecar if one
// exists on the volume — the on-demand half of the JM-15 reconcile. The
// /derivatives handler calls this when an inode MISSES the local store, so that
// navigating to a farm-derived asset surfaces it on the fly, without a full sweep
// or an app restart (the auto-reconcile bridge). found=true means a sidecar was
// present and ingested; an absent sidecar is (false, nil) — not an error.
// Idempotent (upserts), safe to call under the WAL the running app reads from.
func ReconcileOneSidecar(store *derivatives.Store, mount string, inode uint64) (found bool, err error) {
	r := reconcileOneSidecarInto(store, mount, inode)
	if r.Sidecars == 0 {
		return false, nil // no sidecar on the volume for this inode
	}
	if r.Assets == 0 && r.Errs > 0 {
		return false, fmt.Errorf("reconcile inode %d: sidecar present but ingest failed", inode)
	}
	return true, nil
}

// blobMediaTypes pins the Content-Type served for each blob kind. It is the
// SINGLE authority: a media type must never come from a file on the volume,
// because /blob sets its response Content-Type from the row and the derivative
// tree is writable by any consumer with mount access.
var blobMediaTypes = map[string]string{
	"ai":          "application/json",
	"proxy":       "video/mp4",
	"audio_proxy": "audio/mp4",
	"thumbnail":   "image/jpeg",
	"filmstrip":   "image/jpeg",
	"waveform":    "application/json",
}

// sanitizeSidecarRow re-derives every trust-bearing field of a manifest row read
// off the volume, and reports whether the row may be ingested at all.
//
// SECURITY (2026-08-04 review, CRITICAL). reconcileOneSidecarInto used to hand
// `sc.Derivatives` straight to PutDeriv with NO validation of kind, producer,
// media_type or blob_rel_path — and the derivatives table has no CHECK
// constraints either. Since manifest.json lives on a volume any consumer can
// write, that was a complete bypass of every guard on POST /derivatives/register:
// write a manifest declaring media_type "text/html" plus an HTML blob, touch
// /derivatives?inode=N to trigger the on-miss reconcile, and /blob then served
// attacker HTML with that Content-Type from the control-plane origin.
//
// The file may carry DATA (which kinds exist, their hashes and sizes). It may
// not carry POLICY (what a kind is called, what type it is served as, where its
// bytes live).
// sanitizeSidecarRow constrains one row read out of a manifest.json.
//
// There is deliberately NO "trusted" mode. manifest.json lives on the volume,
// and the volume is writable by every client — including the consumer that
// contribute-back exists to serve. WHICH BINARY READS THE FILE CONFERS NO TRUST
// ON IT: `jmfarm reconcile` running on the server reads exactly the same
// attacker-reachable bytes the Mac app does. Trust here would have been a lie,
// so the row is always treated as untrusted input and the unforgeable
// Provenance stamp (set by the caller, never by the file) carries the security.
//
// RULE: a file on the volume supplies DATA (which kinds exist, hashes, sizes),
// never POLICY (what a kind is called, how it is served, where its bytes live).
func sanitizeSidecarRow(row derivatives.DerivRow) (derivatives.DerivRow, bool) {
	// Producer is enum-checked but NOT trusted: it is echoed back out of
	// /metadata, so free-form text here would be attacker-chosen output, while a
	// forged "linux-farm" must not confer farm authority. Authority now comes
	// from Provenance (see store.go), so the string may stay as written —
	// which keeps disaster recovery honest when a GENUINE farm row is rebuilt
	// from its sidecar after a DB loss.
	if row.Producer != "linux-farm" && row.Producer != "on-device" && row.Producer != "macos-node" {
		return row, false
	}

	// "failed" is legitimate state the farm writes (farm.go:139/:159) — it tells
	// readers the artifact will never appear, so dropping it would make a known
	// failure look merely ungenerated.
	if row.Status != "ready" && row.Status != "failed" {
		return row, false
	}

	mt, isBlobKind := blobMediaTypes[row.Kind]
	if !isBlobKind {
		// Non-blob kinds (tech/embedding/transcript/ocr/faces) are /metadata
		// rows: they serve no bytes, so they carry no Content-Type risk — but
		// they must not smuggle a blob path either.
		if !nonBlobKinds[row.Kind] {
			return row, false // unknown kind entirely
		}
		row.BlobRelPath, row.MediaType = nil, nil
		return row, true
	}

	// The media type is OURS, always — /blob pins its response Content-Type from
	// this field, so a sidecar-supplied one is a sidecar-supplied Content-Type
	// served from the control-plane origin.
	row.MediaType = &mt

	// The blob path must be the reserved flat filename for the kind: never a
	// path, never another kind's artifact, never an arbitrary name. A `failed`
	// row legitimately has no blob at all.
	if row.BlobRelPath != nil {
		want, ok := reservedBlobName(row.Kind)
		if !ok {
			return row, false
		}
		if *row.BlobRelPath != want {
			// `ai` has two accepted spellings across the logger/loupe cutover.
			if !(row.Kind == "ai" && *row.BlobRelPath == derivatives.AIBlobNameLegacy) {
				return row, false
			}
		}
	}

	// Freshness is the read-gate every consumer stat-verifies against, so a
	// forged (size,mtime) pair pins a stale or wrong derivative as "fresh"
	// forever. Drop the pair rather than believe it: the row is then re-verified
	// against the live source, which self-heals. Costs a genuine DR-rebuilt row
	// one re-verification and nothing else.
	row.SourceSize, row.SourceMtime = nil, nil

	// Geometry is free-form in the file, and readers divide by it: cols==0 is a
	// divide-by-zero in any `i % cols`, and absurd counts over-allocate in a
	// scrubber. The schema's minimums are enforced nowhere in Go. VALIDATE
	// rather than drop — a filmstrip row without geometry is unusable, so
	// dropping it would break the farm's own DR for that kind.
	if row.Filmstrip != nil {
		g := row.Filmstrip
		if g.Cols < 1 || g.Rows < 1 || g.FrameCount < 1 || g.CellW < 1 || g.CellH < 1 ||
			g.IntervalMS < 0 || g.DurationMS < 0 ||
			g.Cols > maxStripDim || g.Rows > maxStripDim ||
			g.CellW > maxStripCell || g.CellH > maxStripCell ||
			g.FrameCount > g.Cols*g.Rows {
			return row, false
		}
	}
	return row, true
}

// Bounds for sidecar-declared filmstrip geometry. Generous enough that no real
// strip the farm produces trips them, tight enough that a reader allocating
// cols*rows cells cannot be walked into an absurd allocation.
const (
	maxStripDim  = 4096
	maxStripCell = 8192
)

// nonBlobKinds are manifest kinds that carry no bytes — they are fetched via
// /metadata, never /blob, so they need no media type or reserved filename.
var nonBlobKinds = map[string]bool{
	"tech": true, "embedding": true, "transcript": true, "ocr": true, "faces": true,
}

// reservedBlobName is the on-disk filename for a blob kind (spec/WRITE_PLACEMENT
// §2). ai is special: it has two accepted spellings across the logger/loupe
// cutover, so the caller compares against whichever is present.
func reservedBlobName(kind string) (string, bool) {
	switch kind {
	case "proxy":
		return "proxy.mp4", true
	case "audio_proxy":
		return "audio_proxy.mp4", true
	case "thumbnail":
		return "poster.jpg", true
	case "filmstrip":
		return "strip.jpg", true
	case "waveform":
		return "waveform.json", true
	case "ai":
		return derivatives.AIBlobName, true
	}
	return "", false
}

// reconcileOneSidecarInto reads + ingests <mount>/.juicemount/derivatives/<inode>/
// manifest.json into the store, returning per-call counts. The shared core of
// both the full walk (ReconcileSidecars) and the on-miss path (ReconcileOneSidecar).
func reconcileOneSidecarInto(store *derivatives.Store, mount string, inode uint64) ReconcileResult {
	var res ReconcileResult
	scPath := filepath.Join(DerivBlobDir(mount, inode), "manifest.json")
	raw, err := os.ReadFile(scPath)
	if err != nil {
		return res // no sidecar for this inode (blob-only dir / absent)
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
		return res
	}
	// CONFUSED-DEPUTY GUARD (2026-08-04). The DIRECTORY is the authority for
	// which asset we are reconciling — we found this file at
	// derivatives/<inode>/manifest.json. The body's `inode` is untrusted input
	// and must never redirect the write: a manifest sitting in dir 123 that
	// declares "inode": 456 would otherwise rewrite 456's rows, from a file any
	// client with mount write access can author. That access is not
	// hypothetical — it is the premise of Tier 1 contribute-back, and this path
	// runs on any /derivatives or /blob miss.
	//
	// A mismatch means the directory was copied/moved or the file was tampered
	// with; both make the row set wrong, so refuse rather than guess which half
	// to believe.
	if sc.Inode != inode {
		jmlog.Warn("sidecar reconcile: manifest inode disagrees with its directory — refusing",
			"dir_inode", inode, "manifest_inode", sc.Inode, "path", scPath)
		res.Errs++
		return res
	}
	if err := store.PutSource(sc.Inode, sc.SourceHash); err != nil {
		res.Errs++
		return res
	}
	// Repopulate /metadata?kind=tech (D1 consume + AI size-guard fallback).
	if sc.Tech != nil && len(sc.Tech.Payload) > 0 {
		_ = store.PutMetadata(sc.Inode, "tech", sc.Tech.Producer, sc.Tech.Version, sc.Tech.Hash, sc.Tech.Payload)
	}
	for _, row := range sc.Derivatives {
		// A manifest on the volume supplies DATA, never POLICY — re-derive the
		// trust-bearing fields and drop anything that does not fit the contract.
		clean, ok := sanitizeSidecarRow(row)
		// The stamp is applied HERE, by the ingesting code path — it is not read
		// from the file and cannot be set by anything on the volume. This is what
		// makes the precedence guard in cbridge.go unforgeable.
		clean.Provenance = derivatives.ProvenanceSidecar
		if !ok {
			jmlog.Warn("sidecar reconcile: dropping row that violates the blob contract",
				"inode", sc.Inode, "kind", row.Kind, "status", row.Status,
				"blob_rel_path", derefStr(row.BlobRelPath))
			res.Errs++
			continue
		}
		if err := store.PutDeriv(sc.Inode, clean); err != nil {
			res.Errs++
			return res
		}
		res.Rows++
	}
	res.Assets++
	return res
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
	return atomicWriteFile(path, b, 0o644)
}

// derefStr is a nil-safe *string for log lines.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
