package farm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	// Enumerated through the anchored walk. Every READ below was already
	// anchored, so a redirected listing could not smuggle content in — but it
	// could still send the sweep off to enumerate somebody else's directory and
	// do a pile of pointless work, and it was the last unanchored access left
	// under the derivative tree.
	baseDir, err := derivatives.OpenDirUnder(mount, filepath.Join(".juicemount", "derivatives"))
	if err != nil {
		return res, err
	}
	entries, err := baseDir.ReadDir(-1)
	baseDir.Close()
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

	// KIND-SCOPED FIELDS ARE CLEARED OFF-KIND (round 7).
	//
	// `extra`-backed fields are PERSISTED for any kind but only re-HYDRATED for
	// the kind they belong to (store.decodeExtra), so a row carrying, say,
	// blob_size on a thumbnail can never round-trip: the reconcile's
	// unchanged-row skip compares a stored row that lost the field against a
	// file row that still has it, decides "changed" every single time, and
	// re-publishes on every sweep with a fresh updated_at — the exact permanent
	// changes-feed churn that skip exists to prevent. Clearing them here is also
	// just the sanitizer's own rule applied properly: the file supplies DATA for
	// the kind it declares, and geometry on a waveform is not data, it is noise.
	if row.Kind != "filmstrip" {
		row.Filmstrip = nil
	}
	if row.Kind != "proxy" && row.Kind != "audio_proxy" {
		row.Codec, row.CodecString, row.BlobSize, row.BitrateBPS = nil, nil, nil, nil
	}
	// Pixel dimensions are meaningful only where decodeExtra re-hydrates them.
	// Persisting a field for a kind that never reads it back is precisely the
	// never-round-trips bug that made the reconcile re-publish a row forever.
	if row.Kind != "thumbnail" && row.Kind != "proxy" && row.Kind != "audio_proxy" && row.Kind != "filmstrip" {
		row.Width, row.Height = nil, nil
	}
	// Bounds: these come off a consumer-writable file and a reader may allocate
	// or divide by them.
	if row.Width != nil && (*row.Width < 1 || *row.Width > 65535) {
		row.Width = nil
	}
	if row.Height != nil && (*row.Height < 1 || *row.Height > 65535) {
		row.Height = nil
	}
	if row.BitrateBPS != nil && (*row.BitrateBPS < 1 || *row.BitrateBPS > int64(10)<<30) {
		row.BitrateBPS = nil
	}

	// FIELDS THAT WERE PASSING THROUGH UNTOUCHED (2026-08-04 round 3).
	//
	// Policy, because the first cut of this got it wrong by rejecting whole rows:
	// a field that is DANGEROUS is neutralised, a field that is merely
	// INFORMATIONAL is clamped or nulled — dropping the row would discard a real
	// artifact over a cosmetic value, which is its own kind of data loss.
	//
	// `hash` is the dangerous one: it is never re-verified against the blob's
	// actual bytes on any serve path, and the consumer's documented freshness
	// check is `hash == source_hash` — both supplied by this same file. Nulling a
	// malformed hash removes the forgery while keeping the artifact usable; the
	// shape is the provider's real output format (farm/hash.go emits %016x).
	if row.Hash != nil && !isHashHex(*row.Hash) {
		row.Hash = nil
	}
	// updated_at is NOT informational — it is the cursor of
	// GET /derivatives/changes?since= and of the farm's published
	// derivatives-changes.json. A forged MaxInt64 pins every consumer's cursor
	// past the end of time, so every genuine derivative from then on is
	// invisible to them, permanently, from one small JSON file with no blob
	// bytes at all. This is the field the "clamp the informational" sweep
	// missed. PutDeriv already treats 0 as "stamp now", so 0 is safe to pass.
	if row.UpdatedAt < 0 || row.UpdatedAt > nowUnix()+updatedAtSkewSlack {
		// Clamp to NOW, not to SourceMtime.
		//
		// Falling back to SourceMtime looked better because it is stable across
		// sweeps — but stable is exactly the problem: a farm host whose clock is
		// ahead by more than the slack got its rows stamped with the asset's OLD
		// mtime, landing BEFORE every consumer's cursor. Those derivatives were
		// then permanently invisible on the changes feed and could never
		// self-heal. One fast clock was enough; no attacker required.
		//
		// `now` is always after a consumer's cursor, so the row is seen. The
		// churn this was originally dodging is not solved by picking a different
		// timestamp — it is solved where it belongs, by not REWRITING a row whose
		// content has not changed (rowContentEqual, at the ingest loop). Choosing
		// a timestamp to avoid churn was treating the symptom.
		row.UpdatedAt = nowUnix()
	}
	if row.Version < 1 {
		row.Version = 1
	} else if row.Version > maxSidecarVersion {
		row.Version = maxSidecarVersion
	}
	if row.Model != nil && len(*row.Model) > maxModelLen {
		row.Model = nil
	}
	if row.Dim != nil && (*row.Dim < 1 || *row.Dim > 1<<16) {
		row.Dim = nil
	}
	if row.CodecString != nil && len(*row.CodecString) > maxCodecStringLen {
		row.CodecString = nil
	}
	if row.BlobSize != nil && (*row.BlobSize < 0 || *row.BlobSize > 1<<42) {
		row.BlobSize = nil
	}
	// Codec is the exception that DOES reject: absent codec is defined to mean
	// h264, so nulling an unrecognised value would silently relabel an
	// undecodable proxy as the guaranteed-decodable floor — the round-1 MEDIUM,
	// reintroduced through this path. There is no safe default, so refuse.
	if row.Codec != nil && !knownCodecs[*row.Codec] {
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

	// FRESHNESS IS PASSED THROUGH, NOT DROPPED.
	//
	// An earlier version nulled these on the theory that a forged pair pins a
	// stale derivative "fresh" forever, and that dropping them would self-heal
	// via re-verification. **That was wrong and it disabled a data-integrity
	// gate.** Nothing on the Mac ever re-stamps a reconciled row (the only
	// writers are the server-side farm and the register route), and reconcile
	// runs only when the inode is not already known — so the nil was permanent,
	// and bridge/thumbs.go treats a nil vouch as "unvouched, therefore serve".
	// The result was the C5 stale-derivative gate disabled for every farm-derived
	// asset: a recycled or overwritten inode served the OLD poster or proxy
	// indefinitely. That is precisely the bug C5 exists to prevent.
	//
	// Pass-through is still the right call, but NOT because it is free: it trades
	// one failure mode for a lesser one, and the earlier comment here claiming it
	// was "no worse against a forger" was wrong in the DENY direction.
	//
	//   nulled      → the row reads as unvouched, and unvouched means SERVE. A
	//                 wrong or stale derivative is handed out forever, silently.
	//   passed thru → a forged size/mtime makes a GENUINE derivative look stale,
	//                 and rejectStaleDeriv does not merely 404 — it evicts the
	//                 cached blob. One attacker-written JSON file, zero blob
	//                 bytes, denies a real thumbnail and forces the full-source
	//                 read the thumbnail plane exists to avoid.
	//
	// Serving wrong bytes silently is worse than denying right ones loudly, so
	// pass-through wins — but the deny lever is real and should be closed by
	// authenticating the sidecar, not by re-nulling the vouch.
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

// rowContentEqual compares everything that MEANS something to a reader, and
// deliberately ignores UpdatedAt (it is the thing being decided) and Provenance
// (set by us, identical for any two sidecar-sourced rows).
func rowContentEqual(a, b derivatives.DerivRow) bool {
	eqs := func(x, y *string) bool {
		if x == nil || y == nil {
			return x == y
		}
		return *x == *y
	}
	eqi := func(x, y *int64) bool {
		if x == nil || y == nil {
			return x == y
		}
		return *x == *y
	}
	eqn := func(x, y *int) bool {
		if x == nil || y == nil {
			return x == y
		}
		return *x == *y
	}
	if a.Kind != b.Kind || a.Status != b.Status || a.Producer != b.Producer || a.Version != b.Version {
		return false
	}
	if !eqs(a.Hash, b.Hash) || !eqs(a.BlobRelPath, b.BlobRelPath) || !eqs(a.MediaType, b.MediaType) ||
		!eqs(a.Model, b.Model) || !eqs(a.Codec, b.Codec) || !eqs(a.CodecString, b.CodecString) {
		return false
	}
	if !eqi(a.SourceSize, b.SourceSize) || !eqi(a.SourceMtime, b.SourceMtime) || !eqi(a.BlobSize, b.BlobSize) {
		return false
	}
	if !eqi(a.BitrateBPS, b.BitrateBPS) {
		return false
	}
	if !eqn(a.Dim, b.Dim) || !eqn(a.Width, b.Width) || !eqn(a.Height, b.Height) {
		return false
	}
	if (a.Filmstrip == nil) != (b.Filmstrip == nil) {
		return false
	}
	if a.Filmstrip != nil && *a.Filmstrip != *b.Filmstrip {
		return false
	}
	return true
}

// isHashHex reports whether s looks like a provider-emitted xxh3-64 digest.
func isHashHex(s string) bool {
	if len(s) != 16 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// sanitizeTechSidecar applies to the `tech` block the same rule the rows get:
// the file supplies DATA, never POLICY or identity.
func sanitizeTechSidecar(t *TechSidecar) (*TechSidecar, bool) {
	if t.Producer != "linux-farm" && t.Producer != "on-device" && t.Producer != "macos-node" {
		return nil, false
	}
	// Clamped, NOT rejected — matching DerivRow's policy. These were opposite
	// for no stated reason, and rejecting here silently discards the whole tech
	// block (and with it /metadata?kind=tech) over a cosmetic integer.
	if t.Version < 1 {
		t.Version = 1
	} else if t.Version > maxSidecarVersion {
		t.Version = maxSidecarVersion
	}
	if t.Hash != nil && !isHashHex(*t.Hash) {
		return nil, false
	}
	if len(t.Payload) > maxTechPayloadBytes {
		return nil, false
	}
	// The payload is stored and handed back out of /metadata, so it must at
	// least be a well-formed JSON OBJECT — not a bare string, not a scalar, and
	// not the truncated fragment an interrupted writer leaves behind.
	var probe map[string]any
	if err := json.Unmarshal(t.Payload, &probe); err != nil {
		return nil, false
	}
	// VALIDATE THE VALUE THIS BLOCK IS CITED FOR, not just its envelope.
	// The reason the tech block matters is that readers fall back to
	// tech.size_bytes for freshness when a row carries no source_size — so
	// checking producer/version/hash/shape and then storing the payload verbatim
	// leaves precisely the field that does the damage unchecked. A negative or
	// absurd size makes a stale derivative compare "fresh" against any source.
	if v, present := probe["size_bytes"]; present {
		n, isNum := v.(float64)
		if !isNum || n < 0 || n > maxPlausibleSourceBytes {
			delete(probe, "size_bytes")
			cleaned, err := json.Marshal(probe)
			if err != nil {
				return nil, false
			}
			t.Payload = cleaned
		}
	}
	if v, present := probe["duration_ms"]; present {
		// BOTH bounds. The first cut checked only n < 0 while size_bytes got a
		// range — so 1e300 sailed through the field readers use for scrubber
		// geometry. maxPlausibleDurationMS is ~1000 years.
		if n, isNum := v.(float64); !isNum || n < 0 || n > maxPlausibleDurationMS {
			delete(probe, "duration_ms")
			cleaned, err := json.Marshal(probe)
			if err != nil {
				return nil, false
			}
			t.Payload = cleaned
		}
	}
	return t, true
}

// knownCodecs mirrors derivatives.schema.json's codec enum. It is asserted
// against the vendored contract by TestSidecarCodecsMatchContract — the first
// cut of this list omitted "opus" and would have DROPPED a legitimate farm row,
// which is the third time a hand-copied vocabulary here has been narrower than
// what the code on the other side actually emits.
var knownCodecs = map[string]bool{
	"h264": true, "hevc": true, "av1": true, "aac": true, "opus": true,
}

// readSidecarBounded reads one manifest.json through the anchored, symlink-
// checking open and refuses anything larger than a manifest could plausibly be.
// The cap is enforced while READING, not after, so a file that lies about its
// size (or reports none) still cannot outrun it.
func readSidecarBounded(mount, rel string) ([]byte, error) {
	f, err := derivatives.OpenRegularUnder(mount, rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxSidecarBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxSidecarBytes {
		return nil, fmt.Errorf("sidecar %q exceeds %d bytes", rel, maxSidecarBytes)
	}
	return raw, nil
}

// maxSidecarBytes: a manifest lists a handful of rows plus one ffprobe payload.
// 8 MiB is orders of magnitude more than any real one and still bounded.
const maxSidecarBytes = int64(8) << 20

// nowUnix is a var so the clamp is testable without waiting on the wall clock.
var nowUnix = func() int64 { return time.Now().Unix() }

// updatedAtSkewSlack tolerates ordinary clock skew between the farm host and
// this Mac without letting a forged far-future stamp through.
// 5 minutes. 24 h was chosen for "ordinary clock skew" and left a rolling,
// indefinitely renewable blind spot: now+86400 passed verbatim, so one JSON file
// with no blob bytes still hid a consumer's changes feed for a full day, and
// every newly-created inode gets a fresh reconcile to renew it. NTP-synced hosts
// are within seconds; five minutes is generous for the real condition and
// useless as an attack window.
const updatedAtSkewSlack = int64(5 * 60)

// maxPlausibleSourceBytes: 1 PiB. Larger is not a media file, it is a forgery.
const maxPlausibleSourceBytes = float64(int64(1) << 50)

// maxPlausibleDurationMS: ~1000 years. Longer is not a clip.
const maxPlausibleDurationMS = float64(1000 * 365 * 24 * 60 * 60 * 1000)

const (
	maxSidecarVersion   = 1 << 16
	maxTechPayloadBytes = 4 << 20
	maxModelLen         = 128
	maxCodecStringLen   = 256
)

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
	// THE FILE EVERY SANITIZER BELOW RUNS ON, read with the same guard as any
	// other derivative — it was the one thing still opened by bare name.
	// os.ReadFile followed both the final component and every ancestor, and had
	// no size cap at all: a symlink to a huge file (or to /dev/zero, which has
	// no size to check) allocated until the process died, amplified 5x by the
	// retry loop below. The attacker picked the number.
	scRel := derivatives.DerivBlobRel(inode, "manifest.json")
	raw, err := readSidecarBounded(mount, scRel)
	if err != nil {
		// ABSENT is normal and silent — most inode dirs are blob-only, and the
		// full walk hits this constantly. REFUSED is not: a manifest rejected for
		// being a symlink, a non-regular file, or over the size cap means an
		// asset's derivatives are invisible, and reporting that identically to
		// "there is no sidecar here" is exactly the silent truncation that makes
		// a cap read as "covered everything".
		if !errors.Is(err, os.ErrNotExist) {
			jmlog.Warn("sidecar reconcile: manifest refused — this asset's derivatives are NOT indexed",
				"inode", inode, "rel", scRel, "error", err)
			res.Errs++
		}
		return res
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
		if r, e := readSidecarBounded(mount, scRel); e == nil {
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
			"dir_inode", inode, "manifest_inode", sc.Inode, "rel", scRel)
		res.Errs++
		return res
	}
	// source_hash is the value a consumer's documented `hash == source_hash`
	// freshness check compares against, and it is never re-derived from bytes on
	// any serve path — so an arbitrary string here is a freshness forgery. Pin
	// it to the shape the provider actually emits (xxh3-64 hex) or drop it.
	if sc.SourceHash != nil && !isHashHex(*sc.SourceHash) {
		sc.SourceHash = nil
	}
	if err := store.PutSource(sc.Inode, sc.SourceHash); err != nil {
		res.Errs++
		return res
	}
	// Repopulate /metadata?kind=tech (D1 consume + AI size-guard fallback).
	if sc.Tech != nil && len(sc.Tech.Payload) > 0 {
		// The `tech` block comes out of the SAME attacker-writable file as the
		// rows above and was going straight to PutMetadata with no checks at all
		// — while sanitizeSidecarRow's own rationale says producer must be
		// enum-checked because it is echoed back out of /metadata. That check
		// existed only for DerivRow. It matters more here, not less: thumbs.go
		// documents consumers falling back to tech.size_bytes for freshness when
		// a row's source_size is absent, so an unchecked payload can become the
		// freshness signal itself.
		if tech, ok := sanitizeTechSidecar(sc.Tech); ok {
			_ = store.PutMetadata(sc.Inode, "tech", tech.Producer, tech.Version, tech.Hash, tech.Payload)
		} else {
			jmlog.Warn("sidecar reconcile: dropping tech block that violates the contract",
				"inode", sc.Inode, "producer", sc.Tech.Producer)
			res.Errs++
		}
	}
	// One read of what is already indexed, so an unchanged manifest re-ingests
	// as a no-op instead of re-emitting every row on the changes feed.
	existing := map[string]derivatives.DerivRow{}
	if prior, perr := store.Manifest(sc.Inode); perr == nil {
		for _, pr := range prior {
			existing[pr.Kind] = pr
		}
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
		// Skip a row that is materially identical to the one already indexed.
		// Every upsert bumps updated_at, and updated_at is the changes-feed
		// cursor — so re-ingesting an UNCHANGED manifest on every sweep made the
		// same derivatives re-emit forever. This is the honest place to fix that:
		// nothing changed, so nothing should be published as changed.
		if prior, ok := existing[clean.Kind]; ok && rowContentEqual(prior, clean) {
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
	// Written THROUGH the directory descriptor. This runs from all three farm
	// entry points, and os.MkdirAll + a path-addressed atomic write followed a
	// symlinked <inode> component straight out of the volume — clobbering
	// whatever sat at the target, as root.
	dir, err := derivDirFor(mount, inode)
	if err != nil {
		return err
	}
	defer dir.Close()
	return derivatives.WriteFileAt(dir, "manifest.json", b, 0o644)
}

// derefStr is a nil-safe *string for log lines.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
