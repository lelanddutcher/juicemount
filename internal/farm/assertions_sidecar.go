package farm

// JM-ASSERT (#51) — the portable `<media>.loupe.json` assertion sidecar
// (ASSERTIONS_SIDECAR.md). This file IS the source of truth for an asset's human
// metadata (ratings, person names, a deliberate log-profile pick): an ordinary
// JSON file next to the media (same dir, name = media basename + `.loupe.json`),
// so the truth travels with the bytes on any cross-volume / SMB / USB copy and is
// readable by a second client with no JuiceMount in the path. JuiceMount's Tier-B
// `assertions` table is a REBUILDABLE INDEX over these sidecars, never the silo.
//
// The writer here is LWW + merge-not-clobber: it reads the existing sidecar,
// applies one incoming triple under last-writer-wins per (namespace,key), leaves
// every sibling namespace untouched, and writes the result atomically (temp +
// fsync + rename) so a concurrent reader never sees a torn file. A retract
// (value:null) is recorded (the triple is kept with value null + the newer
// asserted_at) rather than deleting the line, so the un-assert is itself durable.
// An asset whose only assertion is retracted still has a sidecar (the contract's
// retract fixture); an asset with NO assertions ever has NO sidecar.

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
)

// The versioned shape tag inside every assertion sidecar. Mirrors the `schema`
// enum in assertions-sidecar.schema.json.
//
// WIRE-TERM CUTOVER, phase 2 (2026-08-03). The consumer initially advised
// AGAINST renaming this const (high blast radius, no user-visible value), then
// reversed once contract_version moved to 2: the v2 bump IS the migration
// window, and doing it in any other release would buy a second one. The schema
// const was therefore widened to an enum accepting both spellings, and both
// sides move in lockstep with the legacy tag readable indefinitely.
//
// WRITE RULE — deliberately conservative, and NOT simply "always stamp the new
// tag". An existing sidecar KEEPS whatever tag it already carries; only a
// freshly-created sidecar gets the new one. Flipping the tag inside a file that
// a shipped reader is already reading is exactly the failure we hit hours
// earlier with `logger_version` in a legacy-named blob: the file name stayed
// familiar, the contents stopped parsing, and the consumer's `try?` swallowed
// it. Renaming a file and rewriting a tag inside it are two different
// migrations; this one only ever applies to new files.
const (
	AssertionSidecarSchema       = "logger.assertions/1"
	AssertionSidecarSchemaLegacy = "loupe.assertions/1"
)

// SidecarAssertion is one triple in the sidecar — the verbatim JM-ASSERT wire
// shape (assertions-sidecar.schema.json#/$defs/assertion). Value is `any` so it
// round-trips string|number|bool|null (null == retract).
type SidecarAssertion struct {
	Namespace  string `json:"namespace"`
	Key        string `json:"key"`
	Value      any    `json:"value"`
	AssertedBy string `json:"asserted_by"`
	AssertedAt string `json:"asserted_at"`
}

// AssertionSidecar is the on-disk `<media>.loupe.json` document. additionalProps
// are allowed by the schema (forward-compat); we model the known fields and would
// preserve unknown ones via Extra if needed — for now the writer fully owns the
// file shape, so the round-trip is lossless for the modeled fields.
type AssertionSidecar struct {
	Schema        string             `json:"schema"`
	AssetKey      string             `json:"asset_key"`
	MediaFilename string             `json:"media_filename"`
	Assertions    []SidecarAssertion `json:"assertions"`
}

// ReadAssertionSidecar loads + parses a sidecar. (false, nil) when no sidecar
// exists (the normal "asset has no assertions" case); an error only on a present
// but unreadable/corrupt file.
func ReadAssertionSidecar(path string) (*AssertionSidecar, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var sc AssertionSidecar
	if err := json.Unmarshal(raw, &sc); err != nil {
		return nil, false, err
	}
	return &sc, true, nil
}

// ApplyAssertionResult reports what an ApplyAssertion did, so the index write and
// the HTTP response stay consistent with the sidecar.
type ApplyAssertionResult struct {
	Accepted          bool   // true when the incoming triple won LWW + the sidecar was (re)written
	WinningAssertedAt string // asserted_at of whoever now holds the (namespace,key) slot
}

// ApplyAssertion applies one incoming triple to the sidecar at sidecarPath under
// last-writer-wins (merge-not-clobber), writing the result atomically on accept.
// assetKey + mediaFilename seed a freshly-created sidecar. On a reject-stale
// (incoming asserted_at <= the stored winner for the same namespace,key) the file
// is left byte-unchanged and Accepted=false is returned with the stored winner's
// asserted_at.
func ApplyAssertion(sidecarPath, assetKey, mediaFilename string, incoming SidecarAssertion) (ApplyAssertionResult, error) {
	sc, exists, err := ReadAssertionSidecar(sidecarPath)
	if err != nil {
		return ApplyAssertionResult{}, err
	}
	if !exists {
		sc = &AssertionSidecar{
			Schema:        AssertionSidecarSchema,
			AssetKey:      assetKey,
			MediaFilename: mediaFilename,
			Assertions:    []SidecarAssertion{},
		}
	}
	// Fill ONLY when absent. This is load-bearing since the tag was unified
	// (2026-08-03): an existing sidecar carrying the legacy
	// "loupe.assertions/1" keeps it, because rewriting the tag inside a file a
	// shipped reader already parses is a silent break, not a migration. New
	// sidecars get the new tag from the literal above.
	if sc.Schema == "" {
		sc.Schema = AssertionSidecarSchema
	}
	// Seed identity fields if the existing sidecar lacked them (path-fallback → hash).
	if sc.AssetKey == "" {
		sc.AssetKey = assetKey
	}
	if sc.MediaFilename == "" {
		sc.MediaFilename = mediaFilename
	}

	// Find the existing triple for the same (namespace,key).
	idx := -1
	for i := range sc.Assertions {
		if sc.Assertions[i].Namespace == incoming.Namespace && sc.Assertions[i].Key == incoming.Key {
			idx = i
			break
		}
	}
	if idx >= 0 && incoming.AssertedAt <= sc.Assertions[idx].AssertedAt {
		// Reject-stale: leave the file untouched.
		return ApplyAssertionResult{Accepted: false, WinningAssertedAt: sc.Assertions[idx].AssertedAt}, nil
	}
	if idx >= 0 {
		sc.Assertions[idx] = incoming // LWW replace (retract = value:null kept)
	} else {
		sc.Assertions = append(sc.Assertions, incoming) // new (namespace,key) — merge, don't clobber
	}

	b, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return ApplyAssertionResult{}, err
	}
	b = append(b, '\n')
	if err := atomicWriteFile(sidecarPath, b, 0o644); err != nil {
		return ApplyAssertionResult{}, err
	}
	return ApplyAssertionResult{Accepted: true, WinningAssertedAt: incoming.AssertedAt}, nil
}

// Portable per-file assertion sidecar suffixes.
//
// WIRE-TERM CUTOVER (2026-08-03). The consumer renamed the on-share per-file
// sidecar to `<media>.logger.json` on 2026-07-14 (CONSUMER_STATUS 07-18 §1) and
// stopped reading `.loupe.json`; the shared spec was not updated at the time, so
// the farm kept writing the old name and the two sides silently diverged. The
// founder's 2026-08-03 ruling is to unify on `logger`: we now WRITE
// AssertionSidecarSuffix and READ BOTH.
const (
	AssertionSidecarSuffix       = ".logger.json"
	AssertionSidecarSuffixLegacy = ".loupe.json"
)

// AssertionSidecarPath returns the sidecar path to USE for a media file.
//
// Dual-read: if a legacy `<media>.loupe.json` exists and the post-cutover
// `<media>.logger.json` does not, the legacy path is returned so a read-merge-
// write updates the file that is actually there instead of silently starting an
// empty one beside it and stranding the user's existing ratings/names. Once the
// new name exists it always wins. A media file with no sidecar yet gets the new
// name.
func AssertionSidecarPath(mediaPath string) string {
	newPath := mediaPath + AssertionSidecarSuffix
	if _, err := os.Stat(newPath); err == nil {
		return newPath
	}
	legacy := mediaPath + AssertionSidecarSuffixLegacy
	if _, err := os.Stat(legacy); err == nil {
		return legacy
	}
	return newPath
}

// IsAssertionSidecarName reports whether name is a portable assertion sidecar,
// under either the post-cutover or the legacy suffix.
//
// FARM-1 (CONSUMER_STATUS 2026-08-03): sub-clip children are named
// `<parent>.v-<8hex>.logger.json` and MUST classify as sidecars. Suffix matching
// handles that natively — there is no assumption about how many `.` precede
// `logger`. A child is NEVER the parent's whole-clip sidecar: its fields
// describe a range, so callers pairing a sidecar to media must use
// AssertionSidecarMedia, which strips the `.v-<hex>` infix.
func IsAssertionSidecarName(name string) bool {
	return strings.HasSuffix(name, AssertionSidecarSuffix) ||
		strings.HasSuffix(name, AssertionSidecarSuffixLegacy)
}

// subClipInfix matches the sub-clip child marker `.v-<8hex>` immediately before
// the sidecar suffix.
var subClipInfix = regexp.MustCompile(`\.v-[0-9a-fA-F]{8}$`)

// AssertionSidecarMedia maps a sidecar filename back to the media filename it
// belongs to, and reports whether it is a sub-clip child.
//
// `C0012.MXF.logger.json`             -> ("C0012.MXF", false)
// `C0012.MXF.v-a1b2c3d4.logger.json`  -> ("C0012.MXF", true)
// `C0012.MXF.loupe.json`  (legacy)    -> ("C0012.MXF", false)
//
// This is the name-keyed join DB-4 asks for: a `.v-` child belongs to the same
// media as its parent's sidecar. ok=false when name is not a sidecar at all.
func AssertionSidecarMedia(name string) (media string, isSubClip, ok bool) {
	switch {
	case strings.HasSuffix(name, AssertionSidecarSuffix):
		media = strings.TrimSuffix(name, AssertionSidecarSuffix)
	case strings.HasSuffix(name, AssertionSidecarSuffixLegacy):
		media = strings.TrimSuffix(name, AssertionSidecarSuffixLegacy)
	default:
		return "", false, false
	}
	if trimmed := subClipInfix.ReplaceAllString(media, ""); trimmed != media {
		return trimmed, true, true
	}
	return media, false, true
}
