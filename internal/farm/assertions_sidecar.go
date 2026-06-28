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
)

// AssertionSidecarSchema is the versioned shape tag (bumped only on a breaking
// change). Mirrors assertions-sidecar.schema.json `schema` const.
const AssertionSidecarSchema = "loupe.assertions/1"

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

// AssertionSidecarPath returns the sidecar path for a media file: the media path
// with `.loupe.json` appended (same directory, name = basename + `.loupe.json`).
func AssertionSidecarPath(mediaPath string) string {
	return mediaPath + ".loupe.json"
}
