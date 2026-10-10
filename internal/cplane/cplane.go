// Package cplane holds the parts of the JuiceMount↔OpenLoupe control-plane
// contract that are shared between the two binaries that serve /whoami: the
// GUI core (bridge/cbridge.go, cgo) and the jm5 CLI (cmd/jm5). It is the
// Go-side mirror of the language-neutral contract vendored under contract/.
//
// Only the cross-binary pieces live here (the WhoAmI shape, the capability
// vocabulary + derivation, instance-id persistence). Endpoint handlers that
// touch the GUI's globals (residency, lookup, cache-status) stay in cbridge.
package cplane

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

// ContractVersion is this build's wire contract_version. It MUST match the
// integer in contract/VERSION (the vendored contract). Bumped when the
// provider and consumer ratify a new versioned wire contract.
//
// 1 -> 2 (2026-08-03): the loupe->logger wire-term cutover. Every SCHEMA change
// in that step was a widener, so it was tempting to leave this at 1 — but the
// same step changed the TRAFFIC: /whoami now carries `wire_terms`, and the v1
// whoami schema is additionalProperties:false, so a consumer still pinned to the
// previously-vendored v1 rejects every live /whoami with "Additional properties
// are not allowed ('wire_terms' was unexpected)". Leaving the integer at 1 would
// have handed them a hard failure with no signal to re-vendor, which is exactly
// what this field exists to prevent.
// 2 -> 3 (2026-08-05): discriminated 409 responses for derivative registration.
const ContractVersion = 3

// WireTerms advertises that this build completed the coordinated `loupe` ->
// `logger` wire-term cutover proposed in CONSUMER_STATUS 2026-07-18 §2 and
// acked by the founder on 2026-08-03.
//
// What it promises to a consumer that sees it:
//   - we WRITE the new names — `ai.logger.json`, `logger_version`,
//     `<media>.logger.json`;
//   - we READ BOTH old and new, indefinitely. The legacy names are not
//     scheduled for removal without a further coordinated step.
//
// Absence means pre-cutover (legacy names written). Consumers feature-detect on
// this field only — never on `version` or `contract_version`.
//
// This is deliberately NOT a capability token: `capabilities` is derived from
// the routes a binary actually serves and validated against a closed vocabulary
// (see DeriveCapabilities), and a wire-term dialect is not a route.
const WireTerms = "logger/1"

// WhoAmI is the GET /whoami response. Schema: contract/spec/schema/whoami.schema.json.
// Field order/tags match the golden fixtures contract/fixtures/whoami/*.json.
type WhoAmI struct {
	App             string `json:"app"`              // always "JuiceMount"
	Version         string `json:"version"`          // public release string (internal/version.Version)
	ContractVersion int    `json:"contract_version"` // ContractVersion
	InstanceID      string `json:"instance_id"`      // stable per-install UUID
	// DeviceDigest is the internal/deviceid digest of the Mac serving this
	// instance (never the hardware UUID). Optional: absent when unavailable.
	// It lets the Manager flag one principal (instance_id) arriving from two
	// Macs, the signature of a copied install.
	DeviceDigest   string `json:"device_digest,omitempty"`
	VolumeName     string `json:"volume_name"`   // e.g. "zpool"
	MountPoint     string `json:"mount_point"`   // e.g. "/Volumes/zpool"
	NASRoot        string `json:"nas_root"`      // today == mount_point
	ControlPlane   string `json:"control_plane"` // e.g. "http://127.0.0.1:11050"
	MetadataDBPath string `json:"metadata_db_path,omitempty"`
	Deployment     string `json:"deployment"` // "gui" | "cli"
	// WireTerms is the loupe->logger cutover flag (see the WireTerms const).
	// omitempty is load-bearing: a build that leaves it unset must emit NO key —
	// the documented "absent == pre-cutover" form — rather than "", which the
	// schema's enum rejects and which would fail-closed for a strict consumer.
	WireTerms    string   `json:"wire_terms,omitempty"`
	Capabilities []string `json:"capabilities"` // DERIVED, never hardcoded
}

// capabilityVocab is the ONLY set of tokens that may appear in capabilities.
// Mirror of contract/spec/capabilities.md "Capability tokens (v1)".
var capabilityVocab = map[string]bool{
	"health": true, "whoami": true, "residency": true, "lookup": true,
	"cache-status": true, "offline": true, "spool": true, "activity": true,
	"pin": true, "unpin": true, "self-test": true, "verify-pins": true,
	"metrics": true, "derivatives": true, "metadata": true, "contribute": true,
	"changes": true,
	// PROXY-CODEC (#50) byte-range blob delivery (GET /blob?inode=N&kind=proxy)
	// and JM-ASSERT (#51) portable-human-metadata channel (POST/GET /assertions).
	// Both route paths == their tokens, so no routeCapAlias entry is needed.
	"blob": true, "assertions": true,
	// REGISTER-ROUTE (2026-08-04, founder-prioritized): `contribute` has meant
	// "AI-only contribute-back" since OL-1, and an OLD provider build advertises
	// that exact string — so it cannot tell a consumer whether non-AI kinds are
	// registrable. `contribute-derivatives` is the distinct token that says the
	// widened route is present. Feature-detect on THIS, never on `contribute`.
	"contribute-derivatives": true,
	// JM-22 (2026-08-18): POST /derivatives/batch answers "which of these N
	// inodes have ready derivatives" in one call. The consumer's reconcile pass
	// is thousands of inodes wide, and the single-inode route runs an on-the-fly
	// sidecar reconcile against the FILESYSTEM on a miss — so N single queries
	// drive N filesystem reconciles. Feature-detect on this token; without it
	// the consumer's only signal is a 404 from an older provider.
	"derivatives-batch": true,
}

// routeCapAlias maps a served route (no leading slash) to a capability token when
// the token differs from the route path. OL-1: the write route is
// "/derivatives/register" but the capability is `contribute` (a route whose
// trimmed path isn't itself a vocabulary token).
var routeCapAlias = map[string][]string{
	// One route may advertise MORE THAN ONE token. /derivatives/register emits
	// both the historical `contribute` (AI-only, kept so older consumers keep
	// working unchanged) and `contribute-derivatives` (this build accepts the
	// widened kind set). A consumer seeing only `contribute` is talking to a
	// pre-REGISTER-ROUTE provider and must not attempt a non-AI register.
	"derivatives/register": {"contribute", "contribute-derivatives"},
	"derivatives/changes":  {"changes"},
	"derivatives/batch":    {"derivatives-batch"},
}

// DeriveCapabilities computes the capability list as the intersection of the
// routes THIS binary actually serves with capabilityVocab — so it can never
// claim a route it doesn't serve. Operational/UI routes (reclaim, cache-clear,
// force-eject, stop, mount-now, spool-recover, debug/pprof, manager, migrator)
// are not in the vocabulary and are therefore excluded automatically.
//
// servedRoutes is the set of route paths this binary registers (leading slash
// optional), e.g. the keys of the metrics server's ExtraRoutes plus the
// built-ins "/health" and "/metrics". "whoami" is always included because a
// binary answering /whoami is by definition serving it.
func DeriveCapabilities(servedRoutes []string) []string {
	caps := map[string]bool{"whoami": true}
	for _, r := range servedRoutes {
		token := strings.TrimPrefix(strings.TrimSpace(r), "/")
		if capabilityVocab[token] {
			caps[token] = true
		} else if aliases, ok := routeCapAlias[token]; ok {
			for _, a := range aliases {
				caps[a] = true
			}
		}
	}
	out := make([]string, 0, len(caps))
	for c := range caps {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Files beside metadata.db that hold this install's identity.
const (
	instanceIDFile     = "instance-id"
	instanceDeviceFile = "instance-device"     // deviceid digest the ID is bound to
	instanceCloneFile  = "instance-clone.json" // present after a clone rotation, until enrollment clears it
)

// CloneMessage is what the app and the Manager show after a rotation.
const CloneMessage = "This Mac was migrated or cloned; it must be approved in the Manager as a new Mac."

// CloneNotice records a rotation caused by a device mismatch. It stays on
// disk until the new principal is approved (ClearCloneNotice), so the state
// survives restarts and is readable by the app (Preferences.swift) without a
// running service.
type CloneNotice struct {
	PreviousInstanceID string `json:"previous_instance_id"`
	InstanceID         string `json:"instance_id"`
	DetectedAt         string `json:"detected_at"` // RFC 3339 UTC
	Message            string `json:"message"`
}

// InstanceIdentity is the result of loading the per-install identity.
type InstanceIdentity struct {
	ID string
	// Rotated is true when this load found the stored ID bound to another
	// Mac and replaced it. The old principal's secret must not be used.
	Rotated bool
	// Adopted is true when a stored ID with no device binding (a build from
	// before the binding existed) was bound to this Mac without rotating.
	Adopted bool
}

// LoadOrMintInstanceID returns a stable per-install UUID, persisted as
// <dir(dbPath)>/instance-id (sibling of metadata.db / pin.db). See
// LoadInstanceIdentity for the device binding. On any I/O error it returns a
// fresh ephemeral UUID rather than failing — /whoami must always answer.
func LoadOrMintInstanceID(dbPath string) string {
	return LoadInstanceIdentity(dbPath).ID
}

// LoadInstanceIdentity loads the per-install ID and checks that it was
// created on THIS Mac.
//
// The instance ID names this Mac's Manager principal (mac-<12 hex>). It lives
// in a plain file that Migration Assistant, a Time Machine restore onto
// another Mac, or a cloned disk copy verbatim, which would give two Macs one
// principal and break exclusive project leases. So the ID is stored with the
// deviceid digest of the Mac that minted it:
//
//   - digest matches: keep the ID;
//   - no digest stored (upgrade from a build without the binding): bind the
//     current digest once and keep the ID, so existing Macs need not
//     re-enroll. A disk cloned BEFORE any binding build ran cannot be told
//     apart from an upgrade; that gap is inherent;
//   - digest differs (clone): mint a new ID, bind it, and write a
//     CloneNotice. The credential stores refuse the copied secret on their
//     own (they carry the same digest). Nothing else beside metadata.db is
//     touched here: the metadata cache stays. The spool binds itself the same
//     way (nfs/drainer_publish.go): rows copied from the other Mac are HELD
//     and never drain until the user chooses to upload or discard them, and
//     rows written on this Mac drain once its new principal is approved;
//   - no fingerprint available: keep the ID unverified (nothing to compare).
func LoadInstanceIdentity(dbPath string) InstanceIdentity {
	if dbPath == "" || dbPath == ":memory:" {
		return InstanceIdentity{ID: uuidV4()} // no stable home (tests / in-memory) — ephemeral
	}
	dir := filepath.Dir(dbPath)
	idPath := filepath.Join(dir, instanceIDFile)
	devPath := filepath.Join(dir, instanceDeviceFile)
	device, devErr := deviceid.Fingerprint()

	stored := readTrimmed(idPath)
	if stored == "" {
		id := uuidV4()
		// Best-effort persist; ignore write errors (ephemeral fallback).
		if writeAtomic(idPath, id) == nil && devErr == nil {
			_ = writeAtomic(devPath, device)
		}
		return InstanceIdentity{ID: id}
	}
	if devErr != nil {
		return InstanceIdentity{ID: stored}
	}
	bound := readTrimmed(devPath)
	switch {
	case bound == device:
		return InstanceIdentity{ID: stored}
	case bound == "":
		_ = writeAtomic(devPath, device)
		return InstanceIdentity{ID: stored, Adopted: true}
	}
	// A crash after a rotation wrote the notice and the new ID but before it
	// bound the digest: finish that rotation rather than starting another.
	if n, ok := ReadCloneNotice(dbPath); ok && n.InstanceID == stored {
		_ = writeAtomic(devPath, device)
		return InstanceIdentity{ID: stored, Rotated: true}
	}
	id := uuidV4()
	notice := CloneNotice{
		PreviousInstanceID: stored,
		InstanceID:         id,
		DetectedAt:         time.Now().UTC().Format(time.RFC3339),
		Message:            CloneMessage,
	}
	if b, err := json.Marshal(notice); err == nil {
		_ = writeAtomic(filepath.Join(dir, instanceCloneFile), string(b))
	}
	if err := writeAtomic(idPath, id); err != nil {
		// The copied ID cannot be replaced on disk; still never answer as
		// the other Mac's principal.
		return InstanceIdentity{ID: id, Rotated: true}
	}
	_ = writeAtomic(devPath, device)
	return InstanceIdentity{ID: id, Rotated: true}
}

// DeviceDigest is the /whoami device_digest for this Mac ("" when
// unavailable, which omits the field). Shared by the GUI core and jm5 so both
// servers report it identically.
func DeviceDigest() string {
	d, _ := deviceid.Fingerprint()
	return d
}

// ReadCloneNotice returns the pending clone notice beside dbPath, if any.
func ReadCloneNotice(dbPath string) (CloneNotice, bool) {
	var n CloneNotice
	if dbPath == "" || dbPath == ":memory:" {
		return n, false
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(dbPath), instanceCloneFile))
	if err != nil || json.Unmarshal(b, &n) != nil || n.InstanceID == "" {
		return CloneNotice{}, false
	}
	return n, true
}

// ClearCloneNotice removes the notice once the Manager has approved this
// Mac's new principal. A missing notice is not an error.
func ClearCloneNotice(dbPath string) error {
	if dbPath == "" || dbPath == ":memory:" {
		return nil
	}
	err := os.Remove(filepath.Join(filepath.Dir(dbPath), instanceCloneFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func readTrimmed(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// writeAtomic replaces path with value+"\n" through a synced temp file, so a
// crash leaves either the old or the new content, never a torn file.
func writeAtomic(path, value string) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.WriteString(value + "\n")
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// uuidV4 returns a random RFC-4122 v4 UUID string (uppercase, matching the
// fixture style). Uses crypto/rand; panics are impossible (rand.Read on
// crypto/rand never short-reads without error, and on error we fall back).
func uuidV4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Degenerate fallback — extremely unlikely. A non-unique id is far
		// better than a panic in a /whoami handler.
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
