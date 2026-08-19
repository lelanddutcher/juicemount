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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ContractVersion is this build's wire contract_version. It MUST match the
// integer in contract/VERSION (the vendored contract). Bumped only on a
// breaking wire change.
//
// 1 -> 2 (2026-08-03): the loupe->logger wire-term cutover. Every SCHEMA change
// in that step was a widener, so it was tempting to leave this at 1 — but the
// same step changed the TRAFFIC: /whoami now carries `wire_terms`, and the v1
// whoami schema is additionalProperties:false, so a consumer still pinned to the
// previously-vendored v1 rejects every live /whoami with "Additional properties
// are not allowed ('wire_terms' was unexpected)". Leaving the integer at 1 would
// have handed them a hard failure with no signal to re-vendor, which is exactly
// what this field exists to prevent.
const ContractVersion = 2

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
	VolumeName      string `json:"volume_name"`      // e.g. "zpool"
	MountPoint      string `json:"mount_point"`      // e.g. "/Volumes/zpool"
	NASRoot         string `json:"nas_root"`         // today == mount_point
	ControlPlane    string `json:"control_plane"`    // e.g. "http://127.0.0.1:11050"
	MetadataDBPath  string `json:"metadata_db_path,omitempty"`
	Deployment      string `json:"deployment"` // "gui" | "cli"
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

// LoadOrMintInstanceID returns a stable per-install UUID, persisted as
// <dir(dbPath)>/instance-id (sibling of metadata.db / pin.db). It mints a v4
// UUID on first run and reuses it forever. On any I/O error it returns a fresh
// ephemeral UUID rather than failing — /whoami must always answer; a
// non-persisted id is a cosmetic degradation, not a contract violation.
func LoadOrMintInstanceID(dbPath string) string {
	if dbPath == "" || dbPath == ":memory:" {
		return uuidV4() // no stable home (tests / in-memory) — ephemeral
	}
	idPath := filepath.Join(filepath.Dir(dbPath), "instance-id")
	if b, err := os.ReadFile(idPath); err == nil {
		if id := strings.TrimSpace(string(b)); id != "" {
			return id
		}
	}
	id := uuidV4()
	// Best-effort persist; ignore write errors (ephemeral fallback).
	_ = os.WriteFile(idPath, []byte(id+"\n"), 0o644)
	return id
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
