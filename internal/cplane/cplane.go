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
const ContractVersion = 1

// WhoAmI is the GET /whoami response. Schema: contract/spec/schema/whoami.schema.json.
// Field order/tags match the golden fixtures contract/fixtures/whoami/*.json.
type WhoAmI struct {
	App             string   `json:"app"`              // always "JuiceMount"
	Version         string   `json:"version"`          // public release string (internal/version.Version)
	ContractVersion int      `json:"contract_version"` // ContractVersion
	InstanceID      string   `json:"instance_id"`      // stable per-install UUID
	VolumeName      string   `json:"volume_name"`      // e.g. "zpool"
	MountPoint      string   `json:"mount_point"`      // e.g. "/Volumes/zpool"
	NASRoot         string   `json:"nas_root"`         // today == mount_point
	ControlPlane    string   `json:"control_plane"`    // e.g. "http://127.0.0.1:11050"
	MetadataDBPath  string   `json:"metadata_db_path,omitempty"`
	Deployment      string   `json:"deployment"`   // "gui" | "cli"
	Capabilities    []string `json:"capabilities"` // DERIVED, never hardcoded
}

// capabilityVocab is the ONLY set of tokens that may appear in capabilities.
// Mirror of contract/spec/capabilities.md "Capability tokens (v1)".
var capabilityVocab = map[string]bool{
	"health": true, "whoami": true, "residency": true, "lookup": true,
	"cache-status": true, "offline": true, "spool": true, "activity": true,
	"pin": true, "unpin": true, "self-test": true, "verify-pins": true,
	"metrics": true, "derivatives": true, "metadata": true, "contribute": true,
}

// routeCapAlias maps a served route (no leading slash) to a capability token when
// the token differs from the route path. OL-1: the write route is
// "/derivatives/register" but the capability is `contribute` (a route whose
// trimmed path isn't itself a vocabulary token).
var routeCapAlias = map[string]string{
	"derivatives/register": "contribute",
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
		} else if alias, ok := routeCapAlias[token]; ok {
			caps[alias] = true
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
