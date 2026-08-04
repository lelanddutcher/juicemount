package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// The loupe->logger wire-term cutover (2026-08-03, CONSUMER_STATUS 07-18 §2).
// We WRITE the new names and READ BOTH. These pin both halves.

func TestAssertionSidecarMedia_SubClipAndLegacy(t *testing.T) {
	cases := []struct {
		name      string
		wantMedia string
		wantSub   bool
		wantOK    bool
	}{
		{"C0012.MXF.logger.json", "C0012.MXF", false, true},
		// FARM-1: the sub-clip child. Two dots precede "logger" — the exact
		// shape the consumer warned a naive parser would mis-split.
		{"C0012.MXF.v-a1b2c3d4.logger.json", "C0012.MXF", true, true},
		{"C0012.MXF.v-A1B2C3D4.logger.json", "C0012.MXF", true, true},
		{"C0012.MXF.loupe.json", "C0012.MXF", false, true},
		{"C0012.MXF.v-a1b2c3d4.loupe.json", "C0012.MXF", true, true},
		// Dotted media names must survive.
		{"A_0065.take.2.mov.logger.json", "A_0065.take.2.mov", false, true},
		// Not sidecars.
		{"C0012.MXF", "", false, false},
		{"C0012.MXF.xmp", "", false, false},
		{"ai.logger.json", "ai", false, true}, // suffix match; caller scopes by dir
		// A short/!hex infix is NOT the sub-clip marker.
		{"C0012.MXF.v-zzzz.logger.json", "C0012.MXF.v-zzzz", false, true},
		{"C0012.MXF.v-a1b2.logger.json", "C0012.MXF.v-a1b2", false, true},
	}
	for _, tc := range cases {
		media, sub, ok := AssertionSidecarMedia(tc.name)
		if ok != tc.wantOK || media != tc.wantMedia || sub != tc.wantSub {
			t.Errorf("AssertionSidecarMedia(%q) = (%q,%v,%v), want (%q,%v,%v)",
				tc.name, media, sub, ok, tc.wantMedia, tc.wantSub, tc.wantOK)
		}
		if wantCls := tc.wantOK; IsAssertionSidecarName(tc.name) != wantCls {
			t.Errorf("IsAssertionSidecarName(%q) = %v, want %v",
				tc.name, IsAssertionSidecarName(tc.name), wantCls)
		}
	}
}

func TestAssertionSidecarPath_PrefersNewButFindsLegacy(t *testing.T) {
	dir := t.TempDir()
	media := filepath.Join(dir, "C0012.MXF")

	// Nothing on disk: a fresh sidecar takes the post-cutover name.
	if got := AssertionSidecarPath(media); got != media+".logger.json" {
		t.Fatalf("fresh: got %q, want the .logger.json name", got)
	}

	// Only a legacy sidecar exists: we must update THAT file, not strand the
	// user's existing ratings by starting an empty one beside it.
	legacy := media + ".loupe.json"
	if err := os.WriteFile(legacy, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := AssertionSidecarPath(media); got != legacy {
		t.Fatalf("legacy-only: got %q, want %q", got, legacy)
	}

	// Both exist: the new name wins.
	newer := media + ".logger.json"
	if err := os.WriteFile(newer, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := AssertionSidecarPath(media); got != newer {
		t.Fatalf("both: got %q, want %q", got, newer)
	}
}

func TestResolveAIBlobName_DualRead(t *testing.T) {
	dir := t.TempDir()
	if got := derivatives.ResolveAIBlobName(dir); got != derivatives.AIBlobName {
		t.Fatalf("empty dir: got %q, want %q", got, derivatives.AIBlobName)
	}
	if err := os.WriteFile(filepath.Join(dir, derivatives.AIBlobNameLegacy), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := derivatives.ResolveAIBlobName(dir); got != derivatives.AIBlobNameLegacy {
		t.Fatalf("legacy present: got %q, want %q", got, derivatives.AIBlobNameLegacy)
	}
	if err := os.WriteFile(filepath.Join(dir, derivatives.AIBlobName), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := derivatives.ResolveAIBlobName(dir); got != derivatives.AIBlobName {
		t.Fatalf("both present: got %q, want %q", got, derivatives.AIBlobName)
	}
}

func TestLoupeJSON_VersionKeyDualReadWriteNew(t *testing.T) {
	// READ: the legacy key folds into LoggerVersion.
	var legacy LoupeJSON
	if err := json.Unmarshal([]byte(`{"loupe_version":1,"schema_version":"1.0","indexed_at":"2026-08-03T00:00:00Z"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.LoggerVersion != 1 {
		t.Errorf("legacy loupe_version did not fold into LoggerVersion: %+v", legacy)
	}
	if legacy.LegacyLoupeVersion != 0 {
		t.Errorf("legacy key must be cleared so it is never re-emitted: %+v", legacy)
	}

	// READ: the new key works directly.
	var modern LoupeJSON
	if err := json.Unmarshal([]byte(`{"logger_version":2,"schema_version":"1.0","indexed_at":"2026-08-03T00:00:00Z"}`), &modern); err != nil {
		t.Fatal(err)
	}
	if modern.LoggerVersion != 2 {
		t.Errorf("logger_version not read: %+v", modern)
	}

	// WRITE: only the new key is emitted, even for a doc decoded from legacy.
	b, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var round map[string]any
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
	if _, bad := round["loupe_version"]; bad {
		t.Errorf("must never emit loupe_version, got: %s", b)
	}
	if v, ok := round["logger_version"]; !ok || v.(float64) != 1 {
		t.Errorf("must emit logger_version:1, got: %s", b)
	}
}
