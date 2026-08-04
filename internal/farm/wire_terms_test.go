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

// TestLoadExistingAIMerged_BothBlobsNoSubKindLost pins the fix for the
// data-loss defect found in the 2026-08-03 audit: when BOTH ai.loupe.json and
// ai.logger.json exist for one inode, reading only one of them silently dropped
// the other's sub-kinds. Because the merged doc is written back and the single
// (inode,"ai") manifest row is repointed at it, the dropped faces/embeddings
// were gone for good. Both files coexisting is an EXPECTED transitional state —
// the register schema tells consumers to switch names mid-flight.
func TestLoadExistingAIMerged_BothBlobsNoSubKindLost(t *testing.T) {
	dir := t.TempDir()
	emb := "AAAA"

	// Legacy blob: on-device faces + a global embedding, pushed before cutover.
	legacy := LoupeJSON{
		LoggerVersion: 1, SchemaVersion: "1.0", IndexedAt: "2026-08-01T00:00:00Z",
		AI: &LoupeAI{
			ImageEmbeddingGlobal: &emb,
			Faces:                []LoupeFace{{ClusterID: "c1", TMs: 100, Confidence: 0.9}},
			AIProviderSummary:    map[string]string{"faces": "on-device"},
		},
	}
	// Post-cutover blob: only a transcript. Nothing here knows about the faces.
	modern := LoupeJSON{
		LoggerVersion: 1, SchemaVersion: "1.0", IndexedAt: "2026-08-02T00:00:00Z",
		AI: &LoupeAI{
			Transcript:        &LoupeTranscript{Language: "en", Model: "whisper.cpp/base.en"},
			AIProviderSummary: map[string]string{"transcript": "linux-farm"},
		},
	}
	// Laid out as production does — <mount>/.juicemount/derivatives/<inode>/ —
	// because the reader is now anchored at the mount so that no component of
	// that path can be a symlink.
	const inode = 900001
	blobDir := DerivBlobDir(dir, inode)
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]LoupeJSON{
		derivatives.AIBlobNameLegacy: legacy,
		derivatives.AIBlobName:       modern,
	} {
		b, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(blobDir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got := loadExistingAIMerged(dir, inode)

	// The legacy-only sub-kinds must survive — this is the regression.
	if len(got.Faces) != 1 || got.Faces[0].ClusterID != "c1" {
		t.Errorf("faces from the legacy blob were dropped: %+v", got.Faces)
	}
	if got.ImageEmbeddingGlobal == nil || *got.ImageEmbeddingGlobal != emb {
		t.Errorf("image_embedding_global from the legacy blob was dropped: %+v", got.ImageEmbeddingGlobal)
	}
	// The new blob's sub-kinds must be present too.
	if got.Transcript == nil || got.Transcript.Language != "en" {
		t.Errorf("transcript from the current blob missing: %+v", got.Transcript)
	}
	// Provider summaries union rather than replace.
	if got.AIProviderSummary["faces"] != "on-device" || got.AIProviderSummary["transcript"] != "linux-farm" {
		t.Errorf("ai_provider_summary did not union: %+v", got.AIProviderSummary)
	}

	// And the write target is the post-cutover name when both exist.
	if w := derivatives.AIBlobWriteName(dir); w != derivatives.AIBlobName {
		t.Errorf("write target with both present = %q, want %q", w, derivatives.AIBlobName)
	}
}

// A legacy-only dir must keep being written in place, not abandoned.
func TestAIBlobWriteName_LegacyOnlyStaysLegacy(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, derivatives.AIBlobNameLegacy), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if w := derivatives.AIBlobWriteName(dir); w != derivatives.AIBlobNameLegacy {
		t.Errorf("legacy-only write target = %q, want %q", w, derivatives.AIBlobNameLegacy)
	}
	if w := derivatives.AIBlobWriteName(t.TempDir()); w != derivatives.AIBlobName {
		t.Errorf("empty-dir write target = %q, want %q", w, derivatives.AIBlobName)
	}
}

// TestSetVersionKeyForBlob_KeyFollowsFilename pins the 2026-08-03 audit finding:
// a blob still written under the LEGACY filename must keep the LEGACY version
// key. Emitting logger_version into an ai.loupe.json would silently break every
// consumer that decodes only loupe_version (shipped ClipLogger v0.9.3), and the
// consume path swallows the decode error, so it would go dark with no signal.
func TestSetVersionKeyForBlob_KeyFollowsFilename(t *testing.T) {
	keys := func(doc *LoupeJSON) map[string]any {
		b, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}

	// Legacy filename -> loupe_version ONLY.
	legacy := &LoupeJSON{LoggerVersion: 1, SchemaVersion: "1.0", IndexedAt: "2026-08-03T00:00:00Z"}
	legacy.SetVersionKeyForBlob(derivatives.AIBlobNameLegacy)
	m := keys(legacy)
	if _, ok := m["loupe_version"]; !ok {
		t.Errorf("legacy-named blob must carry loupe_version, got %v", m)
	}
	if _, bad := m["logger_version"]; bad {
		t.Errorf("legacy-named blob must NOT carry logger_version, got %v", m)
	}

	// Post-cutover filename -> logger_version ONLY.
	modern := &LoupeJSON{LoggerVersion: 1, SchemaVersion: "1.0", IndexedAt: "2026-08-03T00:00:00Z"}
	modern.SetVersionKeyForBlob(derivatives.AIBlobName)
	m = keys(modern)
	if _, ok := m["logger_version"]; !ok {
		t.Errorf("current-named blob must carry logger_version, got %v", m)
	}
	if _, bad := m["loupe_version"]; bad {
		t.Errorf("current-named blob must NOT carry loupe_version, got %v", m)
	}

	// A doc decoded FROM legacy (version folded into LoggerVersion) still emits
	// the legacy key when it is written back to the legacy name.
	var round LoupeJSON
	if err := json.Unmarshal([]byte(`{"loupe_version":1,"schema_version":"1.0","indexed_at":"2026-08-03T00:00:00Z"}`), &round); err != nil {
		t.Fatal(err)
	}
	round.SetVersionKeyForBlob(derivatives.AIBlobNameLegacy)
	m = keys(&round)
	if v, ok := m["loupe_version"]; !ok || v.(float64) != 1 {
		t.Errorf("round-tripped legacy blob lost its version: %v", m)
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
