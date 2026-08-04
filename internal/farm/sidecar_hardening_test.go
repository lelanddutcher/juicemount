package farm

import (
	"encoding/json"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// TestUpdatedAtCannotPoisonTheChangesCursor: updated_at is NOT informational —
// it is the cursor of GET /derivatives/changes?since= and of the farm's
// published derivatives-changes.json. A forged MaxInt64 pins every consumer's
// cursor past the end of time, so every genuine derivative from then on is
// invisible to them, permanently. One small JSON file, zero blob bytes.
func TestUpdatedAtCannotPoisonTheChangesCursor(t *testing.T) {
	blob := "poster.jpg"
	for _, tc := range []struct {
		name string
		in   int64
	}{
		{"max-int64", 1<<63 - 1},
		{"far-future", nowUnix() + 86400*365*100},
		{"negative", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sanitizeSidecarRow(derivatives.DerivRow{
				Kind: "thumbnail", Status: "ready", Producer: "linux-farm",
				Version: 1, BlobRelPath: &blob, UpdatedAt: tc.in,
			})
			if !ok {
				t.Fatal("row dropped; it should be accepted with a neutralised stamp")
			}
			if got.UpdatedAt == tc.in {
				t.Errorf("updated_at %d passed through — it is the changes cursor, "+
					"and a forged value silences the change channel permanently", tc.in)
			}
			if got.UpdatedAt != 0 {
				t.Errorf("updated_at = %d, want 0 (PutDeriv stamps now)", got.UpdatedAt)
			}
		})
	}

	// A plausible stamp must survive untouched — clamping everything would make
	// every reconciled row look like it changed on every pass.
	realistic := nowUnix() - 3600
	got, ok := sanitizeSidecarRow(derivatives.DerivRow{
		Kind: "thumbnail", Status: "ready", Producer: "linux-farm",
		Version: 1, BlobRelPath: &blob, UpdatedAt: realistic,
	})
	if !ok || got.UpdatedAt != realistic {
		t.Errorf("legitimate updated_at %d was altered to %d", realistic, got.UpdatedAt)
	}
}

// TestTechPayloadFreshnessSignalValidated: the tech block's whole stated reason
// for mattering is that readers fall back to tech.size_bytes for freshness when
// a row carries no source_size. Validating the envelope (producer/version/hash/
// is-it-an-object) while storing the payload verbatim leaves exactly the field
// that does the damage unchecked.
func TestTechPayloadFreshnessSignalValidated(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"negative size", `{"size_bytes":-999999999999,"codec":"h264"}`},
		{"absurd size", `{"size_bytes":1e30}`},
		{"non-numeric size", `{"size_bytes":"lots"}`},
		{"negative duration", `{"size_bytes":100,"duration_ms":-1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, ok := sanitizeTechSidecar(&TechSidecar{
				Producer: "linux-farm", Version: 1, Payload: json.RawMessage(tc.payload),
			})
			if !ok {
				return // dropping the block entirely is also acceptable
			}
			var got map[string]any
			if err := json.Unmarshal(out.Payload, &got); err != nil {
				t.Fatalf("stored payload is not valid JSON: %v", err)
			}
			if v, present := got["size_bytes"]; present {
				n, isNum := v.(float64)
				if !isNum || n < 0 || n > maxPlausibleSourceBytes {
					t.Errorf("implausible size_bytes %v survived — it is the freshness signal", v)
				}
			}
			if v, present := got["duration_ms"]; present {
				if n, isNum := v.(float64); !isNum || n < 0 {
					t.Errorf("implausible duration_ms %v survived", v)
				}
			}
		})
	}

	// A real ffprobe-shaped payload must pass through intact.
	good := `{"size_bytes":1240000000,"duration_ms":51000,"video":{"codec":"h264"}}`
	out, ok := sanitizeTechSidecar(&TechSidecar{
		Producer: "linux-farm", Version: 1, Payload: json.RawMessage(good),
	})
	if !ok {
		t.Fatal("legitimate tech block dropped")
	}
	var got map[string]any
	_ = json.Unmarshal(out.Payload, &got)
	if got["size_bytes"] != float64(1240000000) || got["duration_ms"] != float64(51000) {
		t.Errorf("legitimate payload was altered: %s", out.Payload)
	}
}

// TestTechVersionClampedNotRejected: DerivRow.Version clamps, TechSidecar.Version
// used to reject — same field, opposite policies, no stated reason. Rejecting
// discards the whole tech block (and /metadata?kind=tech with it) over an integer.
func TestTechVersionClampedNotRejected(t *testing.T) {
	out, ok := sanitizeTechSidecar(&TechSidecar{
		Producer: "linux-farm", Version: 0, Payload: json.RawMessage(`{"size_bytes":1}`),
	})
	if !ok {
		t.Fatal("tech block dropped over version 0 — DerivRow clamps the same field")
	}
	if out.Version != 1 {
		t.Errorf("version = %d, want clamped to 1", out.Version)
	}
}
