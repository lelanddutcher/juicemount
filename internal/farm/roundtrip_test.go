package farm

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// The reconcile's unchanged-row skip compares a row read back from the STORE
// against a freshly sanitized file row. So the property that actually matters is
// not "rowContentEqual compares every DerivRow field" — it is that a sanitized
// row SURVIVES a store round-trip unchanged. If it cannot, the skip never fires
// and the row republishes on every sweep forever.
//
// This is the gap the reflection test could not see: it pins rowContentEqual
// against DerivRow and never touches the store, so an `extra`-backed field that
// is persisted for every kind but re-hydrated for only one slips straight past.
func TestSanitizedRowSurvivesStoreRoundTrip(t *testing.T) {
	geo := derivatives.FilmstripGeo{FrameCount: 4, Cols: 2, Rows: 2, CellW: 16, CellH: 9}
	codec, codecStr := "h264", "avc1.640028"
	blobSize := int64(4096)

	for _, kind := range []string{"tech", "thumbnail", "filmstrip", "waveform", "proxy", "ai"} {
		t.Run(kind, func(t *testing.T) {
			blob, _ := reservedBlobName(kind)
			row := derivatives.DerivRow{
				Kind: kind, Status: "ready", Producer: "linux-farm", Version: 1,
				// Every kind-scoped field set on EVERY kind — what a foreign
				// producer or an attacker can put in the file.
				Filmstrip: &geo, Codec: &codec, CodecString: &codecStr, BlobSize: &blobSize,
			}
			if blob != "" {
				row.BlobRelPath = &blob
			}
			clean, ok := sanitizeSidecarRow(row)
			if !ok {
				t.Skipf("kind %q not accepted by the sanitizer", kind)
			}
			clean.Provenance = derivatives.ProvenanceSidecar

			store, err := derivatives.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.PutDeriv(4242, clean); err != nil {
				t.Fatal(err)
			}
			rows, err := store.Manifest(4242)
			if err != nil || len(rows) != 1 {
				t.Fatalf("manifest: %d rows, err %v", len(rows), err)
			}
			if !rowContentEqual(rows[0], clean) {
				t.Errorf("kind %q does not survive a store round-trip — the reconcile's "+
					"unchanged-row skip can never fire, so this row republishes on EVERY "+
					"sweep forever.\n  stored: filmstrip=%v codec=%v codec_string=%v blob_size=%v"+
					"\n  clean:  filmstrip=%v codec=%v codec_string=%v blob_size=%v",
					kind, rows[0].Filmstrip, rows[0].Codec, rows[0].CodecString, rows[0].BlobSize,
					clean.Filmstrip, clean.Codec, clean.CodecString, clean.BlobSize)
			}
		})
	}
}
