package farm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestSidecarCodecsMatchContract pins knownCodecs to the vendored contract's
// codec enum in BOTH directions.
//
// Direction 1 (narrower than the contract) is the bug that prompted this: the
// first version omitted "opus", so a legitimate farm-written opus row would have
// been rejected outright and a real artifact discarded — because `codec` is the
// one field the sanitizer rejects on rather than nulls, since absent codec is
// defined to mean h264 and silently relabelling an undecodable stream as the
// guaranteed-decodable floor is worse than dropping it.
//
// Direction 2 (wider) matters too: accepting a codec the manifest schema does
// not define means writing a row that fails the consumer's own validation.
func TestSidecarCodecsMatchContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "contract", "spec", "schema", "derivatives.schema.json"))
	if err != nil {
		t.Skipf("vendored contract unavailable: %v", err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}

	// Exact path, not a search: this schema has more than one node keyed
	// "codec", and Go map iteration order would otherwise decide the verdict.
	cur := doc
	for _, seg := range []string{"properties", "derivatives", "items", "properties", "codec"} {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("codec path broke at %q — did the schema move?", seg)
		}
		cur, ok = m[seg]
		if !ok {
			t.Fatalf("no %q on the codec path — did the schema move?", seg)
		}
	}
	enum, ok := cur.(map[string]any)["enum"].([]any)
	if !ok {
		t.Fatal("codec has no enum")
	}

	contract := map[string]bool{}
	for _, v := range enum {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("non-string in the codec enum: %v", v)
		}
		contract[s] = true
		if !knownCodecs[s] {
			t.Errorf("codec %q is in the contract but NOT in knownCodecs — "+
				"the sanitizer would DROP a legitimate row carrying it", s)
		}
	}
	for c := range knownCodecs {
		if !contract[c] {
			t.Errorf("codec %q is accepted by the sanitizer but is NOT in the contract enum — "+
				"a row carrying it would fail the consumer's own validation", c)
		}
	}
}
