package farm

import (
	"reflect"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/derivatives"
)

// rowContentEqual decides whether a reconciled row is republished. A FALSE
// NEGATIVE (reporting "equal" for rows that differ) is worse than the churn it
// exists to prevent: a genuinely changed derivative would never reach the
// changes feed, silently and forever.
//
// Driven by reflection rather than a hand-written list, because the failure mode
// is a field added LATER that nobody remembers to compare — exactly how the
// sanitizer sweep missed updated_at two rounds ago.
func TestRowContentEqualComparesEveryMeaningfulField(t *testing.T) {
	// Fields deliberately excluded from the comparison, with the reason.
	excluded := map[string]string{
		"UpdatedAt":  "it is the value being decided; comparing it would make every row differ",
		"Provenance": "set by us, identical for any two sidecar-sourced rows",
	}

	base := derivatives.DerivRow{
		Kind: "proxy", Status: "ready", Producer: "linux-farm", Version: 1,
		Hash: sp("0123456789abcdef"), BlobRelPath: sp("proxy.mp4"),
		MediaType: sp("video/mp4"), Model: sp("m1"), Dim: ip(768),
		UpdatedAt: 1000, SourceSize: i64(1024), SourceMtime: i64(2048),
		Filmstrip:   &derivatives.FilmstripGeo{FrameCount: 4, Cols: 2, Rows: 2, CellW: 16, CellH: 9},
		Codec:       sp("h264"),
		CodecString: sp("avc1.640028"),
		BlobSize:    i64(4096),
		Provenance:  derivatives.ProvenanceSidecar,
	}

	if !rowContentEqual(base, base) {
		t.Fatal("a row is not equal to itself")
	}

	rt := reflect.TypeOf(base)
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if reason, skip := excluded[f.Name]; skip {
			// Assert the exclusion is REAL: changing it must NOT trigger a
			// republish, or the stated reason is wrong.
			mutated := mutate(t, base, f.Name)
			if !rowContentEqual(base, mutated) {
				t.Errorf("%s is documented as excluded (%s) but changing it reports a difference",
					f.Name, reason)
			}
			continue
		}
		mutated := mutate(t, base, f.Name)
		if rowContentEqual(base, mutated) {
			t.Errorf("changing %s did NOT register as a difference — a derivative that "+
				"changed in this field would never be republished on the changes feed",
				f.Name)
		}
	}
}

// A nil/non-nil transition in a pointer field must also register.
func TestRowContentEqualHandlesNilTransitions(t *testing.T) {
	withVal := derivatives.DerivRow{Kind: "proxy", Status: "ready", Producer: "linux-farm",
		Version: 1, BlobSize: i64(10), Filmstrip: &derivatives.FilmstripGeo{Cols: 1, Rows: 1}}
	withNil := withVal
	withNil.BlobSize = nil
	if rowContentEqual(withVal, withNil) {
		t.Error("BlobSize set -> nil not detected")
	}
	noStrip := withVal
	noStrip.Filmstrip = nil
	if rowContentEqual(withVal, noStrip) {
		t.Error("Filmstrip set -> nil not detected")
	}
	// Geometry differing only inside the struct must register too.
	otherGeo := withVal
	g := *withVal.Filmstrip
	g.Cols = 99
	otherGeo.Filmstrip = &g
	if rowContentEqual(withVal, otherGeo) {
		t.Error("Filmstrip geometry change not detected")
	}
}

// A failed row becoming ready is the transition that MUST republish — a reader
// waiting on `failed` would otherwise never learn the artifact arrived.
func TestRowContentEqualDetectsFailedToReady(t *testing.T) {
	failed := derivatives.DerivRow{Kind: "waveform", Status: "failed", Producer: "linux-farm", Version: 1}
	ready := failed
	ready.Status = "ready"
	ready.BlobRelPath = sp("waveform.json")
	if rowContentEqual(failed, ready) {
		t.Error("failed -> ready not detected; the reader would never learn the artifact exists")
	}
}

func mutate(t *testing.T, row derivatives.DerivRow, field string) derivatives.DerivRow {
	t.Helper()
	out := row
	v := reflect.ValueOf(&out).Elem().FieldByName(field)
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "-changed")
	case reflect.Int, reflect.Int64:
		v.SetInt(v.Int() + 7)
	case reflect.Ptr:
		switch p := v.Interface().(type) {
		case *string:
			n := *p + "-changed"
			v.Set(reflect.ValueOf(&n))
		case *int64:
			n := *p + 7
			v.Set(reflect.ValueOf(&n))
		case *int:
			n := *p + 7
			v.Set(reflect.ValueOf(&n))
		case *derivatives.FilmstripGeo:
			n := *p
			n.FrameCount += 7
			v.Set(reflect.ValueOf(&n))
		default:
			t.Fatalf("mutate: unhandled pointer field %s (%T) — extend this switch", field, p)
		}
	default:
		t.Fatalf("mutate: unhandled kind %s for field %s — extend this switch", v.Kind(), field)
	}
	return out
}

func sp(s string) *string { return &s }
func ip(i int) *int       { return &i }
