package smartfolders

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestNamesCollisionsAreDeterministic(t *testing.T) {
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	at := func(min int) time.Time { return base.Add(time.Duration(min) * time.Minute) }
	entries := []Entry{
		{Path: "Proj/Interviews/clip.mov", Changed: at(1)},
		{Path: "Proj/B-Roll/clip.mov", Changed: at(3)}, // newest clip.mov keeps the name
		{Path: "Proj/clip.mov", Changed: at(2)},        // directly in the folder: labelled with it
		{Path: "Proj/Other/Interviews/clip.mov", Changed: at(0)},
		{Path: "Proj/Interviews/CLIP.MOV", Changed: at(-1)}, // case-insensitive clash
		{Path: "Proj/x/clip (Interviews).mov", Changed: at(-2)},
		{Path: "Proj/x/README", Changed: at(-3)},
		{Path: "Proj/y/README", Changed: at(-4)},
	}
	want := []Link{
		{Name: "clip.mov", Target: "Proj/B-Roll/clip.mov"},
		{Name: "clip (Proj).mov", Target: "Proj/clip.mov"},
		{Name: "clip (Interviews).mov", Target: "Proj/Interviews/clip.mov"},
		{Name: "clip (Interviews 2).mov", Target: "Proj/Other/Interviews/clip.mov"},
		{Name: "CLIP (Interviews 3).MOV", Target: "Proj/Interviews/CLIP.MOV"},
		{Name: "clip (Interviews) (x).mov", Target: "Proj/x/clip (Interviews).mov"},
		{Name: "README", Target: "Proj/x/README"},
		{Name: "README (y)", Target: "Proj/y/README"},
	}
	got := Names(entries, nil)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Names =\n%v\nwant\n%v", got, want)
	}
	// Input order does not matter.
	rev := append([]Entry(nil), entries...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	if !reflect.DeepEqual(Names(rev, nil), want) {
		t.Fatal("Names depends on input order")
	}
	seen := map[string]bool{}
	for _, l := range got {
		if !validName(l.Name) || seen[strings.ToLower(l.Name)] {
			t.Fatalf("bad or duplicate name %q", l.Name)
		}
		seen[strings.ToLower(l.Name)] = true
	}
}

func TestNamesFitNameMax(t *testing.T) {
	long := strings.Repeat("é", 120) + ".mov" // 244 bytes
	label := strings.Repeat("d", 200)
	entries := []Entry{
		{Path: "P/a/" + long, Changed: time.Unix(3, 0)},
		{Path: "P/" + label + "/" + long, Changed: time.Unix(2, 0)},
		{Path: "P/" + label + "x/" + long, Changed: time.Unix(1, 0)},
	}
	got := Names(entries, nil)
	if len(got) != 3 {
		t.Fatalf("got %d links", len(got))
	}
	seen := map[string]bool{}
	for _, l := range got {
		if len(l.Name) > maxNameBytes || !utf8.ValidString(l.Name) || !validName(l.Name) {
			t.Fatalf("invalid name %q (%d bytes)", l.Name, len(l.Name))
		}
		if !strings.HasSuffix(l.Name, ".mov") || seen[l.Name] {
			t.Fatalf("name %q lost its extension or collides", l.Name)
		}
		seen[l.Name] = true
	}
}

// A name stays bound to its target while the target stays in the list: a
// newer file with the same base name is the one that gets the suffix.
func TestNamesAreStickyToTheirTargets(t *testing.T) {
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	first := Names([]Entry{{Path: "P/Interviews/clip.mov", Changed: base}}, nil)
	if first[0].Name != "clip.mov" {
		t.Fatalf("first = %v", first)
	}
	prev := map[string]string{"P/Interviews/clip.mov": "clip.mov"}
	got := Names([]Entry{
		{Path: "P/B-Roll/clip.mov", Changed: base.Add(time.Hour)}, // newer
		{Path: "P/Interviews/clip.mov", Changed: base},
	}, prev)
	want := []Link{
		{Name: "clip (B-Roll).mov", Target: "P/B-Roll/clip.mov"},
		{Name: "clip.mov", Target: "P/Interviews/clip.mov"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Once the old file leaves the list its name is free again, but the
	// newcomer keeps the name it already has.
	prev = map[string]string{"P/B-Roll/clip.mov": "clip (B-Roll).mov"}
	got = Names([]Entry{{Path: "P/B-Roll/clip.mov", Changed: base.Add(time.Hour)}}, prev)
	if got[0].Name != "clip (B-Roll).mov" {
		t.Fatalf("renamed while still listed: %v", got)
	}
	// Two targets claiming the same previous name: the newer keeps it.
	prev = map[string]string{"P/a/x.mov": "x.mov", "P/b/x.mov": "X.MOV"}
	got = Names([]Entry{{Path: "P/a/x.mov", Changed: base}, {Path: "P/b/x.mov", Changed: base.Add(time.Minute)}}, prev)
	if got[0].Name != "X.MOV" || got[1].Name != "x (a).mov" {
		t.Fatalf("clash = %v", got)
	}
}
