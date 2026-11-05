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

func TestFolderNameSanitises(t *testing.T) {
	long := strings.Repeat("é", 200) // 400 bytes
	for in, want := range map[string]string{
		"Client A / Shoot 2": "Client A - Shoot 2",
		"Day: 1":             "Day- 1",
		"tab\there\r\n":      "tab-here--",
		"..hidden":           "hidden",
		" . ":                DefaultFolderName,
		"":                   DefaultFolderName,
		"/":                  "-",
		"Icon\r":             "Icon-",
		"bad\xffutf8":        "bad-utf8",
	} {
		if got := FolderName(in); got != want {
			t.Errorf("FolderName(%q) = %q, want %q", in, got, want)
		}
	}
	got := FolderName(long)
	if len(got) > maxFolderBaseBytes || !utf8.ValidString(got) || !validName(got) {
		t.Fatalf("long name = %d bytes, valid %v", len(got), utf8.ValidString(got))
	}
}

func TestFolderNamesCollisionsAreDeterministic(t *testing.T) {
	cols := []Folder{
		{Key: "c1", Name: "Shoot"},
		{Key: "c2", Name: "shoot"}, // clashes case-insensitively
		{Key: "c3", Name: "Sh/oot"},
		{Key: "c4", Name: "Sh:oot"}, // sanitises to the same as c3
		{Key: "c1", Name: "ignored duplicate"},
	}
	want := map[string]string{"c1": "Shoot", "c2": "shoot (2)", "c3": "Sh-oot", "c4": "Sh-oot (2)"}
	if got := FolderNames(cols, nil, nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("FolderNames = %v, want %v", got, want)
	}
	// The earlier collection is deleted: the later one keeps its folder.
	if got := FolderNames(cols[1:2], want, nil); got["c2"] != "shoot (2)" {
		t.Fatalf("kept name = %v", got)
	}
	// A rename gives a new folder; a name freed by a rename is reused.
	renamed := []Folder{{Key: "c1", Name: "Final"}, {Key: "c2", Name: "shoot"}, {Key: "c5", Name: "Shoot"}}
	got := FolderNames(renamed, want, nil)
	if !reflect.DeepEqual(got, map[string]string{"c1": "Final", "c2": "shoot (2)", "c5": "Shoot"}) {
		t.Fatalf("after rename = %v", got)
	}
	// A previous name that no longer derives from the name is dropped.
	if got := FolderNames([]Folder{{Key: "c2", Name: "shoot"}}, map[string]string{"c2": "shoot (02)"}, nil); got["c2"] != "shoot" {
		t.Fatalf("stale previous name kept: %v", got)
	}
}

func TestLinkNamesKeepGivenPrecedence(t *testing.T) {
	// The member that joined first keeps the plain name.
	got := LinkNames([]string{"B/clip.mov", "A/clip.mov", "B/clip.mov"}, nil)
	want := []Link{{Name: "clip.mov", Target: "B/clip.mov"}, {Name: "clip (A).mov", Target: "A/clip.mov"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LinkNames = %v, want %v", got, want)
	}
	// A moved member gets a fresh name; the one staying keeps its own.
	got = LinkNames([]string{"C/clip.mov", "A/clip.mov"}, map[string]string{"A/clip.mov": "clip (A).mov"})
	want = []Link{{Name: "clip.mov", Target: "C/clip.mov"}, {Name: "clip (A).mov", Target: "A/clip.mov"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LinkNames = %v, want %v", got, want)
	}
}

func TestFolderNamesCaseAndBlocked(t *testing.T) {
	// A rename that only changes case keeps the folder.
	if got := FolderNames([]Folder{{Key: "c1", Name: "SHOOT"}}, map[string]string{"c1": "Shoot"}, nil); got["c1"] != "Shoot" {
		t.Fatalf("case-only rename = %v", got)
	}
	// Names taken in the root (a user's folder, another view left behind,
	// in any case) are skipped, for newcomers and kept names alike.
	blocked := func(n string) bool { return strings.EqualFold(n, "Shoot") || n == "Final" }
	got := FolderNames([]Folder{{Key: "c1", Name: "shoot"}, {Key: "c2", Name: "Final"}}, map[string]string{"c2": "Final"}, blocked)
	if !reflect.DeepEqual(got, map[string]string{"c1": "shoot (2)", "c2": "Final (2)"}) {
		t.Fatalf("blocked = %v", got)
	}
}
