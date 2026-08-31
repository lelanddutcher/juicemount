package nfs

import (
	"os"
	"sort"
	"strings"
	"testing"
)

func TestReaddirPairsAppleDoubleAfterPrincipal(t *testing.T) {
	t.Setenv("JM_READDIR_PAIR_APPLEDOUBLE", "1")
	names := []string{"z.mov", "._a.mov", "a.mov", ".DS_Store", "._z.mov", "middle", "._orphan"}
	sort.Slice(names, func(i, j int) bool { return readdirEntryLess(names[i], names[j]) })
	want := []string{".DS_Store", "a.mov", "._a.mov", "middle", "._orphan", "z.mov", "._z.mov"}
	if strings.Join(names, "\n") != strings.Join(want, "\n") {
		t.Fatalf("paired order = %q, want %q", names, want)
	}
}

func TestReaddirAppleDoublePairingRollback(t *testing.T) {
	t.Setenv("JM_READDIR_PAIR_APPLEDOUBLE", "0")
	names := []string{"z.mov", "._a.mov", "a.mov", "._z.mov"}
	sort.Slice(names, func(i, j int) bool { return readdirEntryLess(names[i], names[j]) })
	want := []string{"._a.mov", "._z.mov", "a.mov", "z.mov"}
	if strings.Join(names, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rollback order = %q, want lexical %q", names, want)
	}
}

func TestOrderReadDirEntriesPairsSortedAndUnsortedInput(t *testing.T) {
	t.Setenv("JM_READDIR_PAIR_APPLEDOUBLE", "1")
	want := []string{".DS_Store", "a.mov", "._a.mov", "middle", "._orphan", "z.mov", "._z.mov"}
	for _, names := range [][]string{
		{".DS_Store", "._a.mov", "._orphan", "._z.mov", "a.mov", "middle", "z.mov"},
		{"z.mov", "._a.mov", "a.mov", ".DS_Store", "._z.mov", "middle", "._orphan"},
	} {
		entries := make([]os.FileInfo, 0, len(names))
		for _, name := range names {
			entries = append(entries, &mockFileInfo{name: name})
		}
		orderReadDirEntries(entries)
		got := make([]string, 0, len(entries))
		for _, entry := range entries {
			got = append(got, entry.Name())
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("linear paired order = %q, want %q", got, want)
		}
	}
}

func TestOrderReadDirEntriesRollbackIsLexical(t *testing.T) {
	t.Setenv("JM_READDIR_PAIR_APPLEDOUBLE", "0")
	entries := []os.FileInfo{
		&mockFileInfo{name: "z.mov"},
		&mockFileInfo{name: "._a.mov"},
		&mockFileInfo{name: "a.mov"},
		&mockFileInfo{name: "._z.mov"},
	}
	orderReadDirEntries(entries)
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	want := []string{"._a.mov", "._z.mov", "a.mov", "z.mov"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("rollback order = %q, want lexical %q", got, want)
	}
}
