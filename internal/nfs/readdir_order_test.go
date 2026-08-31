package nfs

import (
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
