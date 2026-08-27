package nfs

import (
	"slices"
	"sort"
	"testing"
)

func TestDirectoryNameLessPairsAppleDoubleWithPrincipal(t *testing.T) {
	names := []string{
		"orphan", "._file2.dat", "file1.dat", ".DS_Store",
		"._orphan", "file2.dat", "._file1.dat", "unpaired.dat",
	}
	sort.Slice(names, func(i, j int) bool { return directoryNameLess(names[i], names[j]) })
	want := []string{
		".DS_Store",
		"file1.dat", "._file1.dat",
		"file2.dat", "._file2.dat",
		"orphan", "._orphan",
		"unpaired.dat",
	}
	if !slices.Equal(names, want) {
		t.Fatalf("paired directory order = %q, want %q", names, want)
	}
}

func TestDirectoryNameLessIsDeterministicForUnpairedNames(t *testing.T) {
	names := []string{"z.mov", "._missing.mov", "a.mov", "._", "..odd"}
	sort.Slice(names, func(i, j int) bool { return directoryNameLess(names[i], names[j]) })
	want := []string{"..odd", "._", "a.mov", "._missing.mov", "z.mov"}
	if !slices.Equal(names, want) {
		t.Fatalf("unpaired directory order = %q, want %q", names, want)
	}
}
