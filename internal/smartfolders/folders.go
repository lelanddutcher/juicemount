package smartfolders

import (
	"strconv"
	"strings"
	"unicode"
)

// Folder is one wanted view named after a display name (a collection's
// name) rather than after a folder of the volume.
type Folder struct {
	Key  string // stable identity, e.g. a collection ID
	Name string // display name: any text
}

// DefaultFolderName stands in for a display name with nothing usable left.
const DefaultFolderName = "Untitled"

// maxFolderBaseBytes leaves room for a " (n)" suffix within NAME_MAX.
const maxFolderBaseBytes = maxNameBytes - 16

// FolderName turns a display name into a view folder name: "/" and ":"
// (which Finder shows as "/") and control characters become "-", leading
// dots are dropped (a dot-name is hidden), surrounding spaces are trimmed
// and the result is cut to fit NAME_MAX with room for a " (n)" suffix. A
// name with nothing left is DefaultFolderName.
func FolderName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToValidUTF8(name, "-") {
		if r == '/' || r == ':' || unicode.IsControl(r) {
			r = '-'
		}
		b.WriteRune(r)
	}
	s := strings.TrimSpace(b.String())
	s = strings.TrimSpace(strings.TrimLeft(s, "."))
	s = strings.TrimSpace(truncateRunes(s, maxFolderBaseBytes))
	if !validName(s) {
		return DefaultFolderName
	}
	return s
}

// FolderNames assigns each folder its view folder name, by key. Folders are
// given in order of precedence. A folder keeps the name it had (prev maps
// key to name) while that name still derives from its display name, so a
// path an app remembered keeps its contents. Any other folder gets
// FolderName of its display name if no folder earlier in the order holds
// it (compared case-insensitively, as macOS does), else the first free
// "<name> (2)", "<name> (3)", ... A key given twice counts once. Same input,
// same names.
func FolderNames(folders []Folder, prev map[string]string) map[string]string {
	out := map[string]string{}
	used := map[string]bool{}
	seen := map[string]bool{}
	var fresh []Folder
	for _, f := range folders {
		if seen[f.Key] {
			continue
		}
		seen[f.Key] = true
		base := FolderName(f.Name)
		if n, ok := prev[f.Key]; ok && derivesFrom(n, base) && !used[foldName(n)] {
			used[foldName(n)] = true
			out[f.Key] = n
			continue
		}
		fresh = append(fresh, f)
	}
	for _, f := range fresh {
		base := FolderName(f.Name)
		name := base
		for n := 2; used[foldName(name)]; n++ {
			name = base + " (" + strconv.Itoa(n) + ")"
		}
		used[foldName(name)] = true
		out[f.Key] = name
	}
	return out
}

// derivesFrom reports whether name is base or base numbered " (n)", n>=2.
func derivesFrom(name, base string) bool {
	if name == base {
		return true
	}
	rest, ok := strings.CutPrefix(name, base+" (")
	if !ok {
		return false
	}
	digits, ok := strings.CutSuffix(rest, ")")
	if !ok || digits == "" || digits[0] == '0' {
		return false
	}
	n, err := strconv.Atoi(digits)
	return err == nil && n >= 2
}
