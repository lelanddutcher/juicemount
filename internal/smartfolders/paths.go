package smartfolders

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"
)

const (
	// DefaultRoot is the default name of the top-level smart-folder root.
	DefaultRoot = "Recents"

	// tempPrefix starts the names of the transient links written while a
	// link is retargeted. Only temp names recorded in the Manager's state
	// are ever cleaned up.
	tempPrefix = ".juicemount-smart-folder.tmp-"

	// maxPathBytes and maxNameBytes bound what is accepted from events and
	// state files (PATH_MAX and NAME_MAX on Linux and macOS).
	maxPathBytes = 4096
	maxNameBytes = 255
)

// The volume namespaces no smart folder may ever link into. Both are hidden
// names, so isHiddenComponent already excludes them; they are named here so
// the rule is explicit and tested on its own.
var reservedTopLevel = map[string]bool{
	".trash":      true, // JuiceFS trash
	".juicemount": true, // server-side derivatives; hidden by the NFS layer
}

// ValidateRootName checks a smart-folder root name: one visible path
// component that is not one of the volume's reserved namespaces.
func ValidateRootName(name string) error {
	if name == "" {
		return errors.New("smart-folder root name is empty")
	}
	if strings.ContainsAny(name, "/\x00") || name == "." || name == ".." {
		return fmt.Errorf("smart-folder root %q must be a single path component", name)
	}
	if !utf8.ValidString(name) || len(name) > maxNameBytes {
		return fmt.Errorf("smart-folder root %q is not a valid name", name)
	}
	if isHiddenComponent(name) || reservedTopLevel[name] {
		return fmt.Errorf("smart-folder root %q must not be hidden or reserved", name)
	}
	return nil
}

// cleanVolumePath validates a volume-relative path as published on the
// juicemount:metadata channel ("Projects/A/clip.mov"; one leading slash is
// tolerated) and returns its components. Anything that is not already in
// canonical form (empty components, ".", "..", NUL, invalid UTF-8, overlong)
// is refused rather than repaired: event payloads come from any client.
func cleanVolumePath(p string) ([]string, bool) {
	p = strings.TrimPrefix(p, "/")
	if p == "" || len(p) > maxPathBytes || !utf8.ValidString(p) || strings.ContainsRune(p, 0) {
		return nil, false
	}
	if path.Clean(p) != p {
		return nil, false
	}
	parts := strings.Split(p, "/")
	for _, c := range parts {
		if c == "" || c == "." || c == ".." || len(c) > maxNameBytes {
			return nil, false
		}
	}
	return parts, true
}

// isHiddenComponent reports names Finder hides or the volume reserves: every
// dot-name (".DS_Store", "._clip.mov" AppleDouble files, ".trash",
// ".juicemount", editor temp files) and the classic Finder custom-icon file.
func isHiddenComponent(c string) bool {
	return strings.HasPrefix(c, ".") || c == "Icon\r"
}

// validName reports whether name can be a link in a view: a single visible
// component that cannot be confused with this package's own files.
func validName(name string) bool {
	if name == "" || len(name) > maxNameBytes || !utf8.ValidString(name) {
		return false
	}
	if strings.ContainsAny(name, "/\x00") || name == "." || name == ".." {
		return false
	}
	return !isHiddenComponent(name)
}
