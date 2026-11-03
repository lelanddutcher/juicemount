package smartfolders

import (
	"path"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Link is one wanted entry in a view.
type Link struct {
	Name   string // display name inside the view directory
	Target string // volume-relative path of the target file
}

// Names assigns display names to a recency list. A file that already has a
// name (prev maps target path to its current link name) keeps it while it
// stays in the list, so a link path an app remembered never starts opening
// a different file. A newcomer gets its base name if free; otherwise
// (compared case-insensitively, as macOS does) it is labelled with its
// parent folder ("clip (Interviews).mov"), then numbered ("clip
// (Interviews 2).mov"). Newcomers are named newest first. Same input, same
// names.
func Names(entries []Entry, prev map[string]string) []Link {
	es := append([]Entry(nil), entries...)
	sortEntries(es)
	used := map[string]bool{}
	named := map[string]string{} // target -> name
	seen := map[string]bool{}
	var fresh []Entry
	for _, e := range es {
		if seen[e.Path] || !validName(path.Base(e.Path)) {
			continue
		}
		seen[e.Path] = true
		if n, ok := prev[e.Path]; ok && validName(n) && !strings.HasPrefix(n, tempPrefix) && !used[foldName(n)] {
			used[foldName(n)] = true
			named[e.Path] = n
			continue
		}
		fresh = append(fresh, e)
	}
	for _, e := range fresh {
		base := path.Base(e.Path)
		name := base
		if used[foldName(name)] {
			label := truncateRunes(path.Base(path.Dir(e.Path)), maxLabelBytes)
			stem, ext := splitExt(base)
			name = ""
			for n := 1; name == "" || used[foldName(name)]; n++ {
				suffix := " (" + label + ")"
				if n > 1 {
					suffix = " (" + label + " " + strconv.Itoa(n) + ")"
				}
				name = fitName(stem, suffix, ext)
			}
		}
		used[foldName(name)] = true
		named[e.Path] = name
	}
	out := make([]Link, 0, len(named))
	for _, e := range es {
		if n, ok := named[e.Path]; ok {
			out = append(out, Link{Name: n, Target: e.Path})
			delete(named, e.Path)
		}
	}
	return out
}

// maxLabelBytes caps the parent-folder label so a long folder name cannot
// crowd the file's own name (or its number) out of NAME_MAX.
const maxLabelBytes = 64

func foldName(s string) string { return strings.ToLower(s) }

func truncateRunes(s string, max int) string {
	for len(s) > max {
		_, size := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-size]
	}
	return s
}

// splitExt splits "clip.mov" into "clip", ".mov". A name whose only dot is
// leading has no extension (names here are never hidden, but stay safe).
func splitExt(name string) (string, string) {
	ext := path.Ext(name)
	if ext == name || ext == "." {
		return name, ""
	}
	return strings.TrimSuffix(name, ext), ext
}

// fitName joins stem+suffix+ext, shortening the stem (on a rune boundary)
// so the result fits NAME_MAX.
func fitName(stem, suffix, ext string) string {
	if len(suffix)+len(ext) > maxNameBytes {
		// A pathological extension; drop it rather than the suffix that
		// keeps the name unique. The suffix alone is short (capped label).
		ext = ""
	}
	if room := maxNameBytes - len(suffix) - len(ext); len(stem) > room {
		stem = truncateRunes(stem, room)
	}
	return stem + suffix + ext
}
