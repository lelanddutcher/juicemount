package agentmcp

import (
	"errors"
	"path"
	"path/filepath"
	"strings"
)

// pathForm says how the Manager writes a volume path in one answer.
type pathForm int

const (
	// rootedPath: "/Clients/A/clip.mov" (find and asset answers).
	rootedPath pathForm = iota
	// relativePath: "Clients/A/clip.mov" (collection members, query
	// results).
	relativePath
)

// CleanMount checks the mount point: an absolute, clean path other than "/".
func CleanMount(mount string) (string, error) {
	if mount == "" || !filepath.IsAbs(mount) || filepath.Clean(mount) != mount || mount == string(filepath.Separator) || strings.ContainsRune(mount, 0) {
		return "", errors.New("the mount point must be an absolute, clean path such as /Volumes/zpool")
	}
	return mount, nil
}

// mountPath is where a volume path the Manager returned is on this Mac: the
// mount root joined with the path made relative. ok is false (the result
// must be skipped) when the path is not in the form the answer uses, is
// empty, holds a NUL or a ".." segment, or would not stay below the root.
// It is pure string work: nothing under the mount is opened, stat'ed or
// resolved, so a symlink is never followed here.
func mountPath(root, p string, form pathForm) (string, bool) {
	if p == "" || strings.ContainsRune(p, 0) {
		return "", false
	}
	rel := p
	if form == rootedPath {
		var ok bool
		if rel, ok = strings.CutPrefix(p, "/"); !ok {
			return "", false
		}
	}
	if strings.HasPrefix(rel, "/") {
		return "", false // absolute
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == ".." {
			return "", false
		}
	}
	rel = path.Clean(rel)
	if rel == "." || !filepath.IsLocal(filepath.FromSlash(rel)) {
		return "", false
	}
	joined := filepath.Join(root, filepath.FromSlash(rel))
	if !strings.HasPrefix(joined, root+string(filepath.Separator)) {
		return "", false
	}
	return joined, true
}
