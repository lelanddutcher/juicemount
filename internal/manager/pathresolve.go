package manager

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Shared kernel-like path resolver, used by every path check: the host
// side (pathAllowed), file destinations (resolveFileDestPath) and the
// volume side (confineVolumePath).
//
// walkPhysicalPath returns where the kernel lands when it opens p. It walks
// forward one component at a time: Lstat each component, and Readlink and
// splice in the target of a symlink. ".." is applied to the resolved
// prefix, after the symlink before it, as the kernel does. The cost is one
// Lstat per component (plus the components of every symlink target), so it
// is linear in the path length. The resolvers it replaces called
// filepath.EvalSymlinks once per missing component, which re-walked the
// whole existing prefix each time.
//
// A path whose tail does not exist yet resolves to the resolved existing
// prefix plus the missing remainder as text: that is where a create would
// land. ".." inside the missing remainder is refused: it cannot be opened
// today, and collapsing it as text could land on an existing symlink.

const (
	// maxResolvePathBytes bounds the input (Linux PATH_MAX).
	maxResolvePathBytes = 4096
	// maxSymlinkHops matches Linux's MAXSYMLINKS (40), after which the
	// kernel returns ELOOP.
	maxSymlinkHops = 40
	// maxResolveComponents bounds the components processed, including
	// those spliced in from symlink targets.
	maxResolveComponents = 8192
)

// errDanglingSymlink is returned in strict mode for a symlink whose target
// does not exist.
var errDanglingSymlink = errors.New("dangling symlink")

// walkPhysicalPath resolves p. A relative p is taken against the working
// directory. With allowDangling false, a symlink whose target does not
// exist is refused (its target is unknown to the caller's policy). With
// allowDangling true, it resolves to where its target would be created.
func walkPhysicalPath(p string, allowDangling bool) (string, error) {
	if p == "" {
		return "", errors.New("empty path")
	}
	if len(p) > maxResolvePathBytes {
		return "", fmt.Errorf("path longer than %d bytes", maxResolvePathBytes)
	}
	if !filepath.IsAbs(p) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		p = wd + string(filepath.Separator) + p
	}
	queue := splitPathComponents(p)
	cur := string(filepath.Separator)
	var missing []string
	hops, processed := 0, 0
	// boundary: while len(queue) >= boundary after a pop, the popped
	// component came from a symlink target. noBoundary = not inside one.
	const noBoundary = int(^uint(0) >> 1)
	boundary := noBoundary
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		processed++
		if processed > maxResolveComponents {
			return "", fmt.Errorf("path has more than %d components", maxResolveComponents)
		}
		if len(queue) < boundary {
			boundary = noBoundary
		}
		inLink := boundary != noBoundary
		if missing != nil {
			if c == ".." {
				return "", fmt.Errorf("path climbs out of a missing folder: %s", p)
			}
			missing = append(missing, c)
			continue
		}
		if c == ".." {
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, c)
		fi, err := os.Lstat(next)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return "", err
			}
			if inLink && !allowDangling {
				return "", fmt.Errorf("%w in %s", errDanglingSymlink, p)
			}
			missing = []string{c}
			continue
		}
		switch {
		case fi.Mode()&fs.ModeSymlink != 0:
			hops++
			if hops > maxSymlinkHops {
				return "", &fs.PathError{Op: "resolve", Path: p, Err: syscall.ELOOP}
			}
			target, err := os.Readlink(next)
			if err != nil {
				return "", err
			}
			if target == "" {
				return "", fmt.Errorf("empty symlink at %s", next)
			}
			if boundary == noBoundary {
				boundary = len(queue)
			}
			if filepath.IsAbs(target) {
				cur = string(filepath.Separator)
			}
			queue = append(splitPathComponents(target), queue...)
		case fi.IsDir():
			cur = next
		default:
			if len(queue) > 0 {
				return "", &fs.PathError{Op: "resolve", Path: next, Err: syscall.ENOTDIR}
			}
			cur = next
		}
	}
	if missing != nil {
		return filepath.Join(append([]string{cur}, missing...)...), nil
	}
	return cur, nil
}

// splitPathComponents splits on the separator and drops "" and ".".
func splitPathComponents(p string) []string {
	parts := strings.Split(p, string(filepath.Separator))
	out := parts[:0]
	for _, s := range parts {
		if s != "" && s != "." {
			out = append(out, s)
		}
	}
	return out
}
