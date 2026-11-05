package manager

import (
	"fmt"
	"os"
	"path"
	"strings"
)

// Copies into the volume never write directly into a smart-folder root.
//
// Smart-folder views are written by the Manager under its own RESP-proxy
// principal, and so are migration and sync jobs (juicefs sync with
// JM_META). The proxy records a new folder that this principal creates
// directly in one of its smart-folder roots as a view, which the Manager
// may later remove once it is empty (internal/respproxy smartview.go). A
// sync job must therefore never create folders there: a source with a
// top-level Recents/ copied into the volume root would otherwise make
// every empty folder under it removable by the Manager.

// smartRootsInUse lists the smart-folder roots this Manager runs, read as
// the smart-folder and continuity services read them (none while smart
// folders are off; the collection views root only with JM_CONTINUITY=on).
func smartRootsInUse(getenv func(string) string) []string {
	on, s, err := smartFoldersEnv(getenv)
	if err != nil || !on {
		return nil
	}
	roots := []string{s.Root}
	if c, err := continuityEnv(getenv); err == nil && c && s.CollectionsRoot != "" {
		roots = append(roots, s.CollectionsRoot)
	}
	return roots
}

// checkSyncSmartRoots refuses a copy whose destination dst writes into the
// volume (volumePath: /jfs/..., jfs://<volume>/..., or under the embedded
// FUSE mount fuseMount) inside a smart-folder root, or at the volume root
// while the source src (a host path) or one of its entries has a
// smart-folder root's name (either may land there, depending on the
// trailing-slash form). Names are compared with case folding, as the
// Manager compares roots.
func checkSyncSmartRoots(getenv func(string) string, readDir func(string) ([]os.DirEntry, error), src, dst, fuseMount string) error {
	roots := smartRootsInUse(getenv)
	if len(roots) == 0 {
		return nil
	}
	rel, inVolume, err := volumePath(dst, fuseMount)
	if err != nil {
		return err
	}
	if !inVolume {
		return nil
	}
	isRoot := func(name string) string {
		for _, r := range roots {
			if strings.EqualFold(name, r) {
				return r
			}
		}
		return ""
	}
	if rel != "" {
		if r := isRoot(strings.SplitN(rel, "/", 2)[0]); r != "" {
			return fmt.Errorf("destination %q is inside the smart-folder root /%s, which only the Manager's smart folders write", dst, r)
		}
		return nil
	}
	src = path.Clean(strings.TrimSpace(src))
	if r := isRoot(path.Base(src)); r != "" {
		return fmt.Errorf("copying %q into the volume root would write into the smart-folder root /%s; choose a destination folder", src, r)
	}
	entries, err := readDir(src)
	if err != nil {
		return fmt.Errorf("copying into the volume root: cannot check %q for smart-folder root names (%v); choose a destination folder", src, err)
	}
	for _, e := range entries {
		if r := isRoot(e.Name()); r != "" {
			return fmt.Errorf("copying %q into the volume root would write its %q into the smart-folder root /%s; choose a destination folder", src, e.Name(), r)
		}
	}
	return nil
}

// volumePath returns the volume-relative path (no leading slash; "" for the
// volume root) a sync destination writes to, and whether it writes into
// the volume at all: /jfs/..., any jfs:// URL (the path after the volume
// name), and, with an embedded FUSE mount, file:// URLs and host paths
// under it. A destination with a ".." segment is refused rather than
// resolved, so it cannot reach the volume in a form this check misreads.
func volumePath(dst, fuseMount string) (string, bool, error) {
	dst = strings.TrimSpace(dst)
	for _, seg := range strings.Split(dst, "/") {
		if seg == ".." {
			return "", false, fmt.Errorf("destination %q has a \"..\" segment; give the folder's own path", dst)
		}
	}
	under := func(p, base string) (string, bool) {
		p, base = path.Clean(p), path.Clean(base)
		switch {
		case p == base:
			return "", true
		case strings.HasPrefix(p, base+"/"):
			return strings.TrimPrefix(p, base+"/"), true
		}
		return "", false
	}
	if i := strings.Index(dst, "://"); i > 0 && i < 10 {
		scheme, rest := strings.ToLower(dst[:i]), dst[i+3:]
		switch scheme {
		case "jfs":
			vol, p, _ := strings.Cut(rest, "/")
			if vol == "" {
				return "", false, fmt.Errorf("destination %q names no volume", dst)
			}
			return strings.TrimPrefix(path.Clean("/"+p), "/"), true, nil
		case "file":
			if fuseMount == "" {
				return "", false, nil
			}
			rel, ok := under(rest, fuseMount)
			return rel, ok, nil
		}
		return "", false, nil
	}
	if rel, ok := under(dst, "/jfs"); ok && strings.HasPrefix(dst, "/") {
		return rel, true, nil
	}
	if fuseMount != "" && strings.HasPrefix(dst, "/") {
		rel, ok := under(dst, fuseMount)
		return rel, ok, nil
	}
	return "", false, nil
}
