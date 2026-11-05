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

// checkSyncSmartRoots refuses a copy whose destination dst (a volume path,
// /jfs/...) is inside a smart-folder root, or is the volume root while the
// source src (a host path) or one of its entries has a smart-folder root's
// name (either may land there, depending on the trailing-slash form).
// Names are compared with case folding, as the Manager compares roots.
func checkSyncSmartRoots(getenv func(string) string, readDir func(string) ([]os.DirEntry, error), src, dst string) error {
	roots := smartRootsInUse(getenv)
	dst = strings.TrimSpace(dst)
	if len(roots) == 0 || (dst != "/jfs" && !strings.HasPrefix(dst, "/jfs/")) {
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
	rel := strings.TrimPrefix(path.Clean("/"+strings.TrimPrefix(dst, "/jfs")), "/")
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
