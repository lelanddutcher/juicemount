//go:build !linux

package manager

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// applyPermsBeneath off Linux (the macOS jm5 embedded mode) runs chmod -R
// and chown -R -h on the resolved path. BSD chmod follows a symlink named
// on its command line, so every component of rel, the target included, is
// checked with Lstat first and a symlink anywhere refuses the walk; while
// recursing, BSD and GNU chmod -R skip symlinks, and chown -h changes a
// link itself. Unlike the Linux walk, a component swapped for a symlink
// between this check and chmod is not caught.
func applyPermsBeneath(ctx context.Context, mountReal, rel string, uid, gid int, recursive bool) error {
	target := filepath.Join(mountReal, rel)
	cur := mountReal
	for _, part := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			return fmt.Errorf("permissions: %q leaves the volume", rel)
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			return fmt.Errorf("permissions: %w", err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("permissions: %s is a symlink; not following it", cur)
		}
	}
	chmodArgs := []string{"-R", "u+rwX,g+rwX,o+rX", target}
	cargs := chownArgs(uid, gid, target)
	if !recursive {
		chmodArgs = chmodArgs[1:]
		if cargs != nil {
			cargs = []string{"-h", chownSpec(uid, gid), target}
		}
	}
	if out, err := exec.CommandContext(ctx, "chmod", chmodArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("chmod failed: %s", firstLine(out))
	}
	if cargs != nil {
		if out, err := exec.CommandContext(ctx, "chown", cargs...).CombinedOutput(); err != nil {
			return fmt.Errorf("chown failed: %s", firstLine(out))
		}
	}
	return nil
}
