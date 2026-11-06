//go:build !linux

package manager

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
)

// applyPermsBeneath off Linux (the macOS jm5 embedded mode) runs chmod -R
// and chown -R -h on the resolved path. The caller resolved rel, so no
// symlink is named on the command line; BSD and GNU chmod -R skip symlinks
// found while recursing, and chown -h changes a link itself. Unlike the
// Linux walk, a component swapped for a symlink after the caller's check
// is not caught.
func applyPermsBeneath(ctx context.Context, mountReal, rel string, uid, gid int, recursive bool) error {
	target := filepath.Join(mountReal, rel)
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
