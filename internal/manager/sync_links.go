package manager

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Symlinks inside a sync tree.
//
// Without --links, `juicefs sync` follows symlinks while it lists the
// source (pkg/sync ListAll with followLink = !Links; pkg/object/file.go
// readDirSorted stats the target and lists into a folder). So a symlink
// stored deep in a volume tree made an Out job copy whatever it points to.
// On the write side, the file store opens <root>/<key> and runs MkdirAll
// on its parent folders, so a symlink already in a volume destination
// leads writes through it. --links does not change that.
//
// guardSyncSymlinks closes both for jobs that touch the FUSE mount:
//
//   - Source side: --links is passed when both ends can store symlinks
//     (file, sftp, nfs, jfs), so a symlink is copied as a link and never
//     followed. An object store or WebDAV cannot store one, and juicefs
//     then drops --links with only a warning (cmd/sync.go "does not
//     support symlink, ignore it") and follows symlinks again. For those
//     destinations a volume source is walked first, and the job is
//     refused when any symlink in it leads outside the volume. Symlinks
//     that stay inside the volume are followed, as before.
//   - Destination side: a volume destination is walked first, and the job
//     is refused when any symlink in it leads outside the volume.
//
// The walks are not atomic with the copy. A symlink planted while juicefs
// runs is not caught; closing that needs juicefs itself confined (its own
// mount namespace, a chroot, or a non-root user).

// syncURISupportsSymlinks reports whether juicefs sync can store a symlink
// at uri (pkg/object SupportSymlink: file, nfs, sftp, and jfs volumes).
func syncURISupportsSymlinks(uri string) bool {
	i := strings.Index(uri, "://")
	if i <= 0 {
		return false
	}
	switch strings.ToLower(uri[:i]) {
	case "file", "nfs", "sftp", "jfs":
		return true
	}
	return false
}

// guardSyncSymlinks applies the policy above. source and destination are
// the user-facing paths RunSync was given (already confined and cleaned);
// src and dst are the juicefs URIs. It returns whether to pass --links.
func guardSyncSymlinks(ctx context.Context, fuseMount, source, destination, src, dst string) (bool, error) {
	srcVol := isVolumeUserPath(strings.TrimSpace(source))
	dstVol := isVolumeUserPath(strings.TrimSpace(destination))
	if !srcVol && !dstVol {
		return false, nil
	}
	if dstVol {
		vp, err := confineVolumePath(destination, volumeUserPrefix, fuseMount, true)
		if err != nil {
			return false, fmt.Errorf("sync destination %q refused: %w", destination, err)
		}
		esc, err := findEscapingSymlink(ctx, vp.Real, vp.MountReal)
		if err != nil {
			return false, fmt.Errorf("check destination %q for symlinks: %w", destination, err)
		}
		if esc != "" {
			return false, fmt.Errorf("sync destination refused: %s is a symlink that leads outside the JuiceFS volume, and the copy would write through it; remove it or choose another destination", esc)
		}
	}
	links := syncURISupportsSymlinks(src) && syncURISupportsSymlinks(dst)
	if srcVol && !links {
		vp, err := confineVolumePath(source, volumeUserPrefix, fuseMount, true)
		if err != nil {
			return false, fmt.Errorf("sync source %q refused: %w", source, err)
		}
		esc, err := findEscapingSymlink(ctx, vp.Real, vp.MountReal)
		if err != nil {
			return false, fmt.Errorf("check source %q for symlinks: %w", source, err)
		}
		if esc != "" {
			return false, fmt.Errorf("sync source refused: %s is a symlink that leads outside the JuiceFS volume, and this destination cannot store symlinks, so the copy would include its target; remove it or use a file, SFTP or JuiceFS destination", esc)
		}
	}
	return links, nil
}

// findEscapingSymlink walks root (a resolved path under mountReal) without
// following symlinks. It returns the user-facing path (/jfs/...) of the
// first symlink whose target resolves outside mountReal, or "" if none
// does. A missing root holds nothing. A dangling symlink is judged by
// where its target would be created.
func findEscapingSymlink(ctx context.Context, root, mountReal string) (string, error) {
	if _, err := os.Lstat(root); errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	var found string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if d.Type()&fs.ModeSymlink == 0 {
			return nil
		}
		target, rerr := walkPhysicalPath(p, true)
		if rerr != nil || !pathWithin(target, mountReal) {
			found = volumeUserPrefix + strings.TrimPrefix(p, strings.TrimSuffix(mountReal, "/"))
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return found, nil
}
