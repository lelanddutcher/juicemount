package manager

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Volume-side path confinement.
//
// A user-facing volume path (/jfs/...) maps to <FUSEMount>/<rel>, the real
// FUSE mount the Manager opens. The volume holds user data, and any client
// that can write to it can store a symlink there. The kernel follows such a
// symlink when the Manager opens a path under the mount, so checking only
// the text is not enough: "/jfs/link" can land anywhere the link points.
//
// confineVolumePath is the single gate for every caller that turns a /jfs
// path into a FUSE path. It cleans the input once, maps it, resolves it the
// way the kernel would (walkPhysicalPath), and requires the result to stay
// under the resolved FUSE mount. Callers must use only the values it returns.
//
// Symlinks that stay inside the volume keep working. The check is not
// atomic with the later open: a symlink swapped in between is not caught.

// errOutsideVolume is the refusal for a path that is, or resolves, outside
// the JuiceFS volume.
var errOutsideVolume = errors.New("path outside the JuiceFS volume")

// volumeUserPrefix is the user-facing prefix RunSync and normalizeAnyURI
// map onto the FUSE mount.
const volumeUserPrefix = "/jfs"

// isVolumeUserPath reports whether p (already trimmed) is the volume
// prefix or under it. "/jfsx/..." is not. RunSync's confinement and its
// destination rewrite both use this one check, so they cannot disagree.
func isVolumeUserPath(p string) bool {
	return p == volumeUserPrefix || strings.HasPrefix(p, volumeUserPrefix+"/")
}

// confinedPath is a volume path that passed confineVolumePath.
type confinedPath struct {
	// User is the cleaned user-facing path (/jfs/...).
	User string
	// Fuse is the cleaned path under the FUSE mount text. It holds no
	// "..", so the kernel resolves it to Real (at check time).
	Fuse string
	// Real is where the kernel lands. With followFinal false, the final
	// component is not resolved (it is the object itself, for Lstat,
	// rename and remove).
	Real string
	// MountReal is the resolved FUSE mount.
	MountReal string
}

// confineVolumePath cleans userPath once, requires it to be destMount or
// under it, maps it under fuseMount, and requires its kernel resolution to
// stay under the resolved fuseMount.
//
// followFinal selects the final component's treatment. Use true for
// callers that follow it (ReadDir, Stat, walks, juicefs sync, warmup,
// chmod). Use false for callers that act on the entry itself (Lstat,
// rename, remove); the parent is still resolved.
func confineVolumePath(userPath, destMount, fuseMount string, followFinal bool) (confinedPath, error) {
	if destMount == "" || fuseMount == "" {
		return confinedPath{}, errors.New("volume paths need a FUSE mount")
	}
	userPath = strings.TrimSpace(userPath)
	if !filepath.IsAbs(userPath) {
		return confinedPath{}, errOutsideVolume
	}
	clean := filepath.Clean(userPath)
	dm := filepath.Clean(destMount)
	if clean != dm && !strings.HasPrefix(clean, dm+"/") {
		return confinedPath{}, errOutsideVolume
	}
	mount := filepath.Clean(fuseMount)
	fuse := filepath.Join(mount, strings.TrimPrefix(clean, dm))
	mountReal, err := walkPhysicalPath(mount, false)
	if err != nil {
		return confinedPath{}, errors.New("cannot resolve the FUSE mount")
	}
	var real string
	switch {
	case fuse == mount:
		real = mountReal
	case followFinal:
		real, err = walkPhysicalPath(fuse, false)
	default:
		var parent string
		parent, err = walkPhysicalPath(filepath.Dir(fuse), false)
		real = filepath.Join(parent, filepath.Base(fuse))
	}
	if err != nil {
		// A dangling symlink or an unreadable component: where the
		// kernel would land is unknown. The detail names FUSE paths, so
		// it is not passed on to API replies or job errors.
		return confinedPath{}, errOutsideVolume
	}
	if !pathWithin(real, mountReal) {
		return confinedPath{}, errOutsideVolume
	}
	return confinedPath{User: clean, Fuse: fuse, Real: real, MountReal: mountReal}, nil
}

// confineVolume is confineVolumePath with the API's configured mounts.
func (a *API) confineVolume(userPath string, followFinal bool) (confinedPath, error) {
	return confineVolumePath(userPath, a.destMount, a.fuseMount, followFinal)
}

// confineSyncVolumePath is RunSync's point-of-use check. A plain /jfs path
// (not a URL) in embedded mode is cleaned and confined, and the cleaned
// path is returned for the sync to use. Other values pass through.
func confineSyncVolumePath(p, fuseMount string) (string, error) {
	t := strings.TrimSpace(p)
	if fuseMount == "" || !isVolumeUserPath(t) {
		return p, nil
	}
	vp, err := confineVolumePath(t, volumeUserPrefix, fuseMount, true)
	if err != nil {
		return "", fmt.Errorf("sync path %q refused: %w", t, err)
	}
	return vp.User, nil
}

// jfsVolumeSourceAllowed is the gate for a volume path the Manager will
// read or write through the FUSE mount (an Out or Between source). It is
// jfsPathAllowed plus, in embedded mode, confineVolumePath. Standalone
// mode has no FUSE mount, so only the text check applies there.
func (a *API) jfsVolumeSourceAllowed(p string) bool {
	if !a.jfsPathAllowed(p) {
		return false
	}
	if a.fuseMount == "" {
		return true
	}
	_, err := a.confineVolume(p, true)
	return err == nil
}
