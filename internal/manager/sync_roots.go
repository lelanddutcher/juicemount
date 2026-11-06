package manager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sync roots handed to juicefs.
//
// The juicefs the Manager ships (third_party/juicefs, patch 0016, version
// suffix -jm2 or later) confines its file:// backend to the root folder it
// is given. That only protects a root the backend opens itself. A root
// named by path is resolved by path, and every folder below the FUSE mount
// is writable by Mac clients, so a folder on the way could be swapped for
// a symlink between the Manager's check and juicefs's open.
//
// So, with a confining juicefs, the Manager opens every local root itself
// and passes the open folder to juicefs as a file descriptor:
//
//   - The trusted anchor is opened by path: the FUSE mount for volume
//     paths, the configured source root for host paths. Both are set by
//     the operator.
//   - The root is opened beneath the anchor with openat2(RESOLVE_BENEATH),
//     after the path was confined and resolved. A missing destination is
//     created level by level with mkdirat beneath the anchor.
//   - juicefs gets file:///proc/self/fd/N/ and JM_FILE_ROOT_FD_ONLY=1, so
//     the patched backend refuses any file:// root it would have to open
//     by path.
//
// --links is passed only when both ends are confined file:// roots or
// jfs:// volumes. sftp:// and nfs:// are not confined by the patch, so a
// stored link there could lead a later write on that server astray.
//
// With an older juicefs (no -jmN suffix, or N < 2), the Manager keeps the
// previous behaviour: paths by name, no --links. It logs a warning once.

// confiningJuicefsMin is the first build whose file:// backend resolves
// beneath its root.
const confiningJuicefsMin = 2

var jmSuffix = regexp.MustCompile(`-jm([0-9]+)\s*$`)

var (
	juicefsCapsMu sync.Mutex
	juicefsCaps   = map[string]bool{}
)

// juicefsConfinesFileRoots reports whether bin is a confining juicefs on
// this platform. The answer is cached per binary path.
func juicefsConfinesFileRoots(bin string) bool {
	juicefsCapsMu.Lock()
	defer juicefsCapsMu.Unlock()
	if v, ok := juicefsCaps[bin]; ok {
		return v
	}
	ok, version := probeJuicefsConfines(bin)
	juicefsCaps[bin] = ok
	if !ok {
		log.Printf("manager: WARNING: %q (version %q) does not confine file:// sync roots; syncs run without --links and pass roots by path. Install the juicefs built from third_party/juicefs (version suffix -jm%d or later).", bin, version, confiningJuicefsMin)
	}
	return ok
}

func probeJuicefsConfines(bin string) (bool, string) {
	if runtime.GOOS != "linux" {
		return false, "(" + runtime.GOOS + ")"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	version := strings.TrimSpace(string(out))
	if err != nil {
		return false, version
	}
	m := jmSuffix.FindStringSubmatch(version)
	if m == nil {
		return false, version
	}
	n, err := strconv.Atoi(m[1])
	return err == nil && n >= confiningJuicefsMin, version
}

// withSourceBasename implements preserve_structure=false: the source
// folder itself is copied into the destination, as
// <destination>/<basename of source>/. juicefs's own form for this (both
// URIs without a trailing slash) treats the root as a name prefix: it
// confines to the parent folder and also copies same-prefix siblings. So
// the Manager names the destination subfolder itself, and both ends keep
// their trailing slash.
func withSourceBasename(source, destination string) string {
	sp := source
	if i := strings.Index(sp, "://"); i > 0 {
		sp = sp[i+3:]
		if j := strings.Index(sp, "/"); j >= 0 {
			sp = sp[j:]
		}
	}
	base := filepath.Base(filepath.Clean("/" + strings.TrimSpace(sp)))
	if base == "/" || base == "." || base == "" {
		return destination
	}
	d := strings.TrimSpace(destination)
	if strings.Contains(d, "://") {
		return strings.TrimSuffix(d, "/") + "/" + base
	}
	return filepath.Join(d, base)
}

// syncRoot is one end of a sync: either a local folder opened beneath its
// anchor (file != nil), or a URI juicefs opens itself.
type syncRoot struct {
	uri   string
	file  *os.File
	label string // user-facing path, for error messages
}

// localRootPath decides whether p (a user path or URI) is a local folder,
// and if so returns its trusted anchor and its resolved path.
func localRootPath(spec RunSyncSpec, p string) (local bool, anchor, real string, err error) {
	t := strings.TrimSpace(p)
	if isVolumeUserPath(t) {
		if spec.Mode != ModeEmbedded || spec.FUSEMount == "" {
			return false, "", "", nil // standalone: jfs://
		}
		vp, err := confineVolumePath(t, volumeUserPrefix, spec.FUSEMount, true)
		if err != nil {
			return false, "", "", err
		}
		return true, vp.MountReal, vp.Real, nil
	}
	path := t
	if strings.HasPrefix(t, "file://") {
		path = strings.TrimPrefix(t, "file://")
	} else if strings.Contains(t, "://") || !filepath.IsAbs(t) {
		return false, "", "", nil
	}
	resolved, err := walkPhysicalPath(filepath.Clean(path), false)
	if err != nil {
		return false, "", "", fmt.Errorf("resolve %q: %w", t, err)
	}
	for _, root := range spec.SourceRoots {
		rr, rerr := walkPhysicalPath(root, false)
		if rerr == nil && pathWithin(resolved, rr) {
			return true, rr, resolved, nil
		}
	}
	return false, "", "", fmt.Errorf("%q is not under a configured source root", t)
}

// openSyncRoots opens the local ends beneath their anchors. The caller
// closes the returned files after juicefs has started.
func openSyncRoots(spec RunSyncSpec, source, destination, srcURI, dstURI string) (src, dst syncRoot, err error) {
	open := func(p, uri string, create bool) (syncRoot, error) {
		local, anchor, real, err := localRootPath(spec, p)
		if err != nil {
			return syncRoot{}, err
		}
		if !local {
			return syncRoot{uri: uri, label: p}, nil
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(real, anchor), "/")
		f, err := openRootBeneath(anchor, rel, create)
		if err != nil {
			return syncRoot{}, fmt.Errorf("open sync root %q: %w", p, err)
		}
		return syncRoot{file: f, label: p}, nil
	}
	if src, err = open(source, srcURI, false); err != nil {
		return syncRoot{}, syncRoot{}, err
	}
	if dst, err = open(destination, dstURI, true); err != nil {
		if src.file != nil {
			src.file.Close()
		}
		return syncRoot{}, syncRoot{}, err
	}
	return src, dst, nil
}

// errRootNotBeneath is returned when a root resolves outside its anchor.
var errRootNotBeneath = errors.New("sync root resolves outside its permitted root (symlink)")
