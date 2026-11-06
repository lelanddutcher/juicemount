package manager

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
//     the patched juicefs sync refuses any file:// root it would have to
//     open by path.
//
// --links is passed only into the volume (RunSync).
//
// With an older juicefs (no -jmN suffix, or N < 2), the Manager keeps the
// previous behaviour: paths by name, no --links. It logs a warning once.
// When the version cannot be read at all, the sync fails and the next one
// asks again (juicefsConfinesFileRoots).

// confiningJuicefsMin is the first build whose file:// backend resolves
// beneath its root.
const confiningJuicefsMin = 2

var jmSuffix = regexp.MustCompile(`-jm([0-9]+)\s*$`)

var (
	juicefsCapsMu sync.Mutex
	juicefsCaps   = map[string]bool{}
	// juicefsVersionTimeout bounds `juicefs --version`; tests shorten it.
	juicefsVersionTimeout = 10 * time.Second
)

// juicefsConfinesFileRoots reports whether bin is a confining juicefs on
// this platform. Only an answer from a version string that parsed is
// cached, per binary path. When `juicefs --version` fails or times out
// (a slow boot, a broken binary), the status is unknown: it returns an
// error, so the sync fails with a clear message instead of running
// unconfined, and the next sync probes again. A binary that answers with
// a stock version (no -jmN suffix, or N < 2) gets the previous behaviour
// and one warning.
func juicefsConfinesFileRoots(bin string) (bool, error) {
	if runtime.GOOS != "linux" {
		return false, nil // the patch confines file roots on Linux only
	}
	juicefsCapsMu.Lock()
	defer juicefsCapsMu.Unlock()
	if v, ok := juicefsCaps[bin]; ok {
		return v, nil
	}
	version, err := probeJuicefsVersion(bin)
	if err != nil {
		return false, fmt.Errorf("cannot tell whether %q confines sync roots: `juicefs --version` failed (%v); the sync was not started, try again", bin, err)
	}
	ok := versionConfines(version)
	juicefsCaps[bin] = ok
	if !ok {
		log.Printf("manager: WARNING: %q (version %q) does not confine file:// sync roots; syncs run without --links and pass roots by path. Install the juicefs built from third_party/juicefs (version suffix -jm%d or later).", bin, version, confiningJuicefsMin)
	}
	return ok, nil
}

// probeJuicefsVersion runs `bin --version` and returns its version line.
// Output that is not a juicefs version line is an error.
func probeJuicefsVersion(bin string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), juicefsVersionTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	// A child that keeps stdout open must not hold the probe past its
	// timeout.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if ctx.Err() != nil {
		return "", fmt.Errorf("timed out after %v", juicefsVersionTimeout)
	}
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(out))
	if !strings.HasPrefix(version, "juicefs version ") {
		return "", fmt.Errorf("unexpected output %q", version)
	}
	return version, nil
}

// versionConfines reports whether a version line names a build with the
// confined file backend: suffix -jmN with N >= confiningJuicefsMin.
func versionConfines(version string) bool {
	m := jmSuffix.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	n, err := strconv.Atoi(m[1])
	return err == nil && n >= confiningJuicefsMin
}

// syncRoot is one end of a sync: either a local folder opened beneath its
// anchor (file != nil), or a URI juicefs opens itself.
type syncRoot struct {
	uri    string
	file   *os.File
	label  string // user-facing path, for error messages
	volume bool   // the JuiceFS volume: a confined FUSE root or jfs://
}

// localRootPath decides whether p (a user path or URI) is a local folder,
// and if so returns its trusted anchor and its resolved path.
func localRootPath(spec RunSyncSpec, p string) (local, volume bool, anchor, real string, err error) {
	t := strings.TrimSpace(p)
	if isVolumeUserPath(t) {
		if spec.Mode != ModeEmbedded || spec.FUSEMount == "" {
			return false, true, "", "", nil // standalone: jfs://
		}
		vp, err := confineVolumePath(t, volumeUserPrefix, spec.FUSEMount, true)
		if err != nil {
			return false, false, "", "", err
		}
		return true, true, vp.MountReal, vp.Real, nil
	}
	path := t
	if strings.HasPrefix(t, "file://") {
		path = strings.TrimPrefix(t, "file://")
	} else if strings.Contains(t, "://") || !filepath.IsAbs(t) {
		return false, false, "", "", nil
	}
	resolved, err := walkPhysicalPath(filepath.Clean(path), false)
	if err != nil {
		return false, false, "", "", fmt.Errorf("resolve %q: %w", t, err)
	}
	for _, root := range spec.SourceRoots {
		rr, rerr := walkPhysicalPath(root, false)
		if rerr == nil && pathWithin(resolved, rr) {
			return true, false, rr, resolved, nil
		}
	}
	return false, false, "", "", fmt.Errorf("%q is not under a configured source root", t)
}

// openSyncRoots opens the local ends beneath their anchors. The source
// must exist. A missing destination is created level by level beneath its
// anchor, except on a dry run: then nothing is created, and a missing
// destination is handed over as an empty private folder, so juicefs sees
// it as empty and nothing changes at the destination. The caller runs
// cleanup after juicefs exits.
func openSyncRoots(spec RunSyncSpec, source, destination, srcURI, dstURI string, dry bool) (src, dst syncRoot, cleanup func(), err error) {
	var files []*os.File
	var tmpDirs []string
	cleanup = func() {
		for _, f := range files {
			f.Close()
		}
		for _, d := range tmpDirs {
			os.RemoveAll(d)
		}
	}
	open := func(p, uri string, create bool) (syncRoot, error) {
		local, volume, anchor, real, err := localRootPath(spec, p)
		if err != nil {
			return syncRoot{}, err
		}
		if !local {
			return syncRoot{uri: uri, label: p, volume: strings.HasPrefix(uri, "jfs://")}, nil
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(real, anchor), "/")
		f, err := openRootBeneath(anchor, rel, create)
		if err != nil && dry && !create && errors.Is(err, fs.ErrNotExist) && p == destination {
			// Dry run into a destination that does not exist yet.
			dir, terr := os.MkdirTemp("", "jm-dry-run-")
			if terr != nil {
				return syncRoot{}, terr
			}
			tmpDirs = append(tmpDirs, dir)
			f, err = os.Open(dir)
		}
		if err != nil {
			return syncRoot{}, fmt.Errorf("open sync root %q: %w", p, err)
		}
		files = append(files, f)
		return syncRoot{file: f, label: p, volume: volume}, nil
	}
	if src, err = open(source, srcURI, false); err != nil {
		cleanup()
		return syncRoot{}, syncRoot{}, func() {}, err
	}
	if dst, err = open(destination, dstURI, !dry); err != nil {
		cleanup()
		return syncRoot{}, syncRoot{}, func() {}, err
	}
	return src, dst, cleanup, nil
}

// errRootNotBeneath is returned when a root resolves outside its anchor.
var errRootNotBeneath = errors.New("sync root resolves outside its permitted root (symlink)")
