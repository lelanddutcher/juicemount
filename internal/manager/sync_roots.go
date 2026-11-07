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
// The juicefs the Manager ships (third_party/juicefs, patches 0016 and
// 0017, version suffix -jm3 or later) confines its file:// backend to the
// root folder it is given, and keeps a volume destination's writes inside
// their project folders (syncProjectDepth). That only protects a root the backend opens itself. A root
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
// With an older juicefs (no -jmN suffix, or N < 3), the Manager refuses a
// sync into the volume (RunSync). Other syncs keep the previous behaviour:
// paths by name, no --links. It logs a warning once.
// When the version cannot be read at all, the sync fails and the next one
// asks again (juicefsConfinesFileRoots).

// confiningJuicefsMin is the first build whose file:// backend resolves
// beneath its root (-jm2) and that has the project fence (-jm3).
const confiningJuicefsMin = 3

var jmSuffix = regexp.MustCompile(`-jm([0-9]+)\s*$`)

var (
	// juicefsCapsMu guards the two maps only; no probe runs under it.
	juicefsCapsMu         sync.Mutex
	juicefsCaps           = map[string]bool{}          // parsed answers, per binary path
	juicefsInflight       = map[string]*juicefsProbe{} // probes running now
	juicefsVersionTimeout = 10 * time.Second           // tests shorten it
)

// juicefsProbe is one `juicefs --version` run that concurrent syncs share.
type juicefsProbe struct {
	done     chan struct{} // closed when the probe has finished
	confined bool
	err      error
}

// juicefsConfinesFileRoots reports whether bin is a confining juicefs on
// this platform.
//
// Only an answer from a version line that parsed is cached, per binary
// path. When `juicefs --version` fails, times out or prints anything but
// one version line, the status is unknown: the sync fails with a message
// that says what was found, nothing is cached, and the next sync asks
// again. Concurrent syncs share one running probe (single flight) instead
// of each queuing behind it. A binary that answers with a stock version
// (no -jmN suffix, or N < 3) gets the previous behaviour, except that a
// sync into the volume is refused (RunSync), and one warning.
func juicefsConfinesFileRoots(bin string) (bool, error) {
	if runtime.GOOS != "linux" {
		return false, nil // the patch confines file roots on Linux only
	}
	juicefsCapsMu.Lock()
	if v, ok := juicefsCaps[bin]; ok {
		juicefsCapsMu.Unlock()
		return v, nil
	}
	if p, ok := juicefsInflight[bin]; ok {
		juicefsCapsMu.Unlock()
		<-p.done
		return p.confined, p.err
	}
	p := &juicefsProbe{done: make(chan struct{})}
	juicefsInflight[bin] = p
	juicefsCapsMu.Unlock()

	version, err := probeJuicefsVersion(bin)
	if err != nil {
		p.err = fmt.Errorf("sync not started: %w", err)
	} else {
		p.confined = versionConfines(version)
	}
	juicefsCapsMu.Lock()
	delete(juicefsInflight, bin)
	if p.err == nil {
		juicefsCaps[bin] = p.confined
	}
	juicefsCapsMu.Unlock()
	close(p.done)
	if p.err == nil && !p.confined {
		log.Printf("manager: WARNING: %q (version %q) does not confine file:// sync roots or keep writes inside a project; syncs into the volume are refused, other syncs run without --links and pass roots by path. Install the juicefs built from third_party/juicefs (version suffix -jm%d or later).", bin, version, confiningJuicefsMin)
	}
	return p.confined, p.err
}

// probeJuicefsVersion runs `bin --version` and returns its version line.
// Anything but exactly one line starting with "juicefs version " is an
// error that says what was found and what to fix.
func probeJuicefsVersion(bin string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), juicefsVersionTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	// A child that keeps stdout open must not hold the probe past its
	// timeout.
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if ctx.Err() != nil {
		return "", fmt.Errorf("`%s --version` did not answer within %v; if the host is busy, start the job again, otherwise check that %q is a working juicefs", bin, juicefsVersionTimeout, bin)
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("`%s --version` failed (%v); check that %q is a working juicefs", bin, err, bin)
		}
		return "", fmt.Errorf("cannot run %q (%v); point the Manager's juicefs setting (--juicefs, or JM_SYNC_JUICEFS for jm5) at the juicefs built from third_party/juicefs", bin, err)
	}
	text := strings.TrimSpace(string(out))
	if strings.Contains(text, "\n") || !strings.HasPrefix(text, "juicefs version ") {
		return "", fmt.Errorf("`%s --version` printed %q, not a single \"juicefs version ...\" line, so the Manager cannot tell whether it confines sync roots; run the juicefs binary directly, not through a wrapper that adds output", bin, text)
	}
	return text, nil
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

// syncProjectDepth returns JM_SYNC_PROJECT_DEPTH for a sync into the
// volume: the number of key components below the destination root that
// name a project folder (projectDepth below the volume root). Zero or less
// means the root is a project folder or inside one. The patched juicefs
// then refuses a write that a symlink would carry into another project and
// skips a link whose target leaves its project. Leases are taken per
// project (checkoutForSync), so without this a symlink in one project could
// lead the sync into a project it holds no lease on.
//
// In embedded mode the destination is the folder juicefs was handed (dst,
// opened beneath the FUSE mount): its real place is read back from the open
// fd, so a destination reached through a symlink is measured where it
// really is. Its project folder must be the one the leases were taken for
// (the destination's project by name), and above project depth it must be
// the folder its name says; otherwise the sync is refused. In standalone
// mode juicefs resolves the jfs:// path itself, including its prefix, and
// checks the project of the path by name.
//
// fence is false only for a dry run into a missing destination, which
// juicefs gets as an empty private folder outside the volume.
func syncProjectDepth(spec RunSyncSpec, destination string, dst *os.File, dry bool) (depth int, fence bool, err error) {
	named := strings.FieldsFunc(filepath.Clean("/"+strings.TrimPrefix(strings.TrimSpace(destination), volumeUserPrefix)), func(r rune) bool { return r == '/' })
	if dst == nil {
		return projectDepth - len(named), true, nil
	}
	real, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(int(dst.Fd())))
	if err != nil {
		return 0, false, fmt.Errorf("read back the destination folder: %w", err)
	}
	mount, err := walkPhysicalPath(spec.FUSEMount, false)
	if err != nil {
		return 0, false, errors.New("cannot resolve the FUSE mount")
	}
	if !pathWithin(real, mount) {
		if dry {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("destination %s: %w", destination, errRootNotBeneath)
	}
	at := strings.FieldsFunc(strings.TrimPrefix(real, mount), func(r rune) bool { return r == '/' })
	n := projectDepth
	if len(named) < n {
		n = len(named)
	}
	same := len(at) >= n
	for i := 0; same && i < n; i++ {
		same = at[i] == named[i]
	}
	if !same || (len(named) < projectDepth && len(at) != len(named)) {
		return 0, false, fmt.Errorf("destination %s resolves (through a symlink) into a different project folder than its name; refused, because the sync holds only the leases of the named project", destination)
	}
	return projectDepth - len(at), true, nil
}
