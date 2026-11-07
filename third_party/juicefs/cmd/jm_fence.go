/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package cmd

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	gosync "sync"
	"syscall"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/sync"
)

// The project fence (patch 0017). JuiceMount leases projects, the folders a
// fixed number of levels below the volume root. A Manager sync checks out
// the projects it writes and sets JM_SYNC_PROJECT_DEPTH: the number of key
// components below the destination root that name a project (zero or less
// when the root is the project or a folder inside it). The destination
// backend then refuses a write that a symlink would carry into another
// project, and a link whose target leaves its project is not stored
// (pkg/sync jmCheckLink). A destination backend that cannot keep writes
// inside a project fails the sync.

// jmProjectFence applies JM_SYNC_PROJECT_DEPTH to the destination and
// returns the destination to use. A jfs:// destination named through a
// symlink (a same-project link, /data/P/alias -> sub) is resolved once by
// the fence's rules, refused if it leaves its project, and then used by its
// real path: links are stored, and checked, where they really land.
func jmProjectFence(dst object.ObjectStorage) (object.ObjectStorage, error) {
	v, ok := os.LookupEnv("JM_SYNC_PROJECT_DEPTH")
	if !ok {
		return dst, nil
	}
	depth, err := strconv.Atoi(v)
	if err != nil {
		return nil, fmt.Errorf("refused: JM_SYNC_PROJECT_DEPTH=%q is not a number", v)
	}
	inner, prefix := object.JMUnwrapPrefix(dst)
	f, ok := inner.(object.JMProjectFenced)
	if !ok {
		return nil, fmt.Errorf("refused: destination %s cannot keep writes inside a project folder (JM_SYNC_PROJECT_DEPTH)", dst)
	}
	named := jmSplit(prefix)
	volDepth := depth + len(named) // the backend's keys include the prefix
	if j, ok := inner.(*juiceFS); ok && len(named) > 0 && volDepth > 0 {
		real, err := j.jmWalk("open destination", "/"+strings.Join(named, "/"), volDepth, true, false)
		if err != nil {
			return nil, err
		}
		if strings.Join(real, "/") != strings.Join(named, "/") {
			logger.Infof("destination %s resolves to /%s", dst, strings.Join(real, "/"))
			dst = object.WithPrefix(inner, strings.Join(real, "/")+"/")
			depth = volDepth - len(real)
		}
	}
	if err := f.JMSetProjectDepth(volDepth); err != nil {
		return nil, err
	}
	sync.JMSetProjectDepth(depth)
	return dst, nil
}

// jmFences holds the jfs:// backends' project fences (*juiceFS -> depth,
// the number of volume path components that name a project). Only the
// sync destination has one.
var jmFences gosync.Map

func (j *juiceFS) JMSetProjectDepth(depth int) error {
	jmFences.Store(j, depth)
	return nil
}

func jmSplit(p string) []string {
	var out []string
	for _, c := range strings.Split(p, "/") {
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

func jmHasPrefix(cur, proj []string) bool {
	if len(cur) < len(proj) {
		return false
	}
	for i := range proj {
		if cur[i] != proj[i] {
			return false
		}
	}
	return true
}

// jmMaxHops matches Linux's MAXSYMLINKS.
const jmMaxHops = 40

// jmCheck resolves the volume path p through the volume's symlinks before a
// write and refuses it when the write would land outside p's project:
//
//   - the first depth components (the project folder and those above it)
//     must be real folders, so must every component of a path that is not
//     inside a project;
//   - below the project folder, every symlink followed must name a path
//     inside the project, and an absolute target (never followed by this
//     backend) is refused;
//   - with noLinks (a new symlink's parent), no component may be a symlink,
//     so the link's place is the one its key names.
//
// followFinal resolves the final component too (the operation follows it).
//
// The walk mirrors pkg/fs doResolve, which the write itself uses: a link's
// target is joined to the link's folder BY NAME (the components of the
// path being resolved that come before the link, not where earlier links
// led), cleaned lexically, and resolved from the volume root. So the
// check judges exactly the path juicefs will follow. A missing component
// ends the walk: the rest is created there as real folders, and after the
// lexical clean no ".." is left to climb out.
//
// The walk is not atomic with the write that follows. In JuiceMount's
// standalone mode every write also passes the Redis proxy, which checks the
// lease of the project the write really lands in; but the proxy grants a
// free project to the first writer, so it is no backstop for a project
// nobody holds.
func (j *juiceFS) jmCheck(op, p string, followFinal, noLinks bool) error {
	v, ok := jmFences.Load(j)
	if !ok || v.(int) <= 0 {
		return nil
	}
	_, err := j.jmWalk(op, p, v.(int), followFinal, noLinks)
	return err
}

// jmWalk is jmCheck's walk; it returns where p really is (the components
// after a missing one are kept by name).
func (j *juiceFS) jmWalk(op, p string, depth int, followFinal, noLinks bool) ([]string, error) {
	want := jmSplit(path.Clean("/" + p))
	inProject := len(want) > depth
	var proj []string
	if inProject {
		proj = want[:depth]
	}
	w := &jmWalker{j: j, op: op, p: p, depth: depth, proj: proj, inProject: inProject, noLinks: noLinks}
	if len(want) == 0 {
		return nil, nil
	}
	if followFinal {
		return w.resolve(want)
	}
	parent, err := w.resolve(want[:len(want)-1])
	if err != nil {
		return nil, err
	}
	return append(parent, want[len(want)-1]), nil
}

type jmWalker struct {
	j         *juiceFS
	op, p     string
	depth     int
	proj      []string
	inProject bool
	noLinks   bool
	hops      int
}

func (w *jmWalker) refuse() error {
	return &os.PathError{Op: w.op, Path: w.p, Err: object.ErrOutsideProject}
}

// resolve follows every component of names (a cleaned path from the volume
// root, as components), like doResolve with followLastSymlink.
func (w *jmWalker) resolve(names []string) ([]string, error) {
	var cur []string
	for i, name := range names {
		cur = append(cur, name)
		at := "/" + strings.Join(cur, "/")
		fi, eno := w.j.jfs.Lstat(ctx, at)
		if eno == syscall.ENOENT {
			return append(cur, names[i+1:]...), nil // created here as real folders
		}
		if eno != 0 {
			return nil, &os.PathError{Op: w.op, Path: at, Err: eno}
		}
		if !fi.IsSymlink() {
			continue
		}
		if w.noLinks || !w.inProject || len(cur) <= w.depth {
			return nil, w.refuse()
		}
		if w.hops++; w.hops > jmMaxHops {
			return nil, &os.PathError{Op: w.op, Path: w.p, Err: syscall.ELOOP}
		}
		target, eno := w.j.jfs.Readlink(ctx, at)
		if eno != 0 {
			return nil, &os.PathError{Op: w.op, Path: at, Err: eno}
		}
		t := string(target)
		if strings.HasPrefix(t, "/") || strings.Contains(t, "://") {
			return nil, w.refuse()
		}
		// doResolve: path.Join(ss[:i], target), the link's folder by name.
		next := jmSplit(path.Join("/"+strings.Join(names[:i], "/"), t))
		if len(next) < w.depth || !jmHasPrefix(next, w.proj) {
			return nil, w.refuse()
		}
		real, err := w.resolve(next)
		if err != nil {
			return nil, err
		}
		cur = real
	}
	return cur, nil
}
