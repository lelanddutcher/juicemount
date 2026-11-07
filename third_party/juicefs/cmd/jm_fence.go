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

// jmProjectFence applies JM_SYNC_PROJECT_DEPTH to the destination.
func jmProjectFence(dst object.ObjectStorage) error {
	v, ok := os.LookupEnv("JM_SYNC_PROJECT_DEPTH")
	if !ok {
		return nil
	}
	depth, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("refused: JM_SYNC_PROJECT_DEPTH=%q is not a number", v)
	}
	inner, prefix := object.JMUnwrapPrefix(dst)
	f, ok := inner.(object.JMProjectFenced)
	if !ok {
		return fmt.Errorf("refused: destination %s cannot keep writes inside a project folder (JM_SYNC_PROJECT_DEPTH)", dst)
	}
	// The backend's keys include the prefix.
	if err := f.JMSetProjectDepth(depth + len(jmSplit(prefix))); err != nil {
		return err
	}
	sync.JMSetProjectDepth(depth)
	return nil
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
//   - below the project folder, a symlink may not lead above it or into
//     another folder, and an absolute target (never followed by this
//     backend) is refused;
//   - with noLinks (a new symlink's parent), no component may be a symlink,
//     so the link's place is the one its key names.
//
// followFinal resolves the final component too (the operation follows it).
// A missing component ends the walk: the rest is created as real folders.
//
// The walk is not atomic with the write that follows. In JuiceMount's
// standalone mode every write also passes the Redis proxy, which checks
// the lease of the project the write really lands in.
func (j *juiceFS) jmCheck(op, p string, followFinal, noLinks bool) error {
	v, ok := jmFences.Load(j)
	if !ok || v.(int) <= 0 {
		return nil
	}
	depth := v.(int)
	want := jmSplit(path.Clean("/" + p))
	if len(want) == 0 {
		return nil
	}
	todo := want
	if !followFinal {
		todo = want[:len(want)-1]
	}
	inProject := len(want) > depth
	var proj []string
	if inProject {
		proj = want[:depth]
	}
	refuse := func() error {
		return &os.PathError{Op: op, Path: p, Err: object.ErrOutsideProject}
	}
	var cur []string
	rest := append([]string(nil), todo...)
	hops := 0
	reached := false // the walk has been at the project folder
	for len(rest) > 0 {
		c := rest[0]
		rest = rest[1:]
		switch c {
		case "", ".":
			continue
		case "..":
			if len(cur) > 0 {
				cur = cur[:len(cur)-1]
			}
		default:
			cur = append(cur, c)
			name := "/" + strings.Join(cur, "/")
			fi, eno := j.jfs.Lstat(ctx, name)
			if eno == syscall.ENOENT {
				rest = nil // created below as real folders
				break
			}
			if eno != 0 {
				return &os.PathError{Op: op, Path: name, Err: eno}
			}
			if fi.IsSymlink() {
				if noLinks || !inProject || len(cur) <= depth {
					return refuse()
				}
				if hops++; hops > jmMaxHops {
					return &os.PathError{Op: op, Path: p, Err: syscall.ELOOP}
				}
				target, eno := j.jfs.Readlink(ctx, name)
				if eno != 0 {
					return &os.PathError{Op: op, Path: name, Err: eno}
				}
				t := string(target)
				if strings.HasPrefix(t, "/") || strings.Contains(t, "://") {
					return refuse()
				}
				cur = cur[:len(cur)-1]
				rest = append(strings.Split(t, "/"), rest...)
			}
		}
		// Once at the project folder, never above it or beside it.
		if inProject && len(cur) >= depth {
			reached = true
		}
		if reached && (len(cur) < depth || !jmHasPrefix(cur, proj)) {
			return refuse()
		}
	}
	return nil
}
