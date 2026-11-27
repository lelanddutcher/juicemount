// Package enginehost starts, watches, restarts and stops the engine helper
// (`juicefs jm-engine`, JuiceFS patch 0037) and talks to it over its private
// Unix socket.
//
// Step 1 of engine-direct ("shadow"): the helper runs read-only with no
// cache beside the unchanged macFUSE path. Only health passes through the
// socket (hello, ping, stats, getattr, shutdown). No user data does.
//
// Step 2 ("reads", door.go): `juicefs mount` itself serves the socket beside
// its kernel mount ("both doors", one engine, one cache), and the NFS front
// reads file bytes through it by inode: open, read, release.
package enginehost

import (
	"fmt"
	"strings"
)

// Level is the JM_ENGINE switch. Levels above Shadow arrive in later steps.
type Level int

const (
	// LevelFUSE is today's behaviour: no helper. The default.
	LevelFUSE Level = iota
	// LevelShadow runs an idle, read-only helper beside juicefs mount.
	LevelShadow
	// LevelReads opens the engine socket inside juicefs mount and reads
	// file bytes through it. No helper process runs.
	LevelReads
)

func (l Level) String() string {
	switch l {
	case LevelShadow:
		return "shadow"
	case LevelReads:
		return "reads"
	}
	return "fuse"
}

// laterLevels are the design's levels this build does not have yet.
var laterLevels = map[string]bool{"control": true, "meta": true, "writes": true, "direct": true}

// ParseLevel reads a JM_ENGINE value. Empty and "fuse" are LevelFUSE. A
// level this build does not implement, or an unknown word, is an error;
// the caller then keeps LevelFUSE.
func ParseLevel(raw string) (Level, error) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", "fuse":
		return LevelFUSE, nil
	case "shadow":
		return LevelShadow, nil
	case "reads":
		return LevelReads, nil
	default:
		if laterLevels[v] {
			return LevelFUSE, fmt.Errorf("engine level %q is not in this build (it has fuse, shadow and reads)", v)
		}
		return LevelFUSE, fmt.Errorf("unknown engine level %q (fuse, shadow or reads)", v)
	}
}
