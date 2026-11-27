// Package enginehost starts, watches, restarts and stops the engine helper
// (`juicefs jm-engine`, JuiceFS patch 0037) and talks to it over its private
// Unix socket.
//
// Step 1 of engine-direct ("shadow"): the helper runs read-only with no
// cache beside the unchanged macFUSE path. Only health passes through the
// socket (hello, ping, stats, getattr, shutdown). No user data does.
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
)

func (l Level) String() string {
	if l == LevelShadow {
		return "shadow"
	}
	return "fuse"
}

// laterLevels are the design's levels this build does not have yet.
var laterLevels = map[string]bool{"reads": true, "control": true, "meta": true, "writes": true, "direct": true}

// ParseLevel reads a JM_ENGINE value. Empty and "fuse" are LevelFUSE. A
// level this build does not implement, or an unknown word, is an error;
// the caller then keeps LevelFUSE.
func ParseLevel(raw string) (Level, error) {
	switch v := strings.ToLower(strings.TrimSpace(raw)); v {
	case "", "fuse":
		return LevelFUSE, nil
	case "shadow":
		return LevelShadow, nil
	default:
		if laterLevels[v] {
			return LevelFUSE, fmt.Errorf("engine level %q is not in this build (it has fuse and shadow)", v)
		}
		return LevelFUSE, fmt.Errorf("unknown engine level %q (fuse or shadow)", v)
	}
}
