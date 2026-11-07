/*
 * JuiceMount local source (not upstream JuiceFS). Apache-2.0.
 */

package sync

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
)

// The project fence (patch 0017). JuiceMount leases projects: the folders
// a fixed number of levels below the volume root. A Manager sync takes the
// leases of the projects it writes and passes JM_SYNC_PROJECT_DEPTH; the
// destination backend then refuses writes that a symlink would carry into
// another project, and copyLink does not store a link whose target leaves
// the project it is stored in.
var jmFence struct {
	on    bool
	depth int // key components naming the project; <= 0: the root is inside it
}

// JMSetProjectDepth turns the link check on for the destination's keys.
func JMSetProjectDepth(depth int) {
	jmFence.on, jmFence.depth = true, depth
}

// jmLinkLeavesProject reports whether a relative symlink stored at key
// (relative to the destination root) with this target resolves outside the
// project folder it is stored in. The check is by name: the backend stores
// the link through real folders only, so key is the link's real place. An
// absolute target is not resolved here (the confined backends never follow
// one), so it is not refused. A link stored above the project level (its
// folder is not inside a project) is refused.
func jmLinkLeavesProject(key, target string, depth int) bool {
	if strings.HasPrefix(target, "/") {
		return false
	}
	var dir []string
	for _, c := range strings.Split(strings.Trim(key, "/"), "/") {
		if c != "" {
			dir = append(dir, c)
		}
	}
	if len(dir) > 0 {
		dir = dir[:len(dir)-1]
	}
	level := len(dir) - depth // levels below the project folder
	if level < 0 {
		return true
	}
	for _, c := range strings.Split(target, "/") {
		switch c {
		case "", ".":
		case "..":
			level--
			if level < 0 {
				return true
			}
		default:
			level++
		}
	}
	return false
}

// jmCheckLink skips (utils.ErrSkipped, with a warning the Manager counts)
// a link whose target leaves its project. The warning names the source
// path, like the other skipped-link warnings.
func jmCheckLink(src object.ObjectStorage, key, target string) error {
	if !jmFence.on || !jmLinkLeavesProject(key, target, jmFence.depth) {
		return nil
	}
	name := key
	if u, err := url.Parse(src.String()); err == nil && u.Path != "" {
		name = strings.TrimSuffix(u.Path, "/") + "/" + key
	}
	logger.Warnf("skipped symlink %q that leads outside its project (target %q)", name, target)
	return fmt.Errorf("symlink %s leads outside its project: %w", key, utils.ErrSkipped)
}
