package health

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
	"github.com/lelanddutcher/juicemount/nfs"
)

// The policy the NFS server sends a running juicefs when the class changes
// (nfs/jfs_link_policy.go, bundled juicefs patch 0027) equals the flags a
// mount on the same classes gets, for every class pair and with the
// rollback switches.
func TestJuiceFSLivePolicyMatchesMountArgs(t *testing.T) {
	classes := []netprofile.LinkClass{netprofile.ClassMetered, netprofile.ClassSlow, netprofile.ClassMedium, netprofile.ClassFast}
	for _, env := range [][2]string{{"", ""}, {"0", ""}, {"", "0"}} {
		t.Setenv("JM_JFS_DEMAND_FIRST", env[0])
		t.Setenv("JM_JFS_OBJECT_TIMEOUTS", env[1])
		for _, class := range classes {
			for _, readClass := range classes {
				mount := append(desktopJuiceFSObjectIOArgs(readClass), desktopJuiceFSDemandFirstArgs(class)...)
				live := nfs.JFSLinkPolicyFlags(class, readClass)
				if !reflect.DeepEqual(normDurations(t, mount), normDurations(t, live)) {
					t.Errorf("env %v class %s read %s: mount %v, live %v", env, class, readClass, mount, live)
				}
			}
		}
	}
}

// normDurations writes "3m" and "3m0s" the same way.
func normDurations(t *testing.T, args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a
		if i > 0 && strings.HasSuffix(args[i-1], "-timeout") {
			d, err := time.ParseDuration(a)
			if err != nil {
				t.Fatal(err)
			}
			out[i] = d.String()
		}
	}
	return out
}
