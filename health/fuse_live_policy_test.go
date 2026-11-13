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
// (nfs/jfs_link_policy.go, bundled juicefs patch 0027) equals the flags
// desktopJuiceFSLinkArgs, the function Mount calls, gives a mount on the same
// profile. The profiles include a far, thin link where Class (slow) and
// ReadPolicyClass (metered) differ, so a helper fed the wrong class fails.
func TestJuiceFSLivePolicyMatchesMountArgs(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	profiles := map[string]func() *netprofile.Profile{
		"no signal": netprofile.New,
		"forced fast": func() *netprofile.Profile {
			p := netprofile.New()
			c := netprofile.ClassFast
			p.ForceClass(&c)
			return p
		},
		"forced medium": func() *netprofile.Profile {
			p := netprofile.New()
			c := netprofile.ClassMedium
			p.ForceClass(&c)
			return p
		},
		"lan": func() *netprofile.Profile {
			p := netprofile.New()
			p.ObserveRTT(500 * time.Microsecond)
			return p
		},
		"far, RTT only": func() *netprofile.Profile {
			p := netprofile.New()
			p.ObserveRTT(200 * time.Millisecond)
			return p
		},
		"far and thin": func() *netprofile.Profile {
			p := netprofile.New()
			p.ObserveRTT(200 * time.Millisecond)
			for i := 0; i < 4; i++ {
				p.ObserveThroughput(1<<20, time.Second) // 1 MiB/s
			}
			return p
		},
	}
	sawSplit := false
	for _, env := range [][2]string{{"", ""}, {"0", ""}, {"", "0"}} {
		t.Setenv("JM_JFS_DEMAND_FIRST", env[0])
		t.Setenv("JM_JFS_OBJECT_TIMEOUTS", env[1])
		for name, mk := range profiles {
			p := mk()
			if p.Class() != p.ReadPolicyClass() {
				sawSplit = true
			}
			mount := desktopJuiceFSLinkArgs(p)
			live := nfs.JFSLinkPolicyFlags(p)
			if !reflect.DeepEqual(normDurations(t, mount), normDurations(t, live)) {
				t.Errorf("env %v %s (class %s, read %s): mount %v, live %v", env, name, p.Class(), p.ReadPolicyClass(), mount, live)
			}
		}
	}
	if !sawSplit {
		t.Fatal("no profile had Class != ReadPolicyClass: the test would not catch a swapped class")
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
