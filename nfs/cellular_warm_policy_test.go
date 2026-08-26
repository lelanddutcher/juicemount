package nfs

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestSpeculativeDirectoryWarmPolicy(t *testing.T) {
	tests := []struct {
		class netprofile.LinkClass
		want  bool
	}{
		{netprofile.ClassMetered, false},
		{netprofile.ClassSlow, false},
		{netprofile.ClassMedium, true},
		{netprofile.ClassFast, true},
	}
	for _, tt := range tests {
		if got := allowSpeculativeDirectoryWarm(tt.class); got != tt.want {
			t.Fatalf("allowSpeculativeDirectoryWarm(%s) = %v, want %v", tt.class, got, tt.want)
		}
	}
}

// A constrained-link listing must not merely choose a smaller fan-out: it must
// perform zero speculative backend work. This functional check catches a
// regression where the policy helper remains correct but prefetchChildren
// stops consulting it before entering FUSE.
func TestSpeculativeDirectoryWarmDoesNotScanFUSEOnConstrainedLinks(t *testing.T) {
	for _, class := range []netprofile.LinkClass{
		netprofile.ClassMetered,
		netprofile.ClassSlow,
	} {
		t.Run(class.String(), func(t *testing.T) {
			jfs, store, fuseRoot := newDirRefreshHarness(t)
			netprofile.Default().ForceClass(&class)
			t.Cleanup(func() { netprofile.Default().ForceClass(nil) })

			mkFUSEDir(t, fuseRoot, "known", "remote-only.mov")
			jfs.handler.prefetchChildren("known")

			if got := store.LookupByPath("known/remote-only.mov"); got != nil {
				t.Fatal("constrained-link speculative prefetch touched FUSE and populated the mirror")
			}
		})
	}
}
