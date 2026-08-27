package nfs

import (
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

func TestMutationOpTimeoutForLink(t *testing.T) {
	const base = 800 * time.Millisecond
	tests := []struct {
		name        string
		class       netprofile.LinkClass
		highLatency bool
		want        time.Duration
	}{
		{name: "metered", class: netprofile.ClassMetered, want: 2 * time.Second},
		{name: "slow", class: netprofile.ClassSlow, want: 2 * time.Second},
		{name: "medium LAN", class: netprofile.ClassMedium, want: base},
		{name: "fast LAN", class: netprofile.ClassFast, want: base},
		{name: "fast but far", class: netprofile.ClassFast, highLatency: true, want: 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := mutationOpTimeoutFor(tc.class, tc.highLatency, base); got != tc.want {
				t.Fatalf("mutationOpTimeoutFor(%s, highLatency=%v, %v) = %v, want %v",
					tc.class, tc.highLatency, base, got, tc.want)
			}
		})
	}
}

func TestMutationOpTimeoutForDoesNotShrinkOperatorBase(t *testing.T) {
	const base = 5 * time.Second
	if got := mutationOpTimeoutFor(netprofile.ClassMetered, true, base); got != base {
		t.Fatalf("mutation timeout = %v, want operator base %v", got, base)
	}
}

func TestMutationOpTimeoutExplicitOverride(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "3500")
	if got := mutationOpTimeout(); got != 3500*time.Millisecond {
		t.Fatalf("mutation timeout override = %v, want 3.5s", got)
	}
}
