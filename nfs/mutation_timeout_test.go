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

func TestFuseReadTimeoutForLink(t *testing.T) {
	tests := []struct {
		name        string
		class       netprofile.LinkClass
		highLatency bool
		want        time.Duration
	}{
		{name: "fast LAN", class: netprofile.ClassFast, want: 4 * time.Second},
		{name: "medium LAN", class: netprofile.ClassMedium, want: 4 * time.Second},
		{name: "slow", class: netprofile.ClassSlow, want: 8 * time.Second},
		{name: "metered", class: netprofile.ClassMetered, want: 8 * time.Second},
		{name: "far fast", class: netprofile.ClassFast, highLatency: true, want: 8 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fuseReadTimeoutFor(tc.class, tc.highLatency); got != tc.want {
				t.Fatalf("fuseReadTimeoutFor(%s, highLatency=%v)=%v, want %v",
					tc.class, tc.highLatency, got, tc.want)
			}
		})
	}
}

func TestFuseReadTimeoutExplicitOverride(t *testing.T) {
	t.Setenv("JM_FUSE_READ_TIMEOUT_MS", "1750")
	if got := fuseReadTimeout(); got != 1750*time.Millisecond {
		t.Fatalf("read timeout override = %v, want 1.75s", got)
	}
}

func TestUnrangedReadTimeoutFitsAWholeBlock(t *testing.T) {
	const rtt = 200 * time.Millisecond
	tests := []struct {
		name  string
		base  time.Duration
		rtt   time.Duration
		block int64
		want  time.Duration
	}{
		// 6 RTT (1.2 s) + 4 MiB at 2 Mbit/s (16.777 s).
		{name: "4 MiB abroad", base: 8 * time.Second, rtt: rtt, block: 4 << 20, want: 17977216 * time.Microsecond},
		{name: "1 MiB fits in the base", base: 8 * time.Second, rtt: rtt, block: 1 << 20, want: 8 * time.Second},
		{name: "512 KiB fits in the base", base: 8 * time.Second, rtt: rtt, block: 512 << 10, want: 8 * time.Second},
		{name: "16 MiB capped under the client timeo", base: 8 * time.Second, rtt: rtt, block: 16 << 20, want: 20 * time.Second},
		{name: "fast link keeps its base", base: 4 * time.Second, rtt: time.Millisecond, block: 4 << 20, want: 4 * time.Second},
		{name: "no RTT sample yet", base: 8 * time.Second, block: 4 << 20, want: 16777216 * time.Microsecond},
		{name: "unknown block size", base: 8 * time.Second, rtt: rtt, want: 8 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := unrangedReadTimeoutFor(tc.base, tc.rtt, tc.block); got != tc.want {
				t.Fatalf("unrangedReadTimeoutFor(%v, %v, %d) = %v, want %v", tc.base, tc.rtt, tc.block, got, tc.want)
			}
		})
	}
}
