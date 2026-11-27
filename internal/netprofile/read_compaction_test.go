package netprofile

import (
	"testing"
	"time"
)

// Reads compact (juicefs patch 0036) only near the backend: medium or fast,
// low latency, no route ceiling. JM_JFS_READ_COMPACTION=1 turns it on
// everywhere.
func TestJuiceFSReadCompaction(t *testing.T) {
	t.Setenv("JM_NET_CLASS", "")
	cases := []struct {
		name string
		mk   func() *Profile
		want bool
	}{
		{"no signal (medium)", New, true},
		{"lan", func() *Profile { p := New(); p.ObserveRTT(500 * time.Microsecond); return p }, true},
		{"lan through a tunnel", func() *Profile {
			p := New()
			p.SetBackendRoute("utun4", true)
			p.ObserveRTT(500 * time.Microsecond)
			return p
		}, false},
		{"measured fast tunnel (medium)", func() *Profile {
			p := New()
			p.SetBackendRoute("utun4", true)
			p.ObserveRTT(12 * time.Millisecond)
			for i := 0; i < 6; i++ {
				p.ObserveThroughput(16<<20, time.Second)
			}
			return p
		}, false},
		{"far, no tunnel", func() *Profile { p := New(); p.ObserveRTT(200 * time.Millisecond); return p }, false},
		{"forced fast but far", func() *Profile {
			p := New()
			c := ClassFast
			p.ForceClass(&c)
			p.ObserveRTT(100 * time.Millisecond)
			return p
		}, false},
		{"route lookup failed (ceiling)", func() *Profile {
			p := New()
			c := ClassSlow
			p.CeilingClass(&c)
			p.ObserveRTT(500 * time.Microsecond)
			return p
		}, false},
		{"metered cost", func() *Profile {
			p := New()
			c := ClassMetered
			p.SetNetworkCostClass(&c)
			p.ObserveRTT(500 * time.Microsecond)
			return p
		}, false},
	}
	for _, rollback := range []bool{false, true} {
		if rollback {
			t.Setenv("JM_JFS_READ_COMPACTION", "1")
		}
		for _, c := range cases {
			want := c.want || rollback
			if got := c.mk().JuiceFSReadCompaction(); got != want {
				t.Errorf("rollback %v, %s: got %v, want %v", rollback, c.name, got, want)
			}
		}
	}
}
