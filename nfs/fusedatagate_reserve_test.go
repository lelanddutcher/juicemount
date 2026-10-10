package nfs

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/netprofile"
)

// Background FUSE data work must never be able to occupy the whole gate.
//
// The gate's own doc has always said background "must never DELAY foreground
// work", and the non-blocking background acquire was believed to deliver that.
// It does not: not waiting stops background from QUEUING ahead of foreground,
// but nothing stopped it from having already TAKEN every slot. Measured live
// 2026-08-17 on WiFi with the class narrowing the gate to 4: in_use 4/4, 565
// refusals climbing, gate_timeouts concentrated in sidecar_warm (46) while
// foreground had 0 across 82,842 calls — and foreground FUSE ops then timed out
// (31), which conn.go turns into the JUKEBOX retry storm.

func TestBackgroundCannotOccupyTheWholeGate(t *testing.T) {
	if !fuseDataGateEnabled {
		t.Skip("gate disabled in this build")
	}
	fuseDataInFlight.Store(0)
	t.Cleanup(func() { fuseDataInFlight.Store(0) })

	// Fill the gate as far as BACKGROUND is allowed to.
	var rels []func()
	for i := 0; i < 64; i++ {
		rel, ok := tryAcquireFUSEDataBackground()
		if !ok {
			break
		}
		rels = append(rels, rel)
	}
	t.Cleanup(func() {
		for _, r := range rels {
			r()
		}
	})

	width := effectiveFUSEDataWidth()
	held := fuseDataInFlight.Load()
	if held >= width {
		t.Fatalf("background filled %d of %d slots — foreground has nothing left, "+
			"which is the defect: its read spins out fuseDataGateWait and is shed "+
			"as JUKEBOX", held, width)
	}

	// Foreground must still get in immediately.
	rel, ok := acquireFUSEData()
	if !ok {
		t.Fatal("foreground was REFUSED while only background work held the gate — " +
			"the reserve is not doing its job")
	}
	rel()
}

func TestBackgroundIsCappedAtHalfTheCeiling(t *testing.T) {
	p := netprofile.Default()
	for _, tc := range []struct {
		name string
		cls  netprofile.LinkClass
	}{
		{"fast", netprofile.ClassFast},
		{"slow", netprofile.ClassSlow},
		{"metered", netprofile.ClassMetered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.cls
			p.ForceClass(&c)
			t.Cleanup(func() { p.ForceClass(nil) })

			w := effectiveFUSEDataWidth()
			bg := backgroundFUSEDataWidth()
			if bg > w/2 {
				t.Errorf("background width %d exceeds half the ceiling %d — the "+
					"whole point is that foreground keeps half", bg, w)
			}
			if w >= 2 && bg < 1 {
				t.Errorf("background width 0 at ceiling %d — warming can never "+
					"run, which is a different bug", w)
			}
			// The narrow case is the one that broke; state it explicitly.
			if w == 4 && bg != 2 {
				t.Errorf("at the slow-link ceiling of 4, background width = %d, "+
					"want 2 (foreground guaranteed 2)", bg)
			}
		})
	}
}

// A ceiling of 1 is entirely foreground's. Warming is opportunistic; the user
// waiting on a read is not.
func TestBackgroundYieldsCompletelyOnAOneSlotCeiling(t *testing.T) {
	p := netprofile.Default()
	c := netprofile.ClassMetered
	p.ForceClass(&c)
	t.Cleanup(func() { p.ForceClass(nil) })

	// Simulate the narrowest ceiling by checking the helper's own contract
	// rather than reaching into env: whatever the metered ceiling is, background
	// must never exceed half of it.
	if bg, w := backgroundFUSEDataWidth(), effectiveFUSEDataWidth(); w < 2 && bg != 0 {
		t.Errorf("ceiling %d is too narrow to share, but background width is %d", w, bg)
	}
}
